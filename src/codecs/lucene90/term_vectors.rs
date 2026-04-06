// SPDX-License-Identifier: Apache-2.0
//! Term vectors writer producing `.tvd`, `.tvx`, `.tvm` files.

use std::collections::BTreeSet;
use std::io;
use std::mem;

use mem_dbg::MemSize;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::packed_writers::{BlockPackedWriter, DirectMonotonicWriter, DirectWriter};
use crate::encoding::lz4;
use crate::encoding::packed::{packed_bits_required, packed_ints_write, unsigned_bits_required};
use crate::index::index_file_names;
use crate::store::{self, DataOutputWriter, IndexOutput, SharedDirectory, VecOutput};
use crate::util::byte_block_pool::ByteSliceReader;

// File extensions
pub(crate) const VECTORS_EXTENSION: &str = "tvd";
pub(crate) const INDEX_EXTENSION: &str = "tvx";
pub(crate) const META_EXTENSION: &str = "tvm";

// Codec names and version
pub(crate) const DATA_CODEC: &str = "Lucene90TermVectorsData";
pub(crate) const INDEX_CODEC_IDX: &str = "Lucene90TermVectorsIndexIdx";
pub(crate) const INDEX_CODEC_META: &str = "Lucene90TermVectorsIndexMeta";
pub(crate) const VERSION: i32 = 0;

// PackedInts.VERSION_CURRENT
const PACKED_INTS_VERSION: i32 = 2;
const CHUNK_SIZE: i32 = 4096;
const BLOCK_SHIFT: u32 = 10;
const PACKED_BLOCK_SIZE: usize = 64;

// Flag bits for term vector features
const POSITIONS: u8 = 0b001;
const OFFSETS: u8 = 0b010;
const PAYLOADS: u8 = 0b100;

/// FLAGS_BITS = unsigned_bits_required(POSITIONS | OFFSETS | PAYLOADS) = 4
const FLAGS_BITS: u32 = 4;

/// Initial capacity for shared position/offset/payload buffers.
const INITIAL_BUF_SIZE: usize = 1024;

/// Maximum docs per chunk before flushing.
const MAX_DOCS_PER_CHUNK: usize = 128;

// ---------------------------------------------------------------------------
// DocData / FieldData
// ---------------------------------------------------------------------------

/// Per-field metadata within a pending chunk. Position and offset data lives
/// in shared buffers on the writer, indexed by `pos_start`/`off_start`.
#[derive(Debug, MemSize)]
pub(crate) struct FieldData {
    field_num: u32,
    flags: i32,
    num_terms: usize,
    has_positions: bool,
    has_offsets: bool,
    has_payloads: bool,
    freqs: Vec<i32>,
    prefix_lengths: Vec<i32>,
    suffix_lengths: Vec<i32>,
    pub(crate) pos_start: usize,
    pub(crate) off_start: usize,
    pay_start: usize,
    pub(crate) total_positions: usize,
    ord: usize,
}

impl FieldData {
    /// Records a term's frequency and prefix/suffix lengths.
    fn add_term(&mut self, freq: i32, prefix_length: i32, suffix_length: i32) {
        self.freqs[self.ord] = freq;
        self.prefix_lengths[self.ord] = prefix_length;
        self.suffix_lengths[self.ord] = suffix_length;
        self.ord += 1;
    }
}

/// Per-document metadata within a pending chunk.
#[derive(Debug, MemSize)]
struct DocData {
    num_fields: i32,
    fields: Vec<FieldData>,
    pos_start: usize,
    off_start: usize,
    pay_start: usize,
}

impl DocData {
    /// Adds a new field, computing its buffer offsets from the previous field.
    fn add_field(
        &mut self,
        field_num: u32,
        num_terms: usize,
        positions: bool,
        offsets: bool,
        payloads: bool,
    ) -> &mut FieldData {
        let (pos_start, off_start, pay_start) = if let Some(last) = self.fields.last() {
            let ps = last.pos_start
                + if last.has_positions {
                    last.total_positions
                } else {
                    0
                };
            let os = last.off_start
                + if last.has_offsets {
                    last.total_positions
                } else {
                    0
                };
            let pas = last.pay_start
                + if last.has_payloads {
                    last.total_positions
                } else {
                    0
                };
            (ps, os, pas)
        } else {
            (self.pos_start, self.off_start, self.pay_start)
        };

        let flags = (if positions { POSITIONS as i32 } else { 0 })
            | (if offsets { OFFSETS as i32 } else { 0 })
            | (if payloads { PAYLOADS as i32 } else { 0 });

        self.fields.push(FieldData {
            field_num,
            flags,
            num_terms,
            has_positions: positions,
            has_offsets: offsets,
            has_payloads: payloads,
            freqs: vec![0; num_terms],
            prefix_lengths: vec![0; num_terms],
            suffix_lengths: vec![0; num_terms],
            pos_start,
            off_start,
            pay_start,
            total_positions: 0,
            ord: 0,
        });
        self.fields.last_mut().unwrap()
    }
}

// ---------------------------------------------------------------------------
// CompressingTermVectorsWriter
// ---------------------------------------------------------------------------

/// Writes term vector `.tvd`, `.tvx`, `.tvm` files for a segment.
pub(crate) struct CompressingTermVectorsWriter {
    /// Open `.tvd` handle (header already written).
    vectors_stream: Box<dyn IndexOutput>,
    /// Documents in the current (unflushed) chunk.
    pending_docs: Vec<DocData>,
    /// Current document being built.
    cur_doc: Option<DocData>,
    /// Current field being built (owned; moved into `cur_doc.fields` on `finish_field`).
    pub(crate) cur_field: Option<FieldData>,
    /// Last term bytes for prefix compression (reset per field).
    last_term: Vec<u8>,
    /// Shared position buffer across all docs/fields/terms in the chunk.
    pub(crate) positions_buf: Vec<i32>,
    /// Shared start-offset buffer.
    pub(crate) start_offsets_buf: Vec<i32>,
    /// Shared offset-length buffer (stores `end_offset - start_offset`).
    pub(crate) lengths_buf: Vec<i32>,
    /// Shared payload-length buffer.
    payload_lengths_buf: Vec<i32>,
    /// Accumulated term suffix bytes for LZ4 compression.
    term_suffixes: Vec<u8>,
    /// Accumulated payload bytes (appended to term_suffixes on finish_document).
    payload_bytes: Vec<u8>,
    /// Total number of docs seen.
    num_docs: i32,
    /// Chunk doc bases accumulated across all flushed chunks (for `.tvx`).
    doc_bases: Vec<i64>,
    /// Chunk start pointers accumulated across all flushed chunks (for `.tvx`).
    start_pointers: Vec<i64>,
    /// Total number of flushed chunks.
    num_chunks: i64,
    /// Number of chunks that were force-flushed (dirty).
    num_dirty_chunks: i64,
    /// Number of documents in dirty chunks.
    num_dirty_docs: i64,
    /// High-water mark of positions_buf usage in current chunk (for MemSize).
    pub(crate) pos_buf_used: usize,
    /// High-water mark of offset buffer usage in current chunk (for MemSize).
    pub(crate) off_buf_used: usize,
}

impl mem_dbg::MemSize for CompressingTermVectorsWriter {
    fn mem_size_rec(
        &self,
        flags: mem_dbg::SizeFlags,
        refs: &mut mem_dbg::HashMap<usize, usize>,
    ) -> usize {
        // Report used buffer size, not capacity. The shared buffers retain
        // capacity across chunk flushes (that's the optimization), but the
        // stall control should only see data actively accumulated.
        mem::size_of::<Self>()
            + self.pending_docs.mem_size_rec(flags, refs)
            + self.pos_buf_used * mem::size_of::<i32>()
            + self.off_buf_used * 2 * mem::size_of::<i32>()
            + self.term_suffixes.len()
            + self.payload_bytes.len()
            + self.last_term.capacity()
    }
}

impl CompressingTermVectorsWriter {
    /// Creates a new writer. Writes the `.tvd` header immediately.
    pub(crate) fn new(
        mut vectors_stream: Box<dyn IndexOutput>,
        segment_id: &[u8; 16],
        segment_suffix: &str,
    ) -> io::Result<Self> {
        codec_util::write_index_header(
            &mut *vectors_stream,
            DATA_CODEC,
            VERSION,
            segment_id,
            segment_suffix,
        )?;
        Ok(Self {
            vectors_stream,
            pending_docs: Vec::new(),
            cur_doc: None,
            cur_field: None,
            last_term: Vec::new(),
            positions_buf: vec![0; INITIAL_BUF_SIZE],
            start_offsets_buf: vec![0; INITIAL_BUF_SIZE],
            lengths_buf: vec![0; INITIAL_BUF_SIZE],
            payload_lengths_buf: vec![0; INITIAL_BUF_SIZE],
            term_suffixes: Vec::new(),
            payload_bytes: Vec::new(),
            num_docs: 0,
            doc_bases: Vec::new(),
            start_pointers: Vec::new(),
            num_chunks: 0,
            num_dirty_chunks: 0,
            num_dirty_docs: 0,
            pos_buf_used: 0,
            off_buf_used: 0,
        })
    }

    // -- Streaming API -------------------------------------------------------

    /// Begins a new document.
    pub(crate) fn start_document(&mut self, num_vector_fields: i32) {
        self.cur_doc = Some(self.add_doc_data(num_vector_fields));
    }

    /// Finishes the current document.
    pub(crate) fn finish_document(&mut self) -> io::Result<()> {
        // Move cur_doc into pending_docs. Stored separately during building
        // because Rust can't hold a reference into a Vec while mutating it.
        let doc = self.cur_doc.take().unwrap();
        self.pending_docs.push(doc);
        // Append payload bytes after the term suffixes
        self.term_suffixes.append(&mut self.payload_bytes);
        self.num_docs += 1;
        if self.trigger_flush() {
            self.flush(false)?;
        }
        Ok(())
    }

    /// Begins a new field within the current document.
    pub(crate) fn start_field(
        &mut self,
        field_number: u32,
        num_terms: i32,
        positions: bool,
        offsets: bool,
        payloads: bool,
    ) {
        let doc = self.cur_doc.as_mut().unwrap();
        doc.add_field(
            field_number,
            num_terms as usize,
            positions,
            offsets,
            payloads,
        );
        // Move the field out of cur_doc into cur_field for the borrow checker
        self.cur_field = self.cur_doc.as_mut().unwrap().fields.pop();
        self.last_term.clear();
    }

    /// Finishes the current field.
    pub(crate) fn finish_field(&mut self) {
        let field = self.cur_field.take().unwrap();
        self.cur_doc.as_mut().unwrap().fields.push(field);
    }

    /// Begins a new term.
    pub(crate) fn start_term(&mut self, term: &[u8], freq: i32) {
        assert!(freq >= 1);
        let prefix = if self.last_term.is_empty() {
            0
        } else {
            shared_prefix_length(&self.last_term, term)
        };

        self.cur_field.as_mut().unwrap().add_term(
            freq,
            prefix as i32,
            (term.len() - prefix) as i32,
        );
        self.term_suffixes.extend_from_slice(&term[prefix..]);

        // Copy last term
        self.last_term.clear();
        self.last_term.extend_from_slice(term);
    }

    /// No-op kept for caller compatibility — there is no corresponding finish step.
    pub(crate) fn finish_term(&mut self) {}

    /// Bulk-reads position/offset data from byte slice readers.
    pub(crate) fn add_prox(
        &mut self,
        num_prox: i32,
        positions: Option<&mut ByteSliceReader<'_>>,
        offsets: Option<&mut ByteSliceReader<'_>>,
    ) {
        let (pos_start, off_start, pay_start, total_pos, has_payloads) = {
            let f = self.cur_field.as_ref().unwrap();
            (
                f.pos_start,
                f.off_start,
                f.pay_start,
                f.total_positions,
                f.has_payloads,
            )
        };

        if let Some(pos_reader) = positions {
            let write_start = pos_start + total_pos;
            let needed = write_start + num_prox as usize;
            if needed > self.positions_buf.len() {
                self.positions_buf.resize(oversize(needed), 0);
            }

            let mut position = 0i32;
            if has_payloads {
                let pay_write = pay_start + total_pos;
                if pay_write + num_prox as usize > self.payload_lengths_buf.len() {
                    self.payload_lengths_buf
                        .resize(oversize(pay_write + num_prox as usize), 0);
                }
                for i in 0..num_prox as usize {
                    let code = store::read_vint(pos_reader).unwrap();
                    if (code & 1) != 0 {
                        let payload_length = store::read_vint(pos_reader).unwrap();
                        self.payload_lengths_buf[pay_write + i] = payload_length;
                        for _ in 0..payload_length {
                            let mut buf = [0u8; 1];
                            io::Read::read_exact(pos_reader, &mut buf).unwrap();
                            self.payload_bytes.push(buf[0]);
                        }
                    } else {
                        self.payload_lengths_buf[pay_write + i] = 0;
                    }
                    position += code >> 1;
                    self.positions_buf[write_start + i] = position;
                }
            } else {
                for i in 0..num_prox as usize {
                    let code = store::read_vint(pos_reader).unwrap();
                    position += code >> 1;
                    self.positions_buf[write_start + i] = position;
                }
            }
        }

        if let Some(off_reader) = offsets {
            let write_start = off_start + total_pos;
            let needed = write_start + num_prox as usize;
            if needed > self.start_offsets_buf.len() {
                let new_len = oversize(needed);
                self.start_offsets_buf.resize(new_len, 0);
                self.lengths_buf.resize(new_len, 0);
            }

            let mut last_end_offset = 0i32;
            for i in 0..num_prox as usize {
                let start_offset = last_end_offset + store::read_vint(off_reader).unwrap();
                let end_offset = start_offset + store::read_vint(off_reader).unwrap();
                last_end_offset = end_offset;
                self.start_offsets_buf[write_start + i] = start_offset;
                self.lengths_buf[write_start + i] = end_offset - start_offset;
            }
        }

        let field = self.cur_field.as_mut().unwrap();
        field.total_positions += num_prox as usize;
        self.pos_buf_used = self
            .pos_buf_used
            .max(pos_start + total_pos + num_prox as usize);
        self.off_buf_used = self
            .off_buf_used
            .max(off_start + total_pos + num_prox as usize);
    }

    // -- Chunk flush logic --------------------------------------------------

    /// Returns true if the current chunk should be flushed.
    fn trigger_flush(&self) -> bool {
        self.term_suffixes.len() >= CHUNK_SIZE as usize
            || self.pending_docs.len() >= MAX_DOCS_PER_CHUNK
    }

    /// Creates a `DocData` for a new document, computing buffer offsets from
    /// the last pending doc.
    fn add_doc_data(&mut self, num_vector_fields: i32) -> DocData {
        let (pos_start, off_start, pay_start) = if let Some(last_doc) = self.pending_docs.last() {
            if let Some(last) = last_doc.fields.last() {
                let ps = last.pos_start
                    + if last.has_positions {
                        last.total_positions
                    } else {
                        0
                    };
                let os = last.off_start
                    + if last.has_offsets {
                        last.total_positions
                    } else {
                        0
                    };
                let pas = last.pay_start
                    + if last.has_payloads {
                        last.total_positions
                    } else {
                        0
                    };
                (ps, os, pas)
            } else {
                (last_doc.pos_start, last_doc.off_start, last_doc.pay_start)
            }
        } else {
            (0, 0, 0)
        };

        DocData {
            num_fields: num_vector_fields,
            fields: Vec::with_capacity(num_vector_fields as usize),
            pos_start,
            off_start,
            pay_start,
        }
    }

    /// Writes one chunk of pending documents to `.tvd`.
    fn flush(&mut self, force: bool) -> io::Result<()> {
        let chunk_docs = self.pending_docs.len() as i32;
        assert!(chunk_docs > 0);

        self.num_chunks += 1;
        if force {
            self.num_dirty_chunks += 1;
            self.num_dirty_docs += chunk_docs as i64;
        }

        // Record chunk position for index
        let doc_base = self.num_docs - chunk_docs;
        self.doc_bases.push(doc_base as i64);
        self.start_pointers
            .push(self.vectors_stream.file_pointer() as i64);

        // Chunk header: docBase, (chunkDocs << 1) | dirty_bit
        self.vectors_stream.write_vint(doc_base)?;
        let dirty_bit = if force { 1 } else { 0 };
        self.vectors_stream
            .write_vint((chunk_docs << 1) | dirty_bit)?;

        let total_fields = self.flush_num_fields(chunk_docs)?;

        if total_fields > 0 {
            let field_nums = self.flush_field_nums()?;
            self.flush_fields(total_fields, &field_nums)?;
            self.flush_flags(total_fields, &field_nums)?;
            self.flush_num_terms(total_fields)?;
            self.flush_term_lengths()?;
            self.flush_term_freqs()?;
            self.flush_positions()?;
            self.flush_offsets(&field_nums)?;
            self.flush_payload_lengths()?;

            // Compress term suffixes with plain LZ4 (CompressionMode.FAST)
            let compressed = lz4::compress(&self.term_suffixes);
            self.vectors_stream.write_bytes(&compressed)?;
        }

        // Reset
        self.pending_docs.clear();
        self.cur_doc = None;
        self.cur_field = None;
        self.term_suffixes.clear();
        self.pos_buf_used = 0;
        self.off_buf_used = 0;
        Ok(())
    }

    /// Flushes any remaining docs as a dirty chunk, then writes `.tvx` and `.tvm`
    /// index/meta files. Consumes the writer.
    ///
    /// Returns the names of the three files written (`.tvd`, `.tvx`, `.tvm`).
    pub(crate) fn finish(
        mut self,
        directory: &SharedDirectory,
        segment_name: &str,
        segment_suffix: &str,
        segment_id: &[u8; 16],
        num_docs: i32,
    ) -> io::Result<Vec<String>> {
        // Flush remaining docs as a dirty chunk
        if !self.pending_docs.is_empty() {
            self.flush(true)?;
        }

        let tvd_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, VECTORS_EXTENSION);
        let tvx_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, INDEX_EXTENSION);
        let tvm_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);

        let (mut tvx, mut tvm) = {
            let mut dir = directory.lock().unwrap();
            (dir.create_output(&tvx_name)?, dir.create_output(&tvm_name)?)
        };

        // Write .tvx and .tvm headers
        codec_util::write_index_header(
            &mut *tvx,
            INDEX_CODEC_IDX,
            VERSION,
            segment_id,
            segment_suffix,
        )?;
        codec_util::write_index_header(
            &mut *tvm,
            INDEX_CODEC_META,
            VERSION,
            segment_id,
            segment_suffix,
        )?;

        // PackedInts version and chunk size
        tvm.write_vint(PACKED_INTS_VERSION)?;
        tvm.write_vint(CHUNK_SIZE)?;

        let max_pointer = self.vectors_stream.file_pointer() as i64;
        let total_chunks = self.num_chunks as u32;

        // Write FieldsIndex to .tvx and .tvm (mirrors FieldsIndexWriter.finish())
        tvm.write_le_int(num_docs)?;
        tvm.write_le_int(BLOCK_SHIFT as i32)?;
        tvm.write_le_int((total_chunks + 1) as i32)?;

        tvm.write_le_long(tvx.file_pointer() as i64)?;

        let mut docs_writer = DirectMonotonicWriter::new(BLOCK_SHIFT);
        for &db in &self.doc_bases {
            docs_writer.add(db);
        }
        if total_chunks > 0 {
            docs_writer.add(num_docs as i64);
        }
        docs_writer.finish(&mut *tvm, &mut *tvx)?;

        tvm.write_le_long(tvx.file_pointer() as i64)?;

        let mut fp_writer = DirectMonotonicWriter::new(BLOCK_SHIFT);
        for &sp in &self.start_pointers {
            fp_writer.add(sp);
        }
        fp_writer.add(max_pointer);
        fp_writer.finish(&mut *tvm, &mut *tvx)?;

        tvm.write_le_long(tvx.file_pointer() as i64)?;
        codec_util::write_footer(&mut *tvx)?;

        tvm.write_le_long(max_pointer)?;

        debug!(
            "term_vectors: num_chunks={}, num_dirty_chunks={}, num_dirty_docs={}",
            self.num_chunks, self.num_dirty_chunks, self.num_dirty_docs
        );
        tvm.write_vlong(self.num_chunks)?;
        tvm.write_vlong(self.num_dirty_chunks)?;
        tvm.write_vlong(self.num_dirty_docs)?;

        codec_util::write_footer(&mut *tvm)?;
        codec_util::write_footer(&mut *self.vectors_stream)?;

        Ok(vec![tvd_name, tvx_name, tvm_name])
    }

    // -- Flush helpers -------------------------------------------------------

    fn flush_num_fields(&mut self, chunk_docs: i32) -> io::Result<i32> {
        if chunk_docs == 1 {
            let num_fields = self.pending_docs[0].num_fields;
            self.vectors_stream.write_vint(num_fields)?;
            return Ok(num_fields);
        }

        let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
        let mut total_fields = 0i32;
        for doc in &self.pending_docs {
            writer.add(&mut *self.vectors_stream, doc.num_fields as i64)?;
            total_fields += doc.num_fields;
        }
        writer.finish(&mut *self.vectors_stream)?;
        Ok(total_fields)
    }

    fn flush_field_nums(&mut self) -> io::Result<Vec<u32>> {
        let mut field_nums_set = BTreeSet::new();
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                field_nums_set.insert(fd.field_num);
            }
        }

        let field_nums: Vec<u32> = field_nums_set.into_iter().collect();
        let num_distinct = field_nums.len();
        assert!(num_distinct > 0);

        let max_field_num = field_nums[num_distinct - 1] as i64;
        let bits_required = packed_bits_required(max_field_num);
        let token = ((num_distinct - 1).min(0x07) << 5) as u8 | bits_required as u8;
        self.vectors_stream.write_byte(token)?;
        if num_distinct > 0x07 {
            self.vectors_stream
                .write_vint((num_distinct - 1 - 0x07) as i32)?;
        }

        let values: Vec<i64> = field_nums.iter().map(|&n| n as i64).collect();
        packed_ints_write(
            &mut DataOutputWriter(&mut *self.vectors_stream),
            &values,
            bits_required,
        )?;

        Ok(field_nums)
    }

    fn flush_fields(&mut self, _total_fields: i32, field_nums: &[u32]) -> io::Result<()> {
        let bpv = unsigned_bits_required((field_nums.len() - 1) as i64);
        let mut writer = DirectWriter::new(bpv);
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                let idx = field_nums
                    .binary_search(&fd.field_num)
                    .expect("field number must be in field_nums");
                writer.add(idx as i64);
            }
        }
        let mut scratch = Vec::new();
        writer.finish(&mut VecOutput(&mut scratch))?;
        self.vectors_stream.write_vlong(scratch.len() as i64)?;
        self.vectors_stream.write_bytes(&scratch)
    }

    fn flush_flags(&mut self, _total_fields: i32, field_nums: &[u32]) -> io::Result<()> {
        let mut field_flags: Vec<i32> = vec![-1; field_nums.len()];
        let mut non_changing = true;

        'outer: for doc in &self.pending_docs {
            for fd in &doc.fields {
                let idx = field_nums
                    .binary_search(&fd.field_num)
                    .expect("field number must be in field_nums");
                if field_flags[idx] == -1 {
                    field_flags[idx] = fd.flags;
                } else if field_flags[idx] != fd.flags {
                    non_changing = false;
                    break 'outer;
                }
            }
        }

        if non_changing {
            self.vectors_stream.write_vint(0)?;
            let mut scratch = Vec::new();
            let mut writer = DirectWriter::new(FLAGS_BITS);
            for &flags in &field_flags {
                assert!(flags >= 0);
                writer.add(flags as i64);
            }
            writer.finish(&mut VecOutput(&mut scratch))?;
            self.vectors_stream.write_vint(scratch.len() as i32)?;
            self.vectors_stream.write_bytes(&scratch)
        } else {
            self.vectors_stream.write_vint(1)?;
            let mut scratch = Vec::new();
            let mut writer = DirectWriter::new(FLAGS_BITS);
            for doc in &self.pending_docs {
                for fd in &doc.fields {
                    writer.add(fd.flags as i64);
                }
            }
            writer.finish(&mut VecOutput(&mut scratch))?;
            self.vectors_stream.write_vint(scratch.len() as i32)?;
            self.vectors_stream.write_bytes(&scratch)
        }
    }

    fn flush_num_terms(&mut self, _total_fields: i32) -> io::Result<()> {
        let mut max_num_terms: i32 = 0;
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                max_num_terms |= fd.num_terms as i32;
            }
        }

        let bpv = unsigned_bits_required(max_num_terms as i64);
        self.vectors_stream.write_vint(bpv as i32)?;
        let mut scratch = Vec::new();
        let mut writer = DirectWriter::new(bpv);
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                writer.add(fd.num_terms as i64);
            }
        }
        writer.finish(&mut VecOutput(&mut scratch))?;
        self.vectors_stream.write_vint(scratch.len() as i32)?;
        self.vectors_stream.write_bytes(&scratch)
    }

    fn flush_term_lengths(&mut self) -> io::Result<()> {
        let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                for i in 0..fd.num_terms {
                    writer.add(&mut *self.vectors_stream, fd.prefix_lengths[i] as i64)?;
                }
            }
        }
        writer.finish(&mut *self.vectors_stream)?;

        writer.reset();
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                for i in 0..fd.num_terms {
                    writer.add(&mut *self.vectors_stream, fd.suffix_lengths[i] as i64)?;
                }
            }
        }
        writer.finish(&mut *self.vectors_stream)
    }

    fn flush_term_freqs(&mut self) -> io::Result<()> {
        let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                for i in 0..fd.num_terms {
                    writer.add(&mut *self.vectors_stream, (fd.freqs[i] - 1) as i64)?;
                }
            }
        }
        writer.finish(&mut *self.vectors_stream)
    }

    fn flush_positions(&mut self) -> io::Result<()> {
        let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                if fd.has_positions {
                    let mut pos = 0usize;
                    for i in 0..fd.num_terms {
                        let mut previous_position = 0;
                        for _ in 0..fd.freqs[i] as usize {
                            let position = self.positions_buf[fd.pos_start + pos];
                            writer.add(
                                &mut *self.vectors_stream,
                                (position - previous_position) as i64,
                            )?;
                            previous_position = position;
                            pos += 1;
                        }
                    }
                    assert_eq!(pos, fd.total_positions);
                }
            }
        }
        writer.finish(&mut *self.vectors_stream)
    }

    fn flush_offsets(&mut self, field_nums: &[u32]) -> io::Result<()> {
        let has_offsets = self
            .pending_docs
            .iter()
            .any(|doc| doc.fields.iter().any(|f| f.has_offsets));
        if !has_offsets {
            return Ok(());
        }

        // Compute charsPerTerm per unique field number
        let mut sum_pos = vec![0i64; field_nums.len()];
        let mut sum_offsets = vec![0i64; field_nums.len()];

        for doc in &self.pending_docs {
            for fd in &doc.fields {
                if fd.has_offsets && fd.has_positions {
                    let idx = field_nums
                        .binary_search(&fd.field_num)
                        .expect("field number must be in field_nums");
                    let mut pos = 0usize;
                    for i in 0..fd.num_terms {
                        let freq = fd.freqs[i] as usize;
                        if freq > 0 {
                            sum_pos[idx] += self.positions_buf
                                [fd.pos_start + fd.freqs[i] as usize - 1 + pos]
                                as i64;
                            sum_offsets[idx] += self.start_offsets_buf
                                [fd.off_start + fd.freqs[i] as usize - 1 + pos]
                                as i64;
                        }
                        pos += freq;
                    }
                    assert_eq!(pos, fd.total_positions);
                }
            }
        }

        let mut chars_per_term = vec![0.0f32; field_nums.len()];
        for i in 0..field_nums.len() {
            chars_per_term[i] = if sum_pos[i] <= 0 || sum_offsets[i] <= 0 {
                0.0
            } else {
                (sum_offsets[i] as f64 / sum_pos[i] as f64) as f32
            };
        }

        // Write charsPerTerm as LE ints
        for &cpt in &chars_per_term {
            self.vectors_stream.write_le_int(f32::to_bits(cpt) as i32)?;
        }

        // Start offset deltas
        let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                if (fd.flags & OFFSETS as i32) != 0 {
                    let idx = field_nums
                        .binary_search(&fd.field_num)
                        .expect("field number must be in field_nums");
                    let cpt = chars_per_term[idx];
                    let mut pos = 0usize;
                    for i in 0..fd.num_terms {
                        let mut previous_pos = 0i32;
                        let mut previous_off = 0i32;
                        for _ in 0..fd.freqs[i] as usize {
                            let position = if fd.has_positions {
                                self.positions_buf[fd.pos_start + pos]
                            } else {
                                0
                            };
                            let start_offset = self.start_offsets_buf[fd.off_start + pos];
                            let delta = start_offset
                                - previous_off
                                - (cpt * (position - previous_pos) as f32) as i32;
                            writer.add(&mut *self.vectors_stream, delta as i64)?;
                            previous_pos = position;
                            previous_off = start_offset;
                            pos += 1;
                        }
                    }
                }
            }
        }
        writer.finish(&mut *self.vectors_stream)?;

        // Offset lengths: length - prefixLength - suffixLength
        writer.reset();
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                if (fd.flags & OFFSETS as i32) != 0 {
                    let mut pos = 0usize;
                    for i in 0..fd.num_terms {
                        for _ in 0..fd.freqs[i] as usize {
                            let length = self.lengths_buf[fd.off_start + pos];
                            writer.add(
                                &mut *self.vectors_stream,
                                (length - fd.prefix_lengths[i] - fd.suffix_lengths[i]) as i64,
                            )?;
                            pos += 1;
                        }
                    }
                    assert_eq!(pos, fd.total_positions);
                }
            }
        }
        writer.finish(&mut *self.vectors_stream)
    }

    fn flush_payload_lengths(&mut self) -> io::Result<()> {
        let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
        for doc in &self.pending_docs {
            for fd in &doc.fields {
                if fd.has_payloads {
                    for i in 0..fd.total_positions {
                        writer.add(
                            &mut *self.vectors_stream,
                            self.payload_lengths_buf[fd.pay_start + i] as i64,
                        )?;
                    }
                }
            }
        }
        writer.finish(&mut *self.vectors_stream)
    }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

/// Grows a buffer size by at least 1/8 plus a small constant.
pub(crate) fn oversize(min_size: usize) -> usize {
    let extra = (min_size >> 3).max(3);
    min_size + extra
}

/// Returns the length of the shared prefix between two byte slices.
fn shared_prefix_length(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use assertables::*;

    use super::*;
    use crate::store::memory::MemoryDirectory;

    fn make_directory() -> SharedDirectory {
        Mutex::new(Box::new(MemoryDirectory::new()))
    }

    fn make_segment_id() -> [u8; 16] {
        [0u8; 16]
    }

    /// Creates a writer, calls `build_fn` to populate it, then finishes.
    fn write_with<F>(dir: &SharedDirectory, num_docs: i32, build_fn: F) -> Vec<String>
    where
        F: FnOnce(&mut CompressingTermVectorsWriter),
    {
        let tvd_name = index_file_names::segment_file_name("_0", "", VECTORS_EXTENSION);
        let tvd = {
            let mut d = dir.lock().unwrap();
            d.create_output(&tvd_name).unwrap()
        };
        let mut w = CompressingTermVectorsWriter::new(tvd, &make_segment_id(), "").unwrap();
        build_fn(&mut w);
        w.finish(dir, "_0", "", &make_segment_id(), num_docs)
            .unwrap()
    }

    #[test]
    fn test_empty_docs() {
        let dir = make_directory();
        let files = write_with(&dir, 0, |_| {});
        assert_len_eq_x!(&files, 3);
        assert!(files[0].ends_with(".tvd"));
        assert!(files[1].ends_with(".tvx"));
        assert!(files[2].ends_with(".tvm"));
    }

    #[test]
    fn test_single_doc_no_fields() {
        let dir = make_directory();
        let files = write_with(&dir, 1, |w| {
            w.start_document(0);
            w.finish_document().unwrap();
        });
        assert_len_eq_x!(&files, 3);
    }

    #[test]
    fn test_single_doc_single_field_single_term() {
        let dir = make_directory();
        let files = write_with(&dir, 1, |w| {
            w.start_document(1);
            w.start_field(0, 1, false, false, false);
            w.start_term(b"hello", 1);
            w.finish_term();
            w.finish_field();
            w.finish_document().unwrap();
        });
        assert_len_eq_x!(&files, 3);

        let dir_guard = dir.lock().unwrap();
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        assert_gt!(tvd_len, 40);
    }

    #[test]
    fn test_single_doc_multiple_terms() {
        let dir = make_directory();
        let files = write_with(&dir, 1, |w| {
            w.start_document(1);
            w.start_field(0, 2, false, false, false);
            w.start_term(b"bar", 1);
            w.finish_term();
            w.start_term(b"foo", 2);
            w.finish_term();
            w.finish_field();
            w.finish_document().unwrap();
        });
        assert_len_eq_x!(&files, 3);
    }

    #[test]
    fn test_multiple_docs_different_fields() {
        let dir = make_directory();
        let files = write_with(&dir, 2, |w| {
            w.start_document(1);
            w.start_field(0, 1, false, false, false);
            w.start_term(b"alpha", 1);
            w.finish_term();
            w.finish_field();
            w.finish_document().unwrap();

            w.start_document(1);
            w.start_field(2, 1, false, false, false);
            w.start_term(b"beta", 1);
            w.finish_term();
            w.finish_field();
            w.finish_document().unwrap();
        });
        assert_len_eq_x!(&files, 3);
    }

    #[test]
    fn test_term_prefix_compression() {
        let dir = make_directory();
        let files = write_with(&dir, 1, |w| {
            w.start_document(1);
            w.start_field(0, 3, false, false, false);
            w.start_term(b"abc", 1);
            w.finish_term();
            w.start_term(b"abd", 1);
            w.finish_term();
            w.start_term(b"xyz", 1);
            w.finish_term();
            w.finish_field();
            w.finish_document().unwrap();
        });
        assert_len_eq_x!(&files, 3);
    }

    #[test]
    fn test_shared_prefix_length() {
        assert_eq!(shared_prefix_length(b"abc", b"abd"), 2);
        assert_eq!(shared_prefix_length(b"abc", b"abc"), 3);
        assert_eq!(shared_prefix_length(b"abc", b"xyz"), 0);
        assert_eq!(shared_prefix_length(b"", b"abc"), 0);
        assert_eq!(shared_prefix_length(b"abc", b""), 0);
    }

    #[test]
    fn test_many_terms_no_positions() {
        let dir = make_directory();
        let files = write_with(&dir, 1, |w| {
            w.start_document(1);
            w.start_field(0, 20, false, false, false);
            for i in 0..20i32 {
                let term = format!("term_{i:04}");
                w.start_term(term.as_bytes(), 1);
                w.finish_term();
            }
            w.finish_field();
            w.finish_document().unwrap();
        });
        assert_len_eq_x!(&files, 3);

        let dir_guard = dir.lock().unwrap();
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        assert_gt!(tvd_len, 40, "tvd should have substantial content");
    }

    #[test]
    fn test_multi_chunk_by_doc_count() {
        let dir = make_directory();
        let num_docs = 200i32;
        let files = write_with(&dir, num_docs, |w| {
            for i in 0..num_docs {
                w.start_document(1);
                w.start_field(0, 1, false, false, false);
                let term = format!("t{i}");
                w.start_term(term.as_bytes(), 1);
                w.finish_term();
                w.finish_field();
                w.finish_document().unwrap();
            }
        });
        assert_len_eq_x!(&files, 3);

        let dir_guard = dir.lock().unwrap();
        let tvm_bytes = dir_guard.read_file(&files[2]).unwrap();
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        assert_gt!(
            tvd_len,
            60,
            "multi-chunk tvd should have substantial content"
        );
        assert_gt!(tvm_bytes.len(), 16);
    }

    #[test]
    fn test_multi_chunk_by_term_bytes() {
        let dir = make_directory();
        let num_docs = 10i32;
        let files = write_with(&dir, num_docs, |w| {
            for i in 0..num_docs {
                w.start_document(1);
                w.start_field(0, 1, false, false, false);
                let long_term = format!("term_{i:0>500}");
                w.start_term(long_term.as_bytes(), 1);
                w.finish_term();
                w.finish_field();
                w.finish_document().unwrap();
            }
        });
        assert_len_eq_x!(&files, 3);

        let dir_guard = dir.lock().unwrap();
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        assert_gt!(
            tvd_len,
            60,
            "multi-chunk tvd should have substantial content"
        );
    }

    // -- FieldData.add_term -------------------------------------------------

    #[test]
    fn test_field_data_add_term_records_freq_and_lengths() {
        let mut doc = DocData {
            num_fields: 0,
            fields: Vec::new(),
            pos_start: 0,
            off_start: 0,
            pay_start: 0,
        };
        doc.add_field(0, 3, true, false, false);
        let fd = doc.fields.last_mut().unwrap();

        fd.add_term(5, 0, 4);
        fd.add_term(2, 2, 3);
        fd.add_term(1, 4, 1);

        assert_eq!(fd.freqs, vec![5, 2, 1]);
        assert_eq!(fd.prefix_lengths, vec![0, 2, 4]);
        assert_eq!(fd.suffix_lengths, vec![4, 3, 1]);
        assert_eq!(fd.ord, 3);
    }

    // -- DocData.add_field offset calculations ------------------------------

    #[test]
    fn test_doc_data_add_field_first_field_inherits_doc_offsets() {
        let mut doc = DocData {
            num_fields: 0,
            fields: Vec::new(),
            pos_start: 10,
            off_start: 20,
            pay_start: 30,
        };
        doc.add_field(0, 1, true, true, true);
        let fd = &doc.fields[0];

        assert_eq!(fd.pos_start, 10);
        assert_eq!(fd.off_start, 20);
        assert_eq!(fd.pay_start, 30);
    }

    #[test]
    fn test_doc_data_add_field_second_field_offsets_from_first() {
        let mut doc = DocData {
            num_fields: 0,
            fields: Vec::new(),
            pos_start: 0,
            off_start: 0,
            pay_start: 0,
        };
        doc.add_field(0, 2, true, true, false);
        // Simulate 5 positions written to first field
        doc.fields[0].total_positions = 5;

        doc.add_field(1, 3, true, true, false);
        let fd2 = &doc.fields[1];

        assert_eq!(fd2.pos_start, 5);
        assert_eq!(fd2.off_start, 5);
        assert_eq!(fd2.pay_start, 0); // first field had no payloads
    }

    #[test]
    fn test_doc_data_add_field_skips_disabled_features() {
        let mut doc = DocData {
            num_fields: 0,
            fields: Vec::new(),
            pos_start: 0,
            off_start: 0,
            pay_start: 0,
        };
        // First field: positions only, 10 total_positions
        doc.add_field(0, 1, true, false, false);
        doc.fields[0].total_positions = 10;

        // Second field starts at pos_start=10, off_start=0 (offsets disabled on field 0)
        doc.add_field(1, 1, true, true, false);
        let fd2 = &doc.fields[1];

        assert_eq!(fd2.pos_start, 10);
        assert_eq!(fd2.off_start, 0);
    }

    #[test]
    fn test_doc_data_add_field_flags() {
        let mut doc = DocData {
            num_fields: 0,
            fields: Vec::new(),
            pos_start: 0,
            off_start: 0,
            pay_start: 0,
        };
        doc.add_field(0, 1, true, true, true);
        assert_eq!(doc.fields[0].flags, 0b111);

        doc.add_field(1, 1, true, false, false);
        assert_eq!(doc.fields[1].flags, 0b001);

        doc.add_field(2, 1, false, true, false);
        assert_eq!(doc.fields[2].flags, 0b010);

        doc.add_field(3, 1, false, false, false);
        assert_eq!(doc.fields[3].flags, 0b000);
    }

    // -- Buffer growth via oversize -----------------------------------------

    #[test]
    fn test_oversize_grows_by_at_least_one_eighth() {
        assert_ge!(oversize(100), 100 + 100 / 8);
        assert_ge!(oversize(1000), 1000 + 1000 / 8);
    }

    #[test]
    fn test_oversize_small_inputs_grow_by_at_least_3() {
        assert_ge!(oversize(1), 4);
        assert_ge!(oversize(0), 3);
    }

    #[test]
    fn test_multi_field_different_field_numbers() {
        let dir = make_directory();
        let files = write_with(&dir, 1, |w| {
            w.start_document(3);
            w.start_field(0, 1, false, false, false);
            w.start_term(b"alpha", 1);
            w.finish_term();
            w.finish_field();
            w.start_field(5, 1, false, false, false);
            w.start_term(b"beta", 1);
            w.finish_term();
            w.finish_field();
            w.start_field(10, 1, false, false, false);
            w.start_term(b"gamma", 1);
            w.finish_term();
            w.finish_field();
            w.finish_document().unwrap();
        });
        assert_len_eq_x!(&files, 3);

        let dir_guard = dir.lock().unwrap();
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        assert_gt!(tvd_len, 40);
    }

    // -- trigger_flush ------------------------------------------------------

    #[test]
    fn test_trigger_flush_by_suffix_size() {
        let dir = make_directory();
        let tvd_name = index_file_names::segment_file_name("_0", "", VECTORS_EXTENSION);
        let tvd = {
            let mut d = dir.lock().unwrap();
            d.create_output(&tvd_name).unwrap()
        };
        let mut w = CompressingTermVectorsWriter::new(tvd, &make_segment_id(), "").unwrap();

        assert!(!w.trigger_flush());

        // Fill term_suffixes beyond CHUNK_SIZE (4096)
        w.term_suffixes.resize(CHUNK_SIZE as usize, b'x');
        assert!(w.trigger_flush());
    }

    #[test]
    fn test_trigger_flush_by_doc_count() {
        let dir = make_directory();
        let tvd_name = index_file_names::segment_file_name("_0", "", VECTORS_EXTENSION);
        let tvd = {
            let mut d = dir.lock().unwrap();
            d.create_output(&tvd_name).unwrap()
        };
        let mut w = CompressingTermVectorsWriter::new(tvd, &make_segment_id(), "").unwrap();

        assert!(!w.trigger_flush());

        // Add MAX_DOCS_PER_CHUNK empty docs to pending
        for _ in 0..MAX_DOCS_PER_CHUNK {
            w.pending_docs.push(DocData {
                num_fields: 0,
                fields: Vec::new(),
                pos_start: 0,
                off_start: 0,
                pay_start: 0,
            });
        }
        assert!(w.trigger_flush());
    }

    // -- finish_document pushes cur_doc into pending_docs --------------------

    #[test]
    fn test_finish_document_moves_cur_doc_to_pending() {
        let dir = make_directory();
        let tvd_name = index_file_names::segment_file_name("_0", "", VECTORS_EXTENSION);
        let tvd = {
            let mut d = dir.lock().unwrap();
            d.create_output(&tvd_name).unwrap()
        };
        let mut w = CompressingTermVectorsWriter::new(tvd, &make_segment_id(), "").unwrap();

        assert!(w.pending_docs.is_empty());
        assert!(w.cur_doc.is_none());

        w.start_document(1);
        assert!(w.cur_doc.is_some());
        assert!(w.pending_docs.is_empty());

        w.start_field(0, 1, false, false, false);
        w.start_term(b"x", 1);
        w.finish_term();
        w.finish_field();
        w.finish_document().unwrap();

        assert!(w.cur_doc.is_none());
        assert_eq!(w.pending_docs.len(), 1);
        assert_eq!(w.pending_docs[0].num_fields, 1);
        assert_eq!(w.pending_docs[0].fields.len(), 1);
    }

    #[test]
    fn test_finish_document_increments_num_docs() {
        let dir = make_directory();
        let tvd_name = index_file_names::segment_file_name("_0", "", VECTORS_EXTENSION);
        let tvd = {
            let mut d = dir.lock().unwrap();
            d.create_output(&tvd_name).unwrap()
        };
        let mut w = CompressingTermVectorsWriter::new(tvd, &make_segment_id(), "").unwrap();

        assert_eq!(w.num_docs, 0);

        w.start_document(0);
        w.finish_document().unwrap();
        assert_eq!(w.num_docs, 1);

        w.start_document(0);
        w.finish_document().unwrap();
        assert_eq!(w.num_docs, 2);
    }

    // -- MemSize reports used, not capacity ---------------------------------

    #[test]
    fn test_mem_size_reports_used_not_capacity() {
        use mem_dbg::MemSize;

        let dir = make_directory();
        let tvd_name = index_file_names::segment_file_name("_0", "", VECTORS_EXTENSION);
        let tvd = {
            let mut d = dir.lock().unwrap();
            d.create_output(&tvd_name).unwrap()
        };
        let mut w = CompressingTermVectorsWriter::new(tvd, &make_segment_id(), "").unwrap();

        let mut refs = mem_dbg::HashMap::default();
        let empty_size = w.mem_size_rec(mem_dbg::SizeFlags::default(), &mut refs);

        // Write docs with enough terms to accumulate meaningful suffix data
        for i in 0..10 {
            w.start_document(1);
            w.start_field(0, 3, false, false, false);
            for j in 0..3 {
                let term = format!("term_{i}_{j}_padding");
                w.start_term(term.as_bytes(), 1);
                w.finish_term();
            }
            w.finish_field();
            w.finish_document().unwrap();
        }

        refs.clear();
        let with_data = w.mem_size_rec(mem_dbg::SizeFlags::default(), &mut refs);
        assert_gt!(with_data, empty_size);

        // After flush, used size should drop even though buffers retain capacity
        w.flush(true).unwrap();
        refs.clear();
        let after_flush = w.mem_size_rec(mem_dbg::SizeFlags::default(), &mut refs);
        assert_lt!(after_flush, with_data);

        // Buffer capacity is still there but not reported
        assert_gt!(w.positions_buf.capacity(), 0);
        assert_eq!(w.pos_buf_used, 0);
        assert_eq!(w.off_buf_used, 0);
    }

    // -- start_term prefix compression and suffix accumulation ---------------

    #[test]
    fn test_start_term_accumulates_suffixes() {
        let dir = make_directory();
        let tvd_name = index_file_names::segment_file_name("_0", "", VECTORS_EXTENSION);
        let tvd = {
            let mut d = dir.lock().unwrap();
            d.create_output(&tvd_name).unwrap()
        };
        let mut w = CompressingTermVectorsWriter::new(tvd, &make_segment_id(), "").unwrap();

        w.start_document(1);
        w.start_field(0, 3, false, false, false);

        w.start_term(b"abc", 1); // prefix=0, suffix="abc"
        w.start_term(b"abd", 1); // prefix=2 ("ab"), suffix="d"
        w.start_term(b"xyz", 1); // prefix=0, suffix="xyz"

        assert_eq!(&w.term_suffixes, b"abcdxyz");

        let fd = w.cur_field.as_ref().unwrap();
        assert_eq!(fd.prefix_lengths[0], 0);
        assert_eq!(fd.suffix_lengths[0], 3);
        assert_eq!(fd.prefix_lengths[1], 2);
        assert_eq!(fd.suffix_lengths[1], 1);
        assert_eq!(fd.prefix_lengths[2], 0);
        assert_eq!(fd.suffix_lengths[2], 3);
    }

    #[test]
    fn test_start_term_resets_prefix_per_field() {
        let dir = make_directory();
        let tvd_name = index_file_names::segment_file_name("_0", "", VECTORS_EXTENSION);
        let tvd = {
            let mut d = dir.lock().unwrap();
            d.create_output(&tvd_name).unwrap()
        };
        let mut w = CompressingTermVectorsWriter::new(tvd, &make_segment_id(), "").unwrap();

        w.start_document(2);
        // Field 0: term "abc"
        w.start_field(0, 1, false, false, false);
        w.start_term(b"abc", 1);
        w.finish_term();
        w.finish_field();

        // Field 1: term "abd" — should NOT share prefix with field 0's "abc"
        w.start_field(1, 1, false, false, false);
        w.start_term(b"abd", 1);

        let fd = w.cur_field.as_ref().unwrap();
        assert_eq!(fd.prefix_lengths[0], 0); // no prefix sharing across fields
        assert_eq!(fd.suffix_lengths[0], 3);
    }

    // -- add_doc_data offset calculations across documents -------------------

    // -- flush resets state but retains buffer capacity ----------------------

    #[test]
    fn test_flush_resets_pending_and_suffixes() {
        let dir = make_directory();
        let tvd_name = index_file_names::segment_file_name("_0", "", VECTORS_EXTENSION);
        let tvd = {
            let mut d = dir.lock().unwrap();
            d.create_output(&tvd_name).unwrap()
        };
        let mut w = CompressingTermVectorsWriter::new(tvd, &make_segment_id(), "").unwrap();

        w.start_document(1);
        w.start_field(0, 1, false, false, false);
        w.start_term(b"test", 1);
        w.finish_term();
        w.finish_field();
        w.finish_document().unwrap();

        assert!(!w.pending_docs.is_empty());
        assert!(!w.term_suffixes.is_empty());

        w.flush(true).unwrap();

        assert!(w.pending_docs.is_empty());
        assert!(w.term_suffixes.is_empty());
        assert_eq!(w.pos_buf_used, 0);
        assert_eq!(w.off_buf_used, 0);
        // Buffer capacity retained
        assert_ge!(w.positions_buf.len(), INITIAL_BUF_SIZE);
    }
}
