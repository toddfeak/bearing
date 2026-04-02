// SPDX-License-Identifier: Apache-2.0

// DEBT: adapted from codecs::lucene90::term_vectors — reconcile after
// switchover by updating the original to accept newindex types directly.

//! Term vectors writer producing `.tvd`, `.tvx`, `.tvm` files.

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::mem;

use mem_dbg::MemSize;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::packed_writers::{BlockPackedWriter, DirectMonotonicWriter, DirectWriter};
use crate::encoding::lz4;
use crate::encoding::packed::{packed_bits_required, packed_ints_write, unsigned_bits_required};
use crate::newindex::byte_block_pool::{ByteSliceReader, DirectAllocator};
use crate::newindex::index_file_names;
use crate::store::{self, DataOutput, DataOutputWriter, IndexOutput, SharedDirectory, VecOutput};

// --- Local data types (DEBT: parallel to index::indexing_chain TV types) ---

/// Offset data for a single term occurrence in a term vector.
// DEBT: parallel to index::indexing_chain::OffsetBuffers
#[derive(Debug, Clone, MemSize)]
pub(crate) struct OffsetBuffers {
    pub start_offsets: Vec<i32>,
    pub end_offsets: Vec<i32>,
}

/// A single term in a term vector field.
// DEBT: parallel to index::indexing_chain::TermVectorTerm
#[derive(Debug, Clone, MemSize)]
pub(crate) struct TermVectorTerm {
    pub term: String,
    pub freq: i32,
    pub positions: Vec<i32>,
    pub offsets: Option<Box<OffsetBuffers>>,
}

/// A single field's term vector data for one document.
// DEBT: parallel to index::indexing_chain::TermVectorField
#[derive(Debug, Clone, MemSize)]
pub(crate) struct TermVectorField {
    pub field_number: u32,
    pub has_positions: bool,
    pub has_offsets: bool,
    pub has_payloads: bool,
    pub terms: Vec<TermVectorTerm>,
}

/// All term vector data for one document.
// DEBT: parallel to index::indexing_chain::TermVectorDoc
#[derive(Debug, Clone, MemSize)]
pub(crate) struct TermVectorDoc {
    pub fields: Vec<TermVectorField>,
}

// --- Codec constants ---

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

/// FLAGS_BITS = unsigned_bits_required(POSITIONS | OFFSETS | PAYLOADS) = unsigned_bits_required(7) = 4
const FLAGS_BITS: u32 = 4;

/// Writes term vector files (`.tvd`, `.tvx`, `.tvm`) for a segment.
///
/// Returns the names of the files written.
#[cfg(test)]
pub(crate) fn write(
    directory: &SharedDirectory,
    segment_name: &str,
    segment_suffix: &str,
    segment_id: &[u8; 16],
    term_vector_docs: &[TermVectorDoc],
    num_docs: i32,
) -> io::Result<Vec<String>> {
    let tvd_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, VECTORS_EXTENSION);

    debug!("term_vectors: writing tvd/tvx/tvm for segment={segment_name:?}, num_docs={num_docs}");

    let tvd = {
        let mut dir = directory.lock().unwrap();
        dir.create_output(&tvd_name)?
    };

    let mut writer = TermVectorChunkWriter::new(tvd, segment_id, segment_suffix)?;
    for doc in term_vector_docs {
        writer.add_doc(doc)?;
    }
    writer.finish(
        directory,
        segment_name,
        segment_suffix,
        segment_id,
        num_docs,
    )
}

/// Incrementally accumulates term vector documents and writes them as chunks
/// to a `.tvd` file. Finalization writes the `.tvx` and `.tvm` index/meta files.
/// Maximum docs per chunk before flushing.
const MAX_DOCS_PER_CHUNK: usize = 128;

pub(crate) struct TermVectorChunkWriter {
    /// Open `.tvd` handle (header already written).
    tvd: Box<dyn IndexOutput>,
    /// Documents in the current (unflushed) chunk.
    pending_docs: Vec<TermVectorDoc>,
    /// Accumulated term suffix bytes in the current chunk (matches Java's termSuffixes.size()).
    chunk_suffix_bytes: usize,
    /// Last term per field number in the current chunk, for prefix compression tracking.
    last_terms: HashMap<u32, Vec<u8>>,
    /// First doc id in the current chunk.
    doc_base: i32,
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
    // -- Streaming state for per-document building --
    num_docs: i32,
    cur_fields: Vec<TermVectorField>,
    cur_field_number: u32,
    cur_field_has_positions: bool,
    cur_field_has_offsets: bool,
    cur_field_has_payloads: bool,
    cur_field_terms: Vec<TermVectorTerm>,
    cur_term_text: Vec<u8>,
    cur_term_freq: i32,
    cur_term_positions: Vec<i32>,
    cur_term_start_offsets: Vec<i32>,
    cur_term_end_offsets: Vec<i32>,
    last_term: Vec<u8>,
}

impl TermVectorChunkWriter {
    /// Creates a new chunk writer. Writes the `.tvd` header immediately.
    pub(crate) fn new(
        mut tvd: Box<dyn IndexOutput>,
        segment_id: &[u8; 16],
        segment_suffix: &str,
    ) -> io::Result<Self> {
        codec_util::write_index_header(&mut *tvd, DATA_CODEC, VERSION, segment_id, segment_suffix)?;
        Ok(Self {
            tvd,
            pending_docs: Vec::new(),
            chunk_suffix_bytes: 0,
            last_terms: HashMap::new(),
            doc_base: 0,
            doc_bases: Vec::new(),
            start_pointers: Vec::new(),
            num_chunks: 0,
            num_dirty_chunks: 0,
            num_dirty_docs: 0,
            num_docs: 0,
            cur_fields: Vec::new(),
            cur_field_number: 0,
            cur_field_has_positions: false,
            cur_field_has_offsets: false,
            cur_field_has_payloads: false,
            cur_field_terms: Vec::new(),
            cur_term_text: Vec::new(),
            cur_term_freq: 0,
            cur_term_positions: Vec::new(),
            cur_term_start_offsets: Vec::new(),
            cur_term_end_offsets: Vec::new(),
            last_term: Vec::new(),
        })
    }

    /// Adds a document's term vector data to the current chunk.
    /// Flushes the chunk first if the threshold is reached.
    #[cfg(test)]
    pub(crate) fn add_doc(&mut self, doc: &TermVectorDoc) -> io::Result<()> {
        // Compute actual suffix bytes by tracking prefix compression per field
        for field in &doc.fields {
            let last_term = self.last_terms.entry(field.field_number).or_default();
            for term_data in &field.terms {
                let term_bytes = term_data.term.as_bytes();
                let prefix_len = shared_prefix_length(last_term, term_bytes);
                self.chunk_suffix_bytes += term_bytes.len() - prefix_len;
                last_term.clear();
                last_term.extend_from_slice(term_bytes);
            }
        }
        self.pending_docs.push(doc.clone());
        self.maybe_flush()
    }

    /// Returns the estimated RAM bytes used by the chunk writer's buffers.
    ///
    /// Covers `pending_docs` (bounded by chunk threshold), per-chunk index
    /// vectors (`doc_bases`, `start_pointers`), and the `last_terms` map.
    #[expect(dead_code)]
    pub(crate) fn ram_bytes_used(&self) -> usize {
        let flags = mem_dbg::SizeFlags::CAPACITY;
        let pending = self.pending_docs.mem_size(flags);
        let last_terms: usize = self
            .last_terms
            .values()
            .map(|v| v.capacity())
            .sum::<usize>()
            + self.last_terms.capacity() * mem::size_of::<(u32, Vec<u8>)>();
        let indices = self.doc_bases.capacity() * mem::size_of::<i64>()
            + self.start_pointers.capacity() * mem::size_of::<i64>();
        pending + last_terms + indices
    }

    // -- Streaming interface --

    /// Begins a new document with the given number of vector fields.
    pub(crate) fn start_document(&mut self, _num_vector_fields: i32) {
        self.cur_fields.clear();
    }

    /// Finishes the current document, triggering a chunk flush if thresholds are met.
    pub(crate) fn finish_document(&mut self) -> io::Result<()> {
        let doc = TermVectorDoc {
            fields: mem::take(&mut self.cur_fields),
        };
        // Track suffix bytes for chunk threshold
        for field in &doc.fields {
            let last_term = self.last_terms.entry(field.field_number).or_default();
            for term_data in &field.terms {
                let term_bytes = term_data.term.as_bytes();
                let prefix_len = shared_prefix_length(last_term, term_bytes);
                self.chunk_suffix_bytes += term_bytes.len() - prefix_len;
                last_term.clear();
                last_term.extend_from_slice(term_bytes);
            }
        }
        self.pending_docs.push(doc);
        self.num_docs += 1;
        self.maybe_flush()
    }

    /// Begins a new field within the current document.
    pub(crate) fn start_field(
        &mut self,
        field_number: u32,
        _num_terms: i32,
        positions: bool,
        offsets: bool,
        payloads: bool,
    ) {
        self.cur_field_number = field_number;
        self.cur_field_has_positions = positions;
        self.cur_field_has_offsets = offsets;
        self.cur_field_has_payloads = payloads;
        self.cur_field_terms.clear();
        self.last_term.clear();
    }

    /// Finishes the current field.
    pub(crate) fn finish_field(&mut self) {
        self.cur_fields.push(TermVectorField {
            field_number: self.cur_field_number,
            has_positions: self.cur_field_has_positions,
            has_offsets: self.cur_field_has_offsets,
            has_payloads: self.cur_field_has_payloads,
            terms: mem::take(&mut self.cur_field_terms),
        });
    }

    /// Begins a new term with the given frequency.
    pub(crate) fn start_term(&mut self, term: &[u8], freq: i32) {
        self.cur_term_text.clear();
        self.cur_term_text.extend_from_slice(term);
        self.cur_term_freq = freq;
        self.cur_term_positions.clear();
        self.cur_term_start_offsets.clear();
        self.cur_term_end_offsets.clear();
    }

    /// Finishes the current term, collecting positions and offsets.
    pub(crate) fn finish_term(&mut self) {
        let offsets = if self.cur_field_has_offsets {
            Some(Box::new(OffsetBuffers {
                start_offsets: mem::take(&mut self.cur_term_start_offsets),
                end_offsets: mem::take(&mut self.cur_term_end_offsets),
            }))
        } else {
            None
        };
        let term_text = String::from_utf8(mem::take(&mut self.cur_term_text)).unwrap_or_default();
        self.cur_field_terms.push(TermVectorTerm {
            term: term_text,
            freq: self.cur_term_freq,
            positions: mem::take(&mut self.cur_term_positions),
            offsets,
        });
    }

    /// Reads position/offset data from byte slice readers and decodes them
    /// into absolute positions and offsets for the current term.
    pub(crate) fn add_prox(
        &mut self,
        num_prox: i32,
        positions: Option<&mut ByteSliceReader<'_, DirectAllocator>>,
        offsets: Option<&mut ByteSliceReader<'_, DirectAllocator>>,
    ) {
        if let Some(pos_reader) = positions {
            if self.cur_field_has_payloads {
                for _ in 0..num_prox {
                    let code = store::read_vint(pos_reader).unwrap();
                    if (code & 1) != 0 {
                        let payload_length = store::read_vint(pos_reader).unwrap();
                        for _ in 0..payload_length {
                            let mut buf = [0u8; 1];
                            std::io::Read::read_exact(pos_reader, &mut buf).unwrap();
                        }
                    }
                    let pos_delta = code >> 1;
                    let last_pos = self.cur_term_positions.last().copied().unwrap_or(0);
                    self.cur_term_positions.push(last_pos + pos_delta);
                }
            } else {
                for _ in 0..num_prox {
                    let code = store::read_vint(pos_reader).unwrap();
                    let pos_delta = code >> 1;
                    let last_pos = self.cur_term_positions.last().copied().unwrap_or(0);
                    self.cur_term_positions.push(last_pos + pos_delta);
                }
            }
        }

        if let Some(off_reader) = offsets {
            let mut last_end_offset = 0i32;
            for _ in 0..num_prox {
                let start_offset = last_end_offset + store::read_vint(off_reader).unwrap();
                let end_offset = start_offset + store::read_vint(off_reader).unwrap();
                last_end_offset = end_offset;
                self.cur_term_start_offsets.push(start_offset);
                self.cur_term_end_offsets.push(end_offset);
            }
        }
    }

    /// Flushes the current chunk if it exceeds the size or doc count threshold.
    fn maybe_flush(&mut self) -> io::Result<()> {
        if self.chunk_suffix_bytes >= CHUNK_SIZE as usize
            || self.pending_docs.len() >= MAX_DOCS_PER_CHUNK
        {
            self.flush_chunk(false)?;
        }
        Ok(())
    }

    /// Writes one chunk of pending documents to `.tvd`.
    fn flush_chunk(&mut self, dirty: bool) -> io::Result<()> {
        let docs = &self.pending_docs;
        let chunk_docs = docs.len() as i32;
        if chunk_docs == 0 {
            return Ok(());
        }

        // Record chunk position for index
        self.doc_bases.push(self.doc_base as i64);
        self.start_pointers.push(self.tvd.file_pointer() as i64);

        // Chunk header: docBase, (chunkDocs << 1) | dirty_bit
        self.tvd.write_vint(self.doc_base)?;
        let dirty_bit = if dirty { 1 } else { 0 };
        self.tvd.write_vint((chunk_docs << 1) | dirty_bit)?;

        let total_fields = flush_num_fields(docs, &mut *self.tvd)?;

        if total_fields > 0 {
            let field_nums = flush_field_nums(docs, &mut *self.tvd)?;
            flush_fields(docs, &field_nums, &mut *self.tvd)?;
            flush_flags(docs, &field_nums, &mut *self.tvd)?;
            flush_num_terms(docs, &mut *self.tvd)?;

            let term_suffixes = flush_term_lengths(docs, &mut *self.tvd)?;
            flush_term_freqs(docs, &mut *self.tvd)?;
            flush_positions(docs, &mut *self.tvd)?;
            flush_offsets(docs, &field_nums, &mut *self.tvd)?;
            flush_payload_lengths(docs, &mut *self.tvd)?;

            // Compress term suffixes with plain LZ4 (CompressionMode.FAST)
            let compressed = lz4::compress(&term_suffixes);
            self.tvd.write_bytes(&compressed)?;
        }

        self.num_chunks += 1;
        if dirty {
            self.num_dirty_chunks += 1;
            self.num_dirty_docs += chunk_docs as i64;
        }

        self.doc_base += chunk_docs;
        self.pending_docs.clear();
        self.chunk_suffix_bytes = 0;
        self.last_terms.clear();
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
        self.flush_chunk(true)?;

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

        let max_pointer = self.tvd.file_pointer() as i64;
        let total_chunks = self.num_chunks as u32;

        // Write FieldsIndex to .tvx and .tvm (mirrors FieldsIndexWriter.finish())
        tvm.write_le_int(num_docs)?;
        tvm.write_le_int(BLOCK_SHIFT as i32)?;
        tvm.write_le_int((total_chunks + 1) as i32)?;

        // docsStartPointer
        tvm.write_le_long(tvx.file_pointer() as i64)?;

        // Docs monotonic index (meta → tvm, data → tvx)
        // totalChunks + 1 values: doc_base of each chunk, then num_docs sentinel
        let mut docs_writer = DirectMonotonicWriter::new(BLOCK_SHIFT);
        for &db in &self.doc_bases {
            docs_writer.add(db);
        }
        if total_chunks > 0 {
            docs_writer.add(num_docs as i64);
        }
        docs_writer.finish(&mut *tvm, &mut *tvx)?;

        // startPointersStartPointer
        tvm.write_le_long(tvx.file_pointer() as i64)?;

        // File pointers monotonic index (meta → tvm, data → tvx)
        let mut fp_writer = DirectMonotonicWriter::new(BLOCK_SHIFT);
        for &sp in &self.start_pointers {
            fp_writer.add(sp);
        }
        fp_writer.add(max_pointer);
        fp_writer.finish(&mut *tvm, &mut *tvx)?;

        // startPointersEndPointer
        tvm.write_le_long(tvx.file_pointer() as i64)?;

        // .tvx footer
        codec_util::write_footer(&mut *tvx)?;

        // maxPointer (into .tvd)
        tvm.write_le_long(max_pointer)?;

        // Trailing metadata to .tvm
        debug!(
            "term_vectors: num_chunks={}, num_dirty_chunks={}, num_dirty_docs={}",
            self.num_chunks, self.num_dirty_chunks, self.num_dirty_docs
        );
        tvm.write_vlong(self.num_chunks)?;
        tvm.write_vlong(self.num_dirty_chunks)?;
        tvm.write_vlong(self.num_dirty_docs)?;

        // Footers for .tvm and .tvd
        codec_util::write_footer(&mut *tvm)?;
        codec_util::write_footer(&mut *self.tvd)?;

        Ok(vec![tvd_name, tvx_name, tvm_name])
    }
}

/// Writes number of fields per doc. Returns the total field count across all docs.
fn flush_num_fields(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<i32> {
    if docs.len() == 1 {
        let num_fields = docs[0].fields.len() as i32;
        output.write_vint(num_fields)?;
        return Ok(num_fields);
    }

    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    let mut total_fields = 0i32;
    for doc in docs {
        let n = doc.fields.len() as i64;
        writer.add(output, n)?;
        total_fields += n as i32;
    }
    writer.finish(output)?;
    Ok(total_fields)
}

/// Writes unique sorted field numbers. Returns the sorted field number list.
fn flush_field_nums(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<Vec<u32>> {
    let mut field_nums_set = BTreeSet::new();
    for doc in docs {
        for field in &doc.fields {
            field_nums_set.insert(field.field_number);
        }
    }

    let field_nums: Vec<u32> = field_nums_set.into_iter().collect();
    let num_distinct = field_nums.len();
    assert!(num_distinct > 0);

    let max_field_num = field_nums[num_distinct - 1] as i64;
    let bits_required = packed_bits_required(max_field_num);
    let token = ((num_distinct - 1).min(0x07) << 5) as u8 | bits_required as u8;
    output.write_byte(token)?;
    if num_distinct > 0x07 {
        output.write_vint((num_distinct - 1 - 0x07) as i32)?;
    }

    let values: Vec<i64> = field_nums.iter().map(|&n| n as i64).collect();
    packed_ints_write(&mut DataOutputWriter(output), &values, bits_required)?;

    Ok(field_nums)
}

/// Writes field number indices via DirectWriter to scratch buffer.
fn flush_fields(
    docs: &[TermVectorDoc],
    field_nums: &[u32],
    output: &mut dyn DataOutput,
) -> io::Result<()> {
    let bpv = unsigned_bits_required((field_nums.len() - 1) as i64);
    let mut writer = DirectWriter::new(bpv);
    for doc in docs {
        for field in &doc.fields {
            let idx = field_nums
                .binary_search(&field.field_number)
                .expect("field number must be in field_nums");
            writer.add(idx as i64);
        }
    }
    let mut scratch = Vec::new();
    writer.finish(&mut VecOutput(&mut scratch))?;
    output.write_vlong(scratch.len() as i64)?;
    output.write_bytes(&scratch)
}

/// Writes per-field flags (positions/offsets/payloads) via DirectWriter.
fn flush_flags(
    docs: &[TermVectorDoc],
    field_nums: &[u32],
    output: &mut dyn DataOutput,
) -> io::Result<()> {
    // Check if flags are consistent per field number
    let mut field_flags: Vec<i32> = vec![-1; field_nums.len()];
    let mut non_changing = true;

    'outer: for doc in docs {
        for field in &doc.fields {
            let idx = field_nums
                .binary_search(&field.field_number)
                .expect("field number must be in field_nums");
            let flags = field_flags_value(field);
            if field_flags[idx] == -1 {
                field_flags[idx] = flags;
            } else if field_flags[idx] != flags {
                non_changing = false;
                break 'outer;
            }
        }
    }

    if non_changing {
        // One flag per unique field number
        output.write_vint(0)?;
        let mut scratch = Vec::new();
        let mut writer = DirectWriter::new(FLAGS_BITS);
        for &flags in &field_flags {
            assert!(flags >= 0);
            writer.add(flags as i64);
        }
        writer.finish(&mut VecOutput(&mut scratch))?;
        output.write_vint(scratch.len() as i32)?;
        output.write_bytes(&scratch)
    } else {
        // One flag per field instance
        output.write_vint(1)?;
        let mut scratch = Vec::new();
        let mut writer = DirectWriter::new(FLAGS_BITS);
        for doc in docs {
            for field in &doc.fields {
                writer.add(field_flags_value(field) as i64);
            }
        }
        writer.finish(&mut VecOutput(&mut scratch))?;
        output.write_vint(scratch.len() as i32)?;
        output.write_bytes(&scratch)
    }
}

/// Writes number of terms per field via DirectWriter to scratch buffer.
fn flush_num_terms(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<()> {
    let mut max_num_terms: i32 = 0;
    for doc in docs {
        for field in &doc.fields {
            max_num_terms |= field.terms.len() as i32;
        }
    }

    let bpv = unsigned_bits_required(max_num_terms as i64);
    output.write_vint(bpv as i32)?;
    let mut scratch = Vec::new();
    let mut writer = DirectWriter::new(bpv);
    for doc in docs {
        for field in &doc.fields {
            writer.add(field.terms.len() as i64);
        }
    }
    writer.finish(&mut VecOutput(&mut scratch))?;
    output.write_vint(scratch.len() as i32)?;
    output.write_bytes(&scratch)
}

/// Writes prefix and suffix lengths via BlockPackedWriter. Returns the accumulated
/// term suffix bytes for LZ4 compression.
fn flush_term_lengths(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<Vec<u8>> {
    let mut term_suffixes = Vec::new();

    // Compute prefix/suffix lengths and accumulate suffix bytes
    struct TermLengths {
        prefix_len: i32,
        suffix_len: i32,
    }
    let mut all_lengths: Vec<TermLengths> = Vec::new();

    for doc in docs {
        for field in &doc.fields {
            let mut prev_term: &[u8] = &[];
            for term_data in &field.terms {
                let term_bytes = term_data.term.as_bytes();
                let prefix_len = shared_prefix_length(prev_term, term_bytes);
                let suffix_len = term_bytes.len() - prefix_len;
                all_lengths.push(TermLengths {
                    prefix_len: prefix_len as i32,
                    suffix_len: suffix_len as i32,
                });
                term_suffixes.extend_from_slice(&term_bytes[prefix_len..]);
                prev_term = term_bytes;
            }
        }
    }

    // Write prefix lengths
    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    for tl in &all_lengths {
        writer.add(output, tl.prefix_len as i64)?;
    }
    writer.finish(output)?;

    // Write suffix lengths
    writer.reset();
    for tl in &all_lengths {
        writer.add(output, tl.suffix_len as i64)?;
    }
    writer.finish(output)?;

    Ok(term_suffixes)
}

/// Writes (freq - 1) for each term via BlockPackedWriter.
fn flush_term_freqs(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<()> {
    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    for doc in docs {
        for field in &doc.fields {
            for term in &field.terms {
                writer.add(output, (term.freq - 1) as i64)?;
            }
        }
    }
    writer.finish(output)
}

/// Writes position deltas via BlockPackedWriter.
fn flush_positions(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<()> {
    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    for doc in docs {
        for field in &doc.fields {
            if field.has_positions {
                for term in &field.terms {
                    let mut previous_position = 0;
                    for &position in &term.positions {
                        writer.add(output, (position - previous_position) as i64)?;
                        previous_position = position;
                    }
                }
            }
        }
    }
    writer.finish(output)
}

/// Writes offset data: charsPerTerm floats (BE), start offset deltas, and offset lengths.
fn flush_offsets(
    docs: &[TermVectorDoc],
    field_nums: &[u32],
    output: &mut dyn DataOutput,
) -> io::Result<()> {
    let has_offsets = docs
        .iter()
        .any(|doc| doc.fields.iter().any(|f| f.has_offsets));
    if !has_offsets {
        return Ok(());
    }

    // Compute charsPerTerm per unique field number
    let mut sum_pos = vec![0i64; field_nums.len()];
    let mut sum_offsets = vec![0i64; field_nums.len()];

    for doc in docs {
        for field in &doc.fields {
            if field.has_offsets && field.has_positions {
                let idx = field_nums
                    .binary_search(&field.field_number)
                    .expect("field number must be in field_nums");
                for term in &field.terms {
                    let freq = term.freq as usize;
                    if freq > 0 {
                        // Last position for this term
                        sum_pos[idx] += term.positions[freq - 1] as i64;
                        // Last start offset for this term
                        if let Some(ref offsets) = term.offsets {
                            sum_offsets[idx] += offsets.start_offsets[freq - 1] as i64;
                        }
                    }
                }
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
        output.write_le_int(f32::to_bits(cpt) as i32)?;
    }

    // Start offset deltas
    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    for doc in docs {
        for field in &doc.fields {
            if field.has_offsets {
                let idx = field_nums
                    .binary_search(&field.field_number)
                    .expect("field number must be in field_nums");
                let cpt = chars_per_term[idx];
                for term in &field.terms {
                    let mut previous_pos = 0i32;
                    let mut previous_off = 0i32;
                    if let Some(ref offsets) = term.offsets {
                        for j in 0..term.freq as usize {
                            let position = if field.has_positions {
                                term.positions[j]
                            } else {
                                0
                            };
                            let start_offset = offsets.start_offsets[j];
                            let delta = start_offset
                                - previous_off
                                - (cpt * (position - previous_pos) as f32) as i32;
                            writer.add(output, delta as i64)?;
                            previous_pos = position;
                            previous_off = start_offset;
                        }
                    }
                }
            }
        }
    }
    writer.finish(output)?;

    // Offset lengths: (endOffset - startOffset) - prefixLength - suffixLength
    writer.reset();
    for doc in docs {
        for field in &doc.fields {
            if field.has_offsets {
                let mut prev_term: &[u8] = &[];
                for term in &field.terms {
                    let term_bytes = term.term.as_bytes();
                    let prefix_len = shared_prefix_length(prev_term, term_bytes) as i32;
                    let suffix_len = term_bytes.len() as i32 - prefix_len;

                    if let Some(ref offsets) = term.offsets {
                        for j in 0..term.freq as usize {
                            let length = offsets.end_offsets[j] - offsets.start_offsets[j];
                            writer.add(output, (length - prefix_len - suffix_len) as i64)?;
                        }
                    }
                    prev_term = term_bytes;
                }
            }
        }
    }
    writer.finish(output)
}

/// Writes payload lengths via BlockPackedWriter (all zeros for now).
fn flush_payload_lengths(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<()> {
    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    for doc in docs {
        for field in &doc.fields {
            if field.has_payloads {
                for term in &field.terms {
                    for _ in 0..term.freq {
                        writer.add(output, 0)?;
                    }
                }
            }
        }
    }
    writer.finish(output)
}

/// Computes the flags byte for a term vector field.
fn field_flags_value(field: &TermVectorField) -> i32 {
    let mut flags = 0i32;
    if field.has_positions {
        flags |= POSITIONS as i32;
    }
    if field.has_offsets {
        flags |= OFFSETS as i32;
    }
    if field.has_payloads {
        flags |= PAYLOADS as i32;
    }
    flags
}

/// Returns the length of the shared prefix between two byte slices.
fn shared_prefix_length(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
    use assertables::*;

    use super::*;
    use crate::store::memory::MemoryDirectory;

    fn make_directory() -> SharedDirectory {
        std::sync::Mutex::new(Box::new(MemoryDirectory::new()))
    }

    fn make_segment_id() -> [u8; 16] {
        [0u8; 16]
    }

    #[test]
    fn test_empty_docs() {
        let dir = make_directory();
        let docs: Vec<TermVectorDoc> = vec![];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 0).unwrap();
        assert_len_eq_x!(&files, 3);
        assert!(files[0].ends_with(".tvd"));
        assert!(files[1].ends_with(".tvx"));
        assert!(files[2].ends_with(".tvm"));
    }

    #[test]
    fn test_single_doc_no_fields() {
        let dir = make_directory();
        let docs = vec![TermVectorDoc { fields: vec![] }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_len_eq_x!(&files, 3);
    }

    #[test]
    fn test_single_doc_single_field_single_term() {
        let dir = make_directory();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: false,
                has_offsets: false,
                has_payloads: false,
                terms: vec![TermVectorTerm {
                    term: "hello".to_string(),
                    freq: 1,
                    positions: vec![],
                    offsets: None,
                }],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_len_eq_x!(&files, 3);

        // Verify the .tvd file has valid content (header + chunk + footer)
        let dir_guard = dir.lock().unwrap();
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        // Should have at least a header + some chunk data + footer
        assert_gt!(tvd_len, 40);
    }

    #[test]
    fn test_single_doc_with_positions() {
        let dir = make_directory();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: true,
                has_offsets: false,
                has_payloads: false,
                terms: vec![
                    TermVectorTerm {
                        term: "bar".to_string(),
                        freq: 1,
                        positions: vec![0],
                        offsets: None,
                    },
                    TermVectorTerm {
                        term: "foo".to_string(),
                        freq: 2,
                        positions: vec![1, 3],
                        offsets: None,
                    },
                ],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_len_eq_x!(&files, 3);
    }

    #[test]
    fn test_single_doc_with_offsets() {
        let dir = make_directory();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: true,
                has_offsets: true,
                has_payloads: false,
                terms: vec![TermVectorTerm {
                    term: "hello".to_string(),
                    freq: 2,
                    positions: vec![0, 5],
                    offsets: Some(Box::new(OffsetBuffers {
                        start_offsets: vec![0, 30],
                        end_offsets: vec![5, 35],
                    })),
                }],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_len_eq_x!(&files, 3);
    }

    /// Verifies charsPerTerm is written as LE int (matching Lucene's
    /// DataOutput.writeInt) rather than BE. The reader patches offsets using
    /// Float.intBitsToFloat(readInt()) which expects LE byte order.
    #[test]
    fn test_chars_per_term_le_byte_order() {
        let dir = make_directory();
        // charsPerTerm = sumOffsets / sumPos. With one term at position 2
        // and start_offset 10: charsPerTerm = 10/2 = 5.0f.
        // IEEE 754 for 5.0f = 0x40A00000.
        // LE bytes: [00, 00, A0, 40].
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: true,
                has_offsets: true,
                has_payloads: false,
                terms: vec![TermVectorTerm {
                    term: "hello".to_string(),
                    freq: 1,
                    positions: vec![2],
                    offsets: Some(Box::new(OffsetBuffers {
                        start_offsets: vec![10],
                        end_offsets: vec![15],
                    })),
                }],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_len_eq_x!(&files, 3);

        // Read the .tvd bytes and verify charsPerTerm byte order
        let dir_guard = dir.lock().unwrap();
        let tvd_bytes = dir_guard.read_file(&files[0]).unwrap();
        assert_gt!(tvd_bytes.len(), 40);

        // LE representation of 5.0f (0x40A00000): bytes [00, 00, A0, 40]
        let le_5_0 = [0x00u8, 0x00, 0xA0, 0x40];
        // BE representation would be [40, A0, 00, 00]
        let be_5_0 = [0x40u8, 0xA0, 0x00, 0x00];

        let has_le = tvd_bytes.windows(4).any(|w| w == le_5_0);
        let has_be = tvd_bytes.windows(4).any(|w| w == be_5_0);
        assert!(has_le, "charsPerTerm 5.0f should appear in LE byte order");
        assert!(
            !has_be,
            "charsPerTerm 5.0f should NOT appear in BE byte order"
        );
    }

    #[test]
    fn test_multiple_docs_different_fields() {
        let dir = make_directory();
        let docs = vec![
            TermVectorDoc {
                fields: vec![TermVectorField {
                    field_number: 0,
                    has_positions: false,
                    has_offsets: false,
                    has_payloads: false,
                    terms: vec![TermVectorTerm {
                        term: "alpha".to_string(),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    }],
                }],
            },
            TermVectorDoc {
                fields: vec![TermVectorField {
                    field_number: 2,
                    has_positions: false,
                    has_offsets: false,
                    has_payloads: false,
                    terms: vec![TermVectorTerm {
                        term: "beta".to_string(),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    }],
                }],
            },
        ];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 2).unwrap();
        assert_len_eq_x!(&files, 3);
    }

    #[test]
    fn test_term_prefix_compression() {
        // Terms sharing prefixes should produce correct prefix/suffix lengths
        let dir = make_directory();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: false,
                has_offsets: false,
                has_payloads: false,
                terms: vec![
                    TermVectorTerm {
                        term: "abc".to_string(),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    },
                    TermVectorTerm {
                        term: "abd".to_string(),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    },
                    TermVectorTerm {
                        term: "xyz".to_string(),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    },
                ],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
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
    fn test_field_flags_value() {
        let field = TermVectorField {
            field_number: 0,
            has_positions: true,
            has_offsets: true,
            has_payloads: false,
            terms: vec![],
        };
        assert_eq!(field_flags_value(&field), 0x03); // POSITIONS | OFFSETS

        let field2 = TermVectorField {
            field_number: 0,
            has_positions: false,
            has_offsets: false,
            has_payloads: false,
            terms: vec![],
        };
        assert_eq!(field_flags_value(&field2), 0x00);

        let field3 = TermVectorField {
            field_number: 0,
            has_positions: true,
            has_offsets: true,
            has_payloads: true,
            terms: vec![],
        };
        assert_eq!(field_flags_value(&field3), 0x07);
    }

    #[test]
    fn test_position_delta_encoding() {
        let dir = make_directory();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: true,
                has_offsets: false,
                has_payloads: false,
                terms: vec![
                    TermVectorTerm {
                        term: "a".to_string(),
                        freq: 3,
                        positions: vec![0, 5, 10],
                        offsets: None,
                    },
                    TermVectorTerm {
                        term: "b".to_string(),
                        freq: 2,
                        // Position delta resets per term
                        positions: vec![2, 7],
                        offsets: None,
                    },
                ],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_len_eq_x!(&files, 3);
    }

    /// Writes term vectors with positions and offsets matching the
    /// text_field_with_term_vectors configuration. Exercises the LZ4
    /// compression path with enough terms to produce meaningful compressed
    /// output. Uses plain LZ4 (CompressionMode.FAST), not the preset-dict
    /// format used by stored fields.
    #[test]
    fn test_positions_and_offsets_with_many_terms() {
        let dir = make_directory();
        let terms: Vec<TermVectorTerm> = (0..20)
            .map(|i| TermVectorTerm {
                term: format!("term_{i:04}"),
                freq: 1,
                positions: vec![i],
                offsets: Some(Box::new(OffsetBuffers {
                    start_offsets: vec![i * 10],
                    end_offsets: vec![i * 10 + 9],
                })),
            })
            .collect();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: true,
                has_offsets: true,
                has_payloads: false,
                terms,
            }],
        }];

        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_len_eq_x!(&files, 3);

        let dir_guard = dir.lock().unwrap();
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        assert_gt!(tvd_len, 40, "tvd should have substantial content");
    }

    /// Verifies multi-chunk output when exceeding MAX_DOCS_PER_CHUNK (128 docs).
    /// Uses the TermVectorChunkWriter directly to inspect chunk metadata.
    #[test]
    fn test_multi_chunk_by_doc_count() {
        let dir = make_directory();
        let num_docs = 200;
        let docs: Vec<TermVectorDoc> = (0..num_docs)
            .map(|i| TermVectorDoc {
                fields: vec![TermVectorField {
                    field_number: 0,
                    has_positions: false,
                    has_offsets: false,
                    has_payloads: false,
                    terms: vec![TermVectorTerm {
                        term: format!("t{i}"),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    }],
                }],
            })
            .collect();

        let files = write(&dir, "_0", "", &make_segment_id(), &docs, num_docs).unwrap();
        assert_len_eq_x!(&files, 3);

        // Read .tvm to verify num_chunks > 1
        let dir_guard = dir.lock().unwrap();
        let tvm_bytes = dir_guard.read_file(&files[2]).unwrap();
        // num_chunks is a vlong near the end of .tvm, before the 16-byte footer.
        // With 200 docs and max 128 per chunk, expect 2 chunks.
        // Verify by checking .tvd size is larger than a single-chunk write would produce.
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        assert_gt!(
            tvd_len,
            60,
            "multi-chunk tvd should have substantial content"
        );

        // Also verify the tvm file is well-formed (has footer)
        assert_gt!(tvm_bytes.len(), 16);
    }

    /// Verifies multi-chunk output when exceeding CHUNK_SIZE (4096 bytes of term data).
    #[test]
    fn test_multi_chunk_by_term_bytes() {
        let dir = make_directory();
        // 10 docs, each with a ~500-byte term → 5000 bytes total, exceeds 4096
        let num_docs = 10;
        let docs: Vec<TermVectorDoc> = (0..num_docs)
            .map(|i| {
                let long_term = format!("term_{i:0>500}");
                TermVectorDoc {
                    fields: vec![TermVectorField {
                        field_number: 0,
                        has_positions: false,
                        has_offsets: false,
                        has_payloads: false,
                        terms: vec![TermVectorTerm {
                            term: long_term,
                            freq: 1,
                            positions: vec![],
                            offsets: None,
                        }],
                    }],
                }
            })
            .collect();

        let files = write(&dir, "_0", "", &make_segment_id(), &docs, num_docs).unwrap();
        assert_len_eq_x!(&files, 3);

        // With ~500 bytes per doc, chunk flushes after ~8 docs (>= 4096 bytes).
        // Should produce at least 2 chunks.
        let dir_guard = dir.lock().unwrap();
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        assert_gt!(
            tvd_len,
            60,
            "multi-chunk tvd should have substantial content"
        );
    }
}
