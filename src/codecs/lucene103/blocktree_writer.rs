// SPDX-License-Identifier: Apache-2.0
//! Block tree terms dictionary writer that organizes terms into a trie of blocks.

use std::collections::HashSet;
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::competitive_impact::NormsLookup;
use crate::document::IndexOptions;
use crate::index::index_file_names::segment_file_name;
use crate::index::indexing_chain::PostingList;
use crate::index::{FieldInfo, FieldInfos};
use crate::store::{DataOutput, IndexOutput, SharedDirectory, VecOutput};
use crate::util::BytesRef;
use crate::util::compress::{lowercase_ascii, lz4};

use super::postings_format::{
    self, BLOCKTREE_VERSION_CURRENT, DEFAULT_MAX_BLOCK_SIZE, DEFAULT_MIN_BLOCK_SIZE,
    IntBlockTermState, TERMS_CODEC, TERMS_CODEC_NAME, TERMS_EXTENSION, TERMS_INDEX_CODEC_NAME,
    TERMS_INDEX_EXTENSION, TERMS_META_CODEC_NAME, TERMS_META_EXTENSION, VERSION_CURRENT,
};
use super::postings_writer::PostingsWriter;

// ============================================================================
// Block writing helper structs
// ============================================================================

/// Describes a single block to write to the .tim file.
/// Groups the 6 block-descriptor parameters that are always passed together.
struct BlockSpec {
    start: usize,
    end: usize,
    is_floor: bool,
    floor_lead_label: i32,
    has_terms: bool,
    has_sub_blocks: bool,
}

/// Groups field-level context needed when writing blocks to output.
struct FieldWriteContext<'a> {
    field_info: &'a FieldInfo,
    write_freqs: bool,
    write_positions: bool,
}

// ============================================================================
// BlockTreeTermsWriter
// ============================================================================

/// Writes .tim, .tip, .tmd files plus delegates to PostingsWriter for .doc/.pos/.psm.
pub struct BlockTreeTermsWriter {
    terms_out: Box<dyn IndexOutput>,
    index_out: Box<dyn IndexOutput>,
    meta_out: Box<dyn IndexOutput>,
    postings_writer: PostingsWriter,
    min_items_in_block: usize,
    max_items_in_block: usize,
    field_metas: Vec<Vec<u8>>,
}

impl BlockTreeTermsWriter {
    /// Creates a new BlockTreeTermsWriter that streams to outputs from the directory.
    pub fn new(
        directory: &SharedDirectory,
        segment: &str,
        suffix: &str,
        id: &[u8; 16],
        field_infos: &FieldInfos,
    ) -> io::Result<Self> {
        // Create outputs from directory
        let tim_name = segment_file_name(segment, suffix, TERMS_EXTENSION);
        let tip_name = segment_file_name(segment, suffix, TERMS_INDEX_EXTENSION);
        let tmd_name = segment_file_name(segment, suffix, TERMS_META_EXTENSION);

        let (mut terms_out, mut index_out, mut meta_out) = {
            let mut dir = directory.lock().unwrap();
            (
                dir.create_output(&tim_name)?,
                dir.create_output(&tip_name)?,
                dir.create_output(&tmd_name)?,
            )
        };

        codec_util::write_index_header(
            &mut *terms_out,
            TERMS_CODEC_NAME,
            BLOCKTREE_VERSION_CURRENT,
            id,
            suffix,
        )?;
        codec_util::write_index_header(
            &mut *index_out,
            TERMS_INDEX_CODEC_NAME,
            BLOCKTREE_VERSION_CURRENT,
            id,
            suffix,
        )?;
        codec_util::write_index_header(
            &mut *meta_out,
            TERMS_META_CODEC_NAME,
            BLOCKTREE_VERSION_CURRENT,
            id,
            suffix,
        )?;

        // Create PostingsWriter (which creates .doc, .pos, .psm)
        let has_positions = field_infos.has_prox();
        let postings_writer = PostingsWriter::new(directory, segment, suffix, id, has_positions)?;

        // Write postings header into .tmd
        // In Java: postingsWriter.init(metaOut, state) writes TERMS_CODEC header + BLOCK_SIZE
        // to the metaOut (.tmd), not termsOut (.tim).
        codec_util::write_index_header(&mut *meta_out, TERMS_CODEC, VERSION_CURRENT, id, suffix)?;
        meta_out.write_vint(postings_format::BLOCK_SIZE as i32)?;

        Ok(Self {
            terms_out,
            index_out,
            meta_out,
            postings_writer,
            min_items_in_block: DEFAULT_MIN_BLOCK_SIZE,
            max_items_in_block: DEFAULT_MAX_BLOCK_SIZE,
            field_metas: Vec::new(),
        })
    }

    /// Write all terms for one indexed field.
    /// Terms must be pre-sorted by byte order.
    pub fn write_field(
        &mut self,
        field_info: &FieldInfo,
        sorted_terms: &[(&str, &PostingList)],
        norms: &NormsLookup,
    ) -> io::Result<()> {
        if sorted_terms.is_empty() {
            return Ok(());
        }

        debug!(
            "blocktree_writer: write_field name={} num_terms={}",
            field_info.name(),
            sorted_terms.len()
        );

        let write_freqs = field_info.index_options().has_freqs();
        let write_positions = field_info.index_options().has_positions();

        let mut tw = TermsWriter::new(
            field_info,
            write_freqs,
            write_positions,
            self.min_items_in_block,
            self.max_items_in_block,
        );

        // Compute doc_count and process terms in a single pass (no double decode)
        let mut docs_seen = HashSet::new();

        for (term_str, posting_list) in sorted_terms {
            let decoded = posting_list.decode();

            // Accumulate unique doc IDs for doc_count
            for p in &decoded {
                docs_seen.insert(p.doc_id);
            }

            let postings_data: Vec<(i32, i32, &[i32])> = decoded
                .iter()
                .map(|p| (p.doc_id, p.freq, p.positions.as_slice()))
                .collect();

            let state = self.postings_writer.write_term(
                &postings_data,
                field_info.index_options(),
                norms,
            )?;

            let term = BytesRef::from_utf8(term_str);
            tw.add_term(
                &term,
                state,
                &mut *self.terms_out,
                &self.postings_writer,
                field_info,
            )?;
        }

        tw.doc_count = docs_seen.len() as i32;

        tw.finish(
            &mut *self.terms_out,
            &mut *self.index_out,
            &self.postings_writer,
            field_info,
            &mut self.field_metas,
        )?;

        Ok(())
    }

    /// Finalize: write footers, drop outputs (auto-persists to directory).
    /// Returns the file names written.
    pub fn finish(mut self) -> io::Result<Vec<String>> {
        let mut all_names = Vec::new();

        // Write .tmd field count + field metas
        self.meta_out.write_vint(self.field_metas.len() as i32)?;
        for field_meta in &self.field_metas {
            self.meta_out.write_bytes(field_meta)?;
        }

        // Write .tip footer
        codec_util::write_footer(&mut *self.index_out)?;
        // Write .tip end pointer to .tmd
        self.meta_out
            .write_le_long(self.index_out.file_pointer() as i64)?;

        // Write .tim footer
        codec_util::write_footer(&mut *self.terms_out)?;
        // Write .tim end pointer to .tmd
        self.meta_out
            .write_le_long(self.terms_out.file_pointer() as i64)?;

        // Write .tmd footer
        codec_util::write_footer(&mut *self.meta_out)?;

        // Collect file names
        all_names.push(self.terms_out.name().to_string());
        all_names.push(self.index_out.name().to_string());
        all_names.push(self.meta_out.name().to_string());

        // Get postings file names
        let postings_names = self.postings_writer.finish()?;
        all_names.extend(postings_names);

        // Dropping self auto-persists all outputs to directory
        Ok(all_names)
    }
}

// ============================================================================
// PendingEntry / TermsWriter
// ============================================================================

enum PendingEntry {
    Term(PendingTerm),
    Block(PendingBlock),
}

impl PendingEntry {
    fn is_term(&self) -> bool {
        matches!(self, PendingEntry::Term(_))
    }
}

struct PendingTerm {
    term_bytes: Vec<u8>,
    state: IntBlockTermState,
}

struct PendingBlock {
    prefix: Vec<u8>,
    fp: u64,
    has_terms: bool,
    is_floor: bool,
    floor_lead_byte: i32,
    index: Option<TrieBuilder>,
    sub_indices: Option<Vec<TrieBuilder>>,
}

/// Accumulates terms for a single field and builds blocks.
struct TermsWriter {
    write_freqs: bool,
    write_positions: bool,
    min_items_in_block: usize,
    max_items_in_block: usize,
    pending: Vec<PendingEntry>,
    last_term: Vec<u8>,
    prefix_starts: Vec<usize>,
    num_terms: u64,
    sum_total_term_freq: i64,
    sum_doc_freq: i64,
    doc_count: i32,
    first_term_bytes: Vec<u8>,
    last_term_bytes: Vec<u8>,
    lz4_ht: lz4::HighCompressionHashTable,
}

impl TermsWriter {
    fn new(
        _field_info: &FieldInfo,
        write_freqs: bool,
        write_positions: bool,
        min_items_in_block: usize,
        max_items_in_block: usize,
    ) -> Self {
        Self {
            write_freqs,
            write_positions,
            min_items_in_block,
            max_items_in_block,
            pending: Vec::new(),
            last_term: Vec::new(),
            prefix_starts: Vec::new(),
            num_terms: 0,
            sum_total_term_freq: 0,
            sum_doc_freq: 0,
            doc_count: 0,
            lz4_ht: lz4::HighCompressionHashTable::new(),
            first_term_bytes: Vec::new(),
            last_term_bytes: Vec::new(),
        }
    }

    fn add_term(
        &mut self,
        term: &BytesRef,
        state: IntBlockTermState,
        terms_out: &mut dyn IndexOutput,
        postings_writer: &PostingsWriter,
        field_info: &FieldInfo,
    ) -> io::Result<()> {
        self.sum_doc_freq += state.doc_freq as i64;
        if self.write_freqs {
            self.sum_total_term_freq += state.total_term_freq;
        }
        self.num_terms += 1;

        if self.num_terms == 1 {
            self.first_term_bytes.clear();
            self.first_term_bytes.extend_from_slice(&term.bytes);
        }
        self.last_term_bytes.clear();
        self.last_term_bytes.extend_from_slice(&term.bytes);

        let term_bytes = term.bytes.clone();
        self.push_term(&term_bytes, terms_out, postings_writer, field_info)?;

        self.pending
            .push(PendingEntry::Term(PendingTerm { term_bytes, state }));

        Ok(())
    }

    /// Push a new term, writing blocks as needed when prefix groups get large enough.
    fn push_term(
        &mut self,
        text: &[u8],
        terms_out: &mut dyn IndexOutput,
        postings_writer: &PostingsWriter,
        field_info: &FieldInfo,
    ) -> io::Result<()> {
        let prefix_length = mismatch(&self.last_term, text);

        // Close abandoned suffixes
        for i in (prefix_length..self.last_term.len()).rev() {
            if self.pending.len() < self.prefix_starts[i] {
                continue;
            }
            let prefix_top_size = self.pending.len() - self.prefix_starts[i];
            if prefix_top_size >= self.min_items_in_block {
                self.write_blocks(
                    i + 1,
                    prefix_top_size,
                    terms_out,
                    postings_writer,
                    field_info,
                )?;
                self.prefix_starts[i] = self.prefix_starts[i].saturating_sub(prefix_top_size - 1);
            }
        }

        // Grow prefix_starts if needed
        if self.prefix_starts.len() < text.len() {
            self.prefix_starts.resize(text.len(), 0);
        }

        // Init new tail
        for i in prefix_length..text.len() {
            self.prefix_starts[i] = self.pending.len();
        }

        self.last_term = text.to_vec();

        Ok(())
    }

    /// Write the top `count` entries from pending as blocks.
    /// This writes block data directly to `terms_out`, matching Java's BlockTreeTermsWriter.writeBlocks.
    fn write_blocks(
        &mut self,
        prefix_length: usize,
        count: usize,
        terms_out: &mut dyn IndexOutput,
        postings_writer: &PostingsWriter,
        field_info: &FieldInfo,
    ) -> io::Result<()> {
        let start = self.pending.len() - count;
        let end = self.pending.len();

        let mut last_suffix_lead_label: i32 = -1;
        let mut has_terms = false;
        let mut has_sub_blocks = false;
        let mut next_block_start = start;
        let mut next_floor_lead_label: i32 = -1;

        let mut block_specs: Vec<BlockSpec> = Vec::new();

        for i in start..end {
            let suffix_lead_label = match &self.pending[i] {
                PendingEntry::Term(t) => {
                    if t.term_bytes.len() == prefix_length {
                        -1
                    } else {
                        t.term_bytes[prefix_length] as i32
                    }
                }
                PendingEntry::Block(b) => {
                    if b.prefix.len() > prefix_length {
                        b.prefix[prefix_length] as i32
                    } else {
                        -1
                    }
                }
            };

            if suffix_lead_label != last_suffix_lead_label {
                let items_in_block = i - next_block_start;
                if items_in_block >= self.min_items_in_block
                    && end - next_block_start > self.max_items_in_block
                {
                    let is_floor = items_in_block < count;
                    block_specs.push(BlockSpec {
                        start: next_block_start,
                        end: i,
                        is_floor,
                        floor_lead_label: next_floor_lead_label,
                        has_terms,
                        has_sub_blocks,
                    });
                    has_terms = false;
                    has_sub_blocks = false;
                    next_floor_lead_label = suffix_lead_label;
                    next_block_start = i;
                }
                last_suffix_lead_label = suffix_lead_label;
            }

            if self.pending[i].is_term() {
                has_terms = true;
            } else {
                has_sub_blocks = true;
            }
        }

        if next_block_start < end {
            let items_in_block = end - next_block_start;
            let is_floor = items_in_block < count;
            block_specs.push(BlockSpec {
                start: next_block_start,
                end,
                is_floor,
                floor_lead_label: next_floor_lead_label,
                has_terms,
                has_sub_blocks,
            });
        }

        // Write each block to terms_out
        let field_ctx = FieldWriteContext {
            field_info,
            write_freqs: self.write_freqs,
            write_positions: self.write_positions,
        };
        let mut new_blocks: Vec<PendingBlock> = Vec::new();

        for spec in &block_specs {
            let block = write_block_to_output(
                &self.pending,
                &self.last_term,
                prefix_length,
                spec,
                terms_out,
                postings_writer,
                &field_ctx,
                &mut self.lz4_ht,
            )?;
            new_blocks.push(block);
        }

        // Compile index for all blocks
        if !new_blocks.is_empty() {
            let is_floor = new_blocks[0].is_floor;

            let mut floor_data_bytes: Option<Vec<u8>> = None;
            if is_floor && new_blocks.len() > 1 {
                let mut fd = Vec::new();
                {
                    let mut buf = VecOutput(&mut fd);
                    buf.write_vint((new_blocks.len() - 1) as i32)?;
                    let base_fp = new_blocks[0].fp;
                    for block in new_blocks.iter().skip(1) {
                        buf.write_byte(block.floor_lead_byte as u8)?;
                        let delta = block.fp - base_fp;
                        let encoded = (delta << 1) | if block.has_terms { 1 } else { 0 };
                        buf.write_vlong(encoded as i64)?;
                    }
                }
                floor_data_bytes = Some(fd);
            }

            let floor_data = floor_data_bytes
                .as_ref()
                .map(|fd| BytesRef::new(fd.clone()));

            let output = TrieOutput {
                fp: new_blocks[0].fp as i64,
                has_terms: new_blocks[0].has_terms,
                floor_data,
            };

            let prefix_bytes = new_blocks[0].prefix.clone();
            let mut trie_builder =
                TrieBuilder::from_bytes_ref(&BytesRef::new(prefix_bytes), output);

            for block in new_blocks.iter_mut() {
                if let Some(sub_indices) = block.sub_indices.take() {
                    for sub_index in sub_indices {
                        trie_builder.append(sub_index);
                    }
                }
            }

            new_blocks[0].index = Some(trie_builder);
        }

        // Replace pending entries
        let pending_len = self.pending.len();
        self.pending.truncate(pending_len - count);

        if !new_blocks.is_empty() {
            let first_block = new_blocks.remove(0);
            self.pending.push(PendingEntry::Block(first_block));
        }

        Ok(())
    }

    /// Finalize: write remaining blocks, then field metadata.
    fn finish(
        &mut self,
        terms_out: &mut dyn IndexOutput,
        index_out: &mut dyn IndexOutput,
        postings_writer: &PostingsWriter,
        field_info: &FieldInfo,
        field_metas: &mut Vec<Vec<u8>>,
    ) -> io::Result<()> {
        if self.num_terms == 0 {
            return Ok(());
        }

        // Add empty term to force closing of all final blocks
        self.push_term(&[], terms_out, postings_writer, field_info)?;
        self.push_term(&[], terms_out, postings_writer, field_info)?;

        // Write all remaining pending entries as root block
        let pending_count = self.pending.len();
        self.write_blocks(0, pending_count, terms_out, postings_writer, field_info)?;

        // The last pending entry should be a single root block
        assert!(
            self.pending.len() == 1 && !self.pending[0].is_term(),
            "expected single root block, got {} entries",
            self.pending.len()
        );

        let root = match self.pending.remove(0) {
            PendingEntry::Block(b) => b,
            _ => unreachable!(),
        };

        // Compute doc_count from docs_seen
        let doc_count = self.doc_count;

        // Write field metadata to a buffer
        let mut meta_buf = Vec::new();
        {
            let mut meta = VecOutput(&mut meta_buf);
            meta.write_vint(field_info.number() as i32)?;
            meta.write_vlong(self.num_terms as i64)?;
            if field_info.index_options() != IndexOptions::Docs {
                meta.write_vlong(self.sum_total_term_freq)?;
            }
            meta.write_vlong(self.sum_doc_freq)?;
            meta.write_vint(doc_count)?;

            // Min term
            if !self.first_term_bytes.is_empty() {
                write_bytes_ref(&mut meta, &self.first_term_bytes)?;
            } else {
                meta.write_vint(0)?;
            }

            // Max term
            if !self.last_term_bytes.is_empty() {
                write_bytes_ref(&mut meta, &self.last_term_bytes)?;
            } else {
                meta.write_vint(0)?;
            }

            // Save trie index
            if let Some(trie) = root.index {
                trie.save(&mut meta, index_out)?;
            }
        }

        field_metas.push(meta_buf);

        debug!(
            "blocktree_writer: finish field={} num_terms={} sum_doc_freq={} doc_count={}",
            field_info.name(),
            self.num_terms,
            self.sum_doc_freq,
            doc_count
        );

        Ok(())
    }
}

/// Write a single block to .tim output.
#[allow(clippy::too_many_arguments)]
fn write_block_to_output(
    pending: &[PendingEntry],
    last_term: &[u8],
    prefix_length: usize,
    spec: &BlockSpec,
    terms_out: &mut dyn IndexOutput,
    postings_writer: &PostingsWriter,
    field_ctx: &FieldWriteContext,
    lz4_ht: &mut lz4::HighCompressionHashTable,
) -> io::Result<PendingBlock> {
    let start_fp = terms_out.file_pointer();
    let has_floor_lead_label = spec.is_floor && spec.floor_lead_label != -1;

    let mut prefix = Vec::with_capacity(prefix_length + if has_floor_lead_label { 1 } else { 0 });
    prefix.extend_from_slice(&last_term[..prefix_length.min(last_term.len())]);
    // Pad if last_term is shorter than prefix_length
    while prefix.len() < prefix_length {
        prefix.push(0);
    }

    let num_entries = spec.end - spec.start;
    let is_leaf_block = !spec.has_sub_blocks;

    // Block header: (numEntries << 1) | isLastBlock
    let code = (num_entries << 1) | if spec.end == pending.len() { 1 } else { 0 };
    terms_out.write_vint(code as i32)?;

    // Buffers for suffix data, suffix lengths, stats, and metadata
    let mut suffix_bytes = Vec::new();
    let mut suffix_lengths_bytes = Vec::new();
    let mut stats_bytes = Vec::new();
    let mut meta_bytes = Vec::new();

    let mut suffix_lengths_writer = VecOutput(&mut suffix_lengths_bytes);
    let mut stats_writer = StatsWriter::new(&mut stats_bytes, field_ctx.write_freqs);
    let mut last_state = IntBlockTermState::new();
    let mut absolute = true;

    let mut sub_indices: Vec<TrieBuilder> = Vec::new();

    if is_leaf_block {
        // Block contains only terms
        for entry in &pending[spec.start..spec.end] {
            if let PendingEntry::Term(term) = entry {
                let suffix_len = term.term_bytes.len() - prefix_length;
                suffix_lengths_writer.write_vint(suffix_len as i32)?;
                suffix_bytes.extend_from_slice(&term.term_bytes[prefix_length..]);

                stats_writer.add(term.state.doc_freq, term.state.total_term_freq)?;

                let empty_state = IntBlockTermState::new();
                let ref_state = if absolute { &empty_state } else { &last_state };
                postings_writer.encode_term(
                    &mut meta_bytes,
                    field_ctx.field_info,
                    &term.state,
                    ref_state,
                    field_ctx.write_positions,
                )?;
                last_state = term.state;
                absolute = false;
            }
        }
        stats_writer.finish()?;
    } else {
        // Block has terms and/or sub-blocks
        for entry in &pending[spec.start..spec.end] {
            match entry {
                PendingEntry::Term(term) => {
                    let suffix_len = term.term_bytes.len() - prefix_length;
                    // Steal 1 bit: even = term
                    suffix_lengths_writer.write_vint((suffix_len << 1) as i32)?;
                    suffix_bytes.extend_from_slice(&term.term_bytes[prefix_length..]);

                    stats_writer.add(term.state.doc_freq, term.state.total_term_freq)?;

                    let empty_state = IntBlockTermState::new();
                    let ref_state = if absolute { &empty_state } else { &last_state };
                    postings_writer.encode_term(
                        &mut meta_bytes,
                        field_ctx.field_info,
                        &term.state,
                        ref_state,
                        field_ctx.write_positions,
                    )?;
                    last_state = term.state;
                    absolute = false;
                }
                PendingEntry::Block(block) => {
                    let suffix_len = block.prefix.len() - prefix_length;
                    // Steal 1 bit: odd = sub-block
                    suffix_lengths_writer.write_vint(((suffix_len << 1) | 1) as i32)?;
                    suffix_bytes.extend_from_slice(&block.prefix[prefix_length..]);

                    // Write sub-block fp delta
                    suffix_lengths_writer.write_vlong((start_fp - block.fp) as i64)?;

                    if let Some(idx) = &block.index {
                        sub_indices.push(idx.clone());
                    }
                }
            }
        }
        stats_writer.finish()?;
    }

    // Write suffix data, optionally compressed with LZ4 or lowercase ASCII.
    let suffix_len = suffix_bytes.len();
    let mut compression_code: i64 = 0; // 0=none, 1=lowercase_ascii, 2=lz4
    let mut compressed_bytes: Option<Vec<u8>> = None;

    // Only try compression when there are enough suffix bytes per entry
    // and the prefix is long enough (>2) to avoid hurting fuzzy query perf.
    if suffix_len > 2 * num_entries && prefix_length > 2 {
        // Try LZ4 first if average suffix > 6 bytes
        if suffix_len > 6 * num_entries {
            let lz4_compressed = lz4::compress_high(&suffix_bytes, lz4_ht);
            // Use LZ4 only if it saves > 25%
            if lz4_compressed.len() < suffix_len - (suffix_len >> 2) {
                compression_code = 2;
                compressed_bytes = Some(lz4_compressed);
            }
        }
        // Fall back to lowercase ASCII if LZ4 didn't help
        if compression_code == 0
            && let Some(ascii_compressed) = lowercase_ascii::compress(&suffix_bytes, suffix_len)
        {
            compression_code = 1;
            compressed_bytes = Some(ascii_compressed);
        }
    }

    let mut token = (suffix_len as i64) << 3;
    if is_leaf_block {
        token |= 0x04;
    }
    token |= compression_code;
    terms_out.write_vlong(token)?;
    if let Some(ref data) = compressed_bytes {
        terms_out.write_bytes(data)?;
    } else {
        terms_out.write_bytes(&suffix_bytes)?;
    }

    // Write suffix lengths
    let num_suffix_bytes = suffix_lengths_bytes.len();
    if num_suffix_bytes > 1 && all_equal(&suffix_lengths_bytes[1..], suffix_lengths_bytes[0]) {
        terms_out.write_vint(((num_suffix_bytes << 1) | 1) as i32)?;
        terms_out.write_byte(suffix_lengths_bytes[0])?;
    } else {
        terms_out.write_vint((num_suffix_bytes << 1) as i32)?;
        terms_out.write_bytes(&suffix_lengths_bytes)?;
    }

    // Write stats
    terms_out.write_vint(stats_bytes.len() as i32)?;
    terms_out.write_bytes(&stats_bytes)?;

    // Write metadata
    terms_out.write_vint(meta_bytes.len() as i32)?;
    terms_out.write_bytes(&meta_bytes)?;

    if has_floor_lead_label {
        prefix.push(spec.floor_lead_label as u8);
    }

    Ok(PendingBlock {
        prefix,
        fp: start_fp,
        has_terms: spec.has_terms,
        is_floor: spec.is_floor,
        floor_lead_byte: spec.floor_lead_label,
        index: None,
        sub_indices: if sub_indices.is_empty() {
            None
        } else {
            Some(sub_indices)
        },
    })
}

fn all_equal(bytes: &[u8], value: u8) -> bool {
    bytes.iter().all(|&b| b == value)
}

fn write_bytes_ref(out: &mut VecOutput, bytes: &[u8]) -> io::Result<()> {
    out.write_vint(bytes.len() as i32)?;
    out.write_bytes(bytes)?;
    Ok(())
}

fn mismatch(a: &[u8], b: &[u8]) -> usize {
    let len = a.len().min(b.len());
    for i in 0..len {
        if a[i] != b[i] {
            return i;
        }
    }
    len
}

// ============================================================================
// StatsWriter — run-length encodes singleton (df=1, ttf=1) terms
// ============================================================================

struct StatsWriter<'a> {
    out: &'a mut Vec<u8>,
    has_freqs: bool,
    singleton_count: i32,
}

impl<'a> StatsWriter<'a> {
    fn new(out: &'a mut Vec<u8>, has_freqs: bool) -> Self {
        Self {
            out,
            has_freqs,
            singleton_count: 0,
        }
    }

    fn add(&mut self, df: i32, ttf: i64) -> io::Result<()> {
        if df == 1 && (!self.has_freqs || ttf == 1) {
            self.singleton_count += 1;
        } else {
            self.flush_singletons()?;
            let mut buf = VecOutput(self.out);
            buf.write_vint(df << 1)?;
            if self.has_freqs {
                buf.write_vlong(ttf - df as i64)?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        self.flush_singletons()
    }

    fn flush_singletons(&mut self) -> io::Result<()> {
        if self.singleton_count > 0 {
            let mut buf = VecOutput(self.out);
            buf.write_vint(((self.singleton_count - 1) << 1) | 1)?;
            self.singleton_count = 0;
        }
        Ok(())
    }
}

// ============================================================================
// TrieBuilder — prefix trie for .tip index
// ============================================================================

// Node type signatures
const SIGN_NO_CHILDREN: u8 = 0x00;
const SIGN_SINGLE_CHILD_WITH_OUTPUT: u8 = 0x01;
const SIGN_SINGLE_CHILD_WITHOUT_OUTPUT: u8 = 0x02;
const SIGN_MULTI_CHILDREN: u8 = 0x03;

const LEAF_NODE_HAS_TERMS: u8 = 1 << 5;
const LEAF_NODE_HAS_FLOOR: u8 = 1 << 6;
const NON_LEAF_NODE_HAS_TERMS: u64 = 1 << 1;
const NON_LEAF_NODE_HAS_FLOOR: u64 = 1 << 0;

#[derive(Clone, Debug)]
pub struct TrieOutput {
    pub fp: i64,
    pub has_terms: bool,
    pub floor_data: Option<BytesRef>,
}

#[derive(Clone, Debug)]
struct TrieNode {
    label: u8,
    output: Option<TrieOutput>,
    children: Vec<TrieNode>,
}

#[derive(Clone, Debug)]
pub struct TrieBuilder {
    root: TrieNode,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

impl TrieBuilder {
    pub fn from_bytes_ref(key: &BytesRef, output: TrieOutput) -> Self {
        let mut root = TrieNode {
            label: 0,
            output: if key.is_empty() {
                Some(output.clone())
            } else {
                None
            },
            children: Vec::new(),
        };

        if !key.is_empty() {
            // Build chain of nodes for each byte
            let mut nodes: Vec<TrieNode> = Vec::new();

            for (i, &b) in key.bytes.iter().enumerate() {
                let is_last = i == key.bytes.len() - 1;
                nodes.push(TrieNode {
                    label: b,
                    output: if is_last { Some(output.clone()) } else { None },
                    children: Vec::new(),
                });
            }

            // Build tree bottom-up
            while nodes.len() > 1 {
                let child = nodes.pop().unwrap();
                nodes.last_mut().unwrap().children.push(child);
            }

            if let Some(node) = nodes.pop() {
                root.children.push(node);
            }
        }

        Self {
            root,
            min_key: key.bytes.clone(),
            max_key: key.bytes.clone(),
        }
    }

    /// Append another trie (whose keys are all greater than this one's max key).
    pub fn append(&mut self, other: TrieBuilder) {
        let mismatch_pos = mismatch(&self.max_key, &other.min_key);
        merge_nodes(&mut self.root, &other.root, 0, mismatch_pos);
        self.max_key = other.max_key;
    }

    /// Save the trie to .tmd (meta) and .tip (index) outputs.
    pub fn save(&self, meta: &mut dyn DataOutput, index: &mut dyn IndexOutput) -> io::Result<()> {
        meta.write_vlong(index.file_pointer() as i64)?; // indexStartFP
        let root_fp = self.save_nodes(index)?;
        meta.write_vlong(root_fp)?; // rootCode (offset of root node in .tip)
        index.write_le_long(0)?; // 8 bytes for over-reading
        meta.write_vlong(index.file_pointer() as i64)?; // indexEndFP
        Ok(())
    }

    /// Save trie nodes to the index output in post-order.
    /// Returns the root node's FP (relative to the start of this save).
    fn save_nodes(&self, index: &mut dyn IndexOutput) -> io::Result<i64> {
        let start_fp = index.file_pointer();
        let root_fp = save_node_recursive(&self.root, index, start_fp)?;
        Ok(root_fp)
    }
}

/// Recursively save nodes in post-order (children before parent).
/// Returns the fp (relative to start_fp) where this node was written.
fn save_node_recursive(
    node: &TrieNode,
    index: &mut dyn IndexOutput,
    start_fp: u64,
) -> io::Result<i64> {
    // First save all children
    let mut child_fps: Vec<i64> = Vec::new();
    for child in &node.children {
        let fp = save_node_recursive(child, index, start_fp)?;
        child_fps.push(fp);
    }

    let node_fp = (index.file_pointer() - start_fp) as i64;

    let children_num = node.children.len();

    if children_num == 0 {
        // Leaf node
        if let Some(ref output) = node.output {
            let output_fp_bytes = bytes_required_vlong(output.fp);
            let header: u8 = SIGN_NO_CHILDREN
                | ((output_fp_bytes as u8 - 1) << 2)
                | (if output.has_terms {
                    LEAF_NODE_HAS_TERMS
                } else {
                    0
                })
                | (if output.floor_data.is_some() {
                    LEAF_NODE_HAS_FLOOR
                } else {
                    0
                });
            index.write_byte(header)?;
            write_long_n_bytes(output.fp, output_fp_bytes, index)?;
            if let Some(ref floor_data) = output.floor_data {
                index.write_bytes(floor_data.as_slice())?;
            }
        }
    } else if children_num == 1 {
        // Single child node
        let child_delta_fp = node_fp - child_fps[0];
        let child_fp_bytes = bytes_required_vlong(child_delta_fp);
        let encoded_output_fp_bytes: u32 = match node.output {
            Some(ref output) => bytes_required_vlong(encode_fp(output)),
            None => 0,
        };

        let sign: u32 = if node.output.is_some() {
            SIGN_SINGLE_CHILD_WITH_OUTPUT as u32
        } else {
            SIGN_SINGLE_CHILD_WITHOUT_OUTPUT as u32
        };

        // Java unconditionally includes encoded_output_fp_bytes in the header.
        // When output is absent (encoded_output_fp_bytes=0), the (0-1)<<5 overflow
        // sets the high bits to 0xE0. The reader ignores these bits for
        // SIGN_SINGLE_CHILD_WITHOUT_OUTPUT, but we must match the bytes.
        let header = (sign
            | ((child_fp_bytes - 1) << 2)
            | (encoded_output_fp_bytes.wrapping_sub(1) << 5)) as u8;
        index.write_byte(header)?;
        index.write_byte(node.children[0].label)?;
        write_long_n_bytes(child_delta_fp, child_fp_bytes, index)?;

        if let Some(ref output) = node.output {
            let encoded_fp = encode_fp(output);
            write_long_n_bytes(encoded_fp, encoded_output_fp_bytes, index)?;
            if let Some(ref floor_data) = output.floor_data {
                index.write_bytes(floor_data.as_slice())?;
            }
        }
    } else {
        // Multi-children node
        let min_label = node.children[0].label;
        let max_label = node.children.last().unwrap().label;

        let strategy = choose_child_save_strategy(min_label, max_label, children_num);
        let strategy_bytes = strategy.need_bytes(min_label, max_label, children_num);

        let max_child_delta_fp = node_fp - child_fps[0];
        let children_fp_bytes = bytes_required_vlong(max_child_delta_fp);
        let encoded_output_fp_bytes = if let Some(ref output) = node.output {
            bytes_required_vlong(encode_fp(output))
        } else {
            1
        };

        let header: u32 = (SIGN_MULTI_CHILDREN as u32)
            | ((children_fp_bytes - 1) << 2)
            | ((if node.output.is_some() { 1u32 } else { 0 }) << 5)
            | (((encoded_output_fp_bytes as u32) - 1) << 6)
            | ((strategy.code() as u32) << 9)
            | (((strategy_bytes as u32) - 1) << 11)
            | ((min_label as u32) << 16);

        write_long_n_bytes(header as i64, 3, index)?;

        if let Some(ref output) = node.output {
            let encoded_fp = encode_fp(output);
            write_long_n_bytes(encoded_fp, encoded_output_fp_bytes, index)?;
            if output.floor_data.is_some() {
                index.write_byte((children_num - 1) as u8)?;
            }
        }

        // Write strategy data
        strategy.save(node, strategy_bytes, index)?;

        // Write children fps (deltas from node_fp)
        for &child_fp in &child_fps {
            write_long_n_bytes(node_fp - child_fp, children_fp_bytes, index)?;
        }

        // Write floor data if present
        if let Some(ref output) = node.output
            && let Some(ref floor_data) = output.floor_data
        {
            index.write_bytes(floor_data.as_slice())?;
        }
    }

    Ok(node_fp)
}

fn encode_fp(output: &TrieOutput) -> i64 {
    let mut encoded = output.fp << 2;
    if output.has_terms {
        encoded |= NON_LEAF_NODE_HAS_TERMS as i64;
    }
    if output.floor_data.is_some() {
        encoded |= NON_LEAF_NODE_HAS_FLOOR as i64;
    }
    encoded
}

fn bytes_required_vlong(v: i64) -> u32 {
    let v = v as u64 | 1; // ensure at least 1 byte
    (8 - (v.leading_zeros() >> 3)).max(1)
}

fn write_long_n_bytes(v: i64, n: u32, out: &mut dyn DataOutput) -> io::Result<()> {
    let mut v = v as u64;
    for _ in 0..n {
        out.write_byte(v as u8)?;
        v >>= 8;
    }
    Ok(())
}

/// Merge nodes from `other` into `this` at the given depth.
fn merge_nodes(this: &mut TrieNode, other: &TrieNode, depth: usize, mismatch_pos: usize) {
    if depth < mismatch_pos {
        // Find matching child and recurse
        if let Some(other_child) = other.children.first() {
            if let Some(this_child) = this
                .children
                .iter_mut()
                .find(|c| c.label == other_child.label)
            {
                merge_nodes(this_child, other_child, depth + 1, mismatch_pos);
                // Also add any additional children from other
                for other_c in other.children.iter().skip(1) {
                    this.children.push(other_c.clone());
                }
            } else {
                // No matching child — just add all other's children
                for c in &other.children {
                    this.children.push(c.clone());
                }
            }
        }
    } else {
        // Past mismatch point — add all children from other
        for c in &other.children {
            if !this.children.iter().any(|tc| tc.label == c.label) {
                this.children.push(c.clone());
            }
        }
    }

    // Keep children sorted by label
    this.children.sort_by_key(|c| c.label);
}

// ============================================================================
// ChildSaveStrategy
// ============================================================================

enum ChildSaveStrategy {
    Bits,
    Array,
    ReverseArray,
}

impl ChildSaveStrategy {
    fn code(&self) -> u32 {
        match self {
            ChildSaveStrategy::ReverseArray => 0,
            ChildSaveStrategy::Array => 1,
            ChildSaveStrategy::Bits => 2,
        }
    }

    fn need_bytes(&self, min_label: u8, max_label: u8, label_cnt: usize) -> u32 {
        let byte_distance = (max_label as u32) - (min_label as u32) + 1;
        match self {
            ChildSaveStrategy::Bits => byte_distance.div_ceil(8),
            ChildSaveStrategy::Array => (label_cnt - 1) as u32,
            ChildSaveStrategy::ReverseArray => byte_distance - label_cnt as u32 + 1,
        }
    }

    fn save(
        &self,
        node: &TrieNode,
        _strategy_bytes: u32,
        out: &mut dyn DataOutput,
    ) -> io::Result<()> {
        match self {
            ChildSaveStrategy::Bits => {
                let min_label = node.children[0].label;
                let mut presence_bits: u8 = 1; // First child always present
                let mut presence_index = 0usize;
                let mut previous_label = min_label;

                for child in node.children.iter().skip(1) {
                    presence_index += (child.label - previous_label) as usize;
                    while presence_index >= 8 {
                        out.write_byte(presence_bits)?;
                        presence_bits = 0;
                        presence_index -= 8;
                    }
                    presence_bits |= 1 << presence_index;
                    previous_label = child.label;
                }
                out.write_byte(presence_bits)?;
            }
            ChildSaveStrategy::Array => {
                // Write all labels except the minimum (which is in the header)
                for child in node.children.iter().skip(1) {
                    out.write_byte(child.label)?;
                }
            }
            ChildSaveStrategy::ReverseArray => {
                // Write max label, then absent labels
                let max_label = node.children.last().unwrap().label;
                out.write_byte(max_label)?;
                let min_label = node.children[0].label;
                let mut last_label = min_label;
                for child in node.children.iter().skip(1) {
                    let mut next = last_label + 1;
                    while next < child.label {
                        out.write_byte(next)?;
                        next += 1;
                    }
                    last_label = child.label;
                }
            }
        }
        Ok(())
    }
}

fn choose_child_save_strategy(min_label: u8, max_label: u8, label_cnt: usize) -> ChildSaveStrategy {
    let strategies = [
        ChildSaveStrategy::Bits,
        ChildSaveStrategy::Array,
        ChildSaveStrategy::ReverseArray,
    ];

    let mut best = &strategies[0];
    let mut best_bytes = u32::MAX;

    for strategy in &strategies {
        let cost = strategy.need_bytes(min_label, max_label, label_cnt);
        if cost < best_bytes {
            best_bytes = cost;
            best = strategy;
        }
    }

    // Return a new instance matching the best
    match best {
        ChildSaveStrategy::Bits => ChildSaveStrategy::Bits,
        ChildSaveStrategy::Array => ChildSaveStrategy::Array,
        ChildSaveStrategy::ReverseArray => ChildSaveStrategy::ReverseArray,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::DocValuesType;
    use crate::index::PointDimensionConfig;
    use crate::index::indexing_chain::PostingList;
    use crate::store::{MemoryDirectory, MemoryIndexOutput, SharedDirectory};
    use std::collections::HashMap;

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    /// Sorts a HashMap of terms into the slice format expected by write_field.
    fn sort_terms(terms: &HashMap<String, PostingList>) -> Vec<(&str, &PostingList)> {
        let mut sorted: Vec<_> = terms.iter().map(|(k, v)| (k.as_str(), v)).collect();
        sorted.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
        sorted
    }

    fn make_field_info(name: &str, number: u32, index_options: IndexOptions) -> FieldInfo {
        FieldInfo::new(
            name.to_string(),
            number,
            false,
            false,
            index_options,
            DocValuesType::None,
            PointDimensionConfig::default(),
        )
    }

    /// Builds a PostingList from a slice of (doc_id, freq, positions) tuples.
    /// Uses the PostingList's streaming API to simulate how IndexingChain
    /// would build postings during document processing.
    fn make_posting_list(postings: &[(i32, i32, &[i32])], has_positions: bool) -> PostingList {
        let has_freqs = true; // all test cases use freqs
        let mut pl = PostingList::new(has_freqs, has_positions, false);
        for &(doc_id, freq, positions) in postings {
            pl.start_doc(doc_id);
            // freq starts at 1 from start_doc, so increment (freq-1) times
            for _ in 1..freq {
                pl.increment_freq();
            }
            for &pos in positions {
                pl.add_position(pos);
            }
        }
        pl.finalize_current_doc();
        pl
    }

    #[test]
    fn test_stats_writer_singletons() {
        let mut buf = Vec::new();
        let mut sw = StatsWriter::new(&mut buf, true);
        sw.add(1, 1).unwrap(); // singleton
        sw.add(1, 1).unwrap(); // singleton
        sw.add(1, 1).unwrap(); // singleton
        sw.finish().unwrap();

        // Should be run-length encoded: ((3-1) << 1) | 1 = 5
        assert_eq!(buf, vec![5]);
    }

    #[test]
    fn test_stats_writer_mixed() {
        let mut buf = Vec::new();
        let mut sw = StatsWriter::new(&mut buf, true);
        sw.add(1, 1).unwrap(); // singleton
        sw.add(3, 5).unwrap(); // non-singleton: df=3, ttf=5
        sw.finish().unwrap();

        // First: singleton count=1 → ((1-1)<<1)|1 = 1
        // Then: df=3 → 3<<1 = 6, ttf-df = 5-3 = 2
        assert_eq!(buf, vec![1, 6, 2]);
    }

    #[test]
    fn test_trie_builder_single_key() {
        let output = TrieOutput {
            fp: 42,
            has_terms: true,
            floor_data: None,
        };
        let trie = TrieBuilder::from_bytes_ref(&BytesRef::from_utf8("abc"), output);

        assert_none!(trie.root.output);
        assert_eq!(trie.root.children.len(), 1);
        assert_eq!(trie.root.children[0].label, b'a');
    }

    #[test]
    fn test_trie_builder_empty_key() {
        let output = TrieOutput {
            fp: 10,
            has_terms: true,
            floor_data: None,
        };
        let trie = TrieBuilder::from_bytes_ref(&BytesRef::new(Vec::new()), output);

        assert_some!(trie.root.output);
        assert_is_empty!(trie.root.children);
    }

    #[test]
    fn test_trie_builder_append() {
        let out1 = TrieOutput {
            fp: 10,
            has_terms: true,
            floor_data: None,
        };
        let out2 = TrieOutput {
            fp: 20,
            has_terms: true,
            floor_data: None,
        };

        let mut trie1 = TrieBuilder::from_bytes_ref(&BytesRef::from_utf8("abc"), out1);
        let trie2 = TrieBuilder::from_bytes_ref(&BytesRef::from_utf8("abd"), out2);

        trie1.append(trie2);

        // Root should have one child 'a'
        assert_eq!(trie1.root.children.len(), 1);
        let a_node = &trie1.root.children[0];
        assert_eq!(a_node.label, b'a');
        // 'a' should have one child 'b'
        assert_eq!(a_node.children.len(), 1);
        let b_node = &a_node.children[0];
        assert_eq!(b_node.label, b'b');
        // 'b' should have two children 'c' and 'd'
        assert_eq!(b_node.children.len(), 2);
        assert_eq!(b_node.children[0].label, b'c');
        assert_eq!(b_node.children[1].label, b'd');
    }

    #[test]
    fn test_trie_save_simple() {
        let output = TrieOutput {
            fp: 42,
            has_terms: true,
            floor_data: None,
        };
        let trie = TrieBuilder::from_bytes_ref(&BytesRef::from_utf8("a"), output);

        let mut meta = Vec::new();
        let mut index = MemoryIndexOutput::new("test.tip".to_string());

        trie.save(&mut VecOutput(&mut meta), &mut index).unwrap();

        // Index should have some bytes
        assert_gt!(index.file_pointer(), 0);
        // Meta should have indexStartFP, rootFP, indexEndFP
        assert_not_empty!(meta);
    }

    #[test]
    fn test_trie_save_root_fp_nonzero_with_children() {
        // Regression test: root_fp must not be 0 when the trie has child nodes.
        // Previously root_fp was always 0 (placeholder), causing Java to read
        // the first child instead of the root.
        let output_a = TrieOutput {
            fp: 10,
            has_terms: true,
            floor_data: None,
        };
        let output_b = TrieOutput {
            fp: 20,
            has_terms: true,
            floor_data: None,
        };
        let mut trie = TrieBuilder::from_bytes_ref(&BytesRef::from_utf8("a"), output_a);
        trie.append(TrieBuilder::from_bytes_ref(
            &BytesRef::from_utf8("b"),
            output_b,
        ));

        let mut meta = Vec::new();
        let mut index = MemoryIndexOutput::new("test.tip".to_string());

        trie.save(&mut VecOutput(&mut meta), &mut index).unwrap();

        // Parse meta: [indexStartFP (vlong), rootFP (vlong), indexEndFP (vlong)]
        let mut pos = 0;
        let (index_start_fp, n) = read_vlong_at(&meta, pos);
        pos += n;
        let (root_fp, n) = read_vlong_at(&meta, pos);
        pos += n;
        let (index_end_fp, _) = read_vlong_at(&meta, pos);

        assert_eq!(
            index_start_fp, 0,
            "indexStartFP should be 0 (start of .tip)"
        );
        assert_gt!(
            root_fp,
            0,
            "rootFP must be > 0 for a trie with children (got {root_fp})"
        );
        assert_gt!(
            index_end_fp,
            root_fp,
            "indexEndFP ({index_end_fp}) should be > rootFP ({root_fp})"
        );
    }

    /// Read a VLong from a byte slice at a given position. Returns (value, bytes_consumed).
    fn read_vlong_at(data: &[u8], pos: usize) -> (i64, usize) {
        let mut result: i64 = 0;
        let mut shift = 0;
        let mut i = pos;
        loop {
            let b = data[i] as i64;
            i += 1;
            result |= (b & 0x7F) << shift;
            if (b & 0x80) == 0 {
                break;
            }
            shift += 7;
        }
        (result, i - pos)
    }

    #[test]
    fn test_bytes_required_vlong() {
        assert_eq!(bytes_required_vlong(0), 1);
        assert_eq!(bytes_required_vlong(1), 1);
        assert_eq!(bytes_required_vlong(255), 1);
        assert_eq!(bytes_required_vlong(256), 2);
        assert_eq!(bytes_required_vlong(0xFFFF), 2);
        assert_eq!(bytes_required_vlong(0x10000), 3);
    }

    #[test]
    fn test_write_field_simple() {
        // Create a field with 3 terms
        let fi = make_field_info("test", 0, IndexOptions::DocsAndFreqs);
        let field_infos = FieldInfos::new(vec![fi.clone()]);

        let mut terms = HashMap::new();
        terms.insert(
            "apple".to_string(),
            make_posting_list(&[(0, 2, &[])], false),
        );
        terms.insert(
            "banana".to_string(),
            make_posting_list(&[(0, 1, &[]), (1, 3, &[])], false),
        );
        terms.insert(
            "cherry".to_string(),
            make_posting_list(&[(2, 1, &[])], false),
        );

        let id = [0u8; 16];
        let dir = test_directory();
        let mut btw = BlockTreeTermsWriter::new(&dir, "_0", "", &id, &field_infos).unwrap();
        btw.write_field(&fi, &sort_terms(&terms), &NormsLookup::no_norms())
            .unwrap();
        let names = btw.finish().unwrap();

        // Should produce .tim, .tip, .tmd, .doc, .psm files (no .pos since no positions)
        assert_ge!(
            names.len(),
            4,
            "expected at least 4 files, got {}",
            names.len()
        );

        assert!(
            names.iter().any(|n| n.ends_with(".tim")),
            "missing .tim: {:?}",
            names
        );
        assert!(
            names.iter().any(|n| n.ends_with(".tip")),
            "missing .tip: {:?}",
            names
        );
        assert!(
            names.iter().any(|n| n.ends_with(".tmd")),
            "missing .tmd: {:?}",
            names
        );
        assert!(
            names.iter().any(|n| n.ends_with(".doc")),
            "missing .doc: {:?}",
            names
        );
        assert!(
            names.iter().any(|n| n.ends_with(".psm")),
            "missing .psm: {:?}",
            names
        );

        // Verify all files have content
        for name in &names {
            let data = dir.lock().unwrap().read_file(name).unwrap();
            assert_not_empty!(data, "file {name} is empty");
        }
    }

    #[test]
    fn test_write_field_with_positions() {
        let fi = make_field_info("contents", 0, IndexOptions::DocsAndFreqsAndPositions);
        let field_infos = FieldInfos::new(vec![fi.clone()]);

        let mut terms = HashMap::new();
        terms.insert(
            "hello".to_string(),
            make_posting_list(&[(0, 2, &[0, 5]), (1, 1, &[3])], true),
        );
        terms.insert(
            "world".to_string(),
            make_posting_list(&[(0, 1, &[1])], true),
        );

        let id = [0u8; 16];
        let dir = test_directory();
        let mut btw = BlockTreeTermsWriter::new(&dir, "_0", "", &id, &field_infos).unwrap();
        btw.write_field(&fi, &sort_terms(&terms), &NormsLookup::no_norms())
            .unwrap();
        let names = btw.finish().unwrap();

        // Should produce .tim, .tip, .tmd, .doc, .pos, .psm files
        assert!(
            names.iter().any(|n| n.ends_with(".pos")),
            "missing .pos: {:?}",
            names
        );
    }

    /// Regression test for usize overflow in push_term when processing many terms
    /// with shared prefixes. In Java, `prefixStarts` is `int[]` (signed), so
    /// `pending.size() - prefixStarts[i]` can go negative without error. In Rust
    /// with `usize`, this caused a panic: "attempt to subtract with overflow".
    ///
    /// The bug triggers when write_blocks() collapses entries in pending,
    /// making prefix_starts[i] stale (larger than pending.len()) for subsequent
    /// loop iterations.
    #[test]
    fn test_push_term_many_terms_no_overflow() {
        let fi = make_field_info("contents", 0, IndexOptions::DocsAndFreqsAndPositions);
        let field_infos = FieldInfos::new(vec![fi.clone()]);

        // Generate enough terms with shared prefixes to trigger block writing.
        // min_items_in_block = 25, so we need > 25 terms sharing a prefix,
        // then switch to a different prefix to trigger the suffix-closing
        // loop where the overflow occurred.
        let mut terms = HashMap::new();

        // 30 terms starting with "aaa" (sorted: aaa_00..aaa_29)
        for i in 0..30 {
            let term = format!("aaa_{:02}", i);
            terms.insert(term, make_posting_list(&[(0, 1, &[i])], true));
        }

        // 30 terms starting with "bbb" — forces closing of "aaa" prefix group
        for i in 0..30 {
            let term = format!("bbb_{:02}", i);
            terms.insert(term, make_posting_list(&[(0, 1, &[30 + i])], true));
        }

        // 30 terms starting with "ccc" — forces closing of "bbb" prefix group
        for i in 0..30 {
            let term = format!("ccc_{:02}", i);
            terms.insert(term, make_posting_list(&[(0, 1, &[60 + i])], true));
        }

        let id = [0u8; 16];
        let dir = test_directory();
        let mut btw = BlockTreeTermsWriter::new(&dir, "_0", "", &id, &field_infos).unwrap();
        // This panicked before the fix with "attempt to subtract with overflow"
        btw.write_field(&fi, &sort_terms(&terms), &NormsLookup::no_norms())
            .unwrap();
        let names = btw.finish().unwrap();

        assert!(
            names.iter().any(|n| n.ends_with(".tim")),
            "missing .tim: {:?}",
            names
        );
    }

    #[test]
    fn test_doc_count_computed_correctly() {
        // 3 terms spanning 3 unique docs: doc_count should be 3
        let fi = make_field_info("test", 0, IndexOptions::DocsAndFreqs);
        let field_infos = FieldInfos::new(vec![fi.clone()]);

        let mut terms = HashMap::new();
        terms.insert(
            "alpha".to_string(),
            make_posting_list(&[(0, 1, &[]), (1, 1, &[])], false),
        );
        terms.insert(
            "beta".to_string(),
            make_posting_list(&[(1, 1, &[]), (2, 1, &[])], false),
        );

        let id = [0u8; 16];
        let dir = test_directory();
        let mut btw = BlockTreeTermsWriter::new(&dir, "_0", "", &id, &field_infos).unwrap();
        btw.write_field(&fi, &sort_terms(&terms), &NormsLookup::no_norms())
            .unwrap();
        let names = btw.finish().unwrap();

        // Find .tmd and parse the field metadata to check doc_count
        let tmd_name = names.iter().find(|n| n.ends_with(".tmd")).unwrap();
        let tmd_bytes = dir.lock().unwrap().read_file(tmd_name).unwrap();

        // After the BlockTreeTermsMeta header, the postingsWriter.init writes
        // a TERMS_CODEC header + BLOCK_SIZE(VInt), then numFields(VInt), then per-field metadata.
        let meta_hdr_len = codec_util::index_header_length(
            crate::codecs::lucene103::postings_format::TERMS_META_CODEC_NAME,
            "",
        );
        let terms_hdr_len = codec_util::index_header_length(
            crate::codecs::lucene103::postings_format::TERMS_CODEC,
            "",
        );
        let mut pos = meta_hdr_len + terms_hdr_len;
        // Skip BLOCK_SIZE VInt (128 = 1 byte)
        let (_, n) = read_vint(&tmd_bytes[pos..]);
        pos += n;

        // numFields
        let (num_fields, n) = read_vint(&tmd_bytes[pos..]);
        assert_eq!(num_fields, 1);
        pos += n;

        // field_number
        let (field_num, n) = read_vint(&tmd_bytes[pos..]);
        assert_eq!(field_num, 0);
        pos += n;

        // num_terms (VLong)
        let (num_terms, n) = read_vlong(&tmd_bytes[pos..]);
        assert_eq!(num_terms, 2);
        pos += n;

        // sum_total_term_freq (VLong) — only for non-DOCS-only
        let (sttf, n) = read_vlong(&tmd_bytes[pos..]);
        assert_eq!(sttf, 4); // 2+2
        pos += n;

        // sum_doc_freq (VLong)
        let (sdf, n) = read_vlong(&tmd_bytes[pos..]);
        assert_eq!(sdf, 4); // 2+2
        pos += n;

        // doc_count (VInt) — this is what we're testing
        let (doc_count, _) = read_vint(&tmd_bytes[pos..]);
        assert_eq!(doc_count, 3, "doc_count should be 3 (unique docs: 0, 1, 2)");
    }

    /// Regression: first_term_bytes was never captured because num_terms was
    /// incremented before the `== 0` check, so min term was always empty.
    #[test]
    fn test_min_max_terms_written_to_tmd() {
        let fi = make_field_info("test", 0, IndexOptions::DocsAndFreqs);
        let field_infos = FieldInfos::new(vec![fi.clone()]);

        let mut terms = HashMap::new();
        terms.insert(
            "apple".to_string(),
            make_posting_list(&[(0, 2, &[])], false),
        );
        terms.insert(
            "banana".to_string(),
            make_posting_list(&[(0, 1, &[]), (1, 3, &[])], false),
        );
        terms.insert(
            "cherry".to_string(),
            make_posting_list(&[(2, 1, &[])], false),
        );

        let id = [0u8; 16];
        let dir = test_directory();
        let mut btw = BlockTreeTermsWriter::new(&dir, "_0", "", &id, &field_infos).unwrap();
        btw.write_field(&fi, &sort_terms(&terms), &NormsLookup::no_norms())
            .unwrap();
        let names = btw.finish().unwrap();

        let tmd_name = names.iter().find(|n| n.ends_with(".tmd")).unwrap();
        let tmd_bytes = dir.lock().unwrap().read_file(tmd_name).unwrap();

        // Skip headers: BlockTreeTermsMeta + Lucene103PostingsWriterTerms + BLOCK_SIZE VInt
        let meta_hdr_len = codec_util::index_header_length(
            crate::codecs::lucene103::postings_format::TERMS_META_CODEC_NAME,
            "",
        );
        let terms_hdr_len = codec_util::index_header_length(
            crate::codecs::lucene103::postings_format::TERMS_CODEC,
            "",
        );
        let mut pos = meta_hdr_len + terms_hdr_len;
        let (_, n) = read_vint(&tmd_bytes[pos..]); // BLOCK_SIZE
        pos += n;

        let (num_fields, n) = read_vint(&tmd_bytes[pos..]);
        assert_eq!(num_fields, 1);
        pos += n;

        // field_number
        let (_, n) = read_vint(&tmd_bytes[pos..]);
        pos += n;
        // num_terms
        let (_, n) = read_vlong(&tmd_bytes[pos..]);
        pos += n;
        // sum_total_term_freq (non-DOCS field)
        let (_, n) = read_vlong(&tmd_bytes[pos..]);
        pos += n;
        // sum_doc_freq
        let (_, n) = read_vlong(&tmd_bytes[pos..]);
        pos += n;
        // doc_count
        let (_, n) = read_vint(&tmd_bytes[pos..]);
        pos += n;

        // min term: VInt length + bytes
        let (min_len, n) = read_vint(&tmd_bytes[pos..]);
        pos += n;
        let min_term = &tmd_bytes[pos..pos + min_len as usize];
        pos += min_len as usize;

        // max term: VInt length + bytes
        let (max_len, n) = read_vint(&tmd_bytes[pos..]);
        pos += n;
        let max_term = &tmd_bytes[pos..pos + max_len as usize];

        assert_eq!(
            std::str::from_utf8(min_term).unwrap(),
            "apple",
            "min term should be the first term in sorted order"
        );
        assert_eq!(
            std::str::from_utf8(max_term).unwrap(),
            "cherry",
            "max term should be the last term in sorted order"
        );
    }

    /// Helper to read a VInt from a byte slice. Returns (value, bytes_consumed).
    fn read_vint(bytes: &[u8]) -> (i32, usize) {
        let mut result = 0i32;
        let mut shift = 0;
        let mut pos = 0;
        loop {
            let b = bytes[pos] as i32;
            pos += 1;
            result |= (b & 0x7F) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        (result, pos)
    }

    /// Helper to read a VLong from a byte slice. Returns (value, bytes_consumed).
    fn read_vlong(bytes: &[u8]) -> (i64, usize) {
        let mut result = 0i64;
        let mut shift = 0;
        let mut pos = 0;
        loop {
            let b = bytes[pos] as i64;
            pos += 1;
            result |= (b & 0x7F) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        (result, pos)
    }

    /// Regression test: write_blocks must not corrupt PendingBlock prefix data.
    /// When blocks are created by an inner write_blocks call, their prefix is used
    /// later when a parent write_blocks processes them as sub-blocks. Taking ownership
    /// of the prefix (e.g., via std::mem::take) leaves an empty Vec, causing a panic
    /// when the parent block tries to compute suffix_len = block.prefix.len() - prefix_length.
    ///
    /// To trigger the bug, we need 25+ sub-groups under a shared prefix so that
    /// write_blocks is called at the parent prefix level with PendingBlocks in its range.
    #[test]
    fn test_pending_block_prefix_preserved_after_write_blocks() {
        let fi = make_field_info("contents", 0, IndexOptions::DocsAndFreqsAndPositions);
        let field_infos = FieldInfos::new(vec![fi.clone()]);

        let mut terms = HashMap::new();
        let mut pos = 0i32;

        // Create 26 sub-prefix groups under "a" (aa..az), each with 30 terms.
        // Each group of 30 triggers write_blocks(prefix_length=2), producing a
        // PendingBlock. After all groups, there are 26 PendingBlocks under prefix "a".
        // When terms starting with "b" arrive, the suffix-closing loop in push_term
        // calls write_blocks(prefix_length=1, count=26), which processes those blocks
        // as sub-blocks and accesses block.prefix[1]. If the prefix was emptied by
        // std::mem::take, this panics with a usize underflow.
        for group in b'a'..=b'z' {
            for i in 0..30 {
                let term = format!("a{}{:02}", group as char, i);
                terms.insert(term, make_posting_list(&[(0, 1, &[pos])], true));
                pos += 1;
            }
        }

        // Terms starting with "b" to force closing of the "a*" prefix group
        for i in 0..30 {
            let term = format!("b_{:02}", i);
            terms.insert(term, make_posting_list(&[(0, 1, &[pos + i])], true));
        }

        let id = [0u8; 16];
        let dir = test_directory();
        let mut btw = BlockTreeTermsWriter::new(&dir, "_0", "", &id, &field_infos).unwrap();
        btw.write_field(&fi, &sort_terms(&terms), &NormsLookup::no_norms())
            .unwrap();
        let names = btw.finish().unwrap();

        assert!(
            names.iter().any(|n| n.ends_with(".tim")),
            "missing .tim: {:?}",
            names
        );
    }

    /// Regression test: Java's TrieBuilder unconditionally includes
    /// `(encodedOutputFpBytes - 1) << 5` in the single-child node header, even
    /// when the node has no output (encodedOutputFpBytes=0). The integer overflow
    /// `(0 - 1) << 5 = 0xE0` leaks into the high bits. The reader ignores these
    /// bits for SIGN_SINGLE_CHILD_WITHOUT_OUTPUT, but we must write them to be
    /// byte-identical with Java.
    #[test]
    fn test_trie_single_child_without_output_header_matches_java() {
        // Key "ab" creates: root(no output) -> 'a'(no output) -> 'b'(output)
        // Both root and 'a' are single-child-without-output nodes.
        let output = TrieOutput {
            fp: 42,
            has_terms: true,
            floor_data: None,
        };
        let trie = TrieBuilder::from_bytes_ref(&BytesRef::from_utf8("ab"), output);

        let mut meta = Vec::new();
        let mut index = MemoryIndexOutput::new("test.tip".to_string());
        trie.save(&mut VecOutput(&mut meta), &mut index).unwrap();

        let bytes = index.bytes();

        // Post-order serialization:
        //   offset 0: leaf 'b' — header=0x20 (SIGN_NO_CHILDREN | LEAF_NODE_HAS_TERMS), fp=42
        //   offset 2: node 'a' — SIGN_SINGLE_CHILD_WITHOUT_OUTPUT header, label='b', deltaFP
        //   offset 5: root    — SIGN_SINGLE_CHILD_WITHOUT_OUTPUT header, label='a', deltaFP

        // Verify the single-child-without-output headers have 0xE0 high bits set
        let a_header = bytes[2];
        let root_header = bytes[5];

        assert_eq!(
            a_header & 0x03,
            SIGN_SINGLE_CHILD_WITHOUT_OUTPUT,
            "node 'a' should have SIGN_SINGLE_CHILD_WITHOUT_OUTPUT"
        );
        assert_eq!(
            a_header & 0xE0,
            0xE0,
            "node 'a' header high bits should be 0xE0 to match Java (got {:#04x})",
            a_header
        );

        assert_eq!(
            root_header & 0x03,
            SIGN_SINGLE_CHILD_WITHOUT_OUTPUT,
            "root should have SIGN_SINGLE_CHILD_WITHOUT_OUTPUT"
        );
        assert_eq!(
            root_header & 0xE0,
            0xE0,
            "root header high bits should be 0xE0 to match Java (got {:#04x})",
            root_header
        );
    }

    #[test]
    fn test_child_save_strategy_bits() {
        let strategy = ChildSaveStrategy::Bits;
        // Labels 'a' (97) to 'f' (102) = range 6, needs 1 byte
        assert_eq!(strategy.need_bytes(97, 102, 6), 1);
        // Labels 'a' (97) to 'z' (122) = range 26, needs 4 bytes
        assert_eq!(strategy.need_bytes(97, 122, 26), 4);
    }
}
