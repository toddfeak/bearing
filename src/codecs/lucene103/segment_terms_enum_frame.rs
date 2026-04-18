// SPDX-License-Identifier: Apache-2.0

//! Per-block frame for the segment terms enumerator.
//!
//! Each frame in the [`super::segment_terms_enum::SegmentTermsEnum`] stack
//! represents one loaded block at a specific depth in the block tree. Frames
//! own their decoded block data (suffix bytes, suffix lengths, stats, metadata)
//! and provide methods to iterate entries and decode term metadata.

use std::io;

use crate::codecs::lucene103::postings_format::IntBlockTermState;
use crate::codecs::lucene103::segment_terms_enum::read_compressed;
use crate::codecs::lucene103::trie_reader::Node;
use crate::encoding::read_encoding::ReadEncoding;
use crate::store::slice_reader::SliceReader;
use crate::store::{DataInput, IndexInput};

/// File pointers tracking where a block lives in the `.tim` file.
#[derive(Debug, Default)]
pub(super) struct BlockPosition {
    /// File pointer where this block was loaded from.
    pub fp: i64,
    /// Original file pointer (before floor block navigation).
    pub fp_orig: i64,
    /// File pointer to the end of this block (start of next block).
    pub fp_end: i64,
}

/// Boolean flags describing the structure of a loaded block.
#[derive(Debug, Default)]
pub(super) struct BlockFlags {
    /// Whether this block has term entries (vs. only sub-blocks).
    pub has_terms: bool,
    /// Original `has_terms` value (before iteration changes it).
    pub has_terms_orig: bool,
    /// Whether this is a floor block.
    pub is_floor: bool,
    /// Whether this is the last sub-block in a floor block.
    pub is_last_in_floor: bool,
    /// Whether all entries are terms (no sub-blocks).
    pub is_leaf_block: bool,
    /// Whether all suffix lengths are equal.
    pub all_equal: bool,
}

/// Decoded block data read from the `.tim` file.
///
/// Contains the five sections of a term block: suffix bytes, suffix lengths,
/// stats, and metadata, along with reader cursors for suffix iteration.
#[derive(Debug, Default)]
pub(super) struct BlockData {
    /// Decompressed suffix bytes for all entries.
    pub suffix_bytes: Vec<u8>,
    /// Suffix reader position (cursor into `suffix_bytes`).
    pub suffix_pos: usize,
    /// Suffix length bytes for all entries.
    pub suffix_length_bytes: Vec<u8>,
    /// Suffix length reader position (cursor into `suffix_length_bytes`).
    pub suffix_length_pos: usize,
    /// Stats bytes for all entries.
    pub stat_bytes: Vec<u8>,
    /// Metadata bytes for all entries.
    pub meta_bytes: Vec<u8>,
    /// Total suffix bytes in block (for stats).
    pub total_suffix_bytes: i64,
}

/// Floor block navigation state from the `.tip` file.
#[derive(Debug, Default)]
pub(super) struct FloorState {
    /// Floor data bytes.
    pub data: Vec<u8>,
    /// Rewind position within `data`.
    pub rewind_pos: usize,
    /// Current read position within `data`.
    pub data_pos: usize,
    /// Label of the next floor block.
    pub next_label: i32,
    /// Number of follow-on floor blocks remaining.
    pub num_follow_blocks: i32,
}

impl FloorState {
    /// Parses floor data from a byte slice.
    #[cfg(test)]
    pub fn set(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.data = bytes.to_vec();
        let mut reader = SliceReader::new(&self.data);
        self.rewind_pos = 0;
        self.num_follow_blocks = reader.read_vint()?;
        self.next_label = reader.read_byte()? as i32 & 0xff;
        self.data_pos = reader.pos();
        Ok(())
    }

    /// Rewinds floor state to its initial position.
    pub fn rewind(&mut self) -> io::Result<()> {
        let mut reader = SliceReader::new(&self.data);
        reader.skip(self.rewind_pos);
        self.num_follow_blocks = reader.read_vint()?;
        debug_assert!(self.num_follow_blocks > 0);
        self.next_label = reader.read_byte()? as i32 & 0xff;
        self.data_pos = reader.pos();
        Ok(())
    }
}

/// A single frame in the [`SegmentTermsEnum`](super::segment_terms_enum::SegmentTermsEnum)
/// frame stack.
///
/// Each frame holds the decoded contents of one term block and tracks
/// iteration state within that block.
#[derive(Debug)]
#[expect(dead_code)]
pub(super) struct SegmentTermsEnumFrame {
    /// Index of this frame in the stack.
    pub ord: i32,
    /// Block file pointers in `.tim`.
    pub pos: BlockPosition,
    /// Structural flags for the loaded block.
    pub flags: BlockFlags,
    /// Decoded block section data.
    pub data: BlockData,
    /// Floor block navigation state.
    pub floor: FloorState,

    /// Length of the prefix shared by all terms in this block.
    pub prefix_length: usize,
    /// Number of entries (term or sub-block) in this block.
    pub ent_count: usize,
    /// Which entry we will next read, or -1 if block isn't loaded yet.
    pub next_ent: i32,

    /// Remaining count in a singleton stats run.
    pub stats_singleton_run_length: i32,
    /// How many terms have had their metadata decoded so far.
    pub meta_data_upto: i32,
    /// The current term's block term state.
    pub state: IntBlockTermState,
    /// File pointer of the last sub-block seen during iteration.
    pub last_sub_fp: i64,

    /// The trie node at this frame's depth.
    pub node: Option<Node>,
    /// Start byte position of the current entry's suffix in `suffix_bytes`.
    pub start_byte_pos: usize,
    /// Length of the current entry's suffix.
    pub suffix_length: usize,
}

#[expect(dead_code)]
impl SegmentTermsEnumFrame {
    /// Creates a new unloaded frame at the given stack position.
    pub fn new(ord: i32) -> Self {
        Self {
            ord,
            pos: BlockPosition::default(),
            flags: BlockFlags::default(),
            data: BlockData::default(),
            floor: FloorState::default(),
            prefix_length: 0,
            ent_count: 0,
            next_ent: -1,
            stats_singleton_run_length: 0,
            meta_data_upto: 0,
            state: IntBlockTermState::new(),
            last_sub_fp: -1,
            node: None,
            start_byte_pos: 0,
            suffix_length: 0,
        }
    }

    /// Returns the current term block ordinal.
    pub fn get_term_block_ord(&self) -> i32 {
        if self.flags.is_leaf_block {
            self.next_ent
        } else {
            self.state.term_block_ord
        }
    }

    /// Loads the next floor block.
    pub fn load_next_floor_block(&mut self, terms_in: &mut dyn IndexInput) -> io::Result<()> {
        debug_assert!(self.node.is_none() || self.flags.is_floor);
        self.pos.fp = self.pos.fp_end;
        self.next_ent = -1;
        self.load_block(terms_in)
    }

    /// Loads a term block from the `.tim` file at the current `fp`.
    ///
    /// Reads block header, suffix data, suffix lengths, stats, and metadata
    /// into owned buffers. Does not decode stats or metadata — that happens
    /// lazily via `decode_meta_data`.
    pub fn load_block(&mut self, mut terms_in: &mut dyn IndexInput) -> io::Result<()> {
        if self.next_ent != -1 {
            // Already loaded
            return Ok(());
        }

        terms_in.seek(self.pos.fp as u64)?;

        // Section 1: Block header
        let code = terms_in.read_vint()?;
        self.ent_count = (code as u32 >> 1) as usize;
        debug_assert!(self.ent_count > 0);
        self.flags.is_last_in_floor = (code & 1) != 0;

        debug_assert!(
            self.node.is_none() || self.flags.is_last_in_floor || self.flags.is_floor,
            "fp={} node={:?} is_floor={} is_last_in_floor={}",
            self.pos.fp,
            self.node,
            self.flags.is_floor,
            self.flags.is_last_in_floor,
        );

        // Section 2: Suffix data
        let start_suffix_fp = terms_in.file_pointer();
        let code_l = terms_in.read_vlong()?;
        self.flags.is_leaf_block = (code_l & 0x04) != 0;
        let num_suffix_bytes = (code_l as u64 >> 3) as usize;
        let compression_code = (code_l & 0x03) as u32;

        self.data.suffix_bytes = read_compressed(terms_in, num_suffix_bytes, compression_code)?;
        self.data.suffix_pos = 0;

        // Section 3: Suffix lengths
        let num_suffix_length_bytes_token = terms_in.read_vint()?;
        self.flags.all_equal = (num_suffix_length_bytes_token & 1) != 0;
        let num_suffix_length_bytes = (num_suffix_length_bytes_token as u32 >> 1) as usize;

        if self.flags.all_equal {
            let common = terms_in.read_byte()?;
            self.data.suffix_length_bytes = vec![common; num_suffix_length_bytes];
        } else {
            self.data.suffix_length_bytes = vec![0u8; num_suffix_length_bytes];
            terms_in.read_exact(&mut self.data.suffix_length_bytes)?;
        }
        self.data.suffix_length_pos = 0;
        self.data.total_suffix_bytes = terms_in.file_pointer() as i64 - start_suffix_fp as i64;

        // Section 4: Stats
        let num_stat_bytes = terms_in.read_vint()? as usize;
        self.data.stat_bytes = vec![0u8; num_stat_bytes];
        terms_in.read_exact(&mut self.data.stat_bytes)?;
        self.stats_singleton_run_length = 0;
        self.meta_data_upto = 0;

        self.state.term_block_ord = 0;
        self.next_ent = 0;
        self.last_sub_fp = -1;

        // Section 5: Metadata
        let num_meta_bytes = terms_in.read_vint()? as usize;
        self.data.meta_bytes = vec![0u8; num_meta_bytes];
        terms_in.read_exact(&mut self.data.meta_bytes)?;

        // fp_end = position after all block data
        self.pos.fp_end = terms_in.file_pointer() as i64;

        Ok(())
    }

    /// Rewinds this frame to its original state, forcing a reload on next access.
    pub fn rewind(&mut self) -> io::Result<()> {
        self.pos.fp = self.pos.fp_orig;
        self.next_ent = -1;
        self.flags.has_terms = self.flags.has_terms_orig;
        if self.flags.is_floor {
            self.floor.rewind()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::codecs::competitive_impact::BufferedNormsLookup;
    use crate::codecs::lucene103::blocktree_reader::BlockTreeTermsReader;
    use crate::codecs::lucene103::blocktree_writer::{BlockTreeTermsWriter, BufferedFieldTerms};
    use crate::document::{DocValuesType, IndexOptions, TermOffset};
    use crate::index::pipeline::terms_hash::{FreqProxTermsWriterPerField, TermsHash};
    use crate::index::{FieldInfo, FieldInfos, PointDimensionConfig};
    use crate::store::memory::MemoryDirectory;
    use crate::store::{Directory, SharedDirectory};
    use crate::util::byte_block_pool::ByteBlockPool;
    use assertables::*;

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

    struct TestTerms {
        writer: FreqProxTermsWriterPerField,
        term_pool: ByteBlockPool,
        terms_hash: TermsHash,
    }

    impl TestTerms {
        fn new(field_name: &str, index_options: IndexOptions) -> Self {
            let term_pool = ByteBlockPool::new(32 * 1024);
            Self {
                writer: FreqProxTermsWriterPerField::new(field_name.to_string(), index_options),
                term_pool,
                terms_hash: TermsHash::new(),
            }
        }

        fn add(&mut self, term: &str, doc_id: i32) {
            self.writer.current_position = 0;
            self.writer.current_offset = TermOffset::default();
            self.writer
                .add(
                    &mut self.term_pool,
                    &mut self.terms_hash,
                    term.as_bytes(),
                    doc_id,
                )
                .unwrap();
        }

        fn finalize(&mut self) {
            self.writer.flush_pending_docs(&mut self.terms_hash);
            self.writer.sort_terms(&self.term_pool);
        }
    }

    fn write_terms(
        terms: &[(&str, &[i32])],
        index_options: IndexOptions,
    ) -> io::Result<(SharedDirectory, FieldInfos, [u8; 16])> {
        let field_infos = FieldInfos::new(vec![make_field_info("f", 0, index_options)]);
        let segment_id = [0u8; 16];
        let shared_dir = MemoryDirectory::create();

        {
            let mut writer =
                BlockTreeTermsWriter::new(&shared_dir, "_0", "", &segment_id, index_options)?;

            let mut tt = TestTerms::new("f", index_options);
            // Add in doc-major order
            let max_doc = terms
                .iter()
                .flat_map(|(_, docs)| docs.iter())
                .copied()
                .max()
                .unwrap_or(-1);
            for doc_id in 0..=max_doc {
                for (term, doc_ids) in terms {
                    if doc_ids.contains(&doc_id) {
                        tt.add(term, doc_id);
                    }
                }
            }
            tt.finalize();

            let field_terms =
                BufferedFieldTerms::new(&tt.writer, &tt.term_pool, &tt.terms_hash, "f", 0);
            let norms = BufferedNormsLookup::no_norms();
            writer.write_field(&field_terms, &norms)?;
            writer.finish()?;
        }

        Ok((shared_dir, field_infos, segment_id))
    }

    fn open_reader(
        dir: &dyn Directory,
        field_infos: &FieldInfos,
        segment_id: &[u8; 16],
    ) -> BlockTreeTermsReader {
        BlockTreeTermsReader::open(dir, "_0", "", segment_id, field_infos).unwrap()
    }

    // --- FloorState tests ---

    // Floor data: VInt(num_follow_blocks) then byte(next_label).
    // VInt 3 = [0x03], label 'a' = [0x61].
    fn sample_floor_data() -> Vec<u8> {
        vec![0x03, 0x61]
    }

    #[test]
    fn test_floor_state_set() {
        let mut floor = FloorState::default();
        floor.set(&sample_floor_data()).unwrap();

        assert_eq!(floor.num_follow_blocks, 3);
        assert_eq!(floor.next_label, 0x61);
        assert_eq!(floor.rewind_pos, 0);
        assert_gt!(floor.data_pos, 0);
    }

    #[test]
    fn test_floor_state_rewind() {
        let mut floor = FloorState::default();
        floor.set(&sample_floor_data()).unwrap();

        // Mutate state to simulate iteration
        floor.num_follow_blocks = 0;
        floor.next_label = 0xFF;

        floor.rewind().unwrap();
        assert_eq!(floor.num_follow_blocks, 3);
        assert_eq!(floor.next_label, 0x61);
    }

    #[test]
    fn test_frame_new_defaults() {
        let frame = SegmentTermsEnumFrame::new(5);

        assert_eq!(frame.ord, 5);
        assert_eq!(frame.next_ent, -1);
        assert_eq!(frame.ent_count, 0);
        assert_eq!(frame.pos.fp, 0);
        assert_eq!(frame.pos.fp_orig, 0);
        assert_eq!(frame.pos.fp_end, 0);
        assert_eq!(frame.last_sub_fp, -1);
        assert_none!(frame.node);
    }

    #[test]
    fn test_get_term_block_ord_leaf() {
        let mut frame = SegmentTermsEnumFrame::new(0);
        frame.flags.is_leaf_block = true;
        frame.next_ent = 7;
        frame.state.term_block_ord = 99;

        assert_eq!(frame.get_term_block_ord(), 7);
    }

    #[test]
    fn test_get_term_block_ord_non_leaf() {
        let mut frame = SegmentTermsEnumFrame::new(0);
        frame.flags.is_leaf_block = false;
        frame.next_ent = 7;
        frame.state.term_block_ord = 3;

        assert_eq!(frame.get_term_block_ord(), 3);
    }

    #[test]
    fn test_frame_rewind() {
        let mut frame = SegmentTermsEnumFrame::new(0);
        frame.pos.fp_orig = 42;
        frame.pos.fp = 100;
        frame.next_ent = 5;
        frame.flags.has_terms_orig = true;
        frame.flags.has_terms = false;
        frame.flags.is_floor = false;

        frame.rewind().unwrap();

        assert_eq!(frame.pos.fp, 42);
        assert_eq!(frame.next_ent, -1);
        assert!(frame.flags.has_terms);
    }

    #[test]
    fn test_frame_rewind_with_floor() {
        let mut frame = SegmentTermsEnumFrame::new(0);
        frame.pos.fp_orig = 42;
        frame.pos.fp = 100;
        frame.flags.is_floor = true;
        frame.flags.has_terms_orig = true;
        frame.floor.set(&sample_floor_data()).unwrap();

        // Mutate floor state
        frame.floor.num_follow_blocks = 0;
        frame.floor.next_label = 0xFF;

        frame.rewind().unwrap();

        assert_eq!(frame.pos.fp, 42);
        assert_eq!(frame.floor.num_follow_blocks, 3);
        assert_eq!(frame.floor.next_label, 0x61);
    }

    // --- load_block tests ---

    #[test]
    fn test_load_block_small() {
        let terms = vec![("alpha", &[0][..]), ("beta", &[1]), ("gamma", &[2])];
        let (dir, fi, id) = write_terms(&terms, IndexOptions::Docs).unwrap();

        let reader = open_reader(&dir, &fi, &id);
        let fr = reader.field_reader(0).unwrap();

        let terms_in = reader.terms_in();
        let mut terms_in = terms_in.slice("test", 0, terms_in.length()).unwrap();
        let trie = fr.new_trie_reader().unwrap();
        let trie_result = trie.seek_to_block(b"alpha").unwrap().unwrap();

        let mut frame = SegmentTermsEnumFrame::new(0);
        frame.pos.fp = trie_result.output_fp;
        frame.load_block(terms_in.as_mut()).unwrap();

        assert_eq!(frame.ent_count, 3);
        assert!(frame.flags.is_leaf_block);
        assert_eq!(frame.next_ent, 0);
        assert!(frame.pos.fp_end > frame.pos.fp);
    }

    #[test]
    fn test_load_block_many_terms() {
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..100 {
            terms_data.push((format!("term_{i:04}"), vec![i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();
        let (dir, fi, id) = write_terms(&terms, IndexOptions::Docs).unwrap();

        let reader = open_reader(&dir, &fi, &id);
        let fr = reader.field_reader(0).unwrap();

        let terms_in = reader.terms_in();
        let mut terms_in = terms_in.slice("test", 0, terms_in.length()).unwrap();
        let trie = fr.new_trie_reader().unwrap();
        let trie_result = trie.seek_to_block(b"term_0000").unwrap().unwrap();

        let mut frame = SegmentTermsEnumFrame::new(0);
        frame.pos.fp = trie_result.output_fp;
        frame.load_block(terms_in.as_mut()).unwrap();

        assert!(frame.ent_count > 0);
        assert_eq!(frame.next_ent, 0);
        assert!(frame.pos.fp_end > frame.pos.fp);
    }

    #[test]
    fn test_load_block_idempotent() {
        let terms = vec![("hello", &[0][..]), ("world", &[1])];
        let (dir, fi, id) = write_terms(&terms, IndexOptions::Docs).unwrap();

        let reader = open_reader(&dir, &fi, &id);
        let fr = reader.field_reader(0).unwrap();

        let terms_in = reader.terms_in();
        let mut terms_in = terms_in.slice("test", 0, terms_in.length()).unwrap();
        let trie = fr.new_trie_reader().unwrap();
        let trie_result = trie.seek_to_block(b"hello").unwrap().unwrap();

        let mut frame = SegmentTermsEnumFrame::new(0);
        frame.pos.fp = trie_result.output_fp;
        frame.load_block(terms_in.as_mut()).unwrap();

        let ent_count = frame.ent_count;
        let fp_end = frame.pos.fp_end;

        // Loading again should be a no-op (next_ent != -1)
        frame.load_block(terms_in.as_mut()).unwrap();
        assert_eq!(frame.ent_count, ent_count);
        assert_eq!(frame.pos.fp_end, fp_end);
    }
}
