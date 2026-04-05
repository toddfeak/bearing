// SPDX-License-Identifier: Apache-2.0

//! Term block parser and stateful iterator for the Lucene 103 block tree
//! terms dictionary.
//!
//! [`SegmentTermsEnum`] implements [`TermsEnum`] by navigating the trie in the
//! `.tip` file and scanning term blocks in the `.tim` file.
//!
//! Internal helpers parse `.tim` term blocks found by
//! [`super::trie_reader::TrieReader`] and scan within them to find exact terms
//! and decode their metadata ([`IntBlockTermState`]).

use std::cmp::Ordering;
use std::io;

use crate::codecs::lucene103::postings_format::IntBlockTermState;
use crate::codecs::lucene103::trie_reader::TrieReader;
use crate::document::IndexOptions;
use crate::encoding::{lowercase_ascii, lz4, pfor, zigzag};
use crate::index::terms::TermsEnum;
use crate::store::{DataInput, DataInputReader, IndexInput};

const COMPRESSION_NONE: u32 = 0;
const COMPRESSION_LOWERCASE_ASCII: u32 = 1;
const COMPRESSION_LZ4: u32 = 2;

/// Stateful iterator over terms in a single field of a single segment.
///
/// Wraps trie navigation + block scanning into a [`TermsEnum`] implementation.
/// Created by `FieldReader::iterator()` (via the [`Terms`](crate::index::terms::Terms) trait).
pub struct SegmentTermsEnum {
    /// Clone of the `.tim` terms dictionary input.
    terms_in: Box<dyn IndexInput>,
    /// Clone of the `.tip` index input (for floor data reads).
    index_in: Box<dyn IndexInput>,
    /// Trie navigator for this field.
    trie: TrieReader,
    /// Index options for this field.
    index_options: IndexOptions,
    /// The current term after a successful seek.
    current_term: Vec<u8>,
    /// The current term's metadata after a successful seek.
    current_state: Option<IntBlockTermState>,
}

impl SegmentTermsEnum {
    /// Creates a new `SegmentTermsEnum` for a field.
    pub fn new(
        terms_in: Box<dyn IndexInput>,
        index_in: Box<dyn IndexInput>,
        trie: TrieReader,
        index_options: IndexOptions,
    ) -> Self {
        Self {
            terms_in,
            index_in,
            trie,
            index_options,
            current_term: Vec::new(),
            current_state: None,
        }
    }
}

impl TermsEnum for SegmentTermsEnum {
    fn seek_exact(&mut self, target: &[u8]) -> io::Result<bool> {
        let trie_result = match self.trie.seek_to_block(target)? {
            Some(r) => r,
            None => {
                self.current_state = None;
                return Ok(false);
            }
        };

        let state = seek_exact_in_block(
            &*self.terms_in,
            &trie_result,
            target,
            self.index_options,
            &*self.index_in,
        )?;

        match state {
            Some(s) => {
                self.current_term.clear();
                self.current_term.extend_from_slice(target);
                self.current_state = Some(s);
                Ok(true)
            }
            None => {
                self.current_state = None;
                Ok(false)
            }
        }
    }

    fn seek_exact_with_state(&mut self, term: &[u8], state: IntBlockTermState) {
        self.current_term.clear();
        self.current_term.extend_from_slice(term);
        self.current_state = Some(state);
    }

    fn term(&self) -> &[u8] {
        &self.current_term
    }

    fn doc_freq(&self) -> io::Result<i32> {
        match &self.current_state {
            Some(s) => Ok(s.doc_freq),
            None => Err(io::Error::other("TermsEnum not positioned")),
        }
    }

    fn total_term_freq(&self) -> io::Result<i64> {
        match &self.current_state {
            Some(s) => Ok(s.total_term_freq),
            None => Err(io::Error::other("TermsEnum not positioned")),
        }
    }

    fn term_state(&self) -> io::Result<IntBlockTermState> {
        match &self.current_state {
            Some(s) => Ok(*s),
            None => Err(io::Error::other("TermsEnum not positioned")),
        }
    }
}

/// Seeks to an exact term in the `.tim` file and returns its metadata.
fn seek_exact_in_block(
    terms_in: &dyn IndexInput,
    trie_result: &super::trie_reader::TrieSeekResult,
    target: &[u8],
    index_options: IndexOptions,
    index_in: &dyn IndexInput,
) -> io::Result<Option<IntBlockTermState>> {
    let prefix_length = trie_result.depth;
    let mut block_fp = trie_result.output_fp;

    // Handle floor blocks: find the right sub-block for the target's next byte
    if trie_result.floor_data_fp >= 0 && target.len() > prefix_length {
        let target_label = target[prefix_length];
        block_fp = scan_to_floor_block(
            index_in,
            trie_result.floor_data_fp,
            trie_result.output_fp,
            target_label,
        )?;
    }

    let mut input = terms_in.slice("seek_exact", 0, terms_in.length())?;
    input.seek(block_fp as u64)?;

    let result = scan_block(input.as_mut(), target, prefix_length, index_options)?;
    match result {
        ScanResult::Found(state) => Ok(Some(state)),
        ScanResult::NotFound => Ok(None),
    }
}

/// Scans floor data from `.tip` to find the block FP for the given target label.
///
/// Floor data format (written by blocktree_writer):
/// ```text
/// VInt(num_follow_blocks)
/// For each follow-on block:
///     byte(floor_lead_byte)
///     VLong((fp_delta << 1) | has_terms)
/// ```
fn scan_to_floor_block(
    index_in: &dyn IndexInput,
    floor_data_fp: i64,
    base_fp: i64,
    target_label: u8,
) -> io::Result<i64> {
    let mut input = index_in.slice("floor_data", 0, index_in.length())?;
    input.seek(floor_data_fp as u64)?;

    let num_follow_blocks = input.read_vint()?;
    let mut result_fp = base_fp; // Start with the first block

    for i in 0..num_follow_blocks {
        let floor_lead_byte = input.read_byte()?;
        let code = input.read_vlong()?;
        let fp_delta = code >> 1;
        let fp = base_fp + fp_delta;

        if target_label < floor_lead_byte {
            // Target is before this floor block — use the previous block
            break;
        }

        result_fp = fp;

        if i == num_follow_blocks - 1 {
            // Last floor block — use it
            break;
        }
    }

    Ok(result_fp)
}

enum ScanResult {
    Found(IntBlockTermState),
    NotFound,
}

/// Loads and scans a single term block from the current position.
fn scan_block(
    input: &mut dyn DataInput,
    target: &[u8],
    prefix_length: usize,
    index_options: IndexOptions,
) -> io::Result<ScanResult> {
    // Section 1: Block header
    let code = input.read_vint()?;
    let entry_count = (code >> 1) as usize;
    let _is_last_in_floor = (code & 1) != 0;

    // Section 2: Suffix data
    let suffix_token = input.read_vlong()?;
    let is_leaf_block = (suffix_token & 0x04) != 0;
    let num_suffix_bytes = (suffix_token >> 3) as usize;
    let compression_code = (suffix_token & 0x03) as u32;

    let suffix_bytes = read_compressed(input, num_suffix_bytes, compression_code)?;

    // Section 3: Suffix lengths
    let suffix_lengths_token = input.read_vint()?;
    let all_equal = (suffix_lengths_token & 1) != 0;
    let num_suffix_length_bytes = (suffix_lengths_token >> 1) as usize;

    let suffix_length_bytes = if all_equal {
        let common = input.read_byte()?;
        vec![common; num_suffix_length_bytes]
    } else {
        let mut buf = vec![0u8; num_suffix_length_bytes];
        input.read_bytes(&mut buf)?;
        buf
    };

    // Section 4: Stats
    let num_stats_bytes = input.read_vint()? as usize;
    let mut stats_bytes = vec![0u8; num_stats_bytes];
    input.read_bytes(&mut stats_bytes)?;

    // Section 5: Metadata
    let num_meta_bytes = input.read_vint()? as usize;
    let mut meta_bytes = vec![0u8; num_meta_bytes];
    input.read_bytes(&mut meta_bytes)?;

    // Now scan suffixes to find the target
    let target_suffix = &target[prefix_length..];

    let mut suffix_reader = SliceReader::new(&suffix_bytes);
    let mut suffix_lengths_reader = SliceReader::new(&suffix_length_bytes);

    // Track which term index we found (for stats/metadata decoding)
    let mut term_ord = 0usize;

    for _entry_idx in 0..entry_count {
        let (suffix_len, is_sub_block) = if is_leaf_block {
            let len = suffix_lengths_reader.read_vint()? as usize;
            (len, false)
        } else {
            let code = suffix_lengths_reader.read_vint()?;
            let len = (code >> 1) as usize;
            let is_sub = (code & 1) != 0;
            if is_sub {
                // Read and discard sub-block FP delta
                suffix_lengths_reader.read_vlong()?;
            }
            (len, is_sub)
        };

        let suffix_start = suffix_reader.pos;
        suffix_reader.skip(suffix_len);

        if is_sub_block {
            // Sub-block entry — skip (we don't recurse into sub-blocks for seekExact)
            continue;
        }

        // Compare suffix with target
        let suffix = &suffix_bytes[suffix_start..suffix_start + suffix_len];
        let cmp = suffix.cmp(target_suffix);

        match cmp {
            Ordering::Equal => {
                // Found it! Decode stats and metadata up to term_ord
                let state = decode_term_state(&stats_bytes, &meta_bytes, term_ord, index_options)?;
                return Ok(ScanResult::Found(state));
            }
            Ordering::Greater => {
                // Past the target — term doesn't exist in this block
                return Ok(ScanResult::NotFound);
            }
            Ordering::Less => {
                // Haven't reached the target yet — advance term_ord
                term_ord += 1;
            }
        }
    }

    // Exhausted all entries without finding the target
    Ok(ScanResult::NotFound)
}

/// Decode stats and metadata for the Nth term in a block.
fn decode_term_state(
    stats_bytes: &[u8],
    meta_bytes: &[u8],
    target_ord: usize,
    index_options: IndexOptions,
) -> io::Result<IntBlockTermState> {
    let mut stats_reader = SliceReader::new(stats_bytes);
    let mut meta_reader = SliceReader::new(meta_bytes);
    let has_freqs = index_options.has_freqs();
    let has_positions = index_options.has_positions();

    let mut state = IntBlockTermState::new();
    let mut last_state = IntBlockTermState::new();
    let mut singleton_run = 0i32;

    // Decode stats and metadata up to and including target_ord
    for ord in 0..=target_ord {
        // Stats decoding (with singleton RLE)
        if singleton_run > 0 {
            state.doc_freq = 1;
            state.total_term_freq = 1;
            singleton_run -= 1;
        } else {
            let token = stats_reader.read_vint()?;
            if (token & 1) == 1 {
                // Singleton run
                state.doc_freq = 1;
                state.total_term_freq = 1;
                singleton_run = token >> 1;
            } else {
                state.doc_freq = token >> 1;
                if !has_freqs {
                    state.total_term_freq = state.doc_freq as i64;
                } else {
                    state.total_term_freq = state.doc_freq as i64 + stats_reader.read_vlong()?;
                }
            }
        }

        // Metadata decoding (reverse of PostingsWriter::encode_term)
        let empty_state = IntBlockTermState::new();
        let ref_state = if ord == 0 { &empty_state } else { &last_state };
        decode_term_meta(&mut meta_reader, &mut state, ref_state, has_positions)?;

        if ord < target_ord {
            last_state = state;
        }
    }

    Ok(state)
}

/// Decode one term's postings metadata from the metadata bytes.
fn decode_term_meta(
    reader: &mut SliceReader,
    state: &mut IntBlockTermState,
    last_state: &IntBlockTermState,
    has_positions: bool,
) -> io::Result<()> {
    let code = reader.read_vlong()?;
    if (code & 1) != 0 {
        // Singleton-singleton delta: both are singletons at same doc_start_fp
        let encoded = code >> 1;
        let delta = zigzag::decode_i64(encoded);
        state.singleton_doc_id = (last_state.singleton_doc_id as i64 + delta) as i32;
        state.doc_start_fp = last_state.doc_start_fp;
    } else {
        // Normal: file pointer delta
        let fp_delta = code >> 1;
        state.doc_start_fp = last_state.doc_start_fp + fp_delta;
        if state.doc_freq == 1 {
            state.singleton_doc_id = reader.read_vint()?;
        } else {
            state.singleton_doc_id = -1;
        }
    }

    if has_positions {
        state.pos_start_fp = last_state.pos_start_fp + reader.read_vlong()?;
        if state.total_term_freq > pfor::BLOCK_SIZE as i64 {
            state.last_pos_block_offset = reader.read_vlong()?;
        } else {
            state.last_pos_block_offset = -1;
        }
    }

    Ok(())
}

/// Read and decompress suffix bytes.
fn read_compressed(
    input: &mut dyn DataInput,
    uncompressed_len: usize,
    compression_code: u32,
) -> io::Result<Vec<u8>> {
    match compression_code {
        COMPRESSION_NONE => {
            let mut buf = vec![0u8; uncompressed_len];
            input.read_bytes(&mut buf)?;
            Ok(buf)
        }
        COMPRESSION_LZ4 => {
            let mut reader = DataInputReader(input);
            Ok(lz4::decompress_from_reader(&mut reader, uncompressed_len)?)
        }
        COMPRESSION_LOWERCASE_ASCII => {
            let mut reader = DataInputReader(input);
            Ok(lowercase_ascii::decompress_from_reader(
                &mut reader,
                uncompressed_len,
            )?)
        }
        _ => Err(io::Error::other(format!(
            "unknown compression code: {compression_code}"
        ))),
    }
}

/// Simple reader over a byte slice with VInt/VLong support.
struct SliceReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> SliceReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn skip(&mut self, n: usize) {
        self.pos += n;
    }

    fn read_vint(&mut self) -> io::Result<i32> {
        DataInput::read_vint(self)
    }

    fn read_vlong(&mut self) -> io::Result<i64> {
        DataInput::read_vlong(self)
    }
}

impl DataInput for SliceReader<'_> {
    fn read_byte(&mut self) -> io::Result<u8> {
        if self.pos >= self.data.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of input"));
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let end = self.pos + buf.len();
        if end > self.data.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of input"));
        }
        buf.copy_from_slice(&self.data[self.pos..end]);
        self.pos = end;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::competitive_impact::NormsLookup;
    use crate::codecs::lucene103::blocktree_reader::BlockTreeTermsReader;
    use crate::codecs::lucene103::blocktree_writer::{BlockTreeTermsWriter, FieldWriteContext};
    use crate::document::DocValuesType;
    use crate::index::pipeline::terms_hash::{FreqProxTermsWriterPerField, TermsHash};
    use crate::index::{FieldInfo, FieldInfos, PointDimensionConfig};
    use crate::store::memory::MemoryDirectory;
    use crate::store::{Directory, SharedDirectory};
    use crate::util::byte_block_pool::{ByteBlockPool, DirectAllocator};
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
        term_pool: ByteBlockPool<DirectAllocator>,
        terms_hash: TermsHash,
    }

    impl TestTerms {
        fn new(field_name: &str, index_options: IndexOptions) -> Self {
            let mut term_pool = ByteBlockPool::new(DirectAllocator);
            term_pool.next_buffer();
            Self {
                writer: FreqProxTermsWriterPerField::new(field_name.to_string(), index_options),
                term_pool,
                terms_hash: TermsHash::new(),
            }
        }

        fn add(&mut self, term: &str, doc_id: i32, position: i32) {
            self.writer.current_position = position;
            self.writer.current_start_offset = 0;
            self.writer.current_end_offset = 0;
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

    /// Add terms in doc-major order from term-major test data.
    fn add_terms_doc_major(tt: &mut TestTerms, terms: &[(&str, &[i32])]) {
        let max_doc = terms
            .iter()
            .flat_map(|(_, docs)| docs.iter())
            .copied()
            .max()
            .unwrap_or(-1);
        for doc_id in 0..=max_doc {
            for (term, doc_ids) in terms {
                if doc_ids.contains(&doc_id) {
                    tt.add(term, doc_id, 0);
                }
            }
        }
    }

    /// Write terms and return (directory, field_infos, segment_id).
    fn write_terms(
        terms: Vec<(&str, &[i32])>,
        index_options: IndexOptions,
    ) -> io::Result<(Box<dyn Directory>, FieldInfos, [u8; 16])> {
        let field_infos = FieldInfos::new(vec![make_field_info("f", 0, index_options)]);
        let segment_name = "_0";
        let segment_suffix = "";
        let segment_id = [0u8; 16];

        let shared_dir = SharedDirectory::new(Box::new(MemoryDirectory::new()));

        {
            let has_positions = index_options.has_positions();
            let mut writer = BlockTreeTermsWriter::new(
                &shared_dir,
                segment_name,
                segment_suffix,
                &segment_id,
                has_positions,
            )?;

            let mut tt = TestTerms::new("f", index_options);
            add_terms_doc_major(&mut tt, &terms);
            tt.finalize();

            let ctx = FieldWriteContext {
                field_name: "f".to_string(),
                field_number: 0,
                write_freqs: index_options.has_freqs(),
                write_positions: index_options.has_positions(),
            };
            let norms = NormsLookup::no_norms();
            writer.write_field(&ctx, &tt.writer, &tt.term_pool, &tt.terms_hash, &norms)?;

            writer.finish()?;
        }

        let dir = shared_dir.into_inner().unwrap();
        Ok((dir, field_infos, segment_id))
    }

    /// Open the blocktree reader and seek for a term via TermsEnum.
    fn seek_term(
        dir: &dyn Directory,
        field_infos: &FieldInfos,
        segment_id: &[u8; 16],
        term: &[u8],
    ) -> io::Result<Option<IntBlockTermState>> {
        let reader = BlockTreeTermsReader::open(dir, "_0", "", segment_id, field_infos)?;
        let fr = reader.field_reader(0).unwrap();
        let mut terms_enum = fr.iterator()?;
        if terms_enum.seek_exact(term)? {
            Ok(Some(terms_enum.term_state()?))
        } else {
            Ok(None)
        }
    }

    #[test]
    fn test_seek_exact_singleton_term() {
        let terms = vec![("hello", &[5][..]), ("world", &[10])];
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        let state = seek_term(dir.as_ref(), &fi, &id, b"hello")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.singleton_doc_id, 5);

        let state = seek_term(dir.as_ref(), &fi, &id, b"world")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.singleton_doc_id, 10);
    }

    #[test]
    fn test_seek_exact_multi_doc_term_small() {
        let terms = vec![("hello", &[5, 6][..]), ("world", &[10, 11])];
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        let state = seek_term(dir.as_ref(), &fi, &id, b"hello")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 2);

        let state = seek_term(dir.as_ref(), &fi, &id, b"world")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 2);
    }

    #[test]
    fn test_seek_exact_multi_doc_term() {
        let terms = vec![
            ("alpha", &[0, 1, 2][..]),
            ("beta", &[1, 3]),
            ("gamma", &[0, 2, 4]),
        ];
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        let state = seek_term(dir.as_ref(), &fi, &id, b"alpha")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 3);
        assert_eq!(state.singleton_doc_id, -1);

        let state = seek_term(dir.as_ref(), &fi, &id, b"beta").unwrap().unwrap();
        assert_eq!(state.doc_freq, 2);

        let state = seek_term(dir.as_ref(), &fi, &id, b"gamma")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 3);
    }

    #[test]
    fn test_seek_exact_nonexistent_term() {
        let terms = vec![("alpha", &[0][..]), ("gamma", &[1])];
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        let result = seek_term(dir.as_ref(), &fi, &id, b"beta").unwrap();
        assert_none!(&result);

        let result = seek_term(dir.as_ref(), &fi, &id, b"zzz").unwrap();
        assert_none!(&result);
    }

    #[test]
    fn test_seek_exact_with_freqs() {
        let terms = vec![("hello", &[0, 1, 2][..]), ("world", &[0])];
        let (dir, fi, id) = write_terms(terms, IndexOptions::DocsAndFreqs).unwrap();

        let state = seek_term(dir.as_ref(), &fi, &id, b"hello")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 3);
        assert_ge!(state.total_term_freq, 3);
    }

    #[test]
    fn test_seek_exact_many_terms() {
        // Generate enough terms to force multi-block trie navigation
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..100 {
            terms_data.push((format!("term_{i:04}"), vec![i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        // Seek each term
        for i in 0..100 {
            let term = format!("term_{i:04}");
            let state = seek_term(dir.as_ref(), &fi, &id, term.as_bytes()).unwrap();
            assert_some!(&state);
            assert_eq!(state.unwrap().doc_freq, 1);
        }

        // Nonexistent term
        let result = seek_term(dir.as_ref(), &fi, &id, b"term_9999").unwrap();
        assert_none!(&result);
    }

    #[test]
    fn test_seek_exact_singleton_rle_run() {
        // Multiple consecutive singletons exercise the RLE decoding path.
        let term_list = [
            "aardvark", "badger", "cat", "dog", "elephant", "fox", "giraffe", "hippo", "iguana",
            "jaguar",
        ];
        let doc_ids: Vec<Vec<i32>> = (0..10).map(|i| vec![i]).collect();
        let terms: Vec<(&str, &[i32])> = term_list
            .iter()
            .zip(doc_ids.iter())
            .map(|(&t, d)| (t, d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        // First term in the block
        let state = seek_term(dir.as_ref(), &fi, &id, b"aardvark")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.singleton_doc_id, 0);

        // Middle term (exercises skipping through singleton RLE)
        let state = seek_term(dir.as_ref(), &fi, &id, b"fox").unwrap().unwrap();
        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.singleton_doc_id, 5);

        // Last term in the block
        let state = seek_term(dir.as_ref(), &fi, &id, b"jaguar")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.singleton_doc_id, 9);
    }

    #[test]
    fn test_seek_exact_mixed_singleton_and_multi_doc() {
        // Mix of singleton (df=1) and multi-doc (df>1) terms in the same block.
        let terms = vec![
            ("alpha", &[0][..]),      // singleton
            ("beta", &[0, 1, 2][..]), // multi-doc
            ("gamma", &[3][..]),      // singleton
            ("delta", &[4, 5][..]),   // multi-doc
        ];
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        let state = seek_term(dir.as_ref(), &fi, &id, b"alpha")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.singleton_doc_id, 0);

        let state = seek_term(dir.as_ref(), &fi, &id, b"beta").unwrap().unwrap();
        assert_eq!(state.doc_freq, 3);
        assert_eq!(state.singleton_doc_id, -1);

        let state = seek_term(dir.as_ref(), &fi, &id, b"delta")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 2);
        assert_eq!(state.singleton_doc_id, -1);

        let state = seek_term(dir.as_ref(), &fi, &id, b"gamma")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.singleton_doc_id, 3);
    }

    #[test]
    fn test_seek_exact_variable_length_suffixes() {
        // Terms with different suffix lengths exercise the allEqual=false path
        // in suffix length decoding.
        let terms = vec![
            ("a", &[0][..]),
            ("bb", &[1]),
            ("ccc", &[2]),
            ("dddddddddd", &[3]),
        ];
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        for (term, doc) in [("a", 0), ("bb", 1), ("ccc", 2), ("dddddddddd", 3)] {
            let state = seek_term(dir.as_ref(), &fi, &id, term.as_bytes())
                .unwrap()
                .unwrap();
            assert_eq!(state.doc_freq, 1);
            assert_eq!(state.singleton_doc_id, doc);
        }
    }

    #[test]
    fn test_seek_exact_with_positions() {
        let terms = vec![("hello", &[0, 1][..]), ("world", &[0])];
        let (dir, fi, id) = write_terms(terms, IndexOptions::DocsAndFreqsAndPositions).unwrap();

        let state = seek_term(dir.as_ref(), &fi, &id, b"hello")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 2);
        assert_ge!(state.pos_start_fp, 0);

        let state = seek_term(dir.as_ref(), &fi, &id, b"world")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 1);
    }

    #[test]
    fn test_seek_exact_floor_blocks_different_targets() {
        // 100 terms split across floor blocks. Verify terms in different floor
        // sub-blocks are all found.
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..100 {
            terms_data.push((format!("term_{i:04}"), vec![i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        // First term
        let state = seek_term(dir.as_ref(), &fi, &id, b"term_0000")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 1);

        // Last term
        let state = seek_term(dir.as_ref(), &fi, &id, b"term_0099")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 1);

        // Term in the middle (likely different floor block than first/last)
        let state = seek_term(dir.as_ref(), &fi, &id, b"term_0050")
            .unwrap()
            .unwrap();
        assert_eq!(state.doc_freq, 1);

        // Before first
        let result = seek_term(dir.as_ref(), &fi, &id, b"term_").unwrap();
        assert_none!(&result);

        // After last
        let result = seek_term(dir.as_ref(), &fi, &id, b"term_0100").unwrap();
        assert_none!(&result);
    }

    #[test]
    fn test_seek_exact_compressed_suffixes() {
        // Long lowercase terms with a shared prefix > 2 chars can trigger LZ4
        // or lowercase ASCII compression. Generate terms that are likely to compress.
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..40 {
            // Long suffixes with repeating patterns to encourage compression
            let term = format!("longprefix_abcdefghij_{i:04}_suffix");
            terms_data.push((term, vec![i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        // Verify several terms are findable (compression is transparent to reader)
        for i in [0, 10, 20, 39] {
            let term = format!("longprefix_abcdefghij_{i:04}_suffix");
            let state = seek_term(dir.as_ref(), &fi, &id, term.as_bytes()).unwrap();
            assert_some!(&state);
            assert_eq!(state.unwrap().doc_freq, 1);
        }
    }

    #[test]
    fn test_seek_exact_singleton_singleton_delta() {
        // Two consecutive singletons at the same doc_start_fp exercise the
        // singleton-singleton delta path in metadata decoding.
        // When both terms are singletons (df=1) and their doc_start_fp is the
        // same (no .doc data written), the metadata uses zigzag-encoded docID delta.
        let terms = vec![("aaa", &[10][..]), ("bbb", &[20]), ("ccc", &[30])];
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        let state = seek_term(dir.as_ref(), &fi, &id, b"aaa").unwrap().unwrap();
        assert_eq!(state.singleton_doc_id, 10);

        let state = seek_term(dir.as_ref(), &fi, &id, b"bbb").unwrap().unwrap();
        assert_eq!(state.singleton_doc_id, 20);

        let state = seek_term(dir.as_ref(), &fi, &id, b"ccc").unwrap().unwrap();
        assert_eq!(state.singleton_doc_id, 30);
    }

    // --- SegmentTermsEnum (struct API) tests ---

    use crate::index::terms::Terms;

    /// Open a SegmentTermsEnum via FieldReader::iterator() for field 0.
    fn open_terms_enum(
        dir: &dyn Directory,
        field_infos: &FieldInfos,
        segment_id: &[u8; 16],
    ) -> Box<dyn TermsEnum> {
        let reader = BlockTreeTermsReader::open(dir, "_0", "", segment_id, field_infos).unwrap();
        let fr = reader.field_reader(0).unwrap();
        fr.iterator().unwrap()
    }

    #[test]
    fn test_terms_enum_seek_exact() {
        let terms = vec![("alpha", &[0, 1][..]), ("beta", &[2]), ("gamma", &[3])];
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        let mut te = open_terms_enum(dir.as_ref(), &fi, &id);
        assert!(te.seek_exact(b"alpha").unwrap());
        assert_eq!(te.term(), b"alpha");
        assert_eq!(te.doc_freq().unwrap(), 2);

        assert!(te.seek_exact(b"gamma").unwrap());
        assert_eq!(te.term(), b"gamma");
        assert_eq!(te.doc_freq().unwrap(), 1);

        assert!(!te.seek_exact(b"nonexistent").unwrap());
    }

    #[test]
    fn test_terms_enum_term_state_roundtrip() {
        let terms = vec![("hello", &[0, 1, 2][..]), ("world", &[3])];
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        let mut te = open_terms_enum(dir.as_ref(), &fi, &id);
        assert!(te.seek_exact(b"hello").unwrap());
        let state = te.term_state().unwrap();
        assert_eq!(state.doc_freq, 3);

        // Seek to a different term
        assert!(te.seek_exact(b"world").unwrap());
        assert_eq!(te.doc_freq().unwrap(), 1);

        // Restore via seek_exact_with_state
        te.seek_exact_with_state(b"hello", state);
        assert_eq!(te.term(), b"hello");
        assert_eq!(te.doc_freq().unwrap(), 3);
        assert_eq!(te.term_state().unwrap().doc_freq, 3);
    }

    #[test]
    fn test_terms_enum_total_term_freq() {
        let terms = vec![("hello", &[0, 1][..]), ("world", &[0])];
        let (dir, fi, id) = write_terms(terms, IndexOptions::DocsAndFreqs).unwrap();

        let mut te = open_terms_enum(dir.as_ref(), &fi, &id);
        assert!(te.seek_exact(b"hello").unwrap());
        assert_ge!(te.total_term_freq().unwrap(), 2);
    }

    #[test]
    fn test_terms_enum_unpositioned_errors() {
        let terms = vec![("hello", &[0][..])];
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        let te = open_terms_enum(dir.as_ref(), &fi, &id);
        // Before any seek, doc_freq and term_state should error
        assert!(te.doc_freq().is_err());
        assert!(te.term_state().is_err());
    }

    #[test]
    fn test_terms_enum_many_terms() {
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..100 {
            terms_data.push((format!("term_{i:04}"), vec![i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();
        let (dir, fi, id) = write_terms(terms, IndexOptions::Docs).unwrap();

        let mut te = open_terms_enum(dir.as_ref(), &fi, &id);
        for i in 0..100 {
            let term = format!("term_{i:04}");
            assert!(te.seek_exact(term.as_bytes()).unwrap());
            assert_eq!(te.doc_freq().unwrap(), 1);
            assert_eq!(te.term(), term.as_bytes());
        }
    }
}
