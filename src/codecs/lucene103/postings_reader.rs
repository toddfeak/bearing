// SPDX-License-Identifier: Apache-2.0

//! Postings reader for the Lucene103 postings format.
//!
//! Reads `.psm` (metadata), `.doc` (document IDs), and `.pos` (positions) files
//! written by [`super::postings_writer::PostingsWriter`]. Only metadata and file
//! headers are read during construction; posting list data is read lazily via
//! [`BlockDocIterator`].

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::lucene103::for_util::{self, BLOCK_SIZE};
use crate::codecs::lucene103::postings_format::{
    self, DOC_CODEC, DOC_EXTENSION, META_CODEC, META_EXTENSION, POS_CODEC, POS_EXTENSION,
    VERSION_CURRENT, VERSION_START,
};
use crate::index::{FieldInfos, index_file_names};
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory, IndexInput};

/// Reads postings metadata for a segment and provides access to doc ID iteration.
///
/// Opens `.psm`, `.doc`, and `.pos` files during construction. Only the `.psm`
/// metadata (impact statistics) is read eagerly; `.doc` and `.pos` file handles
/// are retained for lazy posting list reads via [`BlockDocIterator`].
pub struct PostingsReader {
    /// Open handle to the `.doc` file for reading posting lists.
    doc_in: Box<dyn IndexInput>,
    /// Maximum number of competitive impacts at skip level 0.
    max_num_impacts_at_level0: i32,
    /// Maximum bytes for encoded impacts at skip level 0.
    max_impact_num_bytes_at_level0: i32,
    /// Maximum number of competitive impacts at skip level 1.
    max_num_impacts_at_level1: i32,
    /// Maximum bytes for encoded impacts at skip level 1.
    max_impact_num_bytes_at_level1: i32,
}

impl PostingsReader {
    /// Opens postings files (`.psm`, `.doc`, `.pos`) for the given segment.
    pub fn open(
        directory: &dyn Directory,
        segment_name: &str,
        segment_suffix: &str,
        segment_id: &[u8; codec_util::ID_LENGTH],
        field_infos: &FieldInfos,
    ) -> io::Result<Self> {
        // Open .psm (metadata) with checksum validation
        let psm_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);
        let meta_input = directory.open_input(&psm_name)?;
        let mut meta_in = ChecksumIndexInput::new(meta_input);

        codec_util::check_index_header(
            &mut meta_in,
            META_CODEC,
            VERSION_START,
            VERSION_CURRENT,
            segment_id,
            segment_suffix,
        )?;

        // Read impact statistics
        let max_num_impacts_at_level0 = meta_in.read_le_int()?;
        let max_impact_num_bytes_at_level0 = meta_in.read_le_int()?;
        let max_num_impacts_at_level1 = meta_in.read_le_int()?;
        let max_impact_num_bytes_at_level1 = meta_in.read_le_int()?;

        // Read expected file lengths
        let _expected_doc_file_length = meta_in.read_le_long()?;
        if field_infos.has_prox() {
            let _expected_pos_file_length = meta_in.read_le_long()?;
        }

        codec_util::check_footer(&mut meta_in)?;

        // Validate .doc header
        let doc_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, DOC_EXTENSION);
        let mut doc_in = directory.open_input(&doc_name)?;
        codec_util::check_index_header(
            doc_in.as_mut(),
            DOC_CODEC,
            VERSION_START,
            VERSION_CURRENT,
            segment_id,
            segment_suffix,
        )?;

        // Validate .pos header if positions exist
        if field_infos.has_prox() {
            let pos_name =
                index_file_names::segment_file_name(segment_name, segment_suffix, POS_EXTENSION);
            let mut pos_in = directory.open_input(&pos_name)?;
            codec_util::check_index_header(
                pos_in.as_mut(),
                POS_CODEC,
                VERSION_START,
                VERSION_CURRENT,
                segment_id,
                segment_suffix,
            )?;
        }

        debug!(
            "postings_reader: opened for segment {segment_name}, \
             impacts=[{max_num_impacts_at_level0}, {max_impact_num_bytes_at_level0}, \
             {max_num_impacts_at_level1}, {max_impact_num_bytes_at_level1}]"
        );

        Ok(Self {
            doc_in,
            max_num_impacts_at_level0,
            max_impact_num_bytes_at_level0,
            max_num_impacts_at_level1,
            max_impact_num_bytes_at_level1,
        })
    }

    /// Returns the maximum number of competitive impacts at skip level 0.
    pub fn max_num_impacts_at_level0(&self) -> i32 {
        self.max_num_impacts_at_level0
    }

    /// Returns the maximum impact byte size at skip level 0.
    pub fn max_impact_num_bytes_at_level0(&self) -> i32 {
        self.max_impact_num_bytes_at_level0
    }

    /// Returns the maximum number of competitive impacts at skip level 1.
    pub fn max_num_impacts_at_level1(&self) -> i32 {
        self.max_num_impacts_at_level1
    }

    /// Returns the maximum impact byte size at skip level 1.
    pub fn max_impact_num_bytes_at_level1(&self) -> i32 {
        self.max_impact_num_bytes_at_level1
    }

    /// Creates a [`BlockDocIterator`] for the given term state.
    ///
    /// The returned iterator yields doc IDs sequentially. For singleton terms
    /// (docFreq=1), the doc ID is taken directly from the term metadata.
    /// For multi-doc terms, the `.doc` file is read lazily.
    pub fn block_doc_iterator(
        &self,
        term_state: &postings_format::IntBlockTermState,
        index_has_freq: bool,
    ) -> io::Result<BlockDocIterator> {
        BlockDocIterator::new(&*self.doc_in, term_state, index_has_freq)
    }
}

/// Sequential iterator over doc IDs for a single term.
///
/// Handles three encoding paths:
/// - **Singleton** (docFreq=1): doc ID pulsed in term metadata, no I/O
/// - **VInt tail** (docFreq < 128): group-varint encoded deltas
/// - **Full blocks** (docFreq >= 128): FOR-delta encoded 128-doc blocks with
///   skip headers, plus a VInt tail for remaining docs
///
/// Skip data is read but not used for seeking — this iterator only supports
/// sequential `next_doc()` access.
pub struct BlockDocIterator {
    doc_in: Box<dyn IndexInput>,
    doc_buffer: [i32; BLOCK_SIZE + 1],
    doc_buffer_upto: usize,
    doc_buffer_size: usize,
    doc_count_left: i32,
    doc: i32,
    prev_doc_id: i32,
    doc_freq: i32,
    singleton_doc_id: i32,
    index_has_freq: bool,
}

/// Sentinel value indicating no more documents.
const NO_MORE_DOCS: i32 = i32::MAX;

impl BlockDocIterator {
    fn new(
        doc_in: &dyn IndexInput,
        term_state: &postings_format::IntBlockTermState,
        index_has_freq: bool,
    ) -> io::Result<Self> {
        let doc_freq = term_state.doc_freq;
        let singleton_doc_id = term_state.singleton_doc_id;

        let doc_input = if doc_freq > 1 {
            let mut input = doc_in.slice("BlockDocIterator", 0, doc_in.length())?;
            input.seek(term_state.doc_start_fp as u64)?;
            input
        } else {
            // Singleton — no .doc I/O needed; create a dummy input
            doc_in.slice("BlockDocIterator", 0, 0)?
        };

        Ok(Self {
            doc_in: doc_input,
            doc_buffer: [0i32; BLOCK_SIZE + 1],
            doc_buffer_upto: BLOCK_SIZE,
            doc_buffer_size: BLOCK_SIZE,
            doc_count_left: doc_freq,
            doc: -1,
            prev_doc_id: -1,
            doc_freq,
            singleton_doc_id,
            index_has_freq,
        })
    }

    /// Returns the next doc ID, or `None` if exhausted.
    pub fn next_doc(&mut self) -> io::Result<Option<i32>> {
        if self.doc_buffer_upto == self.doc_buffer_size {
            if self.doc_count_left <= 0 {
                self.doc = NO_MORE_DOCS;
                return Ok(None);
            }
            self.refill_docs()?;
        }
        self.doc = self.doc_buffer[self.doc_buffer_upto];
        if self.doc == NO_MORE_DOCS {
            return Ok(None);
        }
        self.doc_buffer_upto += 1;
        Ok(Some(self.doc))
    }

    /// Returns the current doc ID (last returned by `next_doc`).
    pub fn doc_id(&self) -> i32 {
        self.doc
    }

    fn refill_docs(&mut self) -> io::Result<()> {
        if self.doc_count_left >= BLOCK_SIZE as i32 {
            self.refill_full_block()?;
        } else {
            self.refill_remainder()?;
        }
        Ok(())
    }

    /// Reads a full 128-doc block from the `.doc` file.
    ///
    /// Block layout written by the postings writer:
    /// ```text
    /// VLong(numSkipBytes) | skip_header | level0_data
    /// ```
    /// where `numSkipBytes` = len(skip_header) + len(skip_metadata_in_level0).
    /// After skipping `numSkipBytes` bytes we land at the doc encoding.
    fn refill_full_block(&mut self) -> io::Result<()> {
        let input = self.doc_in.as_mut();

        // Skip past all skip/impact metadata to reach doc encoding
        let num_skip_bytes = input.read_vlong()?;
        input.skip_bytes(num_skip_bytes as u64)?;

        // Read doc encoding flag
        let flag = input.read_byte()? as i8;

        if flag == 0 {
            // CONSECUTIVE: 128 sequential doc IDs
            for i in 0..BLOCK_SIZE {
                self.doc_buffer[i] = self.prev_doc_id + 1 + i as i32;
            }
        } else if flag > 0 {
            // FOR-delta encoded block
            let bpv = flag as u32;
            let mut arr = [0i32; BLOCK_SIZE];
            for_util::for_delta_decode(bpv, input, self.prev_doc_id, &mut arr)?;
            self.doc_buffer[..BLOCK_SIZE].copy_from_slice(&arr);
        } else {
            // BITSET: -flag longs encoding a bitset
            let num_longs = (-flag) as usize;
            let mut bitset = [0u64; BLOCK_SIZE / 2];
            for word in &mut bitset[..num_longs] {
                *word = input.read_le_long()? as u64;
            }
            // Convert bitset to doc IDs via prefix-sum
            let base = self.prev_doc_id + 1;
            let mut idx = 0;
            for (word_idx, &word) in bitset[..num_longs].iter().enumerate() {
                let mut w = word;
                while w != 0 {
                    let bit = w.trailing_zeros() as i32;
                    self.doc_buffer[idx] = base + (word_idx as i32 * 64) + bit;
                    idx += 1;
                    w &= w - 1; // clear lowest set bit
                }
            }
            debug_assert_eq!(idx, BLOCK_SIZE);
        }

        // Skip freq encoding if present
        if self.index_has_freq {
            // PFOR-encoded freqs: read and skip the token + data
            skip_pfor(input)?;
        }

        self.prev_doc_id = self.doc_buffer[BLOCK_SIZE - 1];
        self.doc_count_left -= BLOCK_SIZE as i32;
        self.doc_buffer_upto = 0;
        self.doc_buffer_size = BLOCK_SIZE;
        Ok(())
    }

    fn refill_remainder(&mut self) -> io::Result<()> {
        if self.doc_freq == 1 {
            // Singleton
            self.doc_buffer[0] = self.singleton_doc_id;
            self.doc_buffer[1] = NO_MORE_DOCS;
            self.doc_count_left = 0;
            self.doc_buffer_size = 1;
        } else {
            // VInt tail
            let num = self.doc_count_left as usize;
            read_vint_block(
                self.doc_in.as_mut(),
                &mut self.doc_buffer,
                num,
                self.index_has_freq,
            )?;
            // Prefix-sum deltas to absolute doc IDs
            let mut sum = self.prev_doc_id;
            for i in 0..num {
                sum += self.doc_buffer[i];
                self.doc_buffer[i] = sum;
            }
            self.doc_buffer[num] = NO_MORE_DOCS;
            self.doc_buffer_size = num;
            self.doc_count_left = 0;
        }
        self.doc_buffer_upto = 0;
        Ok(())
    }
}

/// Skips a PFOR-encoded block of 128 values without decoding.
fn skip_pfor(input: &mut dyn DataInput) -> io::Result<()> {
    let token = input.read_byte()? as u32;
    let bpv = token & 0x1F;
    let num_exceptions = token >> 5;
    if bpv == 0 {
        input.read_vlong()?; // constant value
        input.skip_bytes((num_exceptions * 2) as u64)?;
    } else {
        let for_bytes = (bpv << 4) as u64; // bpv * 16
        input.skip_bytes(for_bytes + (num_exceptions * 2) as u64)?;
    }
    Ok(())
}

/// Reads a VInt block: group-varint doc deltas with freq bit packed in LSB.
fn read_vint_block(
    input: &mut dyn DataInput,
    doc_buffer: &mut [i32],
    num: usize,
    index_has_freq: bool,
) -> io::Result<()> {
    input.read_group_vints(doc_buffer, num)?;
    if index_has_freq {
        for val in &mut doc_buffer[..num] {
            *val >>= 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::competitive_impact::NormsLookup;
    use crate::codecs::lucene103::postings_writer::PostingsWriter;
    use crate::document::{self, Document, IndexOptions};
    use crate::index::{
        FieldInfo, FieldInfos, IndexWriter, IndexWriterConfig, PointDimensionConfig,
    };
    use crate::store::{MemoryDirectory, SharedDirectory};
    use assertables::*;

    /// Write postings for a single DOCS-only term and return the term state + directory.
    fn write_single_term(
        doc_ids: &[i32],
    ) -> io::Result<(postings_format::IntBlockTermState, Box<dyn Directory>)> {
        let segment_name = "_0";
        let segment_suffix = "";
        let segment_id = [0u8; 16];
        let shared_dir = SharedDirectory::new(Box::new(MemoryDirectory::new()));

        let postings: Vec<(i32, i32, &[i32])> = doc_ids
            .iter()
            .map(|&id| (id, 1i32, &[] as &[i32]))
            .collect();
        let norms = NormsLookup::no_norms();

        let term_state = {
            let mut writer = PostingsWriter::new(
                &shared_dir,
                segment_name,
                segment_suffix,
                &segment_id,
                false,
            )?;
            let state = writer.write_term(&postings, IndexOptions::Docs, &norms)?;
            writer.finish()?;
            state
        };

        let dir = shared_dir.into_inner().unwrap();
        Ok((term_state, dir))
    }

    /// Open PostingsReader from a directory written by `write_single_term`.
    fn open_reader(dir: &dyn Directory) -> io::Result<PostingsReader> {
        let field_infos = FieldInfos::new(vec![make_field_info("f", 0)]);
        PostingsReader::open(dir, "_0", "", &[0u8; 16], &field_infos)
    }

    fn make_field_info(name: &str, number: u32) -> FieldInfo {
        FieldInfo::new(
            name.to_string(),
            number,
            false,
            false,
            IndexOptions::Docs,
            crate::document::DocValuesType::None,
            PointDimensionConfig::default(),
        )
    }

    /// Collect all doc IDs from a BlockDocIterator.
    fn collect_docs(iter: &mut BlockDocIterator) -> io::Result<Vec<i32>> {
        let mut docs = Vec::new();
        while let Some(doc) = iter.next_doc()? {
            docs.push(doc);
        }
        Ok(docs)
    }

    #[test]
    fn test_open_postings_reader() {
        let config = IndexWriterConfig::new().set_use_compound_file(false);
        let writer = IndexWriter::with_config(config);
        let mut doc = Document::new();
        doc.add(document::text_field("content", "hello world"));
        writer.add_document(doc).unwrap();

        let result = writer.commit().unwrap();
        let seg_files = result.into_segment_files().unwrap();

        let mut mem_dir = MemoryDirectory::new();
        for sf in &seg_files {
            mem_dir.write_file(&sf.name, &sf.data).unwrap();
        }
        let dir = Box::new(mem_dir) as Box<dyn Directory>;

        let files = dir.list_all().unwrap();
        let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
        let infos = crate::index::segment_infos::read(dir.as_ref(), segments_file).unwrap();
        let seg = &infos.segments[0];

        let si =
            crate::codecs::lucene99::segment_info_format::read(dir.as_ref(), &seg.name, &seg.id)
                .unwrap();
        let field_infos =
            crate::codecs::lucene94::field_infos_format::read(dir.as_ref(), &si, "").unwrap();

        let suffix = field_infos
            .iter()
            .find_map(|fi| {
                let format = fi.get_attribute("PerFieldPostingsFormat.format")?;
                let sfx = fi.get_attribute("PerFieldPostingsFormat.suffix")?;
                Some(format!("{format}_{sfx}"))
            })
            .unwrap();

        let reader =
            PostingsReader::open(dir.as_ref(), &seg.name, &suffix, &seg.id, &field_infos).unwrap();

        assert_ge!(reader.max_num_impacts_at_level0(), 0);
        assert_ge!(reader.max_impact_num_bytes_at_level0(), 0);
        assert_ge!(reader.max_num_impacts_at_level1(), 0);
        assert_ge!(reader.max_impact_num_bytes_at_level1(), 0);
    }

    #[test]
    fn test_block_doc_iterator_singleton() {
        let (state, dir) = write_single_term(&[42]).unwrap();
        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.singleton_doc_id, 42);

        let reader = open_reader(dir.as_ref()).unwrap();
        let mut iter = reader.block_doc_iterator(&state, false).unwrap();
        let docs = collect_docs(&mut iter).unwrap();
        assert_eq!(docs, vec![42]);
    }

    #[test]
    fn test_block_doc_iterator_vint_tail() {
        let doc_ids: Vec<i32> = (0..10).collect();
        let (state, dir) = write_single_term(&doc_ids).unwrap();
        assert_eq!(state.doc_freq, 10);
        assert_eq!(state.singleton_doc_id, -1);

        let reader = open_reader(dir.as_ref()).unwrap();
        let mut iter = reader.block_doc_iterator(&state, false).unwrap();
        let docs = collect_docs(&mut iter).unwrap();
        assert_eq!(docs, doc_ids);
    }

    #[test]
    fn test_block_doc_iterator_vint_tail_sparse() {
        let doc_ids = vec![0, 5, 10, 50, 100, 500, 1000];
        let (state, dir) = write_single_term(&doc_ids).unwrap();
        assert_eq!(state.doc_freq, 7);

        let reader = open_reader(dir.as_ref()).unwrap();
        let mut iter = reader.block_doc_iterator(&state, false).unwrap();
        let docs = collect_docs(&mut iter).unwrap();
        assert_eq!(docs, doc_ids);
    }

    #[test]
    fn test_block_doc_iterator_full_block() {
        // 128 docs — exactly one full block, no tail
        let doc_ids: Vec<i32> = (0..128).collect();
        let (state, dir) = write_single_term(&doc_ids).unwrap();
        assert_eq!(state.doc_freq, 128);

        let reader = open_reader(dir.as_ref()).unwrap();
        let mut iter = reader.block_doc_iterator(&state, false).unwrap();
        let docs = collect_docs(&mut iter).unwrap();
        assert_eq!(docs, doc_ids);
    }

    #[test]
    fn test_block_doc_iterator_block_plus_tail() {
        // 200 docs — one full block (128) + VInt tail (72)
        let doc_ids: Vec<i32> = (0..200).collect();
        let (state, dir) = write_single_term(&doc_ids).unwrap();
        assert_eq!(state.doc_freq, 200);

        let reader = open_reader(dir.as_ref()).unwrap();
        let mut iter = reader.block_doc_iterator(&state, false).unwrap();
        let docs = collect_docs(&mut iter).unwrap();
        assert_eq!(docs, doc_ids);
    }

    #[test]
    fn test_block_doc_iterator_multiple_blocks() {
        // 300 docs — two full blocks (256) + VInt tail (44)
        let doc_ids: Vec<i32> = (0..300).collect();
        let (state, dir) = write_single_term(&doc_ids).unwrap();
        assert_eq!(state.doc_freq, 300);

        let reader = open_reader(dir.as_ref()).unwrap();
        let mut iter = reader.block_doc_iterator(&state, false).unwrap();
        let docs = collect_docs(&mut iter).unwrap();
        assert_eq!(docs, doc_ids);
    }

    #[test]
    fn test_block_doc_iterator_exhausted_returns_none() {
        let (state, dir) = write_single_term(&[7]).unwrap();
        let reader = open_reader(dir.as_ref()).unwrap();
        let mut iter = reader.block_doc_iterator(&state, false).unwrap();

        assert_eq!(iter.next_doc().unwrap(), Some(7));
        assert_eq!(iter.next_doc().unwrap(), None);
        assert_eq!(iter.next_doc().unwrap(), None);
    }
}
