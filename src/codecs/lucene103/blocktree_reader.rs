// SPDX-License-Identifier: Apache-2.0

//! Block tree terms reader for the Lucene 103 codec.
//!
//! Reads `.tim` (terms dictionary), `.tip` (terms index / trie), and `.tmd`
//! (terms metadata) files written by [`super::blocktree_writer`].
//!
//! Follows the Java `Lucene103BlockTreeTermsReader` design: metadata is read
//! eagerly from `.tmd` during construction, while `.tim` and `.tip` file handles
//! are kept open for lazy term enumeration at query time.

use std::collections::HashMap;
use std::io;

use crate::codecs::codec_util;
use crate::codecs::lucene103::postings_format::{
    self, BLOCKTREE_VERSION_CURRENT, BLOCKTREE_VERSION_START, TERMS_CODEC, TERMS_CODEC_NAME,
    TERMS_INDEX_CODEC_NAME, TERMS_INDEX_EXTENSION, TERMS_META_CODEC_NAME, TERMS_META_EXTENSION,
    VERSION_CURRENT, VERSION_START,
};
use crate::document::IndexOptions;
use crate::index::{FieldInfos, index_file_names};
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory, IndexInput};

/// Per-field metadata read from the `.tmd` terms metadata file.
///
/// Mirrors Java's `FieldReader` — holds all metadata needed to access a field's
/// terms dictionary. The trie index pointers (`index_start`, `root_fp`,
/// `index_end`) are stored for future lazy `TrieReader` creation.
#[derive(Debug)]
pub struct FieldReader {
    /// Field number from [`FieldInfos`].
    pub field_number: u32,
    /// Total number of unique terms in this field.
    pub num_terms: i64,
    /// Sum of `totalTermFreq` across all terms in this field.
    pub sum_total_term_freq: i64,
    /// Sum of `docFreq` across all terms in this field.
    pub sum_doc_freq: i64,
    /// Number of documents that have at least one term in this field.
    pub doc_count: i32,
    /// Lexicographically smallest term in this field.
    pub min_term: Box<[u8]>,
    /// Lexicographically largest term in this field.
    pub max_term: Box<[u8]>,
    // Trie index pointers — used when creating a TrieReader for term enumeration
    #[allow(dead_code)]
    index_start: i64,
    #[allow(dead_code)]
    root_fp: i64,
    #[allow(dead_code)]
    index_end: i64,
}

/// Block tree terms reader that provides access to per-field term metadata
/// and keeps file handles open for future lazy term enumeration.
///
/// Mirrors Java's `Lucene103BlockTreeTermsReader`. Construction reads all
/// per-field metadata from `.tmd` (then closes it), while `.tim` and `.tip`
/// remain open.
pub struct BlockTreeTermsReader {
    /// Open handle to the terms dictionary (`.tim`).
    #[allow(dead_code)]
    terms_in: Box<dyn IndexInput>,
    /// Open handle to the terms index / trie (`.tip`).
    #[allow(dead_code)]
    index_in: Box<dyn IndexInput>,
    /// Per-field metadata, keyed by field number.
    fields: HashMap<u32, FieldReader>,
}

impl BlockTreeTermsReader {
    /// Opens the block tree terms reader for a segment.
    ///
    /// Reads metadata eagerly from `.tmd`, validates headers on `.tim` and `.tip`,
    /// and keeps all three file handles. The `.tmd` checksum input is consumed
    /// and dropped after construction.
    pub fn open(
        directory: &dyn Directory,
        segment_name: &str,
        segment_suffix: &str,
        segment_id: &[u8; codec_util::ID_LENGTH],
        field_infos: &FieldInfos,
    ) -> io::Result<Self> {
        // Open .tim and validate header
        let terms_name = index_file_names::segment_file_name(
            segment_name,
            segment_suffix,
            postings_format::TERMS_EXTENSION,
        );
        let mut terms_in = directory.open_input(&terms_name)?;
        let version = codec_util::check_index_header(
            terms_in.as_mut(),
            TERMS_CODEC_NAME,
            BLOCKTREE_VERSION_START,
            BLOCKTREE_VERSION_CURRENT,
            segment_id,
            segment_suffix,
        )?;

        // Open .tip and validate header
        let index_name = index_file_names::segment_file_name(
            segment_name,
            segment_suffix,
            TERMS_INDEX_EXTENSION,
        );
        let mut index_in = directory.open_input(&index_name)?;
        codec_util::check_index_header(
            index_in.as_mut(),
            TERMS_INDEX_CODEC_NAME,
            version,
            version,
            segment_id,
            segment_suffix,
        )?;

        // Open .tmd with checksum validation and read all metadata
        let meta_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, TERMS_META_EXTENSION);
        let meta_input = directory.open_input(&meta_name)?;
        let mut meta_in = ChecksumIndexInput::new(meta_input);

        codec_util::check_index_header(
            &mut meta_in,
            TERMS_META_CODEC_NAME,
            version,
            version,
            segment_id,
            segment_suffix,
        )?;

        // Postings reader init: validate postings header + block size
        codec_util::check_index_header(
            &mut meta_in,
            TERMS_CODEC,
            VERSION_START,
            VERSION_CURRENT,
            segment_id,
            segment_suffix,
        )?;
        let block_size = meta_in.read_vint()?;
        if block_size != postings_format::BLOCK_SIZE as i32 {
            return Err(io::Error::other(format!(
                "invalid block size: {block_size}, expected {}",
                postings_format::BLOCK_SIZE
            )));
        }

        // Read per-field metadata
        let num_fields = meta_in.read_vint()?;
        if num_fields < 0 {
            return Err(io::Error::other(format!("invalid numFields: {num_fields}")));
        }

        let mut fields = HashMap::with_capacity(num_fields as usize);
        for _ in 0..num_fields {
            let field_reader = read_field_metadata(&mut meta_in, field_infos)?;
            let field_number = field_reader.field_number;
            if fields.insert(field_number, field_reader).is_some() {
                return Err(io::Error::other(format!(
                    "duplicate field number: {field_number}"
                )));
            }
        }

        // Read .tip and .tim end pointers (for future integrity checks)
        let _index_length = meta_in.read_le_long()?;
        let _terms_length = meta_in.read_le_long()?;

        // Validate .tmd footer
        codec_util::check_footer(&mut meta_in)?;

        Ok(Self {
            terms_in,
            index_in,
            fields,
        })
    }

    /// Returns the [`FieldReader`] for the given field number, if it exists.
    pub fn field_reader(&self, field_number: u32) -> Option<&FieldReader> {
        self.fields.get(&field_number)
    }

    /// Returns the number of indexed fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns `true` if there are no indexed fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Reads a single field's metadata from the `.tmd` stream.
fn read_field_metadata(
    input: &mut dyn DataInput,
    field_infos: &FieldInfos,
) -> io::Result<FieldReader> {
    let field_number = input.read_vint()?;
    if field_number < 0 {
        return Err(io::Error::other(format!(
            "invalid field number: {field_number}"
        )));
    }
    let field_number = field_number as u32;

    let num_terms = input.read_vlong()?;
    if num_terms <= 0 {
        return Err(io::Error::other(format!(
            "invalid numTerms for field {field_number}: {num_terms}"
        )));
    }

    let field_info = field_infos
        .field_info_by_number(field_number)
        .ok_or_else(|| io::Error::other(format!("invalid field number: {field_number}")))?;

    // When frequencies are omitted (DOCS only), sumDocFreq == sumTotalTermFreq
    // and only one value is written.
    let sum_total_term_freq = input.read_vlong()?;
    let sum_doc_freq = if field_info.index_options() == IndexOptions::Docs {
        sum_total_term_freq
    } else {
        input.read_vlong()?
    };

    let doc_count = input.read_vint()?;

    let min_term = read_bytes_ref(input)?;
    let max_term = read_bytes_ref(input)?;

    // Validate metadata consistency (matches Java reader checks)
    if doc_count < 0 {
        return Err(io::Error::other(format!("invalid docCount: {doc_count}")));
    }
    if sum_doc_freq < doc_count as i64 {
        return Err(io::Error::other(format!(
            "invalid sumDocFreq: {sum_doc_freq} docCount: {doc_count}"
        )));
    }
    if sum_total_term_freq < sum_doc_freq {
        return Err(io::Error::other(format!(
            "invalid sumTotalTermFreq: {sum_total_term_freq} sumDocFreq: {sum_doc_freq}"
        )));
    }

    // Trie index pointers (stored for future lazy TrieReader creation)
    let index_start = input.read_vlong()?;
    let root_fp = input.read_vlong()?;
    let index_end = input.read_vlong()?;

    Ok(FieldReader {
        field_number,
        num_terms,
        sum_total_term_freq,
        sum_doc_freq,
        doc_count,
        min_term,
        max_term,
        index_start,
        root_fp,
        index_end,
    })
}

/// Reads a length-prefixed byte array (VInt length + raw bytes).
fn read_bytes_ref(input: &mut dyn DataInput) -> io::Result<Box<[u8]>> {
    let len = input.read_vint()?;
    if len < 0 {
        return Err(io::Error::other(format!("invalid bytes length: {len}")));
    }
    let mut buf = vec![0u8; len as usize];
    input.read_bytes(&mut buf)?;
    Ok(buf.into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::competitive_impact::NormsLookup;
    use crate::codecs::lucene103::blocktree_writer::BlockTreeTermsWriter;
    use crate::document::IndexOptions;
    use crate::index::indexing_chain::PostingsArray;
    use crate::index::{FieldInfo, FieldInfos, PointDimensionConfig};
    use crate::store::memory::MemoryDirectory;
    use assertables::*;

    fn make_field_info(name: &str, number: u32, index_options: IndexOptions) -> FieldInfo {
        FieldInfo::new(
            name.to_string(),
            number,
            false, // store_term_vector
            false, // omit_norms
            index_options,
            crate::document::DocValuesType::None,
            PointDimensionConfig::default(),
        )
    }

    fn make_postings<'a>(
        terms: &'a [(&str, &[i32])],
        has_freqs: bool,
        has_positions: bool,
    ) -> (Vec<(&'a str, usize)>, PostingsArray) {
        let mut postings = PostingsArray::new(has_freqs, has_positions, false, false, false);
        let mut sorted_terms = Vec::new();

        for (term, doc_ids) in terms {
            let term_id = postings.add_term();
            for &doc_id in *doc_ids {
                postings.record_occurrence(term_id, doc_id, 0, 0, 0);
            }
            sorted_terms.push((*term, term_id));
        }

        // Sort by term bytes
        sorted_terms.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        (sorted_terms, postings)
    }

    fn write_and_read(
        field_infos_vec: Vec<FieldInfo>,
        fields_data: Vec<(&str, IndexOptions, Vec<(&str, &[i32])>)>,
    ) -> io::Result<BlockTreeTermsReader> {
        let field_infos = FieldInfos::new(field_infos_vec);
        let segment_name = "_0";
        let segment_suffix = "";
        let segment_id = [0u8; 16];

        let shared_dir = crate::store::SharedDirectory::new(Box::new(MemoryDirectory::new()));

        {
            let mut writer = BlockTreeTermsWriter::new(
                &shared_dir,
                segment_name,
                segment_suffix,
                &segment_id,
                &field_infos,
            )?;

            for (field_name, index_options, terms) in &fields_data {
                let fi = field_infos.field_info_by_name(field_name).unwrap();
                let has_freqs = index_options.has_freqs();
                let has_positions = index_options.has_positions();
                let (sorted_terms, postings) = make_postings(terms, has_freqs, has_positions);
                let norms = NormsLookup::no_norms();
                writer.write_field(fi, &sorted_terms, &postings, &norms)?;
            }

            writer.finish()?;
        }

        let dir = shared_dir.into_inner().unwrap();
        BlockTreeTermsReader::open(
            dir.as_ref(),
            segment_name,
            segment_suffix,
            &segment_id,
            &field_infos,
        )
    }

    // Ported from org.apache.lucene.codecs.lucene103.blocktree.TestBlockTreeTermsReader
    #[test]
    fn test_roundtrip_single_field() -> io::Result<()> {
        let fi = make_field_info("title", 0, IndexOptions::Docs);
        let terms = vec![
            ("alpha", &[0, 1, 2][..]),
            ("beta", &[1, 3]),
            ("gamma", &[0, 2, 4]),
        ];

        let reader = write_and_read(vec![fi], vec![("title", IndexOptions::Docs, terms)])?;

        assert_eq!(reader.len(), 1);

        let fr = reader.field_reader(0).unwrap();
        assert_eq!(fr.field_number, 0);
        assert_eq!(fr.num_terms, 3);
        // For DOCS-only, sumTotalTermFreq == sumDocFreq
        assert_eq!(fr.sum_total_term_freq, fr.sum_doc_freq);
        assert_eq!(&*fr.min_term, b"alpha");
        assert_eq!(&*fr.max_term, b"gamma");
        assert_ge!(fr.doc_count, 1);

        Ok(())
    }

    #[test]
    fn test_roundtrip_with_freqs() -> io::Result<()> {
        let fi = make_field_info("body", 0, IndexOptions::DocsAndFreqs);
        let terms = vec![("hello", &[0, 1][..]), ("world", &[0])];

        let reader = write_and_read(vec![fi], vec![("body", IndexOptions::DocsAndFreqs, terms)])?;

        let fr = reader.field_reader(0).unwrap();
        assert_eq!(fr.num_terms, 2);
        // With freqs, sumTotalTermFreq >= sumDocFreq
        assert_ge!(fr.sum_total_term_freq, fr.sum_doc_freq);
        assert_ge!(fr.sum_doc_freq, fr.doc_count as i64);
        assert_eq!(&*fr.min_term, b"hello");
        assert_eq!(&*fr.max_term, b"world");

        Ok(())
    }

    #[test]
    fn test_roundtrip_multiple_fields() -> io::Result<()> {
        let fi0 = make_field_info("path", 0, IndexOptions::Docs);
        let fi1 = make_field_info("contents", 1, IndexOptions::DocsAndFreqsAndPositions);

        let reader = write_and_read(
            vec![fi0, fi1],
            vec![
                (
                    "path",
                    IndexOptions::Docs,
                    vec![("doc1.txt", &[0][..]), ("doc2.txt", &[1])],
                ),
                (
                    "contents",
                    IndexOptions::DocsAndFreqsAndPositions,
                    vec![("hello", &[0, 1][..]), ("world", &[0]), ("rust", &[1])],
                ),
            ],
        )?;

        assert_eq!(reader.len(), 2);

        let path_fr = reader.field_reader(0).unwrap();
        assert_eq!(path_fr.num_terms, 2);

        let contents_fr = reader.field_reader(1).unwrap();
        assert_eq!(contents_fr.num_terms, 3);
        assert_ge!(contents_fr.sum_total_term_freq, contents_fr.sum_doc_freq);

        // Non-existent field
        assert_none!(reader.field_reader(99));

        Ok(())
    }

    // --- Validation tests for read_field_metadata ---

    use crate::store::{DataOutput, VecOutput};

    /// Simple DataInput over a byte slice, for feeding crafted bytes.
    struct ByteSliceInput<'a> {
        data: &'a [u8],
        pos: usize,
    }

    impl<'a> ByteSliceInput<'a> {
        fn new(data: &'a [u8]) -> Self {
            Self { data, pos: 0 }
        }
    }

    impl DataInput for ByteSliceInput<'_> {
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

    /// Builds a valid field metadata byte stream for field 0 (DOCS-only).
    fn valid_field_metadata_bytes() -> Vec<u8> {
        let mut buf = Vec::new();
        let mut out = VecOutput(&mut buf);
        out.write_vint(0).unwrap(); // field_number
        out.write_vlong(5).unwrap(); // num_terms
        out.write_vlong(10).unwrap(); // sumTotalTermFreq (== sumDocFreq for DOCS)
        out.write_vint(3).unwrap(); // doc_count
        out.write_vint(1).unwrap(); // min_term length
        out.write_byte(b'a').unwrap(); // min_term
        out.write_vint(1).unwrap(); // max_term length
        out.write_byte(b'z').unwrap(); // max_term
        out.write_vlong(0).unwrap(); // index_start
        out.write_vlong(0).unwrap(); // root_fp
        out.write_vlong(0).unwrap(); // index_end
        buf
    }

    fn docs_field_infos() -> FieldInfos {
        FieldInfos::new(vec![make_field_info("test", 0, IndexOptions::Docs)])
    }

    #[test]
    fn test_valid_field_metadata_parses() {
        let data = valid_field_metadata_bytes();
        let fi = docs_field_infos();
        let mut input = ByteSliceInput::new(&data);
        let fr = read_field_metadata(&mut input, &fi).unwrap();
        assert_eq!(fr.field_number, 0);
        assert_eq!(fr.num_terms, 5);
        assert_eq!(fr.doc_count, 3);
        assert_eq!(&*fr.min_term, b"a");
        assert_eq!(&*fr.max_term, b"z");
    }

    #[test]
    fn test_invalid_num_terms_zero() {
        let mut buf = Vec::new();
        let mut out = VecOutput(&mut buf);
        out.write_vint(0).unwrap(); // field_number
        out.write_vlong(0).unwrap(); // num_terms = 0 (invalid)

        let fi = docs_field_infos();
        let mut input = ByteSliceInput::new(&buf);
        let err = read_field_metadata(&mut input, &fi).unwrap_err();
        assert!(err.to_string().contains("invalid numTerms"), "{err}");
    }

    #[test]
    fn test_invalid_field_number_not_in_infos() {
        let mut buf = Vec::new();
        let mut out = VecOutput(&mut buf);
        out.write_vint(99).unwrap(); // field_number not in FieldInfos
        out.write_vlong(1).unwrap(); // num_terms

        let fi = docs_field_infos();
        let mut input = ByteSliceInput::new(&buf);
        let err = read_field_metadata(&mut input, &fi).unwrap_err();
        assert!(err.to_string().contains("invalid field number"), "{err}");
    }

    #[test]
    fn test_invalid_sum_doc_freq_less_than_doc_count() {
        let mut buf = Vec::new();
        let mut out = VecOutput(&mut buf);
        out.write_vint(0).unwrap(); // field_number
        out.write_vlong(5).unwrap(); // num_terms
        out.write_vlong(2).unwrap(); // sumTotalTermFreq (== sumDocFreq for DOCS)
        out.write_vint(10).unwrap(); // doc_count > sumDocFreq (invalid)
        out.write_vint(0).unwrap(); // min_term (empty)
        out.write_vint(0).unwrap(); // max_term (empty)

        let fi = docs_field_infos();
        let mut input = ByteSliceInput::new(&buf);
        let err = read_field_metadata(&mut input, &fi).unwrap_err();
        assert!(err.to_string().contains("invalid sumDocFreq"), "{err}");
    }

    #[test]
    fn test_invalid_sum_total_term_freq_less_than_sum_doc_freq() {
        // Need a non-DOCS field to have separate sumTotalTermFreq and sumDocFreq
        let fi = FieldInfos::new(vec![make_field_info("body", 0, IndexOptions::DocsAndFreqs)]);

        let mut buf = Vec::new();
        let mut out = VecOutput(&mut buf);
        out.write_vint(0).unwrap(); // field_number
        out.write_vlong(5).unwrap(); // num_terms
        out.write_vlong(3).unwrap(); // sumTotalTermFreq
        out.write_vlong(5).unwrap(); // sumDocFreq > sumTotalTermFreq (invalid)
        out.write_vint(2).unwrap(); // doc_count
        out.write_vint(0).unwrap(); // min_term (empty)
        out.write_vint(0).unwrap(); // max_term (empty)

        let mut input = ByteSliceInput::new(&buf);
        let err = read_field_metadata(&mut input, &fi).unwrap_err();
        assert!(
            err.to_string().contains("invalid sumTotalTermFreq"),
            "{err}"
        );
    }

    #[test]
    fn test_open_missing_file_returns_error() {
        let dir = MemoryDirectory::new();
        let fi = docs_field_infos();
        let result = BlockTreeTermsReader::open(&dir, "_0", "", &[0u8; 16], &fi);
        assert!(result.is_err());
    }
}
