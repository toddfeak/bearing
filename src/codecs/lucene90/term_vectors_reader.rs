// SPDX-License-Identifier: Apache-2.0

//! Term vectors metadata reader for the Lucene90 compressing term vectors format.
//!
//! Reads `.tvm` (metadata) and `.tvx` (index) files written by [`super::term_vectors::write`].
//! Metadata is read eagerly during construction; chunk data in `.tvd` is not touched.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::lucene90::stored_fields_reader::FieldsIndexReader;
use crate::codecs::lucene90::term_vectors::{
    DATA_CODEC, INDEX_CODEC_IDX, INDEX_CODEC_META, INDEX_EXTENSION, META_EXTENSION,
    VECTORS_EXTENSION, VERSION,
};
use crate::index::index_file_names;
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory};

/// Reads term vectors for a segment.
///
/// Opens `.tvm`, `.tvx`, and `.tvd` files during construction. Metadata and
/// chunk index are read eagerly; chunk data in `.tvd` is accessed lazily.
/// Matches the constructor pattern of Java's `Lucene90CompressingTermVectorsReader`.
///
/// When lazy chunk reads are added, this struct will also hold:
/// - `index_reader: FieldsIndexReader` — chunk index for doc ID → chunk lookup
/// - `data: Box<dyn IndexInput>` — `.tvd` file handle for chunk data
/// - `chunk_size: i32` — decompression buffer sizing
/// - `num_dirty_chunks: i64` — incomplete chunk count
pub struct TermVectorsReader {
    /// Total number of chunks.
    num_chunks: i64,
}

impl TermVectorsReader {
    /// Opens term vectors metadata (`.tvm`, `.tvx`) for the given segment.
    pub fn open(
        directory: &dyn Directory,
        segment_name: &str,
        segment_suffix: &str,
        segment_id: &[u8; codec_util::ID_LENGTH],
    ) -> io::Result<Self> {
        // Open .tvm (metadata) with checksum validation
        let tvm_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);
        let meta_input = directory.open_input(&tvm_name)?;
        let mut meta_in = ChecksumIndexInput::new(meta_input);

        codec_util::check_index_header(
            &mut meta_in,
            INDEX_CODEC_META,
            VERSION,
            VERSION,
            segment_id,
            segment_suffix,
        )?;

        let _packed_ints_version = meta_in.read_vint()?;
        let _chunk_size = meta_in.read_vint()?;

        // Open .tvx (index) and validate header
        let tvx_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, INDEX_EXTENSION);
        let tvx_input = directory.open_input(&tvx_name)?;
        let mut tvx_check = directory.open_input(&tvx_name)?;
        codec_util::check_index_header(
            tvx_check.as_mut(),
            INDEX_CODEC_IDX,
            VERSION,
            VERSION,
            segment_id,
            segment_suffix,
        )?;

        // Read FieldsIndex — consumes index metadata from .tvm, loads from .tvx
        let _index_reader = FieldsIndexReader::open(&mut meta_in, tvx_input.as_ref())?;

        // Read remaining metadata (max_pointer already consumed by FieldsIndexReader)
        let num_chunks = meta_in.read_vlong()?;
        let _num_dirty_chunks = meta_in.read_vlong()?;
        let _num_dirty_docs = meta_in.read_vlong()?;

        codec_util::check_footer(&mut meta_in)?;

        // Validate .tvd header (data file kept closed until chunk reads are needed)
        let tvd_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, VECTORS_EXTENSION);
        let mut tvd = directory.open_input(&tvd_name)?;
        codec_util::check_index_header(
            tvd.as_mut(),
            DATA_CODEC,
            VERSION,
            VERSION,
            segment_id,
            segment_suffix,
        )?;

        debug!("term_vectors_reader: {num_chunks} chunks for segment {segment_name}");

        Ok(Self { num_chunks })
    }

    /// Returns the total number of chunks in the term vectors data.
    pub fn num_chunks(&self) -> i64 {
        self.num_chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::lucene90::term_vectors;
    use crate::index::indexing_chain::TermVectorDoc;
    use crate::store::{MemoryDirectory, SharedDirectory};
    use assertables::*;

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    fn make_tv_doc(field_number: u32, terms: Vec<&str>) -> TermVectorDoc {
        use crate::index::indexing_chain::{OffsetBuffers, TermVectorField, TermVectorTerm};

        let tv_terms: Vec<TermVectorTerm> = terms
            .into_iter()
            .map(|t| TermVectorTerm {
                term: t.to_string(),
                freq: 1,
                positions: vec![0],
                offsets: Some(Box::new(OffsetBuffers {
                    start_offsets: vec![0],
                    end_offsets: vec![t.len() as i32],
                })),
            })
            .collect();

        TermVectorDoc {
            fields: vec![TermVectorField {
                field_number,
                has_positions: true,
                has_offsets: true,
                has_payloads: false,
                terms: tv_terms,
            }],
        }
    }

    fn write_and_read(tv_docs: &[TermVectorDoc], num_docs: i32) -> TermVectorsReader {
        let segment_id = [0u8; 16];
        let dir = test_directory();
        term_vectors::write(&dir, "_0", "", &segment_id, tv_docs, num_docs).unwrap();
        let guard = dir.lock().unwrap();
        TermVectorsReader::open(guard.as_ref(), "_0", "", &segment_id).unwrap()
    }

    #[test]
    fn test_single_doc() {
        let docs = vec![make_tv_doc(2, vec!["hello", "world"])];
        let reader = write_and_read(&docs, 1);
        // 1 doc → 1 dirty chunk (force-flushed at finish)
        assert_eq!(reader.num_chunks(), 1);
        assert_eq!(reader.num_chunks, 1);
    }

    #[test]
    fn test_multiple_docs_one_chunk() {
        let docs: Vec<TermVectorDoc> = (0..10).map(|_| make_tv_doc(2, vec!["term"])).collect();
        let reader = write_and_read(&docs, 10);
        // 10 small docs fit in one chunk (chunk_size=4096)
        assert_eq!(reader.num_chunks(), 1);
    }

    #[test]
    fn test_multiple_docs_multiple_fields() {
        use crate::index::indexing_chain::{TermVectorField, TermVectorTerm};

        let docs: Vec<TermVectorDoc> = (0..5)
            .map(|_| TermVectorDoc {
                fields: vec![
                    TermVectorField {
                        field_number: 0,
                        has_positions: true,
                        has_offsets: false,
                        has_payloads: false,
                        terms: vec![TermVectorTerm {
                            term: "alpha".to_string(),
                            freq: 1,
                            positions: vec![0],
                            offsets: None,
                        }],
                    },
                    TermVectorField {
                        field_number: 1,
                        has_positions: true,
                        has_offsets: false,
                        has_payloads: false,
                        terms: vec![TermVectorTerm {
                            term: "beta".to_string(),
                            freq: 1,
                            positions: vec![0],
                            offsets: None,
                        }],
                    },
                ],
            })
            .collect();
        let reader = write_and_read(&docs, 5);
        assert_ge!(reader.num_chunks(), 1);
    }
}
