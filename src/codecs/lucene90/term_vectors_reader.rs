// SPDX-License-Identifier: Apache-2.0

//! Term vectors reader for the Lucene90 compressing term vectors format.
//!
//! Reads `.tvm` (metadata), `.tvx` (index), and `.tvd` (data) files written
//! by [`super::term_vectors`]. Metadata and chunk index are read eagerly;
//! chunk data in `.tvd` is available via the retained `vectors_stream` handle.

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
use crate::store::{DataInput, Directory, IndexInput};

/// Reads term vectors for a segment.
///
/// Opens `.tvd` first (keeps handle),
/// reads metadata from `.tvm`, creates chunk index from `.tvx`, validates
/// dirty chunk invariants.
pub struct TermVectorsReader {
    /// Open handle to the `.tvd` data file.
    #[expect(dead_code)]
    vectors_stream: Box<dyn IndexInput>,
    /// Chunk index for doc ID → chunk lookup.
    #[expect(dead_code)]
    index_reader: FieldsIndexReader,
    /// Header version from `.tvd`.
    #[expect(dead_code)]
    version: i32,
    /// Packed ints version from metadata.
    #[expect(dead_code)]
    packed_ints_version: i32,
    /// Chunk size for decompression buffer sizing.
    #[expect(dead_code)]
    chunk_size: i32,
    /// Total number of chunks.
    num_chunks: i64,
    /// Number of incomplete (dirty) chunks.
    #[expect(dead_code)]
    num_dirty_chunks: i64,
    /// Number of documents in incomplete chunks.
    #[expect(dead_code)]
    num_dirty_docs: i64,
}

impl TermVectorsReader {
    /// Opens term vectors files for the given segment.
    ///
    /// Follows Java's `Lucene90CompressingTermVectorsReader` constructor order:
    /// 1. Open `.tvd` (data) — keep handle
    /// 2. Open `.tvm` (meta) with checksum — read metadata
    /// 3. `FieldsIndexReader` opens `.tvx` internally
    /// 4. Validate dirty chunk invariants
    pub fn open(
        directory: &dyn Directory,
        segment_name: &str,
        segment_suffix: &str,
        segment_id: &[u8; codec_util::ID_LENGTH],
    ) -> io::Result<Self> {
        // 1. Open .tvd (data) — keep handle
        let tvd_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, VECTORS_EXTENSION);
        let mut vectors_stream = directory.open_input(&tvd_name)?;
        let version = codec_util::check_index_header(
            vectors_stream.as_mut(),
            DATA_CODEC,
            VERSION,
            VERSION,
            segment_id,
            segment_suffix,
        )?;

        // 2. Open .tvm (meta) with checksum
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

        let packed_ints_version = meta_in.read_vint()?;
        let chunk_size = meta_in.read_vint()?;

        // Validate .tvd footer structure
        codec_util::retrieve_checksum(vectors_stream.as_mut())?;

        // 3. Open .tvx (index) and create FieldsIndexReader
        let tvx_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, INDEX_EXTENSION);
        let mut tvx_input = directory.open_input(&tvx_name)?;
        codec_util::check_index_header(
            tvx_input.as_mut(),
            INDEX_CODEC_IDX,
            VERSION,
            VERSION,
            segment_id,
            segment_suffix,
        )?;
        let index_reader = FieldsIndexReader::open(&mut meta_in, tvx_input.as_ref())?;

        // 4. Read chunk counts
        let num_chunks = meta_in.read_vlong()?;
        let num_dirty_chunks = meta_in.read_vlong()?;
        let num_dirty_docs = meta_in.read_vlong()?;

        // Validate dirty chunk invariants (matches Java lines 196-219)
        if num_dirty_chunks > num_chunks {
            return Err(io::Error::other(format!(
                "invalid numDirtyChunks: dirty={num_dirty_chunks} total={num_chunks}"
            )));
        }
        if (num_dirty_chunks == 0) != (num_dirty_docs == 0) {
            return Err(io::Error::other(format!(
                "dirty chunks/docs mismatch: dirtyChunks={num_dirty_chunks} dirtyDocs={num_dirty_docs}"
            )));
        }
        if num_dirty_docs < num_dirty_chunks {
            return Err(io::Error::other(format!(
                "numDirtyDocs < numDirtyChunks: dirtyDocs={num_dirty_docs} dirtyChunks={num_dirty_chunks}"
            )));
        }

        codec_util::check_footer(&mut meta_in)?;

        debug!("term_vectors_reader: {num_chunks} chunks for segment {segment_name}");

        Ok(Self {
            vectors_stream,
            index_reader,
            version,
            packed_ints_version,
            chunk_size,
            num_chunks,
            num_dirty_chunks,
            num_dirty_docs,
        })
    }

    /// Returns the total number of chunks in the term vectors data.
    pub fn num_chunks(&self) -> i64 {
        self.num_chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::lucene90::term_vectors::{CompressingTermVectorsWriter, TermVectorsWriter};
    use crate::store::{MemoryDirectory, SharedDirectory};
    use assertables::*;

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    fn segment_id() -> [u8; 16] {
        [0u8; 16]
    }

    /// Writes docs via the streaming API, finishes, then opens a reader.
    fn write_and_read<F>(num_docs: i32, build_fn: F) -> TermVectorsReader
    where
        F: FnOnce(&mut CompressingTermVectorsWriter),
    {
        let dir = test_directory();
        {
            let mut w = CompressingTermVectorsWriter::new(&dir, "_0", "", &segment_id()).unwrap();
            build_fn(&mut w);
            w.finish(num_docs).unwrap();
        }
        let guard = dir.lock().unwrap();
        TermVectorsReader::open(guard.as_ref(), "_0", "", &segment_id()).unwrap()
    }

    #[test]
    fn test_single_doc() {
        let reader = write_and_read(1, |w| {
            w.start_document(1);
            w.start_field(2, 2, false, false, false);
            w.start_term(b"hello", 1);
            w.finish_term();
            w.start_term(b"world", 1);
            w.finish_term();
            w.finish_field();
            w.finish_document().unwrap();
        });
        assert_eq!(reader.num_chunks(), 1);
    }

    #[test]
    fn test_multiple_docs_one_chunk() {
        let reader = write_and_read(10, |w| {
            for _ in 0..10 {
                w.start_document(1);
                w.start_field(2, 1, false, false, false);
                w.start_term(b"term", 1);
                w.finish_term();
                w.finish_field();
                w.finish_document().unwrap();
            }
        });
        assert_eq!(reader.num_chunks(), 1);
    }

    #[test]
    fn test_multiple_docs_multiple_fields() {
        let reader = write_and_read(5, |w| {
            for _ in 0..5 {
                w.start_document(2);
                w.start_field(0, 1, false, false, false);
                w.start_term(b"alpha", 1);
                w.finish_term();
                w.finish_field();
                w.start_field(1, 1, false, false, false);
                w.start_term(b"beta", 1);
                w.finish_term();
                w.finish_field();
                w.finish_document().unwrap();
            }
        });
        assert_ge!(reader.num_chunks(), 1);
    }
}
