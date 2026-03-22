// SPDX-License-Identifier: Apache-2.0

//! Directory-level reader that opens a Lucene index and provides access to all segments.
//!
//! [`DirectoryReader`] is the primary entry point for reading a Lucene index.
//! It reads the `segments_N` commit point, creates a [`SegmentReader`] for each
//! segment, and provides access to them via [`LeafReaderContext`].
//!
//! # Example
//!
//! ```no_run
//! use bearing::index::directory_reader::DirectoryReader;
//! use bearing::store::FSDirectory;
//! use std::path::Path;
//!
//! let dir = FSDirectory::open(Path::new("/path/to/index")).unwrap();
//! let reader = DirectoryReader::open(&dir).unwrap();
//!
//! println!("Index has {} documents across {} segments",
//!     reader.max_doc(), reader.leaves().len());
//!
//! for leaf in reader.leaves() {
//!     let seg = &leaf.reader;
//!     println!("  Segment {}: {} docs, {} fields",
//!         seg.segment_name(), seg.max_doc(), seg.field_infos().len());
//! }
//! ```

use std::io;

use crate::index::segment_infos;
use crate::index::segment_reader::SegmentReader;
use crate::store::Directory;

/// Per-segment context providing a [`SegmentReader`] and its position within the index.
///
/// Each segment has a `doc_base` — the absolute document ID offset for this
/// segment within the full index. Local document IDs from the segment reader
/// are converted to global IDs by adding `doc_base`.
pub struct LeafReaderContext {
    /// The ordinal position of this segment (0-based).
    pub ord: usize,
    /// The absolute document ID offset for this segment.
    ///
    /// Global doc ID = `doc_base + local_doc_id`.
    pub doc_base: i32,
    /// The segment reader for this leaf.
    pub reader: SegmentReader,
}

/// Opens a Lucene index directory and provides access to all segments.
///
/// Reads the latest `segments_N` commit point and creates a [`SegmentReader`]
/// for each segment. Segments are accessed via [`leaves()`](Self::leaves),
/// which returns [`LeafReaderContext`] wrappers that include the segment's
/// position in the overall document ID space.
///
/// This is the read-side counterpart to [`super::IndexWriter`] — it opens
/// indexes that were written by either Bearing or Java Lucene.
pub struct DirectoryReader {
    segments: Box<[LeafReaderContext]>,
    max_doc: i32,
}

impl DirectoryReader {
    /// Opens the latest committed index from the given directory.
    ///
    /// Finds the `segments_N` file, reads the commit point, and opens a
    /// [`SegmentReader`] for each segment. All codec readers are initialized
    /// lazily — only metadata is read during construction.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory contains no `segments_N` file, or if
    /// any segment file is missing or corrupt.
    pub fn open(directory: &dyn Directory) -> io::Result<Self> {
        let files = directory.list_all()?;
        let segments_file = files
            .iter()
            .filter(|f| f.starts_with("segments_"))
            .max()
            .ok_or_else(|| io::Error::other("no segments_N file found in directory"))?;

        let infos = segment_infos::read(directory, segments_file)?;

        let mut segments = Vec::with_capacity(infos.segments.len());
        let mut doc_base = 0i32;

        for (ord, seg) in infos.segments.iter().enumerate() {
            let reader = SegmentReader::open(directory, &seg.name, &seg.id)?;
            let max_doc = reader.max_doc();

            segments.push(LeafReaderContext {
                ord,
                doc_base,
                reader,
            });

            doc_base += max_doc;
        }

        Ok(Self {
            max_doc: doc_base,
            segments: segments.into_boxed_slice(),
        })
    }

    /// Returns the total number of documents across all segments (including deleted).
    pub fn max_doc(&self) -> i32 {
        self.max_doc
    }

    /// Returns the number of live (non-deleted) documents.
    ///
    /// Currently equivalent to [`max_doc()`](Self::max_doc) since deletes are
    /// not yet supported.
    pub fn num_docs(&self) -> i32 {
        self.max_doc
    }

    /// Returns the per-segment leaf reader contexts.
    ///
    /// Each [`LeafReaderContext`] provides the segment reader and its position
    /// in the overall document ID space. Queries iterate over leaves to search
    /// each segment independently.
    pub fn leaves(&self) -> &[LeafReaderContext] {
        &self.segments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{self, Document};
    use crate::index::{IndexWriter, IndexWriterConfig};
    use crate::store::MemoryDirectory;
    use assertables::*;

    fn write_index(num_docs: usize, compound: bool) -> Box<dyn Directory> {
        let config = IndexWriterConfig::new().set_use_compound_file(compound);
        let writer = IndexWriter::with_config(config);

        for i in 0..num_docs {
            let mut doc = Document::new();
            doc.add(document::text_field("content", &format!("doc {i}")));
            writer.add_document(doc).unwrap();
        }

        let result = writer.commit().unwrap();
        let seg_files = result.into_segment_files().unwrap();

        let mut mem_dir = MemoryDirectory::new();
        for sf in &seg_files {
            mem_dir.write_file(&sf.name, &sf.data).unwrap();
        }
        Box::new(mem_dir)
    }

    #[test]
    fn test_open_single_segment() {
        let dir = write_index(5, false);
        let reader = DirectoryReader::open(dir.as_ref()).unwrap();

        assert_eq!(reader.max_doc(), 5);
        assert_eq!(reader.num_docs(), 5);
        assert_eq!(reader.leaves().len(), 1);

        let leaf = &reader.leaves()[0];
        assert_eq!(leaf.ord, 0);
        assert_eq!(leaf.doc_base, 0);
        assert_eq!(leaf.reader.max_doc(), 5);
    }

    #[test]
    fn test_open_compound() {
        let dir = write_index(3, true);
        let reader = DirectoryReader::open(dir.as_ref()).unwrap();

        assert_eq!(reader.max_doc(), 3);
        assert_eq!(reader.leaves().len(), 1);
    }

    #[test]
    fn test_multi_segment() {
        // Write two separate commits to get multiple segments
        let config = IndexWriterConfig::new()
            .set_use_compound_file(false)
            .set_max_buffered_docs(2);
        let writer = IndexWriter::with_config(config);

        for i in 0..5 {
            let mut doc = Document::new();
            doc.add(document::text_field("content", &format!("doc {i}")));
            writer.add_document(doc).unwrap();
        }

        let result = writer.commit().unwrap();
        let seg_files = result.into_segment_files().unwrap();

        let mut mem_dir = MemoryDirectory::new();
        for sf in &seg_files {
            mem_dir.write_file(&sf.name, &sf.data).unwrap();
        }
        let dir = Box::new(mem_dir) as Box<dyn Directory>;

        let reader = DirectoryReader::open(dir.as_ref()).unwrap();

        // With max_buffered_docs=2, 5 docs should create multiple segments
        assert_eq!(reader.max_doc(), 5);
        assert_gt!(reader.leaves().len(), 1);

        // Verify doc_base is cumulative
        let mut expected_base = 0;
        for (i, leaf) in reader.leaves().iter().enumerate() {
            assert_eq!(leaf.ord, i);
            assert_eq!(leaf.doc_base, expected_base);
            expected_base += leaf.reader.max_doc();
        }
        assert_eq!(expected_base, 5);
    }

    #[test]
    fn test_leaf_reader_access() {
        // Write a doc with a stored field
        let config = IndexWriterConfig::new().set_use_compound_file(false);
        let writer = IndexWriter::with_config(config);
        let mut doc = Document::new();
        doc.add(document::string_field("path", "/test.txt", true));
        writer.add_document(doc).unwrap();

        let result = writer.commit().unwrap();
        let seg_files = result.into_segment_files().unwrap();
        let mut mem_dir = MemoryDirectory::new();
        for sf in &seg_files {
            mem_dir.write_file(&sf.name, &sf.data).unwrap();
        }

        let mut reader = DirectoryReader::open(&mem_dir).unwrap();

        // Access stored fields through the hierarchy
        let leaf = &mut reader.segments[0];
        let sfr = leaf.reader.stored_fields_reader().unwrap();
        let fields = sfr.document(0).unwrap();
        assert!(!fields.is_empty());
    }

    #[test]
    fn test_empty_directory_fails() {
        let dir = Box::new(MemoryDirectory::new()) as Box<dyn Directory>;
        let result = DirectoryReader::open(dir.as_ref());
        assert!(result.is_err());
    }
}
