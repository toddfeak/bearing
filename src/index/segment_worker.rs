// SPDX-License-Identifier: Apache-2.0
//! Per-thread segment worker that buffers documents in memory before flushing.

// Each SegmentWorker wraps an IndexingChain and a segment name. It is exclusively
// owned by one thread at a time, and consumed on flush.

use std::collections::HashMap;
use std::io;

use crate::analysis::Analyzer;
use crate::document::Document;
use crate::index::index_writer::{FlushedSegment, SegmentWriteState, flush_segment_to_files};
use crate::index::indexing_chain::IndexingChain;
use crate::index::{SegmentCommitInfo, SegmentInfo};
use crate::store::SharedDirectory;
use crate::util::string_helper;

/// A per-thread segment worker that accumulates documents into an IndexingChain.
///
/// Created by the SegmentWorkerPool, exclusively owned by one thread, consumed on flush.
pub struct SegmentWorker {
    chain: IndexingChain,
    segment_name: String,
}

impl SegmentWorker {
    /// Creates a new worker with the given segment name and global field numbers.
    pub fn new(
        segment_name: String,
        global_field_numbers: HashMap<String, u32>,
        next_field_number: u32,
    ) -> Self {
        Self {
            chain: IndexingChain::with_global_field_numbers(
                global_field_numbers,
                next_field_number,
            ),
            segment_name,
        }
    }

    /// Adds a document to this worker's indexing chain.
    pub fn add_document(&mut self, doc: Document, analyzer: &dyn Analyzer) -> io::Result<()> {
        self.chain.process_document(doc, analyzer)
    }

    /// Returns the number of documents buffered in this worker.
    pub fn num_docs(&self) -> i32 {
        self.chain.num_docs()
    }

    /// Returns the estimated RAM bytes used by this worker's buffered data.
    pub fn ram_bytes_used(&self) -> usize {
        self.chain.ram_bytes_used()
    }

    /// Returns the segment name for this worker.
    pub fn segment_name(&self) -> &str {
        &self.segment_name
    }

    /// Returns an iterator over field name -> field number mappings.
    pub fn field_number_mappings(&self) -> impl Iterator<Item = (&str, u32)> {
        self.chain.field_number_mappings()
    }

    /// Consumes this worker, flushing its chain into a FlushedSegment.
    ///
    /// Segment files are written to the given directory immediately.
    pub(crate) fn flush(
        mut self,
        directory: &SharedDirectory,
        use_compound_file: bool,
    ) -> io::Result<FlushedSegment> {
        if self.chain.num_docs() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot flush empty segment worker",
            ));
        }

        // Finalize all pending postings (encode the last in-progress doc
        // for every PostingList into the byte stream) before flush.
        self.chain.finalize_pending_postings();

        let state = Self::build_write_state(&self.chain, &self.segment_name, use_compound_file);
        flush_segment_to_files(&state, directory, use_compound_file)
    }

    fn build_write_state<'a>(
        chain: &'a IndexingChain,
        segment_name: &str,
        use_compound_file: bool,
    ) -> SegmentWriteState<'a> {
        let segment_id = string_helper::random_id();
        let field_infos = chain.build_field_infos();

        let mut diagnostics = HashMap::new();
        diagnostics.insert("source".to_string(), "flush".to_string());
        diagnostics.insert("os.name".to_string(), std::env::consts::OS.to_string());
        diagnostics.insert("os.arch".to_string(), std::env::consts::ARCH.to_string());
        diagnostics.insert("java.runtime.version".to_string(), "bearing".to_string());

        let mut attributes = HashMap::new();
        attributes.insert(
            "Lucene90StoredFieldsFormat.mode".to_string(),
            "BEST_SPEED".to_string(),
        );

        let segment_info = SegmentInfo::new(
            segment_name.to_string(),
            chain.num_docs(),
            use_compound_file,
            segment_id,
            diagnostics,
            attributes,
        );

        let segment_commit_info = SegmentCommitInfo::new(
            segment_info,
            field_infos.clone(),
            Some(string_helper::random_id()),
        );

        SegmentWriteState {
            segment_commit_info,
            field_infos,
            chain,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::standard::StandardAnalyzer;
    use crate::document;
    use crate::store::SharedDirectory;
    use crate::store::memory::MemoryDirectory;

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    #[test]
    fn test_worker_add_and_flush() {
        let mut worker = SegmentWorker::new("_0".to_string(), HashMap::new(), 0);
        let analyzer = StandardAnalyzer::new();

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/test.txt"));
        doc.add(document::long_field("modified", 1000));
        doc.add(document::text_field("contents", "hello world"));
        worker.add_document(doc, &analyzer).unwrap();

        assert_eq!(worker.num_docs(), 1);
        assert_eq!(worker.segment_name(), "_0");

        let dir = test_directory();
        let flushed = worker.flush(&dir, true).unwrap();
        assert_eq!(flushed.segment_commit_info.info.name, "_0");
        assert_eq!(flushed.segment_commit_info.info.max_doc, 1);
        assert_not_empty!(flushed.file_names);
    }

    #[test]
    fn test_worker_flush_empty_fails() {
        let dir = test_directory();
        let worker = SegmentWorker::new("_0".to_string(), HashMap::new(), 0);
        assert!(worker.flush(&dir, true).is_err());
    }
}
