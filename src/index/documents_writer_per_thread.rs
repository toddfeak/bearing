// SPDX-License-Identifier: Apache-2.0

// Each DWPT wraps an IndexingChain and a segment name. It is exclusively
// owned by one thread at a time, and consumed on flush.

use std::collections::HashMap;
use std::io;

use crate::analysis::Analyzer;
use crate::document::Document;
use crate::index::index_writer::{FlushedSegment, SegmentWriteState, flush_segment_to_files};
use crate::index::indexing_chain::IndexingChain;
use crate::index::{SegmentCommitInfo, SegmentInfo};
use crate::util::string_helper;

/// A per-thread document writer that accumulates documents into an IndexingChain.
///
/// Created by the DwptPool, exclusively owned by one thread, consumed on flush.
pub struct DocumentsWriterPerThread {
    chain: IndexingChain,
    segment_name: String,
}

impl DocumentsWriterPerThread {
    /// Creates a new DWPT with the given segment name and global field numbers.
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

    /// Adds a document to this DWPT's indexing chain.
    pub fn add_document(&mut self, doc: &Document, analyzer: &dyn Analyzer) {
        self.chain.process_document(doc, analyzer);
    }

    /// Returns the number of documents buffered in this DWPT.
    pub fn num_docs(&self) -> i32 {
        self.chain.num_docs()
    }

    /// Returns the estimated RAM bytes used by this DWPT's buffered data.
    pub fn ram_bytes_used(&self) -> usize {
        self.chain.ram_bytes_used()
    }

    /// Returns the segment name for this DWPT.
    pub fn segment_name(&self) -> &str {
        &self.segment_name
    }

    /// Returns an iterator over field name -> field number mappings.
    pub fn field_number_mappings(&self) -> impl Iterator<Item = (&str, u32)> {
        self.chain.field_number_mappings()
    }

    /// Consumes this DWPT, flushing its chain into a FlushedSegment.
    pub(crate) fn flush(mut self) -> io::Result<FlushedSegment> {
        if self.chain.num_docs() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot flush empty DWPT",
            ));
        }

        // Finalize all pending postings (encode the last in-progress doc
        // for every PostingList into the byte stream) before flush.
        self.chain.finalize_pending_postings();

        let state = Self::build_write_state(&self.chain, &self.segment_name);
        flush_segment_to_files(&state)
    }

    fn build_write_state<'a>(
        chain: &'a IndexingChain,
        segment_name: &str,
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
            true,
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

    #[test]
    fn test_dwpt_add_and_flush() {
        let mut dwpt = DocumentsWriterPerThread::new("_0".to_string(), HashMap::new(), 0);
        let analyzer = StandardAnalyzer::new();

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/test.txt"));
        doc.add(document::long_field("modified", 1000));
        doc.add(document::text_field("contents", "hello world"));
        dwpt.add_document(&doc, &analyzer);

        assert_eq!(dwpt.num_docs(), 1);
        assert_eq!(dwpt.segment_name(), "_0");

        let flushed = dwpt.flush().unwrap();
        assert_eq!(flushed.segment_commit_info.info.name, "_0");
        assert_eq!(flushed.segment_commit_info.info.max_doc, 1);
    }

    #[test]
    fn test_dwpt_flush_empty_fails() {
        let dwpt = DocumentsWriterPerThread::new("_0".to_string(), HashMap::new(), 0);
        assert!(dwpt.flush().is_err());
    }
}
