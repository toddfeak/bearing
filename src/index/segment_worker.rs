// SPDX-License-Identifier: Apache-2.0
//! Per-thread segment worker that buffers documents in memory before flushing.

// Each SegmentWorker wraps an IndexingChain and a segment name. It is exclusively
// owned by one thread at a time, and consumed on flush.

use std::collections::HashMap;
use std::io;

use std::sync::Arc;

use crate::analysis::Analyzer;
use crate::codecs::lucene90::term_vectors::TermVectorChunkWriter;
use crate::document::Document;
use crate::index::index_file_names;
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
    directory: Arc<SharedDirectory>,
    /// Segment ID, generated at worker creation for use by streaming codec writers.
    segment_id: [u8; 16],
    /// Streaming term vector writer, lazily created on first TV doc.
    tv_writer: Option<TermVectorChunkWriter>,
}

impl SegmentWorker {
    /// Creates a new worker with the given segment name, global field numbers, and directory.
    pub fn new(
        segment_name: String,
        global_field_numbers: HashMap<String, u32>,
        next_field_number: u32,
        directory: Arc<SharedDirectory>,
    ) -> Self {
        Self {
            chain: IndexingChain::with_global_field_numbers(
                global_field_numbers,
                next_field_number,
            ),
            segment_name,
            directory,
            segment_id: string_helper::random_id(),
            tv_writer: None,
        }
    }

    /// Adds a document to this worker's indexing chain, streaming term vectors.
    pub fn add_document(&mut self, doc: Document, analyzer: &dyn Analyzer) -> io::Result<()> {
        self.chain.process_document(doc, analyzer)?;

        // Stream term vector data to chunk writer instead of accumulating
        if let Some(tv_doc) = self.chain.take_last_tv_doc()
            && !tv_doc.fields.is_empty()
        {
            let writer = self.ensure_tv_writer()?;
            writer.add_doc(&tv_doc)?;
        }
        Ok(())
    }

    /// Lazily creates the term vector chunk writer on first TV doc.
    fn ensure_tv_writer(&mut self) -> io::Result<&mut TermVectorChunkWriter> {
        if self.tv_writer.is_none() {
            let tvd_name = index_file_names::segment_file_name(&self.segment_name, "", "tvd");
            let tvd = {
                let mut dir = self.directory.lock().unwrap();
                dir.create_output(&tvd_name)?
            };
            self.tv_writer = Some(TermVectorChunkWriter::new(tvd, &self.segment_id, "")?);
        }
        Ok(self.tv_writer.as_mut().unwrap())
    }

    /// Returns the number of documents buffered in this worker.
    pub fn num_docs(&self) -> i32 {
        self.chain.num_docs()
    }

    /// Returns the estimated RAM bytes used by this worker's buffered data.
    pub fn ram_bytes_used(&self) -> usize {
        let chain_bytes = self.chain.ram_bytes_used();
        let tv_bytes = self.tv_writer.as_ref().map_or(0, |w| w.ram_bytes_used());
        chain_bytes + tv_bytes
    }

    /// Logs a per-component memory breakdown for debugging.
    pub fn log_ram_breakdown(&self, label: &str) {
        self.chain.log_ram_breakdown(label);
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
    /// Segment files are written to the worker's directory immediately.
    pub(crate) fn flush(mut self, use_compound_file: bool) -> io::Result<FlushedSegment> {
        if self.chain.num_docs() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot flush empty segment worker",
            ));
        }

        // Finalize all pending postings (encode the last in-progress doc
        // for every PostingList into the byte stream) before flush.
        self.chain.finalize_pending_postings();

        // Finalize streaming TV writer if present
        let tv_file_names = if let Some(tv_writer) = self.tv_writer.take() {
            Some(tv_writer.finish(
                &self.directory,
                &self.segment_name,
                "",
                &self.segment_id,
                self.chain.num_docs(),
            )?)
        } else {
            None
        };

        let state = Self::build_write_state(
            &self.chain,
            &self.segment_name,
            self.segment_id,
            use_compound_file,
        );
        flush_segment_to_files(&state, &self.directory, use_compound_file, tv_file_names)
    }

    fn build_write_state<'a>(
        chain: &'a IndexingChain,
        segment_name: &str,
        segment_id: [u8; 16],
        use_compound_file: bool,
    ) -> SegmentWriteState<'a> {
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
    use std::sync::Arc;

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
        let dir = Arc::new(test_directory());
        let mut worker = SegmentWorker::new("_0".to_string(), HashMap::new(), 0, Arc::clone(&dir));
        let analyzer = StandardAnalyzer::new();

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/test.txt"));
        doc.add(document::long_field("modified", 1000));
        doc.add(document::text_field("contents", "hello world"));
        worker.add_document(doc, &analyzer).unwrap();

        assert_eq!(worker.num_docs(), 1);
        assert_eq!(worker.segment_name(), "_0");

        let flushed = worker.flush(true).unwrap();
        assert_eq!(flushed.segment_commit_info.info.name, "_0");
        assert_eq!(flushed.segment_commit_info.info.max_doc, 1);
        assert_not_empty!(flushed.file_names);
    }

    #[test]
    fn test_worker_flush_empty_fails() {
        let dir = Arc::new(test_directory());
        let worker = SegmentWorker::new("_0".to_string(), HashMap::new(), 0, Arc::clone(&dir));
        assert!(worker.flush(true).is_err());
    }
}
