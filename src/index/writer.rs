// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::sync::Arc;

use crate::document::Document;
use crate::index::config::IndexWriterConfig;
use crate::index::coordinator::IndexCoordinator;
use crate::index::default_worker_factory::DefaultWorkerFactory;
use crate::index::id_generator::RandomIdGenerator;
use crate::index::segment::FlushedSegment;
use crate::store::SharedDirectory;

/// Manages the indexing pipeline: accepts documents, coordinates worker
/// threads, and flushes segments to the directory.
pub struct IndexWriter {
    coordinator: IndexCoordinator,
    directory: Arc<SharedDirectory>,
}

impl IndexWriter {
    /// Creates a new `IndexWriter` for the given directory.
    ///
    /// The caller retains shared access to the directory via `Arc`, matching
    /// Lucene's model where the `Directory` is shared between writer and reader.
    pub fn new(config: IndexWriterConfig, directory: Arc<SharedDirectory>) -> Self {
        let factory = Arc::new(DefaultWorkerFactory::new(Arc::clone(&directory)));
        let coordinator = IndexCoordinator::new(
            &config,
            Box::new(RandomIdGenerator),
            Arc::clone(&directory),
            factory,
        );
        Self {
            coordinator,
            directory,
        }
    }

    /// Returns the directory this writer is writing to.
    pub fn directory(&self) -> &Arc<SharedDirectory> {
        &self.directory
    }

    /// Adds a document to the index.
    ///
    /// The document is handed off to the internal worker pool for
    /// processing. This method is safe to call from any thread.
    pub fn add_document(&self, mut doc: Document) -> io::Result<()> {
        log::debug!("add_document: {} fields", doc.fields().len());
        self.coordinator.add_document(doc)
    }

    /// Commits all pending changes: flushes remaining segments and
    /// writes the `segments_N` commit point.
    pub fn commit(self) -> io::Result<Vec<FlushedSegment>> {
        self.coordinator.shutdown()
    }
}
