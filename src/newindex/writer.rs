// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::sync::Arc;

use crate::newindex::config::IndexWriterConfig;
use crate::newindex::coordinator::IndexCoordinator;
use crate::newindex::default_worker_factory::DefaultWorkerFactory;
use crate::newindex::document::Document;
use crate::newindex::id_generator::RandomIdGenerator;
use crate::newindex::segment::FlushedSegment;
use crate::store::{Directory, SharedDirectory};

/// Manages the indexing pipeline: accepts documents, coordinates worker
/// threads, and flushes segments to the directory.
// LOCKED
pub struct IndexWriter {
    coordinator: IndexCoordinator,
}

impl IndexWriter {
    /// Creates a new `IndexWriter` for the given directory.
    pub fn new(config: IndexWriterConfig, directory: Box<dyn Directory>) -> Self {
        let directory = Arc::new(SharedDirectory::new(directory));
        let factory = Arc::new(DefaultWorkerFactory::new(Arc::clone(&directory)));
        let coordinator =
            IndexCoordinator::new(&config, Box::new(RandomIdGenerator), directory, factory);
        Self { coordinator }
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
