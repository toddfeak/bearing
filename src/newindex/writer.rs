// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::sync::Arc;

use crate::newindex::config::IndexWriterConfig;
use crate::newindex::coordinator::{IndexCoordinator, WorkerFactory};
use crate::newindex::directory::Directory;
use crate::newindex::document::Document;
use crate::newindex::id_generator::IdGenerator;
use crate::newindex::segment::FlushedSegment;

/// Manages the indexing pipeline: accepts documents, coordinates worker
/// threads, and flushes segments to the directory.
// LOCKED
pub struct IndexWriter {
    coordinator: IndexCoordinator,
}

impl IndexWriter {
    /// Creates a new `IndexWriter` with the given configuration,
    /// directory, ID generator, and worker factory.
    pub fn new(
        config: IndexWriterConfig,
        directory: Arc<dyn Directory>,
        id_generator: Box<dyn IdGenerator>,
        worker_factory: Arc<dyn WorkerFactory>,
    ) -> Self {
        let coordinator = IndexCoordinator::new(&config, id_generator, directory, worker_factory);
        Self { coordinator }
    }

    /// Adds a document to the index.
    ///
    /// The document is handed off to the internal worker pool for
    /// processing. This method is safe to call from any thread.
    pub fn add_document(&self, doc: Document) -> io::Result<()> {
        log::debug!("add_document: {} fields", doc.fields().len());
        self.coordinator.add_document(doc)
    }

    /// Commits all pending changes: flushes remaining segments and
    /// writes the `segments_N` commit point.
    pub fn commit(self) -> io::Result<Vec<FlushedSegment>> {
        self.coordinator.shutdown()
    }
}
