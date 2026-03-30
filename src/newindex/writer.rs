// SPDX-License-Identifier: Apache-2.0

use std::io;

use crate::newindex::config::IndexWriterConfig;
use crate::newindex::directory::Directory;
use crate::newindex::document::Document;

/// Manages the indexing pipeline: accepts documents, coordinates worker
/// threads, and flushes segments to the directory.
// LOCKED
pub struct IndexWriter {
    config: IndexWriterConfig,
    directory: Box<dyn Directory>,
}

impl IndexWriter {
    /// Creates a new `IndexWriter` with the given configuration and directory.
    pub fn new(config: IndexWriterConfig, directory: Box<dyn Directory>) -> Self {
        Self { config, directory }
    }

    /// Adds a document to the index.
    ///
    /// The document is handed off to the internal worker pool for
    /// processing. This method is safe to call from any thread.
    pub fn add_document(&self, doc: Document) -> io::Result<()> {
        log::debug!("add_document: {} fields", doc.fields().len());
        todo!("hand off to index coordinator")
    }

    /// Commits all pending changes, making them durable and visible to readers.
    ///
    /// Signals the index coordinator to flush any remaining buffered data, then
    /// writes the segments file to the directory.
    pub fn commit(self) -> io::Result<()> {
        todo!("drain index coordinator, write segments file")
    }
}
