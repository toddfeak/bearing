// SPDX-License-Identifier: Apache-2.0

/// Configuration for an [`IndexWriter`](super::writer::IndexWriter).
///
/// All fields have sensible defaults. Use the builder methods to override.
// LOCKED
#[derive(Debug, Clone)]
pub struct IndexWriterConfig {
    /// Number of threads in the internal indexing pool.
    /// Default: 1 (single-threaded).
    pub num_threads: usize,

    /// RAM budget in MB for buffered documents before flushing.
    /// Default: 16.0 MB.
    pub ram_buffer_size_mb: f64,

    /// Maximum number of documents per segment before flushing.
    /// -1 disables this threshold (flush is RAM-driven only).
    /// Default: -1.
    pub max_buffered_docs: i32,

    /// Whether to package segment files into compound format (.cfs/.cfe).
    /// Default: false.
    pub use_compound_file: bool,
}

impl Default for IndexWriterConfig {
    fn default() -> Self {
        Self {
            num_threads: 1,
            ram_buffer_size_mb: 16.0,
            max_buffered_docs: -1,
            use_compound_file: false,
        }
    }
}
