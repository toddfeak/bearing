// SPDX-License-Identifier: Apache-2.0

/// Configuration for an [`IndexWriter`](crate::index::writer::IndexWriter).
///
/// # Defaults
///
/// | Field | Default |
/// |---|---|
/// | `num_threads` | `1` |
/// | `ram_buffer_size_mb` | `16.0` |
/// | `max_buffered_docs` | `-1` (disabled) |
/// | `use_compound_file` | `false` |
///
/// # Example
///
/// ```
/// use bearing::index::config::IndexWriterConfig;
///
/// let config = IndexWriterConfig::default()
///     .num_threads(4)
///     .ram_buffer_size_mb(64.0)
///     .use_compound_file(true);
/// ```
#[derive(Debug, Clone)]
pub struct IndexWriterConfig {
    num_threads: usize,
    ram_buffer_size_mb: f64,
    max_buffered_docs: i32,
    use_compound_file: bool,
}

impl IndexWriterConfig {
    /// Sets the number of threads in the internal indexing pool.
    pub fn num_threads(mut self, n: usize) -> Self {
        self.num_threads = n;
        self
    }

    /// Sets the RAM budget in MB for buffered documents before flushing.
    pub fn ram_buffer_size_mb(mut self, mb: f64) -> Self {
        self.ram_buffer_size_mb = mb;
        self
    }

    /// Sets the maximum number of documents per segment before flushing.
    /// Use -1 to disable (flush is RAM-driven only).
    pub fn max_buffered_docs(mut self, n: i32) -> Self {
        self.max_buffered_docs = n;
        self
    }

    /// Sets whether to package segment files into compound format (.cfs/.cfe).
    pub fn use_compound_file(mut self, enabled: bool) -> Self {
        self.use_compound_file = enabled;
        self
    }

    /// Returns the number of threads in the internal indexing pool.
    pub fn get_num_threads(&self) -> usize {
        self.num_threads
    }

    /// Returns the RAM budget in MB for buffered documents before flushing.
    pub fn get_ram_buffer_size_mb(&self) -> f64 {
        self.ram_buffer_size_mb
    }

    /// Returns the maximum number of documents per segment before flushing.
    pub fn get_max_buffered_docs(&self) -> i32 {
        self.max_buffered_docs
    }

    /// Returns whether segment files are packaged into compound format.
    pub fn get_use_compound_file(&self) -> bool {
        self.use_compound_file
    }
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

#[cfg(test)]
mod tests {
    use assertables::*;

    use super::*;

    #[test]
    fn test_default_values() {
        let config = IndexWriterConfig::default();
        assert_eq!(config.get_num_threads(), 1);
        assert_in_delta!(config.get_ram_buffer_size_mb(), 16.0, f64::EPSILON);
        assert_eq!(config.get_max_buffered_docs(), -1);
        assert!(!config.get_use_compound_file());
    }

    #[test]
    fn test_builder_chain() {
        let config = IndexWriterConfig::default()
            .num_threads(4)
            .ram_buffer_size_mb(64.0)
            .max_buffered_docs(1000)
            .use_compound_file(true);

        assert_eq!(config.get_num_threads(), 4);
        assert_in_delta!(config.get_ram_buffer_size_mb(), 64.0, f64::EPSILON);
        assert_eq!(config.get_max_buffered_docs(), 1000);
        assert!(config.get_use_compound_file());
    }

    #[test]
    fn test_partial_builder() {
        let config = IndexWriterConfig::default().use_compound_file(true);

        assert_eq!(config.get_num_threads(), 1);
        assert!(config.get_use_compound_file());
    }
}
