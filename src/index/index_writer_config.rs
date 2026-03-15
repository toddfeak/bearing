// Ported from org.apache.lucene.index.IndexWriterConfig

/// Configuration for an IndexWriter.
///
/// Controls flush behavior (when in-memory segments are written to disk).
/// Matches Java's IndexWriterConfig defaults:
/// - `max_buffered_docs = -1` (disabled)
/// - `ram_buffer_size_mb = 16.0` (flush when a DWPT exceeds 16 MB)
pub struct IndexWriterConfig {
    /// Maximum number of documents buffered in memory before a flush is triggered.
    /// -1 means doc-count flushing is disabled.
    max_buffered_docs: i32,
    /// RAM buffer size in megabytes. When a DWPT's estimated RAM usage
    /// exceeds this threshold, it is flushed to disk. 0.0 or negative disables
    /// RAM-based flushing. Default: 16.0 (matches Java Lucene).
    ram_buffer_size_mb: f64,
}

impl IndexWriterConfig {
    /// Default RAM buffer size matching Java Lucene's IndexWriterConfig.
    pub const DEFAULT_RAM_BUFFER_SIZE_MB: f64 = 16.0;

    /// Creates a new config with Java Lucene defaults:
    /// RAM-based flushing at 16 MB, doc-count flushing disabled.
    pub fn new() -> Self {
        Self {
            max_buffered_docs: -1,
            ram_buffer_size_mb: Self::DEFAULT_RAM_BUFFER_SIZE_MB,
        }
    }

    /// Sets the maximum number of documents buffered before flushing.
    /// -1 disables doc-count-based flushing.
    pub fn set_max_buffered_docs(mut self, max: i32) -> Self {
        self.max_buffered_docs = max;
        self
    }

    /// Returns the maximum buffered docs setting.
    pub fn max_buffered_docs(&self) -> i32 {
        self.max_buffered_docs
    }

    /// Sets the RAM buffer size in megabytes.
    /// 0.0 or negative disables RAM-based flushing.
    pub fn set_ram_buffer_size_mb(mut self, mb: f64) -> Self {
        self.ram_buffer_size_mb = mb;
        self
    }

    /// Returns the RAM buffer size in megabytes.
    pub fn ram_buffer_size_mb(&self) -> f64 {
        self.ram_buffer_size_mb
    }

    /// Returns the RAM buffer size as bytes, or 0 if disabled.
    pub fn ram_buffer_size_bytes(&self) -> usize {
        if self.ram_buffer_size_mb > 0.0 {
            (self.ram_buffer_size_mb * 1024.0 * 1024.0) as usize
        } else {
            0
        }
    }
}

impl Default for IndexWriterConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = IndexWriterConfig::new();
        assert_eq!(config.max_buffered_docs(), -1);
        assert!((config.ram_buffer_size_mb() - 16.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_set_max_buffered_docs() {
        let config = IndexWriterConfig::new().set_max_buffered_docs(100);
        assert_eq!(config.max_buffered_docs(), 100);
    }

    #[test]
    fn test_set_ram_buffer_size_mb() {
        let config = IndexWriterConfig::new().set_ram_buffer_size_mb(32.0);
        assert!((config.ram_buffer_size_mb() - 32.0).abs() < f64::EPSILON);
        assert_eq!(config.ram_buffer_size_bytes(), 32 * 1024 * 1024);
    }

    #[test]
    fn test_ram_buffer_disabled() {
        let config = IndexWriterConfig::new().set_ram_buffer_size_mb(0.0);
        assert_eq!(config.ram_buffer_size_bytes(), 0);

        let config2 = IndexWriterConfig::new().set_ram_buffer_size_mb(-1.0);
        assert_eq!(config2.ram_buffer_size_bytes(), 0);
    }

    #[test]
    fn test_builder_chaining() {
        let config = IndexWriterConfig::new()
            .set_max_buffered_docs(50)
            .set_ram_buffer_size_mb(8.0);
        assert_eq!(config.max_buffered_docs(), 50);
        assert!((config.ram_buffer_size_mb() - 8.0).abs() < f64::EPSILON);
    }
}
