// SPDX-License-Identifier: Apache-2.0
//! Flush policies that decide when a document writer should be flushed to disk.

use crate::index::index_writer_config::IndexWriterConfig;

/// Determines when an in-memory segment should be flushed to disk.
pub trait FlushPolicy: Send + Sync {
    /// Returns true if the current worker should be flushed.
    fn should_flush(
        &self,
        num_docs: i32,
        ram_bytes_used: usize,
        config: &IndexWriterConfig,
    ) -> bool;
}

/// Flushes when RAM usage exceeds `ram_buffer_size_mb` OR doc count exceeds
/// `max_buffered_docs`. Matches Java's `FlushByRamOrCountsPolicy`.
///
/// Note: Java's version checks **global** active bytes across all workers, then
/// flushes the largest worker. Our simplified version checks per-worker RAM, which
/// is sufficient for single-threaded indexing and a reasonable approximation
/// for multi-threaded use.
pub struct FlushByRamOrCountsPolicy;

impl FlushPolicy for FlushByRamOrCountsPolicy {
    fn should_flush(
        &self,
        num_docs: i32,
        ram_bytes_used: usize,
        config: &IndexWriterConfig,
    ) -> bool {
        // Doc count trigger (if enabled)
        let max_docs = config.max_buffered_docs();
        if max_docs > 0 && num_docs >= max_docs {
            return true;
        }
        // RAM trigger (if enabled)
        let ram_limit = config.ram_buffer_size_bytes();
        if ram_limit > 0 && ram_bytes_used >= ram_limit {
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Flushes when the number of buffered documents reaches `max_buffered_docs`.
    ///
    /// If `max_buffered_docs` is -1 (disabled), never triggers a flush.
    struct DocCountFlushPolicy;

    impl FlushPolicy for DocCountFlushPolicy {
        fn should_flush(
            &self,
            num_docs: i32,
            _ram_bytes_used: usize,
            config: &IndexWriterConfig,
        ) -> bool {
            let max = config.max_buffered_docs();
            max > 0 && num_docs >= max
        }
    }

    // --- DocCountFlushPolicy tests ---

    #[test]
    fn test_doc_count_flush_disabled() {
        let policy = DocCountFlushPolicy;
        let config = IndexWriterConfig::new(); // max_buffered_docs = -1
        assert!(!policy.should_flush(100, 0, &config));
        assert!(!policy.should_flush(0, 0, &config));
    }

    #[test]
    fn test_doc_count_flush_triggers() {
        let policy = DocCountFlushPolicy;
        let config = IndexWriterConfig::new().set_max_buffered_docs(10);
        assert!(!policy.should_flush(9, 0, &config));
        assert!(policy.should_flush(10, 0, &config));
        assert!(policy.should_flush(11, 0, &config));
    }

    #[test]
    fn test_doc_count_flush_threshold_one() {
        let policy = DocCountFlushPolicy;
        let config = IndexWriterConfig::new().set_max_buffered_docs(1);
        assert!(policy.should_flush(1, 0, &config));
    }

    // --- FlushByRamOrCountsPolicy tests ---

    #[test]
    fn test_ram_or_counts_doc_count_trigger() {
        let policy = FlushByRamOrCountsPolicy;
        let config = IndexWriterConfig::new()
            .set_max_buffered_docs(10)
            .set_ram_buffer_size_mb(0.0); // disable RAM
        assert!(!policy.should_flush(9, 0, &config));
        assert!(policy.should_flush(10, 0, &config));
    }

    #[test]
    fn test_ram_or_counts_ram_trigger() {
        let policy = FlushByRamOrCountsPolicy;
        let config = IndexWriterConfig::new(); // 16 MB default, doc count disabled
        let limit = config.ram_buffer_size_bytes();
        assert!(!policy.should_flush(100, limit - 1, &config));
        assert!(policy.should_flush(100, limit, &config));
        assert!(policy.should_flush(100, limit + 1, &config));
    }

    #[test]
    fn test_ram_or_counts_both_disabled() {
        let policy = FlushByRamOrCountsPolicy;
        let config = IndexWriterConfig::new()
            .set_max_buffered_docs(-1)
            .set_ram_buffer_size_mb(0.0);
        // Never flushes
        assert!(!policy.should_flush(10_000, 100_000_000, &config));
    }

    #[test]
    fn test_ram_or_counts_either_triggers() {
        let policy = FlushByRamOrCountsPolicy;
        let config = IndexWriterConfig::new()
            .set_max_buffered_docs(5)
            .set_ram_buffer_size_mb(1.0);

        // Doc count triggers, RAM below threshold
        assert!(policy.should_flush(5, 100, &config));
        // RAM triggers, doc count below threshold
        assert!(policy.should_flush(1, 2 * 1024 * 1024, &config));
        // Neither triggers
        assert!(!policy.should_flush(4, 100, &config));
    }
}
