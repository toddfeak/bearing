// SPDX-License-Identifier: Apache-2.0

//! Flush coordination for multi-threaded indexing.
//!
//! Workers call [`FlushControl::after_document`] after each document to
//! report memory usage and advance their document count. FlushControl
//! evaluates both RAM and document-count thresholds, signaling workers
//! to flush when either is exceeded. After flushing, workers call
//! [`FlushControl::reset_worker`] to zero their counters.

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

use log::debug;

/// Fraction of `ram_buffer_size_bytes` to target after signaling flushes.
/// When total RAM exceeds the threshold, enough workers are signaled
/// (largest first) to bring total RAM below `ram_buffer_size_bytes * FLUSH_TARGET_RATIO`.
const FLUSH_TARGET_RATIO: f64 = 0.8;

/// Shared flush coordination state for multi-threaded indexing.
///
/// Each worker thread has a slot (indexed by `worker_id`) for tracking
/// RAM usage and document count. After each document, the worker calls
/// [`after_document`](Self::after_document) which evaluates flush
/// thresholds and signals workers as needed.
///
/// Thread safety: all state is atomic. Multiple workers can call
/// `after_document` concurrently. Races are benign — at worst,
/// slightly more workers flush than strictly necessary.
#[derive(Debug)]
pub struct FlushControl {
    per_worker_bytes: Vec<AtomicU64>,
    per_worker_docs: Vec<AtomicI32>,
    flush_signals: Vec<AtomicBool>,
    ram_buffer_size_bytes: u64,
    max_buffered_docs: i32,
}

impl FlushControl {
    /// Creates a new `FlushControl` for `num_workers` threads.
    ///
    /// `ram_buffer_size_mb` is the total RAM threshold in megabytes.
    /// A value of `0.0` disables RAM-based flushing.
    ///
    /// `max_buffered_docs` is the per-worker document count threshold.
    /// A value of `-1` disables document-count flushing.
    pub fn new(num_workers: usize, ram_buffer_size_mb: f64, max_buffered_docs: i32) -> Self {
        let ram_buffer_size_bytes = (ram_buffer_size_mb * 1024.0 * 1024.0) as u64;
        let per_worker_bytes = (0..num_workers)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        let per_worker_docs = (0..num_workers)
            .map(|_| AtomicI32::new(0))
            .collect::<Vec<_>>();
        let flush_signals = (0..num_workers)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>();

        Self {
            per_worker_bytes,
            per_worker_docs,
            flush_signals,
            ram_buffer_size_bytes,
            max_buffered_docs,
        }
    }

    /// Called after each document is processed by a worker.
    ///
    /// Increments the worker's document count, stores its current RAM
    /// usage, and evaluates flush thresholds:
    ///
    /// - **Document count:** If this worker's doc count reaches
    ///   `max_buffered_docs`, it is signaled directly.
    /// - **RAM:** If total RAM across all workers exceeds the threshold,
    ///   enough workers (largest first) are signaled to bring total
    ///   below the target.
    pub fn after_document(&self, worker_id: usize, ram_bytes: u64) {
        let doc_count = self.per_worker_docs[worker_id].fetch_add(1, Ordering::Relaxed) + 1;
        self.per_worker_bytes[worker_id].store(ram_bytes, Ordering::Relaxed);

        // Document count threshold (per-worker).
        if self.max_buffered_docs > 0 && doc_count >= self.max_buffered_docs {
            self.flush_signals[worker_id].store(true, Ordering::Relaxed);
            return;
        }

        // RAM threshold (global).
        if self.ram_buffer_size_bytes == 0 {
            return;
        }

        let total: u64 = self
            .per_worker_bytes
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .sum();

        if total < self.ram_buffer_size_bytes {
            return;
        }

        let target = (self.ram_buffer_size_bytes as f64 * FLUSH_TARGET_RATIO) as u64;

        // Collect per-worker usage and sort descending by bytes.
        let mut workers: Vec<(usize, u64)> = self
            .per_worker_bytes
            .iter()
            .enumerate()
            .map(|(id, b)| (id, b.load(Ordering::Relaxed)))
            .collect();
        workers.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        debug!(
            "flush_control: total={:.1}MB limit={:.1}MB target={:.1}MB workers=[{}]",
            total as f64 / 1_048_576.0,
            self.ram_buffer_size_bytes as f64 / 1_048_576.0,
            target as f64 / 1_048_576.0,
            workers
                .iter()
                .map(|(id, b)| format!("w{}={:.1}MB", id, *b as f64 / 1_048_576.0))
                .collect::<Vec<_>>()
                .join(", "),
        );

        // Signal workers largest-first until projected total <= target.
        let mut bytes_to_flush: u64 = 0;
        let mut signaled: Vec<usize> = Vec::new();
        for (id, worker_bytes) in &workers {
            if total.saturating_sub(bytes_to_flush) <= target {
                break;
            }
            if *worker_bytes == 0 {
                break;
            }
            self.flush_signals[*id].store(true, Ordering::Relaxed);
            bytes_to_flush += worker_bytes;
            signaled.push(*id);
        }

        debug!(
            "flush_control: signaling flush for workers {:?} ({:.1}MB)",
            signaled,
            bytes_to_flush as f64 / 1_048_576.0,
        );
    }

    /// Returns `true` if this worker has been signaled to flush,
    /// clearing the signal atomically.
    pub fn should_flush(&self, worker_id: usize) -> bool {
        self.flush_signals[worker_id].swap(false, Ordering::Relaxed)
    }

    /// Resets a worker's counters after a flush.
    ///
    /// Zeros both the document count and RAM usage for the given worker.
    /// Called by the worker thread after flushing and before resuming
    /// with a replacement worker.
    pub fn reset_worker(&self, worker_id: usize) {
        self.per_worker_docs[worker_id].store(0, Ordering::Relaxed);
        self.per_worker_bytes[worker_id].store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;

    #[test]
    fn all_disabled_never_signals() {
        let fc = FlushControl::new(2, 0.0, -1);
        fc.after_document(0, 1_000_000_000);
        assert!(!fc.should_flush(0));
        assert!(!fc.should_flush(1));
    }

    #[test]
    fn ram_below_threshold_no_signal() {
        let fc = FlushControl::new(2, 1.0, -1);
        fc.after_document(0, 400_000);
        fc.after_document(1, 400_000);
        assert!(!fc.should_flush(0));
        assert!(!fc.should_flush(1));
    }

    #[test]
    fn ram_above_threshold_signals_largest() {
        let fc = FlushControl::new(3, 1.0, -1); // 1 MB = 1_048_576 bytes
        fc.after_document(0, 200_000);
        fc.after_document(1, 500_000);
        fc.after_document(2, 400_000); // total = 1_100_000, exceeds 1 MB

        // Target = 1_048_576 * 0.8 = 838_860
        // Largest is worker 1 (500_000). After flushing: 1_100_000 - 500_000 = 600_000 <= 838_860
        assert!(!fc.should_flush(0));
        assert!(fc.should_flush(1));
        assert!(!fc.should_flush(2));
    }

    #[test]
    fn ram_signals_multiple_workers_when_needed() {
        let fc = FlushControl::new(3, 1.0, -1); // 1 MB = 1_048_576 bytes
        fc.after_document(0, 400_000);
        fc.after_document(1, 500_000);
        fc.after_document(2, 450_000); // total = 1_350_000

        // Target = 838_860
        // Largest first: worker 1 (500_000), remaining = 850_000 > 838_860
        // Next: worker 2 (450_000), remaining = 400_000 <= 838_860. Stop.
        assert!(!fc.should_flush(0));
        assert!(fc.should_flush(1));
        assert!(fc.should_flush(2));
    }

    #[test]
    fn doc_count_signals_worker() {
        let fc = FlushControl::new(2, 0.0, 3); // 3 docs, RAM disabled
        fc.after_document(0, 100);
        fc.after_document(0, 200);
        assert!(!fc.should_flush(0));

        fc.after_document(0, 300); // 3rd doc — hits threshold
        assert!(fc.should_flush(0));
        // Other worker unaffected.
        assert!(!fc.should_flush(1));
    }

    #[test]
    fn doc_count_is_per_worker() {
        let fc = FlushControl::new(2, 0.0, 3);
        fc.after_document(0, 100);
        fc.after_document(0, 200);
        fc.after_document(1, 100); // worker 1 only has 1 doc

        fc.after_document(0, 300); // worker 0 hits 3
        assert!(fc.should_flush(0));
        assert!(!fc.should_flush(1));
    }

    #[test]
    fn should_flush_clears_signal() {
        let fc = FlushControl::new(1, 1.0, -1);
        fc.after_document(0, 2_000_000);
        assert!(fc.should_flush(0));
        assert!(!fc.should_flush(0));
    }

    #[test]
    fn reset_worker_clears_counters() {
        let fc = FlushControl::new(1, 0.0, 3);
        fc.after_document(0, 100);
        fc.after_document(0, 200);
        // 2 docs, not yet at threshold.

        fc.reset_worker(0);

        // After reset, need 3 more docs to trigger.
        fc.after_document(0, 100);
        fc.after_document(0, 200);
        assert!(!fc.should_flush(0));
        fc.after_document(0, 300);
        assert!(fc.should_flush(0));
    }

    #[test]
    fn reset_worker_clears_ram() {
        let fc = FlushControl::new(2, 1.0, -1);
        fc.after_document(0, 600_000);
        fc.after_document(1, 600_000); // total = 1.2M, over threshold

        let w0 = fc.should_flush(0);
        let w1 = fc.should_flush(1);
        assert!(w0 || w1);

        // Reset the flushed worker.
        if w0 {
            fc.reset_worker(0);
        }
        if w1 {
            fc.reset_worker(1);
        }

        // No signals pending — total back under threshold.
        assert!(!fc.should_flush(0));
        assert!(!fc.should_flush(1));
    }

    #[test]
    fn single_worker_signals_itself() {
        let fc = FlushControl::new(1, 1.0, -1);
        fc.after_document(0, 2_000_000);
        assert!(fc.should_flush(0));
    }

    #[test]
    fn zero_byte_workers_not_signaled() {
        let fc = FlushControl::new(3, 1.0, -1);
        fc.after_document(0, 0);
        fc.after_document(1, 0);
        fc.after_document(2, 1_200_000);

        assert!(!fc.should_flush(0));
        assert!(!fc.should_flush(1));
        assert!(fc.should_flush(2));
    }

    #[test]
    fn concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        let fc = Arc::new(FlushControl::new(4, 1.0, -1));
        let mut handles = Vec::new();

        for id in 0..4 {
            let fc = Arc::clone(&fc);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    fc.after_document(id, (i + 1) * 10_000);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // After all updates, total = 4 * 1_000_000 = 4 MB, well over 1 MB.
        let any_signaled = (0..4).any(|id| fc.should_flush(id));
        assert!(any_signaled);
    }

    #[test]
    fn doc_count_triggers_before_ram() {
        // Small doc count threshold, large RAM buffer — doc count fires first.
        let fc = FlushControl::new(1, 1000.0, 2);
        fc.after_document(0, 100);
        assert!(!fc.should_flush(0));
        fc.after_document(0, 200); // 2nd doc, hits max_buffered_docs
        assert!(fc.should_flush(0));
    }

    #[test]
    fn flush_target_ratio_constant() {
        assert_gt!(FLUSH_TARGET_RATIO, 0.5);
        assert_lt!(FLUSH_TARGET_RATIO, 1.0);
    }
}
