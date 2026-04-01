// SPDX-License-Identifier: Apache-2.0

//! RAM-based flush coordination for multi-threaded indexing.
//!
//! Workers report their memory usage after each document. When total
//! RAM across all workers exceeds the configured buffer size,
//! `FlushControl` signals enough workers (largest first) to bring
//! usage back below a target threshold.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use log::debug;

/// Fraction of `ram_buffer_size_bytes` to target after signaling flushes.
/// When total RAM exceeds the threshold, enough workers are signaled
/// (largest first) to bring total RAM below `ram_buffer_size_bytes * FLUSH_TARGET_RATIO`.
const FLUSH_TARGET_RATIO: f64 = 0.8;

/// Shared flush coordination state for multi-threaded indexing.
///
/// Each worker thread has a slot (indexed by `worker_id`) for reporting
/// its current RAM usage. When total usage exceeds the configured
/// threshold, the control signals enough workers to flush (largest
/// first) to bring usage below the target.
///
/// Thread safety: all state is atomic. Multiple workers can call
/// `update_ram_usage` concurrently. Races are benign — at worst,
/// slightly more workers flush than strictly necessary.
#[derive(Debug)]
pub struct FlushControl {
    per_worker_bytes: Vec<AtomicU64>,
    flush_signals: Vec<AtomicBool>,
    ram_buffer_size_bytes: u64,
}

impl FlushControl {
    /// Creates a new `FlushControl` for `num_workers` threads.
    ///
    /// `ram_buffer_size_bytes_mb` is the RAM threshold in megabytes.
    /// A value of `0.0` disables RAM-based flushing.
    pub fn new(num_workers: usize, ram_buffer_size_bytes_mb: f64) -> Self {
        let ram_buffer_size_bytes = (ram_buffer_size_bytes_mb * 1024.0 * 1024.0) as u64;
        let per_worker_bytes = (0..num_workers)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        let flush_signals = (0..num_workers)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>();

        Self {
            per_worker_bytes,
            flush_signals,
            ram_buffer_size_bytes,
        }
    }

    /// Reports a worker's current RAM usage and evaluates flush signals.
    ///
    /// Stores the worker's bytes in its slot, then checks if total RAM
    /// across all workers exceeds the threshold. If so, signals enough
    /// workers (largest first) to bring total below the target.
    pub fn update_ram_usage(&self, worker_id: usize, bytes: u64) {
        self.per_worker_bytes[worker_id].store(bytes, Ordering::Relaxed);

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

    /// Returns `true` if RAM-based flushing is disabled (buffer size is 0).
    pub fn is_disabled(&self) -> bool {
        self.ram_buffer_size_bytes == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;

    #[test]
    fn disabled_never_signals() {
        let fc = FlushControl::new(2, 0.0);
        fc.update_ram_usage(0, 1_000_000_000);
        assert!(!fc.should_flush(0));
        assert!(!fc.should_flush(1));
        assert!(fc.is_disabled());
    }

    #[test]
    fn below_threshold_no_signal() {
        let fc = FlushControl::new(2, 1.0); // 1 MB
        fc.update_ram_usage(0, 400_000);
        fc.update_ram_usage(1, 400_000);
        assert!(!fc.should_flush(0));
        assert!(!fc.should_flush(1));
    }

    #[test]
    fn above_threshold_signals_largest() {
        let fc = FlushControl::new(3, 1.0); // 1 MB = 1_048_576 bytes
        fc.update_ram_usage(0, 200_000);
        fc.update_ram_usage(1, 500_000);
        fc.update_ram_usage(2, 400_000); // total = 1_100_000, exceeds 1 MB

        // Target = 1_048_576 * 0.8 = 838_860
        // Largest is worker 1 (500_000). After flushing: 1_100_000 - 500_000 = 600_000 <= 838_860
        // So only worker 1 should be signaled.
        assert!(!fc.should_flush(0));
        assert!(fc.should_flush(1));
        assert!(!fc.should_flush(2));
    }

    #[test]
    fn signals_multiple_workers_when_needed() {
        let fc = FlushControl::new(3, 1.0); // 1 MB = 1_048_576 bytes
        fc.update_ram_usage(0, 400_000);
        fc.update_ram_usage(1, 500_000);
        fc.update_ram_usage(2, 450_000); // total = 1_350_000

        // Target = 838_860
        // Largest first: worker 1 (500_000), remaining = 850_000 > 838_860
        // Next: worker 2 (450_000), remaining = 400_000 <= 838_860. Stop.
        // Workers 1 and 2 should be signaled.
        assert!(!fc.should_flush(0));
        assert!(fc.should_flush(1));
        assert!(fc.should_flush(2));
    }

    #[test]
    fn should_flush_clears_signal() {
        let fc = FlushControl::new(1, 1.0);
        fc.update_ram_usage(0, 2_000_000); // way over threshold
        assert!(fc.should_flush(0));
        // Second call should be false — signal was cleared.
        assert!(!fc.should_flush(0));
    }

    #[test]
    fn update_after_flush_resets_bytes() {
        let fc = FlushControl::new(2, 1.0); // 1 MB
        fc.update_ram_usage(0, 600_000);
        fc.update_ram_usage(1, 600_000); // total = 1_200_000, over threshold

        // Worker 0 gets signaled (or worker 1 — depends on sort stability).
        // Consume the signal.
        let w0 = fc.should_flush(0);
        let w1 = fc.should_flush(1);
        assert!(w0 || w1);

        // Simulate flush: report zero for the flushed worker.
        if w0 {
            fc.update_ram_usage(0, 0);
        }
        if w1 {
            fc.update_ram_usage(1, 0);
        }

        // No signals should be pending now (total should be below threshold).
        assert!(!fc.should_flush(0));
        assert!(!fc.should_flush(1));
    }

    #[test]
    fn single_worker_signals_itself() {
        let fc = FlushControl::new(1, 1.0);
        fc.update_ram_usage(0, 2_000_000);
        assert!(fc.should_flush(0));
    }

    #[test]
    fn zero_byte_workers_not_signaled() {
        let fc = FlushControl::new(3, 1.0);
        fc.update_ram_usage(0, 0);
        fc.update_ram_usage(1, 0);
        fc.update_ram_usage(2, 1_200_000); // over threshold, but only worker 2 has bytes

        assert!(!fc.should_flush(0));
        assert!(!fc.should_flush(1));
        assert!(fc.should_flush(2));
    }

    #[test]
    fn concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        let fc = Arc::new(FlushControl::new(4, 1.0));
        let mut handles = Vec::new();

        for id in 0..4 {
            let fc = Arc::clone(&fc);
            handles.push(thread::spawn(move || {
                // Each worker reports increasing RAM over 100 iterations.
                for i in 0..100 {
                    fc.update_ram_usage(id, (i + 1) * 10_000);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // After all updates, total = 4 * 1_000_000 = 4 MB, well over 1 MB.
        // At least one worker should be signaled.
        let any_signaled = (0..4).any(|id| fc.should_flush(id));
        assert!(any_signaled);
    }

    #[test]
    fn flush_target_ratio_constant() {
        // Verify the constant is in a reasonable range.
        assert_gt!(FLUSH_TARGET_RATIO, 0.5);
        assert_lt!(FLUSH_TARGET_RATIO, 1.0);
    }
}
