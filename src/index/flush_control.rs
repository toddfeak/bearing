// SPDX-License-Identifier: Apache-2.0
//! Flush control that tracks active and pending-flush segment workers.

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

use crate::index::segment_worker::SegmentWorker;

/// Maximum number of workers that can be flushing concurrently before
/// stalling new indexing threads.
const MAX_CONCURRENT_FLUSHES: usize = 4;

/// Coordinates which workers flush and when.
///
/// Uses Mutex + Condvar for stall control: if too many workers are flushing
/// concurrently, indexing threads block until a flush completes.
pub struct FlushControl {
    inner: Mutex<FlushControlInner>,
    stall_cvar: Condvar,
}

struct FlushControlInner {
    /// Workers waiting to be flushed.
    flush_queue: VecDeque<SegmentWorker>,
    /// Number of workers currently being flushed.
    flushing_count: usize,
}

impl Default for FlushControl {
    fn default() -> Self {
        Self::new()
    }
}

impl FlushControl {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(FlushControlInner {
                flush_queue: VecDeque::new(),
                flushing_count: 0,
            }),
            stall_cvar: Condvar::new(),
        }
    }

    /// Signals that a flush has completed, unblocking stalled threads.
    pub fn flush_completed(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.flushing_count = inner.flushing_count.saturating_sub(1);
        drop(inner);
        self.stall_cvar.notify_all();
    }

    /// Blocks if too many concurrent flushes are in progress.
    pub fn wait_if_stalled(&self) {
        let mut inner = self.inner.lock().unwrap();
        while inner.flushing_count >= MAX_CONCURRENT_FLUSHES {
            inner = self.stall_cvar.wait(inner).unwrap();
        }
    }

    /// Drains all pending workers from the flush queue.
    pub fn drain_pending(&self) -> Vec<SegmentWorker> {
        self.inner.lock().unwrap().flush_queue.drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    impl FlushControl {
        /// Adds a worker to the flush queue.
        fn enqueue_for_flush(&self, worker: SegmentWorker) {
            self.inner.lock().unwrap().flush_queue.push_back(worker);
        }

        /// Takes the next worker from the flush queue, if any.
        fn next_pending_flush(&self) -> Option<SegmentWorker> {
            let mut inner = self.inner.lock().unwrap();
            if let Some(worker) = inner.flush_queue.pop_front() {
                inner.flushing_count += 1;
                Some(worker)
            } else {
                None
            }
        }

        /// Returns the number of workers waiting in the flush queue.
        fn pending_count(&self) -> usize {
            self.inner.lock().unwrap().flush_queue.len()
        }

        /// Returns the number of workers currently being flushed.
        fn flushing_count(&self) -> usize {
            self.inner.lock().unwrap().flushing_count
        }
    }

    fn make_worker(name: &str) -> SegmentWorker {
        use crate::store::memory::MemoryDirectory;
        use std::sync::{Arc, Mutex};
        let dir = Arc::new(Mutex::new(
            Box::new(MemoryDirectory::new()) as Box<dyn crate::store::Directory>
        ));
        SegmentWorker::new(name.to_string(), HashMap::new(), 0, dir)
    }

    #[test]
    fn test_flush_control_default() {
        let fc = FlushControl::default();
        assert_eq!(fc.pending_count(), 0);
        assert_eq!(fc.flushing_count(), 0);
    }

    #[test]
    fn test_enqueue_and_dequeue() {
        let fc = FlushControl::new();
        fc.enqueue_for_flush(make_worker("_0"));
        fc.enqueue_for_flush(make_worker("_1"));

        assert_eq!(fc.pending_count(), 2);

        let d0 = fc.next_pending_flush().unwrap();
        assert_eq!(d0.segment_name(), "_0");
        assert_eq!(fc.flushing_count(), 1);

        let d1 = fc.next_pending_flush().unwrap();
        assert_eq!(d1.segment_name(), "_1");
        assert_eq!(fc.flushing_count(), 2);

        assert!(fc.next_pending_flush().is_none());
    }

    #[test]
    fn test_flush_completed_decrements() {
        let fc = FlushControl::new();
        fc.enqueue_for_flush(make_worker("_0"));
        let _d = fc.next_pending_flush();
        assert_eq!(fc.flushing_count(), 1);

        fc.flush_completed();
        assert_eq!(fc.flushing_count(), 0);
    }

    #[test]
    fn test_drain_pending() {
        let fc = FlushControl::new();
        fc.enqueue_for_flush(make_worker("_0"));
        fc.enqueue_for_flush(make_worker("_1"));

        let drained = fc.drain_pending();
        assert_eq!(drained.len(), 2);
        assert_eq!(fc.pending_count(), 0);
    }
}
