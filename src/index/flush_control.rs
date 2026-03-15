// SPDX-License-Identifier: Apache-2.0
//! Flush control that tracks active and pending-flush document writers.

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

use crate::index::documents_writer_per_thread::DocumentsWriterPerThread;

/// Maximum number of DWPTs that can be flushing concurrently before
/// stalling new indexing threads.
const MAX_CONCURRENT_FLUSHES: usize = 4;

/// Coordinates which DWPTs flush and when.
///
/// Uses Mutex + Condvar for stall control: if too many DWPTs are flushing
/// concurrently, indexing threads block until a flush completes.
pub struct FlushControl {
    inner: Mutex<FlushControlInner>,
    stall_cvar: Condvar,
}

struct FlushControlInner {
    /// DWPTs waiting to be flushed.
    flush_queue: VecDeque<DocumentsWriterPerThread>,
    /// Number of DWPTs currently being flushed.
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

    /// Adds a DWPT to the flush queue.
    pub fn enqueue_for_flush(&self, dwpt: DocumentsWriterPerThread) {
        self.inner.lock().unwrap().flush_queue.push_back(dwpt);
    }

    /// Takes the next DWPT from the flush queue, if any.
    ///
    /// Increments the flushing count so stall control knows how many
    /// are in flight.
    pub fn next_pending_flush(&self) -> Option<DocumentsWriterPerThread> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(dwpt) = inner.flush_queue.pop_front() {
            inner.flushing_count += 1;
            Some(dwpt)
        } else {
            None
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

    /// Returns the number of DWPTs waiting in the flush queue.
    pub fn pending_count(&self) -> usize {
        self.inner.lock().unwrap().flush_queue.len()
    }

    /// Returns the number of DWPTs currently being flushed.
    pub fn flushing_count(&self) -> usize {
        self.inner.lock().unwrap().flushing_count
    }

    /// Drains all pending DWPTs from the flush queue.
    pub fn drain_pending(&self) -> Vec<DocumentsWriterPerThread> {
        self.inner.lock().unwrap().flush_queue.drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_dwpt(name: &str) -> DocumentsWriterPerThread {
        DocumentsWriterPerThread::new(name.to_string(), HashMap::new(), 0)
    }

    #[test]
    fn test_enqueue_and_dequeue() {
        let fc = FlushControl::new();
        fc.enqueue_for_flush(make_dwpt("_0"));
        fc.enqueue_for_flush(make_dwpt("_1"));

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
        fc.enqueue_for_flush(make_dwpt("_0"));
        let _d = fc.next_pending_flush();
        assert_eq!(fc.flushing_count(), 1);

        fc.flush_completed();
        assert_eq!(fc.flushing_count(), 0);
    }

    #[test]
    fn test_drain_pending() {
        let fc = FlushControl::new();
        fc.enqueue_for_flush(make_dwpt("_0"));
        fc.enqueue_for_flush(make_dwpt("_1"));

        let drained = fc.drain_pending();
        assert_eq!(drained.len(), 2);
        assert_eq!(fc.pending_count(), 0);
    }
}
