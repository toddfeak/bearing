// SPDX-License-Identifier: Apache-2.0
//! Pool of per-thread segment workers for concurrent indexing.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::index::index_file_names;
use crate::index::segment_worker::SegmentWorker;
use crate::store::SharedDirectory;

/// Thread-safe pool of SegmentWorker instances.
///
/// Threads obtain a worker from the pool to index documents, then return
/// it when done or enqueue it for flushing.
pub struct SegmentWorkerPool {
    /// Free workers available for reuse.
    free_list: Mutex<VecDeque<SegmentWorker>>,
    /// Next segment number (monotonically increasing).
    segment_counter: Mutex<u64>,
    /// Global field name -> number map, shared across all workers.
    global_field_numbers: Mutex<HashMap<String, u32>>,
    /// Next field number to assign.
    next_field_number: Mutex<u32>,
}

impl Default for SegmentWorkerPool {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentWorkerPool {
    pub fn new() -> Self {
        Self {
            free_list: Mutex::new(VecDeque::new()),
            segment_counter: Mutex::new(0),
            global_field_numbers: Mutex::new(HashMap::new()),
            next_field_number: Mutex::new(0),
        }
    }

    /// Obtains a worker from the pool, creating a new one if none are free.
    pub fn obtain(&self, directory: &Arc<SharedDirectory>) -> SegmentWorker {
        let mut free = self.free_list.lock().unwrap();
        if let Some(worker) = free.pop_front() {
            return worker;
        }
        drop(free);

        // Create a new worker
        let segment_name = self.next_segment_name();
        let gfn = self.global_field_numbers.lock().unwrap().clone();
        let nfn = *self.next_field_number.lock().unwrap();

        SegmentWorker::new(segment_name, gfn, nfn, Arc::clone(directory))
    }

    /// Returns a worker to the pool for reuse.
    pub fn release(&self, worker: SegmentWorker) {
        self.free_list.lock().unwrap().push_back(worker);
    }

    /// Generates the next segment name using radix-36 encoding.
    fn next_segment_name(&self) -> String {
        let mut counter = self.segment_counter.lock().unwrap();
        let name = format!("_{}", index_file_names::radix36(*counter));
        *counter += 1;
        name
    }

    /// Returns the current segment counter value.
    pub fn segment_counter(&self) -> u64 {
        *self.segment_counter.lock().unwrap()
    }

    /// Updates global field numbers from a flushed worker.
    pub fn update_field_numbers<'a>(&self, mappings: impl Iterator<Item = (&'a str, u32)>) {
        let mut gfn = self.global_field_numbers.lock().unwrap();
        let mut nfn = self.next_field_number.lock().unwrap();
        for (name, number) in mappings {
            gfn.entry(name.to_string()).or_insert(number);
            if number >= *nfn {
                *nfn = number + 1;
            }
        }
    }

    /// Drains any remaining free workers from the pool.
    pub fn drain_free(&self) -> Vec<SegmentWorker> {
        self.free_list.lock().unwrap().drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryDirectory;

    fn test_directory() -> Arc<SharedDirectory> {
        Arc::new(Mutex::new(Box::new(MemoryDirectory::new())))
    }

    #[test]
    fn test_obtain_creates_new_worker() {
        let pool = SegmentWorkerPool::new();
        let dir = test_directory();
        let worker = pool.obtain(&dir);
        assert_eq!(worker.segment_name(), "_0");
        assert_eq!(worker.num_docs(), 0);
    }

    #[test]
    fn test_obtain_increments_segment_counter() {
        let pool = SegmentWorkerPool::new();
        let dir = test_directory();
        let d0 = pool.obtain(&dir);
        let d1 = pool.obtain(&dir);
        assert_eq!(d0.segment_name(), "_0");
        assert_eq!(d1.segment_name(), "_1");
    }

    #[test]
    fn test_release_and_reuse() {
        let pool = SegmentWorkerPool::new();
        let dir = test_directory();
        let worker = pool.obtain(&dir);
        let name = worker.segment_name().to_string();
        pool.release(worker);

        let reused = pool.obtain(&dir);
        assert_eq!(reused.segment_name(), name);
    }

    #[test]
    fn test_segment_counter() {
        let pool = SegmentWorkerPool::new();
        let dir = test_directory();
        assert_eq!(pool.segment_counter(), 0);
        let _d = pool.obtain(&dir);
        assert_eq!(pool.segment_counter(), 1);
    }
}
