// SPDX-License-Identifier: Apache-2.0
//! Pool of per-thread document writers for concurrent indexing.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use crate::index::documents_writer_per_thread::DocumentsWriterPerThread;
use crate::index::index_file_names;

/// Thread-safe pool of DocumentsWriterPerThread instances.
///
/// Threads obtain a DWPT from the pool to index documents, then return
/// it when done or enqueue it for flushing.
pub struct DwptPool {
    /// Free DWPTs available for reuse.
    free_list: Mutex<VecDeque<DocumentsWriterPerThread>>,
    /// Next segment number (monotonically increasing).
    segment_counter: Mutex<u64>,
    /// Global field name -> number map, shared across all DWPTs.
    global_field_numbers: Mutex<HashMap<String, u32>>,
    /// Next field number to assign.
    next_field_number: Mutex<u32>,
}

impl Default for DwptPool {
    fn default() -> Self {
        Self::new()
    }
}

impl DwptPool {
    pub fn new() -> Self {
        Self {
            free_list: Mutex::new(VecDeque::new()),
            segment_counter: Mutex::new(0),
            global_field_numbers: Mutex::new(HashMap::new()),
            next_field_number: Mutex::new(0),
        }
    }

    /// Obtains a DWPT from the pool, creating a new one if none are free.
    pub fn obtain(&self) -> DocumentsWriterPerThread {
        let mut free = self.free_list.lock().unwrap();
        if let Some(dwpt) = free.pop_front() {
            return dwpt;
        }
        drop(free);

        // Create a new DWPT
        let segment_name = self.next_segment_name();
        let gfn = self.global_field_numbers.lock().unwrap().clone();
        let nfn = *self.next_field_number.lock().unwrap();

        DocumentsWriterPerThread::new(segment_name, gfn, nfn)
    }

    /// Returns a DWPT to the pool for reuse.
    pub fn release(&self, dwpt: DocumentsWriterPerThread) {
        self.free_list.lock().unwrap().push_back(dwpt);
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

    /// Updates global field numbers from a flushed DWPT.
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

    /// Drains any remaining free DWPTs from the pool.
    pub fn drain_free(&self) -> Vec<DocumentsWriterPerThread> {
        self.free_list.lock().unwrap().drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_obtain_creates_new_dwpt() {
        let pool = DwptPool::new();
        let dwpt = pool.obtain();
        assert_eq!(dwpt.segment_name(), "_0");
        assert_eq!(dwpt.num_docs(), 0);
    }

    #[test]
    fn test_obtain_increments_segment_counter() {
        let pool = DwptPool::new();
        let d0 = pool.obtain();
        let d1 = pool.obtain();
        assert_eq!(d0.segment_name(), "_0");
        assert_eq!(d1.segment_name(), "_1");
    }

    #[test]
    fn test_release_and_reuse() {
        let pool = DwptPool::new();
        let dwpt = pool.obtain();
        let name = dwpt.segment_name().to_string();
        pool.release(dwpt);

        let reused = pool.obtain();
        assert_eq!(reused.segment_name(), name);
    }

    #[test]
    fn test_segment_counter() {
        let pool = DwptPool::new();
        assert_eq!(pool.segment_counter(), 0);
        let _d = pool.obtain();
        assert_eq!(pool.segment_counter(), 1);
    }
}
