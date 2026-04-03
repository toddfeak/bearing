// SPDX-License-Identifier: Apache-2.0

//! Adapts the existing `store::Directory` to the `newindex::Directory` trait.

use std::fmt;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use crate::newindex::directory;
use crate::store;

/// Adapts a `store::Directory` to implement `newindex::Directory`.
///
/// Wraps the store directory in a `Mutex` to provide the `&self` interface
/// that `newindex::Directory` requires (the store trait uses `&mut self`).
pub struct DirectoryAdapter {
    inner: Arc<store::SharedDirectory>,
}

impl fmt::Debug for DirectoryAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectoryAdapter").finish_non_exhaustive()
    }
}

impl DirectoryAdapter {
    /// Creates a new adapter wrapping the given store directory.
    pub fn new(directory: Box<dyn store::Directory>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(directory)),
        }
    }

    /// Returns a reference to the underlying shared directory.
    ///
    /// Consumers that need to call existing codec writers can use this
    /// to obtain the `SharedDirectory` handle those writers expect.
    pub fn shared_directory(&self) -> Arc<store::SharedDirectory> {
        Arc::clone(&self.inner)
    }
}

impl directory::Directory for DirectoryAdapter {
    fn create_output(&self, name: &str) -> io::Result<Box<dyn directory::IndexOutput>> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| io::Error::other(e.to_string()))?;
        let store_output = guard.create_output(name)?;
        Ok(Box::new(IndexOutputAdapter {
            inner: store_output,
        }))
    }

    fn sync(&self, names: &[&str]) -> io::Result<()> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| io::Error::other(e.to_string()))?;
        guard.sync(names)
    }

    fn rename(&self, source: &str, dest: &str) -> io::Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| io::Error::other(e.to_string()))?;
        guard.rename(source, dest)
    }
}

/// Adapts a `store::IndexOutput` to implement `newindex::IndexOutput`.
struct IndexOutputAdapter {
    inner: Box<dyn store::IndexOutput>,
}

impl Write for IndexOutputAdapter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write_bytes(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl directory::IndexOutput for IndexOutputAdapter {
    fn file_pointer(&self) -> u64 {
        self.inner.file_pointer()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::newindex::directory::Directory;

    fn make_adapter() -> DirectoryAdapter {
        DirectoryAdapter::new(Box::new(store::MemoryDirectory::new()))
    }

    #[test]
    fn create_output_and_write() {
        let adapter = make_adapter();
        let mut out = adapter.create_output("test.txt").unwrap();
        out.write_all(b"hello").unwrap();
        assert_eq!(out.file_pointer(), 5);
    }

    #[test]
    fn sync_succeeds() {
        let adapter = make_adapter();
        adapter.create_output("a.txt").unwrap();
        adapter.sync(&["a.txt"]).unwrap();
    }

    #[test]
    fn rename_succeeds() {
        let adapter = make_adapter();
        adapter.create_output("old.txt").unwrap();
        adapter.rename("old.txt", "new.txt").unwrap();
    }

    #[test]
    fn shared_directory_is_same_instance() {
        let adapter = make_adapter();
        let sd1 = adapter.shared_directory();
        let sd2 = adapter.shared_directory();
        assert!(Arc::ptr_eq(&sd1, &sd2));
    }

    #[test]
    fn file_written_through_adapter_visible_via_shared_directory() {
        let adapter = make_adapter();
        {
            let mut out = adapter.create_output("data.bin").unwrap();
            out.write_all(b"content").unwrap();
        }
        let sd = adapter.shared_directory();
        let guard = sd.lock().unwrap();
        let data = guard.read_file("data.bin").unwrap();
        assert_eq!(data, b"content");
    }
}
