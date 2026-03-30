// SPDX-License-Identifier: Apache-2.0

use std::io::{self, Write};

/// Abstraction over file storage for writing index data.
///
/// A flat file store — no subdirectories. Files are created, written
/// sequentially, and synced to stable storage.
// LOCKED
pub trait Directory: Send + Sync {
    /// Creates a new, empty file and returns a writer for appending data.
    fn create_output(&self, name: &str) -> io::Result<Box<dyn IndexOutput>>;

    /// Ensures that writes to the named files are moved to stable storage.
    fn sync(&self, names: &[&str]) -> io::Result<()>;

    /// Atomically renames a file. Used for committing the segments file.
    fn rename(&self, source: &str, dest: &str) -> io::Result<()>;
}

/// Sequential write handle for a single index file.
///
/// Extends `std::io::Write` with position tracking.
// LOCKED
pub trait IndexOutput: Write {
    /// Returns the current write position (number of bytes written).
    fn file_pointer(&self) -> u64;
}
