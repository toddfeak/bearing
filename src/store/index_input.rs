// SPDX-License-Identifier: Apache-2.0

//! Index file input with position tracking and random access.

use std::io;

use crate::store::DataInput;

/// Trait for index file input with position tracking and random access.
pub trait IndexInput: DataInput + Send + Sync {
    /// Returns the name of this input (the file name).
    fn name(&self) -> &str;

    /// Returns the current read position (byte offset).
    fn file_pointer(&self) -> u64;

    /// Sets the read position to the given byte offset.
    fn seek(&mut self, pos: u64) -> io::Result<()>;

    /// Returns the total length of the file in bytes.
    fn length(&self) -> u64;

    /// Creates a new IndexInput representing a slice of this input.
    fn slice(&self, description: &str, offset: u64, length: u64)
    -> io::Result<Box<dyn IndexInput>>;

    /// Returns a [`RandomAccessInput`] for absolute-position reads over the
    /// full extent of this input. Used by data structures like tries that
    /// navigate by absolute file pointer.
    fn random_access(&self) -> io::Result<Box<dyn RandomAccessInput>>;
}

/// Absolute-position reads without mutating seek state.
///
/// Mirrors Java's `RandomAccessInput`. Designed for data structures like tries
/// that navigate by absolute file pointer rather than sequential reads.
/// All multi-byte reads use **little-endian** byte order, matching Java's
/// `RandomAccessInput` convention.
pub trait RandomAccessInput: Send {
    /// Reads a single byte at the given absolute position.
    fn read_byte_at(&self, pos: u64) -> io::Result<u8>;

    /// Reads a 2-byte little-endian short at the given absolute position.
    fn read_le_short_at(&self, pos: u64) -> io::Result<i16>;

    /// Reads a 4-byte little-endian int at the given absolute position.
    fn read_le_int_at(&self, pos: u64) -> io::Result<i32>;

    /// Reads an 8-byte little-endian long at the given absolute position.
    fn read_le_long_at(&self, pos: u64) -> io::Result<i64>;
}
