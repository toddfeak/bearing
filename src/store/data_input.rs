// SPDX-License-Identifier: Apache-2.0

//! Byte-level reading primitives.
//!
//! [`DataInput`] defines the interface for reading raw bytes from an input
//! stream. Encoding-aware methods (VInt, strings, etc.) are provided by
//! [`ReadEncoding`](crate::encoding::read_encoding::ReadEncoding), which is
//! blanket-implemented for all [`Read`] types.

use std::io;
use std::io::Read;

/// Trait for reading raw bytes from an input stream.
///
/// Extends [`Read`] with a single-byte read and convenience methods for
/// fixed-width integers in little-endian and big-endian byte order. All
/// implementors must also implement [`Read`] with a bulk `read_exact`.
pub trait DataInput: Read {
    /// Reads a single byte.
    fn read_byte(&mut self) -> io::Result<u8>;

    /// Skips over `num_bytes` bytes. Implementations may override for efficiency.
    fn skip_bytes(&mut self, num_bytes: u64) -> io::Result<()> {
        let mut remaining = num_bytes;
        let mut skip_buf = [0u8; 1024];
        while remaining > 0 {
            let to_read = remaining.min(skip_buf.len() as u64) as usize;
            self.read_exact(&mut skip_buf[..to_read])?;
            remaining -= to_read as u64;
        }
        Ok(())
    }

    /// Reads a 16-bit short in **little-endian** byte order.
    fn read_le_short(&mut self) -> io::Result<i16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(i16::from_le_bytes(buf))
    }

    /// Reads a 32-bit integer in **little-endian** byte order.
    fn read_le_int(&mut self) -> io::Result<i32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }

    /// Reads a 64-bit long in **little-endian** byte order.
    fn read_le_long(&mut self) -> io::Result<i64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    /// Reads a 32-bit integer in **big-endian** byte order.
    /// Used by CodecUtil for headers/footers.
    fn read_be_int(&mut self) -> io::Result<i32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }

    /// Reads a 64-bit long in **big-endian** byte order.
    /// Used by CodecUtil for headers/footers.
    fn read_be_long(&mut self) -> io::Result<i64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }
}
