// SPDX-License-Identifier: Apache-2.0

//! Byte-level reading primitives.
//!
//! [`DataInput`] defines the interface for reading primitive types
//! (integers, strings, variable-length encodings) from an input stream.
//! [`DataInputReader`] adapts any `DataInput` to [`io::Read`].

use std::collections::HashMap;
use std::io;

use crate::encoding::group_vint;
use crate::encoding::string;
use crate::encoding::varint;

/// Trait for reading primitive types from an input stream.
pub trait DataInput {
    /// Reads a single byte.
    fn read_byte(&mut self) -> io::Result<u8>;

    /// Reads exactly `buf.len()` bytes into the buffer.
    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<()>;

    /// Skips over `num_bytes` bytes. Implementations may override for efficiency.
    fn skip_bytes(&mut self, num_bytes: u64) -> io::Result<()> {
        let mut remaining = num_bytes;
        let mut skip_buf = [0u8; 1024];
        while remaining > 0 {
            let to_read = remaining.min(skip_buf.len() as u64) as usize;
            self.read_bytes(&mut skip_buf[..to_read])?;
            remaining -= to_read as u64;
        }
        Ok(())
    }

    /// Reads a 16-bit short in **little-endian** byte order.
    fn read_le_short(&mut self) -> io::Result<i16> {
        let mut buf = [0u8; 2];
        self.read_bytes(&mut buf)?;
        Ok(i16::from_le_bytes(buf))
    }

    /// Reads a 32-bit integer in **little-endian** byte order.
    fn read_le_int(&mut self) -> io::Result<i32> {
        let mut buf = [0u8; 4];
        self.read_bytes(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }

    /// Reads a 64-bit long in **little-endian** byte order.
    fn read_le_long(&mut self) -> io::Result<i64> {
        let mut buf = [0u8; 8];
        self.read_bytes(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    /// Reads a variable-length integer (1-5 bytes). High bit = continuation.
    fn read_vint(&mut self) -> io::Result<i32> {
        varint::read_vint(&mut DataInputReader(self))
    }

    /// Reads a variable-length long (1-9 bytes). High bit = continuation.
    fn read_vlong(&mut self) -> io::Result<i64> {
        varint::read_vlong(&mut DataInputReader(self))
    }

    /// Reads a zigzag-encoded variable-length int.
    fn read_zint(&mut self) -> io::Result<i32> {
        varint::read_zint(&mut DataInputReader(self))
    }

    /// Reads a zigzag-encoded variable-length long.
    fn read_zlong(&mut self) -> io::Result<i64> {
        varint::read_zlong(&mut DataInputReader(self))
    }

    /// Reads a 32-bit integer in **big-endian** byte order.
    /// Used by CodecUtil for headers/footers.
    fn read_be_int(&mut self) -> io::Result<i32> {
        let mut buf = [0u8; 4];
        self.read_bytes(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }

    /// Reads a 64-bit long in **big-endian** byte order.
    /// Used by CodecUtil for headers/footers.
    fn read_be_long(&mut self) -> io::Result<i64> {
        let mut buf = [0u8; 8];
        self.read_bytes(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }

    /// Reads a string: VInt-encoded byte length followed by UTF-8 bytes.
    fn read_string(&mut self) -> io::Result<String> {
        string::read_string(&mut DataInputReader(self))
    }

    /// Reads a set of strings: VInt count followed by each string.
    fn read_set_of_strings(&mut self) -> io::Result<Vec<String>> {
        string::read_set_of_strings(&mut DataInputReader(self))
    }

    /// Reads a map of strings: VInt count followed by key-value pairs.
    fn read_map_of_strings(&mut self) -> io::Result<HashMap<String, String>> {
        string::read_map_of_strings(&mut DataInputReader(self))
    }

    /// Reads integers using group-varint encoding.
    fn read_group_vints(&mut self, values: &mut [i32], limit: usize) -> io::Result<()> {
        group_vint::read_group_vints(&mut DataInputReader(self), values, limit)
    }
}

/// Adapter that wraps a [`DataInput`] reference as an [`io::Read`].
///
/// This allows encoding functions (which accept `&mut impl io::Read`) to work
/// with any [`DataInput`].
pub struct DataInputReader<'a, T: ?Sized>(pub &'a mut T);

impl<T: DataInput + ?Sized> io::Read for DataInputReader<'_, T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // Read one byte at a time — encoding functions use read_exact which
        // loops until the buffer is full, so this is correct.
        buf[0] = self.0.read_byte()?;
        Ok(1)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.0.read_bytes(buf)
    }
}

/// Reads a variable-length integer (1-5 bytes) from any `io::Read` source.
///
/// High bit = continuation.
pub fn read_vint(reader: &mut impl io::Read) -> io::Result<i32> {
    varint::read_vint(reader)
}

/// Writes a variable-length integer (1-5 bytes) to any `io::Write` sink.
///
/// High bit = continuation.
pub fn encode_vint(writer: &mut impl io::Write, val: i32) -> io::Result<()> {
    varint::write_vint(writer, val)
}
