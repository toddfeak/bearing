// SPDX-License-Identifier: Apache-2.0

//! Byte-level writing primitives.
//!
//! [`DataOutput`] defines the interface for writing primitive types
//! (integers, strings, variable-length encodings) to an output stream.
//! [`DataOutputWriter`] adapts any `DataOutput` to [`io::Write`].
//! [`VecOutput`] is a convenience adapter for writing into a `Vec<u8>`.

use std::collections::HashMap;
use std::io;

use crate::encoding::group_vint;
use crate::encoding::string;
use crate::encoding::varint;

/// Trait for writing primitive types to an output stream.
pub trait DataOutput {
    /// Writes a single byte.
    fn write_byte(&mut self, b: u8) -> io::Result<()>;

    /// Writes a slice of bytes.
    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<()>;

    /// Writes a 32-bit integer in **little-endian** byte order.
    fn write_le_int(&mut self, i: i32) -> io::Result<()> {
        self.write_bytes(&i.to_le_bytes())
    }

    /// Writes a 16-bit short in **little-endian** byte order.
    fn write_le_short(&mut self, i: i16) -> io::Result<()> {
        self.write_bytes(&i.to_le_bytes())
    }

    /// Writes a 64-bit long in **little-endian** byte order.
    fn write_le_long(&mut self, i: i64) -> io::Result<()> {
        self.write_bytes(&i.to_le_bytes())
    }

    /// Writes a variable-length integer (1-5 bytes). High bit = continuation.
    fn write_vint(&mut self, i: i32) -> io::Result<()> {
        varint::write_vint(&mut DataOutputWriter(self), i)
    }

    /// Writes a variable-length long (1-9 bytes). High bit = continuation.
    fn write_vlong(&mut self, i: i64) -> io::Result<()> {
        varint::write_vlong(&mut DataOutputWriter(self), i)
    }

    /// Writes a zigzag-encoded variable-length int.
    fn write_zint(&mut self, i: i32) -> io::Result<()> {
        varint::write_zint(&mut DataOutputWriter(self), i)
    }

    /// Writes a zigzag-encoded variable-length long.
    fn write_zlong(&mut self, i: i64) -> io::Result<()> {
        varint::write_zlong(&mut DataOutputWriter(self), i)
    }

    /// Writes a variable-length long that may be negative (used by writeZLong).
    fn write_signed_vlong(&mut self, i: i64) -> io::Result<()> {
        varint::write_signed_vlong(&mut DataOutputWriter(self), i)
    }

    /// Writes a 32-bit integer in **big-endian** byte order.
    /// Used by CodecUtil for headers/footers.
    fn write_be_int(&mut self, i: i32) -> io::Result<()> {
        self.write_bytes(&i.to_be_bytes())
    }

    /// Writes a 64-bit long in **big-endian** byte order.
    /// Used by CodecUtil for headers/footers.
    fn write_be_long(&mut self, i: i64) -> io::Result<()> {
        self.write_bytes(&i.to_be_bytes())
    }

    /// Writes a string as VInt-encoded byte length followed by UTF-8 bytes.
    fn write_string(&mut self, s: &str) -> io::Result<()> {
        string::write_string(&mut DataOutputWriter(self), s)
    }

    /// Writes a set of strings: VInt count followed by each string.
    fn write_set_of_strings(&mut self, set: &[String]) -> io::Result<()> {
        string::write_set_of_strings(&mut DataOutputWriter(self), set)
    }

    /// Writes a map of strings: VInt count followed by key-value pairs.
    fn write_map_of_strings(&mut self, map: &HashMap<String, String>) -> io::Result<()> {
        string::write_map_of_strings(&mut DataOutputWriter(self), map)
    }

    /// Writes integers using group-varint encoding.
    /// Groups of 4 are encoded with a flag byte (2 bits per int = byte width - 1)
    /// followed by the ints in LE with variable byte widths.
    /// Remaining values (< 4) are written as regular VInts.
    fn write_group_vints(&mut self, values: &[i32], limit: usize) -> io::Result<()> {
        group_vint::write_group_vints(&mut DataOutputWriter(self), values, limit)
    }
}

/// Adapter that wraps a [`DataOutput`] reference as an [`io::Write`].
///
/// This allows encoding functions (which accept `&mut impl io::Write`) to work
/// with any [`DataOutput`] or [`super::IndexOutput`].
pub struct DataOutputWriter<'a, T: ?Sized>(pub &'a mut T);

impl<T: DataOutput + ?Sized> io::Write for DataOutputWriter<'_, T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write_bytes(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A [`DataOutput`] adapter for writing directly into a `Vec<u8>`.
///
/// Use this instead of defining ad-hoc wrapper structs in individual codec modules.
/// The inner `Vec<u8>` is borrowed mutably, so you can inspect it after writing.
pub struct VecOutput<'a>(pub &'a mut Vec<u8>);

impl DataOutput for VecOutput<'_> {
    fn write_byte(&mut self, b: u8) -> io::Result<()> {
        self.0.push(b);
        Ok(())
    }

    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.extend_from_slice(buf);
        Ok(())
    }
}
