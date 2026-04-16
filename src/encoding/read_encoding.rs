// SPDX-License-Identifier: Apache-2.0

//! Extension trait providing encoding-aware read methods on any [`Read`].
//!
//! [`ReadEncoding`] is blanket-implemented for all [`Read`] types, adding
//! methods like [`read_vint`](ReadEncoding::read_vint) and
//! [`read_string`](ReadEncoding::read_string) that decode Lucene's wire formats.

use std::collections::HashMap;
use std::io;
use std::io::Read;

use crate::encoding::group_vint;
use crate::encoding::string;
use crate::encoding::varint;

/// Encoding-aware read methods available on any [`Read`] type.
pub trait ReadEncoding: Read {
    /// Reads a variable-length integer (1-5 bytes). High bit = continuation.
    fn read_vint(&mut self) -> io::Result<i32>;

    /// Reads a variable-length long (1-9 bytes). High bit = continuation.
    fn read_vlong(&mut self) -> io::Result<i64>;

    /// Reads a zigzag-encoded variable-length int.
    fn read_zint(&mut self) -> io::Result<i32>;

    /// Reads a zigzag-encoded variable-length long.
    fn read_zlong(&mut self) -> io::Result<i64>;

    /// Reads a string: VInt-encoded byte length followed by UTF-8 bytes.
    fn read_string(&mut self) -> io::Result<String>;

    /// Reads a set of strings: VInt count followed by each string.
    fn read_set_of_strings(&mut self) -> io::Result<Vec<String>>;

    /// Reads a map of strings: VInt count followed by key-value pairs.
    fn read_map_of_strings(&mut self) -> io::Result<HashMap<String, String>>;

    /// Reads integers using group-varint encoding.
    fn read_group_vints(&mut self, values: &mut [i32], limit: usize) -> io::Result<()>;
}

impl<T: Read> ReadEncoding for T {
    fn read_vint(&mut self) -> io::Result<i32> {
        varint::read_vint(self)
    }

    fn read_vlong(&mut self) -> io::Result<i64> {
        varint::read_vlong(self)
    }

    fn read_zint(&mut self) -> io::Result<i32> {
        varint::read_zint(self)
    }

    fn read_zlong(&mut self) -> io::Result<i64> {
        varint::read_zlong(self)
    }

    fn read_string(&mut self) -> io::Result<String> {
        string::read_string(self)
    }

    fn read_set_of_strings(&mut self) -> io::Result<Vec<String>> {
        string::read_set_of_strings(self)
    }

    fn read_map_of_strings(&mut self) -> io::Result<HashMap<String, String>> {
        string::read_map_of_strings(self)
    }

    fn read_group_vints(&mut self, values: &mut [i32], limit: usize) -> io::Result<()> {
        group_vint::read_group_vints(self, values, limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::write_encoding::WriteEncoding;
    use crate::store::DataInput;
    use crate::store::byte_slice_input::ByteSliceIndexInput;

    /// Verifies the blanket impl works on a concrete type.
    #[test]
    fn test_vint_roundtrip_on_concrete() {
        let mut buf = Vec::new();
        buf.write_vint(16384).unwrap();
        let mut cursor = &buf[..];
        assert_eq!(cursor.read_vint().unwrap(), 16384);
    }

    /// Verifies the blanket impl works on a trait object (&mut dyn DataInput).
    #[test]
    fn test_vint_on_dyn_data_input() {
        let mut buf = Vec::new();
        buf.write_vint(42).unwrap();
        let mut input = ByteSliceIndexInput::new("test".into(), buf);
        let mut input: &mut dyn DataInput = &mut input;
        assert_eq!(input.read_vint().unwrap(), 42);
    }

    /// Verifies the blanket impl works on Box<dyn DataInput>.
    #[test]
    fn test_vlong_on_box_dyn() {
        let mut buf = Vec::new();
        buf.write_vlong(0x7FFF_FFFF_FFFF_FFFF).unwrap();
        let mut input: Box<dyn DataInput> = Box::new(ByteSliceIndexInput::new("test".into(), buf));
        assert_eq!(input.read_vlong().unwrap(), 0x7FFF_FFFF_FFFF_FFFF);
    }

    /// Verifies string roundtrip through the blanket impl.
    #[test]
    fn test_string_roundtrip() {
        let mut buf = Vec::new();
        buf.write_string("hello world").unwrap();
        let mut cursor = &buf[..];
        assert_eq!(cursor.read_string().unwrap(), "hello world");
    }

    /// Verifies zigzag roundtrip.
    #[test]
    fn test_zigzag_roundtrip() {
        for &val in &[0, 1, -1, 127, -128, i32::MIN, i32::MAX] {
            let mut buf = Vec::new();
            buf.write_zint(val).unwrap();
            let mut cursor = &buf[..];
            assert_eq!(cursor.read_zint().unwrap(), val);
        }
    }
}
