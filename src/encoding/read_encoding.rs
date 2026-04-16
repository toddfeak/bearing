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
