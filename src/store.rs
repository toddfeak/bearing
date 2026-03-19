// SPDX-License-Identifier: Apache-2.0

//! Storage abstraction layer: directories, data output, and index output.
//!
//! The [`Directory`] trait abstracts file storage. [`FSDirectory`] writes to the
//! filesystem; [`MemoryDirectory`] holds files in memory (useful for tests).
//! [`DataOutput`] and [`IndexOutput`] define the byte-level writing interface
//! used by codec writers.

pub mod checksum;
pub mod fs;
pub mod memory;

pub use checksum::CRC32;
pub use fs::FSDirectory;
pub use memory::{MemoryDirectory, MemoryIndexOutput};

use std::collections::HashMap;
use std::io;

/// A named in-memory file produced by codec writers during indexing.
#[derive(Clone, Debug)]
pub struct SegmentFile {
    pub name: String,
    pub data: Vec<u8>,
}

/// Trait for writing primitive types to an output stream.
pub trait DataOutput {
    /// Writes a single byte.
    fn write_byte(&mut self, b: u8) -> io::Result<()>;

    /// Writes a slice of bytes.
    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<()>;

    /// Writes a 32-bit integer in **little-endian** byte order.
    fn write_le_int(&mut self, i: i32) -> io::Result<()> {
        self.write_byte(i as u8)?;
        self.write_byte((i >> 8) as u8)?;
        self.write_byte((i >> 16) as u8)?;
        self.write_byte((i >> 24) as u8)
    }

    /// Writes a 16-bit short in **little-endian** byte order.
    fn write_le_short(&mut self, i: i16) -> io::Result<()> {
        self.write_byte(i as u8)?;
        self.write_byte((i >> 8) as u8)
    }

    /// Writes a 64-bit long in **little-endian** byte order.
    fn write_le_long(&mut self, i: i64) -> io::Result<()> {
        self.write_le_int(i as i32)?;
        self.write_le_int((i >> 32) as i32)
    }

    /// Writes a variable-length integer (1-5 bytes). High bit = continuation.
    fn write_vint(&mut self, i: i32) -> io::Result<()> {
        // Treat as unsigned for shifting
        let mut val = i as u32;
        while (val & !0x7F) != 0 {
            self.write_byte(((val & 0x7F) | 0x80) as u8)?;
            val >>= 7;
        }
        self.write_byte(val as u8)
    }

    /// Writes a variable-length long (1-9 bytes). High bit = continuation.
    fn write_vlong(&mut self, i: i64) -> io::Result<()> {
        let mut val = i as u64;
        while (val & !0x7F) != 0 {
            self.write_byte(((val & 0x7F) | 0x80) as u8)?;
            val >>= 7;
        }
        self.write_byte(val as u8)
    }

    /// Writes a zigzag-encoded variable-length int.
    fn write_zint(&mut self, i: i32) -> io::Result<()> {
        self.write_vint(crate::util::bit_util::zig_zag_encode_i32(i))
    }

    /// Writes a zigzag-encoded variable-length long.
    fn write_zlong(&mut self, i: i64) -> io::Result<()> {
        // Use signed vlong for zigzag (allows full i64 range)
        let encoded = crate::util::bit_util::zig_zag_encode_i64(i);
        self.write_signed_vlong(encoded)
    }

    /// Writes a variable-length long that may be negative (used by writeZLong).
    fn write_signed_vlong(&mut self, mut i: i64) -> io::Result<()> {
        while (i & !0x7Fi64) != 0 {
            self.write_byte(((i & 0x7F) | 0x80) as u8)?;
            i = ((i as u64) >> 7) as i64;
        }
        self.write_byte(i as u8)
    }

    /// Writes a 32-bit integer in **big-endian** byte order.
    /// Used by CodecUtil for headers/footers.
    fn write_be_int(&mut self, i: i32) -> io::Result<()> {
        self.write_byte((i >> 24) as u8)?;
        self.write_byte((i >> 16) as u8)?;
        self.write_byte((i >> 8) as u8)?;
        self.write_byte(i as u8)
    }

    /// Writes a 64-bit long in **big-endian** byte order.
    /// Used by CodecUtil for headers/footers.
    fn write_be_long(&mut self, i: i64) -> io::Result<()> {
        self.write_be_int((i >> 32) as i32)?;
        self.write_be_int(i as i32)
    }

    /// Writes a string as VInt-encoded byte length followed by UTF-8 bytes.
    fn write_string(&mut self, s: &str) -> io::Result<()> {
        let bytes = s.as_bytes();
        self.write_vint(bytes.len() as i32)?;
        self.write_bytes(bytes)
    }

    /// Writes a set of strings: VInt count followed by each string.
    fn write_set_of_strings(&mut self, set: &[String]) -> io::Result<()> {
        self.write_vint(set.len() as i32)?;
        for s in set {
            self.write_string(s)?;
        }
        Ok(())
    }

    /// Writes a map of strings: VInt count followed by key-value pairs.
    fn write_map_of_strings(&mut self, map: &HashMap<String, String>) -> io::Result<()> {
        self.write_vint(map.len() as i32)?;
        for (k, v) in map {
            self.write_string(k)?;
            self.write_string(v)?;
        }
        Ok(())
    }

    /// Writes integers using group-varint encoding.
    /// Groups of 4 are encoded with a flag byte (2 bits per int = byte width - 1)
    /// followed by the ints in LE with variable byte widths.
    /// Remaining values (< 4) are written as regular VInts.
    fn write_group_vints(&mut self, values: &[i32], limit: usize) -> io::Result<()> {
        let mut read_pos = 0;
        let mut scratch = [0u8; 17]; // 1 flag + 4 * 4 bytes max

        while limit - read_pos >= 4 {
            let mut write_pos = 0;
            let n1m1 = num_bytes_for_group_vint(values[read_pos]) - 1;
            let n2m1 = num_bytes_for_group_vint(values[read_pos + 1]) - 1;
            let n3m1 = num_bytes_for_group_vint(values[read_pos + 2]) - 1;
            let n4m1 = num_bytes_for_group_vint(values[read_pos + 3]) - 1;
            let flag = (n1m1 << 6) | (n2m1 << 4) | (n3m1 << 2) | n4m1;
            scratch[write_pos] = flag as u8;
            write_pos += 1;

            // Write each int in LE, only the needed bytes
            let le = (values[read_pos] as u32).to_le_bytes();
            scratch[write_pos..write_pos + n1m1 as usize + 1]
                .copy_from_slice(&le[..n1m1 as usize + 1]);
            write_pos += n1m1 as usize + 1;
            read_pos += 1;

            let le = (values[read_pos] as u32).to_le_bytes();
            scratch[write_pos..write_pos + n2m1 as usize + 1]
                .copy_from_slice(&le[..n2m1 as usize + 1]);
            write_pos += n2m1 as usize + 1;
            read_pos += 1;

            let le = (values[read_pos] as u32).to_le_bytes();
            scratch[write_pos..write_pos + n3m1 as usize + 1]
                .copy_from_slice(&le[..n3m1 as usize + 1]);
            write_pos += n3m1 as usize + 1;
            read_pos += 1;

            let le = (values[read_pos] as u32).to_le_bytes();
            scratch[write_pos..write_pos + n4m1 as usize + 1]
                .copy_from_slice(&le[..n4m1 as usize + 1]);
            write_pos += n4m1 as usize + 1;
            read_pos += 1;

            self.write_bytes(&scratch[..write_pos])?;
        }

        // Tail values as regular VInts
        while read_pos < limit {
            self.write_vint(values[read_pos])?;
            read_pos += 1;
        }

        Ok(())
    }
}

/// Returns the number of bytes needed to represent a non-negative int (1-4).
fn num_bytes_for_group_vint(v: i32) -> u32 {
    // 4 - (leading zeros / 8), but at least 1
    4 - ((v as u32 | 1).leading_zeros() >> 3)
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

/// Trait for index file output with checksum and position tracking.
pub trait IndexOutput: DataOutput + Send {
    /// Returns the name of this output (the file name).
    fn name(&self) -> &str;

    /// Returns the current write position (byte offset).
    fn file_pointer(&self) -> u64;

    /// Returns the current CRC32 checksum of all bytes written so far.
    fn checksum(&self) -> u64;

    /// Aligns the file pointer to the given power-of-2 boundary by writing zero bytes.
    fn align_file_pointer(&mut self, alignment: usize) -> io::Result<u64> {
        let pos = self.file_pointer();
        let aligned = align_offset(pos, alignment);
        let padding = (aligned - pos) as usize;
        for _ in 0..padding {
            self.write_byte(0)?;
        }
        Ok(aligned)
    }
}

/// Calculates the aligned offset for the given position and alignment.
pub(crate) fn align_offset(offset: u64, alignment: usize) -> u64 {
    let a = alignment as u64;
    (offset + a - 1) & !(a - 1)
}

/// A [`Directory`] behind a [`Mutex`](std::sync::Mutex) for shared concurrent access.
pub type SharedDirectory = std::sync::Mutex<Box<dyn Directory>>;

/// Trait for a directory that can create and manage index files.
pub trait Directory: Send {
    /// Creates a new output file with the given name.
    fn create_output(&mut self, name: &str) -> io::Result<Box<dyn IndexOutput>>;

    /// Lists all files in this directory.
    fn list_all(&self) -> io::Result<Vec<String>>;

    /// Returns the byte length of a file.
    fn file_length(&self, name: &str) -> io::Result<u64>;

    /// Deletes a file.
    fn delete_file(&mut self, name: &str) -> io::Result<()>;

    /// Renames a file. Used for atomic commit of segments_N.
    fn rename(&mut self, source: &str, dest: &str) -> io::Result<()>;

    /// Reads the raw bytes of a file into memory.
    fn read_file(&self, name: &str) -> io::Result<Vec<u8>>;

    /// Writes complete byte contents to a file in this directory.
    /// Default implementation uses `create_output` + `write_bytes`.
    fn write_file(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        let mut out = self.create_output(name)?;
        out.write_bytes(data)?;
        Ok(())
    }

    /// Ensures that any writes to the named files are moved to stable storage.
    fn sync(&self, names: &[&str]) -> io::Result<()> {
        let _ = names;
        Ok(())
    }

    /// Ensures that directory metadata (e.g., new file entries) is persisted.
    fn sync_meta_data(&self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_le_int() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_le_int(0x04030201_i32).unwrap();
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_write_le_long() {
        let mut buf = Vec::new();
        VecOutput(&mut buf)
            .write_le_long(0x0807060504030201_i64)
            .unwrap();
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn test_write_le_short() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_le_short(0x0201_i16).unwrap();
        assert_eq!(buf, [0x01, 0x02]);
    }

    #[test]
    fn test_write_be_int() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_be_int(0x04030201_i32).unwrap();
        assert_eq!(buf, [0x04, 0x03, 0x02, 0x01]);
    }

    #[test]
    fn test_write_be_long() {
        let mut buf = Vec::new();
        VecOutput(&mut buf)
            .write_be_long(0x0807060504030201_i64)
            .unwrap();
        assert_eq!(buf, [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
    }

    #[test]
    fn test_write_vint() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vint(0).unwrap();
        assert_eq!(buf, [0x00]);

        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vint(127).unwrap();
        assert_eq!(buf, [0x7F]);

        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vint(128).unwrap();
        assert_eq!(buf, [0x80, 0x01]);

        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vint(16383).unwrap();
        assert_eq!(buf, [0xFF, 0x7F]);

        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vint(16384).unwrap();
        assert_eq!(buf, [0x80, 0x80, 0x01]);
    }

    #[test]
    fn test_write_vlong() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vlong(0).unwrap();
        assert_eq!(buf, [0x00]);

        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vlong(128).unwrap();
        assert_eq!(buf, [0x80, 0x01]);
    }

    #[test]
    fn test_write_string() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_string("hello").unwrap();
        // VInt(5) = 0x05, then "hello" as UTF-8
        assert_eq!(buf, [0x05, b'h', b'e', b'l', b'l', b'o']);
    }

    // Ported from org.apache.lucene.util.TestGroupVInt
    #[test]
    fn test_write_group_vints_basic() {
        let mut buf = Vec::new();
        let values = [1, 2, 3, 4];
        VecOutput(&mut buf).write_group_vints(&values, 4).unwrap();
        // All values fit in 1 byte, so flag = 0b00_00_00_00 = 0x00
        // Then 1, 2, 3, 4 each as 1 byte
        assert_eq!(buf, [0x00, 1, 2, 3, 4]);
    }

    #[test]
    fn test_write_group_vints_mixed_sizes() {
        let mut buf = Vec::new();
        let values = [1, 256, 1, 1]; // 256 needs 2 bytes
        VecOutput(&mut buf).write_group_vints(&values, 4).unwrap();
        // n1m1=0, n2m1=1, n3m1=0, n4m1=0 → flag = (0<<6)|(1<<4)|(0<<2)|0 = 0x10
        assert_eq!(buf[0], 0x10);
        assert_eq!(buf[1], 1); // value 1
        assert_eq!(buf[2], 0); // 256 LE low byte
        assert_eq!(buf[3], 1); // 256 LE high byte
        assert_eq!(buf[4], 1); // value 1
        assert_eq!(buf[5], 1); // value 1
    }

    #[test]
    fn test_write_group_vints_with_tail() {
        let mut buf = Vec::new();
        let values = [1, 2, 3, 4, 5, 6];
        VecOutput(&mut buf).write_group_vints(&values, 6).unwrap();
        // First 4 as group, then 5 and 6 as VInts
        assert_eq!(buf[0], 0x00); // flag for [1,2,3,4]
        assert_eq!(&buf[1..5], &[1, 2, 3, 4]);
        assert_eq!(buf[5], 5); // VInt 5
        assert_eq!(buf[6], 6); // VInt 6
    }

    #[test]
    fn test_align_offset() {
        assert_eq!(align_offset(0, 8), 0);
        assert_eq!(align_offset(1, 8), 8);
        assert_eq!(align_offset(7, 8), 8);
        assert_eq!(align_offset(8, 8), 8);
        assert_eq!(align_offset(9, 8), 16);
    }

    // Ported from org.apache.lucene.store.BaseDataOutputTestCase

    #[test]
    fn test_write_zint() {
        // zigzag(0) = 0 → vint(0)
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_zint(0).unwrap();
        assert_eq!(buf, [0x00]);

        // zigzag(-1) = 1 → vint(1)
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_zint(-1).unwrap();
        assert_eq!(buf, [0x01]);

        // zigzag(1) = 2 → vint(2)
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_zint(1).unwrap();
        assert_eq!(buf, [0x02]);

        // zigzag(i32::MIN) = u32::MAX → vint(0xFFFFFFFF)
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_zint(i32::MIN).unwrap();
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);

        // zigzag(i32::MAX) = u32::MAX - 1 → vint(0xFFFFFFFE)
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_zint(i32::MAX).unwrap();
        assert_eq!(buf, [0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
    }

    #[test]
    fn test_write_zlong() {
        // zigzag(0) = 0
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_zlong(0).unwrap();
        assert_eq!(buf, [0x00]);

        // zigzag(-1) = 1
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_zlong(-1).unwrap();
        assert_eq!(buf, [0x01]);

        // zigzag(1) = 2
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_zlong(1).unwrap();
        assert_eq!(buf, [0x02]);

        // zigzag(i64::MIN) = -1 as i64 (all bits set) → 10 bytes
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_zlong(i64::MIN).unwrap();
        assert_eq!(buf.len(), 10);
        // All continuation bytes are 0xFF, final byte is 0x01
        for &b in &buf[..9] {
            assert_eq!(b, 0xFF);
        }
        assert_eq!(buf[9], 0x01);

        // zigzag(i64::MAX) = i64::MAX << 1 = 0xFFFFFFFFFFFFFFFE → 10 bytes
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_zlong(i64::MAX).unwrap();
        assert_eq!(buf.len(), 10);
        assert_eq!(buf[0], 0xFE);
        for &b in &buf[1..9] {
            assert_eq!(b, 0xFF);
        }
        assert_eq!(buf[9], 0x01);
    }

    #[test]
    fn test_write_set_of_strings_empty() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_set_of_strings(&[]).unwrap();
        assert_eq!(buf, [0x00]); // vint(0)
    }

    #[test]
    fn test_write_set_of_strings() {
        let mut buf = Vec::new();
        let set = vec!["hello".to_string(), "world".to_string()];
        VecOutput(&mut buf).write_set_of_strings(&set).unwrap();
        // vint(2) + string("hello") + string("world")
        let mut expected = vec![0x02]; // count = 2
        expected.extend_from_slice(&[0x05, b'h', b'e', b'l', b'l', b'o']); // "hello"
        expected.extend_from_slice(&[0x05, b'w', b'o', b'r', b'l', b'd']); // "world"
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_write_map_of_strings_empty() {
        let mut buf = Vec::new();
        let map = HashMap::new();
        VecOutput(&mut buf).write_map_of_strings(&map).unwrap();
        assert_eq!(buf, [0x00]); // vint(0)
    }

    #[test]
    fn test_write_map_of_strings() {
        let mut buf = Vec::new();
        let mut map = HashMap::new();
        map.insert("key".to_string(), "val".to_string());
        VecOutput(&mut buf).write_map_of_strings(&map).unwrap();
        // vint(1) + string("key") + string("val")
        assert_eq!(buf[0], 0x01); // count = 1
        // The rest: string("key") = [3, k, e, y] + string("val") = [3, v, a, l]
        assert_eq!(&buf[1..], &[0x03, b'k', b'e', b'y', 0x03, b'v', b'a', b'l']);
    }

    #[test]
    fn test_num_bytes_for_group_vint() {
        assert_eq!(num_bytes_for_group_vint(0), 1);
        assert_eq!(num_bytes_for_group_vint(1), 1);
        assert_eq!(num_bytes_for_group_vint(0xFF), 1);
        assert_eq!(num_bytes_for_group_vint(0x100), 2);
        assert_eq!(num_bytes_for_group_vint(0xFFFF), 2);
        assert_eq!(num_bytes_for_group_vint(0x10000), 3);
        assert_eq!(num_bytes_for_group_vint(0xFFFFFF), 3);
        assert_eq!(num_bytes_for_group_vint(0x1000000), 4);
        assert_eq!(num_bytes_for_group_vint(i32::MAX), 4);
    }
}
