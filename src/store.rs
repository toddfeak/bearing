// SPDX-License-Identifier: Apache-2.0

//! Storage abstraction layer: directories, data I/O, and index I/O.
//!
//! The [`Directory`] trait abstracts file storage. [`FSDirectory`] writes to the
//! filesystem; [`MemoryDirectory`] holds files in memory (useful for tests).
//! [`DataOutput`] and [`IndexOutput`] define the byte-level writing interface
//! used by codec writers. [`DataInput`] defines the byte-level reading interface
//! used by codec readers.

pub mod byte_slice_input;
pub mod checksum;
pub mod checksum_input;
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
        crate::encoding::varint::write_vint(&mut DataOutputWriter(self), i)
    }

    /// Writes a variable-length long (1-9 bytes). High bit = continuation.
    fn write_vlong(&mut self, i: i64) -> io::Result<()> {
        crate::encoding::varint::write_vlong(&mut DataOutputWriter(self), i)
    }

    /// Writes a zigzag-encoded variable-length int.
    fn write_zint(&mut self, i: i32) -> io::Result<()> {
        crate::encoding::varint::write_zint(&mut DataOutputWriter(self), i)
    }

    /// Writes a zigzag-encoded variable-length long.
    fn write_zlong(&mut self, i: i64) -> io::Result<()> {
        crate::encoding::varint::write_zlong(&mut DataOutputWriter(self), i)
    }

    /// Writes a variable-length long that may be negative (used by writeZLong).
    fn write_signed_vlong(&mut self, i: i64) -> io::Result<()> {
        crate::encoding::varint::write_signed_vlong(&mut DataOutputWriter(self), i)
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
        crate::encoding::string::write_string(&mut DataOutputWriter(self), s)
    }

    /// Writes a set of strings: VInt count followed by each string.
    fn write_set_of_strings(&mut self, set: &[String]) -> io::Result<()> {
        crate::encoding::string::write_set_of_strings(&mut DataOutputWriter(self), set)
    }

    /// Writes a map of strings: VInt count followed by key-value pairs.
    fn write_map_of_strings(&mut self, map: &HashMap<String, String>) -> io::Result<()> {
        crate::encoding::string::write_map_of_strings(&mut DataOutputWriter(self), map)
    }

    /// Writes integers using group-varint encoding.
    /// Groups of 4 are encoded with a flag byte (2 bits per int = byte width - 1)
    /// followed by the ints in LE with variable byte widths.
    /// Remaining values (< 4) are written as regular VInts.
    fn write_group_vints(&mut self, values: &[i32], limit: usize) -> io::Result<()> {
        crate::encoding::group_vint::write_group_vints(&mut DataOutputWriter(self), values, limit)
    }
}

/// Adapter that wraps a [`DataOutput`] reference as an [`io::Write`].
///
/// This allows encoding functions (which accept `&mut impl io::Write`) to work
/// with any [`DataOutput`] or [`IndexOutput`].
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
        let b0 = self.read_byte()? as i16;
        let b1 = self.read_byte()? as i16;
        Ok(b0 | (b1 << 8))
    }

    /// Reads a 32-bit integer in **little-endian** byte order.
    fn read_le_int(&mut self) -> io::Result<i32> {
        let b0 = self.read_byte()? as i32;
        let b1 = self.read_byte()? as i32;
        let b2 = self.read_byte()? as i32;
        let b3 = self.read_byte()? as i32;
        Ok(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24))
    }

    /// Reads a 64-bit long in **little-endian** byte order.
    fn read_le_long(&mut self) -> io::Result<i64> {
        let low = self.read_le_int()? as u32 as i64;
        let high = self.read_le_int()? as u32 as i64;
        Ok(low | (high << 32))
    }

    /// Reads a variable-length integer (1-5 bytes). High bit = continuation.
    fn read_vint(&mut self) -> io::Result<i32> {
        crate::encoding::varint::read_vint(&mut DataInputReader(self))
    }

    /// Reads a variable-length long (1-9 bytes). High bit = continuation.
    fn read_vlong(&mut self) -> io::Result<i64> {
        crate::encoding::varint::read_vlong(&mut DataInputReader(self))
    }

    /// Reads a zigzag-encoded variable-length int.
    fn read_zint(&mut self) -> io::Result<i32> {
        crate::encoding::varint::read_zint(&mut DataInputReader(self))
    }

    /// Reads a zigzag-encoded variable-length long.
    fn read_zlong(&mut self) -> io::Result<i64> {
        crate::encoding::varint::read_zlong(&mut DataInputReader(self))
    }

    /// Reads a 32-bit integer in **big-endian** byte order.
    /// Used by CodecUtil for headers/footers.
    fn read_be_int(&mut self) -> io::Result<i32> {
        let b0 = self.read_byte()? as i32;
        let b1 = self.read_byte()? as i32;
        let b2 = self.read_byte()? as i32;
        let b3 = self.read_byte()? as i32;
        Ok((b0 << 24) | (b1 << 16) | (b2 << 8) | b3)
    }

    /// Reads a 64-bit long in **big-endian** byte order.
    /// Used by CodecUtil for headers/footers.
    fn read_be_long(&mut self) -> io::Result<i64> {
        let high = self.read_be_int()? as u32 as i64;
        let low = self.read_be_int()? as u32 as i64;
        Ok((high << 32) | low)
    }

    /// Reads a string: VInt-encoded byte length followed by UTF-8 bytes.
    fn read_string(&mut self) -> io::Result<String> {
        crate::encoding::string::read_string(&mut DataInputReader(self))
    }

    /// Reads a set of strings: VInt count followed by each string.
    fn read_set_of_strings(&mut self) -> io::Result<Vec<String>> {
        crate::encoding::string::read_set_of_strings(&mut DataInputReader(self))
    }

    /// Reads a map of strings: VInt count followed by key-value pairs.
    fn read_map_of_strings(&mut self) -> io::Result<HashMap<String, String>> {
        crate::encoding::string::read_map_of_strings(&mut DataInputReader(self))
    }

    /// Reads integers using group-varint encoding.
    fn read_group_vints(&mut self, values: &mut [i32], limit: usize) -> io::Result<()> {
        crate::encoding::group_vint::read_group_vints(&mut DataInputReader(self), values, limit)
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
    crate::encoding::varint::read_vint(reader)
}

/// Writes a variable-length integer (1-5 bytes) to any `io::Write` sink.
///
/// High bit = continuation.
pub fn encode_vint(writer: &mut impl io::Write, val: i32) -> io::Result<()> {
    crate::encoding::varint::write_vint(writer, val)
}

/// Trait for index file input with position tracking and random access.
pub trait IndexInput: DataInput + Send {
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

    /// Opens an existing file for reading.
    fn open_input(&self, name: &str) -> io::Result<Box<dyn IndexInput>>;

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

    /// A simple DataInput over a byte slice, for testing.
    struct ByteSliceInput<'a> {
        data: &'a [u8],
        pos: usize,
    }

    impl<'a> ByteSliceInput<'a> {
        fn new(data: &'a [u8]) -> Self {
            Self { data, pos: 0 }
        }
    }

    impl DataInput for ByteSliceInput<'_> {
        fn read_byte(&mut self) -> io::Result<u8> {
            if self.pos >= self.data.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of input"));
            }
            let b = self.data[self.pos];
            self.pos += 1;
            Ok(b)
        }

        fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<()> {
            let end = self.pos + buf.len();
            if end > self.data.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of input"));
            }
            buf.copy_from_slice(&self.data[self.pos..end]);
            self.pos = end;
            Ok(())
        }
    }

    // --- DataOutput tests ---

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
    fn test_align_offset() {
        assert_eq!(align_offset(0, 8), 0);
        assert_eq!(align_offset(1, 8), 8);
        assert_eq!(align_offset(7, 8), 8);
        assert_eq!(align_offset(8, 8), 8);
        assert_eq!(align_offset(9, 8), 16);
    }

    // --- DataInput round-trip tests ---

    #[test]
    fn test_roundtrip_le_short() {
        for &val in &[0_i16, 1, -1, 0x0201, i16::MIN, i16::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_le_short(val).unwrap();
            let decoded = ByteSliceInput::new(&buf).read_le_short().unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_le_int() {
        for &val in &[0_i32, 1, -1, 0x04030201, i32::MIN, i32::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_le_int(val).unwrap();
            let decoded = ByteSliceInput::new(&buf).read_le_int().unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_le_long() {
        for &val in &[0_i64, 1, -1, 0x0807060504030201, i64::MIN, i64::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_le_long(val).unwrap();
            let decoded = ByteSliceInput::new(&buf).read_le_long().unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_be_int() {
        for &val in &[0_i32, 1, -1, 0x04030201, i32::MIN, i32::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_be_int(val).unwrap();
            let decoded = ByteSliceInput::new(&buf).read_be_int().unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_be_long() {
        for &val in &[0_i64, 1, -1, 0x0807060504030201, i64::MIN, i64::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_be_long(val).unwrap();
            let decoded = ByteSliceInput::new(&buf).read_be_long().unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_vint() {
        for &val in &[0, 1, 127, 128, 16383, 16384, i32::MAX, -1] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_vint(val).unwrap();
            let decoded = ByteSliceInput::new(&buf).read_vint().unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_vlong() {
        for &val in &[0_i64, 1, 127, 128, 16384, i64::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_vlong(val).unwrap();
            let decoded = ByteSliceInput::new(&buf).read_vlong().unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_zint() {
        for &val in &[0, 1, -1, 127, -128, i32::MIN, i32::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_zint(val).unwrap();
            let decoded = ByteSliceInput::new(&buf).read_zint().unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_zlong() {
        for &val in &[0_i64, 1, -1, 127, -128, i64::MIN, i64::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_zlong(val).unwrap();
            let decoded = ByteSliceInput::new(&buf).read_zlong().unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_string() {
        for s in &["", "hello", "hello world", "\u{00e9}\u{00e8}"] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_string(s).unwrap();
            let decoded = ByteSliceInput::new(&buf).read_string().unwrap();
            assert_eq!(&decoded, s);
        }
    }

    #[test]
    fn test_roundtrip_set_of_strings() {
        let set = vec!["hello".to_string(), "world".to_string()];
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_set_of_strings(&set).unwrap();
        let decoded = ByteSliceInput::new(&buf).read_set_of_strings().unwrap();
        assert_eq!(decoded, set);
    }

    #[test]
    fn test_roundtrip_map_of_strings() {
        let mut map = HashMap::new();
        map.insert("k1".to_string(), "v1".to_string());
        map.insert("k2".to_string(), "v2".to_string());
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_map_of_strings(&map).unwrap();
        let decoded = ByteSliceInput::new(&buf).read_map_of_strings().unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn test_roundtrip_group_vints() {
        let values = [1, 256, 3, 4, 5, 6];
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_group_vints(&values, 6).unwrap();
        let mut decoded = [0i32; 6];
        ByteSliceInput::new(&buf)
            .read_group_vints(&mut decoded, 6)
            .unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_skip_bytes() {
        let data = [1u8, 2, 3, 4, 5];
        let mut input = ByteSliceInput::new(&data);
        input.skip_bytes(3).unwrap();
        assert_eq!(input.read_byte().unwrap(), 4);
    }

    #[test]
    fn test_read_byte_eof() {
        let data = [];
        let mut input = ByteSliceInput::new(&data);
        assert!(input.read_byte().is_err());
    }

    #[test]
    fn test_mixed_primitives() {
        let mut buf = Vec::new();
        let mut out = VecOutput(&mut buf);
        out.write_be_int(0x12345678).unwrap();
        out.write_vint(42).unwrap();
        out.write_string("test").unwrap();
        out.write_le_long(0xDEADBEEF).unwrap();

        let mut input = ByteSliceInput::new(&buf);
        assert_eq!(input.read_be_int().unwrap(), 0x12345678);
        assert_eq!(input.read_vint().unwrap(), 42);
        assert_eq!(input.read_string().unwrap(), "test");
        assert_eq!(input.read_le_long().unwrap(), 0xDEADBEEF);
    }
}
