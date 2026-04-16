// SPDX-License-Identifier: Apache-2.0

//! Storage abstraction layer: directories, data I/O, and index I/O.
//!
//! The [`Directory`] trait abstracts file storage. [`FSDirectory`] opens a
//! filesystem directory; by default it returns an [`MmapDirectory`] for
//! zero-copy reads. [`MemoryDirectory`] holds files in memory (for tests).
//! [`DataOutput`] and [`IndexOutput`] define the byte-level writing interface
//! used by codec writers. [`DataInput`] defines the byte-level reading interface
//! used by codec readers.

pub(crate) mod byte_slice_input;
pub mod checksum;
pub(crate) mod checksum_input;
pub mod data_input;
pub mod data_output;
pub mod fs;
pub mod index_input;
pub mod index_output;
pub mod memory;
pub mod mmap;
pub(crate) mod slice_reader;

pub use checksum::CRC32;
pub use data_input::{DataInput, DataInputReader, encode_vint, read_vint};
pub use data_output::{DataOutput, DataOutputWriter, VecOutput};
pub use fs::FSDirectory;
pub use index_input::{IndexInput, RandomAccessInput};
pub use index_output::IndexOutput;
pub use memory::MemoryDirectory;
pub use mmap::MmapDirectory;

// Re-export CompoundDirectory — a read-only Directory for compound files (.cfs/.cfe)
pub use crate::codecs::lucene90::compound_reader::CompoundDirectory;

use std::io;
use std::sync::Mutex;

/// A named in-memory file produced by codec writers during indexing.
#[derive(Clone, Debug)]
pub(crate) struct SegmentFile {
    pub(crate) name: String,
    pub(crate) data: Vec<u8>,
}

/// A [`Directory`] behind a [`Mutex`] for shared concurrent access.
///
/// Wrap in `Arc<SharedDirectory>` for multi-threaded use with [`IndexWriter`](crate::index::writer::IndexWriter):
///
/// ```no_run
/// use std::sync::Arc;
/// use bearing::store::{FSDirectory, SharedDirectory};
///
/// let fs_dir = FSDirectory::open(std::path::Path::new("/tmp/my-index")).unwrap();
/// let directory = Arc::new(SharedDirectory::new(Box::new(fs_dir)));
/// ```
pub type SharedDirectory = Mutex<Box<dyn Directory>>;

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
    use std::collections::HashMap;

    use super::*;
    use crate::store::byte_slice_input::ByteSliceIndexInput;
    use crate::store::index_output::align_offset;

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
            let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
                .read_le_short()
                .unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_le_int() {
        for &val in &[0_i32, 1, -1, 0x04030201, i32::MIN, i32::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_le_int(val).unwrap();
            let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
                .read_le_int()
                .unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_le_long() {
        for &val in &[0_i64, 1, -1, 0x0807060504030201, i64::MIN, i64::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_le_long(val).unwrap();
            let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
                .read_le_long()
                .unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_be_int() {
        for &val in &[0_i32, 1, -1, 0x04030201, i32::MIN, i32::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_be_int(val).unwrap();
            let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
                .read_be_int()
                .unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_be_long() {
        for &val in &[0_i64, 1, -1, 0x0807060504030201, i64::MIN, i64::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_be_long(val).unwrap();
            let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
                .read_be_long()
                .unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_vint() {
        for &val in &[0, 1, 127, 128, 16383, 16384, i32::MAX, -1] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_vint(val).unwrap();
            let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
                .read_vint()
                .unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_vlong() {
        for &val in &[0_i64, 1, 127, 128, 16384, i64::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_vlong(val).unwrap();
            let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
                .read_vlong()
                .unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_zint() {
        for &val in &[0, 1, -1, 127, -128, i32::MIN, i32::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_zint(val).unwrap();
            let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
                .read_zint()
                .unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_zlong() {
        for &val in &[0_i64, 1, -1, 127, -128, i64::MIN, i64::MAX] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_zlong(val).unwrap();
            let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
                .read_zlong()
                .unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_roundtrip_string() {
        for s in &["", "hello", "hello world", "\u{00e9}\u{00e8}"] {
            let mut buf = Vec::new();
            VecOutput(&mut buf).write_string(s).unwrap();
            let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
                .read_string()
                .unwrap();
            assert_eq!(&decoded, s);
        }
    }

    #[test]
    fn test_roundtrip_set_of_strings() {
        let set = vec!["hello".to_string(), "world".to_string()];
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_set_of_strings(&set).unwrap();
        let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
            .read_set_of_strings()
            .unwrap();
        assert_eq!(decoded, set);
    }

    #[test]
    fn test_roundtrip_map_of_strings() {
        let mut map = HashMap::new();
        map.insert("k1".to_string(), "v1".to_string());
        map.insert("k2".to_string(), "v2".to_string());
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_map_of_strings(&map).unwrap();
        let decoded = ByteSliceIndexInput::new("test".into(), buf.clone())
            .read_map_of_strings()
            .unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn test_roundtrip_group_vints() {
        let values = [1, 256, 3, 4, 5, 6];
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_group_vints(&values, 6).unwrap();
        let mut decoded = [0i32; 6];
        ByteSliceIndexInput::new("test".into(), buf.clone())
            .read_group_vints(&mut decoded, 6)
            .unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_skip_bytes() {
        let mut input = ByteSliceIndexInput::new("test".into(), vec![1u8, 2, 3, 4, 5]);
        input.skip_bytes(3).unwrap();
        assert_eq!(input.read_byte().unwrap(), 4);
    }

    #[test]
    fn test_read_byte_eof() {
        let mut input = ByteSliceIndexInput::new("test".into(), vec![]);
        assert_err!(input.read_byte());
    }

    #[test]
    fn test_mixed_primitives() {
        let mut buf = Vec::new();
        let mut out = VecOutput(&mut buf);
        out.write_be_int(0x12345678).unwrap();
        out.write_vint(42).unwrap();
        out.write_string("test").unwrap();
        out.write_le_long(0xDEADBEEF).unwrap();

        let mut input = ByteSliceIndexInput::new("test".into(), buf.clone());
        assert_eq!(input.read_be_int().unwrap(), 0x12345678);
        assert_eq!(input.read_vint().unwrap(), 42);
        assert_eq!(input.read_string().unwrap(), "test");
        assert_eq!(input.read_le_long().unwrap(), 0xDEADBEEF);
    }
}
