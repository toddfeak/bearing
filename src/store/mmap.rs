// SPDX-License-Identifier: Apache-2.0

//! Memory-mapped [`Directory`] implementation.
//!
//! Files are mapped into the process address space once at open time. All
//! subsequent reads — sequential, sliced, and random-access — are zero-copy
//! array indexing with no syscall overhead. This matches Java's `MMapDirectory`,
//! the default `Directory` for read-heavy workloads in Lucene.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::Mmap;

use crate::store::fs::{
    fs_create_output, fs_delete_file, fs_file_length, fs_list_all, fs_read_file, fs_rename,
    fs_sync, fs_sync_meta_data, fs_write_file,
};
use crate::store::{DataInput, Directory, IndexInput, IndexOutput, RandomAccessInput};

/// Memory-mapped filesystem directory.
///
/// Uses `memmap2::Mmap` to map index files into the process address space.
/// All reads are zero-copy array indexing — no file handles, no seeks, no
/// syscalls. This is the preferred `Directory` for queries and is returned
/// by [`FSDirectory::open()`](super::FSDirectory::open).
pub struct MmapDirectory {
    path: PathBuf,
}

impl MmapDirectory {
    /// Opens (or creates) a memory-mapped directory at the given path.
    pub fn new(path: &Path) -> io::Result<Self> {
        fs::create_dir_all(path)?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    /// Returns the filesystem path of this directory.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Directory for MmapDirectory {
    fn create_output(&mut self, name: &str) -> io::Result<Box<dyn IndexOutput>> {
        fs_create_output(&self.path, name)
    }

    fn open_input(&self, name: &str) -> io::Result<Box<dyn IndexInput>> {
        let file_path = self.path.join(name);
        let file = fs::File::open(&file_path)?;
        let mmap = Arc::new(unsafe { Mmap::map(&file)? });
        let len = mmap.len() as u64;
        Ok(Box::new(MmapIndexInput::new(
            name.to_string(),
            mmap,
            0,
            len,
        )))
    }

    fn list_all(&self) -> io::Result<Vec<String>> {
        fs_list_all(&self.path)
    }

    fn file_length(&self, name: &str) -> io::Result<u64> {
        fs_file_length(&self.path, name)
    }

    fn delete_file(&mut self, name: &str) -> io::Result<()> {
        fs_delete_file(&self.path, name)
    }

    fn rename(&mut self, source: &str, dest: &str) -> io::Result<()> {
        fs_rename(&self.path, source, dest)
    }

    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        fs_read_file(&self.path, name)
    }

    fn write_file(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        fs_write_file(&self.path, name, data)
    }

    fn sync(&self, names: &[&str]) -> io::Result<()> {
        fs_sync(&self.path, names)
    }

    fn sync_meta_data(&self) -> io::Result<()> {
        fs_sync_meta_data(&self.path)
    }
}

/// Memory-mapped [`IndexInput`] backed by a shared `Mmap` region.
///
/// Slicing and random-access create new views into the same mapping
/// without copying or opening additional file handles.
pub struct MmapIndexInput {
    name: String,
    mmap: Arc<Mmap>,
    offset: u64,
    len: u64,
    pos: u64,
}

impl MmapIndexInput {
    fn new(name: String, mmap: Arc<Mmap>, offset: u64, len: u64) -> Self {
        Self {
            name,
            mmap,
            offset,
            len,
            pos: 0,
        }
    }

    /// Returns the byte slice for this input's current view.
    #[inline]
    fn bytes(&self) -> &[u8] {
        let start = self.offset as usize;
        let end = start + self.len as usize;
        &self.mmap[start..end]
    }
}

impl io::Read for MmapIndexInput {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        buf[0] = self.read_byte()?;
        Ok(1)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let end = self.pos + buf.len() as u64;
        if end > self.len {
            return Err(io::Error::other("read past end of MmapIndexInput"));
        }
        let start = self.pos as usize;
        let data = MmapIndexInput::bytes(self);
        buf.copy_from_slice(&data[start..start + buf.len()]);
        self.pos = end;
        Ok(())
    }
}

impl DataInput for MmapIndexInput {
    fn read_byte(&mut self) -> io::Result<u8> {
        if self.pos >= self.len {
            return Err(io::Error::other("read past end of MmapIndexInput"));
        }
        let b = self.bytes()[self.pos as usize];
        self.pos += 1;
        Ok(b)
    }
}

impl IndexInput for MmapIndexInput {
    fn name(&self) -> &str {
        &self.name
    }

    fn file_pointer(&self) -> u64 {
        self.pos
    }

    fn length(&self) -> u64 {
        self.len
    }

    fn seek(&mut self, pos: u64) -> io::Result<()> {
        if pos > self.len {
            return Err(io::Error::other(format!(
                "seek past end: pos={pos}, len={}",
                self.len
            )));
        }
        self.pos = pos;
        Ok(())
    }

    fn slice(&self, name: &str, offset: u64, length: u64) -> io::Result<Box<dyn IndexInput>> {
        if offset + length > self.len {
            return Err(io::Error::other(format!(
                "slice out of bounds: offset={offset}, length={length}, input_len={}",
                self.len
            )));
        }
        Ok(Box::new(MmapIndexInput::new(
            name.to_string(),
            Arc::clone(&self.mmap),
            self.offset + offset,
            length,
        )))
    }

    fn random_access(&self) -> io::Result<Box<dyn RandomAccessInput>> {
        Ok(Box::new(MmapRandomAccessInput::new(
            Arc::clone(&self.mmap),
            self.offset,
            self.len,
        )))
    }
}

/// Zero-copy random access into a memory-mapped file region.
struct MmapRandomAccessInput {
    mmap: Arc<Mmap>,
    offset: u64,
    len: u64,
}

impl MmapRandomAccessInput {
    fn new(mmap: Arc<Mmap>, offset: u64, len: u64) -> Self {
        Self { mmap, offset, len }
    }

    #[inline]
    fn abs_pos(&self, pos: u64) -> usize {
        (self.offset + pos) as usize
    }
}

impl RandomAccessInput for MmapRandomAccessInput {
    fn read_byte_at(&self, pos: u64) -> io::Result<u8> {
        if pos >= self.len {
            return Err(io::Error::other("read_byte_at past end"));
        }
        Ok(self.mmap[self.abs_pos(pos)])
    }

    fn read_le_short_at(&self, pos: u64) -> io::Result<i16> {
        if pos + 2 > self.len {
            return Err(io::Error::other("read_le_short_at past end"));
        }
        let p = self.abs_pos(pos);
        let bytes: [u8; 2] = self.mmap[p..p + 2].try_into().unwrap();
        Ok(i16::from_le_bytes(bytes))
    }

    fn read_le_int_at(&self, pos: u64) -> io::Result<i32> {
        if pos + 4 > self.len {
            return Err(io::Error::other("read_le_int_at past end"));
        }
        let p = self.abs_pos(pos);
        let bytes: [u8; 4] = self.mmap[p..p + 4].try_into().unwrap();
        Ok(i32::from_le_bytes(bytes))
    }

    fn read_le_long_at(&self, pos: u64) -> io::Result<i64> {
        if pos + 8 > self.len {
            return Err(io::Error::other("read_le_long_at past end"));
        }
        let p = self.abs_pos(pos);
        let bytes: [u8; 8] = self.mmap[p..p + 8].try_into().unwrap();
        Ok(i64::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_mmap_directory_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut mmap_dir = MmapDirectory::new(dir.path()).unwrap();

        // Write a file
        {
            let mut out = mmap_dir.create_output("test.dat").unwrap();
            out.write_all(b"hello mmap world").unwrap();
        }

        // Read it back
        let mut input = mmap_dir.open_input("test.dat").unwrap();
        assert_eq!(input.length(), 16);
        let mut buf = vec![0u8; 16];
        input.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hello mmap world");
    }

    #[test]
    fn test_mmap_slice_and_random_access() {
        let dir = tempfile::tempdir().unwrap();
        let mut mmap_dir = MmapDirectory::new(dir.path()).unwrap();

        // Write test data
        {
            let mut out = mmap_dir.create_output("data.bin").unwrap();
            for i in 0u8..100 {
                out.write_byte(i).unwrap();
            }
        }

        let input = mmap_dir.open_input("data.bin").unwrap();
        assert_eq!(input.length(), 100);

        // Slice: bytes [10..30)
        let mut slice = input.slice("sub", 10, 20).unwrap();
        assert_eq!(slice.length(), 20);
        let b = slice.read_byte().unwrap();
        assert_eq!(b, 10);

        // Random access on the slice — covers full slice extent
        let ra = slice.random_access().unwrap();
        assert_eq!(ra.read_byte_at(0).unwrap(), 10);
        assert_eq!(ra.read_byte_at(19).unwrap(), 29);

        // Random access on a slice of the original [50..100)
        let slice2 = input.slice("sub2", 50, 50).unwrap();
        let ra2 = slice2.random_access().unwrap();
        assert_eq!(ra2.read_byte_at(0).unwrap(), 50);
        assert_eq!(ra2.read_byte_at(49).unwrap(), 99);
    }

    #[test]
    fn test_mmap_random_access_le_long() {
        let dir = tempfile::tempdir().unwrap();
        let mut mmap_dir = MmapDirectory::new(dir.path()).unwrap();

        {
            let mut out = mmap_dir.create_output("longs.bin").unwrap();
            // Write a known i64 in LE
            out.write_all(&42i64.to_le_bytes()).unwrap();
            out.write_all(&(-1i64).to_le_bytes()).unwrap();
        }

        let input = mmap_dir.open_input("longs.bin").unwrap();
        let ra = input.random_access().unwrap();
        assert_eq!(ra.read_le_long_at(0).unwrap(), 42);
        assert_eq!(ra.read_le_long_at(8).unwrap(), -1);
    }
}
