// SPDX-License-Identifier: Apache-2.0

//! Filesystem-backed [`Directory`] implementations.
//!
//! [`FSDirectory`] uses file handles for reads (like Java's `NIOFSDirectory`).
//! [`MmapDirectory`](super::mmap::MmapDirectory) uses memory-mapped I/O
//! (like Java's `MMapDirectory`) and is preferred for read-heavy workloads.
//! `FSDirectory::open()` returns an `MmapDirectory`.

use std::fs;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};

use crate::store::checksum::CRC32;
use crate::store::{DataInput, DataOutput, Directory, IndexInput, IndexOutput};

// ============================================================
// Shared filesystem helpers used by both FSDirectory and MmapDirectory
// ============================================================

pub(crate) fn fs_create_output(dir_path: &Path, name: &str) -> io::Result<Box<dyn IndexOutput>> {
    let file_path = dir_path.join(name);
    let file = File::create(&file_path)?;
    let writer = BufWriter::new(file);
    Ok(Box::new(FSIndexOutput::new(name.to_string(), writer)))
}

pub(crate) fn fs_list_all(dir_path: &Path) -> io::Result<Vec<String>> {
    let mut names: Vec<String> = fs::read_dir(dir_path)?
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                let ft = e.file_type().ok()?;
                if ft.is_file() {
                    e.file_name().into_string().ok()
                } else {
                    None
                }
            })
        })
        .collect();
    names.sort();
    Ok(names)
}

pub(crate) fn fs_file_length(dir_path: &Path, name: &str) -> io::Result<u64> {
    let file_path = dir_path.join(name);
    let meta = fs::metadata(&file_path)?;
    Ok(meta.len())
}

pub(crate) fn fs_delete_file(dir_path: &Path, name: &str) -> io::Result<()> {
    fs::remove_file(dir_path.join(name))
}

pub(crate) fn fs_rename(dir_path: &Path, source: &str, dest: &str) -> io::Result<()> {
    fs::rename(dir_path.join(source), dir_path.join(dest))
}

pub(crate) fn fs_read_file(dir_path: &Path, name: &str) -> io::Result<Vec<u8>> {
    fs::read(dir_path.join(name))
}

pub(crate) fn fs_write_file(dir_path: &Path, name: &str, data: &[u8]) -> io::Result<()> {
    fs::write(dir_path.join(name), data)
}

pub(crate) fn fs_sync(dir_path: &Path, names: &[&str]) -> io::Result<()> {
    for name in names {
        let file = File::open(dir_path.join(name))?;
        file.sync_all()?;
    }
    Ok(())
}

pub(crate) fn fs_sync_meta_data(dir_path: &Path) -> io::Result<()> {
    let dir = File::open(dir_path)?;
    dir.sync_all()
}

// ============================================================
// FSDirectory — file-handle-based reads
// ============================================================

/// Filesystem directory using file handles for reads.
///
/// For read-heavy workloads (queries), prefer
/// [`MmapDirectory`](super::mmap::MmapDirectory) which avoids syscalls.
/// `FSDirectory::open()` returns an `MmapDirectory` by default, matching
/// Java's `FSDirectory.open()` behavior.
pub struct FSDirectory {
    path: PathBuf,
}

impl FSDirectory {
    /// Opens (or creates) a filesystem directory at the given path.
    ///
    /// Returns an [`MmapDirectory`](super::mmap::MmapDirectory), matching
    /// Java where `FSDirectory.open()` returns `MMapDirectory` on 64-bit.
    pub fn open(path: &Path) -> io::Result<super::mmap::MmapDirectory> {
        super::mmap::MmapDirectory::new(path)
    }

    /// Opens a filesystem directory with file-handle-based reads.
    ///
    /// Use this only when mmap is not desired. For most read workloads,
    /// prefer `FSDirectory::open()` which returns an `MmapDirectory`.
    pub fn open_with_file_handles(path: &Path) -> io::Result<Self> {
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

impl Directory for FSDirectory {
    fn create_output(&mut self, name: &str) -> io::Result<Box<dyn IndexOutput>> {
        fs_create_output(&self.path, name)
    }

    fn open_input(&self, name: &str) -> io::Result<Box<dyn IndexInput>> {
        let file_path = self.path.join(name);
        let file = File::open(&file_path)?;
        let len = file.metadata()?.len();
        let reader = BufReader::new(file);
        Ok(Box::new(FSIndexInput::new(
            name.to_string(),
            reader,
            len,
            file_path,
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

/// Filesystem-backed IndexOutput wrapping a `BufWriter<File>` with CRC32 tracking.
pub struct FSIndexOutput {
    name: String,
    writer: Option<BufWriter<File>>,
    crc: CRC32,
    position: u64,
}

impl FSIndexOutput {
    fn new(name: String, writer: BufWriter<File>) -> Self {
        Self {
            name,
            writer: Some(writer),
            crc: CRC32::new(),
            position: 0,
        }
    }

    fn writer(&mut self) -> &mut BufWriter<File> {
        self.writer
            .as_mut()
            .expect("writer already consumed by into_inner")
    }

    /// Flushes the BufWriter and returns the underlying File.
    pub fn into_inner(mut self) -> io::Result<File> {
        let mut writer = self.writer.take().expect("writer already consumed");
        writer.flush()?;
        writer.into_inner().map_err(io::Error::other)
    }
}

impl Drop for FSIndexOutput {
    fn drop(&mut self) {
        if let Some(ref mut writer) = self.writer {
            let _ = writer.flush();
        }
    }
}

impl DataOutput for FSIndexOutput {
    fn write_byte(&mut self, b: u8) -> io::Result<()> {
        self.writer().write_all(&[b])?;
        self.crc.update_byte(b);
        self.position += 1;
        Ok(())
    }

    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer().write_all(buf)?;
        self.crc.update(buf);
        self.position += buf.len() as u64;
        Ok(())
    }
}

impl IndexOutput for FSIndexOutput {
    fn name(&self) -> &str {
        &self.name
    }

    fn file_pointer(&self) -> u64 {
        self.position
    }

    fn checksum(&self) -> u64 {
        self.crc.value()
    }
}

/// Filesystem-backed IndexInput wrapping a `BufReader<File>` with seek support.
///
/// Supports slicing: `offset` marks the start of this input's view within the
/// file, and `len` bounds it. Reads and seeks are relative to the slice.
struct FSIndexInput {
    name: String,
    reader: BufReader<File>,
    /// Current position within this slice (0-based).
    pos: u64,
    /// Absolute byte offset of the slice start within the file.
    offset: u64,
    /// Length of this slice in bytes.
    len: u64,
    /// Path to the underlying file (for creating slices).
    path: PathBuf,
}

impl FSIndexInput {
    fn new(name: String, reader: BufReader<File>, len: u64, path: PathBuf) -> Self {
        Self {
            name,
            reader,
            pos: 0,
            offset: 0,
            len,
            path,
        }
    }
}

impl DataInput for FSIndexInput {
    fn read_byte(&mut self) -> io::Result<u8> {
        if self.pos >= self.len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of input"));
        }
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        self.pos += 1;
        Ok(buf[0])
    }

    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<()> {
        if self.pos + buf.len() as u64 > self.len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of input"));
        }
        self.reader.read_exact(buf)?;
        self.pos += buf.len() as u64;
        Ok(())
    }

    fn skip_bytes(&mut self, num_bytes: u64) -> io::Result<()> {
        self.seek(self.pos + num_bytes)
    }
}

impl IndexInput for FSIndexInput {
    fn name(&self) -> &str {
        &self.name
    }

    fn file_pointer(&self) -> u64 {
        self.pos
    }

    fn seek(&mut self, pos: u64) -> io::Result<()> {
        if pos > self.len {
            return Err(io::Error::other(format!(
                "seek past end: {pos} > {}",
                self.len
            )));
        }
        self.reader.seek(io::SeekFrom::Start(self.offset + pos))?;
        self.pos = pos;
        Ok(())
    }

    fn length(&self) -> u64 {
        self.len
    }

    fn slice(
        &self,
        description: &str,
        offset: u64,
        length: u64,
    ) -> io::Result<Box<dyn IndexInput>> {
        if offset + length > self.len {
            return Err(io::Error::other(format!(
                "slice [{offset}..{}] out of bounds (length {})",
                offset + length,
                self.len
            )));
        }
        let abs_offset = self.offset + offset;
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        reader.seek(io::SeekFrom::Start(abs_offset))?;
        Ok(Box::new(FSIndexInput {
            name: description.to_string(),
            reader,
            pos: 0,
            offset: abs_offset,
            len: length,
            path: self.path.clone(),
        }))
    }

    fn random_access(&self) -> io::Result<Box<dyn crate::store::RandomAccessInput>> {
        let mut input = FSIndexInput::new(
            format!("{} [random]", self.name),
            BufReader::new(File::open(&self.path)?),
            self.len,
            self.path.clone(),
        );
        input.offset = self.offset;
        Ok(Box::new(input))
    }
}

impl crate::store::RandomAccessInput for FSIndexInput {
    fn read_byte_at(&self, pos: u64) -> io::Result<u8> {
        if pos >= self.len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("read_byte_at({pos}) past end (len={})", self.len),
            ));
        }
        let mut file = File::open(&self.path)?;
        file.seek(io::SeekFrom::Start(self.offset + pos))?;
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_le_long_at(&self, pos: u64) -> io::Result<i64> {
        if pos + 8 > self.len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("read_le_long_at({pos}) past end (len={})", self.len),
            ));
        }
        let mut file = File::open(&self.path)?;
        file.seek(io::SeekFrom::Start(self.offset + pos))?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("rustlucene_test_fs_{name}_{}", std::process::id()));
        // Clean up any leftover from a previous run
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn test_fs_directory_write_and_list() {
        let dir_path = temp_dir("write_and_list");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("beta.bin", b"hello").unwrap();
        dir.write_file("alpha.bin", b"world").unwrap();

        let files = dir.list_all().unwrap();
        assert_eq!(files, vec!["alpha.bin", "beta.bin"]);
    }

    #[test]
    fn test_fs_directory_file_length() {
        let dir_path = temp_dir("file_length");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("test.bin", b"hello world").unwrap();
        assert_eq!(dir.file_length("test.bin").unwrap(), 11);
    }

    #[test]
    fn test_fs_directory_delete_file() {
        let dir_path = temp_dir("delete_file");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("test.bin", b"data").unwrap();
        assert_ok!(dir.delete_file("test.bin"));
        assert_err!(dir.delete_file("test.bin"));
    }

    #[test]
    fn test_fs_directory_rename() {
        let dir_path = temp_dir("rename");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("old.bin", b"data").unwrap();
        dir.rename("old.bin", "new.bin").unwrap();

        assert_err!(dir.file_length("old.bin"));
        assert_eq!(dir.file_length("new.bin").unwrap(), 4);
    }

    #[test]
    fn test_fs_directory_create_output() {
        let dir_path = temp_dir("create_output");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        {
            let mut out = dir.create_output("test.bin").unwrap();
            out.write_bytes(b"hello").unwrap();
            assert_eq!(out.file_pointer(), 5);
            assert_eq!(out.name(), "test.bin");
            // CRC32 of "hello" = 0x3610A686
            assert_eq!(out.checksum(), 0x3610A686);
        }

        // Verify file contents on disk
        let contents = fs::read(dir_path.join("test.bin")).unwrap();
        assert_eq!(contents, b"hello");
    }

    #[test]
    fn test_fs_output_into_inner() {
        let dir_path = temp_dir("into_inner");
        fs::create_dir_all(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        let file = File::create(dir_path.join("into_inner.bin")).unwrap();
        let writer = BufWriter::new(file);
        let mut fs_out = FSIndexOutput::new("into_inner.bin".to_string(), writer);
        fs_out.write_bytes(b"test data").unwrap();
        let file = fs_out.into_inner().unwrap();
        drop(file);

        // Verify file contents on disk
        let contents = fs::read(dir_path.join("into_inner.bin")).unwrap();
        assert_eq!(contents, b"test data");
    }

    #[test]
    fn test_fs_directory_read_file() {
        let dir_path = temp_dir("read_file");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("test.bin", b"hello world").unwrap();
        let data = dir.read_file("test.bin").unwrap();
        assert_eq!(data, b"hello world");

        assert_err!(dir.read_file("nonexistent.bin"));
    }

    #[test]
    fn test_fs_directory_sync() {
        let dir_path = temp_dir("sync");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("sync_test.bin", b"data").unwrap();
        dir.sync(&["sync_test.bin"]).unwrap();
    }

    #[test]
    fn test_fs_directory_sync_meta_data() {
        let dir_path = temp_dir("sync_meta");
        let dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.sync_meta_data().unwrap();
    }

    #[test]
    fn test_fs_output_write_byte() {
        let dir_path = temp_dir("write_byte");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        {
            let mut out = dir.create_output("byte_test.bin").unwrap();
            out.write_byte(0x42).unwrap();
            out.write_byte(0x43).unwrap();
            assert_eq!(out.file_pointer(), 2);
        }

        let contents = fs::read(dir_path.join("byte_test.bin")).unwrap();
        assert_eq!(contents, &[0x42, 0x43]);
    }

    #[test]
    fn test_fs_output_checksum() {
        let dir_path = temp_dir("checksum");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        let mut out = dir.create_output("checksum.bin").unwrap();
        out.write_bytes(b"test").unwrap();

        // Verify checksum matches manually computed CRC32
        let mut crc = CRC32::new();
        crc.update(b"test");
        assert_eq!(out.checksum(), crc.value());
    }

    #[test]
    fn test_fs_directory_open_input() {
        let dir_path = temp_dir("open_input");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("test.bin", b"hello world").unwrap();

        let mut input = dir.open_input("test.bin").unwrap();
        assert_eq!(input.name(), "test.bin");
        assert_eq!(input.length(), 11);

        let mut buf = [0u8; 5];
        input.read_bytes(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");
        assert_eq!(input.file_pointer(), 5);

        input.seek(0).unwrap();
        input.read_bytes(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn test_fs_directory_open_input_missing() {
        let dir_path = temp_dir("open_input_missing");
        let dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        assert!(dir.open_input("nonexistent.bin").is_err());
    }

    #[test]
    fn test_fs_directory_open_input_roundtrip() {
        let dir_path = temp_dir("open_input_roundtrip");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        {
            let mut out = dir.create_output("roundtrip.bin").unwrap();
            out.write_le_int(0x04030201).unwrap();
            out.write_string("hello").unwrap();
            out.write_be_long(0x0807060504030201).unwrap();
        }

        let mut input = dir.open_input("roundtrip.bin").unwrap();
        assert_eq!(input.read_le_int().unwrap(), 0x04030201);
        assert_eq!(input.read_string().unwrap(), "hello");
        assert_eq!(input.read_be_long().unwrap(), 0x0807060504030201);
    }

    #[test]
    fn test_fs_index_input_slice() {
        let dir_path = temp_dir("slice");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("test.bin", &[10, 20, 30, 40, 50, 60, 70, 80])
            .unwrap();

        let input = dir.open_input("test.bin").unwrap();
        let mut sliced = input.slice("slice", 2, 4).unwrap();

        assert_eq!(sliced.length(), 4);
        assert_eq!(sliced.file_pointer(), 0);
        assert_eq!(sliced.read_byte().unwrap(), 30);
        assert_eq!(sliced.read_byte().unwrap(), 40);
        assert_eq!(sliced.read_byte().unwrap(), 50);
        assert_eq!(sliced.read_byte().unwrap(), 60);
        assert!(sliced.read_byte().is_err());
    }

    #[test]
    fn test_fs_index_input_slice_seek() {
        let dir_path = temp_dir("slice_seek");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("test.bin", &[10, 20, 30, 40, 50]).unwrap();

        let input = dir.open_input("test.bin").unwrap();
        let mut sliced = input.slice("slice", 1, 3).unwrap();

        sliced.seek(2).unwrap();
        assert_eq!(sliced.read_byte().unwrap(), 40);

        sliced.seek(0).unwrap();
        assert_eq!(sliced.read_byte().unwrap(), 20);

        assert!(sliced.seek(4).is_err());
    }

    #[test]
    fn test_fs_index_input_slice_of_slice() {
        let dir_path = temp_dir("slice_of_slice");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("test.bin", &[10, 20, 30, 40, 50, 60, 70, 80])
            .unwrap();

        let input = dir.open_input("test.bin").unwrap();
        let outer = input.slice("outer", 1, 6).unwrap(); // bytes 20..70
        let mut inner = outer.slice("inner", 2, 3).unwrap(); // bytes 40..60

        assert_eq!(inner.length(), 3);
        assert_eq!(inner.read_byte().unwrap(), 40);
        assert_eq!(inner.read_byte().unwrap(), 50);
        assert_eq!(inner.read_byte().unwrap(), 60);
        assert!(inner.read_byte().is_err());
    }

    #[test]
    fn test_fs_random_access_on_slice_preserves_offset() {
        let dir_path = temp_dir("ra_slice_offset");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("test.bin", &[10, 20, 30, 40, 50, 60, 70, 80])
            .unwrap();

        let input = dir.open_input("test.bin").unwrap();
        // Slice covering bytes [2..6) = [30, 40, 50, 60]
        let sliced = input.slice("slice", 2, 4).unwrap();
        let ra = sliced.random_access().unwrap();

        // random_access reads must be relative to the slice, not the file
        assert_eq!(ra.read_byte_at(0).unwrap(), 30);
        assert_eq!(ra.read_byte_at(1).unwrap(), 40);
        assert_eq!(ra.read_byte_at(2).unwrap(), 50);
        assert_eq!(ra.read_byte_at(3).unwrap(), 60);
    }

    #[test]
    fn test_fs_random_access_on_nested_slice_preserves_offset() {
        let dir_path = temp_dir("ra_nested_offset");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("test.bin", &[10, 20, 30, 40, 50, 60, 70, 80])
            .unwrap();

        let input = dir.open_input("test.bin").unwrap();
        let outer = input.slice("outer", 1, 6).unwrap(); // [20..70]
        let inner = outer.slice("inner", 2, 3).unwrap(); // [40, 50, 60]
        let ra = inner.random_access().unwrap();

        assert_eq!(ra.read_byte_at(0).unwrap(), 40);
        assert_eq!(ra.read_byte_at(1).unwrap(), 50);
        assert_eq!(ra.read_byte_at(2).unwrap(), 60);
    }

    #[test]
    fn test_fs_index_input_slice_out_of_bounds() {
        let dir_path = temp_dir("slice_oob");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("test.bin", &[1, 2, 3]).unwrap();

        let input = dir.open_input("test.bin").unwrap();
        assert!(input.slice("bad", 2, 5).is_err());
    }

    /// RAII helper to clean up temp directories after tests.
    struct DirCleanup<'a>(&'a Path);

    impl<'a> Drop for DirCleanup<'a> {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.0);
        }
    }
}
