// SPDX-License-Identifier: Apache-2.0

//! Filesystem-backed [`Directory`](super::Directory) implementation.

use std::fs;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::store::checksum::CRC32;
use crate::store::{DataOutput, Directory, IndexOutput};

/// Filesystem-backed directory for reading and writing index files.
pub struct FSDirectory {
    path: PathBuf,
}

impl FSDirectory {
    /// Opens (or creates) an FSDirectory at the given path.
    pub fn open(path: &Path) -> io::Result<Self> {
        fs::create_dir_all(path)?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }
}

impl Directory for FSDirectory {
    fn create_output(&mut self, name: &str) -> io::Result<Box<dyn IndexOutput>> {
        let file_path = self.path.join(name);
        let file = File::create(&file_path)?;
        let writer = BufWriter::new(file);
        Ok(Box::new(FSIndexOutput::new(name.to_string(), writer)))
    }

    fn list_all(&self) -> io::Result<Vec<String>> {
        let mut names: Vec<String> = fs::read_dir(&self.path)?
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

    fn file_length(&self, name: &str) -> io::Result<u64> {
        let file_path = self.path.join(name);
        let meta = fs::metadata(&file_path)?;
        Ok(meta.len())
    }

    fn delete_file(&mut self, name: &str) -> io::Result<()> {
        let file_path = self.path.join(name);
        fs::remove_file(&file_path)
    }

    fn rename(&mut self, source: &str, dest: &str) -> io::Result<()> {
        let src_path = self.path.join(source);
        let dst_path = self.path.join(dest);
        fs::rename(&src_path, &dst_path)
    }

    fn file_bytes(&self, _name: &str) -> io::Result<&[u8]> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file_bytes not supported by FSDirectory; use MemoryDirectory for compound file building",
        ))
    }

    fn write_file(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        let file_path = self.path.join(name);
        fs::write(&file_path, data)?;
        Ok(())
    }

    fn sync(&self, names: &[&str]) -> io::Result<()> {
        for name in names {
            let file_path = self.path.join(name);
            let file = File::open(&file_path)?;
            file.sync_all()?;
        }
        Ok(())
    }

    fn sync_meta_data(&self) -> io::Result<()> {
        let dir = File::open(&self.path)?;
        dir.sync_all()
    }
}

/// Filesystem-backed IndexOutput wrapping a BufWriter<File> with CRC32 tracking.
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
        assert!(dir.delete_file("test.bin").is_ok());
        assert!(dir.delete_file("test.bin").is_err());
    }

    #[test]
    fn test_fs_directory_rename() {
        let dir_path = temp_dir("rename");
        let mut dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        dir.write_file("old.bin", b"data").unwrap();
        dir.rename("old.bin", "new.bin").unwrap();

        assert!(dir.file_length("old.bin").is_err());
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
    fn test_fs_directory_file_bytes() {
        let dir_path = temp_dir("file_bytes");
        let dir = FSDirectory::open(&dir_path).unwrap();
        let _cleanup = DirCleanup(&dir_path);

        // FSDirectory does not support file_bytes — it should return Err
        let result = dir.file_bytes("any_file.bin");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Unsupported);
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

    /// RAII helper to clean up temp directories after tests.
    struct DirCleanup<'a>(&'a Path);

    impl<'a> Drop for DirCleanup<'a> {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.0);
        }
    }
}
