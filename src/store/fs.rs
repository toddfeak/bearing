// SPDX-License-Identifier: Apache-2.0

//! Filesystem-backed [`Directory`] implementation.

use std::fs;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};

use crate::store::checksum::CRC32;
use crate::store::{DataInput, DataOutput, Directory, IndexInput, IndexOutput};

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

    fn open_input(&self, name: &str) -> io::Result<Box<dyn IndexInput>> {
        let file_path = self.path.join(name);
        let file = File::open(&file_path)?;
        let len = file.metadata()?.len();
        let reader = BufReader::new(file);
        Ok(Box::new(FSIndexInput::new(name.to_string(), reader, len)))
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

    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        fs::read(self.path.join(name))
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
struct FSIndexInput {
    name: String,
    reader: BufReader<File>,
    pos: u64,
    len: u64,
}

impl FSIndexInput {
    fn new(name: String, reader: BufReader<File>, len: u64) -> Self {
        Self {
            name,
            reader,
            pos: 0,
            len,
        }
    }
}

impl DataInput for FSIndexInput {
    fn read_byte(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        self.pos += 1;
        Ok(buf[0])
    }

    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<()> {
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
        self.reader.seek(io::SeekFrom::Start(pos))?;
        self.pos = pos;
        Ok(())
    }

    fn length(&self) -> u64 {
        self.len
    }

    fn slice(
        &self,
        _description: &str,
        _offset: u64,
        _length: u64,
    ) -> io::Result<Box<dyn IndexInput>> {
        todo!("FSIndexInput::slice not yet implemented")
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
        use crate::store::DataInput;

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

    /// RAII helper to clean up temp directories after tests.
    struct DirCleanup<'a>(&'a Path);

    impl<'a> Drop for DirCleanup<'a> {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.0);
        }
    }
}
