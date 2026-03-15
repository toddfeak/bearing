// SPDX-License-Identifier: Apache-2.0

//! In-memory [`Directory`](super::Directory) implementation, useful for tests.

use std::collections::HashMap;
use std::io;

use crate::store::checksum::CRC32;
use crate::store::{DataOutput, Directory, IndexOutput, SegmentFile};

/// In-memory directory backed by a HashMap of byte vectors.
pub struct MemoryDirectory {
    files: HashMap<String, Vec<u8>>,
}

impl Default for MemoryDirectory {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryDirectory {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }
}

impl Directory for MemoryDirectory {
    fn create_output(&mut self, name: &str) -> io::Result<Box<dyn IndexOutput>> {
        Ok(Box::new(MemoryIndexOutput::new(name.to_string())))
    }

    fn list_all(&self) -> io::Result<Vec<String>> {
        let mut names: Vec<String> = self.files.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    fn file_length(&self, name: &str) -> io::Result<u64> {
        match self.files.get(name) {
            Some(data) => Ok(data.len() as u64),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {name}"),
            )),
        }
    }

    fn delete_file(&mut self, name: &str) -> io::Result<()> {
        match self.files.remove(name) {
            Some(_) => Ok(()),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {name}"),
            )),
        }
    }

    fn rename(&mut self, source: &str, dest: &str) -> io::Result<()> {
        match self.files.remove(source) {
            Some(data) => {
                self.files.insert(dest.to_string(), data);
                Ok(())
            }
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {source}"),
            )),
        }
    }

    fn file_bytes(&self, name: &str) -> io::Result<&[u8]> {
        match self.files.get(name) {
            Some(data) => Ok(data.as_slice()),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {name}"),
            )),
        }
    }

    fn write_file(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        self.files.insert(name.to_string(), data.to_vec());
        Ok(())
    }
}

impl MemoryDirectory {
    /// Inserts a completed MemoryIndexOutput's bytes into this directory.
    /// Call this after closing the output to persist its data.
    pub fn insert_output(&mut self, output: MemoryIndexOutput) {
        self.files.insert(output.name, output.buf);
    }
}

/// In-memory IndexOutput that writes to a Vec<u8> with running CRC32.
pub struct MemoryIndexOutput {
    name: String,
    buf: Vec<u8>,
    crc: CRC32,
}

impl MemoryIndexOutput {
    pub fn new(name: String) -> Self {
        Self {
            name,
            buf: Vec::new(),
            crc: CRC32::new(),
        }
    }

    /// Returns a reference to the underlying byte buffer.
    pub fn bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Consumes this output and returns a [`SegmentFile`] with its name and bytes.
    pub fn into_inner(self) -> SegmentFile {
        SegmentFile {
            name: self.name,
            data: self.buf,
        }
    }
}

impl DataOutput for MemoryIndexOutput {
    fn write_byte(&mut self, b: u8) -> io::Result<()> {
        self.buf.push(b);
        self.crc.update_byte(b);
        Ok(())
    }

    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<()> {
        self.buf.extend_from_slice(buf);
        self.crc.update(buf);
        Ok(())
    }
}

impl IndexOutput for MemoryIndexOutput {
    fn name(&self) -> &str {
        &self.name
    }

    fn file_pointer(&self) -> u64 {
        self.buf.len() as u64
    }

    fn checksum(&self) -> u64 {
        self.crc.value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_output_write_and_checksum() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        out.write_bytes(b"hello").unwrap();
        assert_eq!(out.file_pointer(), 5);
        assert_eq!(out.checksum(), 0x3610A686);
        assert_eq!(out.bytes(), b"hello");
    }

    #[test]
    fn test_memory_directory_create_and_list() {
        let mut dir = MemoryDirectory::new();

        let out = MemoryIndexOutput::new("file1.txt".to_string());
        dir.insert_output(out);

        let out = MemoryIndexOutput::new("file2.txt".to_string());
        dir.insert_output(out);

        let files = dir.list_all().unwrap();
        assert_eq!(files, vec!["file1.txt", "file2.txt"]);
    }

    #[test]
    fn test_memory_directory_file_length() {
        let mut dir = MemoryDirectory::new();
        let mut out = MemoryIndexOutput::new("test.bin".to_string());
        out.write_bytes(b"hello").unwrap();
        dir.insert_output(out);

        assert_eq!(dir.file_length("test.bin").unwrap(), 5);
    }

    #[test]
    fn test_memory_directory_delete_file() {
        let mut dir = MemoryDirectory::new();
        let out = MemoryIndexOutput::new("test.bin".to_string());
        dir.insert_output(out);

        assert!(dir.delete_file("test.bin").is_ok());
        assert!(dir.delete_file("test.bin").is_err());
    }

    #[test]
    fn test_memory_directory_rename() {
        let mut dir = MemoryDirectory::new();
        let mut out = MemoryIndexOutput::new("old.bin".to_string());
        out.write_bytes(b"data").unwrap();
        dir.insert_output(out);

        dir.rename("old.bin", "new.bin").unwrap();
        assert!(dir.file_length("old.bin").is_err());
        assert_eq!(dir.file_length("new.bin").unwrap(), 4);
    }

    #[test]
    fn test_memory_directory_file_bytes() {
        let mut dir = MemoryDirectory::new();
        let mut out = MemoryIndexOutput::new("test.bin".to_string());
        out.write_bytes(b"hello world").unwrap();
        dir.insert_output(out);

        assert_eq!(dir.file_bytes("test.bin").unwrap(), b"hello world");
    }

    #[test]
    fn test_memory_output_int_le() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        out.write_le_int(0x01020304).unwrap();
        assert_eq!(out.bytes(), [0x04, 0x03, 0x02, 0x01]);
    }

    #[test]
    fn test_memory_output_be_int() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        out.write_be_int(0x01020304).unwrap();
        assert_eq!(out.bytes(), [0x01, 0x02, 0x03, 0x04]);
    }
}
