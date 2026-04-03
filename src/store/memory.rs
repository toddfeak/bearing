// SPDX-License-Identifier: Apache-2.0

//! In-memory [`Directory`] implementation, useful for tests.

use std::collections::HashMap;
use std::io;
use std::mem;
use std::sync::{Arc, Mutex};

use crate::store::byte_slice_input::ByteSliceIndexInput;
use crate::store::checksum::CRC32;
use crate::store::{DataOutput, Directory, IndexInput, IndexOutput, SegmentFile};

/// In-memory directory backed by a shared HashMap of byte vectors.
///
/// Uses `Arc<Mutex<HashMap>>` for interior mutability so that outputs created
/// by [`create_output`](Directory::create_output) can auto-persist their bytes
/// back into the directory on drop, without requiring the caller to hold a
/// mutable reference to the directory.
pub struct MemoryDirectory {
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl Default for MemoryDirectory {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryDirectory {
    /// Creates a new empty in-memory directory.
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Directory for MemoryDirectory {
    fn create_output(&mut self, name: &str) -> io::Result<Box<dyn IndexOutput>> {
        Ok(Box::new(MemoryDirectoryOutput::new(
            name.to_string(),
            Arc::clone(&self.files),
        )))
    }

    fn open_input(&self, name: &str) -> io::Result<Box<dyn IndexInput>> {
        let files = self.files.lock().unwrap();
        match files.get(name) {
            Some(data) => Ok(Box::new(ByteSliceIndexInput::new(
                name.to_string(),
                data.clone(),
            ))),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {name}"),
            )),
        }
    }

    fn list_all(&self) -> io::Result<Vec<String>> {
        let files = self.files.lock().unwrap();
        let mut names: Vec<String> = files.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    fn file_length(&self, name: &str) -> io::Result<u64> {
        let files = self.files.lock().unwrap();
        match files.get(name) {
            Some(data) => Ok(data.len() as u64),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {name}"),
            )),
        }
    }

    fn delete_file(&mut self, name: &str) -> io::Result<()> {
        let mut files = self.files.lock().unwrap();
        match files.remove(name) {
            Some(_) => Ok(()),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {name}"),
            )),
        }
    }

    fn rename(&mut self, source: &str, dest: &str) -> io::Result<()> {
        let mut files = self.files.lock().unwrap();
        match files.remove(source) {
            Some(data) => {
                files.insert(dest.to_string(), data);
                Ok(())
            }
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {source}"),
            )),
        }
    }

    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        let files = self.files.lock().unwrap();
        match files.get(name) {
            Some(data) => Ok(data.clone()),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {name}"),
            )),
        }
    }

    fn write_file(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        let mut files = self.files.lock().unwrap();
        files.insert(name.to_string(), data.to_vec());
        Ok(())
    }
}

impl MemoryDirectory {
    /// Inserts a completed [`MemoryIndexOutput`]'s bytes into this directory.
    pub fn insert_output(&mut self, output: MemoryIndexOutput) {
        let mut files = self.files.lock().unwrap();
        files.insert(output.name, output.buf);
    }
}

/// Auto-persisting IndexOutput for [`MemoryDirectory`].
///
/// Writes are buffered in a local `Vec<u8>`. On drop, the buffer is inserted
/// into the shared directory map, making the file visible to other callers.
struct MemoryDirectoryOutput {
    name: String,
    buf: Vec<u8>,
    crc: CRC32,
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MemoryDirectoryOutput {
    fn new(name: String, files: Arc<Mutex<HashMap<String, Vec<u8>>>>) -> Self {
        Self {
            name,
            buf: Vec::new(),
            crc: CRC32::new(),
            files,
        }
    }
}

impl Drop for MemoryDirectoryOutput {
    fn drop(&mut self) {
        let buf = mem::take(&mut self.buf);
        let name = mem::take(&mut self.name);
        self.files.lock().unwrap().insert(name, buf);
    }
}

impl DataOutput for MemoryDirectoryOutput {
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

impl IndexOutput for MemoryDirectoryOutput {
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

/// Standalone in-memory IndexOutput that writes to a `Vec<u8>` with running CRC32.
///
/// Unlike `MemoryDirectoryOutput`, this does **not** auto-persist to a directory.
/// Use it for unit tests (via [`bytes()`](MemoryIndexOutput::bytes)) and as scratch
/// buffers (e.g., `address_buffer` in doc_values).
pub struct MemoryIndexOutput {
    name: String,
    buf: Vec<u8>,
    crc: CRC32,
}

impl MemoryIndexOutput {
    /// Creates a new standalone in-memory output.
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
    pub(crate) fn into_inner(self) -> SegmentFile {
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
    fn test_memory_directory_create_output_auto_persists() {
        let mut dir = MemoryDirectory::new();

        {
            let mut out = dir.create_output("auto.bin").unwrap();
            out.write_bytes(b"auto-persisted").unwrap();
            // Output dropped here — should auto-persist
        }

        assert_eq!(dir.read_file("auto.bin").unwrap(), b"auto-persisted");
        assert_eq!(dir.file_length("auto.bin").unwrap(), 14);
    }

    #[test]
    fn test_memory_directory_create_and_list() {
        let mut dir = MemoryDirectory::new();

        {
            let _out = dir.create_output("file1.txt").unwrap();
            let _out = dir.create_output("file2.txt").unwrap();
        }

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

        assert_ok!(dir.delete_file("test.bin"));
        assert_err!(dir.delete_file("test.bin"));
    }

    #[test]
    fn test_memory_directory_rename() {
        let mut dir = MemoryDirectory::new();
        let mut out = MemoryIndexOutput::new("old.bin".to_string());
        out.write_bytes(b"data").unwrap();
        dir.insert_output(out);

        dir.rename("old.bin", "new.bin").unwrap();
        assert_err!(dir.file_length("old.bin"));
        assert_eq!(dir.file_length("new.bin").unwrap(), 4);
    }

    #[test]
    fn test_memory_directory_read_file() {
        let mut dir = MemoryDirectory::new();
        let mut out = MemoryIndexOutput::new("test.bin".to_string());
        out.write_bytes(b"hello world").unwrap();
        dir.insert_output(out);

        assert_eq!(dir.read_file("test.bin").unwrap(), b"hello world");
    }

    #[test]
    fn test_memory_directory_default() {
        let dir = MemoryDirectory::default();
        assert_is_empty!(dir.list_all().unwrap());
    }

    #[test]
    fn test_memory_output_name() {
        let out = MemoryIndexOutput::new("my_file.bin".to_string());
        assert_eq!(out.name(), "my_file.bin");
    }

    #[test]
    fn test_memory_directory_rename_missing() {
        let mut dir = MemoryDirectory::new();
        assert_err!(dir.rename("nonexistent", "dest"));
    }

    #[test]
    fn test_memory_directory_read_missing() {
        let dir = MemoryDirectory::new();
        assert_err!(dir.read_file("nonexistent"));
    }

    #[test]
    fn test_memory_directory_file_length_missing() {
        let dir = MemoryDirectory::new();
        assert_err!(dir.file_length("nonexistent"));
    }

    #[test]
    fn test_memory_directory_open_input() {
        let mut dir = MemoryDirectory::new();
        dir.write_file("test.bin", b"hello world").unwrap();

        let mut input = dir.open_input("test.bin").unwrap();
        assert_eq!(input.name(), "test.bin");
        assert_eq!(input.length(), 11);
        assert_eq!(input.file_pointer(), 0);

        let mut buf = [0u8; 5];
        input.read_bytes(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");
        assert_eq!(input.file_pointer(), 5);
    }

    #[test]
    fn test_memory_directory_open_input_missing() {
        let dir = MemoryDirectory::new();
        assert!(dir.open_input("nonexistent").is_err());
    }

    #[test]
    fn test_memory_directory_open_input_roundtrip() {
        let mut dir = MemoryDirectory::new();
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
    fn test_memory_output_write_byte() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        out.write_byte(0x42).unwrap();
        out.write_byte(0x43).unwrap();
        assert_eq!(out.bytes(), &[0x42, 0x43]);
        assert_eq!(out.file_pointer(), 2);
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
