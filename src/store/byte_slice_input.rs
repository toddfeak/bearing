// SPDX-License-Identifier: Apache-2.0

//! In-memory [`IndexInput`] backed by a `Vec<u8>`.

use std::io;

use crate::store::{DataInput, IndexInput, RandomAccessInput};

/// An [`IndexInput`] that reads from an owned byte vector.
///
/// Used by [`MemoryDirectory::open_input`](crate::store::MemoryDirectory) and
/// as the backing for sliced inputs.
pub(crate) struct ByteSliceIndexInput {
    name: String,
    data: Vec<u8>,
    pos: usize,
    offset: usize,
    len: usize,
}

impl ByteSliceIndexInput {
    pub(crate) fn new(name: String, data: Vec<u8>) -> Self {
        let len = data.len();
        Self {
            name,
            data,
            pos: 0,
            offset: 0,
            len,
        }
    }

    fn slice_internal(name: String, data: Vec<u8>, offset: usize, len: usize) -> Self {
        Self {
            name,
            data,
            pos: 0,
            offset,
            len,
        }
    }
}

impl DataInput for ByteSliceIndexInput {
    fn skip_bytes(&mut self, num_bytes: u64) -> io::Result<()> {
        self.seek(self.file_pointer() + num_bytes)
    }

    fn read_byte(&mut self) -> io::Result<u8> {
        let abs = self.offset + self.pos;
        if self.pos >= self.len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of input"));
        }
        let b = self.data[abs];
        self.pos += 1;
        Ok(b)
    }

    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let abs = self.offset + self.pos;
        let end = self.pos + buf.len();
        if end > self.len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of input"));
        }
        buf.copy_from_slice(&self.data[abs..abs + buf.len()]);
        self.pos = end;
        Ok(())
    }
}

impl IndexInput for ByteSliceIndexInput {
    fn name(&self) -> &str {
        &self.name
    }

    fn file_pointer(&self) -> u64 {
        self.pos as u64
    }

    fn seek(&mut self, pos: u64) -> io::Result<()> {
        let pos = pos as usize;
        if pos > self.len {
            return Err(io::Error::other(format!(
                "seek past end: {pos} > {}",
                self.len
            )));
        }
        self.pos = pos;
        Ok(())
    }

    fn length(&self) -> u64 {
        self.len as u64
    }

    fn slice(
        &self,
        description: &str,
        offset: u64,
        length: u64,
    ) -> io::Result<Box<dyn IndexInput>> {
        let offset = offset as usize;
        let length = length as usize;
        if offset + length > self.len {
            return Err(io::Error::other(format!(
                "slice [{offset}..{}] out of bounds (length {})",
                offset + length,
                self.len
            )));
        }
        Ok(Box::new(ByteSliceIndexInput::slice_internal(
            description.to_string(),
            self.data.clone(),
            self.offset + offset,
            length,
        )))
    }

    fn random_access(&self) -> io::Result<Box<dyn RandomAccessInput>> {
        Ok(Box::new(ByteSliceIndexInput::slice_internal(
            format!("{} [random]", self.name),
            self.data.clone(),
            self.offset,
            self.len,
        )))
    }
}

impl RandomAccessInput for ByteSliceIndexInput {
    fn read_byte_at(&self, pos: u64) -> io::Result<u8> {
        let abs = self.offset + pos as usize;
        if pos as usize >= self.len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("read_byte_at({pos}) past end (len={})", self.len),
            ));
        }
        Ok(self.data[abs])
    }

    fn read_le_long_at(&self, pos: u64) -> io::Result<i64> {
        let abs = self.offset + pos as usize;
        if pos as usize + 8 > self.len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("read_le_long_at({pos}) past end (len={})", self.len),
            ));
        }
        let bytes: [u8; 8] = self.data[abs..abs + 8].try_into().unwrap();
        Ok(i64::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_byte() {
        let mut input = ByteSliceIndexInput::new("test".into(), vec![1, 2, 3]);
        assert_eq!(input.read_byte().unwrap(), 1);
        assert_eq!(input.read_byte().unwrap(), 2);
        assert_eq!(input.read_byte().unwrap(), 3);
        assert_err!(input.read_byte());
    }

    #[test]
    fn test_read_bytes() {
        let mut input = ByteSliceIndexInput::new("test".into(), vec![1, 2, 3, 4]);
        let mut buf = [0u8; 4];
        input.read_bytes(&mut buf).unwrap();
        assert_eq!(buf, [1, 2, 3, 4]);
    }

    #[test]
    fn test_seek_and_file_pointer() {
        let mut input = ByteSliceIndexInput::new("test".into(), vec![10, 20, 30, 40]);
        assert_eq!(input.file_pointer(), 0);
        input.seek(2).unwrap();
        assert_eq!(input.file_pointer(), 2);
        assert_eq!(input.read_byte().unwrap(), 30);
    }

    #[test]
    fn test_seek_past_end() {
        let mut input = ByteSliceIndexInput::new("test".into(), vec![1, 2]);
        assert_err!(input.seek(3));
    }

    #[test]
    fn test_length_and_name() {
        let input = ByteSliceIndexInput::new("my_file".into(), vec![0; 42]);
        assert_eq!(input.name(), "my_file");
        assert_eq!(input.length(), 42);
    }

    #[test]
    fn test_skip_bytes() {
        let mut input = ByteSliceIndexInput::new("test".into(), vec![1, 2, 3, 4, 5]);
        input.skip_bytes(3).unwrap();
        assert_eq!(input.file_pointer(), 3);
        assert_eq!(input.read_byte().unwrap(), 4);
    }

    #[test]
    fn test_slice() {
        let input = ByteSliceIndexInput::new("test".into(), vec![10, 20, 30, 40, 50]);
        let mut sliced = input.slice("slice", 1, 3).unwrap();
        assert_eq!(sliced.length(), 3);
        assert_eq!(sliced.read_byte().unwrap(), 20);
        assert_eq!(sliced.read_byte().unwrap(), 30);
        assert_eq!(sliced.read_byte().unwrap(), 40);
        assert_err!(sliced.read_byte());
    }

    #[test]
    fn test_slice_out_of_bounds() {
        let input = ByteSliceIndexInput::new("test".into(), vec![1, 2, 3]);
        assert!(input.slice("bad", 2, 5).is_err());
    }

    #[test]
    fn test_slice_seek() {
        let input = ByteSliceIndexInput::new("test".into(), vec![10, 20, 30, 40, 50]);
        let mut sliced = input.slice("slice", 1, 3).unwrap();
        sliced.seek(2).unwrap();
        assert_eq!(sliced.read_byte().unwrap(), 40);
    }

    #[test]
    fn test_random_access_read_byte() {
        let input = ByteSliceIndexInput::new("test".into(), vec![10, 20, 30, 40, 50]);
        let ra = input.random_access().unwrap();
        assert_eq!(ra.read_byte_at(0).unwrap(), 10);
        assert_eq!(ra.read_byte_at(2).unwrap(), 30);
        assert_eq!(ra.read_byte_at(4).unwrap(), 50);
        assert_err!(ra.read_byte_at(5));
    }

    #[test]
    fn test_random_access_read_long() {
        let data: Vec<u8> = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0xFF];
        let input = ByteSliceIndexInput::new("test".into(), data);
        let ra = input.random_access().unwrap();
        assert_eq!(ra.read_le_long_at(0).unwrap(), 0x0807060504030201_i64);
        assert_err!(ra.read_le_long_at(2));
    }

    #[test]
    fn test_random_access_on_slice() {
        let input = ByteSliceIndexInput::new("test".into(), vec![10, 20, 30, 40, 50]);
        let sliced = input.slice("slice", 1, 3).unwrap();
        let ra = sliced.random_access().unwrap();
        assert_eq!(ra.read_byte_at(0).unwrap(), 20);
        assert_eq!(ra.read_byte_at(2).unwrap(), 40);
        assert_err!(ra.read_byte_at(3));
    }
}
