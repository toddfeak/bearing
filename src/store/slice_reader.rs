// SPDX-License-Identifier: Apache-2.0

//! Borrowed byte-slice [`DataInput`] for parsing in-memory buffers.

use std::io;

use crate::store::DataInput;

/// A [`DataInput`] that reads from a borrowed byte slice.
///
/// Unlike [`ByteSliceIndexInput`](super::byte_slice_input::ByteSliceIndexInput)
/// which owns its data, `SliceReader` borrows a `&[u8]` and can return
/// zero-copy sub-slices via [`read_slice`](Self::read_slice).
pub(crate) struct SliceReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> SliceReader<'a> {
    /// Creates a new reader starting at position 0.
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Returns the current read position.
    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    /// Advances the position by `n` bytes without reading.
    pub(crate) fn skip(&mut self, n: usize) {
        self.pos += n;
    }

    /// Returns a zero-copy sub-slice of `len` bytes and advances the position.
    pub(crate) fn read_slice(&mut self, len: usize) -> io::Result<&'a [u8]> {
        if self.pos + len > self.data.len() {
            return Err(io::Error::other("read past end of slice"));
        }
        let slice = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }
}

impl DataInput for SliceReader<'_> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{DataOutput, VecOutput};

    #[test]
    fn test_read_byte() {
        let mut reader = SliceReader::new(&[1, 2, 3]);
        assert_eq!(reader.read_byte().unwrap(), 1);
        assert_eq!(reader.read_byte().unwrap(), 2);
        assert_eq!(reader.read_byte().unwrap(), 3);
        assert_err!(reader.read_byte());
    }

    #[test]
    fn test_read_bytes() {
        let mut reader = SliceReader::new(&[10, 20, 30, 40]);
        let mut buf = [0u8; 4];
        reader.read_bytes(&mut buf).unwrap();
        assert_eq!(buf, [10, 20, 30, 40]);
    }

    #[test]
    fn test_read_bytes_past_end() {
        let mut reader = SliceReader::new(&[1, 2]);
        let mut buf = [0u8; 3];
        assert_err!(reader.read_bytes(&mut buf));
    }

    #[test]
    fn test_pos() {
        let mut reader = SliceReader::new(&[1, 2, 3, 4, 5]);
        assert_eq!(reader.pos(), 0);
        reader.read_byte().unwrap();
        assert_eq!(reader.pos(), 1);
        reader.skip(2);
        assert_eq!(reader.pos(), 3);
    }

    #[test]
    fn test_skip() {
        let mut reader = SliceReader::new(&[1, 2, 3, 4, 5]);
        reader.skip(3);
        assert_eq!(reader.read_byte().unwrap(), 4);
    }

    #[test]
    fn test_read_slice() {
        let data = [10, 20, 30, 40, 50];
        let mut reader = SliceReader::new(&data);
        let slice = reader.read_slice(3).unwrap();
        assert_eq!(slice, &[10, 20, 30]);
        assert_eq!(reader.pos(), 3);
        assert_eq!(reader.read_byte().unwrap(), 40);
    }

    #[test]
    fn test_read_slice_past_end() {
        let mut reader = SliceReader::new(&[1, 2]);
        assert_err!(reader.read_slice(3));
    }

    #[test]
    fn test_read_vint_roundtrip() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vint(16384).unwrap();
        let mut reader = SliceReader::new(&buf);
        assert_eq!(reader.read_vint().unwrap(), 16384);
    }
}
