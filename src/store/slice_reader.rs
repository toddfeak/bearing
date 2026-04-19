// SPDX-License-Identifier: Apache-2.0

//! Borrowed byte-slice [`DataInput`] for parsing in-memory buffers.

use std::io;

use crate::store::DataInput;

/// A [`DataInput`] that reads from a borrowed byte slice.
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
}

impl io::Read for SliceReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        buf[0] = self.read_byte()?;
        Ok(1)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let end = self.pos + buf.len();
        if end > self.data.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of input"));
        }
        buf.copy_from_slice(&self.data[self.pos..end]);
        self.pos = end;
        Ok(())
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
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    use crate::encoding::read_encoding::ReadEncoding;
    use crate::encoding::write_encoding::WriteEncoding;
    use crate::store::VecOutput;

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
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, [10, 20, 30, 40]);
    }

    #[test]
    fn test_read_bytes_past_end() {
        let mut reader = SliceReader::new(&[1, 2]);
        let mut buf = [0u8; 3];
        assert_err!(reader.read_exact(&mut buf));
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
    fn test_read_vint_roundtrip() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vint(16384).unwrap();
        let mut reader = SliceReader::new(&buf);
        assert_eq!(reader.read_vint().unwrap(), 16384);
    }
}
