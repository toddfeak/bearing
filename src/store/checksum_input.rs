// SPDX-License-Identifier: Apache-2.0

//! [`ChecksumIndexInput`] wraps an [`IndexInput`] and computes a running CRC32
//! checksum over all bytes read.

use std::io;

use crate::store::checksum::CRC32;
use crate::store::{DataInput, IndexInput, RandomAccessInput};

/// An [`IndexInput`] wrapper that computes a CRC32 checksum over all bytes read.
///
/// Only forward seeks are allowed — backward seeks return an error because
/// the checksum depends on reading every byte in order.
pub struct ChecksumIndexInput {
    inner: Box<dyn IndexInput>,
    crc: CRC32,
}

impl ChecksumIndexInput {
    /// Wraps an existing [`IndexInput`] with checksum tracking.
    pub fn new(inner: Box<dyn IndexInput>) -> Self {
        Self {
            inner,
            crc: CRC32::new(),
        }
    }

    /// Returns the current CRC32 checksum of all bytes read so far.
    pub fn checksum(&self) -> u64 {
        self.crc.value()
    }
}

impl DataInput for ChecksumIndexInput {
    fn read_byte(&mut self) -> io::Result<u8> {
        let b = self.inner.read_byte()?;
        self.crc.update_byte(b);
        Ok(b)
    }

    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read_bytes(buf)?;
        self.crc.update(buf);
        Ok(())
    }

    fn skip_bytes(&mut self, num_bytes: u64) -> io::Result<()> {
        // Must read bytes to update checksum — cannot seek past them
        let mut remaining = num_bytes;
        let mut skip_buf = [0u8; 1024];
        while remaining > 0 {
            let to_read = remaining.min(skip_buf.len() as u64) as usize;
            self.read_bytes(&mut skip_buf[..to_read])?;
            remaining -= to_read as u64;
        }
        Ok(())
    }
}

impl IndexInput for ChecksumIndexInput {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn file_pointer(&self) -> u64 {
        self.inner.file_pointer()
    }

    fn seek(&mut self, pos: u64) -> io::Result<()> {
        let current = self.file_pointer();
        if pos < current {
            return Err(io::Error::other(format!(
                "ChecksumIndexInput cannot seek backward: {pos} < {current}"
            )));
        }
        if pos > current {
            self.skip_bytes(pos - current)?;
        }
        Ok(())
    }

    fn length(&self) -> u64 {
        self.inner.length()
    }

    fn slice(
        &self,
        _description: &str,
        _offset: u64,
        _length: u64,
    ) -> io::Result<Box<dyn IndexInput>> {
        Err(io::Error::other(
            "ChecksumIndexInput does not support slice",
        ))
    }

    fn random_access(&self) -> io::Result<Box<dyn RandomAccessInput>> {
        Err(io::Error::other(
            "ChecksumIndexInput does not support random_access",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::byte_slice_input::ByteSliceIndexInput;
    use crate::store::memory::MemoryIndexOutput;
    use crate::store::{DataOutput, IndexOutput};

    fn make_checksum_input(data: &[u8]) -> ChecksumIndexInput {
        let inner = ByteSliceIndexInput::new("test".into(), data.to_vec());
        ChecksumIndexInput::new(Box::new(inner))
    }

    #[test]
    fn test_read_and_checksum() {
        let data = b"hello";
        let mut input = make_checksum_input(data);

        let mut buf = [0u8; 5];
        input.read_bytes(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");

        // CRC32 of "hello" = 0x3610A686
        assert_eq!(input.checksum(), 0x3610A686);
    }

    #[test]
    fn test_read_byte_updates_checksum() {
        let mut input = make_checksum_input(&[1, 2, 3]);

        input.read_byte().unwrap();
        input.read_byte().unwrap();
        input.read_byte().unwrap();

        let mut crc = CRC32::new();
        crc.update(&[1, 2, 3]);
        assert_eq!(input.checksum(), crc.value());
    }

    #[test]
    fn test_forward_seek() {
        let mut input = make_checksum_input(&[10, 20, 30, 40, 50]);
        input.seek(3).unwrap();
        assert_eq!(input.file_pointer(), 3);

        // Checksum should cover bytes 0..3
        let mut crc = CRC32::new();
        crc.update(&[10, 20, 30]);
        assert_eq!(input.checksum(), crc.value());

        assert_eq!(input.read_byte().unwrap(), 40);
    }

    #[test]
    fn test_backward_seek_fails() {
        let mut input = make_checksum_input(&[1, 2, 3, 4, 5]);
        input.seek(3).unwrap();
        assert_err!(input.seek(1));
    }

    #[test]
    fn test_seek_same_position() {
        let mut input = make_checksum_input(&[1, 2, 3]);
        input.seek(2).unwrap();
        input.seek(2).unwrap(); // no-op
        assert_eq!(input.file_pointer(), 2);
    }

    #[test]
    fn test_skip_bytes_updates_checksum() {
        let mut input = make_checksum_input(&[1, 2, 3, 4, 5]);
        input.skip_bytes(3).unwrap();

        let mut crc = CRC32::new();
        crc.update(&[1, 2, 3]);
        assert_eq!(input.checksum(), crc.value());
    }

    #[test]
    fn test_length_and_name() {
        let input = make_checksum_input(&[0; 42]);
        assert_eq!(input.name(), "test");
        assert_eq!(input.length(), 42);
    }

    #[test]
    fn test_slice_unsupported() {
        let input = make_checksum_input(&[1, 2, 3]);
        assert!(input.slice("bad", 0, 1).is_err());
    }

    #[test]
    fn test_checksum_matches_index_output() {
        let mut out = MemoryIndexOutput::new("test".into());
        out.write_bytes(b"test data here").unwrap();
        let expected_checksum = out.checksum();

        let mut input = make_checksum_input(b"test data here");
        let mut buf = [0u8; 14];
        input.read_bytes(&mut buf).unwrap();

        assert_eq!(input.checksum(), expected_checksum);
    }
}
