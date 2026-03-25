// SPDX-License-Identifier: Apache-2.0

//! Utility types for working with byte data.

use mem_dbg::MemSize;

pub mod byte_block_pool;
pub mod small_float;
pub(crate) mod string_helper;

/// A reference to a range of bytes, analogous to Java's BytesRef.
/// In Rust, we can often use &[u8] directly, but this struct provides
/// owned storage for cases where we need to hold onto the bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, MemSize)]
pub struct BytesRef {
    pub bytes: Vec<u8>,
}

impl BytesRef {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn from_slice(s: &[u8]) -> Self {
        Self { bytes: s.to_vec() }
    }

    pub fn from_utf8(s: &str) -> Self {
        Self {
            bytes: s.as_bytes().to_vec(),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl AsRef<[u8]> for BytesRef {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_ref_from_str() {
        let br = BytesRef::from_utf8("hello");
        assert_eq!(br.as_slice(), b"hello");
        assert_len_eq_x!(&br, 5);
    }

    #[test]
    fn test_bytes_ref_ordering() {
        let a = BytesRef::from_utf8("abc");
        let b = BytesRef::from_utf8("abd");
        assert_lt!(a, b);
    }

    #[test]
    fn test_bytes_ref_empty() {
        let br = BytesRef::default();
        assert_is_empty!(br);
    }

    #[test]
    fn test_bytes_ref_from_slice() {
        let br = BytesRef::from_slice(&[1, 2, 3]);
        assert_eq!(br.as_slice(), &[1, 2, 3]);
        assert_len_eq_x!(&br, 3);
    }

    #[test]
    fn test_bytes_ref_as_ref() {
        let br = BytesRef::from_utf8("hello");
        let as_ref: &[u8] = br.as_ref();
        assert_eq!(as_ref, br.as_slice());
    }

    #[test]
    fn test_bytes_ref_default() {
        let br = BytesRef::default();
        assert_eq!(br.bytes, Vec::<u8>::new());
        assert_eq!(br.as_slice(), &[] as &[u8]);
    }
}
