// SPDX-License-Identifier: Apache-2.0

//! Owned backing storage for a file's bytes.
//!
//! Held by a segment for each of its component files. Readers obtain a
//! `&[u8]` view via [`FileBacking::as_bytes`].

use std::fmt;

use memmap2::Mmap;

/// Owns the bytes of one file for the lifetime of this value.
pub enum FileBacking {
    /// Memory-mapped region of a file on disk.
    Mmap(Mmap),
    /// In-memory byte vector.
    Owned(Vec<u8>),
}

impl FileBacking {
    /// Returns a borrowed view of the file's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            FileBacking::Mmap(m) => m.as_ref(),
            FileBacking::Owned(v) => v.as_slice(),
        }
    }

    /// Returns the length of the file in bytes.
    pub fn len(&self) -> usize {
        match self {
            FileBacking::Mmap(m) => m.len(),
            FileBacking::Owned(v) => v.len(),
        }
    }

    /// Returns `true` if the file is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Debug for FileBacking {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let variant = match self {
            FileBacking::Mmap(_) => "Mmap",
            FileBacking::Owned(_) => "Owned",
        };
        f.debug_struct("FileBacking")
            .field("variant", &variant)
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use memmap2::MmapOptions;
    use tempfile::{TempDir, tempdir};

    use super::*;

    fn owned(bytes: &[u8]) -> FileBacking {
        FileBacking::Owned(bytes.to_vec())
    }

    fn mmap_of(bytes: &[u8]) -> (FileBacking, TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.bin");
        {
            let mut file = File::create(&path).unwrap();
            file.write_all(bytes).unwrap();
        }
        let file = File::open(&path).unwrap();
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        (FileBacking::Mmap(mmap), dir)
    }

    // as_bytes

    #[test]
    fn as_bytes_owned_nonempty() {
        let backing = owned(&[1, 2, 3, 4]);
        assert_eq!(backing.as_bytes(), &[1, 2, 3, 4]);
    }

    #[test]
    fn as_bytes_owned_empty() {
        let backing = owned(&[]);
        assert_is_empty!(backing.as_bytes());
    }

    #[test]
    fn as_bytes_mmap_nonempty() {
        let (backing, _dir) = mmap_of(&[10, 20, 30, 40, 50]);
        assert_eq!(backing.as_bytes(), &[10, 20, 30, 40, 50]);
    }

    #[test]
    fn as_bytes_mmap_empty() {
        let (backing, _dir) = mmap_of(&[]);
        assert_is_empty!(backing.as_bytes());
    }

    // len

    #[test]
    fn len_owned_nonempty() {
        assert_len_eq_x!(owned(&[1, 2, 3]), 3);
    }

    #[test]
    fn len_owned_empty() {
        assert_is_empty!(owned(&[]));
    }

    #[test]
    fn len_mmap_nonempty() {
        let (backing, _dir) = mmap_of(&[10, 20, 30, 40]);
        assert_len_eq_x!(backing, 4);
    }

    #[test]
    fn len_mmap_empty() {
        let (backing, _dir) = mmap_of(&[]);
        assert_is_empty!(backing);
    }

    // is_empty

    #[test]
    fn is_empty_owned_nonempty() {
        assert!(!owned(&[1]).is_empty());
    }

    #[test]
    fn is_empty_owned_empty() {
        assert!(owned(&[]).is_empty());
    }

    #[test]
    fn is_empty_mmap_nonempty() {
        let (backing, _dir) = mmap_of(&[1]);
        assert!(!backing.is_empty());
    }

    #[test]
    fn is_empty_mmap_empty() {
        let (backing, _dir) = mmap_of(&[]);
        assert!(backing.is_empty());
    }

    // debug

    #[test]
    fn debug_owned_shows_variant_and_len_not_bytes() {
        let backing = owned(&[0u8; 1024]);
        let rendered = format!("{backing:?}");
        assert_contains!(rendered, "Owned");
        assert_contains!(rendered, "1024");
        assert_not_contains!(rendered, "0, 0, 0");
    }

    #[test]
    fn debug_mmap_shows_variant_and_len_not_bytes() {
        let (backing, _dir) = mmap_of(&[0u8; 1024]);
        let rendered = format!("{backing:?}");
        assert_contains!(rendered, "Mmap");
        assert_contains!(rendered, "1024");
        assert_not_contains!(rendered, "0, 0, 0");
    }
}
