// SPDX-License-Identifier: Apache-2.0

//! Compound file reader — opens `.cfs`/`.cfe` files as a read-only [`Directory`].

use std::collections::HashMap;
use std::io;

use crate::codecs::codec_util;
use crate::index::index_file_names;
use crate::store::{Directory, IndexInput};

const ENTRIES_EXTENSION: &str = "cfe";
const DATA_EXTENSION: &str = "cfs";
const DATA_CODEC: &str = "Lucene90CompoundData";
const ENTRY_CODEC: &str = "Lucene90CompoundEntries";
const VERSION_START: i32 = 0;
const VERSION_CURRENT: i32 = VERSION_START;

struct FileEntry {
    offset: u64,
    length: u64,
}

/// A read-only [`Directory`] that reads files from a compound file (`.cfs`/`.cfe`).
///
/// Parses the `.cfe` entry table on construction and holds a handle to the `.cfs`
/// data file. Files are accessed by slicing the `.cfs` handle.
pub struct CompoundDirectory {
    segment_name: String,
    entries: HashMap<String, FileEntry>,
    handle: Box<dyn IndexInput>,
}

impl CompoundDirectory {
    /// Opens a compound directory for the given segment.
    ///
    /// Reads the `.cfe` entry table from `directory` and opens the `.cfs` data file.
    pub fn open(
        directory: &dyn Directory,
        segment_name: &str,
        segment_id: &[u8; codec_util::ID_LENGTH],
    ) -> io::Result<Self> {
        let entries = read_entries(directory, segment_name, segment_id)?;
        let cfs_name = index_file_names::segment_file_name(segment_name, "", DATA_EXTENSION);
        let handle = directory.open_input(&cfs_name)?;

        // Validate .cfs header
        let mut header_input = handle.slice("cfs_header", 0, handle.length())?;
        codec_util::check_header(
            &mut *header_input,
            DATA_CODEC,
            VERSION_START,
            VERSION_CURRENT,
        )?;

        Ok(Self {
            segment_name: segment_name.to_string(),
            entries,
            handle,
        })
    }
}

impl Directory for CompoundDirectory {
    fn create_output(&mut self, _name: &str) -> io::Result<Box<dyn crate::store::IndexOutput>> {
        Err(io::Error::other(
            "CompoundDirectory is read-only: cannot create output",
        ))
    }

    fn open_input(&self, name: &str) -> io::Result<Box<dyn IndexInput>> {
        // Try both the raw name and the stripped name (without segment prefix)
        let entry = self
            .entries
            .get(name)
            .or_else(|| {
                let stripped = index_file_names::strip_segment_name(name);
                self.entries.get(stripped)
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("file not found in compound: {name}"),
                )
            })?;

        self.handle.slice(name, entry.offset, entry.length)
    }

    fn list_all(&self) -> io::Result<Vec<String>> {
        let mut names: Vec<String> = self
            .entries
            .keys()
            .map(|k| {
                // Return full name with segment prefix
                format!("{}{k}", self.segment_name)
            })
            .collect();
        names.sort();
        Ok(names)
    }

    fn file_length(&self, name: &str) -> io::Result<u64> {
        let entry = self
            .entries
            .get(name)
            .or_else(|| {
                let stripped = index_file_names::strip_segment_name(name);
                self.entries.get(stripped)
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("file not found in compound: {name}"),
                )
            })?;
        Ok(entry.length)
    }

    fn delete_file(&mut self, _name: &str) -> io::Result<()> {
        Err(io::Error::other(
            "CompoundDirectory is read-only: cannot delete",
        ))
    }

    fn rename(&mut self, _source: &str, _dest: &str) -> io::Result<()> {
        Err(io::Error::other(
            "CompoundDirectory is read-only: cannot rename",
        ))
    }

    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        let mut input = self.open_input(name)?;
        let len = input.length() as usize;
        let mut buf = vec![0u8; len];
        input.read_bytes(&mut buf)?;
        Ok(buf)
    }
}

/// Reads the `.cfe` entry table and returns a map of stripped filename → (offset, length).
fn read_entries(
    directory: &dyn Directory,
    segment_name: &str,
    segment_id: &[u8; codec_util::ID_LENGTH],
) -> io::Result<HashMap<String, FileEntry>> {
    let cfe_name = index_file_names::segment_file_name(segment_name, "", ENTRIES_EXTENSION);
    let mut input = directory.open_input(&cfe_name)?;

    codec_util::check_header(&mut *input, ENTRY_CODEC, VERSION_START, VERSION_CURRENT)?;

    // Skip ID + suffix (we validated the codec/version, skip the rest of the index header)
    let mut id = [0u8; codec_util::ID_LENGTH];
    input.read_bytes(&mut id)?;
    if id != *segment_id {
        return Err(io::Error::other(format!(
            "segment ID mismatch in {cfe_name}: expected {segment_id:02x?}, got {id:02x?}"
        )));
    }
    let suffix_len = input.read_byte()? as usize;
    input.skip_bytes(suffix_len as u64)?;

    let num_entries = input.read_vint()?;
    if num_entries < 0 {
        return Err(io::Error::other(format!(
            "invalid entry count: {num_entries}"
        )));
    }

    let mut entries = HashMap::with_capacity(num_entries as usize);
    for _ in 0..num_entries {
        let name = input.read_string()?;
        let offset = input.read_le_long()? as u64;
        let length = input.read_le_long()? as u64;
        entries.insert(name, FileEntry { offset, length });
    }

    // Skip footer (don't checksum-validate since we're not using ChecksumIndexInput here)
    // The footer is the last 16 bytes — we just need to have read all entries correctly.

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::lucene90::compound;
    use crate::store::memory::MemoryIndexOutput;
    use crate::store::{DataOutput, MemoryDirectory, SegmentFile};

    fn make_test_file(name: &str, segment_id: &[u8; 16], body: &[u8]) -> SegmentFile {
        let mut out = MemoryIndexOutput::new(name.to_string());
        codec_util::write_index_header(&mut out, "TestCodec", 1, segment_id, "").unwrap();
        out.write_bytes(body).unwrap();
        codec_util::write_footer(&mut out).unwrap();
        out.into_inner()
    }

    fn write_compound(
        segment_name: &str,
        segment_id: &[u8; 16],
        files: &[SegmentFile],
    ) -> (SegmentFile, SegmentFile) {
        let cfs_name = index_file_names::segment_file_name(segment_name, "", "cfs");
        let mut cfs_out = MemoryIndexOutput::new(cfs_name);
        let cfe = compound::write_to(segment_name, segment_id, files, &mut cfs_out).unwrap();
        (cfs_out.into_inner(), cfe)
    }

    fn setup_compound_dir(
        segment_name: &str,
        segment_id: &[u8; 16],
        files: &[SegmentFile],
    ) -> (MemoryDirectory, CompoundDirectory) {
        let (cfs, cfe) = write_compound(segment_name, segment_id, files);
        let mut dir = MemoryDirectory::new();
        dir.write_file(&cfs.name, &cfs.data).unwrap();
        dir.write_file(&cfe.name, &cfe.data).unwrap();
        let compound_dir = CompoundDirectory::open(&dir, segment_name, segment_id).unwrap();
        (dir, compound_dir)
    }

    #[test]
    fn test_list_all() {
        let seg_id = [0xABu8; 16];
        let files = vec![
            make_test_file("_0.fnm", &seg_id, b"field data"),
            make_test_file("_0.fdt", &seg_id, b"stored data"),
        ];
        let (_dir, compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        let listed = compound_dir.list_all().unwrap();
        assert_len_eq_x!(&listed, 2);
        // Names should have segment prefix restored
        assert_contains!(listed, &"_0.fnm".to_string());
        assert_contains!(listed, &"_0.fdt".to_string());
    }

    #[test]
    fn test_open_input_and_read() {
        let seg_id = [0xABu8; 16];
        let body = b"hello compound world";
        let files = vec![make_test_file("_0.fnm", &seg_id, body)];
        let (_dir, compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        let mut input = compound_dir.open_input(".fnm").unwrap();
        assert_gt!(input.length(), 0);

        // The embedded file includes its header + body + footer
        // Verify we can read the codec magic at the start
        let magic = input.read_be_int().unwrap();
        assert_eq!(magic, codec_util::CODEC_MAGIC);
    }

    #[test]
    fn test_open_input_with_full_name() {
        let seg_id = [0xABu8; 16];
        let files = vec![make_test_file("_0.fnm", &seg_id, b"data")];
        let (_dir, compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        // Should work with both stripped and full name
        assert!(compound_dir.open_input(".fnm").is_ok());
        assert!(compound_dir.open_input("_0.fnm").is_ok());
    }

    #[test]
    fn test_file_length() {
        let seg_id = [0xABu8; 16];
        let body = b"test body";
        let files = vec![make_test_file("_0.fnm", &seg_id, body)];
        let (_dir, compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        let len = compound_dir.file_length(".fnm").unwrap();
        // Header + body + footer
        let expected = codec_util::index_header_length("TestCodec", "") as u64
            + body.len() as u64
            + codec_util::FOOTER_LENGTH as u64;
        assert_eq!(len, expected);
    }

    #[test]
    fn test_open_input_missing() {
        let seg_id = [0xABu8; 16];
        let files = vec![make_test_file("_0.fnm", &seg_id, b"data")];
        let (_dir, compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        assert!(compound_dir.open_input(".xxx").is_err());
    }

    #[test]
    fn test_read_only_operations() {
        let seg_id = [0xABu8; 16];
        let files = vec![make_test_file("_0.fnm", &seg_id, b"data")];
        let (_dir, mut compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        assert!(compound_dir.create_output("test").is_err());
        assert!(compound_dir.delete_file("test").is_err());
        assert!(compound_dir.rename("a", "b").is_err());
    }

    #[test]
    fn test_multiple_files() {
        let seg_id = [0xABu8; 16];
        let files = vec![
            make_test_file("_0.fnm", &seg_id, b"field info data"),
            make_test_file("_0.fdt", &seg_id, b"stored fields data here"),
            make_test_file("_0.nvd", &seg_id, b"norms"),
        ];
        let (_dir, compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        let listed = compound_dir.list_all().unwrap();
        assert_len_eq_x!(&listed, 3);

        // Each file should be independently readable with valid header
        for name in &[".fnm", ".fdt", ".nvd"] {
            let mut input = compound_dir.open_input(name).unwrap();
            let magic = input.read_be_int().unwrap();
            assert_eq!(magic, codec_util::CODEC_MAGIC, "bad magic for {name}");
        }
    }

    #[test]
    fn test_read_file() {
        let seg_id = [0xABu8; 16];
        let files = vec![make_test_file("_0.fnm", &seg_id, b"body data")];
        let (_dir, compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        let data = compound_dir.read_file(".fnm").unwrap();
        assert_not_empty!(data);
        // First 4 bytes should be codec magic
        assert_eq!(&data[..4], &[0x3F, 0xD7, 0x6C, 0x17]);
    }
}
