// SPDX-License-Identifier: Apache-2.0

//! Compound file reader — opens `.cfs`/`.cfe` files as a read-only [`Directory`].
//!
//! Ported from `Lucene90CompoundReader`.

use std::collections::HashMap;
use std::io;

use crate::codecs::codec_util;
use crate::encoding::read_encoding::ReadEncoding;
use crate::index::index_file_names;
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory, IndexInput, IndexOutput};

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
    #[expect(dead_code)]
    version: i32,
}

impl CompoundDirectory {
    /// Opens a compound directory for the given segment.
    ///
    /// Reads the `.cfe` entry table from `directory` and opens the `.cfs` data
    /// file. Validates headers, footers, and file length.
    pub fn open(
        directory: &dyn Directory,
        segment_name: &str,
        segment_id: &[u8; codec_util::ID_LENGTH],
    ) -> io::Result<Self> {
        let entries_file_name =
            index_file_names::segment_file_name(segment_name, "", ENTRIES_EXTENSION);
        let (version, entries) = read_entries(directory, &entries_file_name, segment_id)?;

        // Find the last FileEntry (largest offset+length) and add footer length
        let expected_length = entries
            .values()
            .map(|e| e.offset + e.length)
            .max()
            .unwrap_or(codec_util::index_header_length(DATA_CODEC, "") as u64)
            + codec_util::FOOTER_LENGTH as u64;

        let data_file_name = index_file_names::segment_file_name(segment_name, "", DATA_EXTENSION);
        let mut handle = directory.open_input(&data_file_name)?;

        codec_util::check_index_header(
            handle.as_mut(),
            DATA_CODEC,
            version,
            version,
            segment_id,
            "",
        )?;

        codec_util::retrieve_checksum(handle.as_mut())?;

        if handle.length() != expected_length {
            return Err(io::Error::other(format!(
                "length should be {expected_length} bytes, but is {} instead",
                handle.length()
            )));
        }

        Ok(Self {
            segment_name: segment_name.to_string(),
            entries,
            handle,
            version,
        })
    }
}

impl Directory for CompoundDirectory {
    fn create_output(&mut self, _name: &str) -> io::Result<Box<dyn IndexOutput>> {
        Err(io::Error::other(
            "CompoundDirectory is read-only: cannot create output",
        ))
    }

    fn open_input(&self, name: &str) -> io::Result<Box<dyn IndexInput>> {
        let id = index_file_names::strip_segment_name(name);
        let entry = self.entries.get(id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "No sub-file with id {id} found in compound file \
                     (fileName={name} files: {:?})",
                    self.entries.keys().collect::<Vec<_>>()
                ),
            )
        })?;
        self.handle.slice(name, entry.offset, entry.length)
    }

    fn list_all(&self) -> io::Result<Vec<String>> {
        let names: Vec<String> = self
            .entries
            .keys()
            .map(|k| format!("{}{k}", self.segment_name))
            .collect();
        Ok(names)
    }

    fn file_length(&self, name: &str) -> io::Result<u64> {
        let id = index_file_names::strip_segment_name(name);
        let entry = self.entries.get(id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("file not found: {name}"))
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
        input.read_exact(&mut buf)?;
        Ok(buf)
    }
}

/// Reads the `.cfe` entry table. Returns `(version, entries)`.
fn read_entries(
    directory: &dyn Directory,
    entries_file_name: &str,
    segment_id: &[u8; codec_util::ID_LENGTH],
) -> io::Result<(i32, HashMap<String, FileEntry>)> {
    let input = directory.open_input(entries_file_name)?;
    let mut input = ChecksumIndexInput::new(input);

    let version = codec_util::check_index_header(
        &mut input,
        ENTRY_CODEC,
        VERSION_START,
        VERSION_CURRENT,
        segment_id,
        "",
    )?;

    let mapping = read_mapping(&mut input)?;

    codec_util::check_footer(&mut input)?;

    Ok((version, mapping))
}

/// Reads the entry mapping from the `.cfe` stream.
fn read_mapping(mut input: &mut dyn DataInput) -> io::Result<HashMap<String, FileEntry>> {
    let num_entries = input.read_vint()?;
    let mut mapping = HashMap::with_capacity(num_entries as usize);

    for _ in 0..num_entries {
        let id = input.read_string()?;
        let offset = input.read_le_long()? as u64;
        let length = input.read_le_long()? as u64;

        if mapping
            .insert(id.clone(), FileEntry { offset, length })
            .is_some()
        {
            return Err(io::Error::other(format!(
                "Duplicate cfs entry id={id} in CFS"
            )));
        }
    }

    Ok(mapping)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::codecs::lucene90::compound;
    use crate::store::memory::MemoryIndexOutput;
    use crate::store::{MemoryDirectory, SegmentFile};
    use assertables::*;

    fn make_test_file(name: &str, segment_id: &[u8; 16], body: &[u8]) -> SegmentFile {
        let mut out = MemoryIndexOutput::new(name.to_string());
        codec_util::write_index_header(&mut out, "TestCodec", 1, segment_id, "").unwrap();
        out.write_all(body).unwrap();
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
        assert_contains!(listed, &"_0.fnm".to_string());
        assert_contains!(listed, &"_0.fdt".to_string());
    }

    #[test]
    fn test_open_input_and_read() {
        let seg_id = [0xABu8; 16];
        let body = b"hello compound world";
        let files = vec![make_test_file("_0.fnm", &seg_id, body)];
        let (_dir, compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        let mut input = compound_dir.open_input("_0.fnm").unwrap();
        assert_gt!(input.length(), 0);

        let magic = input.read_be_int().unwrap();
        assert_eq!(magic, codec_util::CODEC_MAGIC);
    }

    #[test]
    fn test_open_input_strips_segment_name() {
        let seg_id = [0xABu8; 16];
        let files = vec![make_test_file("_0.fnm", &seg_id, b"data")];
        let (_dir, compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        // Both full name and stripped name resolve via strip_segment_name
        assert!(compound_dir.open_input("_0.fnm").is_ok());
        assert!(compound_dir.open_input(".fnm").is_ok());
    }

    #[test]
    fn test_file_length() {
        let seg_id = [0xABu8; 16];
        let body = b"test body";
        let files = vec![make_test_file("_0.fnm", &seg_id, body)];
        let (_dir, compound_dir) = setup_compound_dir("_0", &seg_id, &files);

        let len = compound_dir.file_length("_0.fnm").unwrap();
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

        assert!(compound_dir.open_input("_0.xxx").is_err());
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

        for name in &["_0.fnm", "_0.fdt", "_0.nvd"] {
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

        let data = compound_dir.read_file("_0.fnm").unwrap();
        assert_not_empty!(data);
        // First 4 bytes should be codec magic
        assert_eq!(&data[..4], &[0x3F, 0xD7, 0x6C, 0x17]);
    }

    #[test]
    fn test_truncated_cfs_detected() {
        let seg_id = [0xABu8; 16];
        let files = vec![make_test_file("_0.fnm", &seg_id, b"data")];
        let (cfs, cfe) = write_compound("_0", &seg_id, &files);

        let mut dir = MemoryDirectory::new();
        // Truncate the .cfs file
        let truncated = &cfs.data[..cfs.data.len() - 4];
        dir.write_file(&cfs.name, truncated).unwrap();
        dir.write_file(&cfe.name, &cfe.data).unwrap();

        let result = CompoundDirectory::open(&dir, "_0", &seg_id);
        assert!(result.is_err());
    }
}
