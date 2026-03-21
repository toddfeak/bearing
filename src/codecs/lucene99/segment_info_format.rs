// SPDX-License-Identifier: Apache-2.0
//! Segment info format writer for segment-level metadata (name, doc count, diagnostics).

use std::collections::HashSet;
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::index::SegmentInfo;
use crate::index::index_file_names;
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory, SharedDirectory};

const CODEC_NAME: &str = "Lucene90SegmentInfo";
const VERSION_CURRENT: i32 = 0;
const EXTENSION: &str = "si";

/// Lucene version constants (10.3.2)
const LUCENE_MAJOR: i32 = 10;
const LUCENE_MINOR: i32 = 3;
const LUCENE_BUGFIX: i32 = 2;

/// SegmentInfo.YES = 1, SegmentInfo.NO = -1
const SI_YES: u8 = 1;
const SI_NO: u8 = 0xFF; // -1 as byte

/// Writes the .si (segment info) file to `directory`.
/// Returns the file name written.
pub fn write(
    directory: &SharedDirectory,
    segment_info: &SegmentInfo,
    files: &[String],
) -> io::Result<String> {
    let file_name = index_file_names::segment_file_name(&segment_info.name, "", EXTENSION);
    let mut output = directory.lock().unwrap().create_output(&file_name)?;

    debug!(
        "segment_info: segment={:?}, maxDoc={}, compound={}, files={}",
        segment_info.name,
        segment_info.max_doc,
        segment_info.is_compound_file,
        files.len()
    );

    codec_util::write_index_header(
        &mut *output,
        CODEC_NAME,
        VERSION_CURRENT,
        &segment_info.id,
        "",
    )?;

    // Write Lucene version
    output.write_le_int(LUCENE_MAJOR)?;
    output.write_le_int(LUCENE_MINOR)?;
    output.write_le_int(LUCENE_BUGFIX)?;

    // minVersion: write 1 (present) + version, same as main version
    output.write_byte(1)?; // hasMinVersion = true
    output.write_le_int(LUCENE_MAJOR)?;
    output.write_le_int(LUCENE_MINOR)?;
    output.write_le_int(LUCENE_BUGFIX)?;

    // maxDoc
    output.write_le_int(segment_info.max_doc)?;

    // isCompoundFile (byte: YES=1, NO=-1)
    output.write_byte(if segment_info.is_compound_file {
        SI_YES
    } else {
        SI_NO
    })?;

    // hasBlocks (byte: YES=1, NO=-1)
    output.write_byte(if segment_info.has_blocks {
        SI_YES
    } else {
        SI_NO
    })?;

    // diagnostics (map of strings)
    output.write_map_of_strings(&segment_info.diagnostics)?;

    // files (set of strings) — the files that belong to this segment
    output.write_set_of_strings(files)?;

    // attributes (map of strings)
    output.write_map_of_strings(&segment_info.attributes)?;

    // numSortFields = 0 (no index sorting)
    output.write_vint(0)?;

    codec_util::write_footer(&mut *output)?;

    Ok(file_name)
}

/// Reads a `.si` (segment info) file from `directory`.
///
/// Validates the codec header, reads segment metadata, and verifies the footer checksum.
pub fn read(
    directory: &dyn Directory,
    segment_name: &str,
    segment_id: &[u8; codec_util::ID_LENGTH],
) -> io::Result<SegmentInfo> {
    let file_name = index_file_names::segment_file_name(segment_name, "", EXTENSION);
    let input = directory.open_input(&file_name)?;
    let mut checksum_input = ChecksumIndexInput::new(input);

    codec_util::check_index_header(
        &mut checksum_input,
        CODEC_NAME,
        VERSION_CURRENT,
        VERSION_CURRENT,
        segment_id,
        "",
    )?;

    // Lucene version (LE ints)
    let _major = checksum_input.read_le_int()?;
    let _minor = checksum_input.read_le_int()?;
    let _bugfix = checksum_input.read_le_int()?;

    // Min version
    let has_min_version = checksum_input.read_byte()?;
    match has_min_version {
        0 => {} // no min version
        1 => {
            let _min_major = checksum_input.read_le_int()?;
            let _min_minor = checksum_input.read_le_int()?;
            let _min_bugfix = checksum_input.read_le_int()?;
        }
        _ => {
            return Err(io::Error::other(format!(
                "illegal hasMinVersion value: {has_min_version}"
            )));
        }
    }

    // Max doc
    let max_doc = checksum_input.read_le_int()?;
    if max_doc < 0 {
        return Err(io::Error::other(format!("invalid docCount: {max_doc}")));
    }

    // Compound file flag
    let is_compound_file = checksum_input.read_byte()? == SI_YES;

    // Has blocks flag
    let has_blocks_byte = checksum_input.read_byte()?;
    let has_blocks = has_blocks_byte == SI_YES;

    // Diagnostics
    let diagnostics = checksum_input.read_map_of_strings()?;

    // Files
    let files_vec = checksum_input.read_set_of_strings()?;
    let files: HashSet<String> = files_vec.into_iter().collect();

    // Attributes
    let attributes = checksum_input.read_map_of_strings()?;

    // Sort fields (must be 0 — sorted segments not supported)
    let num_sort_fields = checksum_input.read_vint()?;
    if num_sort_fields != 0 {
        return Err(io::Error::other(format!(
            "index sort not supported, got {num_sort_fields} sort fields"
        )));
    }

    // Footer
    codec_util::check_footer(&mut checksum_input)?;

    let mut si = SegmentInfo::new(
        segment_name.to_string(),
        max_doc,
        is_compound_file,
        *segment_id,
        diagnostics,
        attributes,
    );
    si.has_blocks = has_blocks;
    si.files = files;

    debug!(
        "segment_info: read segment={segment_name}, maxDoc={max_doc}, compound={is_compound_file}"
    );

    Ok(si)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::store::{Directory, MemoryDirectory, SharedDirectory};

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    fn make_test_segment() -> SegmentInfo {
        let mut diagnostics = HashMap::new();
        diagnostics.insert("source".to_string(), "flush".to_string());

        SegmentInfo::new(
            "_0".to_string(),
            3,
            true,
            [0u8; 16],
            diagnostics,
            HashMap::new(),
        )
    }

    #[test]
    fn test_write_segment_info() {
        let si = make_test_segment();
        let files = vec!["_0.cfs".to_string(), "_0.cfe".to_string()];

        let dir = test_directory();
        let name = write(&dir, &si, &files).unwrap();
        assert_eq!(name, "_0.si");
        let data = dir.lock().unwrap().read_file(&name).unwrap();
        assert_not_empty!(data);

        // Verify header magic
        assert_eq!(&data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Verify footer magic
        let footer_start = data.len() - 16;
        assert_eq!(
            &data[footer_start..footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
    }

    #[test]
    fn test_lucene_version_encoding() {
        let si = make_test_segment();
        let dir = test_directory();
        let name = write(&dir, &si, &[]).unwrap();
        let data = dir.lock().unwrap().read_file(&name).unwrap();

        // After the index header, the first data is the Lucene version
        // Header: 4(magic) + 1+19(codec name) + 4(version) + 16(id) + 1(suffix len) = 45 bytes
        let version_offset = 45;

        // major=10
        assert_eq!(data[version_offset], 10);
        assert_eq!(data[version_offset + 1], 0);
        assert_eq!(data[version_offset + 2], 0);
        assert_eq!(data[version_offset + 3], 0);

        // minor=3
        assert_eq!(data[version_offset + 4], 3);
        assert_eq!(data[version_offset + 5], 0);
        assert_eq!(data[version_offset + 6], 0);
        assert_eq!(data[version_offset + 7], 0);

        // bugfix=2
        assert_eq!(data[version_offset + 8], 2);
        assert_eq!(data[version_offset + 9], 0);
        assert_eq!(data[version_offset + 10], 0);
        assert_eq!(data[version_offset + 11], 0);
    }

    // --- Read round-trip tests ---

    #[test]
    fn test_read_roundtrip() {
        let si = make_test_segment();
        let files = vec!["_0.cfs".to_string(), "_0.cfe".to_string()];
        let dir = test_directory();

        write(&dir, &si, &files).unwrap();

        let dir_guard = dir.lock().unwrap();
        let read_si = read(&**dir_guard, &si.name, &si.id).unwrap();

        assert_eq!(read_si.name, si.name);
        assert_eq!(read_si.max_doc, si.max_doc);
        assert_eq!(read_si.is_compound_file, si.is_compound_file);
        assert_eq!(read_si.id, si.id);
        assert_eq!(read_si.diagnostics, si.diagnostics);
        assert_eq!(read_si.has_blocks, si.has_blocks);
        assert_eq!(read_si.files.len(), files.len());
        for f in &files {
            assert!(read_si.files.contains(f));
        }
    }

    #[test]
    fn test_read_non_compound() {
        let mut si = make_test_segment();
        si.is_compound_file = false;
        let files = vec!["_0.fnm".to_string(), "_0.fdt".to_string()];
        let dir = test_directory();

        write(&dir, &si, &files).unwrap();

        let dir_guard = dir.lock().unwrap();
        let read_si = read(&**dir_guard, &si.name, &si.id).unwrap();

        assert!(!read_si.is_compound_file);
        assert_eq!(read_si.max_doc, 3);
    }

    #[test]
    fn test_read_with_attributes() {
        let mut si = make_test_segment();
        si.attributes
            .insert("custom_key".to_string(), "custom_val".to_string());
        let dir = test_directory();

        write(&dir, &si, &[]).unwrap();

        let dir_guard = dir.lock().unwrap();
        let read_si = read(&**dir_guard, &si.name, &si.id).unwrap();

        assert_eq!(read_si.attributes.get("custom_key").unwrap(), "custom_val");
    }

    #[test]
    fn test_read_wrong_segment_id() {
        let si = make_test_segment();
        let dir = test_directory();
        write(&dir, &si, &[]).unwrap();

        let wrong_id = [0xFFu8; 16];
        let dir_guard = dir.lock().unwrap();
        assert!(read(&**dir_guard, &si.name, &wrong_id).is_err());
    }

    // --- Write-side tests ---

    #[test]
    fn test_max_doc_and_flags() {
        let si = make_test_segment();
        let dir = test_directory();
        let name = write(&dir, &si, &[]).unwrap();
        let data = dir.lock().unwrap().read_file(&name).unwrap();

        // After header (45) + version (12) + hasMinVersion(1) + minVersion(12) = 70
        let maxdoc_offset = 70;

        // maxDoc=3
        assert_eq!(data[maxdoc_offset], 3);
        assert_eq!(data[maxdoc_offset + 1], 0);
        assert_eq!(data[maxdoc_offset + 2], 0);
        assert_eq!(data[maxdoc_offset + 3], 0);

        // isCompoundFile = YES = 1
        assert_eq!(data[maxdoc_offset + 4], 1);

        // hasBlocks = NO = 0xFF
        assert_eq!(data[maxdoc_offset + 5], 0xFF);
    }
}
