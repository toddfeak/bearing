// SPDX-License-Identifier: Apache-2.0

// DEBT: adapted from codecs::lucene99::segment_info_format — reconcile after
// switchover by updating the original to accept newindex types directly.

//! Segment info (.si) writer for the newindex pipeline.

use std::collections::HashMap;
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::newindex::index_file_names;
use crate::store::SharedDirectory;

const CODEC_NAME: &str = "Lucene90SegmentInfo";
const VERSION_CURRENT: i32 = 0;
const EXTENSION: &str = "si";

const LUCENE_MAJOR: i32 = 10;
const LUCENE_MINOR: i32 = 3;
const LUCENE_BUGFIX: i32 = 2;

const SI_YES: u8 = 1;
const SI_NO: u8 = 0xFF; // -1 as byte

/// Segment-level metadata for writing the .si file.
// DEBT: parallel to index::SegmentInfo — merge after switchover
#[derive(Debug)]
pub(crate) struct SegmentInfo {
    pub name: String,
    pub max_doc: i32,
    pub is_compound_file: bool,
    pub id: [u8; 16],
    pub diagnostics: HashMap<String, String>,
    pub attributes: HashMap<String, String>,
    pub has_blocks: bool,
}

/// Writes the .si file for a segment. Returns the file name written.
pub(crate) fn write(
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

    // Lucene version (LE ints)
    output.write_le_int(LUCENE_MAJOR)?;
    output.write_le_int(LUCENE_MINOR)?;
    output.write_le_int(LUCENE_BUGFIX)?;

    // minVersion: present + same as main version
    output.write_byte(1)?;
    output.write_le_int(LUCENE_MAJOR)?;
    output.write_le_int(LUCENE_MINOR)?;
    output.write_le_int(LUCENE_BUGFIX)?;

    // maxDoc
    output.write_le_int(segment_info.max_doc)?;

    // isCompoundFile
    output.write_byte(if segment_info.is_compound_file {
        SI_YES
    } else {
        SI_NO
    })?;

    // hasBlocks
    output.write_byte(if segment_info.has_blocks {
        SI_YES
    } else {
        SI_NO
    })?;

    // diagnostics
    output.write_map_of_strings(&segment_info.diagnostics)?;

    // files
    output.write_set_of_strings(files)?;

    // attributes
    output.write_map_of_strings(&segment_info.attributes)?;

    // numSortFields = 0
    output.write_vint(0)?;

    codec_util::write_footer(&mut *output)?;

    Ok(file_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{MemoryDirectory, SharedDirectory};

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    fn make_segment_info() -> SegmentInfo {
        let mut diagnostics = HashMap::new();
        diagnostics.insert("source".to_string(), "flush".to_string());
        SegmentInfo {
            name: "_0".to_string(),
            max_doc: 3,
            is_compound_file: false,
            id: [0u8; 16],
            diagnostics,
            attributes: HashMap::new(),
            has_blocks: false,
        }
    }

    #[test]
    fn write_produces_si_file() {
        let dir = test_directory();
        let si = make_segment_info();
        let files = vec!["_0.fdt".to_string(), "_0.fdx".to_string()];
        let name = write(&dir, &si, &files).unwrap();
        assert_eq!(name, "_0.si");

        let data = dir.lock().unwrap().read_file(&name).unwrap();

        // Header magic
        assert_eq!(&data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Footer magic
        let footer_start = data.len() - 16;
        assert_eq!(
            &data[footer_start..footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
    }

    #[test]
    fn write_encodes_version_and_maxdoc() {
        let dir = test_directory();
        let si = make_segment_info();
        write(&dir, &si, &[]).unwrap();

        let data = dir.lock().unwrap().read_file("_0.si").unwrap();

        // After index header: codec="Lucene90SegmentInfo"(19 chars), version=0
        // Header = 4(magic) + 1+19(codec) + 4(version) + 16(id) + 1(suffix len) = 45
        let offset = 45;

        // Lucene version: major=10 (LE int)
        assert_eq!(data[offset], 10);
        assert_eq!(data[offset + 1], 0);
        assert_eq!(data[offset + 2], 0);
        assert_eq!(data[offset + 3], 0);

        // After version(12) + hasMinVersion(1) + minVersion(12) = 25 more bytes
        let maxdoc_offset = offset + 25;

        // maxDoc=3 (LE int)
        assert_eq!(data[maxdoc_offset], 3);
        assert_eq!(data[maxdoc_offset + 1], 0);
        assert_eq!(data[maxdoc_offset + 2], 0);
        assert_eq!(data[maxdoc_offset + 3], 0);

        // isCompoundFile = NO = 0xFF
        assert_eq!(data[maxdoc_offset + 4], 0xFF);

        // hasBlocks = NO = 0xFF
        assert_eq!(data[maxdoc_offset + 5], 0xFF);
    }
}
