// SPDX-License-Identifier: Apache-2.0
//! Segment info format writer for segment-level metadata (name, doc count, diagnostics).

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::index::SegmentInfo;
use crate::index::index_file_names;
use crate::store::memory::MemoryIndexOutput;
use crate::store::{DataOutput, SegmentFile};

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

/// Writes the .si (segment info) file.
/// Returns a [`SegmentFile`] for the .si file.
pub fn write(segment_info: &SegmentInfo, files: &[String]) -> io::Result<SegmentFile> {
    let file_name = index_file_names::segment_file_name(&segment_info.name, "", EXTENSION);
    let mut output = MemoryIndexOutput::new(file_name.clone());

    debug!(
        "segment_info: segment={:?}, maxDoc={}, compound={}, files={}",
        segment_info.name,
        segment_info.max_doc,
        segment_info.is_compound_file,
        files.len()
    );

    codec_util::write_index_header(
        &mut output,
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

    codec_util::write_footer(&mut output)?;

    Ok(output.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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

        let file = write(&si, &files).unwrap();
        assert_eq!(file.name, "_0.si");
        assert!(!file.data.is_empty());

        // Verify header magic
        assert_eq!(&file.data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Verify footer magic
        let footer_start = file.data.len() - 16;
        assert_eq!(
            &file.data[footer_start..footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
    }

    #[test]
    fn test_lucene_version_encoding() {
        let si = make_test_segment();
        let file = write(&si, &[]).unwrap();

        // After the index header, the first data is the Lucene version
        // Header: 4(magic) + 1+19(codec name) + 4(version) + 16(id) + 1(suffix len) = 45 bytes
        let version_offset = 45;

        // major=10
        assert_eq!(file.data[version_offset], 10);
        assert_eq!(file.data[version_offset + 1], 0);
        assert_eq!(file.data[version_offset + 2], 0);
        assert_eq!(file.data[version_offset + 3], 0);

        // minor=3
        assert_eq!(file.data[version_offset + 4], 3);
        assert_eq!(file.data[version_offset + 5], 0);
        assert_eq!(file.data[version_offset + 6], 0);
        assert_eq!(file.data[version_offset + 7], 0);

        // bugfix=2
        assert_eq!(file.data[version_offset + 8], 2);
        assert_eq!(file.data[version_offset + 9], 0);
        assert_eq!(file.data[version_offset + 10], 0);
        assert_eq!(file.data[version_offset + 11], 0);
    }

    #[test]
    fn test_max_doc_and_flags() {
        let si = make_test_segment();
        let file = write(&si, &[]).unwrap();

        // After header (45) + version (12) + hasMinVersion(1) + minVersion(12) = 70
        let maxdoc_offset = 70;

        // maxDoc=3
        assert_eq!(file.data[maxdoc_offset], 3);
        assert_eq!(file.data[maxdoc_offset + 1], 0);
        assert_eq!(file.data[maxdoc_offset + 2], 0);
        assert_eq!(file.data[maxdoc_offset + 3], 0);

        // isCompoundFile = YES = 1
        assert_eq!(file.data[maxdoc_offset + 4], 1);

        // hasBlocks = NO = 0xFF
        assert_eq!(file.data[maxdoc_offset + 5], 0xFF);
    }
}
