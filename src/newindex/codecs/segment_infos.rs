// SPDX-License-Identifier: Apache-2.0

// DEBT: adapted from index::segment_infos — reconcile after switchover
// by merging into a single segments_N writer.

//! Segments_N commit point writer for the newindex pipeline.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::index::segment::FlushedSegment;
use crate::newindex::index_file_names;
use crate::store::SharedDirectory;
use crate::util::string_helper;

const CODEC_NAME: &str = "segments";
const VERSION_CURRENT: i32 = 10;

const LUCENE_VERSION_MAJOR: i32 = 10;
const LUCENE_VERSION_MINOR: i32 = 3;
const LUCENE_VERSION_BUGFIX: i32 = 2;

const SEGMENT_CODEC_NAME: &str = "Lucene103";

/// Writes a `segments_N` file to the directory.
///
/// Returns the filename written (e.g., "segments_1").
pub(crate) fn write(
    directory: &SharedDirectory,
    segments: &[FlushedSegment],
    generation: u64,
) -> io::Result<String> {
    let gen_suffix = index_file_names::radix_fmt(generation);
    let pending_name = format!("pending_segments_{gen_suffix}");
    let final_name = format!("segments_{gen_suffix}");
    let id = string_helper::random_id();

    let counter = segments
        .iter()
        .map(|s| {
            // Parse the segment number from the name (e.g., "_0" → 0, "_a" → 10)
            let num_str = s.segment_id.name.trim_start_matches('_');
            u64::from_str_radix(num_str, 36).unwrap_or(0)
        })
        .max()
        .map(|m| m + 1)
        .unwrap_or(0) as i64;

    let version = generation as i64;
    let num_segments = segments.len() as i32;

    debug!(
        "segment_infos: writing {final_name}, version={version}, counter={counter}, num_segments={num_segments}"
    );

    let mut output = directory.lock().unwrap().create_output(&pending_name)?;

    // Index header
    codec_util::write_index_header(&mut *output, CODEC_NAME, VERSION_CURRENT, &id, &gen_suffix)?;

    // Lucene version
    output.write_vint(LUCENE_VERSION_MAJOR)?;
    output.write_vint(LUCENE_VERSION_MINOR)?;
    output.write_vint(LUCENE_VERSION_BUGFIX)?;

    // Index created version major
    output.write_vint(LUCENE_VERSION_MAJOR)?;

    // Segment infos version (BE long)
    output.write_be_long(version)?;

    // Counter (VLong)
    output.write_vlong(counter)?;

    // Number of segments (BE int)
    output.write_be_int(num_segments)?;

    // Min segment version (only if segments > 0)
    if !segments.is_empty() {
        output.write_vint(LUCENE_VERSION_MAJOR)?;
        output.write_vint(LUCENE_VERSION_MINOR)?;
        output.write_vint(LUCENE_VERSION_BUGFIX)?;
    }

    // Per-segment entries
    for seg in segments {
        // Segment name
        output.write_string(&seg.segment_id.name)?;

        // Segment ID (16 bytes)
        output.write_bytes(&seg.segment_id.id)?;

        // Codec name
        output.write_string(SEGMENT_CODEC_NAME)?;

        // Del gen (BE long) — -1 for fresh segment
        output.write_be_long(-1)?;

        // Del count (BE int) — 0
        output.write_be_int(0)?;

        // Field infos gen (BE long) — -1
        output.write_be_long(-1)?;

        // Doc values gen (BE long) — -1
        output.write_be_long(-1)?;

        // Soft del count (BE int) — 0
        output.write_be_int(0)?;

        // SCI ID: present (1) + 16 random bytes
        let sci_id = string_helper::random_id();
        output.write_byte(1)?;
        output.write_bytes(&sci_id)?;

        // Field infos files (empty set)
        output.write_set_of_strings(&[])?;

        // Doc values updates files (empty: BE int 0)
        output.write_be_int(0)?;
    }

    // User data (empty map)
    output.write_map_of_strings(&std::collections::HashMap::new())?;

    // Footer
    codec_util::write_footer(&mut *output)?;

    // Flush the output before syncing
    drop(output);

    // Sync and rename
    {
        let dir = directory.lock().unwrap();
        dir.sync(&[&pending_name])?;
    }
    {
        let mut dir = directory.lock().unwrap();
        dir.rename(&pending_name, &final_name)?;
    }

    Ok(final_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::segment::SegmentId;
    use crate::store::{MemoryDirectory, SharedDirectory};

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    fn make_segment(name: &str, doc_count: i32) -> FlushedSegment {
        FlushedSegment {
            segment_id: SegmentId {
                name: name.to_string(),
                id: [0xABu8; 16],
            },
            doc_count,
            file_names: vec![format!("{name}.fdt"), format!("{name}.fdx")],
        }
    }

    #[test]
    fn write_empty_segments() {
        let dir = test_directory();
        let name = write(&dir, &[], 1).unwrap();
        assert_eq!(name, "segments_1");

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
    fn write_single_segment_structure() {
        let dir = test_directory();
        let segments = vec![make_segment("_0", 5)];
        let name = write(&dir, &segments, 1).unwrap();
        assert_eq!(name, "segments_1");

        let data = dir.lock().unwrap().read_file(&name).unwrap();

        // Header magic
        assert_eq!(&data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Footer magic
        let footer_start = data.len() - 16;
        assert_eq!(
            &data[footer_start..footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );

        // After header: codec="segments"(8 chars)
        // Header = 4(magic) + 1+8(codec) + 4(version) + 16(id) + 1+1(suffix "1") = 35
        let offset = 35;

        // Lucene version: 10, 3, 2 as VInts
        assert_eq!(data[offset], 10);
        assert_eq!(data[offset + 1], 3);
        assert_eq!(data[offset + 2], 2);

        // Index created version major
        assert_eq!(data[offset + 3], 10);

        // File should be substantial (header + version + segment entry + footer)
        assert!(data.len() > 80);
    }

    #[test]
    fn generation_suffix_base36() {
        let dir = test_directory();
        let name = write(&dir, &[], 36).unwrap();
        assert_eq!(name, "segments_10");
    }
}
