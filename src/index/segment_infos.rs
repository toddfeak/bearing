// Ported from org.apache.lucene.index.SegmentInfos

use std::collections::HashMap;
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::index::SegmentCommitInfo;
use crate::index::index_file_names;
use crate::store::memory::MemoryIndexOutput;
use crate::store::{DataOutput, SegmentFile};
use crate::util::string_helper;

/// Codec name for the segments_N file header.
const CODEC_NAME: &str = "segments";

/// Format version: VERSION_86 = 10 (Lucene 8.6+).
const VERSION_CURRENT: i32 = 10;

/// Lucene version 10.3.2 — written into segments_N as the index version.
const LUCENE_VERSION_MAJOR: i32 = 10;
const LUCENE_VERSION_MINOR: i32 = 3;
const LUCENE_VERSION_BUGFIX: i32 = 2;

/// The codec name written for each segment entry.
/// For the Lucene103 codec target, this is "Lucene103".
const SEGMENT_CODEC_NAME: &str = "Lucene103";

/// Writes a `segments_N` file.
///
/// This is the commit point file that lists all segments in the index.
/// The generation number determines the filename suffix (e.g., generation 1 → `segments_1`).
///
/// Returns a [`SegmentFile`] for the segments_N file.
///
/// # Arguments
/// * `segments` — the segment commit infos to include
/// * `generation` — the commit generation (≥ 1)
/// * `version` — the segment infos version (monotonically increasing, typically matches generation)
/// * `counter` — the segment name counter (number of segments ever created)
/// * `user_data` — optional commit user data (empty for MVP)
pub fn write(
    segments: &[&SegmentCommitInfo],
    generation: i64,
    version: i64,
    counter: i64,
    user_data: &HashMap<String, String>,
) -> io::Result<SegmentFile> {
    let gen_suffix = index_file_names::radix36(generation as u64);
    let filename = format!("segments_{gen_suffix}");
    let id = string_helper::random_id();

    let mut out = MemoryIndexOutput::new(filename.clone());

    // Index header: codec="segments", version=10, id=random, suffix=generation in base-36
    codec_util::write_index_header(&mut out, CODEC_NAME, VERSION_CURRENT, &id, &gen_suffix)?;

    // Lucene version (Version.LATEST = 10.3.2)
    out.write_vint(LUCENE_VERSION_MAJOR)?;
    out.write_vint(LUCENE_VERSION_MINOR)?;
    out.write_vint(LUCENE_VERSION_BUGFIX)?;

    // Index created version major
    out.write_vint(LUCENE_VERSION_MAJOR)?;

    // Segment infos version (BE long)
    out.write_be_long(version)?;

    // Counter (VLong)
    out.write_vlong(counter)?;

    // Number of segments (BE int)
    let num_segments = segments.len() as i32;
    out.write_be_int(num_segments)?;

    debug!(
        "segment_infos: writing segments_{gen_suffix}, version={version}, \
         counter={counter}, num_segments={num_segments}"
    );

    // Min segment version (only if segments > 0)
    if !segments.is_empty() {
        // All segments are created by this writer, so min version = LUCENE_VERSION
        out.write_vint(LUCENE_VERSION_MAJOR)?;
        out.write_vint(LUCENE_VERSION_MINOR)?;
        out.write_vint(LUCENE_VERSION_BUGFIX)?;
    }

    // Per-segment entries
    for sci in segments {
        let si = &sci.info;

        // Segment name
        out.write_string(&si.name)?;

        // Segment ID (16 bytes)
        out.write_bytes(&si.id)?;

        // Codec name
        out.write_string(SEGMENT_CODEC_NAME)?;

        // Del gen (BE long)
        out.write_be_long(sci.del_gen)?;

        // Del count (BE int)
        out.write_be_int(sci.del_count)?;

        // Field infos gen (BE long)
        out.write_be_long(sci.field_infos_gen)?;

        // Doc values gen (BE long)
        out.write_be_long(sci.doc_values_gen)?;

        // Soft del count (BE int)
        out.write_be_int(sci.soft_del_count)?;

        // SCI ID
        match &sci.id {
            Some(sci_id) => {
                out.write_byte(1)?;
                out.write_bytes(sci_id)?;
            }
            None => {
                out.write_byte(0)?;
            }
        }

        // Field infos files (empty for fresh segment)
        out.write_set_of_strings(&[])?;

        // Doc values updates files (empty for fresh segment)
        out.write_be_int(0)?;

        debug!(
            "segment_infos: segment={} maxDoc={} compound={} delGen={} delCount={}",
            si.name, si.max_doc, si.is_compound_file, sci.del_gen, sci.del_count
        );
    }

    // User data
    out.write_map_of_strings(user_data)?;

    // Footer
    codec_util::write_footer(&mut out)?;

    Ok(out.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::codec_util::{CODEC_MAGIC, FOOTER_LENGTH, FOOTER_MAGIC};
    use crate::document::{DocValuesType, IndexOptions};
    use crate::index::{FieldInfo, FieldInfos, PointDimensionConfig, SegmentInfo};
    use crate::test_util::TestDataReader;

    fn make_test_segment_commit_info(
        name: &str,
        max_doc: i32,
        segment_id: [u8; 16],
        sci_id: Option<[u8; 16]>,
    ) -> SegmentCommitInfo {
        let si = SegmentInfo::new(
            name.to_string(),
            max_doc,
            true,
            segment_id,
            HashMap::new(),
            HashMap::new(),
        );
        let fis = FieldInfos::new(vec![FieldInfo::new(
            "contents".to_string(),
            0,
            false,
            false,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::None,
            PointDimensionConfig::default(),
        )]);
        SegmentCommitInfo::new(si, fis, sci_id)
    }

    // Ported from org.apache.lucene.index.TestSegmentInfos

    #[test]
    fn test_write_empty_segments() {
        let user_data = HashMap::new();
        let file = write(&[], 1, 1, 0, &user_data).unwrap();

        assert_eq!(file.name, "segments_1");

        // Verify header magic
        let mut r = TestDataReader::new(&file.data, 0);
        assert_eq!(r.read_be_int(), CODEC_MAGIC);

        // Verify footer at end
        r.pos = file.data.len() - FOOTER_LENGTH;
        assert_eq!(r.read_be_int(), FOOTER_MAGIC);
        assert_eq!(r.read_be_int(), 0); // algorithm

        // Verify data section
        r.pos = codec_util::index_header_length(CODEC_NAME, "1");

        // Version.LATEST = 10.3.2
        assert_eq!(r.read_vint(), 10);
        assert_eq!(r.read_vint(), 3);
        assert_eq!(r.read_vint(), 2);

        // indexCreatedVersionMajor
        assert_eq!(r.read_vint(), 10);

        // version (BE long)
        assert_eq!(r.read_be_long(), 1);

        // counter (VLong)
        assert_eq!(r.read_vlong(), 0);

        // num segments (BE int)
        assert_eq!(r.read_be_int(), 0);

        // No min segment version (size == 0)

        // userData (empty map: VInt 0)
        assert_eq!(r.read_vint(), 0);

        // Next should be footer
        assert_eq!(r.pos, file.data.len() - FOOTER_LENGTH);
    }

    #[test]
    fn test_write_single_segment() {
        let seg_id = [0xABu8; 16];
        let sci_id = [0xCDu8; 16];
        let sci = make_test_segment_commit_info("_0", 3, seg_id, Some(sci_id));

        let user_data = HashMap::new();
        let file = write(&[&sci], 1, 1, 1, &user_data).unwrap();

        assert_eq!(file.name, "segments_1");

        let mut r =
            TestDataReader::new(&file.data, codec_util::index_header_length(CODEC_NAME, "1"));

        // Version.LATEST
        assert_eq!(r.read_vint(), 10);
        assert_eq!(r.read_vint(), 3);
        assert_eq!(r.read_vint(), 2);

        // indexCreatedVersionMajor
        assert_eq!(r.read_vint(), 10);

        // version (BE long)
        assert_eq!(r.read_be_long(), 1);

        // counter (VLong)
        assert_eq!(r.read_vlong(), 1);

        // num segments (BE int)
        assert_eq!(r.read_be_int(), 1);

        // min segment version (10.3.2)
        assert_eq!(r.read_vint(), 10);
        assert_eq!(r.read_vint(), 3);
        assert_eq!(r.read_vint(), 2);

        // Segment entry
        let name = r.read_string();
        assert_eq!(name, "_0");

        // Segment ID (16 bytes)
        assert_eq!(&file.data[r.pos..r.pos + 16], &[0xABu8; 16]);
        r.pos += 16;

        // Codec name
        let codec = r.read_string();
        assert_eq!(codec, "Lucene103");

        // del_gen (BE long)
        assert_eq!(r.read_be_long(), -1);

        // del_count (BE int)
        assert_eq!(r.read_be_int(), 0);

        // field_infos_gen (BE long)
        assert_eq!(r.read_be_long(), -1);

        // doc_values_gen (BE long)
        assert_eq!(r.read_be_long(), -1);

        // soft_del_count (BE int)
        assert_eq!(r.read_be_int(), 0);

        // SCI ID present (byte 1)
        assert_eq!(file.data[r.pos], 1);
        r.pos += 1;

        // SCI ID (16 bytes)
        assert_eq!(&file.data[r.pos..r.pos + 16], &[0xCDu8; 16]);
        r.pos += 16;

        // fieldInfosFiles (empty set: VInt 0)
        assert_eq!(r.read_vint(), 0);

        // docValuesUpdatesFiles (empty: BE int 0)
        assert_eq!(r.read_be_int(), 0);

        // userData (empty map: VInt 0)
        assert_eq!(r.read_vint(), 0);

        // Next should be footer
        assert_eq!(r.pos, file.data.len() - FOOTER_LENGTH);
    }

    #[test]
    fn test_write_segment_no_sci_id() {
        let seg_id = [0x11u8; 16];
        let sci = make_test_segment_commit_info("_0", 1, seg_id, None);

        let user_data = HashMap::new();
        let file = write(&[&sci], 1, 1, 1, &user_data).unwrap();

        let mut r =
            TestDataReader::new(&file.data, codec_util::index_header_length(CODEC_NAME, "1"));

        // Skip: version(3 VInts) + indexCreatedMajor(VInt) + version(BE long) + counter(VLong) + numSegs(BE int)
        for _ in 0..4 {
            r.read_vint();
        }
        r.read_be_long();
        r.read_vlong();
        r.read_be_int();

        // Skip min segment version
        for _ in 0..3 {
            r.read_vint();
        }

        // Skip: name + segment_id + codec
        r.read_string();
        r.pos += 16;
        r.read_string();

        // Skip: del_gen + del_count + field_infos_gen + doc_values_gen + soft_del_count
        r.read_be_long();
        r.read_be_int();
        r.read_be_long();
        r.read_be_long();
        r.read_be_int();

        // SCI ID absent (byte 0, no ID bytes follow)
        assert_eq!(file.data[r.pos], 0);
        r.pos += 1;

        // fieldInfosFiles (empty)
        assert_eq!(r.read_vint(), 0);

        // docValuesUpdatesFiles (empty)
        assert_eq!(r.read_be_int(), 0);
    }

    #[test]
    fn test_write_generation_suffix() {
        let user_data = HashMap::new();
        let file = write(&[], 36, 36, 0, &user_data).unwrap();
        // 36 in base-36 = "10"
        assert_eq!(file.name, "segments_10");
    }

    #[test]
    fn test_byte_order_correctness() {
        // Verify that BE fields are truly big-endian and VInt fields are variable-length
        let seg_id = [0x00u8; 16];
        let sci = make_test_segment_commit_info("_0", 5, seg_id, Some([0x00; 16]));

        let user_data = HashMap::new();
        let file = write(&[&sci], 1, 0x0102030405060708, 1, &user_data).unwrap();

        let mut r =
            TestDataReader::new(&file.data, codec_util::index_header_length(CODEC_NAME, "1"));

        // Skip Version.LATEST + indexCreatedVersionMajor (4 VInts)
        for _ in 0..4 {
            r.read_vint();
        }

        // Version should be BE: 0x01 0x02 0x03 0x04 0x05 0x06 0x07 0x08
        assert_eq!(file.data[r.pos], 0x01);
        assert_eq!(file.data[r.pos + 1], 0x02);
        assert_eq!(file.data[r.pos + 2], 0x03);
        assert_eq!(file.data[r.pos + 3], 0x04);
        assert_eq!(file.data[r.pos + 4], 0x05);
        assert_eq!(file.data[r.pos + 5], 0x06);
        assert_eq!(file.data[r.pos + 6], 0x07);
        assert_eq!(file.data[r.pos + 7], 0x08);
        let ver = r.read_be_long();
        assert_eq!(ver, 0x0102030405060708);
    }
}
