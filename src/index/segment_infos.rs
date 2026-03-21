// SPDX-License-Identifier: Apache-2.0
//! Segment infos reader and writer for the segments_N commit point file.

use std::collections::HashMap;
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::index::SegmentCommitInfo;
use crate::index::index_file_names;
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::memory::MemoryIndexOutput;
use crate::store::{DataInput, DataOutput, Directory, SegmentFile};
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

/// A raw segment entry parsed from a `segments_N` file.
///
/// Contains only what's stored in the `segments_N` file itself — no data from
/// `.si` or `.fnm` files. The caller is responsible for reading those via the
/// appropriate codec format readers.
#[derive(Debug)]
pub struct SegmentEntry {
    /// Segment name (e.g., "_0").
    pub name: String,
    /// Segment ID (16 bytes).
    pub id: [u8; codec_util::ID_LENGTH],
    /// Codec name (e.g., "Lucene103").
    pub codec: String,
    /// Delete generation.
    pub del_gen: i64,
    /// Number of deleted documents.
    pub del_count: i32,
    /// Field infos generation.
    pub field_infos_gen: i64,
    /// Doc values generation.
    pub doc_values_gen: i64,
    /// Number of soft-deleted documents.
    pub soft_del_count: i32,
    /// Segment commit info ID (optional).
    pub sci_id: Option<[u8; codec_util::ID_LENGTH]>,
}

/// Result of reading a `segments_N` file.
///
/// Contains only the data stored in the `segments_N` file. Per-segment metadata
/// (`.si`, `.fnm`) must be read separately by the caller using the codec name
/// from each [`SegmentEntry`].
pub struct SegmentInfosRead {
    /// The raw segment entries in this commit.
    pub segments: Vec<SegmentEntry>,
    /// The commit generation (from the filename suffix).
    pub generation: i64,
    /// The segment infos version (monotonically increasing).
    pub version: i64,
    /// The segment name counter.
    pub counter: i64,
    /// User data written with this commit.
    pub user_data: HashMap<String, String>,
}

/// Reads a `segments_N` file from `directory`.
///
/// Returns only the data stored in the `segments_N` file. Does NOT read
/// per-segment `.si` or `.fnm` files — the caller should use the codec name
/// from each [`SegmentEntry`] to dispatch to the appropriate format readers.
pub fn read(directory: &dyn Directory, segment_file_name: &str) -> io::Result<SegmentInfosRead> {
    // Parse generation from filename
    let gen_suffix = segment_file_name.strip_prefix("segments_").ok_or_else(|| {
        io::Error::other(format!("invalid segments filename: {segment_file_name}"))
    })?;
    let generation = i64::from_str_radix(gen_suffix, 36)
        .map_err(|e| io::Error::other(format!("invalid generation in {segment_file_name}: {e}")))?;

    let input = directory.open_input(segment_file_name)?;
    let mut input = ChecksumIndexInput::new(input);

    // The segments_N file has a random ID we don't know ahead of time,
    // so use check_header (not check_index_header) then read the ID and suffix manually.
    codec_util::check_header(&mut input, CODEC_NAME, VERSION_CURRENT, VERSION_CURRENT)?;

    // Read segment infos ID (16 bytes) — we discover it here
    let mut _id = [0u8; codec_util::ID_LENGTH];
    input.read_bytes(&mut _id)?;

    // Read and validate suffix (should match generation in base-36)
    let suffix_len = input.read_byte()? as usize;
    let mut suffix_bytes = vec![0u8; suffix_len];
    input.read_bytes(&mut suffix_bytes)?;
    let suffix = String::from_utf8(suffix_bytes).map_err(|e| io::Error::other(e.to_string()))?;
    if suffix != gen_suffix {
        return Err(io::Error::other(format!(
            "segments suffix mismatch: expected {gen_suffix:?}, got {suffix:?}"
        )));
    }

    // Lucene version (VInts)
    let _major = input.read_vint()?;
    let _minor = input.read_vint()?;
    let _bugfix = input.read_vint()?;

    // Index created version major
    let _index_created_version = input.read_vint()?;

    // Segment infos version (BE long)
    let version = input.read_be_long()?;

    // Counter (VLong)
    let counter = input.read_vlong()?;

    // Number of segments (BE int)
    let num_segments = input.read_be_int()?;
    if num_segments < 0 {
        return Err(io::Error::other(format!(
            "invalid segment count: {num_segments}"
        )));
    }

    // Min segment version (only present if segments > 0)
    if num_segments > 0 {
        let _min_major = input.read_vint()?;
        let _min_minor = input.read_vint()?;
        let _min_bugfix = input.read_vint()?;
    }

    // Per-segment entries
    let mut segments = Vec::with_capacity(num_segments as usize);
    for _ in 0..num_segments {
        let seg_name = input.read_string()?;

        let mut seg_id = [0u8; codec_util::ID_LENGTH];
        input.read_bytes(&mut seg_id)?;

        let codec_name = input.read_string()?;

        let del_gen = input.read_be_long()?;
        let del_count = input.read_be_int()?;
        let field_infos_gen = input.read_be_long()?;
        let doc_values_gen = input.read_be_long()?;
        let soft_del_count = input.read_be_int()?;

        let sci_id = match input.read_byte()? {
            1 => {
                let mut id = [0u8; codec_util::ID_LENGTH];
                input.read_bytes(&mut id)?;
                Some(id)
            }
            0 => None,
            marker => {
                return Err(io::Error::other(format!("invalid SCI ID marker: {marker}")));
            }
        };

        // Field infos files (set of strings)
        let _field_infos_files = input.read_set_of_strings()?;

        // Doc values updates files
        let num_dv_fields = input.read_be_int()?;
        for _ in 0..num_dv_fields {
            let _field_number = input.read_be_int()?;
            let _files = input.read_set_of_strings()?;
        }

        segments.push(SegmentEntry {
            name: seg_name,
            id: seg_id,
            codec: codec_name,
            del_gen,
            del_count,
            field_infos_gen,
            doc_values_gen,
            soft_del_count,
            sci_id,
        });
    }

    // User data
    let user_data = input.read_map_of_strings()?;

    // Footer
    codec_util::check_footer(&mut input)?;

    debug!(
        "segment_infos: read {segment_file_name}, version={version}, \
         counter={counter}, num_segments={num_segments}"
    );

    Ok(SegmentInfosRead {
        segments,
        generation,
        version,
        counter,
        user_data,
    })
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

    // --- Read round-trip tests ---

    #[test]
    fn test_read_roundtrip_empty() {
        let user_data = HashMap::new();
        let file = write(&[], 1, 1, 0, &user_data).unwrap();

        // Put the segments_N file into a directory
        let mut dir = crate::store::MemoryDirectory::new();
        dir.write_file(&file.name, &file.data).unwrap();

        let result = read(&dir, &file.name).unwrap();
        assert_is_empty!(&result.segments);
        assert_eq!(result.version, 1);
        assert_eq!(result.counter, 0);
        assert_is_empty!(&result.user_data);
    }

    #[test]
    fn test_read_roundtrip_single_segment() {
        let seg_id = [0xABu8; 16];
        let sci_id = [0xCDu8; 16];
        let sci = make_test_segment_commit_info("_0", 3, seg_id, Some(sci_id));

        let user_data = HashMap::new();
        let file = write(&[&sci], 1, 1, 1, &user_data).unwrap();

        let mut dir = crate::store::MemoryDirectory::new();
        dir.write_file(&file.name, &file.data).unwrap();

        let result = read(&dir, &file.name).unwrap();

        assert_len_eq_x!(&result.segments, 1);
        assert_eq!(result.segments[0].name, "_0");
        assert_eq!(result.segments[0].id, seg_id);
        assert_eq!(result.segments[0].codec, "Lucene103");
        assert_eq!(result.segments[0].sci_id, Some(sci_id));
        assert_eq!(result.version, 1);
        assert_eq!(result.counter, 1);
    }

    // --- Write-side tests ---

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
