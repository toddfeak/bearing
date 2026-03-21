// SPDX-License-Identifier: Apache-2.0
//! Field infos format writer for per-field metadata (index options, doc values type, etc.).

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::document::{DocValuesType, IndexOptions};
use crate::index::index_file_names;
use crate::index::{FieldInfos, SegmentInfo};
use crate::store::SharedDirectory;

use crate::index::{FieldInfo, PointDimensionConfig};
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory};

const CODEC_NAME: &str = "Lucene94FieldInfos";
const FORMAT_CURRENT: i32 = 2; // FORMAT_DOCVALUE_SKIPPER
const EXTENSION: &str = "fnm";

// Field bit flags
const STORE_TERMVECTOR: u8 = 0x1;
const OMIT_NORMS: u8 = 0x2;
const STORE_PAYLOADS: u8 = 0x4;
const SOFT_DELETES_FIELD: u8 = 0x8;
const PARENT_FIELD_FIELD: u8 = 0x10;

/// Writes the .fnm (field infos) file to the directory.
/// Returns the file name written.
pub fn write(
    directory: &SharedDirectory,
    segment_info: &SegmentInfo,
    segment_suffix: &str,
    field_infos: &FieldInfos,
) -> io::Result<String> {
    let file_name =
        index_file_names::segment_file_name(&segment_info.name, segment_suffix, EXTENSION);
    let mut output = directory.lock().unwrap().create_output(&file_name)?;

    codec_util::write_index_header(
        &mut *output,
        CODEC_NAME,
        FORMAT_CURRENT,
        &segment_info.id,
        segment_suffix,
    )?;

    output.write_vint(field_infos.len() as i32)?;

    for fi in field_infos.iter() {
        debug!(
            "field_infos: field={:?} #{}, index_options={:?}, dv_type={:?}, omit_norms={}",
            fi.name(),
            fi.number(),
            fi.index_options(),
            fi.doc_values_type(),
            fi.omit_norms()
        );
        output.write_string(fi.name())?;
        output.write_vint(fi.number() as i32)?;

        // Field bits
        let mut bits: u8 = 0;
        if fi.store_term_vector() {
            bits |= STORE_TERMVECTOR;
        }
        if fi.omit_norms() {
            bits |= OMIT_NORMS;
        }
        if fi.store_payloads() {
            bits |= STORE_PAYLOADS;
        }
        if fi.soft_deletes_field() {
            bits |= SOFT_DELETES_FIELD;
        }
        if fi.is_parent_field() {
            bits |= PARENT_FIELD_FIELD;
        }
        output.write_byte(bits)?;

        // Index options (byte)
        output.write_byte(index_options_byte(fi.index_options()))?;

        // Doc values type (byte) — uses custom encoding, NOT enum ordinal
        output.write_byte(doc_values_byte(fi.doc_values_type()))?;

        // Doc values skip index type (FORMAT >= 2)
        output.write_byte(0)?; // NONE for our MVP

        // Doc values gen
        output.write_le_long(fi.dv_gen())?;

        // Attributes map
        output.write_map_of_strings(fi.attributes())?;

        // Point dimensions
        let pc = fi.point_config();
        output.write_vint(pc.dimension_count as i32)?;
        if pc.dimension_count != 0 {
            output.write_vint(pc.index_dimension_count as i32)?;
            output.write_vint(pc.num_bytes as i32)?;
        }

        // Vector dimension
        output.write_vint(fi.vector_dimension() as i32)?;

        // Vector encoding (byte) — 0 = BYTE
        output.write_byte(0)?;

        // Vector similarity function (byte) — 0 = EUCLIDEAN
        output.write_byte(0)?;
    }

    codec_util::write_footer(&mut *output)?;

    Ok(file_name)
}

/// Reads a `.fnm` (field infos) file from `directory`.
///
/// Validates the codec header, reads per-field metadata, and verifies the footer checksum.
pub fn read(
    directory: &dyn Directory,
    segment_info: &SegmentInfo,
    segment_suffix: &str,
) -> io::Result<FieldInfos> {
    let file_name =
        index_file_names::segment_file_name(&segment_info.name, segment_suffix, EXTENSION);
    let input = directory.open_input(&file_name)?;
    let mut checksum_input = ChecksumIndexInput::new(input);

    codec_util::check_index_header(
        &mut checksum_input,
        CODEC_NAME,
        FORMAT_CURRENT,
        FORMAT_CURRENT,
        &segment_info.id,
        segment_suffix,
    )?;

    let num_fields = checksum_input.read_vint()?;
    if num_fields < 0 {
        return Err(io::Error::other(format!(
            "invalid field count: {num_fields}"
        )));
    }

    let mut fields = Vec::with_capacity(num_fields as usize);

    for _ in 0..num_fields {
        let name = checksum_input.read_string()?;
        let number = checksum_input.read_vint()? as u32;

        // Field bits
        let bits = checksum_input.read_byte()?;
        let store_term_vector = bits & STORE_TERMVECTOR != 0;
        let omit_norms = bits & OMIT_NORMS != 0;
        let store_payloads = bits & STORE_PAYLOADS != 0;
        let soft_deletes_field = bits & SOFT_DELETES_FIELD != 0;
        let is_parent_field = bits & PARENT_FIELD_FIELD != 0;

        // Index options
        let index_options = byte_to_index_options(checksum_input.read_byte()?)?;

        // Doc values type
        let doc_values_type = byte_to_doc_values_type(checksum_input.read_byte()?)?;

        // Doc values skip index type (FORMAT >= 2) — read and discard
        let _dv_skip_index_type = checksum_input.read_byte()?;

        // Doc values gen
        let dv_gen = checksum_input.read_le_long()?;

        // Attributes
        let attributes = checksum_input.read_map_of_strings()?;

        // Point dimensions
        let dimension_count = checksum_input.read_vint()? as u32;
        let point_config = if dimension_count != 0 {
            let index_dimension_count = checksum_input.read_vint()? as u32;
            let num_bytes = checksum_input.read_vint()? as u32;
            PointDimensionConfig {
                dimension_count,
                index_dimension_count,
                num_bytes,
            }
        } else {
            PointDimensionConfig::default()
        };

        // Vector dimension (read and discard — not supported yet)
        let _vector_dimension = checksum_input.read_vint()?;
        // Vector encoding (byte)
        let _vector_encoding = checksum_input.read_byte()?;
        // Vector similarity (byte)
        let _vector_similarity = checksum_input.read_byte()?;

        let mut fi = FieldInfo::new(
            name,
            number,
            store_term_vector,
            omit_norms,
            index_options,
            doc_values_type,
            point_config,
        );
        fi.store_payloads = store_payloads;
        fi.soft_deletes_field = soft_deletes_field;
        fi.is_parent_field = is_parent_field;
        fi.dv_gen = dv_gen;
        for (k, v) in attributes {
            fi.put_attribute(k, v);
        }

        fields.push(fi);
    }

    codec_util::check_footer(&mut checksum_input)?;

    debug!(
        "field_infos: read {} fields from {}",
        fields.len(),
        file_name
    );

    Ok(FieldInfos::new(fields))
}

fn index_options_byte(opts: IndexOptions) -> u8 {
    match opts {
        IndexOptions::None => 0,
        IndexOptions::Docs => 1,
        IndexOptions::DocsAndFreqs => 2,
        IndexOptions::DocsAndFreqsAndPositions => 3,
        IndexOptions::DocsAndFreqsAndPositionsAndOffsets => 4,
    }
}

/// Encodes DocValuesType to the byte used in .fnm files.
/// NOTE: This is NOT the same as the enum ordinal!
/// SORTED_SET=4, SORTED_NUMERIC=5 in .fnm (swapped vs Java enum ordinals).
fn doc_values_byte(dvt: DocValuesType) -> u8 {
    match dvt {
        DocValuesType::None => 0,
        DocValuesType::Numeric => 1,
        DocValuesType::Binary => 2,
        DocValuesType::Sorted => 3,
        DocValuesType::SortedSet => 4,
        DocValuesType::SortedNumeric => 5,
    }
}

fn byte_to_index_options(b: u8) -> io::Result<IndexOptions> {
    match b {
        0 => Ok(IndexOptions::None),
        1 => Ok(IndexOptions::Docs),
        2 => Ok(IndexOptions::DocsAndFreqs),
        3 => Ok(IndexOptions::DocsAndFreqsAndPositions),
        4 => Ok(IndexOptions::DocsAndFreqsAndPositionsAndOffsets),
        _ => Err(io::Error::other(format!("invalid index options byte: {b}"))),
    }
}

fn byte_to_doc_values_type(b: u8) -> io::Result<DocValuesType> {
    match b {
        0 => Ok(DocValuesType::None),
        1 => Ok(DocValuesType::Numeric),
        2 => Ok(DocValuesType::Binary),
        3 => Ok(DocValuesType::Sorted),
        4 => Ok(DocValuesType::SortedSet),
        5 => Ok(DocValuesType::SortedNumeric),
        _ => Err(io::Error::other(format!(
            "invalid doc values type byte: {b}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Directory, MemoryDirectory, SharedDirectory};
    use std::collections::HashMap;

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    // Ported from org.apache.lucene.codecs.lucene94.TestLucene94FieldInfosFormat

    fn make_test_segment() -> SegmentInfo {
        SegmentInfo::new(
            "_0".to_string(),
            3,
            true,
            [0u8; 16],
            HashMap::new(),
            HashMap::new(),
        )
    }

    #[test]
    fn test_write_single_field() {
        let dir = test_directory();
        let si = make_test_segment();
        let fi = FieldInfo::new(
            "test".to_string(),
            0,
            false,
            false,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::None,
            PointDimensionConfig::default(),
        );
        let fis = FieldInfos::new(vec![fi]);

        let name = write(&dir, &si, "", &fis).unwrap();
        assert_eq!(name, "_0.fnm");

        let data = dir.lock().unwrap().read_file(&name).unwrap();
        assert_not_empty!(data);

        // Verify header magic (first 4 bytes, BE)
        assert_eq!(&data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Verify footer magic (last 16 bytes)
        let footer_start = data.len() - 16;
        assert_eq!(
            &data[footer_start..footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
    }

    #[test]
    fn test_write_three_demo_fields() {
        let si = make_test_segment();

        // "path": KeywordField — DOCS, omitNorms, SortedSet DV
        let mut fi_path = FieldInfo::new(
            "path".to_string(),
            0,
            false,
            true,
            IndexOptions::Docs,
            DocValuesType::SortedSet,
            PointDimensionConfig::default(),
        );
        fi_path.put_attribute(
            "PerFieldPostingsFormat.format".to_string(),
            "Lucene103".to_string(),
        );
        fi_path.put_attribute("PerFieldPostingsFormat.suffix".to_string(), "0".to_string());
        fi_path.put_attribute(
            "PerFieldDocValuesFormat.format".to_string(),
            "Lucene90".to_string(),
        );
        fi_path.put_attribute(
            "PerFieldDocValuesFormat.suffix".to_string(),
            "0".to_string(),
        );

        // "modified": LongField — no index, SortedNumeric DV, points
        let mut fi_modified = FieldInfo::new(
            "modified".to_string(),
            1,
            false,
            false,
            IndexOptions::None,
            DocValuesType::SortedNumeric,
            PointDimensionConfig {
                dimension_count: 1,
                index_dimension_count: 1,
                num_bytes: 8,
            },
        );
        fi_modified.put_attribute(
            "PerFieldDocValuesFormat.format".to_string(),
            "Lucene90".to_string(),
        );
        fi_modified.put_attribute(
            "PerFieldDocValuesFormat.suffix".to_string(),
            "0".to_string(),
        );

        // "contents": TextField — DOCS_AND_FREQS_AND_POSITIONS
        let mut fi_contents = FieldInfo::new(
            "contents".to_string(),
            2,
            false,
            false,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::None,
            PointDimensionConfig::default(),
        );
        fi_contents.put_attribute(
            "PerFieldPostingsFormat.format".to_string(),
            "Lucene103".to_string(),
        );
        fi_contents.put_attribute("PerFieldPostingsFormat.suffix".to_string(), "0".to_string());

        let fis = FieldInfos::new(vec![fi_path, fi_modified, fi_contents]);
        let dir = test_directory();
        let name = write(&dir, &si, "", &fis).unwrap();

        assert_eq!(name, "_0.fnm");

        let data = dir.lock().unwrap().read_file(&name).unwrap();
        assert_not_empty!(data);

        // Basic structural checks:
        // Header + 3 fields + footer, should be substantial
        assert_gt!(data.len(), 100);
    }

    #[test]
    fn test_doc_values_byte_encoding() {
        // Verify the .fnm uses different ordinals than the Rust enum
        assert_eq!(doc_values_byte(DocValuesType::None), 0);
        assert_eq!(doc_values_byte(DocValuesType::Numeric), 1);
        assert_eq!(doc_values_byte(DocValuesType::Binary), 2);
        assert_eq!(doc_values_byte(DocValuesType::Sorted), 3);
        assert_eq!(doc_values_byte(DocValuesType::SortedSet), 4); // NOT 5
        assert_eq!(doc_values_byte(DocValuesType::SortedNumeric), 5); // NOT 4
    }

    // --- Read round-trip tests ---

    #[test]
    fn test_read_roundtrip_single_field() {
        let dir = test_directory();
        let si = make_test_segment();
        let fi = FieldInfo::new(
            "test".to_string(),
            0,
            false,
            false,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::None,
            PointDimensionConfig::default(),
        );
        let fis = FieldInfos::new(vec![fi]);
        write(&dir, &si, "", &fis).unwrap();

        let dir_guard = dir.lock().unwrap();
        let read_fis = read(&**dir_guard, &si, "").unwrap();

        assert_eq!(read_fis.len(), 1);
        let f = &read_fis.iter().next().unwrap();
        assert_eq!(f.name(), "test");
        assert_eq!(f.number(), 0);
        assert_eq!(f.index_options(), IndexOptions::DocsAndFreqsAndPositions);
        assert_eq!(f.doc_values_type(), DocValuesType::None);
        assert!(!f.store_term_vector());
        assert!(!f.omit_norms());
    }

    #[test]
    fn test_read_roundtrip_multiple_fields() {
        let dir = test_directory();
        let si = make_test_segment();

        let fi_keyword = FieldInfo::new(
            "path".to_string(),
            0,
            false,
            true,
            IndexOptions::Docs,
            DocValuesType::SortedSet,
            PointDimensionConfig::default(),
        );
        let fi_text = FieldInfo::new(
            "body".to_string(),
            1,
            true,
            false,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::None,
            PointDimensionConfig::default(),
        );
        let fi_point = FieldInfo::new(
            "location".to_string(),
            2,
            false,
            true,
            IndexOptions::None,
            DocValuesType::None,
            PointDimensionConfig {
                dimension_count: 2,
                index_dimension_count: 2,
                num_bytes: 4,
            },
        );
        let fis = FieldInfos::new(vec![fi_keyword, fi_text, fi_point]);
        write(&dir, &si, "", &fis).unwrap();

        let dir_guard = dir.lock().unwrap();
        let read_fis = read(&**dir_guard, &si, "").unwrap();

        assert_eq!(read_fis.len(), 3);
        let fields: Vec<_> = read_fis.iter().collect();

        assert_eq!(fields[0].name(), "path");
        assert!(fields[0].omit_norms());
        assert_eq!(fields[0].doc_values_type(), DocValuesType::SortedSet);

        assert_eq!(fields[1].name(), "body");
        assert!(fields[1].store_term_vector());
        assert_eq!(
            fields[1].index_options(),
            IndexOptions::DocsAndFreqsAndPositions
        );

        assert_eq!(fields[2].name(), "location");
        assert_eq!(fields[2].point_config().dimension_count, 2);
        assert_eq!(fields[2].point_config().num_bytes, 4);
    }

    #[test]
    fn test_read_roundtrip_with_attributes() {
        let dir = test_directory();
        let si = make_test_segment();
        let mut fi = FieldInfo::new(
            "test".to_string(),
            0,
            false,
            false,
            IndexOptions::Docs,
            DocValuesType::Numeric,
            PointDimensionConfig::default(),
        );
        fi.put_attribute("format".to_string(), "Lucene90".to_string());
        fi.put_attribute("suffix".to_string(), "0".to_string());
        let fis = FieldInfos::new(vec![fi]);
        write(&dir, &si, "", &fis).unwrap();

        let dir_guard = dir.lock().unwrap();
        let read_fis = read(&**dir_guard, &si, "").unwrap();
        let f = read_fis.iter().next().unwrap();
        assert_eq!(f.attributes().get("format").unwrap(), "Lucene90");
        assert_eq!(f.attributes().get("suffix").unwrap(), "0");
    }

    // --- Write-side tests ---

    #[test]
    fn test_index_options_byte_encoding() {
        assert_eq!(index_options_byte(IndexOptions::None), 0);
        assert_eq!(index_options_byte(IndexOptions::Docs), 1);
        assert_eq!(index_options_byte(IndexOptions::DocsAndFreqs), 2);
        assert_eq!(
            index_options_byte(IndexOptions::DocsAndFreqsAndPositions),
            3
        );
        assert_eq!(
            index_options_byte(IndexOptions::DocsAndFreqsAndPositionsAndOffsets),
            4
        );
    }
}
