// SPDX-License-Identifier: Apache-2.0

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::document::{DocValuesType, IndexOptions};
use crate::index::index_file_names;
use crate::index::{FieldInfos, SegmentInfo};
use crate::store::memory::MemoryIndexOutput;
use crate::store::{DataOutput, SegmentFile};

const CODEC_NAME: &str = "Lucene94FieldInfos";
const FORMAT_CURRENT: i32 = 2; // FORMAT_DOCVALUE_SKIPPER
const EXTENSION: &str = "fnm";

// Field bit flags
const STORE_TERMVECTOR: u8 = 0x1;
const OMIT_NORMS: u8 = 0x2;
const STORE_PAYLOADS: u8 = 0x4;
const SOFT_DELETES_FIELD: u8 = 0x8;
const PARENT_FIELD_FIELD: u8 = 0x10;

/// Writes the .fnm (field infos) file.
/// Returns a [`SegmentFile`] for the .fnm file.
pub fn write(
    segment_info: &SegmentInfo,
    segment_suffix: &str,
    field_infos: &FieldInfos,
) -> io::Result<SegmentFile> {
    let file_name =
        index_file_names::segment_file_name(&segment_info.name, segment_suffix, EXTENSION);
    let mut output = MemoryIndexOutput::new(file_name.clone());

    codec_util::write_index_header(
        &mut output,
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

    codec_util::write_footer(&mut output)?;

    Ok(output.into_inner())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{FieldInfo, PointDimensionConfig};
    use std::collections::HashMap;

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

        let file = write(&si, "", &fis).unwrap();
        assert_eq!(file.name, "_0.fnm");
        assert!(!file.data.is_empty());

        // Verify header magic (first 4 bytes, BE)
        assert_eq!(&file.data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Verify footer magic (last 16 bytes)
        let footer_start = file.data.len() - 16;
        assert_eq!(
            &file.data[footer_start..footer_start + 4],
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
        let file = write(&si, "", &fis).unwrap();

        assert_eq!(file.name, "_0.fnm");
        assert!(!file.data.is_empty());

        // Basic structural checks:
        // Header + 3 fields + footer, should be substantial
        assert!(file.data.len() > 100);
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
