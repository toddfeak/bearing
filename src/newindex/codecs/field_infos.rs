// SPDX-License-Identifier: Apache-2.0

// DEBT: adapted from codecs::lucene94::field_infos_format — reconcile after
// switchover by updating the original to accept newindex types directly.

//! Field infos (.fnm) writer for the newindex pipeline.

use std::collections::HashMap;
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::document::DocValuesType;
use crate::newindex::index_file_names;
use crate::store::SharedDirectory;

const CODEC_NAME: &str = "Lucene94FieldInfos";
const FORMAT_CURRENT: i32 = 2; // FORMAT_DOCVALUE_SKIPPER
const EXTENSION: &str = "fnm";

/// Per-field metadata for writing the .fnm file.
// DEBT: parallel to index::FieldInfo — merge after switchover
#[derive(Debug, Clone)]
pub(crate) struct FieldInfo {
    pub name: String,
    pub number: u32,
    pub has_norms: bool,
    pub index_options: u8,
    pub doc_values_type: DocValuesType,
}

/// Collection of field metadata for a segment.
// DEBT: parallel to index::FieldInfos — merge after switchover
pub(crate) struct FieldInfos {
    fields: Vec<FieldInfo>,
}

impl FieldInfos {
    pub fn new(fields: Vec<FieldInfo>) -> Self {
        Self { fields }
    }
}

/// Encodes a [`DocValuesType`] to the .fnm byte format.
///
/// Matches `Lucene94FieldInfosFormat.docValuesByte` — note that
/// `SortedSet` and `SortedNumeric` have different byte values than
/// their Rust enum discriminants.
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

/// Writes the .fnm file for a segment. Returns the file name written.
pub(crate) fn write(
    directory: &SharedDirectory,
    segment_name: &str,
    segment_suffix: &str,
    segment_id: &[u8; 16],
    field_infos: &FieldInfos,
) -> io::Result<String> {
    let file_name = index_file_names::segment_file_name(segment_name, segment_suffix, EXTENSION);
    let mut output = directory.lock().unwrap().create_output(&file_name)?;

    codec_util::write_index_header(
        &mut *output,
        CODEC_NAME,
        FORMAT_CURRENT,
        segment_id,
        segment_suffix,
    )?;

    output.write_vint(field_infos.fields.len() as i32)?;

    for fi in &field_infos.fields {
        debug!(
            "field_infos: field={:?} #{}, has_norms={}, index_options={}",
            fi.name, fi.number, fi.has_norms, fi.index_options
        );

        // Field name
        output.write_string(&fi.name)?;

        // Field number
        output.write_vint(fi.number as i32)?;

        // Field bits — matches Lucene94FieldInfosFormat constants:
        //   0x01 = STORE_TERMVECTOR
        //   0x02 = OMIT_NORMS
        //   0x04 = STORE_PAYLOADS
        let mut bits: u8 = 0;
        if !fi.has_norms {
            bits |= 0b0000_0010; // OMIT_NORMS
        }
        output.write_byte(bits)?;

        // Index options
        output.write_byte(fi.index_options)?;

        // Doc values type — custom encoding matching Lucene94FieldInfosFormat,
        // NOT the DocValuesType enum ordinal (SortedSet/SortedNumeric are swapped)
        output.write_byte(doc_values_byte(fi.doc_values_type))?;

        // Doc values skip index type: 0 = NONE
        output.write_byte(0)?;

        // Doc values gen: -1 (no per-gen doc values)
        output.write_le_long(-1)?;

        // Attributes: per-field format metadata
        let mut attrs = HashMap::new();
        if fi.index_options > 0 {
            attrs.insert(
                "PerFieldPostingsFormat.format".to_string(),
                "Lucene103".to_string(),
            );
            attrs.insert("PerFieldPostingsFormat.suffix".to_string(), "0".to_string());
        }
        if fi.doc_values_type != DocValuesType::None {
            attrs.insert(
                "PerFieldDocValuesFormat.format".to_string(),
                "Lucene90".to_string(),
            );
            attrs.insert(
                "PerFieldDocValuesFormat.suffix".to_string(),
                "0".to_string(),
            );
        }
        output.write_map_of_strings(&attrs)?;

        // Point dimensions: 0 (no points)
        output.write_vint(0)?;

        // Vector dimension: 0
        output.write_vint(0)?;

        // Vector encoding: 0 = BYTE
        output.write_byte(0)?;

        // Vector similarity: 0 = EUCLIDEAN
        output.write_byte(0)?;
    }

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

    fn stored_only(name: &str, number: u32) -> FieldInfo {
        FieldInfo {
            name: name.to_string(),
            number,
            has_norms: false,
            index_options: 0,
            doc_values_type: DocValuesType::None,
        }
    }

    fn indexed_with_norms(name: &str, number: u32) -> FieldInfo {
        FieldInfo {
            name: name.to_string(),
            number,
            has_norms: true,
            index_options: 3, // DocsAndFreqsAndPositions
            doc_values_type: DocValuesType::None,
        }
    }

    #[test]
    fn write_produces_fnm_file() {
        let dir = test_directory();
        let fields = vec![stored_only("title", 0), stored_only("body", 1)];
        let fis = FieldInfos::new(fields);
        let name = write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();
        assert_eq!(name, "_0.fnm");

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
    fn write_encodes_field_count_and_names() {
        let dir = test_directory();
        let fields = vec![stored_only("title", 0), stored_only("body", 1)];
        let fis = FieldInfos::new(fields);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();

        // Header = 4(magic) + 1+18(codec "Lucene94FieldInfos") + 4(version) + 16(id) + 1(suffix) = 44
        let offset = 44;

        // Field count = 2 (VInt)
        assert_eq!(data[offset], 2);

        // First field name: VInt length(5) + "title"
        assert_eq!(data[offset + 1], 5); // length
        assert_eq!(&data[offset + 2..offset + 7], b"title");

        // File should be substantial (2 fields with all metadata)
        assert!(data.len() > 80);
    }

    #[test]
    fn stored_only_field_has_omit_norms() {
        let dir = test_directory();
        let fis = FieldInfos::new(vec![stored_only("f", 0)]);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();

        // Header(44) + field_count(1) + name_len(1) + "f"(1) + field_number(1) = 48
        let bits_offset = 48;

        // bits byte: OMIT_NORMS = 0x02 (stored-ness is not in the bits byte)
        assert_eq!(data[bits_offset], 0b0000_0010);

        // index options byte: NONE = 0
        assert_eq!(data[bits_offset + 1], 0);

        // doc values type byte: NONE = 0
        assert_eq!(data[bits_offset + 2], 0);
    }

    #[test]
    fn indexed_field_with_norms() {
        let dir = test_directory();
        let fis = FieldInfos::new(vec![indexed_with_norms("body", 0)]);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();

        // Header(44) + field_count(1) + name_len(1) + "body"(4) + field_number(1) = 51
        let bits_offset = 51;

        // bits byte: no IS_STORED, no OMIT_NORMS = 0x00
        assert_eq!(data[bits_offset], 0b0000_0000);

        // index options byte: 3 = DocsAndFreqsAndPositions
        assert_eq!(data[bits_offset + 1], 3);
    }

    #[test]
    fn indexed_stored_field_with_norms() {
        let dir = test_directory();
        let fields = vec![FieldInfo {
            name: "body".to_string(),
            number: 0,
            has_norms: true,
            index_options: 3,
            doc_values_type: DocValuesType::None,
        }];
        let fis = FieldInfos::new(fields);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();

        let bits_offset = 51;

        // bits byte: no flags (has norms, no term vectors, no payloads)
        assert_eq!(data[bits_offset], 0b0000_0000);

        // index options byte: 3
        assert_eq!(data[bits_offset + 1], 3);
    }

    #[test]
    fn indexed_field_has_postings_format_attributes() {
        let dir = test_directory();
        let fis = FieldInfos::new(vec![indexed_with_norms("body", 0)]);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();
        let content = String::from_utf8_lossy(&data);

        // Indexed fields must have PerFieldPostingsFormat attributes
        assert!(content.contains("PerFieldPostingsFormat.format"));
        assert!(content.contains("Lucene103"));
        assert!(content.contains("PerFieldPostingsFormat.suffix"));
    }

    #[test]
    fn stored_only_field_has_no_postings_format_attributes() {
        let dir = test_directory();
        let fis = FieldInfos::new(vec![stored_only("title", 0)]);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();
        let content = String::from_utf8_lossy(&data);

        // Stored-only fields must NOT have PerFieldPostingsFormat attributes
        assert!(!content.contains("PerFieldPostingsFormat"));
    }

    #[test]
    fn dv_only_field_writes_correct_type_byte() {
        let dir = test_directory();
        let fis = FieldInfos::new(vec![FieldInfo {
            name: "count".to_string(),
            number: 0,
            has_norms: false,
            index_options: 0,
            doc_values_type: DocValuesType::Numeric,
        }]);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();

        // Header(44) + field_count(1) + name_len(1) + "count"(5) + field_number(1) = 52
        let bits_offset = 52;

        // bits byte: OMIT_NORMS
        assert_eq!(data[bits_offset], 0b0000_0010);
        // index options: NONE
        assert_eq!(data[bits_offset + 1], 0);
        // doc values type: Numeric = 1
        assert_eq!(data[bits_offset + 2], 1);
    }

    #[test]
    fn dv_sorted_set_writes_byte_4() {
        let dir = test_directory();
        let fis = FieldInfos::new(vec![FieldInfo {
            name: "tags".to_string(),
            number: 0,
            has_norms: false,
            index_options: 0,
            doc_values_type: DocValuesType::SortedSet,
        }]);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();

        // Header(44) + field_count(1) + name_len(1) + "tags"(4) + field_number(1) = 51
        let bits_offset = 51;

        // doc values type: SortedSet = 4 in .fnm format
        assert_eq!(data[bits_offset + 2], 4);
    }

    #[test]
    fn dv_sorted_numeric_writes_byte_5() {
        let dir = test_directory();
        let fis = FieldInfos::new(vec![FieldInfo {
            name: "vals".to_string(),
            number: 0,
            has_norms: false,
            index_options: 0,
            doc_values_type: DocValuesType::SortedNumeric,
        }]);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();

        // Header(44) + field_count(1) + name_len(1) + "vals"(4) + field_number(1) = 51
        let bits_offset = 51;

        // doc values type: SortedNumeric = 5 in .fnm format
        assert_eq!(data[bits_offset + 2], 5);
    }

    #[test]
    fn dv_field_has_doc_values_format_attributes() {
        let dir = test_directory();
        let fis = FieldInfos::new(vec![FieldInfo {
            name: "count".to_string(),
            number: 0,
            has_norms: false,
            index_options: 0,
            doc_values_type: DocValuesType::SortedNumeric,
        }]);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();
        let content = String::from_utf8_lossy(&data);

        assert!(content.contains("PerFieldDocValuesFormat.format"));
        assert!(content.contains("Lucene90"));
        assert!(content.contains("PerFieldDocValuesFormat.suffix"));
        // No postings format for DV-only field
        assert!(!content.contains("PerFieldPostingsFormat"));
    }

    #[test]
    fn dv_type_none_has_no_dv_attributes() {
        let dir = test_directory();
        let fis = FieldInfos::new(vec![stored_only("title", 0)]);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();
        let content = String::from_utf8_lossy(&data);

        assert!(!content.contains("PerFieldDocValuesFormat"));
    }
}
