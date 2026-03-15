// SPDX-License-Identifier: Apache-2.0
//! Norms writer for per-field normalization values used in scoring.

use std::collections::HashMap;
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::index::FieldInfos;
use crate::index::index_file_names;
use crate::index::indexing_chain::PerFieldData;
use crate::store::memory::MemoryIndexOutput;
use crate::store::{DataOutput, IndexOutput, SegmentFile};

// File extensions
const DATA_EXTENSION: &str = "nvd";
const META_EXTENSION: &str = "nvm";

// Codec names and versions
const DATA_CODEC: &str = "Lucene90NormsData";
const META_CODEC: &str = "Lucene90NormsMetadata";
const VERSION: i32 = 0;

/// Writes norms files (.nvm, .nvd) for a segment.
/// Returns a list of [`SegmentFile`]s.
pub fn write(
    segment_name: &str,
    segment_suffix: &str,
    segment_id: &[u8; 16],
    field_infos: &FieldInfos,
    per_field: &HashMap<String, PerFieldData>,
    num_docs: i32,
) -> io::Result<Vec<SegmentFile>> {
    let nvm_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);
    let nvd_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, DATA_EXTENSION);

    let mut nvm = MemoryIndexOutput::new(nvm_name);
    let mut nvd = MemoryIndexOutput::new(nvd_name);

    // Write index headers
    codec_util::write_index_header(&mut nvm, META_CODEC, VERSION, segment_id, segment_suffix)?;
    codec_util::write_index_header(&mut nvd, DATA_CODEC, VERSION, segment_id, segment_suffix)?;

    // Iterate fields in field-number order
    for fi in field_infos.iter() {
        if !fi.has_norms() {
            continue;
        }

        let Some(pfd) = per_field.get(fi.name()) else {
            // Field declared with norms but no documents contributed norms
            write_empty_norms_metadata(&mut nvm, fi.number())?;
            continue;
        };
        let (norms, norms_docs) = (&pfd.norms, &pfd.norms_docs);

        let num_docs_with_value = norms_docs.len() as i32;

        if num_docs_with_value == 0 {
            debug!(
                "norms: field={:?} (#{}) -> EMPTY pattern",
                fi.name(),
                fi.number()
            );
            write_empty_norms_metadata(&mut nvm, fi.number())?;
            continue;
        }

        // Compute min and max norm values
        let min = *norms.iter().min().unwrap();
        let max = *norms.iter().max().unwrap();
        let bytes_per_norm = num_bytes_per_value(min, max);

        if num_docs_with_value == num_docs {
            // ALL pattern: every document has a norm for this field
            debug!(
                "norms: field={:?} (#{}) -> ALL pattern, bytes_per_norm={}, min={}, max={}, num_docs_with_field={}",
                fi.name(),
                fi.number(),
                bytes_per_norm,
                min,
                max,
                num_docs_with_value
            );
            nvm.write_le_int(fi.number() as i32)?; // field_number
            nvm.write_le_long(-1)?; // docs_with_field_offset = ALL
            nvm.write_le_long(0)?; // docs_with_field_length
            nvm.write_le_short(-1)?; // jump_table_entry_count
            nvm.write_byte(0xFF)?; // dense_rank_power (-1 as byte)
            nvm.write_le_int(num_docs_with_value)?; // num_docs_with_field

            if bytes_per_norm == 0 {
                // Constant: all norms are the same value, store in metadata
                nvm.write_byte(0)?; // bytes_per_norm
                nvm.write_le_long(min)?; // norms_offset = constant value
            } else {
                nvm.write_byte(bytes_per_norm)?;
                let data_offset = nvd.file_pointer() as i64;
                nvm.write_le_long(data_offset)?; // norms_offset

                // Write norm values to .nvd
                write_norm_values(&mut nvd, norms, bytes_per_norm)?;
            }
        } else {
            // SPARSE pattern — not implemented for MVP
            return Err(io::Error::other(format!(
                "SPARSE norms pattern not implemented: field '{}' has {} docs with norms out of {} total",
                fi.name(),
                num_docs_with_value,
                num_docs
            )));
        }
    }

    // EOF marker
    nvm.write_le_int(-1)?;

    // Write footers
    codec_util::write_footer(&mut nvm)?;
    codec_util::write_footer(&mut nvd)?;

    Ok(vec![nvm.into_inner(), nvd.into_inner()])
}

/// Writes metadata for a field with no norms (EMPTY pattern).
fn write_empty_norms_metadata(nvm: &mut dyn DataOutput, field_number: u32) -> io::Result<()> {
    nvm.write_le_int(field_number as i32)?; // field_number
    nvm.write_le_long(-2)?; // docs_with_field_offset = EMPTY
    nvm.write_le_long(0)?; // docs_with_field_length
    nvm.write_le_short(-1)?; // jump_table_entry_count
    nvm.write_byte(0xFF)?; // dense_rank_power (-1 as byte)
    nvm.write_le_int(0)?; // num_docs_with_field
    nvm.write_byte(0)?; // bytes_per_norm
    nvm.write_le_long(0)?; // norms_offset
    Ok(())
}

/// Determines how many bytes are needed per norm value.
/// Matches Java's Lucene90NormsConsumer.numBytesPerValue.
fn num_bytes_per_value(min: i64, max: i64) -> u8 {
    if min >= max {
        0
    } else if min >= -128 && max <= 127 {
        1
    } else if min >= -32768 && max <= 32767 {
        2
    } else if min >= i32::MIN as i64 && max <= i32::MAX as i64 {
        4
    } else {
        8
    }
}

/// Writes norm values to the data file using the given bytes_per_norm encoding.
fn write_norm_values(
    nvd: &mut dyn DataOutput,
    norms: &[i64],
    bytes_per_norm: u8,
) -> io::Result<()> {
    for &norm in norms {
        match bytes_per_norm {
            1 => nvd.write_byte(norm as u8)?,
            2 => nvd.write_le_short(norm as i16)?,
            4 => nvd.write_le_int(norm as i32)?,
            8 => nvd.write_le_long(norm)?,
            _ => unreachable!("invalid bytes_per_norm: {}", bytes_per_norm),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::codec_util::{FOOTER_LENGTH, index_header_length};
    use crate::document::{DocValuesType, IndexOptions};
    use crate::index::indexing_chain::{DocValuesAccumulator, PerFieldData};
    use crate::index::{FieldInfo, FieldInfos};
    use crate::test_util;
    use std::collections::HashMap;

    // Ported from org.apache.lucene.codecs.lucene90.TestLucene90NormsFormat

    fn make_field_info(name: &str, number: u32, has_norms: bool) -> FieldInfo {
        test_util::make_field_info(
            name,
            number,
            !has_norms,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::None,
        )
    }

    fn make_per_field_data(norms: Vec<i64>, norms_docs: Vec<i32>) -> PerFieldData {
        PerFieldData {
            postings: HashMap::new(),
            doc_values: DocValuesAccumulator::None,
            norms,
            norms_docs,
            points: Vec::new(),
        }
    }

    /// Size of one metadata entry in bytes:
    /// 4 (field_number) + 8 (offset) + 8 (length) + 2 (jump_table) + 1 (rank_power)
    /// + 4 (num_docs) + 1 (bytes_per_norm) + 8 (norms_offset) = 36
    const META_ENTRY_SIZE: usize = 36;

    #[test]
    fn test_all_pattern_1byte_norms() {
        let fi = make_field_info("contents", 2, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        // 3 docs with different norm values (e.g., field lengths 3, 4, 5)
        // int_to_byte4(3)=12, int_to_byte4(4)=8, int_to_byte4(5)=10
        per_field.insert(
            "contents".to_string(),
            make_per_field_data(vec![12, 8, 10], vec![0, 1, 2]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 3).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "_0.nvm");
        assert_eq!(result[1].name, "_0.nvd");

        let nvm = &result[0].data;
        let nvd = &result[1].data;

        // Both files should start with codec magic
        assert_eq!(&nvm[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);
        assert_eq!(&nvd[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Parse metadata entry (starts after header)
        let meta_header_len = index_header_length(META_CODEC, "");
        let entry = &nvm[meta_header_len..];

        // field_number = 2
        assert_eq!(&entry[0..4], &2i32.to_le_bytes());

        // docs_with_field_offset = -1 (ALL)
        assert_eq!(&entry[4..12], &(-1i64).to_le_bytes());

        // docs_with_field_length = 0
        assert_eq!(&entry[12..20], &0i64.to_le_bytes());

        // jump_table_entry_count = -1
        assert_eq!(&entry[20..22], &(-1i16).to_le_bytes());

        // dense_rank_power = 0xFF
        assert_eq!(entry[22], 0xFF);

        // num_docs_with_field = 3
        assert_eq!(&entry[23..27], &3i32.to_le_bytes());

        // bytes_per_norm = 1
        assert_eq!(entry[27], 1);

        // norms_offset = 0 (data starts at offset 0 in .nvd data area)
        let data_header_len = index_header_length(DATA_CODEC, "");
        let expected_offset = data_header_len as i64;
        assert_eq!(&entry[28..36], &expected_offset.to_le_bytes());

        // EOF marker = -1
        assert_eq!(
            &nvm[meta_header_len + META_ENTRY_SIZE..meta_header_len + META_ENTRY_SIZE + 4],
            &(-1i32).to_le_bytes()
        );

        // Verify footer magic on .nvm
        let nvm_footer_start = nvm.len() - FOOTER_LENGTH;
        assert_eq!(
            &nvm[nvm_footer_start..nvm_footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );

        // Verify .nvd contains 3 norm bytes at the expected offset
        assert_eq!(nvd[data_header_len], 12u8); // norm for doc 0
        assert_eq!(nvd[data_header_len + 1], 8u8); // norm for doc 1
        assert_eq!(nvd[data_header_len + 2], 10u8); // norm for doc 2

        // Verify footer magic on .nvd
        let nvd_footer_start = nvd.len() - FOOTER_LENGTH;
        assert_eq!(
            &nvd[nvd_footer_start..nvd_footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
    }

    #[test]
    fn test_empty_pattern() {
        let fi = make_field_info("contents", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        // Field exists but no documents contributed norms
        per_field.insert("contents".to_string(), make_per_field_data(vec![], vec![]));

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 3).unwrap();

        let nvm = &result[0].data;
        let meta_header_len = index_header_length(META_CODEC, "");
        let entry = &nvm[meta_header_len..];

        // field_number = 0
        assert_eq!(&entry[0..4], &0i32.to_le_bytes());

        // docs_with_field_offset = -2 (EMPTY)
        assert_eq!(&entry[4..12], &(-2i64).to_le_bytes());

        // num_docs_with_field = 0
        assert_eq!(&entry[23..27], &0i32.to_le_bytes());

        // bytes_per_norm = 0
        assert_eq!(entry[27], 0);
    }

    #[test]
    fn test_empty_pattern_field_not_in_per_field() {
        // Field has norms but doesn't appear in per_field at all
        let fi = make_field_info("missing", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let per_field = HashMap::new(); // empty

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 3).unwrap();

        let nvm = &result[0].data;
        let meta_header_len = index_header_length(META_CODEC, "");
        let entry = &nvm[meta_header_len..];

        // docs_with_field_offset = -2 (EMPTY)
        assert_eq!(&entry[4..12], &(-2i64).to_le_bytes());
    }

    #[test]
    fn test_constant_norms() {
        let fi = make_field_info("contents", 1, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        // All 3 docs have the same norm value
        per_field.insert(
            "contents".to_string(),
            make_per_field_data(vec![12, 12, 12], vec![0, 1, 2]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 3).unwrap();

        let nvm = &result[0].data;
        let nvd = &result[1].data;
        let meta_header_len = index_header_length(META_CODEC, "");
        let entry = &nvm[meta_header_len..];

        // ALL pattern
        assert_eq!(&entry[4..12], &(-1i64).to_le_bytes());

        // num_docs_with_field = 3
        assert_eq!(&entry[23..27], &3i32.to_le_bytes());

        // bytes_per_norm = 0 (constant)
        assert_eq!(entry[27], 0);

        // norms_offset = constant value = 12
        assert_eq!(&entry[28..36], &12i64.to_le_bytes());

        // .nvd should have only header + footer (no data bytes)
        let data_header_len = index_header_length(DATA_CODEC, "");
        assert_eq!(nvd.len(), data_header_len + FOOTER_LENGTH);
    }

    #[test]
    fn test_no_norms_fields_skipped() {
        // Field with omit_norms=true should be skipped entirely
        let fi_path = make_field_info("path", 0, false); // no norms
        let fi_contents = make_field_info("contents", 1, true); // has norms
        let field_infos = FieldInfos::new(vec![fi_path, fi_contents]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "contents".to_string(),
            make_per_field_data(vec![12, 8], vec![0, 1]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 2).unwrap();

        let nvm = &result[0].data;
        let meta_header_len = index_header_length(META_CODEC, "");
        let entry = &nvm[meta_header_len..];

        // Only "contents" (field_number=1) should have a metadata entry
        assert_eq!(&entry[0..4], &1i32.to_le_bytes());

        // After the entry, the next i32 should be EOF marker (-1)
        assert_eq!(
            &nvm[meta_header_len + META_ENTRY_SIZE..meta_header_len + META_ENTRY_SIZE + 4],
            &(-1i32).to_le_bytes()
        );
    }

    #[test]
    fn test_segment_suffix() {
        let fi = make_field_info("f", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert("f".to_string(), make_per_field_data(vec![10], vec![0]));

        let segment_id = [0u8; 16];
        let result = write("_0", "Lucene90_0", &segment_id, &field_infos, &per_field, 1).unwrap();

        assert_eq!(result[0].name, "_0_Lucene90_0.nvm");
        assert_eq!(result[1].name, "_0_Lucene90_0.nvd");
    }

    #[test]
    fn test_num_bytes_per_value() {
        // Constant
        assert_eq!(num_bytes_per_value(5, 5), 0);
        assert_eq!(num_bytes_per_value(0, 0), 0);

        // 1 byte (signed byte range)
        assert_eq!(num_bytes_per_value(0, 127), 1);
        assert_eq!(num_bytes_per_value(-128, 127), 1);
        assert_eq!(num_bytes_per_value(-128, 0), 1);

        // 2 bytes
        assert_eq!(num_bytes_per_value(0, 128), 2);
        assert_eq!(num_bytes_per_value(-129, 0), 2);
        assert_eq!(num_bytes_per_value(-32768, 32767), 2);

        // 4 bytes
        assert_eq!(num_bytes_per_value(0, 32768), 4);
        assert_eq!(num_bytes_per_value(i32::MIN as i64, i32::MAX as i64), 4);

        // 8 bytes
        assert_eq!(num_bytes_per_value(i32::MIN as i64 - 1, 0), 8);
        assert_eq!(num_bytes_per_value(0, i32::MAX as i64 + 1), 8);
    }

    #[test]
    fn test_multiple_fields_with_norms() {
        let fi_a = make_field_info("alpha", 0, true);
        let fi_b = make_field_info("beta", 1, true);
        let field_infos = FieldInfos::new(vec![fi_a, fi_b]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "alpha".to_string(),
            make_per_field_data(vec![5, 5, 5], vec![0, 1, 2]),
        );
        per_field.insert(
            "beta".to_string(),
            make_per_field_data(vec![10, 20, 30], vec![0, 1, 2]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 3).unwrap();

        let nvm = &result[0].data;
        let nvd = &result[1].data;
        let meta_header_len = index_header_length(META_CODEC, "");

        // First entry: "alpha" (field 0), constant norms
        let entry0 = &nvm[meta_header_len..];
        assert_eq!(&entry0[0..4], &0i32.to_le_bytes()); // field_number = 0
        assert_eq!(entry0[27], 0); // bytes_per_norm = 0 (constant)
        assert_eq!(&entry0[28..36], &5i64.to_le_bytes()); // constant value = 5

        // Second entry: "beta" (field 1), 1-byte norms
        let entry1 = &nvm[meta_header_len + META_ENTRY_SIZE..];
        assert_eq!(&entry1[0..4], &1i32.to_le_bytes()); // field_number = 1
        assert_eq!(entry1[27], 1); // bytes_per_norm = 1

        // "beta" norm values in .nvd
        let data_header_len = index_header_length(DATA_CODEC, "");
        assert_eq!(nvd[data_header_len], 10u8);
        assert_eq!(nvd[data_header_len + 1], 20u8);
        assert_eq!(nvd[data_header_len + 2], 30u8);

        // EOF marker after second entry
        assert_eq!(
            &nvm[meta_header_len + 2 * META_ENTRY_SIZE..meta_header_len + 2 * META_ENTRY_SIZE + 4],
            &(-1i32).to_le_bytes()
        );
    }
}
