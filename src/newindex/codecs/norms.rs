// SPDX-License-Identifier: Apache-2.0

// DEBT: adapted from codecs::lucene90::norms — reconcile after switchover
// by updating the original to accept newindex types directly.

//! Norms writer (.nvm, .nvd) for the newindex pipeline.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::lucene90::indexed_disi;
use crate::newindex::index_file_names;
use crate::store::{DataOutput, SharedDirectory};

const DATA_EXTENSION: &str = "nvd";
const META_EXTENSION: &str = "nvm";

const DATA_CODEC: &str = "Lucene90NormsData";
const META_CODEC: &str = "Lucene90NormsMetadata";
const VERSION: i32 = 0;

/// Per-field norms data for writing.
#[derive(Debug)]
pub(crate) struct NormsFieldData {
    /// Field name (for debug logging).
    pub field_name: String,
    /// Field number (must be unique, fields sorted by this).
    pub field_number: u32,
    /// Norm values, one per document that has a norm for this field.
    pub norms: Vec<i64>,
    /// Doc IDs corresponding to each norm value (parallel with `norms`).
    pub docs: Vec<i32>,
}

/// Writes norms files (.nvm, .nvd) for a segment.
///
/// `fields` must be sorted by `field_number`.
/// Returns the names of the files written, or an empty vec if no fields have norms.
pub(crate) fn write(
    directory: &SharedDirectory,
    segment_name: &str,
    segment_suffix: &str,
    segment_id: &[u8; 16],
    fields: &[NormsFieldData],
    num_docs: i32,
) -> io::Result<Vec<String>> {
    if fields.is_empty() {
        return Ok(vec![]);
    }

    let nvm_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);
    let nvd_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, DATA_EXTENSION);

    let (mut nvm, mut nvd) = {
        let mut dir = directory.lock().unwrap();
        (dir.create_output(&nvm_name)?, dir.create_output(&nvd_name)?)
    };

    // Write index headers
    codec_util::write_index_header(&mut *nvm, META_CODEC, VERSION, segment_id, segment_suffix)?;
    codec_util::write_index_header(&mut *nvd, DATA_CODEC, VERSION, segment_id, segment_suffix)?;

    for field in fields {
        let num_docs_with_value = field.docs.len() as i32;

        if num_docs_with_value == 0 {
            debug!(
                "norms: field={:?} (#{}) -> EMPTY pattern",
                field.field_name, field.field_number
            );
            write_empty_norms_metadata(&mut *nvm, field.field_number)?;
            continue;
        }

        // Compute min and max norm values
        let min = *field.norms.iter().min().unwrap();
        let max = *field.norms.iter().max().unwrap();
        let bytes_per_norm = num_bytes_per_value(min, max);

        if num_docs_with_value == num_docs {
            // ALL pattern: every document has a norm for this field
            debug!(
                "norms: field={:?} (#{}) -> ALL pattern, bytes_per_norm={}, min={}, max={}, num_docs_with_field={}",
                field.field_name, field.field_number, bytes_per_norm, min, max, num_docs_with_value
            );
            nvm.write_le_int(field.field_number as i32)?;
            nvm.write_le_long(-1)?; // docs_with_field_offset = ALL
            nvm.write_le_long(0)?; // docs_with_field_length
            nvm.write_le_short(-1)?; // jump_table_entry_count
            nvm.write_byte(0xFF)?; // dense_rank_power (-1 as byte)
            nvm.write_le_int(num_docs_with_value)?;

            if bytes_per_norm == 0 {
                // Constant: all norms are the same value, store in metadata
                nvm.write_byte(0)?;
                nvm.write_le_long(min)?;
            } else {
                nvm.write_byte(bytes_per_norm)?;
                let data_offset = nvd.file_pointer() as i64;
                nvm.write_le_long(data_offset)?;
                write_norm_values(&mut *nvd, &field.norms, bytes_per_norm)?;
            }
        } else {
            // SPARSE pattern: some but not all documents have norms
            debug!(
                "norms: field={:?} (#{}) -> SPARSE pattern, bytes_per_norm={}, min={}, max={}, num_docs_with_field={}/{}",
                field.field_name,
                field.field_number,
                bytes_per_norm,
                min,
                max,
                num_docs_with_value,
                num_docs
            );
            nvm.write_le_int(field.field_number as i32)?;

            // Write IndexedDISI bitset to .nvd
            let disi_offset = nvd.file_pointer() as i64;
            nvm.write_le_long(disi_offset)?;
            let jump_table_entry_count =
                indexed_disi::write_bit_set(&field.docs, num_docs, &mut *nvd)?;
            nvm.write_le_long(nvd.file_pointer() as i64 - disi_offset)?;
            nvm.write_le_short(jump_table_entry_count)?;
            nvm.write_byte(indexed_disi::DEFAULT_DENSE_RANK_POWER as u8)?;

            nvm.write_le_int(num_docs_with_value)?;

            if bytes_per_norm == 0 {
                nvm.write_byte(0)?;
                nvm.write_le_long(min)?;
            } else {
                nvm.write_byte(bytes_per_norm)?;
                let data_offset = nvd.file_pointer() as i64;
                nvm.write_le_long(data_offset)?;
                write_norm_values(&mut *nvd, &field.norms, bytes_per_norm)?;
            }
        }
    }

    // EOF marker
    nvm.write_le_int(-1)?;

    // Write footers
    codec_util::write_footer(&mut *nvm)?;
    codec_util::write_footer(&mut *nvd)?;

    Ok(vec![nvm_name, nvd_name])
}

/// Writes metadata for a field with no norms (EMPTY pattern).
fn write_empty_norms_metadata(nvm: &mut dyn DataOutput, field_number: u32) -> io::Result<()> {
    nvm.write_le_int(field_number as i32)?;
    nvm.write_le_long(-2)?; // docs_with_field_offset = EMPTY
    nvm.write_le_long(0)?;
    nvm.write_le_short(-1)?;
    nvm.write_byte(0xFF)?;
    nvm.write_le_int(0)?;
    nvm.write_byte(0)?;
    nvm.write_le_long(0)?;
    Ok(())
}

/// Determines how many bytes are needed per norm value.
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
    use crate::store::{MemoryDirectory, SharedDirectory};
    use crate::test_util::TestDataReader;
    use assertables::*;

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    fn make_field(name: &str, number: u32, norms: Vec<i64>, docs: Vec<i32>) -> NormsFieldData {
        NormsFieldData {
            field_name: name.to_string(),
            field_number: number,
            norms,
            docs,
        }
    }

    /// Size of one metadata entry in bytes:
    /// 4 (field_number) + 8 (offset) + 8 (length) + 2 (jump_table) + 1 (rank_power)
    /// + 4 (num_docs) + 1 (bytes_per_norm) + 8 (norms_offset) = 36
    const META_ENTRY_SIZE: usize = 36;

    #[test]
    fn empty_fields_returns_no_files() {
        let dir = test_directory();
        let names = write(&dir, "_0", "", &[0u8; 16], &[], 3).unwrap();
        assert_is_empty!(&names);
    }

    #[test]
    fn all_pattern_1byte_norms() {
        let dir = test_directory();
        let fields = vec![make_field("contents", 2, vec![12, 8, 10], vec![0, 1, 2])];
        let names = write(&dir, "_0", "", &[0u8; 16], &fields, 3).unwrap();

        assert_len_eq_x!(&names, 2);
        assert_eq!(names[0], "_0.nvm");
        assert_eq!(names[1], "_0.nvd");

        let nvm = dir.lock().unwrap().read_file(&names[0]).unwrap();
        let nvd = dir.lock().unwrap().read_file(&names[1]).unwrap();

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

        // num_docs_with_field = 3
        assert_eq!(&entry[23..27], &3i32.to_le_bytes());

        // bytes_per_norm = 1
        assert_eq!(entry[27], 1);

        // norms_offset points into .nvd data area
        let data_header_len = index_header_length(DATA_CODEC, "");
        let expected_offset = data_header_len as i64;
        assert_eq!(&entry[28..36], &expected_offset.to_le_bytes());

        // EOF marker = -1
        assert_eq!(
            &nvm[meta_header_len + META_ENTRY_SIZE..meta_header_len + META_ENTRY_SIZE + 4],
            &(-1i32).to_le_bytes()
        );

        // Verify .nvd contains 3 norm bytes
        assert_eq!(nvd[data_header_len], 12u8);
        assert_eq!(nvd[data_header_len + 1], 8u8);
        assert_eq!(nvd[data_header_len + 2], 10u8);

        // Verify footer magic on both files
        let nvm_footer_start = nvm.len() - FOOTER_LENGTH;
        assert_eq!(
            &nvm[nvm_footer_start..nvm_footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
        let nvd_footer_start = nvd.len() - FOOTER_LENGTH;
        assert_eq!(
            &nvd[nvd_footer_start..nvd_footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
    }

    #[test]
    fn empty_pattern() {
        let dir = test_directory();
        let fields = vec![make_field("contents", 0, vec![], vec![])];
        let names = write(&dir, "_0", "", &[0u8; 16], &fields, 3).unwrap();

        let nvm = dir.lock().unwrap().read_file(&names[0]).unwrap();
        let meta_header_len = index_header_length(META_CODEC, "");
        let entry = &nvm[meta_header_len..];

        // docs_with_field_offset = -2 (EMPTY)
        assert_eq!(&entry[4..12], &(-2i64).to_le_bytes());

        // num_docs_with_field = 0
        assert_eq!(&entry[23..27], &0i32.to_le_bytes());
    }

    #[test]
    fn constant_norms() {
        let dir = test_directory();
        let fields = vec![make_field("contents", 1, vec![12, 12, 12], vec![0, 1, 2])];
        let names = write(&dir, "_0", "", &[0u8; 16], &fields, 3).unwrap();

        let nvm = dir.lock().unwrap().read_file(&names[0]).unwrap();
        let nvd = dir.lock().unwrap().read_file(&names[1]).unwrap();
        let meta_header_len = index_header_length(META_CODEC, "");
        let entry = &nvm[meta_header_len..];

        // ALL pattern
        assert_eq!(&entry[4..12], &(-1i64).to_le_bytes());

        // bytes_per_norm = 0 (constant)
        assert_eq!(entry[27], 0);

        // norms_offset = constant value = 12
        assert_eq!(&entry[28..36], &12i64.to_le_bytes());

        // .nvd should have only header + footer (no data bytes)
        let data_header_len = index_header_length(DATA_CODEC, "");
        assert_eq!(nvd.len(), data_header_len + FOOTER_LENGTH);
    }

    #[test]
    fn multiple_fields() {
        let dir = test_directory();
        let fields = vec![
            make_field("alpha", 0, vec![5, 5, 5], vec![0, 1, 2]),
            make_field("beta", 1, vec![10, 20, 30], vec![0, 1, 2]),
        ];
        let names = write(&dir, "_0", "", &[0u8; 16], &fields, 3).unwrap();

        let nvm = dir.lock().unwrap().read_file(&names[0]).unwrap();
        let nvd = dir.lock().unwrap().read_file(&names[1]).unwrap();
        let meta_header_len = index_header_length(META_CODEC, "");

        // First entry: "alpha" (field 0), constant norms
        let entry0 = &nvm[meta_header_len..];
        assert_eq!(&entry0[0..4], &0i32.to_le_bytes());
        assert_eq!(entry0[27], 0); // bytes_per_norm = 0 (constant)
        assert_eq!(&entry0[28..36], &5i64.to_le_bytes());

        // Second entry: "beta" (field 1), 1-byte norms
        let entry1 = &nvm[meta_header_len + META_ENTRY_SIZE..];
        assert_eq!(&entry1[0..4], &1i32.to_le_bytes());
        assert_eq!(entry1[27], 1);

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

    #[test]
    fn sparse_norms() {
        let dir = test_directory();
        // 2 docs with norms out of 5 total
        let fields = vec![make_field("contents", 0, vec![12, 8], vec![1, 3])];
        let names = write(&dir, "_0", "", &[0u8; 16], &fields, 5).unwrap();

        let nvm = dir.lock().unwrap().read_file(&names[0]).unwrap();
        let nvd = dir.lock().unwrap().read_file(&names[1]).unwrap();
        let meta_header_len = index_header_length(META_CODEC, "");
        let mut reader = TestDataReader::new(&nvm[meta_header_len..], 0);

        // field_number = 0
        assert_eq!(reader.read_le_int(), 0);

        // SPARSE: offset >= 0
        let docs_with_field_offset = reader.read_le_long();
        assert_ge!(docs_with_field_offset, 0);

        // docsWithFieldLength > 0
        let docs_with_field_length = reader.read_le_long();
        assert_gt!(docs_with_field_length, 0);

        let _jump_table_entry_count = reader.read_le_short();
        assert_eq!(reader.read_byte(), 9); // denseRankPower

        // numDocsWithField = 2
        assert_eq!(reader.read_le_int(), 2);

        // bytesPerNorm = 1
        assert_eq!(reader.read_byte(), 1);

        // normsOffset points after IndexedDISI data
        let norms_offset = reader.read_le_long();
        let disi_end = docs_with_field_offset + docs_with_field_length;
        assert_eq!(norms_offset, disi_end);

        // Verify norm values in .nvd
        assert_eq!(nvd[norms_offset as usize], 12u8);
        assert_eq!(nvd[norms_offset as usize + 1], 8u8);
    }

    #[test]
    fn sparse_constant_norms() {
        let dir = test_directory();
        // 3 docs with identical norms out of 5 total
        let fields = vec![make_field("title", 0, vec![42, 42, 42], vec![0, 2, 4])];
        let names = write(&dir, "_0", "", &[0u8; 16], &fields, 5).unwrap();

        let nvm = dir.lock().unwrap().read_file(&names[0]).unwrap();
        let meta_header_len = index_header_length(META_CODEC, "");
        let mut reader = TestDataReader::new(&nvm[meta_header_len..], 0);

        assert_eq!(reader.read_le_int(), 0); // field_number

        // SPARSE: offset >= 0
        assert_ge!(reader.read_le_long(), 0);

        let _docs_with_field_length = reader.read_le_long();
        let _jump_table_entry_count = reader.read_le_short();
        assert_eq!(reader.read_byte(), 9); // denseRankPower

        assert_eq!(reader.read_le_int(), 3); // numDocsWithField
        assert_eq!(reader.read_byte(), 0); // bytesPerNorm = 0 (constant)
        assert_eq!(reader.read_le_long(), 42); // constant value
    }

    #[test]
    fn num_bytes_per_value_ranges() {
        // Constant
        assert_eq!(num_bytes_per_value(5, 5), 0);

        // 1 byte
        assert_eq!(num_bytes_per_value(-128, 127), 1);

        // 2 bytes
        assert_eq!(num_bytes_per_value(0, 128), 2);
        assert_eq!(num_bytes_per_value(-32768, 32767), 2);

        // 4 bytes
        assert_eq!(num_bytes_per_value(0, 32768), 4);

        // 8 bytes
        assert_eq!(num_bytes_per_value(i32::MIN as i64 - 1, 0), 8);
    }
}
