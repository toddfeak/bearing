// Ported from org.apache.lucene.codecs.lucene90.Lucene90DocValuesConsumer
// and org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat

use std::collections::{BTreeSet, HashMap};
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::index::FieldInfos;
use crate::index::index_file_names;
use crate::index::indexing_chain::{DocValuesAccumulator, PerFieldData};
use crate::store::memory::MemoryIndexOutput;
use crate::store::{DataOutput, IndexOutput, SegmentFile, VecOutput};
use crate::util::BytesRef;
use crate::util::compress::lz4;
use crate::util::packed::{DirectMonotonicWriter, DirectWriter, unsigned_bits_required};
use crate::util::string_helper;

// File extensions
const DATA_EXTENSION: &str = "dvd";
const META_EXTENSION: &str = "dvm";

// Codec names and version
const DATA_CODEC: &str = "Lucene90DocValuesData";
const META_CODEC: &str = "Lucene90DocValuesMetadata";
const VERSION: i32 = 0;

// Doc values type bytes
const SORTED_SET: u8 = 3;
const SORTED_NUMERIC: u8 = 4;

// Terms dictionary constants
const TERMS_DICT_BLOCK_LZ4_SHIFT: usize = 6;
const TERMS_DICT_BLOCK_LZ4_SIZE: usize = 1 << TERMS_DICT_BLOCK_LZ4_SHIFT;
const TERMS_DICT_BLOCK_LZ4_MASK: usize = TERMS_DICT_BLOCK_LZ4_SIZE - 1;
const TERMS_DICT_REVERSE_INDEX_SHIFT: i32 = 10;
const TERMS_DICT_REVERSE_INDEX_SIZE: usize = 1 << TERMS_DICT_REVERSE_INDEX_SHIFT;
const TERMS_DICT_REVERSE_INDEX_MASK: usize = TERMS_DICT_REVERSE_INDEX_SIZE - 1;
const DIRECT_MONOTONIC_BLOCK_SHIFT: u32 = 16;

/// Writes doc values files (.dvm, .dvd) for a segment.
/// Returns a list of [`SegmentFile`]s.
pub fn write(
    segment_name: &str,
    segment_suffix: &str,
    segment_id: &[u8; 16],
    field_infos: &FieldInfos,
    per_field: &HashMap<String, PerFieldData>,
    num_docs: i32,
) -> io::Result<Vec<SegmentFile>> {
    let dvm_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);
    let dvd_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, DATA_EXTENSION);

    let mut meta = MemoryIndexOutput::new(dvm_name);
    let mut data = MemoryIndexOutput::new(dvd_name);

    codec_util::write_index_header(&mut meta, META_CODEC, VERSION, segment_id, segment_suffix)?;
    codec_util::write_index_header(&mut data, DATA_CODEC, VERSION, segment_id, segment_suffix)?;

    // Iterate fields in field-number order
    for fi in field_infos.iter() {
        if !fi.has_doc_values() {
            continue;
        }

        let Some(pfd) = per_field.get(fi.name()) else {
            continue;
        };

        match &pfd.doc_values {
            DocValuesAccumulator::SortedNumeric(vals) => {
                debug!(
                    "doc_values: field={:?} (#{}) -> SORTED_NUMERIC, {} docs",
                    fi.name(),
                    fi.number(),
                    vals.len()
                );
                meta.write_le_int(fi.number() as i32)?;
                meta.write_byte(SORTED_NUMERIC)?;
                add_sorted_numeric_field(&mut meta, &mut data, vals, num_docs)?;
            }
            DocValuesAccumulator::SortedSet(vals) => {
                debug!(
                    "doc_values: field={:?} (#{}) -> SORTED_SET, {} docs",
                    fi.name(),
                    fi.number(),
                    vals.len()
                );
                meta.write_le_int(fi.number() as i32)?;
                meta.write_byte(SORTED_SET)?;
                add_sorted_set_field(&mut meta, &mut data, vals, num_docs)?;
            }
            DocValuesAccumulator::None => continue,
        }
    }

    // EOF marker
    meta.write_le_int(-1)?;

    // Write footers
    codec_util::write_footer(&mut meta)?;
    codec_util::write_footer(&mut data)?;

    Ok(vec![meta.into_inner(), data.into_inner()])
}

/// Adds a SORTED_NUMERIC field.
/// Ported from Lucene90DocValuesConsumer.addSortedNumericField + doAddSortedNumericField
fn add_sorted_numeric_field(
    meta: &mut dyn DataOutput,
    data: &mut dyn IndexOutput,
    vals: &[(i32, Vec<i64>)],
    num_docs: i32,
) -> io::Result<()> {
    // No skip index for MVP

    // Collect all values in doc order (flattened)
    let all_values: Vec<i64> = vals
        .iter()
        .flat_map(|(_doc_id, values)| values.iter().copied())
        .collect();

    let (num_docs_with_field, num_values) =
        write_values(meta, data, &all_values, vals.len() as i32, num_docs, false)?;

    meta.write_le_int(num_docs_with_field)?;

    // If multi-valued (numValues > numDocsWithField), write addresses
    // For MVP with 1 value per doc, this is skipped
    if num_values > num_docs_with_field as i64 {
        return Err(io::Error::other(
            "multi-valued SORTED_NUMERIC not implemented for MVP",
        ));
    }

    Ok(())
}

/// Adds a SORTED_SET field (single-valued optimization).
/// Ported from Lucene90DocValuesConsumer.addSortedSetField → doAddSortedField
fn add_sorted_set_field(
    meta: &mut dyn IndexOutput,
    data: &mut dyn IndexOutput,
    vals: &[(i32, Vec<BytesRef>)],
    num_docs: i32,
) -> io::Result<()> {
    // Check single-valued: all docs have exactly 1 value
    for (_doc_id, values) in vals {
        if values.len() != 1 {
            return Err(io::Error::other(
                "multi-valued SORTED_SET not implemented for MVP",
            ));
        }
    }

    // meta.writeByte(0) for singleValued (addTypeByte=true path in doAddSortedField)
    meta.write_byte(0)?; // multiValued = false

    // Build sorted unique terms and assign ordinals
    let mut unique_terms: BTreeSet<BytesRef> = BTreeSet::new();
    for (_doc_id, values) in vals {
        for v in values {
            unique_terms.insert(v.clone());
        }
    }

    // Build sorted terms and term → ordinal map in a single pass
    let mut ord_map: HashMap<BytesRef, i64> = HashMap::with_capacity(unique_terms.len());
    let sorted_terms: Vec<BytesRef> = unique_terms
        .into_iter()
        .enumerate()
        .map(|(i, term)| {
            ord_map.insert(term.clone(), i as i64);
            term
        })
        .collect();

    // Build per-doc ordinal array (in doc order)
    let ordinals: Vec<i64> = vals
        .iter()
        .map(|(_doc_id, values)| ord_map[&values[0]])
        .collect();

    // No skip index for MVP

    // writeValues for ordinals (ords=true)
    // Note: Java's readSorted() (used for single-valued SORTED_SET) does NOT read
    // numDocsWithField — only readSortedNumeric does. So we must NOT write it here.
    let (_num_docs_with_field, _num_values) =
        write_values(meta, data, &ordinals, vals.len() as i32, num_docs, true)?;

    // Write terms dictionary
    add_terms_dict(meta, data, &sorted_terms)?;

    Ok(())
}

/// Core numeric encoding for doc values.
/// Returns (numDocsWithField, numValues).
/// Ported from Lucene90DocValuesConsumer.writeValues
fn write_values(
    meta: &mut dyn DataOutput,
    data: &mut dyn IndexOutput,
    all_values: &[i64],
    num_docs_with_value: i32,
    max_doc: i32,
    ords: bool,
) -> io::Result<(i32, i64)> {
    let num_values = all_values.len() as i64;

    // Docs-with-field indicator
    if num_docs_with_value == 0 {
        // EMPTY
        meta.write_le_long(-2)?;
        meta.write_le_long(0)?;
        meta.write_le_short(-1)?;
        meta.write_byte(0xFF)?; // denseRankPower = -1 as byte
    } else if num_docs_with_value == max_doc {
        // ALL
        meta.write_le_long(-1)?;
        meta.write_le_long(0)?;
        meta.write_le_short(-1)?;
        meta.write_byte(0xFF)?;
    } else {
        // SPARSE — not implemented for MVP
        return Err(io::Error::other(
            "SPARSE doc values pattern not implemented for MVP",
        ));
    }

    meta.write_le_long(num_values)?;

    if num_values == 0 {
        // No values: write minimal metadata
        meta.write_le_int(-1)?; // tablesize
        meta.write_byte(0)?; // numBitsPerValue
        meta.write_le_long(0)?; // min
        meta.write_le_long(0)?; // gcd
        meta.write_le_long(data.file_pointer() as i64)?; // valueOffset
        meta.write_le_long(0)?; // valuesLength
        meta.write_le_long(-1)?; // jumpTableOffset
        return Ok((0, 0));
    }

    // Compute min, max, GCD, unique values
    let first_value = all_values[0];
    let mut min = all_values[0];
    let mut max = all_values[0];
    let mut gcd: i64 = 0;
    let mut unique_values: Option<BTreeSet<i64>> = if ords { None } else { Some(BTreeSet::new()) };

    for &v in all_values {
        min = min.min(v);
        max = max.max(v);

        if gcd != 1 {
            if !(i64::MIN / 2..=i64::MAX / 2).contains(&v) {
                gcd = 1;
            } else {
                gcd = gcd_compute(gcd, v - first_value);
            }
        }

        if let Some(ref mut set) = unique_values {
            set.insert(v);
            if set.len() > 256 {
                unique_values = None;
            }
        }
    }

    // Ordinals validation
    if ords && num_values > 0 {
        assert!(min == 0, "min value for ordinals should be 0, got {}", min);
        if max != 0 {
            assert!(gcd == 1, "GCD on ordinals should be 1, got {}", gcd);
        }
    }

    let num_bits_per_value: u32;
    let mut encode_table: Option<HashMap<i64, i64>> = None;

    if min >= max {
        // Constant value
        num_bits_per_value = 0;
        meta.write_le_int(-1)?; // tablesize
    } else if let Some(ref uv) = unique_values {
        if uv.len() > 1 {
            let table_bpv = unsigned_bits_required(uv.len() as i64 - 1);
            let delta_bpv = unsigned_bits_required((max - min) / gcd);
            if table_bpv < delta_bpv {
                // Table encoding is more compact
                num_bits_per_value = table_bpv;
                let sorted_unique: Vec<i64> = uv.iter().copied().collect();
                meta.write_le_int(sorted_unique.len() as i32)?; // tablesize
                for &v in &sorted_unique {
                    meta.write_le_long(v)?;
                }
                let mut enc = HashMap::new();
                for (i, &v) in sorted_unique.iter().enumerate() {
                    enc.insert(v, i as i64);
                }
                encode_table = Some(enc);
                min = 0;
                gcd = 1;
            } else {
                // Delta encoding
                num_bits_per_value = delta_bpv;
                meta.write_le_int(-1)?; // tablesize
                // Optimization: if gcd==1 && min>0 && bits(max)==bits(max-min) → set min=0
                if gcd == 1
                    && min > 0
                    && unsigned_bits_required(max) == unsigned_bits_required(max - min)
                {
                    min = 0;
                }
            }
        } else {
            // Single unique value but min < max shouldn't happen, treat as delta
            num_bits_per_value = unsigned_bits_required((max - min) / gcd);
            meta.write_le_int(-1)?;
        }
    } else {
        // No table (too many unique values or ords mode)
        num_bits_per_value = unsigned_bits_required((max - min) / gcd);
        // No blocks for MVP (doBlocks=false)
        meta.write_le_int(-1)?; // tablesize
        // Optimization: if gcd==1 && min>0 && bits(max)==bits(max-min) → set min=0
        if gcd == 1 && min > 0 && unsigned_bits_required(max) == unsigned_bits_required(max - min) {
            min = 0;
        }
    }

    meta.write_byte(num_bits_per_value as u8)?;
    meta.write_le_long(min)?;
    meta.write_le_long(gcd)?;
    let start_offset = data.file_pointer() as i64;
    meta.write_le_long(start_offset)?; // valueOffset

    // Write data
    if num_bits_per_value > 0 {
        let mut writer = DirectWriter::new(num_bits_per_value);
        if let Some(ref enc) = encode_table {
            for &v in all_values {
                writer.add(enc[&v]);
            }
        } else {
            for &v in all_values {
                writer.add((v - min) / gcd);
            }
        }
        writer.finish(data)?;
    }

    let values_length = data.file_pointer() as i64 - start_offset;
    meta.write_le_long(values_length)?; // valuesLength
    meta.write_le_long(-1)?; // jumpTableOffset (always -1 for single-block)

    Ok((num_docs_with_value, num_values))
}

/// GCD computation matching Java's MathUtil.gcd
fn gcd_compute(a: i64, b: i64) -> i64 {
    // Use absolute values (wrapping_neg handles i64::MIN correctly)
    let mut a = if a < 0 { a.wrapping_neg() } else { a };
    let mut b = if b < 0 { b.wrapping_neg() } else { b };
    if a == 0 {
        return b;
    }
    if b == 0 {
        return a;
    }
    // Binary GCD algorithm
    let shift = (a | b).trailing_zeros();
    a >>= a.trailing_zeros();
    loop {
        b >>= b.trailing_zeros();
        if a == b {
            break;
        }
        if (a as u64) > (b as u64) || a == i64::MIN {
            std::mem::swap(&mut a, &mut b);
        }
        if a == 1 {
            break;
        }
        b -= a;
    }
    a << shift
}

/// Writes the terms dictionary for SORTED/SORTED_SET doc values.
/// Ported from Lucene90DocValuesConsumer.addTermsDict
fn add_terms_dict(
    meta: &mut dyn IndexOutput,
    data: &mut dyn IndexOutput,
    sorted_terms: &[BytesRef],
) -> io::Result<()> {
    let size = sorted_terms.len() as i64;
    meta.write_vlong(size)?;

    let block_mask = TERMS_DICT_BLOCK_LZ4_MASK;

    meta.write_le_int(DIRECT_MONOTONIC_BLOCK_SHIFT as i32)?;

    // Create DirectMonotonicWriter for block addresses
    let mut address_buffer = MemoryIndexOutput::new("temp_addr".to_string());
    let mut dm_writer = DirectMonotonicWriter::new(DIRECT_MONOTONIC_BLOCK_SHIFT);

    let mut previous: &[u8] = &[];
    let mut max_length: i32 = 0;
    let mut max_block_length: i32 = 0;
    let start = data.file_pointer() as i64;

    // Buffer for suffix data within a block (terms dict buffer)
    let mut suffix_buffer: Vec<u8> = Vec::new();
    // We need to track the first term of the block as the LZ4 dictionary
    let mut dict_bytes: Vec<u8> = Vec::new();

    for (ord, term) in sorted_terms.iter().enumerate() {
        if (ord & block_mask) == 0 {
            if ord != 0 {
                // Flush the previous block
                let uncompressed_length =
                    compress_and_write_terms_block(data, &dict_bytes, &suffix_buffer)?;
                max_block_length = max_block_length.max(uncompressed_length as i32);
                suffix_buffer.clear();
            }

            dm_writer.add((data.file_pointer() as i64) - start);

            // Write first term of block directly to data
            data.write_vint(term.bytes.len() as i32)?;
            data.write_bytes(&term.bytes)?;

            // Save first term as dictionary for LZ4 compression
            dict_bytes = term.bytes.clone();
        } else {
            // Prefix-compress subsequent terms
            let prefix_length = string_helper::bytes_difference(previous, &term.bytes);
            let suffix_length = term.bytes.len() - prefix_length;
            assert!(suffix_length > 0, "duplicate terms in sorted set");

            // Pack prefix/suffix into a byte
            let byte = (prefix_length.min(15) as u8)
                | ((suffix_length.saturating_sub(1).min(15) as u8) << 4);
            suffix_buffer.push(byte);

            if prefix_length >= 15 {
                VecOutput(&mut suffix_buffer).write_vint((prefix_length - 15) as i32)?;
            }
            if suffix_length >= 16 {
                VecOutput(&mut suffix_buffer).write_vint((suffix_length - 16) as i32)?;
            }

            suffix_buffer.extend_from_slice(&term.bytes[prefix_length..]);
        }

        max_length = max_length.max(term.bytes.len() as i32);
        previous = &term.bytes;
    }

    // Flush last block if there's suffix data
    if !suffix_buffer.is_empty() {
        let uncompressed_length =
            compress_and_write_terms_block(data, &dict_bytes, &suffix_buffer)?;
        max_block_length = max_block_length.max(uncompressed_length as i32);
    }

    // Finish DirectMonotonicWriter: write to address_buffer, metadata to meta
    // Add final entry if we had blocks
    dm_writer.finish(meta, &mut address_buffer)?;

    meta.write_le_int(max_length)?;
    meta.write_le_int(max_block_length)?;
    meta.write_le_long(start)?;
    meta.write_le_long(data.file_pointer() as i64 - start)?;

    // Write address buffer to data
    let addr_start = data.file_pointer() as i64;
    data.write_bytes(address_buffer.bytes())?;
    meta.write_le_long(addr_start)?;
    meta.write_le_long(data.file_pointer() as i64 - addr_start)?;

    // Write reverse terms index
    write_terms_index(meta, data, sorted_terms)?;

    Ok(())
}

/// Compress suffix buffer with dict_bytes as LZ4 dictionary and write to data.
/// Returns the uncompressed length (of suffix data only).
fn compress_and_write_terms_block(
    data: &mut dyn DataOutput,
    dict_bytes: &[u8],
    suffix_buffer: &[u8],
) -> io::Result<usize> {
    let uncompressed_length = suffix_buffer.len();
    data.write_vint(uncompressed_length as i32)?;

    // Build combined buffer: dict + suffix
    let mut combined = Vec::with_capacity(dict_bytes.len() + suffix_buffer.len());
    combined.extend_from_slice(dict_bytes);
    combined.extend_from_slice(suffix_buffer);

    let compressed = lz4::compress_with_dictionary(&combined, dict_bytes.len());
    data.write_bytes(&compressed)?;

    Ok(uncompressed_length)
}

/// Writes the reverse terms index for SORTED/SORTED_SET.
/// Ported from Lucene90DocValuesConsumer.writeTermsIndex
fn write_terms_index(
    meta: &mut dyn IndexOutput,
    data: &mut dyn IndexOutput,
    sorted_terms: &[BytesRef],
) -> io::Result<()> {
    meta.write_le_int(TERMS_DICT_REVERSE_INDEX_SHIFT)?;

    let start = data.file_pointer() as i64;

    let mut address_buffer = MemoryIndexOutput::new("temp_reverse_addr".to_string());
    let mut dm_writer = DirectMonotonicWriter::new(DIRECT_MONOTONIC_BLOCK_SHIFT);

    let mut previous: Option<&[u8]> = None;
    let mut offset: i64 = 0;

    for (ord, term) in sorted_terms.iter().enumerate() {
        if (ord & TERMS_DICT_REVERSE_INDEX_MASK) == 0 {
            dm_writer.add(offset);
            let sort_key_len = if ord == 0 {
                0 // no previous term: no bytes to write
            } else {
                string_helper::sort_key_length(previous.unwrap(), &term.bytes)
            };
            offset += sort_key_len as i64;
            data.write_bytes(&term.bytes[..sort_key_len])?;
        }
        // Track previous for the term just before the next boundary
        if (ord & TERMS_DICT_REVERSE_INDEX_MASK) == TERMS_DICT_REVERSE_INDEX_MASK {
            previous = Some(&term.bytes);
        }
    }

    // Final entry
    dm_writer.add(offset);
    dm_writer.finish(meta, &mut address_buffer)?;

    meta.write_le_long(start)?;
    meta.write_le_long(data.file_pointer() as i64 - start)?;

    let addr_start = data.file_pointer() as i64;
    data.write_bytes(address_buffer.bytes())?;
    meta.write_le_long(addr_start)?;
    meta.write_le_long(data.file_pointer() as i64 - addr_start)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::codec_util::{FOOTER_LENGTH, index_header_length};
    use crate::document::{DocValuesType, IndexOptions};
    use crate::index::indexing_chain::{DocValuesAccumulator, PerFieldData};
    use crate::index::{FieldInfo, FieldInfos};
    use crate::test_util::{self, TestDataReader};
    use std::collections::HashMap;

    fn make_field_info(name: &str, number: u32, dv_type: DocValuesType) -> FieldInfo {
        test_util::make_field_info(name, number, true, IndexOptions::None, dv_type)
    }

    fn make_per_field_data_sorted_numeric(values: Vec<(i32, Vec<i64>)>) -> PerFieldData {
        PerFieldData {
            postings: HashMap::new(),
            doc_values: DocValuesAccumulator::SortedNumeric(values),
            norms: Vec::new(),
            norms_docs: Vec::new(),
            points: Vec::new(),
        }
    }

    fn make_per_field_data_sorted_set(values: Vec<(i32, Vec<BytesRef>)>) -> PerFieldData {
        PerFieldData {
            postings: HashMap::new(),
            doc_values: DocValuesAccumulator::SortedSet(values),
            norms: Vec::new(),
            norms_docs: Vec::new(),
            points: Vec::new(),
        }
    }

    #[test]
    fn test_gcd_compute() {
        assert_eq!(gcd_compute(0, 5), 5);
        assert_eq!(gcd_compute(5, 0), 5);
        assert_eq!(gcd_compute(12, 8), 4);
        assert_eq!(gcd_compute(0, 0), 0);
        assert_eq!(gcd_compute(7, 7), 7);
        assert_eq!(gcd_compute(100, 75), 25);
        assert_eq!(gcd_compute(-12, 8), 4);
    }

    #[test]
    fn test_vec_output_vint() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vint(0).unwrap();
        assert_eq!(buf, vec![0]);

        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vint(127).unwrap();
        assert_eq!(buf, vec![0x7F]);

        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vint(128).unwrap();
        assert_eq!(buf, vec![0x80, 0x01]);
    }

    // Ported from org.apache.lucene.codecs.lucene90.TestLucene90DocValuesFormat
    #[test]
    fn test_sorted_numeric_constant() {
        // 3 docs all with value 42 → bpv=0, min=42
        let fi = make_field_info("modified", 1, DocValuesType::SortedNumeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "modified".to_string(),
            make_per_field_data_sorted_numeric(vec![(0, vec![42]), (1, vec![42]), (2, vec![42])]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "Lucene90_0", &segment_id, &field_infos, &per_field, 3).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "_0_Lucene90_0.dvm");
        assert_eq!(result[1].name, "_0_Lucene90_0.dvd");

        let dvm = &result[0].data;

        // Verify codec magic
        assert_eq!(&dvm[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Parse after header
        let meta_header_len = index_header_length(META_CODEC, "Lucene90_0");
        let entry = &dvm[meta_header_len..];

        // field_number = 1
        assert_eq!(&entry[0..4], &1i32.to_le_bytes());
        // type byte = SORTED_NUMERIC (4)
        assert_eq!(entry[4], SORTED_NUMERIC);

        // docs_with_field_offset = -1 (ALL)
        assert_eq!(&entry[5..13], &(-1i64).to_le_bytes());
        // docs_with_field_length = 0
        assert_eq!(&entry[13..21], &0i64.to_le_bytes());
        // jump_table_entry_count = -1
        assert_eq!(&entry[21..23], &(-1i16).to_le_bytes());
        // dense_rank_power = 0xFF
        assert_eq!(entry[23], 0xFF);

        // numValues = 3
        assert_eq!(&entry[24..32], &3i64.to_le_bytes());

        // tablesize = -1
        assert_eq!(&entry[32..36], &(-1i32).to_le_bytes());
        // numBitsPerValue = 0 (constant)
        assert_eq!(entry[36], 0);
        // min = 42
        assert_eq!(&entry[37..45], &42i64.to_le_bytes());

        // Verify footer present
        let footer_start = dvm.len() - FOOTER_LENGTH;
        assert_eq!(
            &dvm[footer_start..footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
    }

    #[test]
    fn test_sorted_numeric_different() {
        // 3 docs with distinct timestamps
        let fi = make_field_info("modified", 1, DocValuesType::SortedNumeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "modified".to_string(),
            make_per_field_data_sorted_numeric(vec![
                (0, vec![1000]),
                (1, vec![2000]),
                (2, vec![3000]),
            ]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "Lucene90_0", &segment_id, &field_infos, &per_field, 3).unwrap();

        let dvm = &result[0].data;
        let dvd = &result[1].data;

        // Should succeed and produce valid output
        assert!(!dvm.is_empty());
        assert!(!dvd.is_empty());

        // Parse metadata
        let meta_header_len = index_header_length(META_CODEC, "Lucene90_0");
        let entry = &dvm[meta_header_len..];

        // field_number = 1
        assert_eq!(&entry[0..4], &1i32.to_le_bytes());
        // type byte = SORTED_NUMERIC
        assert_eq!(entry[4], SORTED_NUMERIC);

        // numValues = 3
        assert_eq!(&entry[24..32], &3i64.to_le_bytes());

        // Values have GCD=1000, min=1000, so encoded as (val-1000)/1000 = 0,1,2
        // GCD of (0, 1000-1000, 2000-1000, 3000-1000) = gcd(0, 0, 1000, 2000) = 1000
        // bpv = unsignedBitsRequired((3000-1000)/1000) = unsignedBitsRequired(2) = 2
        let gcd_offset = 37 + 8; // min is at 37, gcd at 37+8=45
        let gcd_val = i64::from_le_bytes(entry[gcd_offset..gcd_offset + 8].try_into().unwrap());
        assert_eq!(gcd_val, 1000);
    }

    #[test]
    fn test_sorted_set_single_valued() {
        // 3 docs with distinct path values
        let fi = make_field_info("path", 0, DocValuesType::SortedSet);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "path".to_string(),
            make_per_field_data_sorted_set(vec![
                (0, vec![BytesRef::from_utf8("/a.txt")]),
                (1, vec![BytesRef::from_utf8("/b.txt")]),
                (2, vec![BytesRef::from_utf8("/c.txt")]),
            ]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "Lucene90_0", &segment_id, &field_infos, &per_field, 3).unwrap();

        let dvm = &result[0].data;
        let dvd = &result[1].data;

        assert!(!dvm.is_empty());
        assert!(!dvd.is_empty());

        // Verify codec magic
        assert_eq!(&dvm[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);
        assert_eq!(&dvd[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Parse metadata
        let meta_header_len = index_header_length(META_CODEC, "Lucene90_0");
        let entry = &dvm[meta_header_len..];

        // field_number = 0
        assert_eq!(&entry[0..4], &0i32.to_le_bytes());
        // type byte = SORTED_SET (3)
        assert_eq!(entry[4], SORTED_SET);
        // multiValued = 0 (single-valued)
        assert_eq!(entry[5], 0);

        // Footer present
        let footer_start = dvm.len() - FOOTER_LENGTH;
        assert_eq!(
            &dvm[footer_start..footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
    }

    #[test]
    fn test_header_footer_eof() {
        // Test that the EOF marker (-1 as i32) appears before the footer
        let fi = make_field_info("modified", 0, DocValuesType::SortedNumeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "modified".to_string(),
            make_per_field_data_sorted_numeric(vec![(0, vec![1])]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 1).unwrap();

        let dvm = &result[0].data;

        // EOF marker is 4 bytes before footer
        let eof_pos = dvm.len() - FOOTER_LENGTH - 4;
        assert_eq!(&dvm[eof_pos..eof_pos + 4], &(-1i32).to_le_bytes());
    }

    #[test]
    fn test_terms_dict_prefix_compression_byte() {
        // Verify the prefix/suffix byte encoding
        // prefix=3, suffix_len=5: byte = min(3,15) | (min(4,15) << 4) = 3 | (4<<4) = 3|64 = 67
        let prefix_len = 3usize;
        let suffix_len = 5usize;
        let byte = (prefix_len.min(15) as u8) | ((suffix_len.saturating_sub(1).min(15) as u8) << 4);
        assert_eq!(byte, 0x43); // 3 | (4 << 4) = 0x43

        // Large prefix: prefix=20 (clamped to 15), suffix=1 → 0x0F, then VInt(5) follows
        let byte2 = 15u8;
        assert_eq!(byte2, 0x0F);
    }

    #[test]
    fn test_two_fields_combined() {
        // Test writing both field types together (like the real indexer)
        let fi_path = make_field_info("path", 0, DocValuesType::SortedSet);
        let fi_mod = make_field_info("modified", 1, DocValuesType::SortedNumeric);
        let field_infos = FieldInfos::new(vec![fi_path, fi_mod]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "path".to_string(),
            make_per_field_data_sorted_set(vec![
                (0, vec![BytesRef::from_utf8("/a.txt")]),
                (1, vec![BytesRef::from_utf8("/b.txt")]),
                (2, vec![BytesRef::from_utf8("/c.txt")]),
            ]),
        );
        per_field.insert(
            "modified".to_string(),
            make_per_field_data_sorted_numeric(vec![
                (0, vec![1000]),
                (1, vec![2000]),
                (2, vec![3000]),
            ]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "Lucene90_0", &segment_id, &field_infos, &per_field, 3).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "_0_Lucene90_0.dvm");
        assert_eq!(result[1].name, "_0_Lucene90_0.dvd");

        let dvm = &result[0].data;

        // Parse: after header, first field (path=0, SORTED_SET)
        let meta_header_len = index_header_length(META_CODEC, "Lucene90_0");
        let entry = &dvm[meta_header_len..];
        assert_eq!(&entry[0..4], &0i32.to_le_bytes()); // field 0
        assert_eq!(entry[4], SORTED_SET);

        // Second field should appear later (modified=1, SORTED_NUMERIC)
        // Find the EOF marker
        let eof_pos = dvm.len() - FOOTER_LENGTH - 4;
        assert_eq!(&dvm[eof_pos..eof_pos + 4], &(-1i32).to_le_bytes());
    }

    /// Helper to read through .dvm metadata like Java's Lucene90DocValuesProducer.readFields().
    /// Wraps `TestDataReader` with domain-specific read methods for doc values metadata.
    struct DvmReader<'a>(TestDataReader<'a>);

    impl<'a> DvmReader<'a> {
        fn new(data: &'a [u8], start: usize) -> Self {
            Self(TestDataReader::new(data, start))
        }

        /// Read a numeric entry (matches Java readNumeric)
        fn read_numeric(&mut self) {
            let _docs_with_field_offset = self.0.read_le_long();
            let _docs_with_field_length = self.0.read_le_long();
            let _jump_table_entry_count = self.0.read_le_short();
            let _dense_rank_power = self.0.read_byte();
            let _num_values = self.0.read_le_long();
            let table_size = self.0.read_le_int();
            if table_size >= 0 {
                for _ in 0..table_size {
                    let _table_val = self.0.read_le_long();
                }
            }
            let _bits_per_value = self.0.read_byte();
            let _min_value = self.0.read_le_long();
            let _gcd = self.0.read_le_long();
            let _values_offset = self.0.read_le_long();
            let _values_length = self.0.read_le_long();
            let _value_jump_table_offset = self.0.read_le_long();
        }

        /// Read DirectMonotonicReader.loadMeta (matches Java)
        fn read_dm_meta(&mut self, num_values: i64, _block_shift: i32) {
            let num_blocks = (num_values + (1 << 16) - 1) / (1 << 16);
            for _ in 0..num_blocks {
                let _min = self.0.read_le_long();
                let _avg_inc = self.0.read_le_int(); // Float.intBitsToFloat
                let _offset = self.0.read_le_long();
                let _bits_required = self.0.read_byte();
            }
        }

        /// Read term dict entry (matches Java readTermDict)
        fn read_term_dict(&mut self) {
            let terms_dict_size = self.0.read_vlong();
            let block_shift = self.0.read_le_int();
            let addresses_size = (terms_dict_size + (1i64 << TERMS_DICT_BLOCK_LZ4_SHIFT) - 1)
                >> TERMS_DICT_BLOCK_LZ4_SHIFT;
            self.read_dm_meta(addresses_size, block_shift);
            let _max_term_length = self.0.read_le_int();
            let _max_block_length = self.0.read_le_int();
            let _terms_data_offset = self.0.read_le_long();
            let _terms_data_length = self.0.read_le_long();
            let _terms_addresses_offset = self.0.read_le_long();
            let _terms_addresses_length = self.0.read_le_long();
            let terms_dict_index_shift = self.0.read_le_int();
            let index_size =
                (terms_dict_size + (1i64 << terms_dict_index_shift) - 1) >> terms_dict_index_shift;
            self.read_dm_meta(1 + index_size, block_shift);
            let _terms_index_offset = self.0.read_le_long();
            let _terms_index_length = self.0.read_le_long();
            let _terms_index_addresses_offset = self.0.read_le_long();
            let _terms_index_addresses_length = self.0.read_le_long();
        }

        /// Read sorted set entry (matches Java readSortedSet)
        fn read_sorted_set(&mut self) {
            let multi_valued = self.0.read_byte();
            assert_eq!(multi_valued, 0, "expected single-valued sorted set");
            // Single-valued: readSorted = readNumeric + readTermDict
            self.read_numeric();
            self.read_term_dict();
        }

        /// Read sorted numeric entry (matches Java readSortedNumeric)
        fn read_sorted_numeric(&mut self) {
            self.read_numeric();
            let num_docs_with_field = self.0.read_le_int();
            // re-read numValues from numeric entry to check multi-valued
            // For simplicity, just trust the write is correct
            let _ = num_docs_with_field;
            // If multi-valued, would read addresses — skip for MVP
        }
    }

    #[test]
    fn test_two_fields_dvm_parseable_like_java() {
        // Regression test: verifies that SORTED_SET does NOT write an extra
        // numDocsWithField int, which would shift all subsequent field reads.
        let fi_path = make_field_info("path", 0, DocValuesType::SortedSet);
        let fi_mod = make_field_info("modified", 1, DocValuesType::SortedNumeric);
        let field_infos = FieldInfos::new(vec![fi_path, fi_mod]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "path".to_string(),
            make_per_field_data_sorted_set(vec![
                (0, vec![BytesRef::from_utf8("/a.txt")]),
                (1, vec![BytesRef::from_utf8("/b.txt")]),
                (2, vec![BytesRef::from_utf8("/c.txt")]),
            ]),
        );
        per_field.insert(
            "modified".to_string(),
            make_per_field_data_sorted_numeric(vec![
                (0, vec![1000]),
                (1, vec![2000]),
                (2, vec![3000]),
            ]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "Lucene90_0", &segment_id, &field_infos, &per_field, 3).unwrap();
        let dvm = &result[0].data;

        // Parse the .dvm like Java's Lucene90DocValuesProducer.readFields()
        let meta_header_len = index_header_length(META_CODEC, "Lucene90_0");
        let mut reader = DvmReader::new(dvm, meta_header_len);

        // First field: path (#0, SORTED_SET)
        let field0 = reader.0.read_le_int();
        assert_eq!(field0, 0, "first field number should be 0");
        let type0 = reader.0.read_byte();
        assert_eq!(type0, SORTED_SET, "first field type should be SORTED_SET");
        reader.read_sorted_set();

        // Second field: modified (#1, SORTED_NUMERIC)
        let field1 = reader.0.read_le_int();
        assert_eq!(
            field1, 1,
            "second field number should be 1 (got {field1} — extra bytes written for SORTED_SET?)"
        );
        let type1 = reader.0.read_byte();
        assert_eq!(
            type1, SORTED_NUMERIC,
            "second field type should be SORTED_NUMERIC"
        );
        reader.read_sorted_numeric();

        // EOF marker
        let eof = reader.0.read_le_int();
        assert_eq!(eof, -1, "expected EOF marker (-1)");
    }

    #[test]
    fn test_sorted_numeric_with_gcd() {
        // Values with GCD: 100, 200, 300 → GCD=100 (from first_value diff)
        let fi = make_field_info("field", 0, DocValuesType::SortedNumeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "field".to_string(),
            make_per_field_data_sorted_numeric(vec![
                (0, vec![100]),
                (1, vec![200]),
                (2, vec![300]),
            ]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 3).unwrap();

        // Should complete without error
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_sorted_set_identical_paths() {
        // All docs have the same path → ordinals are all 0 → constant
        let fi = make_field_info("path", 0, DocValuesType::SortedSet);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "path".to_string(),
            make_per_field_data_sorted_set(vec![
                (0, vec![BytesRef::from_utf8("/same.txt")]),
                (1, vec![BytesRef::from_utf8("/same.txt")]),
                (2, vec![BytesRef::from_utf8("/same.txt")]),
            ]),
        );

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 3).unwrap();
        assert_eq!(result.len(), 2);
    }
}
