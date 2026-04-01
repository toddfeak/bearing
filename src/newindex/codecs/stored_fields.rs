// SPDX-License-Identifier: Apache-2.0

// DEBT: adapted from codecs::lucene90::stored_fields — reconcile after
// switchover by updating the original to accept newindex types directly.

//! Stored fields (.fdt, .fdx, .fdm) writer for the newindex pipeline.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::packed_writers::DirectMonotonicWriter;
use crate::document::StoredValue;
use crate::encoding::lz4;
use crate::encoding::zigzag;
use crate::newindex::index_file_names;
use crate::store::{DataOutput, SharedDirectory, VecOutput};

// File extensions
const FIELDS_EXTENSION: &str = "fdt";
const INDEX_EXTENSION: &str = "fdx";
const META_EXTENSION: &str = "fdm";

// Codec names and versions
const FORMAT_NAME: &str = "Lucene90StoredFieldsFastData";
const INDEX_CODEC_NAME_IDX: &str = "Lucene90FieldsIndexIdx";
const INDEX_CODEC_NAME_META: &str = "Lucene90FieldsIndexMeta";
const FDT_VERSION: i32 = 1;
const FDX_VERSION: i32 = 0;
const FDM_VERSION: i32 = 1;

// Compression parameters (BEST_SPEED mode)
const CHUNK_SIZE: i32 = 10 * 8 * 1024; // 81920 bytes
const BLOCK_SHIFT: u32 = 10;
const NUM_SUB_BLOCKS: usize = 10;
const DICT_SIZE_FACTOR: usize = 2;
const LZ4_MAX_DISTANCE: usize = 1 << 16;

// Type codes for stored field values
const TYPE_STRING: u64 = 0x00;
const TYPE_BYTE_ARR: u64 = 0x01;
const TYPE_NUMERIC_INT: u64 = 0x02;
const TYPE_NUMERIC_FLOAT: u64 = 0x03;
const TYPE_NUMERIC_LONG: u64 = 0x04;
const TYPE_NUMERIC_DOUBLE: u64 = 0x05;
const TYPE_BITS: u32 = 3;

// Timestamp compression constants for writeTLong
const SECOND: i64 = 1000;
const HOUR: i64 = 60 * 60 * SECOND;
const DAY: i64 = 24 * HOUR;
const SECOND_ENCODING: u8 = 0x40;
const HOUR_ENCODING: u8 = 0x80;
const DAY_ENCODING: u8 = 0xC0;

/// Per-document stored field data.
// DEBT: parallel to index::indexing_chain::StoredDoc — merge after switchover
#[derive(Clone, Debug, mem_dbg::MemSize)]
pub(crate) struct StoredDoc {
    pub fields: Vec<(u32, StoredValue)>,
}

/// Writes stored fields files (.fdt, .fdx, .fdm) for a segment.
/// Returns the names of the files written.
pub(crate) fn write(
    directory: &SharedDirectory,
    segment_name: &str,
    segment_suffix: &str,
    segment_id: &[u8; 16],
    stored_docs: &[StoredDoc],
    num_docs: i32,
) -> io::Result<Vec<String>> {
    let fdt_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, FIELDS_EXTENSION);
    let fdx_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, INDEX_EXTENSION);
    let fdm_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);

    debug!(
        "stored_fields: writing {fdt_name}, {fdx_name}, {fdm_name} for segment={segment_name:?}, num_docs={num_docs}"
    );

    let (mut fdt, mut fdx, mut fdm) = {
        let mut dir = directory.lock().unwrap();
        (
            dir.create_output(&fdt_name)?,
            dir.create_output(&fdx_name)?,
            dir.create_output(&fdm_name)?,
        )
    };

    // Write headers
    codec_util::write_index_header(
        &mut *fdt,
        FORMAT_NAME,
        FDT_VERSION,
        segment_id,
        segment_suffix,
    )?;
    codec_util::write_index_header(
        &mut *fdx,
        INDEX_CODEC_NAME_IDX,
        FDX_VERSION,
        segment_id,
        segment_suffix,
    )?;
    codec_util::write_index_header(
        &mut *fdm,
        INDEX_CODEC_NAME_META,
        FDM_VERSION,
        segment_id,
        segment_suffix,
    )?;

    // Write chunkSize to meta
    fdm.write_vint(CHUNK_SIZE)?;

    let (buffered_docs, num_stored_fields, end_offsets) = buffer_stored_fields(stored_docs)?;

    let num_buffered_docs = stored_docs.len();
    let mut num_chunks = 0i64;
    let mut num_dirty_chunks = 0i64;
    let mut num_dirty_docs = 0i64;

    // Record chunk start pointer (before writing chunk data)
    let chunk_start_pointer = fdt.file_pointer() as i64;

    // Only flush a chunk if there are buffered docs
    if num_buffered_docs > 0 {
        num_chunks = 1;
        num_dirty_chunks = 1; // force-flushed at end
        num_dirty_docs = num_buffered_docs as i64;

        // Write chunk header to .fdt
        let sliced = buffered_docs.len() >= 2 * CHUNK_SIZE as usize;
        let data = &buffered_docs;
        let sliced_bit: i32 = if sliced { 1 } else { 0 };
        let dirty_bit: i32 = 2; // force-flushed at end

        fdt.write_vint(0)?; // docBase = 0
        fdt.write_vint(((num_buffered_docs as i32) << 2) | dirty_bit | sliced_bit)?;

        // Write numStoredFields
        save_ints(&num_stored_fields, num_buffered_docs, &mut *fdt)?;

        // Transform end_offsets to lengths
        let mut lengths = Vec::with_capacity(num_buffered_docs);
        for i in 0..num_buffered_docs {
            if i == 0 {
                lengths.push(end_offsets[0]);
            } else {
                lengths.push(end_offsets[i] - end_offsets[i - 1]);
            }
        }

        // Write lengths
        save_ints(&lengths, num_buffered_docs, &mut *fdt)?;

        // Compress and write stored field data using LZ4 with preset dict
        if !sliced {
            compress_lz4_preset_dict(data, &mut *fdt)?;
        } else {
            let mut offset = 0;
            while offset < data.len() {
                let l = std::cmp::min(CHUNK_SIZE as usize, data.len() - offset);
                compress_lz4_preset_dict(&data[offset..offset + l], &mut *fdt)?;
                offset += CHUNK_SIZE as usize;
            }
        }
    }

    let max_pointer = fdt.file_pointer() as i64;
    let total_chunks = num_chunks as u32;

    // Write fields index to .fdx and .fdm
    fdm.write_le_int(num_docs)?;
    fdm.write_le_int(BLOCK_SHIFT as i32)?;
    fdm.write_le_int((total_chunks + 1) as i32)?;

    // docsStartPointer
    fdm.write_le_long(fdx.file_pointer() as i64)?;

    // Docs monotonic index
    let mut docs_writer = DirectMonotonicWriter::new(BLOCK_SHIFT);
    docs_writer.add(0);
    if total_chunks > 0 {
        docs_writer.add(num_docs as i64);
    }
    docs_writer.finish(&mut *fdm, &mut *fdx)?;

    // docsEndPointer = startPointersStartPointer
    fdm.write_le_long(fdx.file_pointer() as i64)?;

    // File pointers monotonic index
    let mut fp_writer = DirectMonotonicWriter::new(BLOCK_SHIFT);
    if total_chunks > 0 {
        fp_writer.add(chunk_start_pointer);
    }
    fp_writer.add(max_pointer);
    fp_writer.finish(&mut *fdm, &mut *fdx)?;

    // startPointersEndPointer
    fdm.write_le_long(fdx.file_pointer() as i64)?;

    // .fdx footer
    codec_util::write_footer(&mut *fdx)?;

    // maxPointer
    fdm.write_le_long(max_pointer)?;

    // Trailing metadata
    debug!(
        "stored_fields: num_chunks={num_chunks}, num_dirty_chunks={num_dirty_chunks}, num_dirty_docs={num_dirty_docs}, buffered_bytes={}",
        buffered_docs.len()
    );
    fdm.write_vlong(num_chunks)?;
    fdm.write_vlong(num_dirty_chunks)?;
    fdm.write_vlong(num_dirty_docs)?;

    // Footers
    codec_util::write_footer(&mut *fdm)?;
    codec_util::write_footer(&mut *fdt)?;

    Ok(vec![fdt_name, fdx_name, fdm_name])
}

/// Serializes stored field values into a byte buffer with per-doc field counts and offsets.
fn buffer_stored_fields(stored_docs: &[StoredDoc]) -> io::Result<(Vec<u8>, Vec<i32>, Vec<i32>)> {
    let mut buffered_docs: Vec<u8> = Vec::new();
    let mut num_stored_fields: Vec<i32> = Vec::with_capacity(stored_docs.len());
    let mut end_offsets: Vec<i32> = Vec::with_capacity(stored_docs.len());

    for doc in stored_docs {
        let mut field_count = 0i32;
        for &(field_number, ref value) in &doc.fields {
            field_count += 1;
            let type_code = match value {
                StoredValue::String(_) => TYPE_STRING,
                StoredValue::Int(_) => TYPE_NUMERIC_INT,
                StoredValue::Long(_) => TYPE_NUMERIC_LONG,
                StoredValue::Float(_) => TYPE_NUMERIC_FLOAT,
                StoredValue::Double(_) => TYPE_NUMERIC_DOUBLE,
                StoredValue::Bytes(_) => TYPE_BYTE_ARR,
            };
            let info_and_bits = ((field_number as u64) << TYPE_BITS) | type_code;
            VecOutput(&mut buffered_docs).write_vlong(info_and_bits as i64)?;

            match value {
                StoredValue::String(s) => {
                    VecOutput(&mut buffered_docs).write_string(s)?;
                }
                StoredValue::Int(i) => {
                    VecOutput(&mut buffered_docs).write_zint(*i)?;
                }
                StoredValue::Long(l) => {
                    write_tlong(&mut VecOutput(&mut buffered_docs), *l)?;
                }
                StoredValue::Float(f) => {
                    write_zfloat(&mut VecOutput(&mut buffered_docs), *f)?;
                }
                StoredValue::Double(d) => {
                    write_zdouble(&mut VecOutput(&mut buffered_docs), *d)?;
                }
                StoredValue::Bytes(b) => {
                    VecOutput(&mut buffered_docs).write_vint(b.len() as i32)?;
                    VecOutput(&mut buffered_docs).write_bytes(b)?;
                }
            }
        }
        num_stored_fields.push(field_count);
        end_offsets.push(buffered_docs.len() as i32);
    }

    Ok((buffered_docs, num_stored_fields, end_offsets))
}

// ============================================================
// LZ4 with preset dictionary compression
// ============================================================

fn compress_lz4_preset_dict(data: &[u8], out: &mut dyn DataOutput) -> io::Result<()> {
    let len = data.len();
    let dict_length = std::cmp::min(LZ4_MAX_DISTANCE, len / (NUM_SUB_BLOCKS * DICT_SIZE_FACTOR));
    let block_length = if len <= dict_length {
        0
    } else {
        (len - dict_length).div_ceil(NUM_SUB_BLOCKS)
    };

    out.write_vint(dict_length as i32)?;
    out.write_vint(block_length as i32)?;

    let mut compressed_parts: Vec<Vec<u8>> = Vec::new();
    let mut lz4_ht = lz4::FastHashTable::new();

    // Compress dictionary
    let dict_compressed = lz4::compress_reuse(&data[..dict_length], &mut lz4_ht);
    out.write_vint(dict_compressed.len() as i32)?;
    compressed_parts.push(dict_compressed);

    // Compress sub-blocks with dictionary
    if block_length > 0 {
        let mut buffer = Vec::with_capacity(dict_length + block_length);
        buffer.extend_from_slice(&data[..dict_length]);

        let mut start = dict_length;
        while start < len {
            let l = std::cmp::min(block_length, len - start);
            buffer.truncate(dict_length);
            buffer.extend_from_slice(&data[start..start + l]);

            let block_compressed =
                lz4::compress_with_dictionary_reuse(&buffer, dict_length, &mut lz4_ht);
            out.write_vint(block_compressed.len() as i32)?;
            compressed_parts.push(block_compressed);

            start += block_length;
        }
    }

    // Write all compressed data after the lengths
    for part in &compressed_parts {
        out.write_bytes(part)?;
    }

    Ok(())
}

// ============================================================
// StoredFieldsInts encoding
// ============================================================

fn save_ints(values: &[i32], count: usize, out: &mut dyn DataOutput) -> io::Result<()> {
    if count == 1 {
        out.write_vint(values[0])?;
    } else {
        write_stored_fields_ints(values, count, out)?;
    }
    Ok(())
}

fn write_stored_fields_ints(
    values: &[i32],
    count: usize,
    out: &mut dyn DataOutput,
) -> io::Result<()> {
    let all_equal = values[1..count].iter().all(|&v| v == values[0]);

    if all_equal {
        out.write_byte(0)?;
        out.write_vint(values[0])?;
    } else {
        let max = values[..count].iter().map(|&v| v as u32).max().unwrap_or(0);

        if max <= 0xFF {
            out.write_byte(8)?;
            write_ints_8(values, count, out)?;
        } else if max <= 0xFFFF {
            out.write_byte(16)?;
            write_ints_16(values, count, out)?;
        } else {
            out.write_byte(32)?;
            write_ints_32(values, count, out)?;
        }
    }
    Ok(())
}

fn write_ints_8(values: &[i32], count: usize, out: &mut dyn DataOutput) -> io::Result<()> {
    let block_size = 128;
    let mut k = 0;
    while k + block_size <= count {
        for i in 0..16 {
            let l = ((values[k + i] as u64) << 56)
                | ((values[k + 16 + i] as u64) << 48)
                | ((values[k + 32 + i] as u64) << 40)
                | ((values[k + 48 + i] as u64) << 32)
                | ((values[k + 64 + i] as u64) << 24)
                | ((values[k + 80 + i] as u64) << 16)
                | ((values[k + 96 + i] as u64) << 8)
                | (values[k + 112 + i] as u64);
            out.write_le_long(l as i64)?;
        }
        k += block_size;
    }
    while k < count {
        out.write_byte(values[k] as u8)?;
        k += 1;
    }
    Ok(())
}

fn write_ints_16(values: &[i32], count: usize, out: &mut dyn DataOutput) -> io::Result<()> {
    let block_size = 128;
    let mut k = 0;
    while k + block_size <= count {
        for i in 0..32 {
            let l = ((values[k + i] as u64) << 48)
                | ((values[k + 32 + i] as u64) << 32)
                | ((values[k + 64 + i] as u64) << 16)
                | (values[k + 96 + i] as u64);
            out.write_le_long(l as i64)?;
        }
        k += block_size;
    }
    while k < count {
        out.write_le_short(values[k] as i16)?;
        k += 1;
    }
    Ok(())
}

fn write_ints_32(values: &[i32], count: usize, out: &mut dyn DataOutput) -> io::Result<()> {
    let block_size = 128;
    let mut k = 0;
    while k + block_size <= count {
        for i in 0..64 {
            let l = ((values[k + i] as u64) << 32) | (values[k + 64 + i] as u64);
            out.write_le_long(l as i64)?;
        }
        k += block_size;
    }
    while k < count {
        out.write_le_int(values[k])?;
        k += 1;
    }
    Ok(())
}

// ============================================================
// Timestamp-aware long encoding (writeTLong)
// ============================================================

fn write_tlong(out: &mut dyn DataOutput, l: i64) -> io::Result<()> {
    let mut val = l;
    let header_base: u8;

    if val % SECOND != 0 {
        header_base = 0;
    } else if val % DAY == 0 {
        header_base = DAY_ENCODING;
        val /= DAY;
    } else if val % HOUR == 0 {
        header_base = HOUR_ENCODING;
        val /= HOUR;
    } else {
        header_base = SECOND_ENCODING;
        val /= SECOND;
    }

    let zig_zag = zigzag::encode_i64(val);
    let mut header = header_base | ((zig_zag as u8) & 0x1F);
    let upper_bits = ((zig_zag as u64) >> 5) as i64;

    if upper_bits != 0 {
        header |= 0x20;
    }
    out.write_byte(header)?;
    if upper_bits != 0 {
        out.write_vlong(upper_bits)?;
    }

    Ok(())
}

// ============================================================
// Float/double compression (writeZFloat / writeZDouble)
// ============================================================

const NEGATIVE_ZERO_FLOAT: i32 = f32::to_bits(-0.0_f32) as i32;
const NEGATIVE_ZERO_DOUBLE: i64 = f64::to_bits(-0.0_f64) as i64;

fn write_zfloat(out: &mut dyn DataOutput, f: f32) -> io::Result<()> {
    let int_val = f as i32;
    let float_bits = f32::to_bits(f) as i32;

    if f == int_val as f32 && (-1..=0x7D).contains(&int_val) && float_bits != NEGATIVE_ZERO_FLOAT {
        out.write_byte((0x80 | (1 + int_val)) as u8)?;
    } else if ((float_bits as u32) >> 31) == 0 {
        out.write_byte((float_bits >> 24) as u8)?;
        out.write_le_short(((float_bits as u32) >> 8) as i16)?;
        out.write_byte(float_bits as u8)?;
    } else {
        out.write_byte(0xFF)?;
        out.write_le_int(float_bits)?;
    }
    Ok(())
}

fn write_zdouble(out: &mut dyn DataOutput, d: f64) -> io::Result<()> {
    let int_val = d as i32;
    let double_bits = f64::to_bits(d) as i64;

    if d == int_val as f64 && (-1..=0x7C).contains(&int_val) && double_bits != NEGATIVE_ZERO_DOUBLE
    {
        out.write_byte((0x80 | (int_val + 1)) as u8)?;
    } else if d == (d as f32) as f64 {
        out.write_byte(0xFE)?;
        out.write_le_int(f32::to_bits(d as f32) as i32)?;
    } else if ((double_bits as u64) >> 63) == 0 {
        out.write_byte((double_bits >> 56) as u8)?;
        out.write_le_int(((double_bits as u64) >> 24) as i32)?;
        out.write_le_short(((double_bits as u64) >> 8) as i16)?;
        out.write_byte(double_bits as u8)?;
    } else {
        out.write_byte(0xFF)?;
        out.write_le_long(double_bits)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{MemoryDirectory, SharedDirectory};

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    #[test]
    fn write_produces_three_files() {
        let stored_docs = vec![
            StoredDoc {
                fields: vec![(0, StoredValue::String("doc1".to_string()))],
            },
            StoredDoc {
                fields: vec![(0, StoredValue::String("doc2".to_string()))],
            },
        ];

        let dir = test_directory();
        let names = write(&dir, "_0", "", &[0u8; 16], &stored_docs, 2).unwrap();
        assert_eq!(names.len(), 3);
        assert_eq!(names[0], "_0.fdt");
        assert_eq!(names[1], "_0.fdx");
        assert_eq!(names[2], "_0.fdm");
    }

    #[test]
    fn write_verifies_header_and_footer() {
        let stored_docs = vec![
            StoredDoc {
                fields: vec![
                    (0, StoredValue::String("title one".to_string())),
                    (1, StoredValue::String("body one".to_string())),
                ],
            },
            StoredDoc {
                fields: vec![(0, StoredValue::String("title two".to_string()))],
            },
        ];

        let dir = test_directory();
        write(&dir, "_0", "", &[0u8; 16], &stored_docs, 2).unwrap();

        let guard = dir.lock().unwrap();
        for name in &["_0.fdt", "_0.fdx", "_0.fdm"] {
            let data = guard.read_file(name).unwrap();

            // Header magic
            assert_eq!(&data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

            // Footer magic
            let footer_start = data.len() - 16;
            assert_eq!(
                &data[footer_start..footer_start + 4],
                &[0xc0, 0x28, 0x93, 0xe8]
            );
        }
    }

    #[test]
    fn write_empty_produces_valid_files() {
        let dir = test_directory();
        let names = write(&dir, "_0", "", &[0u8; 16], &[], 0).unwrap();
        assert_eq!(names.len(), 3);

        let locked = dir.lock().unwrap();
        for name in &names {
            let data = locked.read_file(name).unwrap();
            assert!(!data.is_empty());
        }
    }
}
