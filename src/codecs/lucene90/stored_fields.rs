// SPDX-License-Identifier: Apache-2.0
//! Stored fields writer with LZ4 block compression.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::document::StoredValue;
use crate::encoding::lz4;
use crate::encoding::zigzag;
use crate::index::index_file_names;
use crate::index::indexing_chain::StoredDoc;
use crate::store::{DataOutput, SharedDirectory, VecOutput};
use crate::util::packed::DirectMonotonicWriter;

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

/// Writes stored fields files (.fdt, .fdx, .fdm) for a segment.
/// Returns the names of the files written.
pub fn write(
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

    // Buffer all stored field data
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
    // This mirrors FieldsIndexWriter.finish()
    fdm.write_le_int(num_docs)?;
    fdm.write_le_int(BLOCK_SHIFT as i32)?;
    fdm.write_le_int((total_chunks + 1) as i32)?; // numChunks (value count for both DMWs)

    // docsStartPointer — current position in .fdx where docs data begins
    fdm.write_le_long(fdx.file_pointer() as i64)?;

    // Docs monotonic index (meta → fdm, data → fdx)
    let mut docs_writer = DirectMonotonicWriter::new(BLOCK_SHIFT);
    docs_writer.add(0);
    if total_chunks > 0 {
        docs_writer.add(num_docs as i64);
    }
    docs_writer.finish(&mut *fdm, &mut *fdx)?;

    // docsEndPointer = startPointersStartPointer
    fdm.write_le_long(fdx.file_pointer() as i64)?;

    // File pointers monotonic index (meta → fdm, data → fdx)
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

    // maxPointer (into .fdt)
    fdm.write_le_long(max_pointer)?;

    // Trailing metadata to .fdm
    debug!(
        "stored_fields: num_chunks={num_chunks}, num_dirty_chunks={num_dirty_chunks}, num_dirty_docs={num_dirty_docs}, buffered_bytes={}",
        buffered_docs.len()
    );
    fdm.write_vlong(num_chunks)?;
    fdm.write_vlong(num_dirty_chunks)?;
    fdm.write_vlong(num_dirty_docs)?;

    // Footers for .fdm and .fdt
    codec_util::write_footer(&mut *fdm)?;
    codec_util::write_footer(&mut *fdt)?;

    Ok(vec![fdt_name, fdx_name, fdm_name])
}

// ============================================================
// LZ4 with preset dictionary compression
// ============================================================

/// Compress data using LZ4 with preset dictionary format.
/// Matches Java's LZ4WithPresetDictCompressor.compress().
pub(crate) fn compress_lz4_preset_dict(data: &[u8], out: &mut dyn DataOutput) -> io::Result<()> {
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
    // Reusable hash table across blocks, matching Java's FastCompressionHashTable reuse
    let mut lz4_ht = lz4::FastHashTable::new();

    // Compress dictionary (no dictionary for the dict itself)
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

/// Writes integers using StoredFieldsInts encoding.
/// For 1 value: writes a single VInt.
/// For multiple values: uses StoredFieldsInts.writeInts format.
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
    // Check if all values are equal
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

/// Write 8-bit packed integers in 128-element blocks.
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
    // Remainder
    while k < count {
        out.write_byte(values[k] as u8)?;
        k += 1;
    }
    Ok(())
}

/// Write 16-bit packed integers in 128-element blocks.
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

/// Write 32-bit packed integers in 128-element blocks.
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

/// Writes a long in a variable-length format optimized for timestamps.
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

/// Negative zero float bits, used to avoid compressing -0.0f as a small integer.
const NEGATIVE_ZERO_FLOAT: i32 = f32::to_bits(-0.0_f32) as i32; // 0x80000000
/// Negative zero double bits, used to avoid compressing -0.0 as a small integer.
const NEGATIVE_ZERO_DOUBLE: i64 = f64::to_bits(-0.0_f64) as i64; // 0x8000000000000000

/// Writes a float in a variable-length format. Writes between 1 and 5 bytes.
fn write_zfloat(out: &mut dyn DataOutput, f: f32) -> io::Result<()> {
    let int_val = f as i32;
    let float_bits = f32::to_bits(f) as i32;

    if f == int_val as f32 && (-1..=0x7D).contains(&int_val) && float_bits != NEGATIVE_ZERO_FLOAT {
        // Small integer value [-1..125]: single byte
        out.write_byte((0x80 | (1 + int_val)) as u8)?;
    } else if ((float_bits as u32) >> 31) == 0 {
        // Other positive floats: 4 bytes
        out.write_byte((float_bits >> 24) as u8)?;
        out.write_le_short(((float_bits as u32) >> 8) as i16)?;
        out.write_byte(float_bits as u8)?;
    } else {
        // Other negative floats: 5 bytes
        out.write_byte(0xFF)?;
        out.write_le_int(float_bits)?;
    }
    Ok(())
}

/// Writes a double in a variable-length format. Writes between 1 and 9 bytes.
fn write_zdouble(out: &mut dyn DataOutput, d: f64) -> io::Result<()> {
    let int_val = d as i32;
    let double_bits = f64::to_bits(d) as i64;

    if d == int_val as f64 && (-1..=0x7C).contains(&int_val) && double_bits != NEGATIVE_ZERO_DOUBLE
    {
        // Small integer value [-1..124]: single byte
        out.write_byte((0x80 | (int_val + 1)) as u8)?;
    } else if d == (d as f32) as f64 {
        // d has an accurate float representation: 5 bytes
        out.write_byte(0xFE)?;
        out.write_le_int(f32::to_bits(d as f32) as i32)?;
    } else if ((double_bits as u64) >> 63) == 0 {
        // Other positive doubles: 8 bytes
        out.write_byte((double_bits >> 56) as u8)?;
        out.write_le_int(((double_bits as u64) >> 24) as i32)?;
        out.write_le_short(((double_bits as u64) >> 8) as i16)?;
        out.write_byte(double_bits as u8)?;
    } else {
        // Other negative doubles: 9 bytes
        out.write_byte(0xFF)?;
        out.write_le_long(double_bits)?;
    }
    Ok(())
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{MemoryDirectory, MemoryIndexOutput, SharedDirectory};

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    #[test]
    fn test_write_tlong_zero() {
        let mut buf = Vec::new();
        write_tlong(&mut VecOutput(&mut buf), 0).unwrap();
        // 0 % DAY == 0, so header = DAY_ENCODING | (zigzag(0) & 0x1F) = 0xC0 | 0 = 0xC0
        assert_eq!(buf, [0xC0]);
    }

    #[test]
    fn test_write_tlong_day_precision() {
        let mut buf = Vec::new();
        // 86400000ms = 1 day
        write_tlong(&mut VecOutput(&mut buf), DAY).unwrap();
        // DAY / DAY = 1, zigzag(1) = 2, header = 0xC0 | (2 & 0x1F) = 0xC2
        assert_eq!(buf, [0xC2]);
    }

    #[test]
    fn test_write_tlong_hour_precision() {
        let mut buf = Vec::new();
        // 3600000ms = 1 hour
        write_tlong(&mut VecOutput(&mut buf), HOUR).unwrap();
        // HOUR / HOUR = 1, zigzag(1) = 2, header = 0x80 | (2 & 0x1F) = 0x82
        assert_eq!(buf, [0x82]);
    }

    #[test]
    fn test_write_tlong_second_precision() {
        let mut buf = Vec::new();
        // 5000ms = 5 seconds
        write_tlong(&mut VecOutput(&mut buf), 5000).unwrap();
        // 5000 / 1000 = 5, zigzag(5) = 10, header = 0x40 | (10 & 0x1F) = 0x4A
        assert_eq!(buf, [0x4A]);
    }

    #[test]
    fn test_write_tlong_uncompressed() {
        let mut buf = Vec::new();
        // 123 is not a multiple of 1000
        write_tlong(&mut VecOutput(&mut buf), 123).unwrap();
        // header = 0x00, zigzag(123) = 246, lower 5 bits = 246 & 0x1F = 22 = 0x16
        // upper bits = 246 >> 5 = 7
        // header = 0x00 | 0x16 | 0x20 = 0x36
        assert_eq!(buf[0], 0x36);
        // Followed by VLong(7) = byte 0x07
        assert_eq!(buf[1], 0x07);
    }

    #[test]
    fn test_stored_fields_ints_all_equal() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_stored_fields_ints(&[5, 5, 5], 3, &mut out).unwrap();
        let bytes = out.bytes();
        assert_eq!(bytes[0], 0); // all-equal marker
        assert_eq!(bytes[1], 5); // VInt(5)
    }

    #[test]
    fn test_stored_fields_ints_8bit() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_stored_fields_ints(&[1, 2, 3], 3, &mut out).unwrap();
        let bytes = out.bytes();
        assert_eq!(bytes[0], 8); // 8-bit marker
        // Remainder: 3 individual bytes (< 128 block size)
        assert_eq!(bytes[1], 1);
        assert_eq!(bytes[2], 2);
        assert_eq!(bytes[3], 3);
    }

    #[test]
    fn test_save_ints_single() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        save_ints(&[42], 1, &mut out).unwrap();
        let bytes = out.bytes();
        assert_eq!(bytes[0], 42); // VInt(42)
    }

    #[test]
    fn test_compress_lz4_preset_dict_small() {
        let data = b"hello world from stored fields";
        let mut out = MemoryIndexOutput::new("test".to_string());
        compress_lz4_preset_dict(data, &mut out).unwrap();
        assert_not_empty!(out.bytes());
    }

    #[test]
    fn test_write_stored_fields_basic() {
        let stored_docs = vec![
            StoredDoc {
                fields: vec![(0, StoredValue::String("path/doc1.txt".to_string()))],
            },
            StoredDoc {
                fields: vec![(0, StoredValue::String("path/doc2.txt".to_string()))],
            },
            StoredDoc {
                fields: vec![(0, StoredValue::String("path/doc3.txt".to_string()))],
            },
        ];

        let segment_id = [0u8; 16];
        let dir = test_directory();
        let names = write(&dir, "_0", "", &segment_id, &stored_docs, 3).unwrap();
        assert_eq!(names.len(), 3);

        // Verify filenames
        assert_eq!(names[0], "_0.fdt");
        assert_eq!(names[1], "_0.fdx");
        assert_eq!(names[2], "_0.fdm");

        let fdt_data = dir.lock().unwrap().read_file(&names[0]).unwrap();
        let fdx_data = dir.lock().unwrap().read_file(&names[1]).unwrap();
        let fdm_data = dir.lock().unwrap().read_file(&names[2]).unwrap();

        // All files should have content
        assert_not_empty!(fdt_data);
        assert_not_empty!(fdx_data);
        assert_not_empty!(fdm_data);

        // Verify .fdt header magic
        assert_eq!(&fdt_data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Verify .fdt footer magic
        let footer_start = fdt_data.len() - 16;
        assert_eq!(
            &fdt_data[footer_start..footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );

        // Verify .fdx header magic
        assert_eq!(&fdx_data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Verify .fdm header magic
        assert_eq!(&fdm_data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);
    }

    /// Verifies .fdm layout matches Java's FieldsIndexReader read sequence:
    ///   header, chunkSize(VInt), numDocs(Int), blockShift(Int), numChunks(Int),
    ///   docsStartPointer(Long), [docs DMW meta], docsEndPointer(Long),
    ///   [fp DMW meta], startPointersEndPointer(Long), maxPointer(Long),
    ///   numChunks(VLong), numDirtyChunks(VLong), numDirtyDocs(VLong), footer
    #[test]
    fn test_fdm_layout_matches_fields_index_reader() {
        let stored_docs = vec![
            StoredDoc {
                fields: vec![(0, StoredValue::String("doc1".to_string()))],
            },
            StoredDoc {
                fields: vec![(0, StoredValue::String("doc2".to_string()))],
            },
            StoredDoc {
                fields: vec![(0, StoredValue::String("doc3".to_string()))],
            },
        ];

        let segment_id = [0u8; 16];
        let dir = test_directory();
        let names = write(&dir, "_0", "", &segment_id, &stored_docs, 3).unwrap();
        let fdm = dir.lock().unwrap().read_file(&names[2]).unwrap();
        let fdx = dir.lock().unwrap().read_file(&names[1]).unwrap();

        // Compute header length for the meta codec name
        let hdr_len = codec_util::index_header_length(INDEX_CODEC_NAME_META, "");
        let mut pos = hdr_len;

        // chunkSize (VInt) — read by Lucene90CompressingStoredFieldsReader
        assert_eq!(fdm[pos] & 0x80, 0x80, "chunkSize should be multi-byte VInt");
        // Skip VInt (CHUNK_SIZE = 81920 = 0x14000, encoded as 3-byte VInt)
        let mut vint_bytes = 0;
        while fdm[pos + vint_bytes] & 0x80 != 0 {
            vint_bytes += 1;
        }
        vint_bytes += 1; // include final byte
        pos += vint_bytes;

        // numDocs = 3
        let num_docs = i32::from_le_bytes(fdm[pos..pos + 4].try_into().unwrap());
        assert_eq!(num_docs, 3, "numDocs mismatch");
        pos += 4;

        // blockShift = BLOCK_SHIFT
        let block_shift = i32::from_le_bytes(fdm[pos..pos + 4].try_into().unwrap());
        assert_eq!(block_shift, BLOCK_SHIFT as i32, "blockShift mismatch");
        pos += 4;

        // numChunks = totalChunks + 1 = 2
        let num_chunks = i32::from_le_bytes(fdm[pos..pos + 4].try_into().unwrap());
        assert_eq!(num_chunks, 2, "numChunks should be totalChunks+1=2");
        pos += 4;

        // docsStartPointer — pointer into .fdx after its header
        let docs_start = i64::from_le_bytes(fdm[pos..pos + 8].try_into().unwrap());
        let fdx_hdr_len = codec_util::index_header_length(INDEX_CODEC_NAME_IDX, "") as i64;
        assert_eq!(
            docs_start, fdx_hdr_len,
            "docsStartPointer should point right after .fdx header"
        );
        pos += 8;

        // DirectMonotonicWriter metadata for docs: 1 block × 21 bytes
        // (numChunks=2 values, blockShift=10 → blockSize=1024, 1 block)
        let dmw_block_bytes = 8 + 4 + 8 + 1; // min(Long) + avgInc(Int) + offset(Long) + bpv(Byte)
        pos += dmw_block_bytes;

        // docsEndPointer
        let docs_end = i64::from_le_bytes(fdm[pos..pos + 8].try_into().unwrap());
        assert!(
            docs_end >= docs_start,
            "docsEndPointer ({docs_end}) must be >= docsStartPointer ({docs_start})"
        );
        pos += 8;

        // DirectMonotonicWriter metadata for filePointers: 1 block × 21 bytes
        pos += dmw_block_bytes;

        // startPointersEndPointer
        let sp_end = i64::from_le_bytes(fdm[pos..pos + 8].try_into().unwrap());
        assert!(
            sp_end >= docs_end,
            "startPointersEndPointer ({sp_end}) must be >= docsEndPointer ({docs_end})"
        );
        pos += 8;

        // maxPointer — pointer into .fdt
        let max_pointer = i64::from_le_bytes(fdm[pos..pos + 8].try_into().unwrap());
        assert!(max_pointer > 0, "maxPointer should be > 0");
        pos += 8;

        // numChunks (VLong) = 1
        assert_eq!(fdm[pos], 1, "trailing numChunks VLong should be 1");
        pos += 1;

        // numDirtyChunks (VLong) = 1
        assert_eq!(fdm[pos], 1, "numDirtyChunks VLong should be 1");
        pos += 1;

        // numDirtyDocs (VLong) = 3
        assert_eq!(fdm[pos], 3, "numDirtyDocs VLong should be 3");
        pos += 1;

        // Remaining bytes should be exactly the 16-byte footer
        let remaining = fdm.len() - pos;
        assert_eq!(
            remaining, 16,
            "expected 16-byte footer, got {remaining} bytes"
        );

        // Verify .fdx data regions are consistent with pointers
        let fdx_footer_start = fdx.len() - 16;
        assert!(
            (sp_end as usize) <= fdx_footer_start,
            "startPointersEndPointer should be within .fdx data region"
        );
    }

    #[test]
    fn test_write_stored_fields_with_suffix() {
        let stored_docs = vec![StoredDoc {
            fields: vec![(0, StoredValue::String("hello".to_string()))],
        }];

        let segment_id = [1u8; 16];
        let dir = test_directory();
        let names = write(&dir, "_0", "0", &segment_id, &stored_docs, 1).unwrap();

        // With suffix "0", filenames should include it
        assert_eq!(names[0], "_0_0.fdt");
        assert_eq!(names[1], "_0_0.fdx");
        assert_eq!(names[2], "_0_0.fdm");
    }

    #[test]
    fn test_write_stored_fields_empty() {
        // No stored docs - should still produce valid files
        let segment_id = [0u8; 16];
        let dir = test_directory();
        let names = write(&dir, "_0", "", &segment_id, &[], 0).unwrap();
        assert_eq!(names.len(), 3);

        // Files should have at least headers and footers
        let locked = dir.lock().unwrap();
        for name in &names {
            assert_not_empty!(locked.read_file(name).unwrap());
        }
    }

    // Ported from org.apache.lucene.codecs.lucene90.compressing.TestStoredFieldsInt

    #[test]
    fn test_write_ints_16_block_and_remainder() {
        // 140 values: 1 full block of 128 + 12 remainder, all <= 0xFFFF
        let values: Vec<i32> = (0..140).map(|i| (i % 0xFFFF) + 1).collect();
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_ints_16(&values, 140, &mut out).unwrap();
        // 1 block = 32 longs × 8 bytes = 256 bytes, remainder = 12 × 2 bytes = 24 bytes
        assert_eq!(out.bytes().len(), 256 + 24);
    }

    #[test]
    fn test_write_ints_32_block_and_remainder() {
        // 130 values: 1 full block of 128 + 2 remainder, values > 0xFFFF
        let values: Vec<i32> = (0..130).map(|i| 0x10000 + i).collect();
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_ints_32(&values, 130, &mut out).unwrap();
        // 1 block = 64 longs × 8 bytes = 512 bytes, remainder = 2 × 4 bytes = 8 bytes
        assert_eq!(out.bytes().len(), 512 + 8);
    }

    #[test]
    fn test_stored_fields_ints_16bit() {
        // Values in range 256..65535 → format=16
        let values: Vec<i32> = vec![256, 1000, 65535, 300];
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_stored_fields_ints(&values, 4, &mut out).unwrap();
        assert_eq!(out.bytes()[0], 16); // 16-bit format marker
    }

    #[test]
    fn test_write_tlong_negative() {
        // Negative timestamp → uncompressed path (header_base=0, zigzag encoded)
        let mut buf = Vec::new();
        write_tlong(&mut VecOutput(&mut buf), -1).unwrap();
        // -1 % SECOND != 0, so header_base = 0
        // zigzag(-1) = 1, header = 0 | (1 & 0x1F) = 0x01, upper_bits = 0
        assert_eq!(buf, [0x01]);
    }

    #[test]
    fn test_write_tlong_millisecond_precision() {
        // Value divisible by SECOND but not HOUR → SECOND_ENCODING (0x40)
        let mut buf = Vec::new();
        write_tlong(&mut VecOutput(&mut buf), 60_000).unwrap();
        // 60000 / 1000 = 60, zigzag(60) = 120 = 0x78
        // lower 5 bits: 120 & 0x1F = 0x18, upper_bits = 120 >> 5 = 3
        // header = 0x40 | 0x18 | 0x20 = 0x78, followed by vlong(3) = 0x03
        assert_eq!(buf, [0x78, 0x03]);
    }

    // Ported from org.apache.lucene.codecs.lucene90.compressing.TestStoredFieldsFormat

    #[test]
    fn test_write_zfloat_small_integer() {
        // 0.0 → small integer, single byte: 0x80 | (1 + 0) = 0x81
        let mut buf = Vec::new();
        write_zfloat(&mut VecOutput(&mut buf), 0.0).unwrap();
        assert_eq!(buf, [0x81]);

        // 1.0 → 0x80 | (1 + 1) = 0x82
        buf.clear();
        write_zfloat(&mut VecOutput(&mut buf), 1.0).unwrap();
        assert_eq!(buf, [0x82]);

        // -1.0 → 0x80 | (1 + (-1)) = 0x80
        buf.clear();
        write_zfloat(&mut VecOutput(&mut buf), -1.0).unwrap();
        assert_eq!(buf, [0x80]);

        // 125.0 → 0x80 | (1 + 125) = 0x80 | 126 = 0xFE
        buf.clear();
        write_zfloat(&mut VecOutput(&mut buf), 125.0).unwrap();
        assert_eq!(buf, [0xFE]);
    }

    #[test]
    fn test_write_zfloat_positive() {
        // Positive non-integer: 4 bytes
        let mut buf = Vec::new();
        write_zfloat(&mut VecOutput(&mut buf), 1.5).unwrap();
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn test_write_zfloat_negative() {
        // Negative non-integer: 5 bytes (0xFF marker + 4-byte int)
        let mut buf = Vec::new();
        write_zfloat(&mut VecOutput(&mut buf), -1.5).unwrap();
        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], 0xFF);
    }

    #[test]
    fn test_write_zfloat_negative_zero() {
        // -0.0 is not a small integer; it's negative so 5 bytes
        let mut buf = Vec::new();
        write_zfloat(&mut VecOutput(&mut buf), -0.0).unwrap();
        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], 0xFF);
    }

    #[test]
    fn test_write_zdouble_small_integer() {
        // 0.0 → 0x80 | (0 + 1) = 0x81
        let mut buf = Vec::new();
        write_zdouble(&mut VecOutput(&mut buf), 0.0).unwrap();
        assert_eq!(buf, [0x81]);

        // 124.0 → 0x80 | (124 + 1) = 0xFD
        buf.clear();
        write_zdouble(&mut VecOutput(&mut buf), 124.0).unwrap();
        assert_eq!(buf, [0xFD]);
    }

    #[test]
    fn test_write_zdouble_float_representable() {
        // 1.5 has exact float representation → 0xFE + 4-byte float bits = 5 bytes
        let mut buf = Vec::new();
        write_zdouble(&mut VecOutput(&mut buf), 1.5).unwrap();
        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], 0xFE);
    }

    #[test]
    fn test_write_zdouble_positive() {
        // Positive double not representable as float or small int: 8 bytes
        // Use a value that can't be exactly represented as f32
        let mut buf = Vec::new();
        let val = 1.0000000000000002_f64; // differs from f32 roundtrip
        write_zdouble(&mut VecOutput(&mut buf), val).unwrap();
        assert_eq!(buf.len(), 8);
    }

    #[test]
    fn test_write_zdouble_negative() {
        // Negative double not float-representable: 9 bytes
        let mut buf = Vec::new();
        let val = -1.0000000000000002_f64;
        write_zdouble(&mut VecOutput(&mut buf), val).unwrap();
        assert_eq!(buf.len(), 9);
        assert_eq!(buf[0], 0xFF);
    }

    #[test]
    fn test_write_zdouble_negative_zero() {
        // -0.0 is not a small integer, but is float-representable → 0xFE path (5 bytes)
        let mut buf = Vec::new();
        write_zdouble(&mut VecOutput(&mut buf), -0.0).unwrap();
        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], 0xFE);
    }

    #[test]
    fn test_write_stored_fields_with_all_value_types() {
        let stored_docs = vec![StoredDoc {
            fields: vec![
                (0, StoredValue::String("hello".to_string())),
                (1, StoredValue::Int(42)),
                (2, StoredValue::Long(1000)),
                (3, StoredValue::Float(1.5)),
                (4, StoredValue::Double(9.87)),
                (5, StoredValue::Bytes(vec![1, 2, 3])),
            ],
        }];

        let segment_id = [0u8; 16];
        let dir = test_directory();
        let names = write(&dir, "_0", "", &segment_id, &stored_docs, 1).unwrap();
        assert_eq!(names.len(), 3);
        // All files should have content
        let locked = dir.lock().unwrap();
        for name in &names {
            assert_not_empty!(locked.read_file(name).unwrap());
        }
    }
}
