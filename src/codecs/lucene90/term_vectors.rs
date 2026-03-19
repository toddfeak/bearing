// SPDX-License-Identifier: Apache-2.0
//! Term vectors writer producing `.tvd`, `.tvx`, `.tvm` files.

use std::collections::BTreeSet;
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::index::index_file_names;
use crate::index::indexing_chain::TermVectorDoc;
use crate::store::{DataOutput, SharedDirectory, VecOutput};
use crate::util::compress::lz4;
use crate::util::packed::{
    BlockPackedWriter, DirectMonotonicWriter, DirectWriter, packed_bits_required,
    packed_ints_write, unsigned_bits_required,
};

// File extensions
const VECTORS_EXTENSION: &str = "tvd";
const INDEX_EXTENSION: &str = "tvx";
const META_EXTENSION: &str = "tvm";

// Codec names and version
const DATA_CODEC: &str = "Lucene90TermVectorsData";
const INDEX_CODEC_IDX: &str = "Lucene90TermVectorsIndexIdx";
const INDEX_CODEC_META: &str = "Lucene90TermVectorsIndexMeta";
const VERSION: i32 = 0;

// PackedInts.VERSION_CURRENT
const PACKED_INTS_VERSION: i32 = 2;
const CHUNK_SIZE: i32 = 4096;
const BLOCK_SHIFT: u32 = 10;
const PACKED_BLOCK_SIZE: usize = 64;

// Flag bits for term vector features
const POSITIONS: u8 = 0b001;
const OFFSETS: u8 = 0b010;
const PAYLOADS: u8 = 0b100;

/// FLAGS_BITS = unsigned_bits_required(POSITIONS | OFFSETS | PAYLOADS) = unsigned_bits_required(7) = 4
const FLAGS_BITS: u32 = 4;

/// Writes term vector files (`.tvd`, `.tvx`, `.tvm`) for a segment.
///
/// Returns the names of the files written.
pub fn write(
    directory: &SharedDirectory,
    segment_name: &str,
    segment_suffix: &str,
    segment_id: &[u8; 16],
    term_vector_docs: &[TermVectorDoc],
    num_docs: i32,
) -> io::Result<Vec<String>> {
    let tvd_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, VECTORS_EXTENSION);
    let tvx_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, INDEX_EXTENSION);
    let tvm_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);

    debug!(
        "term_vectors: writing {tvd_name}, {tvx_name}, {tvm_name} for segment={segment_name:?}, num_docs={num_docs}"
    );

    let (mut tvd, mut tvx, mut tvm) = {
        let mut dir = directory.lock().unwrap();
        (
            dir.create_output(&tvd_name)?,
            dir.create_output(&tvx_name)?,
            dir.create_output(&tvm_name)?,
        )
    };

    // Write headers
    codec_util::write_index_header(&mut *tvd, DATA_CODEC, VERSION, segment_id, segment_suffix)?;
    codec_util::write_index_header(
        &mut *tvx,
        INDEX_CODEC_IDX,
        VERSION,
        segment_id,
        segment_suffix,
    )?;
    codec_util::write_index_header(
        &mut *tvm,
        INDEX_CODEC_META,
        VERSION,
        segment_id,
        segment_suffix,
    )?;

    // Write PackedInts version and chunk size to meta
    tvm.write_vint(PACKED_INTS_VERSION)?;
    tvm.write_vint(CHUNK_SIZE)?;

    let chunk_docs = term_vector_docs.len() as i32;
    let mut num_chunks: i64 = 0;
    let mut num_dirty_chunks: i64 = 0;
    let mut num_dirty_docs: i64 = 0;

    // Record chunk start pointer
    let chunk_start_pointer = tvd.file_pointer() as i64;

    if chunk_docs > 0 {
        num_chunks = 1;
        num_dirty_chunks = 1; // force-flushed at end
        num_dirty_docs = chunk_docs as i64;

        // Chunk header: docBase=0, (chunkDocs << 1) | 1 (dirty bit)
        tvd.write_vint(0)?;
        tvd.write_vint((chunk_docs << 1) | 1)?;

        let total_fields = flush_num_fields(term_vector_docs, &mut *tvd)?;

        if total_fields > 0 {
            let field_nums = flush_field_nums(term_vector_docs, &mut *tvd)?;
            flush_fields(term_vector_docs, &field_nums, &mut *tvd)?;
            flush_flags(term_vector_docs, &field_nums, &mut *tvd)?;
            flush_num_terms(term_vector_docs, &mut *tvd)?;

            let term_suffixes = flush_term_lengths(term_vector_docs, &mut *tvd)?;
            flush_term_freqs(term_vector_docs, &mut *tvd)?;
            flush_positions(term_vector_docs, &mut *tvd)?;
            flush_offsets(term_vector_docs, &field_nums, &mut *tvd)?;
            flush_payload_lengths(term_vector_docs, &mut *tvd)?;

            // Compress term suffixes with plain LZ4 (CompressionMode.FAST)
            let compressed = lz4::compress(&term_suffixes);
            tvd.write_bytes(&compressed)?;
        }
    }

    let max_pointer = tvd.file_pointer() as i64;
    let total_chunks = num_chunks as u32;

    // Write FieldsIndex to .tvx and .tvm (mirrors FieldsIndexWriter.finish())
    tvm.write_le_int(num_docs)?;
    tvm.write_le_int(BLOCK_SHIFT as i32)?;
    tvm.write_le_int((total_chunks + 1) as i32)?;

    // docsStartPointer
    tvm.write_le_long(tvx.file_pointer() as i64)?;

    // Docs monotonic index (meta → tvm, data → tvx)
    let mut docs_writer = DirectMonotonicWriter::new(BLOCK_SHIFT);
    docs_writer.add(0);
    if total_chunks > 0 {
        docs_writer.add(num_docs as i64);
    }
    docs_writer.finish(&mut *tvm, &mut *tvx)?;

    // startPointersStartPointer
    tvm.write_le_long(tvx.file_pointer() as i64)?;

    // File pointers monotonic index (meta → tvm, data → tvx)
    let mut fp_writer = DirectMonotonicWriter::new(BLOCK_SHIFT);
    if total_chunks > 0 {
        fp_writer.add(chunk_start_pointer);
    }
    fp_writer.add(max_pointer);
    fp_writer.finish(&mut *tvm, &mut *tvx)?;

    // startPointersEndPointer
    tvm.write_le_long(tvx.file_pointer() as i64)?;

    // .tvx footer
    codec_util::write_footer(&mut *tvx)?;

    // maxPointer (into .tvd)
    tvm.write_le_long(max_pointer)?;

    // Trailing metadata to .tvm
    debug!(
        "term_vectors: num_chunks={num_chunks}, num_dirty_chunks={num_dirty_chunks}, num_dirty_docs={num_dirty_docs}"
    );
    tvm.write_vlong(num_chunks)?;
    tvm.write_vlong(num_dirty_chunks)?;
    tvm.write_vlong(num_dirty_docs)?;

    // Footers for .tvm and .tvd
    codec_util::write_footer(&mut *tvm)?;
    codec_util::write_footer(&mut *tvd)?;

    Ok(vec![tvd_name, tvx_name, tvm_name])
}

/// Writes number of fields per doc. Returns the total field count across all docs.
fn flush_num_fields(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<i32> {
    if docs.len() == 1 {
        let num_fields = docs[0].fields.len() as i32;
        output.write_vint(num_fields)?;
        return Ok(num_fields);
    }

    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    let mut total_fields = 0i32;
    for doc in docs {
        let n = doc.fields.len() as i64;
        writer.add(output, n)?;
        total_fields += n as i32;
    }
    writer.finish(output)?;
    Ok(total_fields)
}

/// Writes unique sorted field numbers. Returns the sorted field number list.
fn flush_field_nums(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<Vec<u32>> {
    let mut field_nums_set = BTreeSet::new();
    for doc in docs {
        for field in &doc.fields {
            field_nums_set.insert(field.field_number);
        }
    }

    let field_nums: Vec<u32> = field_nums_set.into_iter().collect();
    let num_distinct = field_nums.len();
    assert!(num_distinct > 0);

    let max_field_num = field_nums[num_distinct - 1] as i64;
    let bits_required = packed_bits_required(max_field_num);
    let token = ((num_distinct - 1).min(0x07) << 5) as u8 | bits_required as u8;
    output.write_byte(token)?;
    if num_distinct > 0x07 {
        output.write_vint((num_distinct - 1 - 0x07) as i32)?;
    }

    let values: Vec<i64> = field_nums.iter().map(|&n| n as i64).collect();
    packed_ints_write(output, &values, bits_required)?;

    Ok(field_nums)
}

/// Writes field number indices via DirectWriter to scratch buffer.
fn flush_fields(
    docs: &[TermVectorDoc],
    field_nums: &[u32],
    output: &mut dyn DataOutput,
) -> io::Result<()> {
    let bpv = unsigned_bits_required((field_nums.len() - 1) as i64);
    let mut writer = DirectWriter::new(bpv);
    for doc in docs {
        for field in &doc.fields {
            let idx = field_nums
                .binary_search(&field.field_number)
                .expect("field number must be in field_nums");
            writer.add(idx as i64);
        }
    }
    let mut scratch = Vec::new();
    writer.finish(&mut VecOutput(&mut scratch))?;
    output.write_vlong(scratch.len() as i64)?;
    output.write_bytes(&scratch)
}

/// Writes per-field flags (positions/offsets/payloads) via DirectWriter.
fn flush_flags(
    docs: &[TermVectorDoc],
    field_nums: &[u32],
    output: &mut dyn DataOutput,
) -> io::Result<()> {
    // Check if flags are consistent per field number
    let mut field_flags: Vec<i32> = vec![-1; field_nums.len()];
    let mut non_changing = true;

    'outer: for doc in docs {
        for field in &doc.fields {
            let idx = field_nums
                .binary_search(&field.field_number)
                .expect("field number must be in field_nums");
            let flags = field_flags_value(field);
            if field_flags[idx] == -1 {
                field_flags[idx] = flags;
            } else if field_flags[idx] != flags {
                non_changing = false;
                break 'outer;
            }
        }
    }

    if non_changing {
        // One flag per unique field number
        output.write_vint(0)?;
        let mut scratch = Vec::new();
        let mut writer = DirectWriter::new(FLAGS_BITS);
        for &flags in &field_flags {
            assert!(flags >= 0);
            writer.add(flags as i64);
        }
        writer.finish(&mut VecOutput(&mut scratch))?;
        output.write_vint(scratch.len() as i32)?;
        output.write_bytes(&scratch)
    } else {
        // One flag per field instance
        output.write_vint(1)?;
        let mut scratch = Vec::new();
        let mut writer = DirectWriter::new(FLAGS_BITS);
        for doc in docs {
            for field in &doc.fields {
                writer.add(field_flags_value(field) as i64);
            }
        }
        writer.finish(&mut VecOutput(&mut scratch))?;
        output.write_vint(scratch.len() as i32)?;
        output.write_bytes(&scratch)
    }
}

/// Writes number of terms per field via DirectWriter to scratch buffer.
fn flush_num_terms(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<()> {
    let mut max_num_terms: i32 = 0;
    for doc in docs {
        for field in &doc.fields {
            max_num_terms |= field.terms.len() as i32;
        }
    }

    let bpv = unsigned_bits_required(max_num_terms as i64);
    output.write_vint(bpv as i32)?;
    let mut scratch = Vec::new();
    let mut writer = DirectWriter::new(bpv);
    for doc in docs {
        for field in &doc.fields {
            writer.add(field.terms.len() as i64);
        }
    }
    writer.finish(&mut VecOutput(&mut scratch))?;
    output.write_vint(scratch.len() as i32)?;
    output.write_bytes(&scratch)
}

/// Writes prefix and suffix lengths via BlockPackedWriter. Returns the accumulated
/// term suffix bytes for LZ4 compression.
fn flush_term_lengths(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<Vec<u8>> {
    let mut term_suffixes = Vec::new();

    // Compute prefix/suffix lengths and accumulate suffix bytes
    struct TermLengths {
        prefix_len: i32,
        suffix_len: i32,
    }
    let mut all_lengths: Vec<TermLengths> = Vec::new();

    for doc in docs {
        for field in &doc.fields {
            let mut prev_term: &[u8] = &[];
            for term_data in &field.terms {
                let term_bytes = term_data.term.as_bytes();
                let prefix_len = shared_prefix_length(prev_term, term_bytes);
                let suffix_len = term_bytes.len() - prefix_len;
                all_lengths.push(TermLengths {
                    prefix_len: prefix_len as i32,
                    suffix_len: suffix_len as i32,
                });
                term_suffixes.extend_from_slice(&term_bytes[prefix_len..]);
                prev_term = term_bytes;
            }
        }
    }

    // Write prefix lengths
    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    for tl in &all_lengths {
        writer.add(output, tl.prefix_len as i64)?;
    }
    writer.finish(output)?;

    // Write suffix lengths
    writer.reset();
    for tl in &all_lengths {
        writer.add(output, tl.suffix_len as i64)?;
    }
    writer.finish(output)?;

    Ok(term_suffixes)
}

/// Writes (freq - 1) for each term via BlockPackedWriter.
fn flush_term_freqs(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<()> {
    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    for doc in docs {
        for field in &doc.fields {
            for term in &field.terms {
                writer.add(output, (term.freq - 1) as i64)?;
            }
        }
    }
    writer.finish(output)
}

/// Writes position deltas via BlockPackedWriter.
fn flush_positions(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<()> {
    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    for doc in docs {
        for field in &doc.fields {
            if field.has_positions {
                for term in &field.terms {
                    let mut previous_position = 0;
                    for &position in &term.positions {
                        writer.add(output, (position - previous_position) as i64)?;
                        previous_position = position;
                    }
                }
            }
        }
    }
    writer.finish(output)
}

/// Writes offset data: charsPerTerm floats (BE), start offset deltas, and offset lengths.
fn flush_offsets(
    docs: &[TermVectorDoc],
    field_nums: &[u32],
    output: &mut dyn DataOutput,
) -> io::Result<()> {
    let has_offsets = docs
        .iter()
        .any(|doc| doc.fields.iter().any(|f| f.has_offsets));
    if !has_offsets {
        return Ok(());
    }

    // Compute charsPerTerm per unique field number
    let mut sum_pos = vec![0i64; field_nums.len()];
    let mut sum_offsets = vec![0i64; field_nums.len()];

    for doc in docs {
        for field in &doc.fields {
            if field.has_offsets && field.has_positions {
                let idx = field_nums
                    .binary_search(&field.field_number)
                    .expect("field number must be in field_nums");
                for term in &field.terms {
                    let freq = term.freq as usize;
                    if freq > 0 {
                        // Last position for this term
                        sum_pos[idx] += term.positions[freq - 1] as i64;
                        // Last start offset for this term
                        if let Some(ref offsets) = term.offsets {
                            sum_offsets[idx] += offsets.start_offsets[freq - 1] as i64;
                        }
                    }
                }
            }
        }
    }

    let mut chars_per_term = vec![0.0f32; field_nums.len()];
    for i in 0..field_nums.len() {
        chars_per_term[i] = if sum_pos[i] <= 0 || sum_offsets[i] <= 0 {
            0.0
        } else {
            (sum_offsets[i] as f64 / sum_pos[i] as f64) as f32
        };
    }

    // Write charsPerTerm as BE ints
    for &cpt in &chars_per_term {
        output.write_be_int(f32::to_bits(cpt) as i32)?;
    }

    // Start offset deltas
    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    for doc in docs {
        for field in &doc.fields {
            if field.has_offsets {
                let idx = field_nums
                    .binary_search(&field.field_number)
                    .expect("field number must be in field_nums");
                let cpt = chars_per_term[idx];
                for term in &field.terms {
                    let mut previous_pos = 0i32;
                    let mut previous_off = 0i32;
                    if let Some(ref offsets) = term.offsets {
                        for j in 0..term.freq as usize {
                            let position = if field.has_positions {
                                term.positions[j]
                            } else {
                                0
                            };
                            let start_offset = offsets.start_offsets[j];
                            let delta = start_offset
                                - previous_off
                                - (cpt * (position - previous_pos) as f32) as i32;
                            writer.add(output, delta as i64)?;
                            previous_pos = position;
                            previous_off = start_offset;
                        }
                    }
                }
            }
        }
    }
    writer.finish(output)?;

    // Offset lengths: (endOffset - startOffset) - prefixLength - suffixLength
    writer.reset();
    for doc in docs {
        for field in &doc.fields {
            if field.has_offsets {
                let mut prev_term: &[u8] = &[];
                for term in &field.terms {
                    let term_bytes = term.term.as_bytes();
                    let prefix_len = shared_prefix_length(prev_term, term_bytes) as i32;
                    let suffix_len = term_bytes.len() as i32 - prefix_len;

                    if let Some(ref offsets) = term.offsets {
                        for j in 0..term.freq as usize {
                            let length = offsets.end_offsets[j] - offsets.start_offsets[j];
                            writer.add(output, (length - prefix_len - suffix_len) as i64)?;
                        }
                    }
                    prev_term = term_bytes;
                }
            }
        }
    }
    writer.finish(output)
}

/// Writes payload lengths via BlockPackedWriter (all zeros for now).
fn flush_payload_lengths(docs: &[TermVectorDoc], output: &mut dyn DataOutput) -> io::Result<()> {
    let mut writer = BlockPackedWriter::new(PACKED_BLOCK_SIZE);
    for doc in docs {
        for field in &doc.fields {
            if field.has_payloads {
                for term in &field.terms {
                    for _ in 0..term.freq {
                        writer.add(output, 0)?;
                    }
                }
            }
        }
    }
    writer.finish(output)
}

/// Computes the flags byte for a term vector field.
fn field_flags_value(field: &crate::index::indexing_chain::TermVectorField) -> i32 {
    let mut flags = 0i32;
    if field.has_positions {
        flags |= POSITIONS as i32;
    }
    if field.has_offsets {
        flags |= OFFSETS as i32;
    }
    if field.has_payloads {
        flags |= PAYLOADS as i32;
    }
    flags
}

/// Returns the length of the shared prefix between two byte slices.
fn shared_prefix_length(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::indexing_chain::{OffsetBuffers, TermVectorField, TermVectorTerm};
    use crate::store::memory::MemoryDirectory;

    fn make_directory() -> SharedDirectory {
        std::sync::Mutex::new(Box::new(MemoryDirectory::new()))
    }

    fn make_segment_id() -> [u8; 16] {
        [0u8; 16]
    }

    #[test]
    fn test_empty_docs() {
        let dir = make_directory();
        let docs: Vec<TermVectorDoc> = vec![];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 0).unwrap();
        assert_eq!(files.len(), 3);
        assert!(files[0].ends_with(".tvd"));
        assert!(files[1].ends_with(".tvx"));
        assert!(files[2].ends_with(".tvm"));
    }

    #[test]
    fn test_single_doc_no_fields() {
        let dir = make_directory();
        let docs = vec![TermVectorDoc { fields: vec![] }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn test_single_doc_single_field_single_term() {
        let dir = make_directory();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: false,
                has_offsets: false,
                has_payloads: false,
                terms: vec![TermVectorTerm {
                    term: "hello".to_string(),
                    freq: 1,
                    positions: vec![],
                    offsets: None,
                }],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_eq!(files.len(), 3);

        // Verify the .tvd file has valid content (header + chunk + footer)
        let dir_guard = dir.lock().unwrap();
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        // Should have at least a header + some chunk data + footer
        assert!(tvd_len > 40);
    }

    #[test]
    fn test_single_doc_with_positions() {
        let dir = make_directory();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: true,
                has_offsets: false,
                has_payloads: false,
                terms: vec![
                    TermVectorTerm {
                        term: "bar".to_string(),
                        freq: 1,
                        positions: vec![0],
                        offsets: None,
                    },
                    TermVectorTerm {
                        term: "foo".to_string(),
                        freq: 2,
                        positions: vec![1, 3],
                        offsets: None,
                    },
                ],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn test_single_doc_with_offsets() {
        let dir = make_directory();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: true,
                has_offsets: true,
                has_payloads: false,
                terms: vec![TermVectorTerm {
                    term: "hello".to_string(),
                    freq: 2,
                    positions: vec![0, 5],
                    offsets: Some(Box::new(OffsetBuffers {
                        start_offsets: vec![0, 30],
                        end_offsets: vec![5, 35],
                    })),
                }],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn test_multiple_docs_different_fields() {
        let dir = make_directory();
        let docs = vec![
            TermVectorDoc {
                fields: vec![TermVectorField {
                    field_number: 0,
                    has_positions: false,
                    has_offsets: false,
                    has_payloads: false,
                    terms: vec![TermVectorTerm {
                        term: "alpha".to_string(),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    }],
                }],
            },
            TermVectorDoc {
                fields: vec![TermVectorField {
                    field_number: 2,
                    has_positions: false,
                    has_offsets: false,
                    has_payloads: false,
                    terms: vec![TermVectorTerm {
                        term: "beta".to_string(),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    }],
                }],
            },
        ];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 2).unwrap();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn test_term_prefix_compression() {
        // Terms sharing prefixes should produce correct prefix/suffix lengths
        let dir = make_directory();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: false,
                has_offsets: false,
                has_payloads: false,
                terms: vec![
                    TermVectorTerm {
                        term: "abc".to_string(),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    },
                    TermVectorTerm {
                        term: "abd".to_string(),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    },
                    TermVectorTerm {
                        term: "xyz".to_string(),
                        freq: 1,
                        positions: vec![],
                        offsets: None,
                    },
                ],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn test_shared_prefix_length() {
        assert_eq!(shared_prefix_length(b"abc", b"abd"), 2);
        assert_eq!(shared_prefix_length(b"abc", b"abc"), 3);
        assert_eq!(shared_prefix_length(b"abc", b"xyz"), 0);
        assert_eq!(shared_prefix_length(b"", b"abc"), 0);
        assert_eq!(shared_prefix_length(b"abc", b""), 0);
    }

    #[test]
    fn test_field_flags_value() {
        let field = TermVectorField {
            field_number: 0,
            has_positions: true,
            has_offsets: true,
            has_payloads: false,
            terms: vec![],
        };
        assert_eq!(field_flags_value(&field), 0x03); // POSITIONS | OFFSETS

        let field2 = TermVectorField {
            field_number: 0,
            has_positions: false,
            has_offsets: false,
            has_payloads: false,
            terms: vec![],
        };
        assert_eq!(field_flags_value(&field2), 0x00);

        let field3 = TermVectorField {
            field_number: 0,
            has_positions: true,
            has_offsets: true,
            has_payloads: true,
            terms: vec![],
        };
        assert_eq!(field_flags_value(&field3), 0x07);
    }

    #[test]
    fn test_position_delta_encoding() {
        let dir = make_directory();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: true,
                has_offsets: false,
                has_payloads: false,
                terms: vec![
                    TermVectorTerm {
                        term: "a".to_string(),
                        freq: 3,
                        positions: vec![0, 5, 10],
                        offsets: None,
                    },
                    TermVectorTerm {
                        term: "b".to_string(),
                        freq: 2,
                        // Position delta resets per term
                        positions: vec![2, 7],
                        offsets: None,
                    },
                ],
            }],
        }];
        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_eq!(files.len(), 3);
    }

    /// Writes term vectors with positions and offsets matching the
    /// text_field_with_term_vectors configuration. Exercises the LZ4
    /// compression path with enough terms to produce meaningful compressed
    /// output. Uses plain LZ4 (CompressionMode.FAST), not the preset-dict
    /// format used by stored fields.
    #[test]
    fn test_positions_and_offsets_with_many_terms() {
        let dir = make_directory();
        let terms: Vec<TermVectorTerm> = (0..20)
            .map(|i| TermVectorTerm {
                term: format!("term_{i:04}"),
                freq: 1,
                positions: vec![i as i32],
                offsets: Some(Box::new(OffsetBuffers {
                    start_offsets: vec![i as i32 * 10],
                    end_offsets: vec![i as i32 * 10 + 9],
                })),
            })
            .collect();
        let docs = vec![TermVectorDoc {
            fields: vec![TermVectorField {
                field_number: 0,
                has_positions: true,
                has_offsets: true,
                has_payloads: false,
                terms,
            }],
        }];

        let files = write(&dir, "_0", "", &make_segment_id(), &docs, 1).unwrap();
        assert_eq!(files.len(), 3);

        let dir_guard = dir.lock().unwrap();
        let tvd_len = dir_guard.file_length(&files[0]).unwrap();
        assert_gt!(tvd_len, 40, "tvd should have substantial content");
    }
}
