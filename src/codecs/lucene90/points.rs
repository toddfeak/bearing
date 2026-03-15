// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::index::FieldInfos;
use crate::index::index_file_names;
use crate::index::indexing_chain::PerFieldData;
use crate::store::memory::MemoryIndexOutput;
use crate::store::{DataOutput, IndexOutput, SegmentFile, VecOutput};

// File extensions
const DATA_EXTENSION: &str = "kdd";
const INDEX_EXTENSION: &str = "kdi";
const META_EXTENSION: &str = "kdm";

// Codec names and version for the outer Lucene90PointsFormat files
const DATA_CODEC: &str = "Lucene90PointsFormatData";
const INDEX_CODEC: &str = "Lucene90PointsFormatIndex";
const META_CODEC: &str = "Lucene90PointsFormatMeta";
const FORMAT_VERSION: i32 = 1; // VERSION_BKD_VECTORIZED_BPV24

// BKD inner codec name and version (simple header, not index header)
const BKD_CODEC: &str = "BKD";
const BKD_VERSION: i32 = 10; // VERSION_VECTORIZE_BPV24_AND_INTRODUCE_BPV21

// BKD configuration
const MAX_POINTS_IN_LEAF: i32 = 512; // BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE

// DocIdsWriter markers
const CONTINUOUS_IDS: u8 = 0xFE; // -2 as byte
const BITSET_IDS: u8 = 0xFF; // -1 as byte
const DELTA_BPV_16: u8 = 16;
const BPV_32: u8 = 32;

/// Writes points files (.kdd, .kdi, .kdm) for a segment.
/// Returns a list of [`SegmentFile`]s.
///
pub fn write(
    segment_name: &str,
    segment_suffix: &str,
    segment_id: &[u8; 16],
    field_infos: &FieldInfos,
    per_field: &HashMap<String, PerFieldData>,
    _num_docs: i32,
) -> io::Result<Vec<SegmentFile>> {
    let kdd_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, DATA_EXTENSION);
    let kdi_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, INDEX_EXTENSION);
    let kdm_name =
        index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);

    let mut data = MemoryIndexOutput::new(kdd_name);
    let mut index = MemoryIndexOutput::new(kdi_name);
    let mut meta = MemoryIndexOutput::new(kdm_name);

    // Write index headers to all 3 files
    codec_util::write_index_header(
        &mut data,
        DATA_CODEC,
        FORMAT_VERSION,
        segment_id,
        segment_suffix,
    )?;
    codec_util::write_index_header(
        &mut index,
        INDEX_CODEC,
        FORMAT_VERSION,
        segment_id,
        segment_suffix,
    )?;
    codec_util::write_index_header(
        &mut meta,
        META_CODEC,
        FORMAT_VERSION,
        segment_id,
        segment_suffix,
    )?;

    // Iterate fields sorted by field number (FieldInfos.iter() is in insertion order,
    // which is by field number)
    for fi in field_infos.iter() {
        if !fi.has_point_values() {
            continue;
        }

        let points = match per_field.get(fi.name()) {
            Some(pfd) if !pfd.points.is_empty() => &pfd.points,
            _ => continue,
        };

        debug!(
            "points: field={:?} (#{}) num_points={}",
            fi.name(),
            fi.number(),
            points.len()
        );

        // Write field number to meta
        meta.write_le_int(fi.number() as i32)?;

        let pc = fi.point_config();
        write_bkd_field(
            &mut data,
            &mut index,
            &mut meta,
            points,
            pc.dimension_count,
            pc.index_dimension_count,
            pc.num_bytes,
        )?;
    }

    // Sentinel: no more fields
    meta.write_le_int(-1)?;

    // Write footers for index and data first (order matters for meta pointers)
    codec_util::write_footer(&mut index)?;
    codec_util::write_footer(&mut data)?;

    // Write total file sizes to meta
    meta.write_le_long(index.file_pointer() as i64)?;
    meta.write_le_long(data.file_pointer() as i64)?;

    // Write footer for meta
    codec_util::write_footer(&mut meta)?;

    Ok(vec![
        data.into_inner(),
        index.into_inner(),
        meta.into_inner(),
    ])
}

/// Writes BKD tree data for a single field.
///
/// Supports both single-leaf (<=512 points) and multi-leaf BKD trees.
/// For 1D data, points are pre-sorted and sliced sequentially into leaves.
///
fn write_bkd_field(
    data: &mut dyn IndexOutput,
    index: &mut dyn IndexOutput,
    meta: &mut dyn DataOutput,
    points: &[(i32, Vec<u8>)],
    num_dims: u32,
    num_index_dims: u32,
    bytes_per_dim: u32,
) -> io::Result<()> {
    let count = points.len();
    let bpd = bytes_per_dim as usize;

    // Sort points by value bytes (ascending), stable sort to preserve doc order for ties
    let mut sorted: Vec<(i32, Vec<u8>)> = points.to_vec();
    sorted.sort_by(|a, b| a.1.cmp(&b.1));

    let min_packed = sorted[0].1.clone();
    let max_packed = sorted[count - 1].1.clone();

    // Record data start position
    let data_start_fp = data.file_pointer();

    let num_leaves = count.div_ceil(MAX_POINTS_IN_LEAF as usize);

    let packed_index = if num_leaves == 1 {
        // Single leaf: write directly
        let common_prefix_len = common_prefix_length(&min_packed, &max_packed);
        let leaf_cardinality = compute_cardinality(&sorted);
        let doc_ids: Vec<i32> = sorted.iter().map(|(id, _)| *id).collect();

        write_leaf_block_docs(data, &doc_ids)?;
        write_common_prefixes(data, &sorted[0].1, common_prefix_len, bytes_per_dim)?;
        write_leaf_block_packed_values(
            data,
            &sorted,
            common_prefix_len,
            bytes_per_dim,
            leaf_cardinality,
        )?;

        // Packed index: VLong(data_start_fp) — absolute .kdd offset
        let mut pi = Vec::new();
        // Infallible: writing to a Vec cannot fail.
        VecOutput(&mut pi)
            .write_vlong(data_start_fp as i64)
            .unwrap();
        pi
    } else {
        // Multi-leaf: build leaves and pack index
        let (leaf_block_fps, split_packed_values) =
            build_leaves(data, &sorted, bytes_per_dim, num_leaves)?;
        pack_index(
            &leaf_block_fps,
            &split_packed_values,
            bpd,
            num_index_dims as usize,
            num_leaves,
        )
    };

    // Record index start position and write packed index to .kdi
    let index_start_fp = index.file_pointer();
    index.write_bytes(&packed_index)?;

    // Write BKD metadata to .kdm
    // Simple header (NOT index header!)
    codec_util::write_header(meta, BKD_CODEC, BKD_VERSION)?;
    meta.write_vint(num_dims as i32)?;
    meta.write_vint(num_index_dims as i32)?;
    meta.write_vint(MAX_POINTS_IN_LEAF)?;
    meta.write_vint(bytes_per_dim as i32)?;
    meta.write_vint(num_leaves as i32)?;
    meta.write_bytes(&min_packed)?;
    meta.write_bytes(&max_packed)?;
    meta.write_vlong(count as i64)?; // pointCount
    meta.write_vint(count as i32)?; // docsSeen (all unique in MVP)
    meta.write_vint(packed_index.len() as i32)?;
    meta.write_le_long(data_start_fp as i64)?; // dataStartFP
    meta.write_le_long(index_start_fp as i64)?; // indexStartFP

    debug!(
        "points: wrote BKD field: {} points, {} leaves, data_fp={}, index_fp={}",
        count, num_leaves, data_start_fp, index_start_fp
    );

    Ok(())
}

/// Writes doc IDs for a leaf block.
fn write_leaf_block_docs(data: &mut dyn DataOutput, doc_ids: &[i32]) -> io::Result<()> {
    let count = doc_ids.len();
    data.write_vint(count as i32)?;

    // Compute min, max, and check if strictly sorted
    let mut min = doc_ids[0];
    let mut max = doc_ids[0];
    let mut strictly_sorted = true;
    for i in 1..count {
        if doc_ids[i] <= doc_ids[i - 1] {
            strictly_sorted = false;
        }
        if doc_ids[i] < min {
            min = doc_ids[i];
        }
        if doc_ids[i] > max {
            max = doc_ids[i];
        }
    }

    let range = (max - min + 1) as usize;

    if strictly_sorted && range == count {
        // CONTINUOUS_IDS: all doc IDs are consecutive and sorted
        data.write_byte(CONTINUOUS_IDS)?;
        data.write_vint(min)?;
    } else if strictly_sorted && range <= 16 * count {
        // BITSET_IDS: sorted and reasonably dense
        write_ids_as_bitset(data, doc_ids, min, max)?;
    } else if range <= 0xFFFF {
        // DELTA_BPV_16: deltas from min fit in 16 bits
        write_delta_bpv16(data, doc_ids, min)?;
    } else {
        // BPV_32: full 32-bit doc IDs
        data.write_byte(BPV_32)?;
        for &id in doc_ids {
            data.write_le_int(id)?;
        }
    }

    Ok(())
}

/// Writes doc IDs as a bitset.
fn write_ids_as_bitset(
    data: &mut dyn DataOutput,
    doc_ids: &[i32],
    min: i32,
    max: i32,
) -> io::Result<()> {
    data.write_byte(BITSET_IDS)?;

    let offset_words = min >> 6;
    let offset_bits = offset_words << 6;
    let total_word_count = ((max - offset_bits) >> 6) + 1;

    data.write_vint(offset_words)?;
    data.write_vint(total_word_count)?;

    // Build bitset words
    let mut words = vec![0u64; total_word_count as usize];
    for &id in doc_ids {
        let bit = (id - offset_bits) as usize;
        words[bit >> 6] |= 1u64 << (bit & 63);
    }

    for &word in &words {
        data.write_le_long(word as i64)?;
    }

    Ok(())
}

/// Writes doc IDs using 16-bit delta encoding.
fn write_delta_bpv16(data: &mut dyn DataOutput, doc_ids: &[i32], min: i32) -> io::Result<()> {
    data.write_byte(DELTA_BPV_16)?;
    data.write_vint(min)?;

    let count = doc_ids.len();
    let half_len = count >> 1;

    // Compute deltas from min
    let deltas: Vec<i32> = doc_ids.iter().map(|&id| id - min).collect();

    // Pack pairs: high 16 bits from first half, low 16 bits from second half
    for i in 0..half_len {
        let packed = (deltas[i] << 16) | (deltas[half_len + i] & 0xFFFF);
        data.write_le_int(packed)?;
    }

    // Odd element
    if count & 1 == 1 {
        data.write_le_short(deltas[count - 1] as i16)?;
    }

    Ok(())
}

/// Writes the common prefix bytes for each dimension.
fn write_common_prefixes(
    data: &mut dyn DataOutput,
    first_value: &[u8],
    common_prefix_len: usize,
    bytes_per_dim: u32,
) -> io::Result<()> {
    // For each dimension (only dim 0 in MVP)
    let bpd = bytes_per_dim as usize;
    let num_dims = first_value.len() / bpd;
    for dim in 0..num_dims {
        let dim_offset = dim * bpd;
        // The common prefix length per dimension — for 1D, it's just the overall prefix
        let prefix_len = if num_dims == 1 {
            common_prefix_len
        } else {
            // For multi-dimensional, would need per-dim calculation
            common_prefix_len.min(bpd)
        };
        data.write_vint(prefix_len as i32)?;
        data.write_bytes(&first_value[dim_offset..dim_offset + prefix_len])?;
    }
    Ok(())
}

/// Writes packed values for a leaf block using run-length compression.
fn write_leaf_block_packed_values(
    data: &mut dyn DataOutput,
    sorted_points: &[(i32, Vec<u8>)],
    common_prefix_len: usize,
    bytes_per_dim: u32,
    leaf_cardinality: usize,
) -> io::Result<()> {
    let bpd = bytes_per_dim as usize;
    let count = sorted_points.len();

    // If all values are identical (common prefix covers entire value)
    if common_prefix_len == bpd {
        data.write_byte(0xFF)?; // -1 as byte: all identical marker
        return Ok(());
    }

    // Determine encoding: compare low vs high cardinality cost
    let (high_cardinality_cost, low_cardinality_cost) = if count == leaf_cardinality {
        // All values are different — always use high cardinality
        (0usize, 1usize)
    } else {
        // Compute cost of runLen compression
        let compressed_byte_offset = common_prefix_len;
        let mut num_run_lens = 0usize;
        let mut i = 0;
        while i < count {
            let end = (i + 0xFF).min(count);
            let rl = run_len(sorted_points, i, end, compressed_byte_offset);
            num_run_lens += 1;
            i += rl;
        }
        let high_cost = count * (bpd - common_prefix_len - 1) + 2 * num_run_lens;
        let low_cost = leaf_cardinality * (bpd - common_prefix_len + 1);
        (high_cost, low_cost)
    };

    if low_cardinality_cost <= high_cardinality_cost {
        // Low cardinality encoding
        write_low_cardinality_leaf_block(data, sorted_points, common_prefix_len, bpd)?;
    } else {
        // High cardinality encoding
        write_high_cardinality_leaf_block(data, sorted_points, common_prefix_len, bpd)?;
    }

    Ok(())
}

/// Writes low cardinality encoding: marker -2, then unique values with run lengths.
fn write_low_cardinality_leaf_block(
    data: &mut dyn DataOutput,
    sorted_points: &[(i32, Vec<u8>)],
    common_prefix_len: usize,
    bytes_per_dim: usize,
) -> io::Result<()> {
    data.write_byte(0xFE)?; // -2 as byte: low cardinality marker

    let count = sorted_points.len();
    let suffix_len = bytes_per_dim - common_prefix_len;
    let mut i = 0;
    while i < count {
        // Find run of identical values
        let mut run_len = 1;
        while i + run_len < count
            && sorted_points[i].1[common_prefix_len..bytes_per_dim]
                == sorted_points[i + run_len].1[common_prefix_len..bytes_per_dim]
        {
            run_len += 1;
        }
        // Write suffix bytes
        data.write_bytes(&sorted_points[i].1[common_prefix_len..bytes_per_dim])?;
        // Write run length as VInt
        data.write_vint((run_len - 1) as i32)?;
        debug!(
            "points: low cardinality run: suffix={:02x?} count={}",
            &sorted_points[i].1[common_prefix_len..bytes_per_dim],
            run_len
        );
        let _ = suffix_len; // used for documentation
        i += run_len;
    }

    Ok(())
}

/// Writes high cardinality encoding with run-length compression on the sorted dimension byte.
fn write_high_cardinality_leaf_block(
    data: &mut dyn DataOutput,
    sorted_points: &[(i32, Vec<u8>)],
    common_prefix_len: usize,
    bytes_per_dim: usize,
) -> io::Result<()> {
    let sorted_dim = 0; // For 1D, always dimension 0
    data.write_byte(sorted_dim as u8)?;

    // Note: numIndexDims == 1, so we do NOT write actual bounds

    let compressed_byte_offset = common_prefix_len;
    let compressed_common_prefix = common_prefix_len + 1;
    let count = sorted_points.len();

    let mut i = 0;
    while i < count {
        // Run-length encode byte at compressed_byte_offset
        let end = (i + 0xFF).min(count);
        let prefix_byte = sorted_points[i].1[compressed_byte_offset];
        let run_len = run_len(sorted_points, i, end, compressed_byte_offset);

        data.write_byte(prefix_byte)?;
        data.write_byte(run_len as u8)?;

        // Write suffix bytes for each value in the run
        for point in sorted_points.iter().skip(i).take(run_len) {
            data.write_bytes(&point.1[compressed_common_prefix..bytes_per_dim])?;
        }

        i += run_len;
    }

    Ok(())
}

/// Computes run length of identical bytes at the given offset.
fn run_len(
    sorted_points: &[(i32, Vec<u8>)],
    start: usize,
    end: usize,
    byte_offset: usize,
) -> usize {
    let b = sorted_points[start].1[byte_offset];
    for (i, point) in sorted_points.iter().enumerate().take(end).skip(start + 1) {
        if point.1[byte_offset] != b {
            return i - start;
        }
    }
    end - start
}

/// Computes the common prefix length between two byte slices.
fn common_prefix_length(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Computes the number of distinct values in a sorted slice of points.
fn compute_cardinality(sorted_points: &[(i32, Vec<u8>)]) -> usize {
    if sorted_points.is_empty() {
        return 0;
    }
    let mut count = 1;
    for i in 1..sorted_points.len() {
        if sorted_points[i].1 != sorted_points[i - 1].1 {
            count += 1;
        }
    }
    count
}

/// Computes the number of leaves in the left subtree of a semi-balanced BKD tree.
/// Direct port of Java BKDWriter.getNumLeftLeafNodes() (line 877).
fn get_num_left_leaf_nodes(num_leaves: usize) -> usize {
    assert!(num_leaves > 1);
    let last_full_level = 31 - (num_leaves as u32).leading_zeros();
    let leaves_full_level = 1usize << last_full_level;
    let mut num_left_leaf_nodes = leaves_full_level / 2;
    let unbalanced_leaf_nodes = num_leaves - leaves_full_level;
    num_left_leaf_nodes += unbalanced_leaf_nodes.min(num_left_leaf_nodes);
    num_left_leaf_nodes
}

/// Flushes the write buffer contents into the blocks list and returns the block size.
fn append_block(write_buffer: &mut Vec<u8>, blocks: &mut Vec<Vec<u8>>) -> usize {
    let block = std::mem::take(write_buffer);
    let len = block.len();
    blocks.push(block);
    len
}

/// Writes leaf blocks to .kdd and collects file pointers + split values.
/// For 1D pre-sorted data, leaves are sequential chunks of MAX_POINTS_IN_LEAF.
///
/// Returns (leaf_block_fps, split_packed_values) where split_packed_values[i * bpd..(i+1) * bpd]
/// is the first point value of leaf i+1 (the split boundary between leaf i and leaf i+1).
fn build_leaves(
    data: &mut dyn IndexOutput,
    sorted: &[(i32, Vec<u8>)],
    bytes_per_dim: u32,
    num_leaves: usize,
) -> io::Result<(Vec<u64>, Vec<u8>)> {
    let bpd = bytes_per_dim as usize;
    let max_per_leaf = MAX_POINTS_IN_LEAF as usize;
    let count = sorted.len();

    let mut leaf_block_fps = Vec::with_capacity(num_leaves);
    let mut split_packed_values = vec![0u8; (num_leaves - 1) * bpd];

    for leaf_idx in 0..num_leaves {
        let from = leaf_idx * max_per_leaf;
        let to = ((leaf_idx + 1) * max_per_leaf).min(count);
        let leaf_points = &sorted[from..to];
        let leaf_count = leaf_points.len();

        // Record split value: first point of each non-first leaf
        if leaf_idx > 0 {
            let split_offset = leaf_idx - 1;
            split_packed_values[split_offset * bpd..(split_offset + 1) * bpd]
                .copy_from_slice(&leaf_points[0].1[..bpd]);
        }

        // Record leaf block file pointer
        leaf_block_fps.push(data.file_pointer());

        // Compute per-leaf common prefix and cardinality
        let common_prefix_len =
            common_prefix_length(&leaf_points[0].1, &leaf_points[leaf_count - 1].1);
        let leaf_cardinality = compute_cardinality(leaf_points);

        // Extract doc IDs in sorted order
        let doc_ids: Vec<i32> = leaf_points.iter().map(|(id, _)| *id).collect();

        // Write leaf block to .kdd
        write_leaf_block_docs(data, &doc_ids)?;
        write_common_prefixes(data, &leaf_points[0].1, common_prefix_len, bytes_per_dim)?;
        write_leaf_block_packed_values(
            data,
            leaf_points,
            common_prefix_len,
            bytes_per_dim,
            leaf_cardinality,
        )?;
    }

    Ok((leaf_block_fps, split_packed_values))
}

/// Packs the leaf block file pointers and split values into a compact byte[] index.
fn pack_index(
    leaf_block_fps: &[u64],
    split_packed_values: &[u8],
    bytes_per_dim: usize,
    num_index_dims: usize,
    num_leaves: usize,
) -> Vec<u8> {
    let mut write_buffer = Vec::new();
    let mut blocks: Vec<Vec<u8>> = Vec::new();
    let mut last_split_values = vec![0u8; bytes_per_dim * num_index_dims];
    let mut negative_deltas = vec![false; num_index_dims];

    let total_size = recurse_pack_index(
        &mut write_buffer,
        leaf_block_fps,
        0,
        &mut blocks,
        &mut last_split_values,
        &mut negative_deltas,
        false,
        0,
        num_leaves,
        split_packed_values,
        bytes_per_dim,
        num_index_dims,
    );

    // Compact blocks into single byte array
    let mut index = Vec::with_capacity(total_size);
    for block in &blocks {
        index.extend_from_slice(block);
    }
    debug_assert_eq!(index.len(), total_size);

    index
}

/// Recursively encodes the BKD tree index using prefix-coded split values.
/// Direct port of BKDWriter.recursePackIndex() (line 1121).
///
/// lastSplitValues is per-dimension split value previously seen; we use this to
/// prefix-code the split byte[] on each inner node.
#[allow(clippy::too_many_arguments)]
fn recurse_pack_index(
    write_buffer: &mut Vec<u8>,
    leaf_block_fps: &[u64],
    min_block_fp: u64,
    blocks: &mut Vec<Vec<u8>>,
    last_split_values: &mut [u8],
    negative_deltas: &mut [bool],
    is_left: bool,
    leaves_offset: usize,
    num_leaves: usize,
    split_packed_values: &[u8],
    bytes_per_dim: usize,
    num_index_dims: usize,
) -> usize {
    if num_leaves == 1 {
        if is_left {
            debug_assert_eq!(leaf_block_fps[leaves_offset] - min_block_fp, 0);
            return 0;
        } else {
            let delta = leaf_block_fps[leaves_offset] - min_block_fp;
            VecOutput(write_buffer).write_vlong(delta as i64).unwrap();
            return append_block(write_buffer, blocks);
        }
    }

    // Inner node
    let left_block_fp;
    if is_left {
        debug_assert_eq!(leaf_block_fps[leaves_offset], min_block_fp);
        left_block_fp = min_block_fp;
    } else {
        left_block_fp = leaf_block_fps[leaves_offset];
        let delta = left_block_fp - min_block_fp;
        VecOutput(write_buffer).write_vlong(delta as i64).unwrap();
    }

    let num_left_leaf_nodes = get_num_left_leaf_nodes(num_leaves);
    let right_offset = leaves_offset + num_left_leaf_nodes;
    let split_offset = right_offset - 1;

    let split_dim = 0; // Always 0 for 1D
    let address = split_offset * bytes_per_dim;
    let split_value = &split_packed_values[address..address + bytes_per_dim];

    // Find common prefix with last split value in this dim
    let last_value_start = split_dim * bytes_per_dim;
    let prefix = common_prefix_length(
        split_value,
        &last_split_values[last_value_start..last_value_start + bytes_per_dim],
    );

    let first_diff_byte_delta;
    if prefix < bytes_per_dim {
        let mut delta =
            (split_value[prefix] as i32) - (last_split_values[last_value_start + prefix] as i32);
        if negative_deltas[split_dim] {
            delta = -delta;
        }
        debug_assert!(delta > 0);
        first_diff_byte_delta = delta;
    } else {
        first_diff_byte_delta = 0;
    }

    // Pack prefix, splitDim and firstDiffByteDelta into a single VInt
    let code = (first_diff_byte_delta * (1 + bytes_per_dim as i32) + prefix as i32)
        * num_index_dims as i32
        + split_dim as i32;
    VecOutput(write_buffer).write_vint(code).unwrap();

    // Write suffix bytes (prefix-coded, skipping first diff byte which is in code)
    let suffix = bytes_per_dim - prefix;
    if suffix > 1 {
        write_buffer.extend_from_slice(&split_value[prefix + 1..prefix + suffix]);
    }

    // Save split value suffix for restoration after recursion
    let sav_split_value =
        last_split_values[last_value_start + prefix..last_value_start + prefix + suffix].to_vec();

    // Copy our split value into last_split_values for children to prefix-code against
    last_split_values[last_value_start + prefix..last_value_start + prefix + suffix]
        .copy_from_slice(&split_value[prefix..prefix + suffix]);

    let num_bytes = append_block(write_buffer, blocks);

    // Placeholder for left-tree numBytes
    let idx_sav = blocks.len();
    blocks.push(Vec::new());

    let sav_negative_delta = negative_deltas[split_dim];
    negative_deltas[split_dim] = true;

    let left_num_bytes = recurse_pack_index(
        write_buffer,
        leaf_block_fps,
        left_block_fp,
        blocks,
        last_split_values,
        negative_deltas,
        true,
        leaves_offset,
        num_left_leaf_nodes,
        split_packed_values,
        bytes_per_dim,
        num_index_dims,
    );

    // Write left subtree size (only if left child is not a single leaf)
    if num_left_leaf_nodes != 1 {
        VecOutput(write_buffer)
            .write_vint(left_num_bytes as i32)
            .unwrap();
    } else {
        debug_assert_eq!(left_num_bytes, 0);
    }

    let bytes2 = std::mem::take(write_buffer);
    blocks[idx_sav] = bytes2;

    negative_deltas[split_dim] = false;
    let right_num_bytes = recurse_pack_index(
        write_buffer,
        leaf_block_fps,
        left_block_fp,
        blocks,
        last_split_values,
        negative_deltas,
        false,
        right_offset,
        num_leaves - num_left_leaf_nodes,
        split_packed_values,
        bytes_per_dim,
        num_index_dims,
    );

    negative_deltas[split_dim] = sav_negative_delta;

    // Restore last_split_values
    last_split_values[last_value_start + prefix..last_value_start + prefix + suffix]
        .copy_from_slice(&sav_split_value);

    num_bytes + blocks[idx_sav].len() + left_num_bytes + right_num_bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::codec_util::{FOOTER_LENGTH, header_length, index_header_length};
    use crate::document::{DocValuesType, IndexOptions};
    use crate::index::indexing_chain::{DocValuesAccumulator, PerFieldData};
    use crate::index::{FieldInfo, FieldInfos, PointDimensionConfig};
    use std::collections::HashMap;

    // Ported from org.apache.lucene.util.bkd.TestBKD

    /// Helper to create sortable bytes for a long value (same as long_to_sortable_bytes).
    fn long_to_sortable_bytes(v: i64) -> Vec<u8> {
        let flipped = (v ^ i64::MIN) as u64;
        flipped.to_be_bytes().to_vec()
    }

    fn make_point_field_info(name: &str, number: u32) -> FieldInfo {
        FieldInfo::new(
            name.to_string(),
            number,
            false,
            false,
            IndexOptions::None, // point fields are typically not indexed as text
            DocValuesType::SortedNumeric,
            PointDimensionConfig {
                dimension_count: 1,
                index_dimension_count: 1,
                num_bytes: 8,
            },
        )
    }

    fn make_per_field_with_points(points: Vec<(i32, Vec<u8>)>) -> PerFieldData {
        PerFieldData {
            postings: HashMap::new(),
            doc_values: DocValuesAccumulator::None,
            norms: Vec::new(),
            norms_docs: Vec::new(),
            points,
        }
    }

    #[test]
    fn test_continuous_doc_ids() {
        let mut data = MemoryIndexOutput::new("test".to_string());
        write_leaf_block_docs(&mut data, &[0, 1, 2]).unwrap();
        let bytes = data.bytes();
        // VInt(3) = 0x03, CONTINUOUS_IDS = 0xFE, VInt(0) = 0x00
        assert_eq!(bytes, &[0x03, 0xFE, 0x00]);
    }

    #[test]
    fn test_continuous_doc_ids_nonzero_start() {
        let mut data = MemoryIndexOutput::new("test".to_string());
        write_leaf_block_docs(&mut data, &[5, 6, 7, 8]).unwrap();
        let bytes = data.bytes();
        // VInt(4) = 0x04, CONTINUOUS_IDS = 0xFE, VInt(5) = 0x05
        assert_eq!(bytes, &[0x04, 0xFE, 0x05]);
    }

    #[test]
    fn test_common_prefix() {
        let v1 = long_to_sortable_bytes(1000);
        let v2 = long_to_sortable_bytes(3000);
        let prefix_len = common_prefix_length(&v1, &v2);
        // 1000 → 80 00 00 00 00 00 03 E8
        // 3000 → 80 00 00 00 00 00 0B B8
        // Common: 80 00 00 00 00 00 (6 bytes)
        assert_eq!(prefix_len, 6);
    }

    #[test]
    fn test_common_prefix_identical() {
        let v1 = long_to_sortable_bytes(42);
        let v2 = long_to_sortable_bytes(42);
        assert_eq!(common_prefix_length(&v1, &v2), 8);
    }

    #[test]
    fn test_common_prefix_completely_different() {
        let v1 = long_to_sortable_bytes(0);
        let v2 = long_to_sortable_bytes(-1);
        // 0 → 80 00 00 00 00 00 00 00
        // -1 → 7F FF FF FF FF FF FF FF
        assert_eq!(common_prefix_length(&v1, &v2), 0);
    }

    #[test]
    fn test_single_leaf_three_points() {
        // Verify the full leaf block for 3 timestamps
        let points: Vec<(i32, Vec<u8>)> = vec![
            (0, long_to_sortable_bytes(1000)),
            (1, long_to_sortable_bytes(2000)),
            (2, long_to_sortable_bytes(3000)),
        ];

        let mut data = MemoryIndexOutput::new("test".to_string());

        // Sort (already sorted by value in this case)
        let mut sorted = points.clone();
        sorted.sort_by(|a, b| a.1.cmp(&b.1));

        let common_prefix_len = common_prefix_length(&sorted[0].1, &sorted[2].1);
        assert_eq!(common_prefix_len, 6);

        let doc_ids: Vec<i32> = sorted.iter().map(|(id, _)| *id).collect();
        let cardinality = compute_cardinality(&sorted);
        assert_eq!(cardinality, 3);

        write_leaf_block_docs(&mut data, &doc_ids).unwrap();
        write_common_prefixes(&mut data, &sorted[0].1, common_prefix_len, 8).unwrap();
        write_leaf_block_packed_values(&mut data, &sorted, common_prefix_len, 8, cardinality)
            .unwrap();

        let bytes = data.bytes();

        // Expected leaf block (20 bytes total):
        // Doc IDs: 03 FE 00
        // Common prefix: 06 80 00 00 00 00 00
        // Packed values: 00 03 01 E8 07 01 D0 0B 01 B8
        let expected: Vec<u8> = vec![
            0x03, 0xFE, 0x00, // VInt(3), CONTINUOUS_IDS, VInt(0)
            0x06, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, // VInt(6) + prefix bytes
            0x00, // sortedDim=0 (high cardinality)
            0x03, 0x01, 0xE8, // run: byte=0x03, len=1, suffix=0xE8
            0x07, 0x01, 0xD0, // run: byte=0x07, len=1, suffix=0xD0
            0x0B, 0x01, 0xB8, // run: byte=0x0B, len=1, suffix=0xB8
        ];

        assert_eq!(bytes.len(), 20);
        assert_eq!(bytes, expected.as_slice());
    }

    #[test]
    fn test_all_identical_points() {
        let val = long_to_sortable_bytes(42);
        let points: Vec<(i32, Vec<u8>)> =
            vec![(0, val.clone()), (1, val.clone()), (2, val.clone())];

        let mut data = MemoryIndexOutput::new("test".to_string());
        let common_prefix_len = common_prefix_length(&points[0].1, &points[2].1);
        assert_eq!(common_prefix_len, 8); // identical → full prefix

        write_leaf_block_packed_values(&mut data, &points, common_prefix_len, 8, 1).unwrap();

        // All identical: just the 0xFF marker
        assert_eq!(data.bytes(), &[0xFF]);
    }

    #[test]
    fn test_packed_index_single_leaf() {
        let mut packed_index = Vec::new();
        VecOutput(&mut packed_index).write_vlong(0).unwrap();
        // VLong(0) = single byte 0x00
        assert_eq!(packed_index, vec![0x00]);
    }

    #[test]
    fn test_bkd_metadata() {
        // Verify BKD metadata fields in .kdm
        let fi = make_point_field_info("modified", 1);
        let field_infos = FieldInfos::new(vec![fi]);

        let points: Vec<(i32, Vec<u8>)> = vec![
            (0, long_to_sortable_bytes(1000)),
            (1, long_to_sortable_bytes(2000)),
            (2, long_to_sortable_bytes(3000)),
        ];

        let mut per_field = HashMap::new();
        per_field.insert("modified".to_string(), make_per_field_with_points(points));

        let segment_id = [0u8; 16];
        let result = write("_0", "Lucene90_0", &segment_id, &field_infos, &per_field, 3).unwrap();

        assert_eq!(result.len(), 3);
        let kdm = &result[2].data;

        let meta_header_len = index_header_length(META_CODEC, "Lucene90_0");

        // field_number = 1
        let mut pos = meta_header_len;
        assert_eq!(
            &kdm[pos..pos + 4],
            &1i32.to_le_bytes(),
            "field number should be 1"
        );
        pos += 4;

        // BKD simple header: "BKD" v10
        let bkd_header_len = header_length(BKD_CODEC);
        let bkd_header = &kdm[pos..pos + bkd_header_len];
        // Magic (BE): 0x3fd76c17
        assert_eq!(&bkd_header[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);
        // Codec name: VInt(3) + "BKD"
        assert_eq!(bkd_header[4], 3);
        assert_eq!(&bkd_header[5..8], b"BKD");
        // Version (BE): 10
        assert_eq!(&bkd_header[8..12], &10i32.to_be_bytes());
        pos += bkd_header_len;

        // VInt(numDims=1)
        assert_eq!(kdm[pos], 1);
        pos += 1;
        // VInt(numIndexDims=1)
        assert_eq!(kdm[pos], 1);
        pos += 1;
        // VInt(maxPointsInLeaf=512) → 512 = 0x200 → VInt: 0x80 0x04
        assert_eq!(&kdm[pos..pos + 2], &[0x80, 0x04]);
        pos += 2;
        // VInt(bytesPerDim=8)
        assert_eq!(kdm[pos], 8);
        pos += 1;
        // VInt(numLeaves=1)
        assert_eq!(kdm[pos], 1);
        pos += 1;

        // minPackedValue (8 bytes): long_to_sortable_bytes(1000)
        let min_expected = long_to_sortable_bytes(1000);
        assert_eq!(&kdm[pos..pos + 8], min_expected.as_slice());
        pos += 8;

        // maxPackedValue (8 bytes): long_to_sortable_bytes(3000)
        let max_expected = long_to_sortable_bytes(3000);
        assert_eq!(&kdm[pos..pos + 8], max_expected.as_slice());
        pos += 8;

        // VLong(pointCount=3)
        assert_eq!(kdm[pos], 3);
        pos += 1;

        // VInt(docsSeen=3)
        assert_eq!(kdm[pos], 3);
        pos += 1;

        // VInt(packedIndex.length=1) — single byte VLong(0)
        assert_eq!(kdm[pos], 1);
        pos += 1;

        // dataStartFP
        let data_header_len = index_header_length(DATA_CODEC, "Lucene90_0");
        let expected_data_fp = data_header_len as i64;
        assert_eq!(&kdm[pos..pos + 8], &expected_data_fp.to_le_bytes());
        pos += 8;

        // indexStartFP
        let index_header_len = index_header_length(INDEX_CODEC, "Lucene90_0");
        let expected_index_fp = index_header_len as i64;
        assert_eq!(&kdm[pos..pos + 8], &expected_index_fp.to_le_bytes());
        pos += 8;

        // Sentinel: -1
        assert_eq!(&kdm[pos..pos + 4], &(-1i32).to_le_bytes());
    }

    #[test]
    fn test_file_headers_footers() {
        let fi = make_point_field_info("modified", 1);
        let field_infos = FieldInfos::new(vec![fi]);

        let points: Vec<(i32, Vec<u8>)> = vec![
            (0, long_to_sortable_bytes(100)),
            (1, long_to_sortable_bytes(200)),
            (2, long_to_sortable_bytes(300)),
        ];

        let mut per_field = HashMap::new();
        per_field.insert("modified".to_string(), make_per_field_with_points(points));

        let segment_id = [0xABu8; 16];
        let result = write("_0", "Lucene90_0", &segment_id, &field_infos, &per_field, 3).unwrap();

        // Check file names
        assert_eq!(result[0].name, "_0_Lucene90_0.kdd");
        assert_eq!(result[1].name, "_0_Lucene90_0.kdi");
        assert_eq!(result[2].name, "_0_Lucene90_0.kdm");

        // All 3 files should start with codec magic
        for f in &result {
            assert_eq!(
                &f.data[0..4],
                &[0x3f, 0xd7, 0x6c, 0x17],
                "{} should start with codec magic",
                f.name
            );
        }

        // All 3 files should end with footer magic
        for f in &result {
            let footer_start = f.data.len() - FOOTER_LENGTH;
            assert_eq!(
                &f.data[footer_start..footer_start + 4],
                &[0xc0, 0x28, 0x93, 0xe8],
                "{} should end with footer magic",
                f.name
            );
        }

        // Verify segment ID is in headers (at offset after header_length("codec"))
        let kdd = &result[0].data;
        let data_codec_header_len = header_length(DATA_CODEC);
        assert_eq!(
            &kdd[data_codec_header_len..data_codec_header_len + 16],
            &[0xAB; 16],
            ".kdd should contain segment ID"
        );
    }

    #[test]
    fn test_points_sorted() {
        // Points provided in unsorted order should be sorted by value
        let points: Vec<(i32, Vec<u8>)> = vec![
            (2, long_to_sortable_bytes(3000)), // doc 2, value 3000
            (0, long_to_sortable_bytes(1000)), // doc 0, value 1000
            (1, long_to_sortable_bytes(2000)), // doc 1, value 2000
        ];

        let mut sorted = points.clone();
        sorted.sort_by(|a, b| a.1.cmp(&b.1));

        // After sorting by value, order should be: 1000, 2000, 3000
        assert_eq!(sorted[0].0, 0); // doc 0 (value 1000)
        assert_eq!(sorted[1].0, 1); // doc 1 (value 2000)
        assert_eq!(sorted[2].0, 2); // doc 2 (value 3000)

        // Write through the full pipeline and verify it works
        let fi = make_point_field_info("modified", 1);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert("modified".to_string(), make_per_field_with_points(points));

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 3).unwrap();
        assert_eq!(result.len(), 3);

        // Verify the .kdd leaf block contains correctly sorted data
        let kdd = &result[0].data;
        let data_header_len = index_header_length(DATA_CODEC, "");

        // After header: VInt(3)=03, CONTINUOUS_IDS=FE, VInt(0)=00
        assert_eq!(kdd[data_header_len], 0x03);
        assert_eq!(kdd[data_header_len + 1], 0xFE);
        assert_eq!(kdd[data_header_len + 2], 0x00);
    }

    #[test]
    fn test_compute_cardinality() {
        let points: Vec<(i32, Vec<u8>)> = vec![
            (0, vec![1, 2, 3]),
            (1, vec![1, 2, 3]), // duplicate
            (2, vec![4, 5, 6]),
        ];
        assert_eq!(compute_cardinality(&points), 2);

        let all_same: Vec<(i32, Vec<u8>)> =
            vec![(0, vec![1, 2, 3]), (1, vec![1, 2, 3]), (2, vec![1, 2, 3])];
        assert_eq!(compute_cardinality(&all_same), 1);

        let all_different: Vec<(i32, Vec<u8>)> = vec![(0, vec![1]), (1, vec![2]), (2, vec![3])];
        assert_eq!(compute_cardinality(&all_different), 3);
    }

    #[test]
    fn test_kdm_total_sizes() {
        // Verify that .kdm contains total .kdi and .kdd sizes before its footer
        let fi = make_point_field_info("modified", 1);
        let field_infos = FieldInfos::new(vec![fi]);

        let points: Vec<(i32, Vec<u8>)> = vec![
            (0, long_to_sortable_bytes(1000)),
            (1, long_to_sortable_bytes(2000)),
            (2, long_to_sortable_bytes(3000)),
        ];

        let mut per_field = HashMap::new();
        per_field.insert("modified".to_string(), make_per_field_with_points(points));

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 3).unwrap();

        let kdd = &result[0].data;
        let kdi = &result[1].data;
        let kdm = &result[2].data;

        // The last 16 bytes before the footer in .kdm are:
        // writeLong(indexFilePointer) + writeLong(dataFilePointer)
        let footer_start = kdm.len() - FOOTER_LENGTH;
        let total_sizes_start = footer_start - 16;

        let index_total_size = i64::from_le_bytes(
            kdm[total_sizes_start..total_sizes_start + 8]
                .try_into()
                .unwrap(),
        );
        let data_total_size = i64::from_le_bytes(
            kdm[total_sizes_start + 8..total_sizes_start + 16]
                .try_into()
                .unwrap(),
        );

        assert_eq!(index_total_size, kdi.len() as i64);
        assert_eq!(data_total_size, kdd.len() as i64);
    }

    #[test]
    fn test_no_point_fields_skipped() {
        // A field without points should be skipped entirely
        let fi_text = FieldInfo::new(
            "contents".to_string(),
            0,
            false,
            false,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::None,
            PointDimensionConfig::default(), // no points
        );
        let fi_point = make_point_field_info("modified", 1);
        let field_infos = FieldInfos::new(vec![fi_text, fi_point]);

        let points: Vec<(i32, Vec<u8>)> = vec![(0, long_to_sortable_bytes(100))];

        let mut per_field = HashMap::new();
        per_field.insert("modified".to_string(), make_per_field_with_points(points));

        let segment_id = [0u8; 16];
        let result = write("_0", "", &segment_id, &field_infos, &per_field, 1).unwrap();

        // Should succeed and produce 3 files
        assert_eq!(result.len(), 3);

        // In the .kdm, the first field number should be 1 (modified), not 0 (contents)
        let kdm = &result[2].data;
        let meta_header_len = index_header_length(META_CODEC, "");
        assert_eq!(
            &kdm[meta_header_len..meta_header_len + 4],
            &1i32.to_le_bytes(),
            "first field in meta should be field 1 (modified)"
        );
    }

    #[test]
    fn test_low_cardinality_encoding() {
        // Test with 3 points where 2 are identical → low cardinality may be chosen
        let val_a = long_to_sortable_bytes(1000);
        let val_b = long_to_sortable_bytes(1000);
        let val_c = long_to_sortable_bytes(2000);

        let sorted_points: Vec<(i32, Vec<u8>)> = vec![(0, val_a), (1, val_b), (2, val_c)];

        let common_prefix_len = common_prefix_length(&sorted_points[0].1, &sorted_points[2].1);
        let cardinality = compute_cardinality(&sorted_points);
        assert_eq!(cardinality, 2);

        let mut data = MemoryIndexOutput::new("test".to_string());
        write_leaf_block_packed_values(
            &mut data,
            &sorted_points,
            common_prefix_len,
            8,
            cardinality,
        )
        .unwrap();

        let bytes = data.bytes();
        // Should use either low or high cardinality encoding depending on cost
        // Both are valid; just verify it produces valid output
        assert!(!bytes.is_empty());
        // First byte should be sorted_dim (0x00) for high card, 0xFE for low card
        assert!(bytes[0] == 0x00 || bytes[0] == 0xFE);
    }

    #[test]
    fn test_vlong_encoding() {
        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vlong(0).unwrap();
        assert_eq!(buf, vec![0x00]);

        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vlong(127).unwrap();
        assert_eq!(buf, vec![0x7F]);

        let mut buf = Vec::new();
        VecOutput(&mut buf).write_vlong(128).unwrap();
        assert_eq!(buf, vec![0x80, 0x01]);
    }

    // --- Multi-leaf BKD tests ---
    // Ported from org.apache.lucene.util.bkd.TestBKD

    #[test]
    fn test_get_num_left_leaf_nodes() {
        // Port of BKDWriter.getNumLeftLeafNodes() balancing logic
        assert_eq!(get_num_left_leaf_nodes(2), 1);
        assert_eq!(get_num_left_leaf_nodes(3), 2);
        assert_eq!(get_num_left_leaf_nodes(4), 2);
        assert_eq!(get_num_left_leaf_nodes(5), 3);
        assert_eq!(get_num_left_leaf_nodes(6), 4);
        assert_eq!(get_num_left_leaf_nodes(7), 4);
        assert_eq!(get_num_left_leaf_nodes(8), 4);
        assert_eq!(get_num_left_leaf_nodes(9), 5);
        assert_eq!(get_num_left_leaf_nodes(100), 64);
    }

    /// Helper to build a BKD via the full write() pipeline and return the 3 files.
    fn write_bkd_with_n_points(n: usize) -> Vec<SegmentFile> {
        let fi = make_point_field_info("modified", 1);
        let field_infos = FieldInfos::new(vec![fi]);

        let points: Vec<(i32, Vec<u8>)> = (0..n)
            .map(|i| (i as i32, long_to_sortable_bytes(i as i64 * 1000)))
            .collect();

        let mut per_field = HashMap::new();
        per_field.insert("modified".to_string(), make_per_field_with_points(points));

        let segment_id = [0u8; 16];
        write("_0", "", &segment_id, &field_infos, &per_field, n as i32).unwrap()
    }

    /// Reads numLeaves from .kdm metadata (after BKD header + config fields).
    fn read_num_leaves_from_kdm(kdm: &[u8]) -> i32 {
        let meta_header_len = index_header_length(META_CODEC, "");
        let mut pos = meta_header_len;
        pos += 4; // field number
        pos += header_length(BKD_CODEC); // BKD simple header
        pos += 1; // VInt(numDims=1)
        pos += 1; // VInt(numIndexDims=1)
        pos += 2; // VInt(maxPointsInLeaf=512) → 0x80 0x04
        pos += 1; // VInt(bytesPerDim=8)
        // numLeaves is next VInt
        // Decode VInt
        let mut result = 0i32;
        let mut shift = 0;
        loop {
            let b = kdm[pos] as i32;
            pos += 1;
            result |= (b & 0x7F) << shift;
            if (b & 0x80) == 0 {
                break;
            }
            shift += 7;
        }
        result
    }

    #[test]
    fn test_two_leaf_bkd() {
        // 513 points exceeds single leaf (512), should produce 2 leaves
        let result = write_bkd_with_n_points(513);
        assert_eq!(result.len(), 3);

        let kdm = &result[2].data;
        let num_leaves = read_num_leaves_from_kdm(kdm);
        assert_eq!(num_leaves, 2, "513 points should produce 2 leaves");
    }

    #[test]
    fn test_three_leaf_bkd() {
        // 1025 points should produce 3 leaves (512 + 512 + 1)
        let result = write_bkd_with_n_points(1025);
        assert_eq!(result.len(), 3);

        let kdm = &result[2].data;
        let num_leaves = read_num_leaves_from_kdm(kdm);
        assert_eq!(num_leaves, 3, "1025 points should produce 3 leaves");
    }

    #[test]
    fn test_exact_boundary_512() {
        // Exactly 512 points should still produce 1 leaf
        let result = write_bkd_with_n_points(512);
        assert_eq!(result.len(), 3);

        let kdm = &result[2].data;
        let num_leaves = read_num_leaves_from_kdm(kdm);
        assert_eq!(num_leaves, 1, "512 points should produce 1 leaf");
    }

    #[test]
    fn test_exact_boundary_1024() {
        // Exactly 1024 points should produce 2 leaves (512 + 512)
        let result = write_bkd_with_n_points(1024);
        assert_eq!(result.len(), 3);

        let kdm = &result[2].data;
        let num_leaves = read_num_leaves_from_kdm(kdm);
        assert_eq!(num_leaves, 2, "1024 points should produce 2 leaves");
    }

    #[test]
    fn test_single_leaf_fp_fix() {
        // Verify packed index for single leaf contains data_start_fp, not 0
        let result = write_bkd_with_n_points(3);
        assert_eq!(result.len(), 3);

        let kdi = &result[1].data;
        let index_header_len = index_header_length(INDEX_CODEC, "");

        // The packed index is written right after the .kdi header.
        // For a single leaf, it should be VLong(data_start_fp).
        // data_start_fp = size of .kdd index header
        let data_header_len = index_header_length(DATA_CODEC, "");

        // Read VLong from .kdi
        let mut pos = index_header_len;
        let mut value = 0i64;
        let mut shift = 0;
        loop {
            let b = kdi[pos] as i64;
            pos += 1;
            value |= (b & 0x7F) << shift;
            if (b & 0x80) == 0 {
                break;
            }
            shift += 7;
        }

        assert_eq!(
            value, data_header_len as i64,
            "single-leaf packed index should contain data_start_fp={}, got {}",
            data_header_len, value
        );
        assert!(value > 0, "packed index FP must not be 0");
    }

    #[test]
    fn test_multi_leaf_file_sizes_consistent() {
        // Verify .kdm contains correct total file sizes for multi-leaf index
        let result = write_bkd_with_n_points(1025);

        let kdd = &result[0].data;
        let kdi = &result[1].data;
        let kdm = &result[2].data;

        let footer_start = kdm.len() - FOOTER_LENGTH;
        let total_sizes_start = footer_start - 16;

        let index_total_size = i64::from_le_bytes(
            kdm[total_sizes_start..total_sizes_start + 8]
                .try_into()
                .unwrap(),
        );
        let data_total_size = i64::from_le_bytes(
            kdm[total_sizes_start + 8..total_sizes_start + 16]
                .try_into()
                .unwrap(),
        );

        assert_eq!(index_total_size, kdi.len() as i64);
        assert_eq!(data_total_size, kdd.len() as i64);
    }
}
