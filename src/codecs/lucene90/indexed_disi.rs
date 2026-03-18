// SPDX-License-Identifier: Apache-2.0
//! IndexedDISI bitset writer for encoding which documents have values.
//!
//! Encodes sorted document IDs into 65536-doc blocks using one of three strategies:
//! - **ALL**: block has all 65536 docs set (no data after header)
//! - **DENSE**: block has 4096–65535 docs set (rank table + bitmap)
//! - **SPARSE**: block has 1–4095 docs set (array of 16-bit offsets)
//!
//! Used by doc values and norms to support sparse fields (present in some but not all documents).

use std::io;

use crate::store::IndexOutput;

/// Number of doc IDs per block.
const BLOCK_SIZE: i32 = 65536;

/// Number of `i64` words in a dense block bitmap.
const DENSE_BLOCK_LONGS: usize = BLOCK_SIZE as usize / 64; // 1024

/// Maximum cardinality for SPARSE encoding; above this threshold, DENSE is used.
const MAX_ARRAY_LENGTH: i32 = (1 << 12) - 1; // 4095

/// Sentinel value indicating no more documents.
const NO_MORE_DOCS: i32 = i32::MAX;

/// Default rank power for DENSE blocks: rank entry every 2^9 = 512 doc IDs.
pub const DEFAULT_DENSE_RANK_POWER: i8 = 9;

/// Writes an IndexedDISI bitset for the given sorted doc IDs.
///
/// Returns the jump table entry count (stored in metadata).
pub fn write_bit_set(doc_ids: &[i32], max_doc: i32, out: &mut dyn IndexOutput) -> io::Result<i16> {
    write_bit_set_with_rank_power(doc_ids, max_doc, out, DEFAULT_DENSE_RANK_POWER)
}

/// Writes an IndexedDISI bitset with a specified dense rank power.
///
/// `dense_rank_power` controls rank granularity for DENSE blocks (valid: 7–15, or -1 to disable).
/// Returns the jump table entry count.
fn write_bit_set_with_rank_power(
    doc_ids: &[i32],
    _max_doc: i32,
    out: &mut dyn IndexOutput,
    dense_rank_power: i8,
) -> io::Result<i16> {
    let origo = out.file_pointer();

    if !(7..=15).contains(&dense_rank_power) && dense_rank_power != -1 {
        return Err(io::Error::other(format!(
            "Acceptable values for denseRankPower are 7-15. The provided power was {}",
            dense_rank_power
        )));
    }

    let mut total_cardinality: i32 = 0;
    let mut jumps: Vec<(i32, i32)> = Vec::new(); // (index, offset) per block
    let mut last_block: i32 = 0;

    // Process doc IDs block by block
    let mut i = 0;
    while i < doc_ids.len() {
        let doc = doc_ids[i];
        let block = doc >> 16;

        // Fill buffer with all docs in this block
        let mut buffer = [0i64; DENSE_BLOCK_LONGS];
        while i < doc_ids.len() && (doc_ids[i] >> 16) == block {
            let within_block = doc_ids[i] & 0xFFFF;
            buffer[(within_block >> 6) as usize] |= 1i64 << (within_block & 63);
            i += 1;
        }

        let block_cardinality = buffer.iter().map(|w| w.count_ones() as i32).sum::<i32>();

        // Add jump entries for blocks from last_block to block+1
        let offset = (out.file_pointer() - origo) as i32;
        add_jumps(&mut jumps, offset, total_cardinality, last_block, block + 1);
        last_block = block + 1;

        // Flush block
        flush(block, &buffer, block_cardinality, dense_rank_power, out)?;

        total_cardinality += block_cardinality;
    }

    // NO_MORE_DOCS sentinel block
    let offset = (out.file_pointer() - origo) as i32;
    add_jumps(
        &mut jumps,
        offset,
        total_cardinality,
        last_block,
        last_block + 1,
    );

    let mut sentinel_buffer = [0i64; DENSE_BLOCK_LONGS];
    let nmd_within = NO_MORE_DOCS & 0xFFFF; // 65535
    sentinel_buffer[(nmd_within >> 6) as usize] |= 1i64 << (nmd_within & 63);
    flush(
        NO_MORE_DOCS >> 16, // 32767
        &sentinel_buffer,
        1,
        dense_rank_power,
        out,
    )?;

    // Flush jump table
    flush_block_jumps(&jumps, last_block + 1, out)
}

/// Writes a single block to the output.
fn flush(
    block: i32,
    buffer: &[i64; DENSE_BLOCK_LONGS],
    cardinality: i32,
    dense_rank_power: i8,
    out: &mut dyn IndexOutput,
) -> io::Result<()> {
    debug_assert!((0..BLOCK_SIZE).contains(&block));
    debug_assert!(cardinality > 0 && cardinality <= BLOCK_SIZE);

    // Block header
    out.write_le_short(block as i16)?;
    out.write_le_short((cardinality - 1) as i16)?;

    if cardinality > MAX_ARRAY_LENGTH {
        if cardinality != BLOCK_SIZE {
            // DENSE block
            if dense_rank_power != -1 {
                let rank = create_rank(buffer, dense_rank_power);
                out.write_bytes(&rank)?;
            }
            for &word in buffer.iter() {
                out.write_le_long(word)?;
            }
        }
        // ALL block: no data needed
    } else {
        // SPARSE block: write each set bit's position as a short
        for (word_idx, &word) in buffer.iter().enumerate() {
            let mut bits = word as u64;
            while bits != 0 {
                let bit = bits.trailing_zeros() as i32;
                let doc_in_block = (word_idx as i32) * 64 + bit;
                out.write_le_short(doc_in_block as i16)?;
                bits &= bits - 1; // clear lowest set bit
            }
        }
    }

    Ok(())
}

/// Creates a rank table for a DENSE block.
///
/// One rank entry per `2^dense_rank_power` bits, each entry is 2 bytes (big-endian within entry).
fn create_rank(buffer: &[i64; DENSE_BLOCK_LONGS], dense_rank_power: i8) -> Vec<u8> {
    let longs_per_rank = 1usize << (dense_rank_power - 6);
    let rank_mark = longs_per_rank - 1;
    let rank_index_shift = (dense_rank_power - 7) as usize;
    let rank_len = DENSE_BLOCK_LONGS >> rank_index_shift;
    let mut rank = vec![0u8; rank_len];

    let mut bit_count: i32 = 0;
    for word in 0..DENSE_BLOCK_LONGS {
        if (word & rank_mark) == 0 {
            // Big-endian within the 2-byte rank entry
            rank[word >> rank_index_shift] = (bit_count >> 8) as u8;
            rank[(word >> rank_index_shift) + 1] = (bit_count & 0xFF) as u8;
        }
        bit_count += buffer[word].count_ones() as i32;
    }

    rank
}

/// Adds jump table entries for block range [start_block, end_block).
fn add_jumps(
    jumps: &mut Vec<(i32, i32)>,
    offset: i32,
    index: i32,
    start_block: i32,
    end_block: i32,
) {
    if jumps.len() < end_block as usize {
        jumps.resize(end_block as usize, (0, 0));
    }
    for b in start_block..end_block {
        jumps[b as usize] = (index, offset);
    }
}

/// Flushes the jump table and returns the entry count.
fn flush_block_jumps(
    jumps: &[(i32, i32)],
    block_count: i32,
    out: &mut dyn IndexOutput,
) -> io::Result<i16> {
    let mut count = block_count;
    if count == 2 {
        // Single real block + NO_MORE_DOCS: not worth storing jump table
        count = 0;
    }

    for &(index, offset) in &jumps[..count as usize] {
        out.write_le_int(index)?; // cumulative doc count
        out.write_le_int(offset)?; // byte offset from origo
    }

    Ok(count as i16)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryIndexOutput;

    /// Helper to write a bitset and return the raw bytes and jump table entry count.
    fn write_and_get_bytes(doc_ids: &[i32], max_doc: i32) -> (Vec<u8>, i16) {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let entry_count = write_bit_set(doc_ids, max_doc, &mut out).unwrap();
        (out.bytes().to_vec(), entry_count)
    }

    #[test]
    fn test_empty_input() {
        // No documents set — should still write NO_MORE_DOCS sentinel block
        let (bytes, entry_count) = write_and_get_bytes(&[], 10);

        // Entry count: 0+1=1 block (just the sentinel), blockCount=1 → written as-is
        // Actually: last_block stays 0, so blockCount = 0+1 = 1
        // blockCount=1 is not 2, so it writes 1 jump entry
        assert_eq!(entry_count, 1);

        // Sentinel block: blockID=32767 (NO_MORE_DOCS >>> 16), cardinality-1=0
        assert_eq!(&bytes[0..2], &32767i16.to_le_bytes());
        assert_eq!(&bytes[2..4], &0i16.to_le_bytes());

        // SPARSE: single entry, doc 65535 within block
        assert_eq!(&bytes[4..6], &(-1i16).to_le_bytes()); // 65535 as i16 = -1

        // Jump table: 1 entry (index=0, offset=0)
        assert_eq!(&bytes[6..10], &0i32.to_le_bytes());
        assert_eq!(&bytes[10..14], &0i32.to_le_bytes());
    }

    #[test]
    fn test_single_doc() {
        let (bytes, entry_count) = write_and_get_bytes(&[42], 100);

        // blockCount = 0+1+1 = 2 (block 0 + sentinel) → optimized to 0
        assert_eq!(entry_count, 0);

        // Block 0: blockID=0, cardinality-1=0
        assert_eq!(&bytes[0..2], &0i16.to_le_bytes());
        assert_eq!(&bytes[2..4], &0i16.to_le_bytes());

        // SPARSE: single entry, doc 42
        assert_eq!(&bytes[4..6], &42i16.to_le_bytes());

        // Sentinel block follows
        assert_eq!(&bytes[6..8], &32767i16.to_le_bytes());
    }

    #[test]
    fn test_sparse_block() {
        // 3 docs in block 0 — well under 4096 threshold
        let doc_ids = vec![10, 100, 1000];
        let (bytes, _) = write_and_get_bytes(&doc_ids, 2000);

        // Block header
        assert_eq!(&bytes[0..2], &0i16.to_le_bytes()); // blockID=0
        assert_eq!(&bytes[2..4], &2i16.to_le_bytes()); // cardinality-1=2

        // SPARSE entries
        assert_eq!(&bytes[4..6], &10i16.to_le_bytes());
        assert_eq!(&bytes[6..8], &100i16.to_le_bytes());
        assert_eq!(&bytes[8..10], &1000i16.to_le_bytes());
    }

    #[test]
    fn test_all_block() {
        // All 65536 docs set — ALL encoding, no data bytes after header
        let doc_ids: Vec<i32> = (0..65536).collect();
        let (bytes, _) = write_and_get_bytes(&doc_ids, 65536);

        // Block header
        assert_eq!(&bytes[0..2], &0i16.to_le_bytes()); // blockID=0
        assert_eq!(&bytes[2..4], &(-1i16).to_le_bytes()); // cardinality-1=65535

        // Next should be sentinel block header (no data for ALL block)
        assert_eq!(&bytes[4..6], &32767i16.to_le_bytes()); // sentinel blockID
    }

    #[test]
    fn test_dense_block() {
        // 5000 docs set — above 4095 threshold, DENSE encoding
        let doc_ids: Vec<i32> = (0..5000).collect();
        let (bytes, _) = write_and_get_bytes(&doc_ids, 65536);

        // Block header
        assert_eq!(&bytes[0..2], &0i16.to_le_bytes()); // blockID=0
        assert_eq!(&bytes[2..4], &4999i16.to_le_bytes()); // cardinality-1=4999

        // Rank table: 128 entries of 2 bytes = 256 bytes
        // First rank entry should be 0 (no bits before position 0)
        assert_eq!(bytes[4], 0);
        assert_eq!(bytes[5], 0);

        // After rank table (256 bytes) comes bitmap (1024 longs = 8192 bytes)
        let bitmap_start = 4 + 256;
        // First long should have bits 0-63 all set
        let first_long =
            i64::from_le_bytes(bytes[bitmap_start..bitmap_start + 8].try_into().unwrap());
        assert_eq!(first_long, -1i64); // all 64 bits set

        // Total block size: 4 (header) + 256 (rank) + 8192 (bitmap) = 8452
        let sentinel_start = 8452;
        assert_eq!(
            &bytes[sentinel_start..sentinel_start + 2],
            &32767i16.to_le_bytes()
        );
    }

    #[test]
    fn test_dense_block_rank_values() {
        // Verify rank table cumulative popcount correctness
        // Set docs 0-511 (first rank entry = 0) and 512-1023 (second rank entry = 512)
        // Plus enough more to be DENSE (need > 4095)
        let doc_ids: Vec<i32> = (0..4100).collect();
        let (bytes, _) = write_and_get_bytes(&doc_ids, 65536);

        // Rank table starts at offset 4 (after header)
        // Entry 0: 0 bits before position 0
        assert_eq!(bytes[4], 0); // high byte
        assert_eq!(bytes[5], 0); // low byte

        // Entry 1: 512 bits in positions 0-511 (8 longs × 64 bits)
        let rank1 = ((bytes[6] as u16) << 8) | bytes[7] as u16;
        assert_eq!(rank1, 512);

        // Entry 2: 1024 bits
        let rank2 = ((bytes[8] as u16) << 8) | bytes[9] as u16;
        assert_eq!(rank2, 1024);
    }

    #[test]
    fn test_multi_block_with_gap() {
        // Docs in block 0 and block 2 (gap at block 1)
        let mut doc_ids: Vec<i32> = vec![0, 1, 2];
        doc_ids.extend(131072..131075); // block 2: docs 131072-131074
        let (bytes, entry_count) = write_and_get_bytes(&doc_ids, 200000);

        // Should have jump entries for blocks 0,1,2 + sentinel = 4
        // blockCount = 3+1 = 4 (last_block after block 2 = 3, +1 for sentinel)
        // Actually: after block 0, last_block=1. After block 2, last_block=3.
        // So blockCount = 3+1 = 4
        assert_eq!(entry_count, 4);

        // Block 0: SPARSE with 3 docs
        assert_eq!(&bytes[0..2], &0i16.to_le_bytes());
        assert_eq!(&bytes[2..4], &2i16.to_le_bytes()); // cardinality-1=2
    }

    #[test]
    fn test_sparse_dense_boundary() {
        // Exactly MAX_ARRAY_LENGTH (4095) docs — should be SPARSE
        let doc_ids: Vec<i32> = (0..4095).collect();
        let (bytes, _) = write_and_get_bytes(&doc_ids, 65536);
        let cardinality_minus_1 = i16::from_le_bytes(bytes[2..4].try_into().unwrap());
        assert_eq!(cardinality_minus_1, 4094);
        // Block data: 4095 shorts = 8190 bytes
        // Sentinel starts at 4 + 8190 = 8194
        assert_eq!(&bytes[8194..8196], &32767i16.to_le_bytes());

        // Exactly MAX_ARRAY_LENGTH + 1 (4096) docs — should be DENSE
        let doc_ids: Vec<i32> = (0..4096).collect();
        let (bytes, _) = write_and_get_bytes(&doc_ids, 65536);
        let cardinality_minus_1 = i16::from_le_bytes(bytes[2..4].try_into().unwrap());
        assert_eq!(cardinality_minus_1, 4095);
        // DENSE: 4 (header) + 256 (rank) + 8192 (bitmap) = 8452
        assert_eq!(&bytes[8452..8454], &32767i16.to_le_bytes());
    }

    #[test]
    fn test_jump_table_structure() {
        // Multiple blocks to verify jump table entries
        // Block 0: 3 docs, Block 1: 2 docs
        let mut doc_ids = vec![10, 20, 30];
        doc_ids.extend([65536 + 5, 65536 + 10]); // block 1
        let (bytes, entry_count) = write_and_get_bytes(&doc_ids, 200000);

        // blockCount = 2+1 = 3 (blocks 0,1 + sentinel)
        assert_eq!(entry_count, 3);

        // Find the jump table at the end
        // Block 0: 4 (header) + 6 (3 shorts) = 10 bytes
        // Block 1: 4 (header) + 4 (2 shorts) = 8 bytes
        // Sentinel: 4 (header) + 2 (1 short) = 6 bytes
        // Total blocks = 24 bytes
        // Jump table: 3 entries × 8 bytes = 24 bytes
        // Total = 48 bytes
        assert_eq!(bytes.len(), 48);

        // Jump entry 0: index=0, offset=0
        assert_eq!(i32::from_le_bytes(bytes[24..28].try_into().unwrap()), 0);
        assert_eq!(i32::from_le_bytes(bytes[28..32].try_into().unwrap()), 0);

        // Jump entry 1: index=3 (3 docs in block 0), offset=10
        assert_eq!(i32::from_le_bytes(bytes[32..36].try_into().unwrap()), 3);
        assert_eq!(i32::from_le_bytes(bytes[36..40].try_into().unwrap()), 10);

        // Jump entry 2 (sentinel): index=5 (3+2 docs), offset=18
        assert_eq!(i32::from_le_bytes(bytes[40..44].try_into().unwrap()), 5);
        assert_eq!(i32::from_le_bytes(bytes[44..48].try_into().unwrap()), 18);
    }

    #[test]
    fn test_single_block_jump_table_optimization() {
        // Single real block + sentinel = blockCount 2 → optimized to 0 entries
        let doc_ids = vec![0, 1, 2];
        let (_, entry_count) = write_and_get_bytes(&doc_ids, 100);
        assert_eq!(entry_count, 0);
    }

    #[test]
    fn test_docs_at_block_boundary() {
        // Doc at last position of block 0 and first position of block 1
        let doc_ids = vec![65535, 65536];
        let (bytes, _) = write_and_get_bytes(&doc_ids, 200000);

        // Block 0: blockID=0, cardinality=1
        assert_eq!(&bytes[0..2], &0i16.to_le_bytes());
        assert_eq!(&bytes[2..4], &0i16.to_le_bytes()); // cardinality-1=0

        // SPARSE: doc 65535 within block
        assert_eq!(&bytes[4..6], &(-1i16).to_le_bytes()); // 65535 as i16

        // Block 1: blockID=1, cardinality=1
        assert_eq!(&bytes[6..8], &1i16.to_le_bytes());
        assert_eq!(&bytes[8..10], &0i16.to_le_bytes()); // cardinality-1=0

        // SPARSE: doc 0 within block
        assert_eq!(&bytes[10..12], &0i16.to_le_bytes());
    }

    #[test]
    fn test_dense_bitmap_correctness() {
        // 4096 contiguous docs — DENSE encoding
        let all_docs: Vec<i32> = (0..4096).collect();
        let (bytes, _) = write_and_get_bytes(&all_docs, 65536);

        // DENSE block: header(4) + rank(256) + bitmap(8192)
        let bitmap_start = 4 + 256;

        // First long: bits 0-63 all set
        let word0 = i64::from_le_bytes(bytes[bitmap_start..bitmap_start + 8].try_into().unwrap());
        assert_eq!(word0, -1i64); // all 64 bits set

        // Long at index 64 (word 64 = docs 4096-4159): should be all zeros
        let word64_offset = bitmap_start + 64 * 8;
        let word64 =
            i64::from_le_bytes(bytes[word64_offset..word64_offset + 8].try_into().unwrap());
        assert_eq!(word64, 0);
    }
}
