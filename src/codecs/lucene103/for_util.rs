// Ported from org.apache.lucene.codecs.lucene103.ForUtil, PForUtil, ForDeltaUtil

use std::io;

use crate::store::DataOutput;
use crate::util::packed::bits_required;

pub const BLOCK_SIZE: usize = 128;

// --- ForUtil ---
// Frame-of-Reference bit packing for 128 integers.
// Ported from org.apache.lucene.codecs.lucene103.ForUtil

/// Returns the number of bytes needed to encode 128 values with the given bits per value.
pub fn num_bytes(bpv: u32) -> u32 {
    // bpv * 128 / 8 = bpv * 16
    bpv << 4
}

// MASKS arrays for remainder bit handling during encode.
// Ported from Java ForUtil static initializer.
const MASKS8: [i32; 8] = build_masks8();
const MASKS16: [i32; 16] = build_masks16();
const MASKS32: [i32; 32] = build_masks32();

const fn expand_mask8(mask8: i32) -> i32 {
    let m16 = mask8 | (mask8 << 8);
    m16 | (m16 << 16)
}

const fn expand_mask16(mask16: i32) -> i32 {
    mask16 | (mask16 << 16)
}

const fn build_masks8() -> [i32; 8] {
    let mut masks = [0i32; 8];
    let mut i = 0;
    while i < 8 {
        masks[i] = expand_mask8((1 << i) - 1);
        i += 1;
    }
    masks
}

const fn build_masks16() -> [i32; 16] {
    let mut masks = [0i32; 16];
    let mut i = 0;
    while i < 16 {
        masks[i] = expand_mask16((1 << i) - 1);
        i += 1;
    }
    masks
}

const fn build_masks32() -> [i32; 32] {
    let mut masks = [0i32; 32];
    let mut i = 0;
    while i < 31 {
        masks[i] = (1i32 << i) - 1;
        i += 1;
    }
    // i=31: (1<<31) overflows i32, use wrapping: 0x80000000 - 1 = 0x7FFFFFFF = -1 as i32
    masks[31] = -1;
    masks
}

/// Collapse 128 ints by interleaving 4 groups of 32, packing 4 values per int.
/// Ported from Java ForUtil.collapse8(int[]).
fn collapse8(ints: &mut [i32; BLOCK_SIZE]) {
    for i in 0..32 {
        ints[i] = (ints[i] << 24) | (ints[32 + i] << 16) | (ints[64 + i] << 8) | ints[96 + i];
    }
}

/// Collapse 128 ints by interleaving 2 groups of 64, packing 2 values per int.
/// Ported from Java ForUtil.collapse16(int[]).
fn collapse16(ints: &mut [i32; BLOCK_SIZE]) {
    for i in 0..64 {
        ints[i] = (ints[i] << 16) | ints[64 + i];
    }
}

/// Encode 128 longs with the given bits per value using FOR bit packing.
/// Uses ForUtil thresholds: bpv<=8 → collapse8, <=16 → collapse16, else → no collapse.
/// Ported from Java ForUtil.encode(int[], int, DataOutput).
pub fn encode(longs: &[i64; BLOCK_SIZE], bpv: u32, out: &mut dyn DataOutput) -> io::Result<()> {
    if bpv == 0 {
        return Ok(());
    }
    let mut ints = [0i32; BLOCK_SIZE];
    for i in 0..BLOCK_SIZE {
        ints[i] = longs[i] as i32;
    }
    let primitive_size = if bpv <= 8 {
        collapse8(&mut ints);
        8
    } else if bpv <= 16 {
        collapse16(&mut ints);
        16
    } else {
        32
    };
    encode_ints(&ints, bpv, primitive_size, out)
}

/// Core encode: 3-phase bit packing into tmp, then write as 32-bit ints.
/// Ported from Java ForUtil.encode(int[], int, int, DataOutput, int[]).
fn encode_ints(
    ints: &[i32; BLOCK_SIZE],
    bpv: u32,
    primitive_size: u32,
    out: &mut dyn DataOutput,
) -> io::Result<()> {
    let num_ints = (BLOCK_SIZE as u32) * primitive_size / 32;
    let num_ints_per_shift = bpv * 4;
    let mut tmp = [0i32; BLOCK_SIZE];

    // Phase 1: shift-and-OR groups of collapsed ints into tmp
    let mut idx: usize = 0;
    let mut shift = (primitive_size - bpv) as i32;
    for t in &mut tmp[..num_ints_per_shift as usize] {
        *t = ints[idx] << shift;
        idx += 1;
    }
    shift -= bpv as i32;
    while shift >= 0 {
        for t in &mut tmp[..num_ints_per_shift as usize] {
            *t |= ints[idx] << shift;
            idx += 1;
        }
        shift -= bpv as i32;
    }

    // Phase 2: handle remainder bits for values spanning int boundaries
    let remaining_bits_per_int = (shift + bpv as i32) as u32;
    let mask_remaining = match primitive_size {
        8 => MASKS8[remaining_bits_per_int as usize],
        16 => MASKS16[remaining_bits_per_int as usize],
        _ => MASKS32[remaining_bits_per_int as usize],
    };

    let mut tmp_idx: usize = 0;
    let mut remaining_bits_per_value = bpv;
    while idx < num_ints as usize {
        if remaining_bits_per_value >= remaining_bits_per_int {
            remaining_bits_per_value -= remaining_bits_per_int;
            tmp[tmp_idx] |= (ints[idx] >> remaining_bits_per_value as i32) & mask_remaining;
            tmp_idx += 1;
            if remaining_bits_per_value == 0 {
                idx += 1;
                remaining_bits_per_value = bpv;
            }
        } else {
            let (mask1, mask2) = match primitive_size {
                8 => (
                    MASKS8[remaining_bits_per_value as usize],
                    MASKS8[(remaining_bits_per_int - remaining_bits_per_value) as usize],
                ),
                16 => (
                    MASKS16[remaining_bits_per_value as usize],
                    MASKS16[(remaining_bits_per_int - remaining_bits_per_value) as usize],
                ),
                _ => (
                    MASKS32[remaining_bits_per_value as usize],
                    MASKS32[(remaining_bits_per_int - remaining_bits_per_value) as usize],
                ),
            };
            tmp[tmp_idx] |=
                (ints[idx] & mask1) << (remaining_bits_per_int - remaining_bits_per_value) as i32;
            idx += 1;
            remaining_bits_per_value += bpv - remaining_bits_per_int;
            tmp[tmp_idx] |= (ints[idx] >> remaining_bits_per_value as i32) & mask2;
            tmp_idx += 1;
        }
    }

    // Phase 3: write output as 32-bit ints
    for &val in &tmp[..num_ints_per_shift as usize] {
        out.write_le_int(val)?;
    }

    Ok(())
}

// --- PForUtil ---
// Patched Frame-of-Reference with up to 7 exceptions.
// Ported from org.apache.lucene.codecs.lucene103.PForUtil

pub const MAX_EXCEPTIONS: usize = 7;

/// Encode 128 values using PForUtil (patched FOR).
/// Ported from org.apache.lucene.codecs.lucene103.PForUtil.encode.
///
/// Uses a min-heap to find the top MAX_EXCEPTIONS+1 values. The heap minimum
/// determines the patched bpv. Exceptions (values exceeding the patched mask)
/// are stored as (index, highBits) byte pairs after the FOR-encoded base values.
pub fn pfor_encode(longs: &mut [i64; BLOCK_SIZE], out: &mut dyn DataOutput) -> io::Result<()> {
    // Find the top MAX_EXCEPTIONS+1 values using a min-heap approach.
    // We maintain a sorted array of size MAX_EXCEPTIONS+1 with the minimum at [0].
    let mut top = [0u64; MAX_EXCEPTIONS + 1];
    let mut top_count = 0;

    // Seed with first MAX_EXCEPTIONS+1 values
    for &v in &longs[..=(MAX_EXCEPTIONS.min(BLOCK_SIZE - 1))] {
        top[top_count] = v as u64;
        top_count += 1;
    }
    top[..top_count].sort_unstable();
    let mut top_value = top[0]; // min of top values

    // Process remaining values
    for &v in &longs[(MAX_EXCEPTIONS + 1)..BLOCK_SIZE] {
        let v = v as u64;
        if v > top_value {
            top[0] = v;
            top[..top_count].sort_unstable();
            top_value = top[0];
        }
    }

    // Find max of all values
    let mut max: u64 = 0;
    for &t in &top[..top_count] {
        max = max.max(t);
    }

    let max_bits_required = bits_required(max);

    // Compute patched bits: reduce by at most 8 (patch stored as byte)
    let patched_bits_required = bits_required(top_value).max(max_bits_required.saturating_sub(8));

    // Count exceptions: heap entries (excluding root/min) that exceed the patched mask
    let max_unpatched_value = if patched_bits_required >= 64 {
        u64::MAX
    } else {
        (1u64 << patched_bits_required) - 1
    };
    let mut num_exceptions: u32 = 0;
    // In the heap, index 0 is the min (topValue). Count entries 1..top_count that are exceptions.
    for &t in &top[1..top_count] {
        if t > max_unpatched_value {
            num_exceptions += 1;
        }
    }

    // Collect exception pairs and mask values
    let mut exceptions = [(0u8, 0u8); MAX_EXCEPTIONS];
    let mut exception_count: usize = 0;
    if num_exceptions > 0 {
        for (i, v) in longs.iter_mut().enumerate() {
            let val = *v as u64;
            if val > max_unpatched_value {
                exceptions[exception_count] = (i as u8, (val >> patched_bits_required) as u8);
                *v = (val & max_unpatched_value) as i64;
                exception_count += 1;
            }
        }
        debug_assert_eq!(exception_count, num_exceptions as usize);
    }

    // Check all-equal (after masking) with maxBitsRequired <= 8 — matches Java's path
    let all_equal = longs.iter().all(|&v| v == longs[0]);
    if all_equal && max_bits_required <= 8 {
        // Shift exception high bits left by patchedBitsRequired (Java line 94)
        for exc in exceptions[..exception_count].iter_mut() {
            exc.1 = ((exc.1 as u64) << patched_bits_required) as u8;
        }
        let token = (num_exceptions as u8) << 5; // bpv = 0
        out.write_byte(token)?;
        out.write_vint(longs[0] as i32)?;
    } else {
        let token = ((num_exceptions as u8) << 5) | (patched_bits_required as u8);
        out.write_byte(token)?;
        encode(longs, patched_bits_required, out)?;
    }

    // Write exception patches as (index, highBits) pairs
    for &(idx, high_bits) in &exceptions[..exception_count] {
        out.write_byte(idx)?;
        out.write_byte(high_bits)?;
    }

    Ok(())
}

// --- ForDeltaUtil ---
// Delta encoding for doc IDs.
// Ported from org.apache.lucene.codecs.lucene103.ForDeltaUtil

/// Returns the bits required to encode the deltas of the given values.
pub fn delta_bits_required(longs: &[i64; BLOCK_SIZE]) -> u32 {
    // Compute deltas and find max
    let mut max_delta: u64 = 0;
    let mut prev = 0i64;
    for &v in longs.iter() {
        let delta = v - prev;
        max_delta = max_delta.max(delta as u64);
        prev = v;
    }
    bits_required(max_delta)
}

/// Encode doc ID deltas with the given bits per value.
/// First computes deltas, picks primitive size, encodes with ForUtil.
pub fn encode_deltas(
    bpv: u32,
    longs: &[i64; BLOCK_SIZE],
    out: &mut dyn DataOutput,
) -> io::Result<()> {
    // Compute deltas in-place (into a copy)
    let mut deltas = [0i64; BLOCK_SIZE];
    let mut prev = 0i64;
    for (i, &v) in longs.iter().enumerate() {
        deltas[i] = v - prev;
        prev = v;
    }

    encode(&deltas, bpv, out)
}

// --- ForDeltaUtil functions for block postings encoding ---
// These use different collapse thresholds from ForUtil.
// Ported from org.apache.lucene.codecs.lucene103.ForDeltaUtil

/// Returns the bits required to encode the given raw delta values (OR-based).
/// Unlike `delta_bits_required` which takes cumulative values, this takes raw deltas directly.
/// Ported from Java ForDeltaUtil.bitsRequired() (line 91).
pub fn for_delta_bits_required(deltas: &[i32; BLOCK_SIZE]) -> u32 {
    let mut or = 0i32;
    for &d in deltas.iter() {
        or |= d;
    }
    bits_required(or as u64)
}

/// Encode raw deltas using ForDelta thresholds:
/// bpv<=3→collapse8, <=10→collapse16, else→no collapse.
/// Ported from Java ForDeltaUtil.encodeDeltas() (line 105).
pub fn for_delta_encode(
    bpv: u32,
    deltas: &[i32; BLOCK_SIZE],
    out: &mut dyn DataOutput,
) -> io::Result<()> {
    let mut ints = [0i32; BLOCK_SIZE];
    ints.copy_from_slice(deltas);

    let primitive_size = if bpv <= 3 {
        collapse8(&mut ints);
        8
    } else if bpv <= 10 {
        collapse16(&mut ints);
        16
    } else {
        32
    };
    encode_ints(&ints, bpv, primitive_size, out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryIndexOutput;

    #[test]
    fn test_for_util_num_bytes() {
        assert_eq!(num_bytes(0), 0);
        assert_eq!(num_bytes(1), 16);
        assert_eq!(num_bytes(8), 128);
        assert_eq!(num_bytes(16), 256);
        assert_eq!(num_bytes(32), 512);
    }

    #[test]
    fn test_for_util_encode_all_zeros() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let longs = [0i64; BLOCK_SIZE];
        encode(&longs, 0, &mut out).unwrap();
        assert_eq!(out.bytes().len(), 0);
    }

    #[test]
    fn test_for_util_encode_1bit() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, long) in longs.iter_mut().enumerate() {
            *long = (i & 1) as i64;
        }
        encode(&longs, 1, &mut out).unwrap();
        assert_eq!(out.bytes().len(), 16); // 1 bit * 128 / 8 = 16 bytes
    }

    #[test]
    fn test_for_util_encode_8bit() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, long) in longs.iter_mut().enumerate() {
            *long = i as i64;
        }
        encode(&longs, 8, &mut out).unwrap();
        assert_eq!(out.bytes().len(), 128); // 8 bits * 128 / 8 = 128 bytes
    }

    #[test]
    fn test_pfor_all_equal() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [42i64; BLOCK_SIZE];
        pfor_encode(&mut longs, &mut out).unwrap();
        let bytes = out.bytes();
        // Token byte with bpv=0, then VLong(42)
        assert_eq!(bytes[0], 0); // token: numExceptions=0, bpv=0
        assert_eq!(bytes[1], 42); // VLong(42)
    }

    #[test]
    fn test_pfor_small_values() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, long) in longs.iter_mut().enumerate() {
            *long = (i % 4) as i64; // values 0-3, need 2 bits
        }
        pfor_encode(&mut longs, &mut out).unwrap();
        let bytes = out.bytes();
        // Token byte: no exceptions, bpv=2
        let token = bytes[0];
        let num_exceptions = token >> 5;
        let bpv = token & 0x1F;
        assert_eq!(num_exceptions, 0);
        assert_eq!(bpv, 2);
        // Data: 2 bits * 128 / 8 = 32 bytes
        assert_eq!(bytes.len(), 1 + 32);
    }

    #[test]
    fn test_delta_bits_required() {
        let mut longs = [0i64; BLOCK_SIZE];
        // Sequential: deltas are all 1
        for (i, long) in longs.iter_mut().enumerate() {
            *long = i as i64;
        }
        assert_eq!(delta_bits_required(&longs), 1);

        // All same: deltas are all 0 (except first)
        let mut longs2 = [5i64; BLOCK_SIZE];
        longs2[0] = 5;
        // First delta is 5, rest are 0
        assert_eq!(delta_bits_required(&longs2), 3);
    }

    #[test]
    fn test_encode_deltas() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, long) in longs.iter_mut().enumerate() {
            *long = i as i64;
        }
        // Deltas are all 1 (except first which is 0), needs 1 bit
        encode_deltas(1, &longs, &mut out).unwrap();
        assert_eq!(out.bytes().len(), 16); // 1 bit * 128 / 8 = 16 bytes
    }

    #[test]
    fn test_for_delta_bits_required() {
        // All deltas = 1 → OR = 1 → 1 bit
        let mut deltas = [1i32; BLOCK_SIZE];
        assert_eq!(for_delta_bits_required(&deltas), 1);

        // Deltas with max = 7 → OR includes 7 → 3 bits
        deltas[0] = 7;
        assert_eq!(for_delta_bits_required(&deltas), 3);

        // Deltas with max = 255 → 8 bits
        let mut deltas2 = [1i32; BLOCK_SIZE];
        deltas2[0] = 255;
        assert_eq!(for_delta_bits_required(&deltas2), 8);

        // Deltas with max = 1023 → 10 bits
        let mut deltas3 = [1i32; BLOCK_SIZE];
        deltas3[0] = 1023;
        assert_eq!(for_delta_bits_required(&deltas3), 10);
    }

    #[test]
    fn test_for_delta_encode_collapse8() {
        // bpv <= 3 uses collapse8 (primitive_size=8)
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut deltas = [1i32; BLOCK_SIZE];
        deltas[0] = 3; // max delta = 3, bpv = 2
        let bpv = for_delta_bits_required(&deltas);
        assert_eq!(bpv, 2);
        for_delta_encode(bpv, &deltas, &mut out).unwrap();
        assert_eq!(out.bytes().len(), (bpv as usize) * 16); // bpv * 16 bytes
    }

    #[test]
    fn test_for_delta_encode_collapse16() {
        // bpv 4-10 uses collapse16 (primitive_size=16)
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut deltas = [1i32; BLOCK_SIZE];
        deltas[0] = 512; // bits_required(512|1) = 10
        let bpv = for_delta_bits_required(&deltas);
        assert!((4..=10).contains(&bpv), "bpv={bpv} should use collapse16");
        for_delta_encode(bpv, &deltas, &mut out).unwrap();
        assert_eq!(out.bytes().len(), (bpv as usize) * 16);
    }

    #[test]
    fn test_for_delta_encode_collapse32() {
        // bpv > 10 uses collapse32 (primitive_size=32)
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut deltas = [1i32; BLOCK_SIZE];
        deltas[0] = 2048; // bits_required(2048|1) = 12
        let bpv = for_delta_bits_required(&deltas);
        assert!(bpv > 10, "bpv={bpv} should use collapse32");
        for_delta_encode(bpv, &deltas, &mut out).unwrap();
        assert_eq!(out.bytes().len(), (bpv as usize) * 16);
    }

    // --- Byte-exact cross-validation tests against Java Lucene 10.3.2 ForUtil ---
    // Expected bytes generated by ForUtilDump.java using reflection on the
    // package-private ForUtil/ForDeltaUtil/PForUtil classes.

    #[test]
    fn test_for_util_bpv1_bytes_match_java() {
        // Ported from org.apache.lucene.codecs.lucene103.TestForUtil
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, v) in longs.iter_mut().enumerate() {
            *v = (i & 1) as i64;
        }
        encode(&longs, 1, &mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
            0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_for_util_bpv3_bytes_match_java() {
        // Ported from org.apache.lucene.codecs.lucene103.TestForUtil
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, v) in longs.iter_mut().enumerate() {
            *v = (i % 8) as i64;
        }
        encode(&longs, 3, &mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x10, 0x10, 0x10, 0x10, 0x34, 0x34, 0x34, 0x34,
            0x59, 0x59, 0x59, 0x59, 0x7D, 0x7D, 0x7D, 0x7D,
            0x80, 0x80, 0x80, 0x80, 0xA7, 0xA7, 0xA7, 0xA7,
            0xCA, 0xCA, 0xCA, 0xCA, 0xED, 0xED, 0xED, 0xED,
            0x11, 0x11, 0x11, 0x11, 0x37, 0x37, 0x37, 0x37,
            0x59, 0x59, 0x59, 0x59, 0x7F, 0x7F, 0x7F, 0x7F,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_for_util_bpv8_bytes_match_java() {
        // Ported from org.apache.lucene.codecs.lucene103.TestForUtil
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, v) in longs.iter_mut().enumerate() {
            *v = i as i64;
        }
        encode(&longs, 8, &mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x60, 0x40, 0x20, 0x00, 0x61, 0x41, 0x21, 0x01,
            0x62, 0x42, 0x22, 0x02, 0x63, 0x43, 0x23, 0x03,
            0x64, 0x44, 0x24, 0x04, 0x65, 0x45, 0x25, 0x05,
            0x66, 0x46, 0x26, 0x06, 0x67, 0x47, 0x27, 0x07,
            0x68, 0x48, 0x28, 0x08, 0x69, 0x49, 0x29, 0x09,
            0x6A, 0x4A, 0x2A, 0x0A, 0x6B, 0x4B, 0x2B, 0x0B,
            0x6C, 0x4C, 0x2C, 0x0C, 0x6D, 0x4D, 0x2D, 0x0D,
            0x6E, 0x4E, 0x2E, 0x0E, 0x6F, 0x4F, 0x2F, 0x0F,
            0x70, 0x50, 0x30, 0x10, 0x71, 0x51, 0x31, 0x11,
            0x72, 0x52, 0x32, 0x12, 0x73, 0x53, 0x33, 0x13,
            0x74, 0x54, 0x34, 0x14, 0x75, 0x55, 0x35, 0x15,
            0x76, 0x56, 0x36, 0x16, 0x77, 0x57, 0x37, 0x17,
            0x78, 0x58, 0x38, 0x18, 0x79, 0x59, 0x39, 0x19,
            0x7A, 0x5A, 0x3A, 0x1A, 0x7B, 0x5B, 0x3B, 0x1B,
            0x7C, 0x5C, 0x3C, 0x1C, 0x7D, 0x5D, 0x3D, 0x1D,
            0x7E, 0x5E, 0x3E, 0x1E, 0x7F, 0x5F, 0x3F, 0x1F,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_for_util_bpv10_bytes_match_java() {
        // Ported from org.apache.lucene.codecs.lucene103.TestForUtil
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, v) in longs.iter_mut().enumerate() {
            *v = (i * 8) as i64;
        }
        encode(&longs, 10, &mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x34, 0x80, 0x14, 0x00, 0x03, 0x82, 0x01, 0x02,
            0x12, 0x84, 0x12, 0x04, 0x0D, 0x86, 0x05, 0x06,
            0x10, 0x88, 0x10, 0x08, 0x35, 0x8A, 0x15, 0x0A,
            0x23, 0x8C, 0x21, 0x0C, 0x18, 0x8E, 0x18, 0x0E,
            0x0D, 0x90, 0x05, 0x10, 0x28, 0x92, 0x28, 0x12,
            0x37, 0x94, 0x17, 0x14, 0x03, 0x96, 0x01, 0x16,
            0x1E, 0x98, 0x1E, 0x18, 0x0E, 0x9A, 0x06, 0x1A,
            0x00, 0x9C, 0x00, 0x1C, 0x38, 0x9E, 0x18, 0x1E,
            0x23, 0xA0, 0x21, 0x20, 0x24, 0xA2, 0x24, 0x22,
            0x0E, 0xA4, 0x06, 0x24, 0x18, 0xA6, 0x18, 0x26,
            0x3A, 0xA8, 0x1A, 0x28, 0x03, 0xAA, 0x01, 0x2A,
            0x2A, 0xAC, 0x2A, 0x2C, 0x0E, 0xAE, 0x06, 0x2E,
            0x30, 0xB0, 0x30, 0x30, 0x3B, 0xB2, 0x1B, 0x32,
            0x23, 0xB4, 0x21, 0x34, 0x30, 0xB6, 0x30, 0x36,
            0x0F, 0xB8, 0x07, 0x38, 0x08, 0xBA, 0x08, 0x3A,
            0x3D, 0xBC, 0x1D, 0x3C, 0x03, 0xBE, 0x01, 0x3E,
            0x36, 0xC0, 0x36, 0x40, 0x0F, 0xC2, 0x07, 0x42,
            0x20, 0xC4, 0x20, 0x44, 0x3E, 0xC6, 0x1E, 0x46,
            0x23, 0xC8, 0x21, 0x48, 0x3C, 0xCA, 0x3C, 0x4A,
            0x0F, 0xCC, 0x07, 0x4C, 0x38, 0xCE, 0x38, 0x4E,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_for_util_bpv16_bytes_match_java() {
        // Ported from org.apache.lucene.codecs.lucene103.TestForUtil
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, v) in longs.iter_mut().enumerate() {
            *v = (i * 512) as i64;
        }
        encode(&longs, 16, &mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x00, 0x80, 0x00, 0x00, 0x00, 0x82, 0x00, 0x02,
            0x00, 0x84, 0x00, 0x04, 0x00, 0x86, 0x00, 0x06,
            0x00, 0x88, 0x00, 0x08, 0x00, 0x8A, 0x00, 0x0A,
            0x00, 0x8C, 0x00, 0x0C, 0x00, 0x8E, 0x00, 0x0E,
            0x00, 0x90, 0x00, 0x10, 0x00, 0x92, 0x00, 0x12,
            0x00, 0x94, 0x00, 0x14, 0x00, 0x96, 0x00, 0x16,
            0x00, 0x98, 0x00, 0x18, 0x00, 0x9A, 0x00, 0x1A,
            0x00, 0x9C, 0x00, 0x1C, 0x00, 0x9E, 0x00, 0x1E,
            0x00, 0xA0, 0x00, 0x20, 0x00, 0xA2, 0x00, 0x22,
            0x00, 0xA4, 0x00, 0x24, 0x00, 0xA6, 0x00, 0x26,
            0x00, 0xA8, 0x00, 0x28, 0x00, 0xAA, 0x00, 0x2A,
            0x00, 0xAC, 0x00, 0x2C, 0x00, 0xAE, 0x00, 0x2E,
            0x00, 0xB0, 0x00, 0x30, 0x00, 0xB2, 0x00, 0x32,
            0x00, 0xB4, 0x00, 0x34, 0x00, 0xB6, 0x00, 0x36,
            0x00, 0xB8, 0x00, 0x38, 0x00, 0xBA, 0x00, 0x3A,
            0x00, 0xBC, 0x00, 0x3C, 0x00, 0xBE, 0x00, 0x3E,
            0x00, 0xC0, 0x00, 0x40, 0x00, 0xC2, 0x00, 0x42,
            0x00, 0xC4, 0x00, 0x44, 0x00, 0xC6, 0x00, 0x46,
            0x00, 0xC8, 0x00, 0x48, 0x00, 0xCA, 0x00, 0x4A,
            0x00, 0xCC, 0x00, 0x4C, 0x00, 0xCE, 0x00, 0x4E,
            0x00, 0xD0, 0x00, 0x50, 0x00, 0xD2, 0x00, 0x52,
            0x00, 0xD4, 0x00, 0x54, 0x00, 0xD6, 0x00, 0x56,
            0x00, 0xD8, 0x00, 0x58, 0x00, 0xDA, 0x00, 0x5A,
            0x00, 0xDC, 0x00, 0x5C, 0x00, 0xDE, 0x00, 0x5E,
            0x00, 0xE0, 0x00, 0x60, 0x00, 0xE2, 0x00, 0x62,
            0x00, 0xE4, 0x00, 0x64, 0x00, 0xE6, 0x00, 0x66,
            0x00, 0xE8, 0x00, 0x68, 0x00, 0xEA, 0x00, 0x6A,
            0x00, 0xEC, 0x00, 0x6C, 0x00, 0xEE, 0x00, 0x6E,
            0x00, 0xF0, 0x00, 0x70, 0x00, 0xF2, 0x00, 0x72,
            0x00, 0xF4, 0x00, 0x74, 0x00, 0xF6, 0x00, 0x76,
            0x00, 0xF8, 0x00, 0x78, 0x00, 0xFA, 0x00, 0x7A,
            0x00, 0xFC, 0x00, 0x7C, 0x00, 0xFE, 0x00, 0x7E,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_for_util_bpv20_bytes_match_java() {
        // Ported from org.apache.lucene.codecs.lucene103.TestForUtil
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, v) in longs.iter_mut().enumerate() {
            *v = (i * 8192) as i64;
        }
        encode(&longs, 20, &mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x00, 0x0A, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x02,
            0x00, 0x02, 0x00, 0x04, 0xA4, 0x00, 0x00, 0x06,
            0x00, 0x00, 0x00, 0x08, 0x60, 0x0A, 0x00, 0x0A,
            0x0A, 0x00, 0x00, 0x0C, 0x00, 0x08, 0x00, 0x0E,
            0xAA, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x12,
            0xC0, 0x0A, 0x00, 0x14, 0x0A, 0x00, 0x00, 0x16,
            0x00, 0x0E, 0x00, 0x18, 0xB0, 0x00, 0x00, 0x1A,
            0x00, 0x00, 0x00, 0x1C, 0x20, 0x0B, 0x00, 0x1E,
            0x0B, 0x00, 0x00, 0x20, 0x00, 0x04, 0x00, 0x22,
            0xB6, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00, 0x26,
            0x80, 0x0B, 0x00, 0x28, 0x0B, 0x00, 0x00, 0x2A,
            0x00, 0x0A, 0x00, 0x2C, 0xBC, 0x00, 0x00, 0x2E,
            0x00, 0x00, 0x00, 0x30, 0xE0, 0x0B, 0x00, 0x32,
            0x0C, 0x00, 0x00, 0x34, 0x00, 0x00, 0x00, 0x36,
            0xC2, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00, 0x3A,
            0x40, 0x0C, 0x00, 0x3C, 0x0C, 0x00, 0x00, 0x3E,
            0x00, 0x06, 0x00, 0x40, 0xC8, 0x00, 0x00, 0x42,
            0x00, 0x00, 0x00, 0x44, 0xA0, 0x0C, 0x00, 0x46,
            0x0C, 0x00, 0x00, 0x48, 0x00, 0x0C, 0x00, 0x4A,
            0xCE, 0x00, 0x00, 0x4C, 0x00, 0x00, 0x00, 0x4E,
            0x00, 0x0D, 0x00, 0x50, 0x0D, 0x00, 0x00, 0x52,
            0x00, 0x02, 0x00, 0x54, 0xD4, 0x00, 0x00, 0x56,
            0x00, 0x00, 0x00, 0x58, 0x60, 0x0D, 0x00, 0x5A,
            0x0D, 0x00, 0x00, 0x5C, 0x00, 0x08, 0x00, 0x5E,
            0xDA, 0x00, 0x00, 0x60, 0x00, 0x00, 0x00, 0x62,
            0xC0, 0x0D, 0x00, 0x64, 0x0D, 0x00, 0x00, 0x66,
            0x00, 0x0E, 0x00, 0x68, 0xE0, 0x00, 0x00, 0x6A,
            0x00, 0x00, 0x00, 0x6C, 0x20, 0x0E, 0x00, 0x6E,
            0x0E, 0x00, 0x00, 0x70, 0x00, 0x04, 0x00, 0x72,
            0xE6, 0x00, 0x00, 0x74, 0x00, 0x00, 0x00, 0x76,
            0x80, 0x0E, 0x00, 0x78, 0x0E, 0x00, 0x00, 0x7A,
            0x00, 0x0A, 0x00, 0x7C, 0xEC, 0x00, 0x00, 0x7E,
            0x00, 0x00, 0x00, 0x80, 0xE0, 0x0E, 0x00, 0x82,
            0x0F, 0x00, 0x00, 0x84, 0x00, 0x00, 0x00, 0x86,
            0xF2, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00, 0x8A,
            0x40, 0x0F, 0x00, 0x8C, 0x0F, 0x00, 0x00, 0x8E,
            0x00, 0x06, 0x00, 0x90, 0xF8, 0x00, 0x00, 0x92,
            0x00, 0x00, 0x00, 0x94, 0xA0, 0x0F, 0x00, 0x96,
            0x0F, 0x00, 0x00, 0x98, 0x00, 0x0C, 0x00, 0x9A,
            0xFE, 0x00, 0x00, 0x9C, 0x00, 0x00, 0x00, 0x9E,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_for_delta_bpv2_bytes_match_java() {
        // Ported from org.apache.lucene.codecs.lucene103.ForDeltaUtil
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut deltas = [1i32; BLOCK_SIZE];
        deltas[0] = 3;
        for_delta_encode(2, &deltas, &mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x55, 0x55, 0x55, 0xD5, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_for_delta_bpv5_bytes_match_java() {
        // Ported from org.apache.lucene.codecs.lucene103.ForDeltaUtil
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut deltas = [1i32; BLOCK_SIZE];
        deltas[0] = 31;
        for_delta_encode(5, &deltas, &mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x42, 0x08, 0x42, 0xF8, 0x42, 0x08, 0x42, 0x08,
            0x42, 0x08, 0x42, 0x08, 0x42, 0x08, 0x42, 0x08,
            0x43, 0x08, 0x43, 0x08, 0x42, 0x08, 0x42, 0x08,
            0x42, 0x08, 0x42, 0x08, 0x42, 0x08, 0x42, 0x08,
            0x42, 0x08, 0x42, 0x08, 0x43, 0x08, 0x43, 0x08,
            0x42, 0x08, 0x42, 0x08, 0x42, 0x08, 0x42, 0x08,
            0x42, 0x08, 0x42, 0x08, 0x42, 0x08, 0x42, 0x08,
            0x43, 0x08, 0x43, 0x08, 0x42, 0x08, 0x42, 0x08,
            0x42, 0x08, 0x42, 0x08, 0x42, 0x08, 0x42, 0x08,
            0x42, 0x08, 0x42, 0x08, 0x43, 0x08, 0x43, 0x08,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_pfor_exceptions_bytes_match_java() {
        // Ported from org.apache.lucene.codecs.lucene103.PForUtil
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut longs = [0i64; BLOCK_SIZE];
        for (i, v) in longs.iter_mut().enumerate() {
            *v = (i % 4) as i64;
        }
        longs[10] = 500;
        longs[50] = 1000;
        longs[100] = 2000;
        pfor_encode(&mut longs, &mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x63, 0x00, 0x00, 0x00, 0x00, 0x24, 0x24, 0x24,
            0x24, 0x49, 0x49, 0x49, 0x49, 0x6D, 0x6D, 0x6D,
            0x6D, 0x00, 0x00, 0x00, 0x00, 0x27, 0x27, 0x27,
            0x27, 0x48, 0x48, 0x40, 0x48, 0x6C, 0x6C, 0x6C,
            0x6C, 0x01, 0x01, 0x01, 0x01, 0x25, 0x25, 0x25,
            0x25, 0x48, 0x48, 0x48, 0x88, 0x6F, 0x6F, 0x6F,
            0x6F, 0x0A, 0x3E, 0x32, 0x7D, 0x64, 0xFA,
        ];
        assert_eq!(out.bytes(), expected);
    }
}
