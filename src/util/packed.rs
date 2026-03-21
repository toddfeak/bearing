// SPDX-License-Identifier: Apache-2.0
//! Packed integer writers for compact storage of fixed-width values.

use std::io;

use crate::store::{DataOutput, IndexOutput};

/// Supported bits-per-value for DirectWriter encoding.
const SUPPORTED_BITS_PER_VALUE: &[u32] = &[1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64];

/// Returns the number of bits required to represent values up to and including `max_value`,
/// interpreted as unsigned, rounded up to the nearest supported BPV.
pub fn unsigned_bits_required(max_value: i64) -> u32 {
    // PackedInts.unsignedBitsRequired: max(1, 64 - Long.numberOfLeadingZeros(bits))
    let raw = if max_value == 0 {
        1
    } else {
        64 - (max_value as u64).leading_zeros()
    };
    // Round up to nearest supported BPV
    *SUPPORTED_BITS_PER_VALUE
        .iter()
        .find(|&&s| s >= raw)
        .expect("raw bits exceeds max supported BPV")
}

/// Writes bit-packed integers.
pub struct DirectWriter {
    bits_per_value: u32,
    values: Vec<i64>,
}

impl DirectWriter {
    /// Creates a new writer with the given bits-per-value.
    pub fn new(bits_per_value: u32) -> Self {
        Self {
            bits_per_value,
            values: Vec::new(),
        }
    }

    /// Adds a value to the writer.
    pub fn add(&mut self, value: i64) {
        self.values.push(value);
    }

    /// Writes all accumulated values as bit-packed data, then padding bytes.
    pub fn finish(&self, output: &mut dyn DataOutput) -> io::Result<()> {
        if self.bits_per_value == 0 {
            return Ok(());
        }

        let bpv = self.bits_per_value;
        let up_to = self.values.len();

        if (bpv & 7) == 0 {
            // bpv is a multiple of 8: 8, 16, 24, 32, 40, 48, 56, 64
            let bytes_per_value = (bpv / 8) as usize;
            for i in 0..up_to {
                let v = self.values[i] as u64;
                // Write LE bytes, only bytes_per_value worth
                let le = v.to_le_bytes();
                output.write_bytes(&le[..bytes_per_value])?;
            }
        } else if bpv < 8 {
            // bpv is 1, 2, or 4: pack values LSB-first into LE longs
            let values_per_long = (64 / bpv) as usize;
            let mut i = 0;
            while i < up_to {
                let mut packed: u64 = 0;
                for j in 0..values_per_long {
                    if i + j < up_to {
                        packed |= (self.values[i + j] as u64) << (bpv * j as u32);
                    }
                }
                let remaining = (up_to - i).min(values_per_long);
                let bytes_needed = (remaining * bpv as usize).div_ceil(8);
                output.write_bytes(&packed.to_le_bytes()[..bytes_needed])?;
                i += values_per_long;
            }
        } else {
            // bpv is 12, 20, or 28: write pairs LSB-first
            let num_bytes_for_2 = (bpv * 2 / 8) as usize;
            let mut i = 0;
            while i < up_to {
                let l1 = self.values[i] as u64;
                let l2 = if i + 1 < up_to {
                    self.values[i + 1] as u64
                } else {
                    0
                };
                let merged = l1 | (l2 << bpv);
                if bpv <= 16 {
                    // 12-bit: write as LE u32 (3 bytes)
                    let le = (merged as u32).to_le_bytes();
                    output.write_bytes(&le[..num_bytes_for_2])?;
                } else {
                    // 20, 28: write as LE u64 (5 or 7 bytes)
                    let le = merged.to_le_bytes();
                    output.write_bytes(&le[..num_bytes_for_2])?;
                }
                i += 2;
            }
        }

        // Add padding bytes for fast I/O reads
        let padding_bits = if bpv > 32 {
            64 - bpv
        } else if bpv > 16 {
            32 - bpv
        } else if bpv > 8 {
            16 - bpv
        } else {
            0
        };
        let padding_bytes = padding_bits.div_ceil(8);
        for _ in 0..padding_bytes {
            output.write_byte(0)?;
        }

        Ok(())
    }
}

/// Writes monotonically-increasing sequences of longs with delta compression.
///
/// Data is split into blocks. For each block:
/// - Meta: min value (VLong), avg increment (Int as float bits), offset (VLong), bits per value (Byte)
/// - Data: bit-packed deltas from expected linear values
pub struct DirectMonotonicWriter {
    block_shift: u32,
    values: Vec<i64>,
}

impl DirectMonotonicWriter {
    /// Creates a new writer with the given block shift.
    pub fn new(block_shift: u32) -> Self {
        Self {
            block_shift,
            values: Vec::new(),
        }
    }

    /// Adds a value to the writer.
    pub fn add(&mut self, value: i64) {
        self.values.push(value);
    }

    /// Writes metadata to meta_output and data to data_output.
    /// Returns the number of blocks written.
    pub fn finish(
        &self,
        meta_output: &mut dyn IndexOutput,
        data_output: &mut dyn IndexOutput,
    ) -> io::Result<u32> {
        let block_size = 1usize << self.block_shift;
        let num_blocks = self.values.len().div_ceil(block_size);
        let base_data_pointer = data_output.file_pointer() as i64;

        for block_idx in 0..num_blocks {
            let start = block_idx * block_size;
            let end = (start + block_size).min(self.values.len());
            let count = end - start;

            // Copy block values so we can mutate them (matching Java's in-place buffer)
            let mut buffer: Vec<i64> = self.values[start..end].to_vec();

            let avg_inc = (buffer[count - 1] - buffer[0]) as f64 / (count - 1).max(1) as f64;
            let avg_inc_f = avg_inc as f32;

            // Subtract expected linear progression
            let mut min = i64::MAX;
            for (i, val) in buffer.iter_mut().enumerate().take(count) {
                let expected = (avg_inc_f as f64 * i as f64) as i64;
                *val -= expected;
                min = min.min(*val);
            }

            // Shift to non-negative and compute max delta using |=
            let mut max_delta: i64 = 0;
            for val in buffer.iter_mut().take(count) {
                *val -= min;
                max_delta |= *val;
            }

            // Write metadata: min (long), avgInc (int as float bits), offset (long), bpv (byte)
            meta_output.write_le_long(min)?;
            meta_output.write_le_int(f32::to_bits(avg_inc_f) as i32)?;
            meta_output.write_le_long(data_output.file_pointer() as i64 - base_data_pointer)?;
            if max_delta == 0 {
                meta_output.write_byte(0)?;
            } else {
                let bits_per_value = unsigned_bits_required(max_delta);
                let mut writer = DirectWriter::new(bits_per_value);
                for &val in buffer.iter().take(count) {
                    writer.add(val);
                }
                writer.finish(data_output)?;
                meta_output.write_byte(bits_per_value as u8)?;
            }
        }

        Ok(num_blocks as u32)
    }
}

/// Returns the number of bits required to represent a value.
pub fn bits_required(value: u64) -> u32 {
    if value == 0 {
        0
    } else {
        64 - value.leading_zeros()
    }
}

// Port of Lucene's BlockPackedWriter and PackedInts MSB-first integer packing codec.
// Key Java sources: AbstractBlockPackedWriter, BlockPackedWriter, BulkOperationPacked.

/// Returns the raw number of bits required to represent `value`, treating it as unsigned.
///
/// Unlike [`unsigned_bits_required`], this does NOT round up to a supported BPV.
/// Returns `max(1, 64 - leading_zeros)`.
pub(crate) fn packed_bits_required(value: i64) -> u32 {
    if value == 0 {
        1
    } else {
        64 - (value as u64).leading_zeros()
    }
}

/// Returns the maximum value representable with the given number of bits.
fn packed_max_value(bits_per_value: u32) -> i64 {
    if bits_per_value == 64 {
        i64::MAX
    } else {
        !(!0i64 << bits_per_value)
    }
}

/// Packs `count` values MSB-first into bytes.
///
/// Returns exactly `ceil(count * bits_per_value / 8)` bytes.
fn pack_msb(values: &[i64], count: usize, bits_per_value: u32) -> Vec<u8> {
    let total_bytes = (count as u64 * bits_per_value as u64).div_ceil(8) as usize;
    let mut blocks = vec![0u8; total_bytes];
    let mut blocks_offset = 0;
    let mut next_block: u8 = 0;
    let mut bits_left: u32 = 8;
    let bpv = bits_per_value;

    for &value in values.iter().take(count) {
        let v = value as u64;
        if bpv < bits_left {
            // just buffer
            next_block |= (v << (bits_left - bpv)) as u8;
            bits_left -= bpv;
        } else {
            // flush as many blocks as possible
            let mut bits = bpv - bits_left;
            blocks[blocks_offset] = next_block | (v >> bits) as u8;
            blocks_offset += 1;
            while bits >= 8 {
                bits -= 8;
                blocks[blocks_offset] = (v >> bits) as u8;
                blocks_offset += 1;
            }
            // then buffer
            bits_left = 8 - bits;
            next_block = ((v & ((1u64 << bits) - 1)) << bits_left) as u8;
        }
    }

    // Write final partial byte if there are buffered bits
    if bits_left < 8 && blocks_offset < total_bytes {
        blocks[blocks_offset] = next_block;
    }

    blocks
}

/// Writes packed integers MSB-first (big-endian bit packing).
///
/// Values are packed contiguously with high-order bits first, producing
/// `ceil(count * bpv / 8)` bytes.
pub(crate) fn packed_ints_write(
    output: &mut dyn DataOutput,
    values: &[i64],
    bits_per_value: u32,
) -> io::Result<()> {
    let packed = pack_msb(values, values.len(), bits_per_value);
    output.write_bytes(&packed)
}

/// Writes a variable-length long that handles negative values via unsigned right shift.
///
/// NOT the same as `DataOutput::write_vlong`.
fn write_block_packed_vlong(output: &mut dyn DataOutput, value: i64) -> io::Result<()> {
    let mut i = value as u64;
    let mut k = 0;
    while (i & !0x7F) != 0 && k < 8 {
        output.write_byte((i & 0x7F | 0x80) as u8)?;
        i >>= 7;
        k += 1;
    }
    output.write_byte(i as u8)
}

/// Zigzag-encodes a signed long into an unsigned representation.
fn zigzag_encode(v: i64) -> i64 {
    (v << 1) ^ (v >> 63)
}

/// Token byte layout: bits 1-7 = bitsPerValue, bit 0 = min-is-zero flag.
const BPV_SHIFT: u32 = 1;
const MIN_VALUE_EQUALS_0: u8 = 1;

/// Writes large sequences of longs using block-level delta compression.
///
/// Values are split into fixed-size blocks. For each block, the minimum value is subtracted
/// and the deltas are packed MSB-first using the minimum number of bits required. Each block
/// has a 1–10 byte header encoding the bits-per-value and optional minimum.
pub(crate) struct BlockPackedWriter {
    block_size: usize,
    values: Vec<i64>,
    off: usize,
    ord: u64,
    finished: bool,
}

impl BlockPackedWriter {
    /// Creates a new writer with the given block size, which must be a multiple of 64.
    pub(crate) fn new(block_size: usize) -> Self {
        assert!(
            block_size >= 64 && block_size.is_multiple_of(64),
            "block_size must be a multiple of 64, got {block_size}"
        );
        Self {
            block_size,
            values: vec![0i64; block_size],
            off: 0,
            ord: 0,
            finished: false,
        }
    }

    /// Appends a value, flushing the current block if full.
    pub(crate) fn add(&mut self, output: &mut dyn DataOutput, value: i64) -> io::Result<()> {
        assert!(!self.finished, "already finished");
        if self.off == self.block_size {
            self.flush(output)?;
        }
        self.values[self.off] = value;
        self.off += 1;
        self.ord += 1;
        Ok(())
    }

    /// Flushes any remaining values and marks the writer as finished.
    pub(crate) fn finish(&mut self, output: &mut dyn DataOutput) -> io::Result<()> {
        assert!(!self.finished, "already finished");
        if self.off > 0 {
            self.flush(output)?;
        }
        self.finished = true;
        Ok(())
    }

    /// Returns the number of values added so far.
    #[cfg(test)]
    pub(crate) fn ord(&self) -> u64 {
        self.ord
    }

    /// Resets the writer for reuse with a new output.
    pub(crate) fn reset(&mut self) {
        self.off = 0;
        self.ord = 0;
        self.finished = false;
    }

    fn flush(&mut self, output: &mut dyn DataOutput) -> io::Result<()> {
        assert!(self.off > 0);

        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for i in 0..self.off {
            min = min.min(self.values[i]);
            max = max.max(self.values[i]);
        }

        let delta = max.wrapping_sub(min);
        let bpv = if delta == 0 {
            0
        } else {
            packed_bits_required(delta)
        };

        if bpv == 64 {
            // No need to delta-encode
            min = 0;
        } else if min > 0 {
            // Shrink min so writeVLong needs fewer bytes
            min = 0i64.max(max - packed_max_value(bpv));
        }

        let token = ((bpv << BPV_SHIFT)
            | if min == 0 {
                MIN_VALUE_EQUALS_0 as u32
            } else {
                0
            }) as u8;
        output.write_byte(token)?;

        if min != 0 {
            write_block_packed_vlong(output, zigzag_encode(min) - 1)?;
        }

        if bpv > 0 {
            if min != 0 {
                for i in 0..self.off {
                    self.values[i] -= min;
                }
            }
            // Zero-fill remainder so pack_msb doesn't read stale values
            for i in self.off..self.block_size {
                self.values[i] = 0;
            }

            let packed = pack_msb(&self.values, self.off, bpv);
            output.write_bytes(&packed)?;
        }

        self.off = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryIndexOutput;

    #[test]
    fn test_bits_required() {
        assert_eq!(bits_required(0), 0);
        assert_eq!(bits_required(1), 1);
        assert_eq!(bits_required(2), 2);
        assert_eq!(bits_required(3), 2);
        assert_eq!(bits_required(255), 8);
        assert_eq!(bits_required(256), 9);
    }

    #[test]
    fn test_unsigned_bits_required() {
        // 0 → raw=1 → rounds to 1
        assert_eq!(unsigned_bits_required(0), 1);
        // 1 → raw=1 → rounds to 1
        assert_eq!(unsigned_bits_required(1), 1);
        // 2 → raw=2 → rounds to 2
        assert_eq!(unsigned_bits_required(2), 2);
        // 3 → raw=2 → rounds to 2
        assert_eq!(unsigned_bits_required(3), 2);
        // 4 → raw=3 → rounds to 4
        assert_eq!(unsigned_bits_required(4), 4);
        // 15 → raw=4 → rounds to 4
        assert_eq!(unsigned_bits_required(15), 4);
        // 16 → raw=5 → rounds to 8
        assert_eq!(unsigned_bits_required(16), 8);
        // 255 → raw=8 → rounds to 8
        assert_eq!(unsigned_bits_required(255), 8);
        // 256 → raw=9 → rounds to 12
        assert_eq!(unsigned_bits_required(256), 12);
        // 4095 → raw=12 → rounds to 12
        assert_eq!(unsigned_bits_required(4095), 12);
        // 4096 → raw=13 → rounds to 16
        assert_eq!(unsigned_bits_required(4096), 16);
        // i64::MAX → raw=63 → rounds to 64
        assert_eq!(unsigned_bits_required(i64::MAX), 64);
        // -1 (all bits set as unsigned) → raw=64 → rounds to 64
        assert_eq!(unsigned_bits_required(-1), 64);
    }

    #[test]
    fn test_direct_writer_bpv8_lsb_first() {
        // bpv=8: each value is 1 byte LE
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = DirectWriter::new(8);
        w.add(0x12);
        w.add(0x34);
        w.add(0xAB);
        w.finish(&mut out).unwrap();
        // No padding for bpv=8
        assert_eq!(out.bytes(), &[0x12, 0x34, 0xAB]);
    }

    #[test]
    fn test_direct_writer_bpv16() {
        // bpv=16: each value is 2 bytes LE
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = DirectWriter::new(16);
        w.add(0x1234);
        w.add(0xABCD);
        w.finish(&mut out).unwrap();
        // 0x1234 LE = [0x34, 0x12], 0xABCD LE = [0xCD, 0xAB]
        assert_eq!(out.bytes(), &[0x34, 0x12, 0xCD, 0xAB]);
    }

    #[test]
    fn test_direct_writer_bpv1_lsb_first() {
        // bpv=1: 64 values per long, LSB-first
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = DirectWriter::new(1);
        // Write values: 1,0,1,0,0,0,0,0 (bits 0-7), rest 0
        w.add(1); // bit 0
        w.add(0); // bit 1
        w.add(1); // bit 2
        for _ in 0..61 {
            w.add(0);
        }
        w.finish(&mut out).unwrap();
        let bytes = out.bytes();
        // First byte should be 0b00000101 = 0x05 (bit 0 and bit 2 set, LSB-first)
        assert_eq!(bytes[0], 0x05);
    }

    #[test]
    fn test_direct_writer_bpv4_lsb_first() {
        // bpv=4: 16 values per long, LSB-first
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = DirectWriter::new(4);
        // Write 16 values to fill one long
        w.add(0xA); // nibble 0 (lowest 4 bits of first byte)
        w.add(0xB); // nibble 1
        for _ in 0..14 {
            w.add(0);
        }
        w.finish(&mut out).unwrap();
        let bytes = out.bytes();
        // First byte: nibble0=0xA, nibble1=0xB → 0xBA
        assert_eq!(bytes[0], 0xBA);
        // Remaining 7 bytes should be 0
        for byte in &bytes[1..8] {
            assert_eq!(*byte, 0);
        }
    }

    #[test]
    fn test_direct_writer_bpv12_pairs() {
        // bpv=12: pairs written as LE int (3 bytes)
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = DirectWriter::new(12);
        w.add(0x123); // 12-bit value 1
        w.add(0x456); // 12-bit value 2
        w.finish(&mut out).unwrap();
        let bytes = out.bytes();
        // merged = 0x123 | (0x456 << 12) = 0x123 | 0x456000 = 0x456123
        // as LE int bytes: [0x23, 0x61, 0x45]
        assert_eq!(bytes[0], 0x23);
        assert_eq!(bytes[1], 0x61);
        assert_eq!(bytes[2], 0x45);
        // Padding: bpv=12 > 8: 16-12=4 bits → 1 byte padding
        assert_len_eq_x!(&bytes, 4); // 3 data + 1 padding
        assert_eq!(bytes[3], 0);
    }

    #[test]
    fn test_direct_writer_bpv32_padding() {
        // bpv=32: each value is 4 bytes LE, no padding (32-32=0)
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = DirectWriter::new(32);
        w.add(1);
        w.finish(&mut out).unwrap();
        assert_eq!(out.bytes(), &[1, 0, 0, 0]);
    }

    #[test]
    fn test_direct_writer_bpv24_padding() {
        // bpv=24: each value is 3 bytes LE, padding = 32-24=8 bits → 1 byte
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = DirectWriter::new(24);
        w.add(0x010203);
        w.finish(&mut out).unwrap();
        // 0x010203 LE = [0x03, 0x02, 0x01] + 1 padding byte
        assert_eq!(out.bytes(), &[0x03, 0x02, 0x01, 0x00]);
    }

    #[test]
    fn test_direct_writer_bpv40_padding() {
        // bpv=40: each value is 5 bytes LE, padding = 64-40=24 bits → 3 bytes
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = DirectWriter::new(40);
        w.add(0x01);
        w.finish(&mut out).unwrap();
        assert_eq!(out.bytes().len(), 5 + 3); // 5 data + 3 padding
    }

    #[test]
    fn test_direct_writer_bpv0() {
        // bpv=0: no output
        let mut out = MemoryIndexOutput::new("test".to_string());
        let w = DirectWriter::new(0);
        w.finish(&mut out).unwrap();
        assert_is_empty!(out.bytes());
    }

    #[test]
    fn test_direct_monotonic_writer_simple() {
        let mut writer = DirectMonotonicWriter::new(2); // block size = 4
        writer.add(0);
        writer.add(10);
        writer.add(20);
        writer.add(30);

        let mut meta = MemoryIndexOutput::new("meta".to_string());
        let mut data = MemoryIndexOutput::new("data".to_string());

        let blocks = writer.finish(&mut meta, &mut data).unwrap();
        assert_eq!(blocks, 1);
        // Meta: 8 (min) + 4 (avgInc) + 8 (offset) + 1 (bpv) = 21 bytes per block
        assert_len_eq_x!(&meta.bytes(), 21);
    }

    #[test]
    fn test_direct_monotonic_writer_multiple_blocks() {
        let mut writer = DirectMonotonicWriter::new(1); // block size = 2
        writer.add(0);
        writer.add(100);
        writer.add(200);
        writer.add(300);
        writer.add(400);

        let mut meta = MemoryIndexOutput::new("meta".to_string());
        let mut data = MemoryIndexOutput::new("data".to_string());

        let blocks = writer.finish(&mut meta, &mut data).unwrap();
        assert_eq!(blocks, 3); // 5 values, block size 2 = 3 blocks
    }

    #[test]
    fn test_direct_monotonic_writer_constant() {
        // All values equal → bpv=0, no data written
        let mut writer = DirectMonotonicWriter::new(2);
        writer.add(42);
        writer.add(42);
        writer.add(42);
        writer.add(42);

        let mut meta = MemoryIndexOutput::new("meta".to_string());
        let mut data = MemoryIndexOutput::new("data".to_string());

        writer.finish(&mut meta, &mut data).unwrap();
        assert_is_empty!(data.bytes()); // no data for constant values
    }

    #[test]
    fn test_packed_bits_required() {
        assert_eq!(packed_bits_required(0), 1);
        assert_eq!(packed_bits_required(1), 1);
        assert_eq!(packed_bits_required(2), 2);
        assert_eq!(packed_bits_required(3), 2);
        assert_eq!(packed_bits_required(255), 8);
        assert_eq!(packed_bits_required(256), 9);
        assert_eq!(packed_bits_required(i64::MAX), 63);
        assert_eq!(packed_bits_required(-1), 64);
    }

    #[test]
    fn test_packed_max_value() {
        assert_eq!(packed_max_value(1), 1);
        assert_eq!(packed_max_value(8), 255);
        assert_eq!(packed_max_value(16), 65535);
        assert_eq!(packed_max_value(63), i64::MAX);
        assert_eq!(packed_max_value(64), i64::MAX);
    }

    #[test]
    fn test_pack_msb_4bit() {
        // Two 4-bit values [0xA, 0xB] → packed MSB-first into [0xAB]
        let values = [0xA_i64, 0xB];
        let result = pack_msb(&values, 2, 4);
        assert_eq!(result, vec![0xAB]);
    }

    #[test]
    fn test_pack_msb_8bit() {
        let values = [0x12_i64, 0x34, 0xAB];
        let result = pack_msb(&values, 3, 8);
        assert_eq!(result, vec![0x12, 0x34, 0xAB]);
    }

    #[test]
    fn test_pack_msb_1bit() {
        // 8 one-bit values: 1,0,1,0,1,1,0,0 → MSB first → 0b10101100 = 0xAC
        let values = [1_i64, 0, 1, 0, 1, 1, 0, 0];
        let result = pack_msb(&values, 8, 1);
        assert_eq!(result, vec![0xAC]);
    }

    #[test]
    fn test_pack_msb_5bit_spanning() {
        // Three 5-bit values spanning bytes: 0b11111=31, 0b10101=21, 0b00001=1
        // Packed MSB: 11111_10101_00001_0 = 0xFD42 (but only 15 bits = 2 bytes)
        // Byte 0: 11111_101 = 0xFD
        // Byte 1: 01_00001_0 = 0x42
        let values = [31_i64, 21, 1];
        let result = pack_msb(&values, 3, 5);
        assert_len_eq_x!(&result, 2); // ceil(15/8) = 2
        assert_eq!(result, vec![0xFD, 0x42]);
    }

    #[test]
    fn test_pack_msb_64bit() {
        let values = [0x0102030405060708_i64];
        let result = pack_msb(&values, 1, 64);
        assert_eq!(result, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn test_packed_ints_write() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let values = [0xA_i64, 0xB];
        packed_ints_write(&mut out, &values, 4).unwrap();
        assert_eq!(out.bytes(), &[0xAB]);
    }

    #[test]
    fn test_write_block_packed_vlong_small() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_block_packed_vlong(&mut out, 42).unwrap();
        assert_eq!(out.bytes(), &[42]);
    }

    #[test]
    fn test_write_block_packed_vlong_large() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        // 128 = 0x80 → needs 2 bytes in VLong: 0x80|0x00=0x80, then 0x01
        write_block_packed_vlong(&mut out, 128).unwrap();
        assert_eq!(out.bytes(), &[0x80, 0x01]);
    }

    #[test]
    fn test_write_block_packed_vlong_negative() {
        // -1 as u64 = 0xFFFFFFFFFFFFFFFF → should write 9 bytes (k<8 loop + final byte)
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_block_packed_vlong(&mut out, -1).unwrap();
        // 8 continuation bytes of 0xFF, then final byte 0xFF
        let bytes = out.bytes();
        assert_len_eq_x!(&bytes, 9);
        for &b in &bytes[..8] {
            assert_eq!(b, 0xFF);
        }
        assert_eq!(bytes[8], 0xFF);
    }

    #[test]
    fn test_zigzag_encode() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_encode(i64::MAX), -2);
        assert_eq!(zigzag_encode(i64::MIN), -1);
    }

    #[test]
    fn test_block_packed_writer_all_same() {
        // All-same values → bpv=0, only a token byte per block
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for _ in 0..64 {
            w.add(&mut out, 42).unwrap();
        }
        w.finish(&mut out).unwrap();
        // Token: bpv=0, min=42≠0 → bit0=0 → token=0x00
        // Then VLong(zigzag(42)-1) = VLong(84-1) = VLong(83) = single byte 83
        // No packed data since bpv=0
        assert_len_eq_x!(&out.bytes(), 2);
        assert_eq!(out.bytes()[0], 0x00); // token
        assert_eq!(out.bytes()[1], 83); // zigzag(42)-1 = 83
    }

    #[test]
    fn test_block_packed_writer_all_zeros() {
        // All zeros → bpv=0, min=0 → token has MIN_VALUE_EQUALS_0 flag
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for _ in 0..64 {
            w.add(&mut out, 0).unwrap();
        }
        w.finish(&mut out).unwrap();
        // Token: bpv=0, min=0 → bit0=1 → token=0x01
        // No VLong, no packed data
        assert_len_eq_x!(&out.bytes(), 1);
        assert_eq!(out.bytes()[0], 0x01);
    }

    #[test]
    fn test_block_packed_writer_sequential() {
        // Values 0..63 → delta=63, bpv=6
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for i in 0..64 {
            w.add(&mut out, i).unwrap();
        }
        w.finish(&mut out).unwrap();
        let bytes = out.bytes();
        // Token: bpv=6, min=0 → (6<<1)|1 = 13 = 0x0D
        assert_eq!(bytes[0], 0x0D);
        // Packed data: 64 values × 6 bits = 384 bits = 48 bytes
        assert_eq!(bytes.len(), 1 + 48);
    }

    #[test]
    fn test_block_packed_writer_partial_block() {
        // Fewer than block_size values → flushed on finish()
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for i in 0..10 {
            w.add(&mut out, i).unwrap();
        }
        w.finish(&mut out).unwrap();
        let bytes = out.bytes();
        // Token: delta=9, bpv=4, min=0 → (4<<1)|1 = 9
        assert_eq!(bytes[0], 9);
        // Packed data: 10 values × 4 bits = 40 bits = 5 bytes
        assert_eq!(bytes.len(), 1 + 5);
    }

    #[test]
    fn test_block_packed_writer_multiple_blocks() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        // 100 values → 1 full block (64) + 1 partial block (36)
        for i in 0..100 {
            w.add(&mut out, i).unwrap();
        }
        w.finish(&mut out).unwrap();
        assert_eq!(w.ord(), 100);

        let bytes = out.bytes();
        // Block 1: values 0..63, delta=63, bpv=6, min=0
        // Token = (6<<1)|1 = 13
        assert_eq!(bytes[0], 13);
        // Block 1 packed data: 64×6=384 bits = 48 bytes
        // Block 2 starts at offset 1+48=49
        // Block 2: values 64..99, delta=35, bpv=6, min=64>0
        // min optimization: max(0, 99 - maxValue(6)) = max(0, 99-63) = 36
        // After subtracting min=36: values become 28..63, still fits in 6 bits
        let block2_token = bytes[49];
        let block2_bpv = (block2_token >> BPV_SHIFT) as u32;
        assert_eq!(block2_bpv, 6);
        // min≠0, so bit0=0
        assert_eq!(block2_token & 1, 0);
    }

    #[test]
    fn test_block_packed_writer_delta_with_min() {
        // Values with large min: [1000, 1001, 1002, 1003]
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for i in 0..4 {
            w.add(&mut out, 1000 + i).unwrap();
        }
        w.finish(&mut out).unwrap();
        let bytes = out.bytes();
        // delta=3, bpv=2, min=1000>0
        // min optimization: max(0, 1003 - maxValue(2)) = max(0, 1003-3) = 1000
        // So min stays 1000
        let token = bytes[0];
        let bpv = (token >> BPV_SHIFT) as u32;
        assert_eq!(bpv, 2);
        assert_eq!(token & 1, 0); // min≠0
    }

    #[test]
    fn test_block_packed_writer_bpv64() {
        // min=i64::MIN, max=i64::MAX → wrapping delta = -1 → bpv=64, min forced to 0
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        w.add(&mut out, i64::MIN).unwrap();
        w.add(&mut out, i64::MAX).unwrap();
        for _ in 2..64 {
            w.add(&mut out, 0).unwrap();
        }
        w.finish(&mut out).unwrap();
        let bytes = out.bytes();
        // bpv=64, min forced to 0 → token = (64<<1)|1 = 129
        assert_eq!(bytes[0], 129u8);
        // No VLong (min=0)
        // Packed data: 64 values × 64 bits = 512 bytes
        assert_eq!(bytes.len(), 1 + 512);
    }

    #[test]
    fn test_block_packed_writer_ord() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        assert_eq!(w.ord(), 0);
        for i in 0..10 {
            w.add(&mut out, i).unwrap();
        }
        assert_eq!(w.ord(), 10);
        w.finish(&mut out).unwrap();
        assert_eq!(w.ord(), 10);
    }

    #[test]
    fn test_block_packed_writer_reset() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for i in 0..10 {
            w.add(&mut out, i).unwrap();
        }
        w.finish(&mut out).unwrap();

        // After reset, can reuse
        w.reset();
        assert_eq!(w.ord(), 0);
        let mut out2 = MemoryIndexOutput::new("test2".to_string());
        for i in 0..5 {
            w.add(&mut out2, i).unwrap();
        }
        w.finish(&mut out2).unwrap();
        assert_eq!(w.ord(), 5);
    }

    // Cross-validation tests: expected bytes from Java Lucene's BlockPackedWriter

    #[test]
    fn test_block_packed_writer_java_64x42() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for _ in 0..64 {
            w.add(&mut out, 42).unwrap();
        }
        w.finish(&mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[0x00, 0x53];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_block_packed_writer_java_0_to_63() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for i in 0..64 {
            w.add(&mut out, i).unwrap();
        }
        w.finish(&mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x0D, 0x00, 0x10, 0x83, 0x10, 0x51, 0x87, 0x20,
            0x92, 0x8B, 0x30, 0xD3, 0x8F, 0x41, 0x14, 0x93,
            0x51, 0x55, 0x97, 0x61, 0x96, 0x9B, 0x71, 0xD7,
            0x9F, 0x82, 0x18, 0xA3, 0x92, 0x59, 0xA7, 0xA2,
            0x9A, 0xAB, 0xB2, 0xDB, 0xAF, 0xC3, 0x1C, 0xB3,
            0xD3, 0x5D, 0xB7, 0xE3, 0x9E, 0xBB, 0xF3, 0xDF,
            0xBF,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_block_packed_writer_java_0_to_99() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for i in 0..100 {
            w.add(&mut out, i).unwrap();
        }
        w.finish(&mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x0D, 0x00, 0x10, 0x83, 0x10, 0x51, 0x87, 0x20,
            0x92, 0x8B, 0x30, 0xD3, 0x8F, 0x41, 0x14, 0x93,
            0x51, 0x55, 0x97, 0x61, 0x96, 0x9B, 0x71, 0xD7,
            0x9F, 0x82, 0x18, 0xA3, 0x92, 0x59, 0xA7, 0xA2,
            0x9A, 0xAB, 0xB2, 0xDB, 0xAF, 0xC3, 0x1C, 0xB3,
            0xD3, 0x5D, 0xB7, 0xE3, 0x9E, 0xBB, 0xF3, 0xDF,
            0xBF, 0x0C, 0x47, 0x71, 0xD7, 0x9F, 0x82, 0x18,
            0xA3, 0x92, 0x59, 0xA7, 0xA2, 0x9A, 0xAB, 0xB2,
            0xDB, 0xAF, 0xC3, 0x1C, 0xB3, 0xD3, 0x5D, 0xB7,
            0xE3, 0x9E, 0xBB, 0xF3, 0xDF, 0xBF,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_block_packed_writer_java_1000_to_1063() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for i in 0..64 {
            w.add(&mut out, 1000 + i).unwrap();
        }
        w.finish(&mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0x0C, 0xCF, 0x0F, 0x00, 0x10, 0x83, 0x10, 0x51,
            0x87, 0x20, 0x92, 0x8B, 0x30, 0xD3, 0x8F, 0x41,
            0x14, 0x93, 0x51, 0x55, 0x97, 0x61, 0x96, 0x9B,
            0x71, 0xD7, 0x9F, 0x82, 0x18, 0xA3, 0x92, 0x59,
            0xA7, 0xA2, 0x9A, 0xAB, 0xB2, 0xDB, 0xAF, 0xC3,
            0x1C, 0xB3, 0xD3, 0x5D, 0xB7, 0xE3, 0x9E, 0xBB,
            0xF3, 0xDF, 0xBF,
        ];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_block_packed_writer_java_0_to_9() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for i in 0..10 {
            w.add(&mut out, i).unwrap();
        }
        w.finish(&mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[0x09, 0x01, 0x23, 0x45, 0x67, 0x89];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_block_packed_writer_java_64x0() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let mut w = BlockPackedWriter::new(64);
        for _ in 0..64 {
            w.add(&mut out, 0).unwrap();
        }
        w.finish(&mut out).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[0x01];
        assert_eq!(out.bytes(), expected);
    }

    // Cross-validation: PackedInts (packed_ints_write) vs Java PackedInts.getWriterNoHeader(PACKED)

    #[test]
    fn test_packed_ints_write_java_4bpv() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let values: Vec<i64> = (0..10).collect();
        packed_ints_write(&mut out, &values, 4).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[0x01, 0x23, 0x45, 0x67, 0x89];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_packed_ints_write_java_5bpv() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let values = [31_i64, 21, 1, 0, 15, 7, 3, 30];
        packed_ints_write(&mut out, &values, 5).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[0xFD, 0x42, 0x07, 0x9C, 0x7E];
        assert_eq!(out.bytes(), expected);
    }

    #[test]
    fn test_packed_ints_write_java_8bpv() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let values = [0x12_i64, 0x34, 0xAB];
        packed_ints_write(&mut out, &values, 8).unwrap();
        #[rustfmt::skip]
        let expected: &[u8] = &[0x12, 0x34, 0xAB];
        assert_eq!(out.bytes(), expected);
    }
}
