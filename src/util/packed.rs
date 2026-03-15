// Ported from org.apache.lucene.util.packed

use std::io;

use crate::store::{DataOutput, IndexOutput};

/// Supported bits-per-value for DirectWriter encoding.
/// Ported from org.apache.lucene.util.packed.DirectWriter.SUPPORTED_BITS_PER_VALUE
const SUPPORTED_BITS_PER_VALUE: &[u32] = &[1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64];

/// Returns the number of bits required to represent values up to and including `max_value`,
/// interpreted as unsigned, rounded up to the nearest supported BPV.
/// Ported from org.apache.lucene.util.packed.DirectWriter.unsignedBitsRequired
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
        .unwrap()
}

/// Writes bit-packed integers.
/// Ported from org.apache.lucene.util.packed.DirectWriter
pub struct DirectWriter {
    bits_per_value: u32,
    values: Vec<i64>,
}

impl DirectWriter {
    pub fn new(bits_per_value: u32) -> Self {
        Self {
            bits_per_value,
            values: Vec::new(),
        }
    }

    pub fn add(&mut self, value: i64) {
        self.values.push(value);
    }

    /// Writes all accumulated values as bit-packed data, then padding bytes.
    /// Ported from org.apache.lucene.util.packed.DirectWriter.flush() + finish()
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
                output.write_bytes(&packed.to_le_bytes())?;
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
/// Ported from org.apache.lucene.util.packed.DirectMonotonicWriter
///
/// Data is split into blocks. For each block:
/// - Meta: min value (VLong), avg increment (Int as float bits), offset (VLong), bits per value (Byte)
/// - Data: bit-packed deltas from expected linear values
pub struct DirectMonotonicWriter {
    block_shift: u32,
    values: Vec<i64>,
}

impl DirectMonotonicWriter {
    pub fn new(block_shift: u32) -> Self {
        Self {
            block_shift,
            values: Vec::new(),
        }
    }

    pub fn add(&mut self, value: i64) {
        self.values.push(value);
    }

    /// Writes metadata to meta_output and data to data_output.
    /// Returns the number of blocks written.
    /// Ported from org.apache.lucene.util.packed.DirectMonotonicWriter.flush/finish
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

    // Ported from org.apache.lucene.util.packed.TestDirectWriter
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

    // Ported from org.apache.lucene.util.packed.TestDirectWriter
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
        assert_eq!(bytes.len(), 4); // 3 data + 1 padding
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
        assert_eq!(out.bytes().len(), 0);
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
        assert_eq!(meta.bytes().len(), 21);
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
        assert_eq!(data.bytes().len(), 0); // no data for constant values
    }
}
