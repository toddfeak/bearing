// SPDX-License-Identifier: Apache-2.0

//! Compressed long value storage using page-based delta packing.
//!
//! Values are accumulated into fixed-size pages. When a page fills, values are
//! delta-encoded (value - min) and bit-packed at the minimum required BPV.

use std::fmt;
use std::mem;

use crate::encoding::packed;

/// Default page size (number of values per page).
pub const DEFAULT_PAGE_SIZE: usize = 256;
const MIN_PAGE_SIZE: usize = 64;
const MAX_PAGE_SIZE: usize = 1 << 20;

/// No memory overhead at all, but the returned implementation may be slow.
pub const COMPACT: f32 = 0.0;
/// At most 25% memory overhead.
pub const DEFAULT: f32 = 0.25;
/// At most 50% memory overhead, always select a reasonably fast implementation.
pub const FAST: f32 = 0.5;
/// At most 700% memory overhead, always select a direct implementation.
pub const FASTEST: f32 = 7.0;

/// Immutable packed long values, built by [`DeltaPackedBuilder`].
///
/// Values are stored in pages of packed integers. Each page has a per-page
/// minimum that is added back during reads (delta encoding).
pub struct PackedLongValues {
    page_shift: u32,
    page_mask: usize,
    pages: Vec<PackedPage>,
    mins: Vec<i64>,
    size: usize,
}

/// A single page of packed values.
enum PackedPage {
    /// All values in this page are zero.
    Null { count: usize },
    /// Bit-packed values (MSB-first).
    Packed {
        data: Vec<u8>,
        bits_per_value: u32,
        count: usize,
    },
}

impl PackedPage {
    fn count(&self) -> usize {
        match self {
            PackedPage::Null { count } => *count,
            PackedPage::Packed { count, .. } => *count,
        }
    }

    fn get(&self, index: usize) -> i64 {
        match self {
            PackedPage::Null { .. } => 0,
            PackedPage::Packed {
                data,
                bits_per_value,
                ..
            } => unpack_msb_single(data, *bits_per_value, index),
        }
    }

    fn decode_block(&self, dest: &mut [i64]) -> usize {
        let count = self.count();
        match self {
            PackedPage::Null { .. } => {
                for d in dest.iter_mut().take(count) {
                    *d = 0;
                }
            }
            PackedPage::Packed {
                data,
                bits_per_value,
                ..
            } => {
                unpack_msb(data, *bits_per_value, &mut dest[..count]);
            }
        }
        count
    }

    fn ram_bytes_used(&self) -> usize {
        match self {
            PackedPage::Null { .. } => 0,
            PackedPage::Packed { data, .. } => data.len(),
        }
    }
}

impl PackedLongValues {
    /// Returns the number of values.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns the value at the given index.
    pub fn get(&self, index: usize) -> i64 {
        assert!(index < self.size);
        let block = index >> self.page_shift;
        let element = index & self.page_mask;
        self.mins[block] + self.pages[block].get(element)
    }

    /// Returns an iterator over all values.
    pub fn iterator(&self) -> PackedLongValuesIterator<'_> {
        let page_size = (self.page_mask + 1).min(self.size);
        let mut current_values = vec![0i64; page_size];
        let current_count = if self.pages.is_empty() {
            0
        } else {
            let count = self.pages[0].decode_block(&mut current_values);
            let min = self.mins[0];
            for v in current_values.iter_mut().take(count) {
                *v += min;
            }
            count
        };
        PackedLongValuesIterator {
            values: self,
            current_values,
            v_off: 0,
            p_off: 0,
            current_count,
        }
    }

    /// Returns the estimated RAM usage in bytes.
    pub fn ram_bytes_used(&self) -> usize {
        mem::size_of::<Self>()
            + self.pages.iter().map(|p| p.ram_bytes_used()).sum::<usize>()
            + self.mins.len() * mem::size_of::<i64>()
    }
}

impl fmt::Debug for PackedLongValues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PackedLongValues")
            .field("size", &self.size)
            .field("pages", &self.pages.len())
            .finish()
    }
}

/// Iterator over values in a [`PackedLongValues`].
pub struct PackedLongValuesIterator<'a> {
    values: &'a PackedLongValues,
    current_values: Vec<i64>,
    v_off: usize,
    p_off: usize,
    current_count: usize,
}

impl<'a> PackedLongValuesIterator<'a> {
    fn fill_block(&mut self) {
        if self.v_off == self.values.pages.len() {
            self.current_count = 0;
        } else {
            self.current_count =
                self.values.pages[self.v_off].decode_block(&mut self.current_values);
            let min = self.values.mins[self.v_off];
            for v in self.current_values.iter_mut().take(self.current_count) {
                *v += min;
            }
            assert!(self.current_count > 0);
        }
    }

    /// Whether there are remaining values.
    pub fn has_next(&self) -> bool {
        self.p_off < self.current_count
    }
}

impl Iterator for PackedLongValuesIterator<'_> {
    type Item = i64;

    fn next(&mut self) -> Option<i64> {
        if !self.has_next() {
            return None;
        }
        let result = self.current_values[self.p_off];
        self.p_off += 1;
        if self.p_off == self.current_count {
            self.v_off += 1;
            self.p_off = 0;
            self.fill_block();
        }
        Some(result)
    }
}

/// Builder for delta-packed [`PackedLongValues`].
///
/// Values are accumulated into a pending buffer. When the buffer fills (at `page_size`),
/// the page is delta-encoded (each value minus the page minimum) and bit-packed.
pub struct DeltaPackedBuilder {
    page_shift: u32,
    page_mask: usize,
    acceptable_overhead_ratio: f32,
    pending: Vec<i64>,
    size: usize,
    pages: Vec<PackedPage>,
    mins: Vec<i64>,
    values_off: usize,
    pending_off: usize,
}

impl DeltaPackedBuilder {
    /// Creates a new builder with the given page size and acceptable overhead ratio.
    ///
    /// The `acceptable_overhead_ratio` controls the trade-off between memory
    /// efficiency and access speed. Use [`COMPACT`] (0.0) for minimum memory,
    /// [`FASTEST`] (7.0) for maximum speed.
    ///
    /// # Panics
    /// Panics if `page_size` is not a power of two or outside `[64, 1048576]`.
    pub fn new(page_size: usize, acceptable_overhead_ratio: f32) -> Self {
        let page_shift = check_block_size(page_size, MIN_PAGE_SIZE, MAX_PAGE_SIZE);
        let page_mask = page_size - 1;
        Self {
            page_shift,
            page_mask,
            acceptable_overhead_ratio,
            pending: vec![0i64; page_size],
            size: 0,
            pages: Vec::with_capacity(16),
            mins: Vec::with_capacity(16),
            values_off: 0,
            pending_off: 0,
        }
    }

    /// Creates a new builder with the default page size (256) and the given overhead ratio.
    pub fn with_default_page_size(acceptable_overhead_ratio: f32) -> Self {
        Self::new(DEFAULT_PAGE_SIZE, acceptable_overhead_ratio)
    }

    /// Adds a value.
    pub fn add(&mut self, value: i64) {
        if self.pending_off == self.pending.len() {
            self.pack();
        }
        self.pending[self.pending_off] = value;
        self.pending_off += 1;
        self.size += 1;
    }

    /// Returns the number of values added so far.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns the estimated RAM usage of the builder in bytes.
    pub fn ram_bytes_used(&self) -> usize {
        mem::size_of::<Self>()
            + self.pending.len() * mem::size_of::<i64>()
            + self.pages.iter().map(|p| p.ram_bytes_used()).sum::<usize>()
            + self.mins.len() * mem::size_of::<i64>()
    }

    /// Builds the immutable [`PackedLongValues`]. This is a destructive operation.
    pub fn build(mut self) -> PackedLongValues {
        self.finish();
        PackedLongValues {
            page_shift: self.page_shift,
            page_mask: self.page_mask,
            pages: self.pages,
            mins: self.mins,
            size: self.size,
        }
    }

    fn finish(&mut self) {
        if self.pending_off > 0 {
            self.pack();
        }
    }

    fn pack(&mut self) {
        let num_values = self.pending_off;
        assert!(num_values > 0);

        // Find min value for delta encoding
        let mut min = self.pending[0];
        for i in 1..num_values {
            min = min.min(self.pending[i]);
        }

        // Subtract min from all values
        for i in 0..num_values {
            self.pending[i] -= min;
        }

        // Find max delta to determine BPV
        let mut max_delta: i64 = 0;
        for i in 0..num_values {
            max_delta = max_delta.max(self.pending[i]);
        }

        let page = if max_delta == 0 {
            PackedPage::Null { count: num_values }
        } else {
            let raw_bpv = packed::packed_bits_required(max_delta);
            let bits_per_value = fastest_bits_per_value(raw_bpv, self.acceptable_overhead_ratio);
            let data = packed::pack_msb(&self.pending, num_values, bits_per_value);
            PackedPage::Packed {
                data,
                bits_per_value,
                count: num_values,
            }
        };

        self.pages.push(page);
        self.mins.push(min);
        self.values_off += 1;

        // Reset pending buffer
        self.pending_off = 0;
    }
}

impl fmt::Debug for DeltaPackedBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeltaPackedBuilder")
            .field("size", &self.size)
            .field("pages", &self.values_off)
            .field("pending", &self.pending_off)
            .finish()
    }
}

/// Given a minimum bits-per-value and an acceptable overhead ratio, returns the
/// actual bits-per-value to use. May round up to 8, 16, 32, or 64 for faster
/// access when the overhead ratio allows it.
fn fastest_bits_per_value(bits_per_value: u32, acceptable_overhead_ratio: f32) -> u32 {
    let ratio = acceptable_overhead_ratio.clamp(COMPACT, FASTEST);
    let acceptable_overhead_per_value = ratio * bits_per_value as f32;
    let max_bits_per_value = bits_per_value + acceptable_overhead_per_value as u32;

    if bits_per_value <= 8 && max_bits_per_value >= 8 {
        8
    } else if bits_per_value <= 16 && max_bits_per_value >= 16 {
        16
    } else if bits_per_value <= 32 && max_bits_per_value >= 32 {
        32
    } else if bits_per_value <= 64 && max_bits_per_value >= 64 {
        64
    } else {
        bits_per_value
    }
}

/// Checks that `block_size` is a power of two within bounds, returns its log2.
fn check_block_size(block_size: usize, min: usize, max: usize) -> u32 {
    assert!(
        block_size >= min && block_size <= max,
        "blockSize must be >= {min} and <= {max}, got {block_size}"
    );
    assert!(
        block_size.is_power_of_two(),
        "blockSize must be a power of two, got {block_size}"
    );
    block_size.trailing_zeros()
}

/// Unpacks a single value at `index` from MSB-packed bytes.
fn unpack_msb_single(data: &[u8], bits_per_value: u32, index: usize) -> i64 {
    if bits_per_value == 0 {
        return 0;
    }
    let bit_offset = index as u64 * bits_per_value as u64;
    let byte_offset = (bit_offset / 8) as usize;
    let bit_shift = bit_offset % 8;

    // Read enough bytes to cover the value
    let total_bits_needed = bit_shift as u32 + bits_per_value;
    let bytes_needed = total_bits_needed.div_ceil(8) as usize;

    let mut raw: u64 = 0;
    for i in 0..bytes_needed {
        raw = (raw << 8) | data[byte_offset + i] as u64;
    }

    let shift = (bytes_needed as u32 * 8) - bit_shift as u32 - bits_per_value;
    let mask = (1u64 << bits_per_value) - 1;
    ((raw >> shift) & mask) as i64
}

/// Unpacks all values from MSB-packed bytes into `dest`.
fn unpack_msb(data: &[u8], bits_per_value: u32, dest: &mut [i64]) {
    for (i, d) in dest.iter_mut().enumerate() {
        *d = unpack_msb_single(data, bits_per_value, i);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;

    #[test]
    fn test_round_trip_sequential() {
        let mut builder = DeltaPackedBuilder::with_default_page_size(COMPACT);
        for i in 0..1000 {
            builder.add(i);
        }
        let values = builder.build();
        assert_eq!(values.size(), 1000);

        for i in 0..1000 {
            assert_eq!(values.get(i), i as i64);
        }
    }

    #[test]
    fn test_round_trip_iterator() {
        let mut builder = DeltaPackedBuilder::with_default_page_size(COMPACT);
        let expected: Vec<i64> = (0..500).map(|i| i * 3 + 7).collect();
        for &v in &expected {
            builder.add(v);
        }
        let values = builder.build();
        let actual: Vec<i64> = values.iterator().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_constant_values() {
        let mut builder = DeltaPackedBuilder::with_default_page_size(COMPACT);
        for _ in 0..300 {
            builder.add(42);
        }
        let values = builder.build();
        assert_eq!(values.size(), 300);

        for i in 0..300 {
            assert_eq!(values.get(i), 42);
        }
    }

    #[test]
    fn test_all_zeros() {
        let mut builder = DeltaPackedBuilder::with_default_page_size(COMPACT);
        for _ in 0..256 {
            builder.add(0);
        }
        let values = builder.build();

        // All-zero page should use NullPage (minimal RAM)
        assert_lt!(values.ram_bytes_used(), 256 * 8);

        for i in 0..256 {
            assert_eq!(values.get(i), 0);
        }
    }

    #[test]
    fn test_ram_less_than_raw() {
        let mut builder = DeltaPackedBuilder::with_default_page_size(COMPACT);
        // Small values that pack into few bits
        for i in 0..1000 {
            builder.add(i % 16); // 4-bit values
        }
        let values = builder.build();
        // Should be significantly less than 1000 * 8 = 8000 bytes
        assert_lt!(values.ram_bytes_used(), 1000);
    }

    #[test]
    fn test_delta_efficiency() {
        let mut builder = DeltaPackedBuilder::with_default_page_size(COMPACT);
        // Large base value but small deltas — delta encoding should pack tightly
        for i in 0..256 {
            builder.add(1_000_000 + i);
        }
        let values = builder.build();

        for i in 0..256 {
            assert_eq!(values.get(i), 1_000_000 + i as i64);
        }

        // Deltas are 0..255, requiring 8 bits. 256 * 1 byte = 256 bytes data
        // Much less than 256 * 8 = 2048 bytes raw
        assert_lt!(values.ram_bytes_used(), 512);
    }

    #[test]
    fn test_page_boundary() {
        let mut builder = DeltaPackedBuilder::new(64, COMPACT);
        // Fill exactly 3 pages + partial
        for i in 0..200 {
            builder.add(i);
        }
        let values = builder.build();
        assert_eq!(values.size(), 200);

        // Verify all values across page boundaries
        for i in 0..200 {
            assert_eq!(values.get(i), i as i64);
        }

        let actual: Vec<i64> = values.iterator().collect();
        let expected: Vec<i64> = (0..200).collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_negative_values() {
        let mut builder = DeltaPackedBuilder::with_default_page_size(COMPACT);
        let expected: Vec<i64> = (-50..50).collect();
        for &v in &expected {
            builder.add(v);
        }
        let values = builder.build();
        let actual: Vec<i64> = values.iterator().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_single_value() {
        let mut builder = DeltaPackedBuilder::with_default_page_size(COMPACT);
        builder.add(999);
        let values = builder.build();
        assert_eq!(values.size(), 1);
        assert_eq!(values.get(0), 999);
    }

    #[test]
    fn test_empty() {
        let builder = DeltaPackedBuilder::with_default_page_size(COMPACT);
        let values = builder.build();
        assert_eq!(values.size(), 0);
    }

    #[test]
    fn test_builder_size_tracking() {
        let mut builder = DeltaPackedBuilder::with_default_page_size(COMPACT);
        assert_eq!(builder.size(), 0);
        builder.add(1);
        assert_eq!(builder.size(), 1);
        builder.add(2);
        assert_eq!(builder.size(), 2);
    }

    #[test]
    fn test_fastest_bits_per_value() {
        // COMPACT (0.0) — always use exact BPV
        assert_eq!(fastest_bits_per_value(5, COMPACT), 5);
        assert_eq!(fastest_bits_per_value(8, COMPACT), 8);

        // FASTEST (7.0) — round up to nearest power-of-two boundary
        assert_eq!(fastest_bits_per_value(1, FASTEST), 8);
        assert_eq!(fastest_bits_per_value(5, FASTEST), 8); // 5 + 5*7 = 40 >= 8
        assert_eq!(fastest_bits_per_value(9, FASTEST), 16); // 9 + 9*7 = 72 >= 16

        // FAST (0.5) — modest rounding
        assert_eq!(fastest_bits_per_value(6, FAST), 8); // 6 + 6*0.5 = 9 >= 8
        assert_eq!(fastest_bits_per_value(12, FAST), 16); // 12 + 12*0.5 = 18 >= 16
        assert_eq!(fastest_bits_per_value(5, FAST), 5); // 5 + 5*0.5 = 7 < 8
    }

    #[test]
    fn test_overhead_ratio_roundtrip() {
        // Values requiring 5 raw bits with FASTEST should round up to 8
        let mut builder = DeltaPackedBuilder::with_default_page_size(FASTEST);
        for i in 0..256 {
            builder.add(i % 20); // values 0-19, need 5 bits
        }
        let values = builder.build();
        for i in 0..256 {
            assert_eq!(values.get(i), (i % 20) as i64);
        }
    }

    #[test]
    fn test_unpack_msb_roundtrip() {
        let values: Vec<i64> = vec![15, 7, 3, 0, 1, 14, 8, 2];
        let packed = packed::pack_msb(&values, values.len(), 4);
        let mut unpacked = vec![0i64; values.len()];
        unpack_msb(&packed, 4, &mut unpacked);
        assert_eq!(unpacked, values);
    }
}
