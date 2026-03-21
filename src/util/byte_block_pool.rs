// SPDX-License-Identifier: Apache-2.0

//! Shared byte arena and variable-length slice allocation for postings data.
//!
//! [`ByteBlockPool`] manages contiguous 32 KB blocks of memory. [`ByteSlicePool`]
//! allocates variable-length slices within the pool, growing through defined levels
//! with forwarding addresses. [`ByteSliceWriter`] and [`ByteSliceReader`] provide
//! cursor-based write and read access over the slice chain.

use std::cell::Cell;
use std::io;
use std::rc::Rc;

use mem_dbg::MemSize;

/// Shift to convert a global byte offset to a buffer index.
pub const BYTE_BLOCK_SHIFT: usize = 15;

/// Size of each buffer in the pool (32 KB).
pub const BYTE_BLOCK_SIZE: usize = 1 << BYTE_BLOCK_SHIFT;

/// Mask to extract position within a buffer from a global offset.
pub const BYTE_BLOCK_MASK: usize = BYTE_BLOCK_SIZE - 1;

// ---------------------------------------------------------------------------
// Allocator trait + implementations
// ---------------------------------------------------------------------------

/// Allocates and recycles byte blocks for [`ByteBlockPool`].
pub trait Allocator {
    /// Allocate a new zeroed byte block.
    fn get_byte_block(&mut self) -> Vec<u8>;

    /// Recycle previously allocated blocks. Implementations may free or pool them.
    fn recycle_byte_blocks(&mut self, blocks: &mut [Vec<u8>]);
}

/// Allocator that creates zeroed blocks and never recycles.
#[derive(Debug, Default, MemSize)]
#[mem_size_flat]
pub struct DirectAllocator;

impl Allocator for DirectAllocator {
    fn get_byte_block(&mut self) -> Vec<u8> {
        vec![0u8; BYTE_BLOCK_SIZE]
    }

    fn recycle_byte_blocks(&mut self, _blocks: &mut [Vec<u8>]) {}
}

/// Allocator that tracks total allocated bytes via a shared counter.
#[derive(Debug, Clone)]
pub struct DirectTrackingAllocator {
    bytes_used: Rc<Cell<usize>>,
}

impl DirectTrackingAllocator {
    /// Create a new tracking allocator sharing the given counter.
    pub fn new(bytes_used: Rc<Cell<usize>>) -> Self {
        Self { bytes_used }
    }
}

impl Allocator for DirectTrackingAllocator {
    fn get_byte_block(&mut self) -> Vec<u8> {
        self.bytes_used.set(self.bytes_used.get() + BYTE_BLOCK_SIZE);
        vec![0u8; BYTE_BLOCK_SIZE]
    }

    fn recycle_byte_blocks(&mut self, blocks: &mut [Vec<u8>]) {
        self.bytes_used
            .set(self.bytes_used.get() - blocks.len() * BYTE_BLOCK_SIZE);
    }
}

/// Default maximum number of buffered (recycled) blocks.
pub const DEFAULT_BUFFERED_BLOCKS: usize = 64;

/// Allocator that recycles unused byte blocks in a free list for reuse.
///
/// On [`recycle_byte_blocks`](Allocator::recycle_byte_blocks), blocks are stashed
/// up to `max_buffered_blocks`. On [`get_byte_block`](Allocator::get_byte_block),
/// a recycled block is returned if available, avoiding reallocation. Tracks total
/// allocated bytes via a shared counter.
#[derive(Debug)]
pub struct RecyclingByteBlockAllocator {
    free_byte_blocks: Vec<Vec<u8>>,
    max_buffered_blocks: usize,
    bytes_used: Rc<Cell<usize>>,
}

impl RecyclingByteBlockAllocator {
    /// Create a new recycling allocator with a maximum free list size and shared counter.
    pub fn new(max_buffered_blocks: usize, bytes_used: Rc<Cell<usize>>) -> Self {
        Self {
            free_byte_blocks: Vec::with_capacity(max_buffered_blocks),
            max_buffered_blocks,
            bytes_used,
        }
    }

    /// Create a new recycling allocator with default settings.
    pub fn with_defaults(bytes_used: Rc<Cell<usize>>) -> Self {
        Self::new(DEFAULT_BUFFERED_BLOCKS, bytes_used)
    }

    /// Number of currently buffered (recyclable) blocks.
    pub fn num_buffered_blocks(&self) -> usize {
        self.free_byte_blocks.len()
    }

    /// Total bytes currently tracked by this allocator.
    pub fn bytes_used(&self) -> usize {
        self.bytes_used.get()
    }

    /// Remove up to `num` blocks from the free list, returning how many were freed.
    pub fn free_blocks(&mut self, num: usize) -> usize {
        let count = num.min(self.free_byte_blocks.len());
        self.free_byte_blocks
            .truncate(self.free_byte_blocks.len() - count);
        self.bytes_used
            .set(self.bytes_used.get() - count * BYTE_BLOCK_SIZE);
        count
    }
}

impl Allocator for RecyclingByteBlockAllocator {
    fn get_byte_block(&mut self) -> Vec<u8> {
        if let Some(block) = self.free_byte_blocks.pop() {
            block
        } else {
            self.bytes_used.set(self.bytes_used.get() + BYTE_BLOCK_SIZE);
            vec![0u8; BYTE_BLOCK_SIZE]
        }
    }

    fn recycle_byte_blocks(&mut self, blocks: &mut [Vec<u8>]) {
        let space = self.max_buffered_blocks - self.free_byte_blocks.len();
        let to_recycle = space.min(blocks.len());

        for block in blocks.iter_mut().take(to_recycle) {
            self.free_byte_blocks.push(std::mem::take(block));
        }

        // Blocks beyond the cap are dropped
        let dropped = blocks.len() - to_recycle;
        self.bytes_used
            .set(self.bytes_used.get() - dropped * BYTE_BLOCK_SIZE);
    }
}

// ---------------------------------------------------------------------------
// ByteBlockPool
// ---------------------------------------------------------------------------

/// A pool of fixed-size byte buffers with append and random-access read support.
///
/// Bytes are written sequentially via [`append`](Self::append) and can be read
/// back with [`read_byte`](Self::read_byte) or [`read_bytes`](Self::read_bytes).
/// The pool grows by allocating new buffers from the [`Allocator`].
#[derive(Debug)]
pub struct ByteBlockPool<A: Allocator> {
    /// Allocated buffers.
    buffers: Vec<Vec<u8>>,
    /// Index of the current head buffer, or -1 if no buffer allocated yet.
    buffer_upto: i32,
    /// Write position within the current head buffer.
    byte_upto: usize,
    /// Global byte offset of the start of the current head buffer.
    /// Equal to `buffer_upto * BYTE_BLOCK_SIZE` (or negative before first allocation).
    byte_offset: i32,
    allocator: A,
}

impl<A: Allocator> ByteBlockPool<A> {
    /// Create a new pool with the given allocator. Call [`next_buffer`](Self::next_buffer)
    /// before writing.
    pub fn new(allocator: A) -> Self {
        Self {
            buffers: Vec::new(),
            buffer_upto: -1,
            byte_upto: BYTE_BLOCK_SIZE,
            byte_offset: -(BYTE_BLOCK_SIZE as i32),
            allocator,
        }
    }

    /// Allocate a new buffer and advance the pool to it.
    pub fn next_buffer(&mut self) {
        let block = self.allocator.get_byte_block();
        let new_index = (self.buffer_upto + 1) as usize;
        if new_index == self.buffers.len() {
            self.buffers.push(block);
        } else {
            self.buffers[new_index] = block;
        }
        self.buffer_upto += 1;
        self.byte_upto = 0;
        self.byte_offset = self
            .byte_offset
            .checked_add(BYTE_BLOCK_SIZE as i32)
            .expect("ByteBlockPool byte_offset overflow");
    }

    /// Read a single byte at the given global offset.
    pub fn read_byte(&self, offset: usize) -> u8 {
        let buffer_index = offset >> BYTE_BLOCK_SHIFT;
        let pos = offset & BYTE_BLOCK_MASK;
        self.buffers[buffer_index][pos]
    }

    /// Read bytes starting at `offset` into `dest`.
    pub fn read_bytes(&self, offset: usize, dest: &mut [u8]) {
        let mut bytes_left = dest.len();
        let mut buffer_index = offset >> BYTE_BLOCK_SHIFT;
        let mut pos = offset & BYTE_BLOCK_MASK;
        let mut dest_offset = 0;
        while bytes_left > 0 {
            let chunk = bytes_left.min(BYTE_BLOCK_SIZE - pos);
            dest[dest_offset..dest_offset + chunk]
                .copy_from_slice(&self.buffers[buffer_index][pos..pos + chunk]);
            dest_offset += chunk;
            bytes_left -= chunk;
            buffer_index += 1;
            pos = 0;
        }
    }

    /// Append bytes at the current write position.
    pub fn append(&mut self, data: &[u8]) {
        let mut offset = 0;
        let mut bytes_left = data.len();
        while bytes_left > 0 {
            let buffer_left = BYTE_BLOCK_SIZE - self.byte_upto;
            if bytes_left < buffer_left {
                let buf_idx = self.buffer_upto as usize;
                self.buffers[buf_idx][self.byte_upto..self.byte_upto + bytes_left]
                    .copy_from_slice(&data[offset..offset + bytes_left]);
                self.byte_upto += bytes_left;
                break;
            } else {
                if buffer_left > 0 {
                    let buf_idx = self.buffer_upto as usize;
                    self.buffers[buf_idx][self.byte_upto..self.byte_upto + buffer_left]
                        .copy_from_slice(&data[offset..offset + buffer_left]);
                }
                self.next_buffer();
                bytes_left -= buffer_left;
                offset += buffer_left;
            }
        }
    }

    /// Reset the pool, optionally zero-filling and reusing the first buffer.
    pub fn reset(&mut self, zero_fill: bool, reuse_first: bool) {
        if self.buffer_upto == -1 {
            return;
        }

        if zero_fill {
            for i in 0..self.buffer_upto as usize {
                self.buffers[i].fill(0);
            }
            // Partial zero fill the final buffer
            let last = self.buffer_upto as usize;
            self.buffers[last][..self.byte_upto].fill(0);
        }

        if self.buffer_upto > 0 || !reuse_first {
            let start = if reuse_first { 1 } else { 0 };
            let end = (1 + self.buffer_upto) as usize;
            self.allocator
                .recycle_byte_blocks(&mut self.buffers[start..end]);
        }

        if reuse_first {
            self.buffer_upto = 0;
            self.byte_upto = 0;
            self.byte_offset = 0;
        } else {
            self.buffer_upto = -1;
            self.byte_upto = BYTE_BLOCK_SIZE;
            self.byte_offset = -(BYTE_BLOCK_SIZE as i32);
        }
    }

    /// Current global write position.
    pub fn position(&self) -> usize {
        (self.buffer_upto as usize) * BYTE_BLOCK_SIZE + self.byte_upto
    }

    /// Get an immutable reference to the buffer at the given index.
    pub fn get_buffer(&self, index: usize) -> &[u8] {
        &self.buffers[index]
    }
}

// ---------------------------------------------------------------------------
// ByteSlicePool
// ---------------------------------------------------------------------------

/// Sizes for each slice growth level.
pub const LEVEL_SIZE_ARRAY: [usize; 10] = [5, 14, 20, 30, 40, 40, 80, 80, 120, 200];

/// Next level index for each level (last level stays at max).
pub const NEXT_LEVEL_ARRAY: [usize; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 9];

/// Size of the first slice level.
pub const FIRST_LEVEL_SIZE: usize = LEVEL_SIZE_ARRAY[0];

/// Allocates variable-length byte slices within a [`ByteBlockPool`].
///
/// Slices grow through predefined levels. Each slice ends with a level marker
/// byte (upper nibble = 0x10, lower nibble = level index). When a slice is
/// exhausted, a forwarding address (LE i32) is written in place of the last
/// 4 bytes, pointing to the next slice.
pub struct ByteSlicePool;

impl ByteSlicePool {
    /// Allocate a new initial slice of `size` bytes. Returns the global offset
    /// where the slice starts.
    pub fn new_slice<A: Allocator>(pool: &mut ByteBlockPool<A>, size: usize) -> usize {
        assert!(
            size <= BYTE_BLOCK_SIZE,
            "slice size {size} exceeds block size {BYTE_BLOCK_SIZE}"
        );

        if pool.byte_upto > BYTE_BLOCK_SIZE - size {
            pool.next_buffer();
        }
        let upto = pool.byte_upto;
        pool.byte_upto += size;
        // Write level 0 marker at end of slice
        let buf_idx = pool.buffer_upto as usize;
        pool.buffers[buf_idx][pool.byte_upto - 1] = 0x10; // 16 = level 0
        upto
    }

    /// Grow a slice to its next level. Returns `(data_start, usable_length)` of
    /// the new slice, where `data_start` is the offset within the current head
    /// buffer and `usable_length` is bytes available for data (excluding the
    /// forwarding address reservation and level marker).
    ///
    /// `buffer_index` and `upto` identify the end-of-slice marker position in
    /// the old slice.
    pub fn alloc_known_size_slice<A: Allocator>(
        pool: &mut ByteBlockPool<A>,
        buffer_index: usize,
        upto: usize,
    ) -> (usize, usize) {
        let level = (pool.buffers[buffer_index][upto] & 0x0F) as usize;
        let new_level = NEXT_LEVEL_ARRAY[level];
        let new_size = LEVEL_SIZE_ARRAY[new_level];

        // Maybe allocate another block
        if pool.byte_upto > BYTE_BLOCK_SIZE - new_size {
            pool.next_buffer();
        }

        let new_upto = pool.byte_upto;
        let new_global_offset = new_upto as i32 + pool.byte_offset;
        pool.byte_upto += new_size;

        // Copy forward the past 3 bytes (about to be overwritten by forwarding address)
        // Use a temp array to handle the case where old and new slices are in the same buffer
        let mut temp = [0u8; 3];
        temp.copy_from_slice(&pool.buffers[buffer_index][upto - 3..upto]);

        // Write forwarding address (LE i32) at end of old slice
        let addr_bytes = new_global_offset.to_le_bytes();
        pool.buffers[buffer_index][upto - 3..upto + 1].copy_from_slice(&addr_bytes);

        // Copy the 3 saved bytes to the start of the new slice
        let new_buf_idx = pool.buffer_upto as usize;
        pool.buffers[new_buf_idx][new_upto..new_upto + 3].copy_from_slice(&temp);

        // Write new level marker at end of new slice
        pool.buffers[new_buf_idx][pool.byte_upto - 1] = 0x10 | new_level as u8;

        // Data starts after the 3 copied bytes; usable length excludes level marker
        (new_upto + 3, new_size - 3)
    }

    /// Convenience: grow a slice and return just the new data start offset
    /// within the current head buffer.
    pub fn alloc_slice<A: Allocator>(
        pool: &mut ByteBlockPool<A>,
        buffer_index: usize,
        upto: usize,
    ) -> usize {
        Self::alloc_known_size_slice(pool, buffer_index, upto).0
    }
}

// ---------------------------------------------------------------------------
// ByteSliceWriter
// ---------------------------------------------------------------------------

/// Write cursor for byte slices allocated by [`ByteSlicePool`].
///
/// Tracks the current write position and automatically grows the slice when the
/// end-of-slice marker is hit. Provides `write_byte`, `write_bytes`, and
/// `write_vint` methods.
pub struct ByteSliceWriter {
    /// Index into `ByteBlockPool.buffers` for the current write position.
    buffer_index: usize,
    /// Offset within the current buffer.
    upto: usize,
}

impl ByteSliceWriter {
    /// Create a new writer starting at the given global offset (as returned by
    /// [`ByteSlicePool::new_slice`]).
    pub fn new<A: Allocator>(pool: &ByteBlockPool<A>, start_offset: usize) -> Self {
        let global = start_offset + pool.byte_offset as usize;
        Self {
            buffer_index: global >> BYTE_BLOCK_SHIFT,
            upto: global & BYTE_BLOCK_MASK,
        }
    }

    /// Write a single byte, growing the slice if needed.
    pub fn write_byte<A: Allocator>(&mut self, pool: &mut ByteBlockPool<A>, b: u8) {
        if pool.buffers[self.buffer_index][self.upto] != 0 {
            // End of slice marker — grow
            let new_offset = ByteSlicePool::alloc_slice(pool, self.buffer_index, self.upto);
            let new_buf_idx = pool.buffer_upto as usize;
            self.buffer_index = new_buf_idx;
            self.upto = new_offset;
        }
        pool.buffers[self.buffer_index][self.upto] = b;
        self.upto += 1;
    }

    /// Write multiple bytes, growing the slice as needed.
    pub fn write_bytes<A: Allocator>(&mut self, pool: &mut ByteBlockPool<A>, data: &[u8]) {
        let mut offset = 0;
        let end = data.len();

        // Write into current slice while there's room
        while pool.buffers[self.buffer_index][self.upto] == 0 && offset < end {
            pool.buffers[self.buffer_index][self.upto] = data[offset];
            self.upto += 1;
            offset += 1;
        }

        // If we still have data, grow slices as needed
        while offset < end {
            let (slice_offset, slice_length) =
                ByteSlicePool::alloc_known_size_slice(pool, self.buffer_index, self.upto);
            let new_buf_idx = pool.buffer_upto as usize;
            self.buffer_index = new_buf_idx;
            // Write as much as fits (reserve last byte for potential end marker check)
            let write_length = (slice_length - 1).min(end - offset);
            pool.buffers[self.buffer_index][slice_offset..slice_offset + write_length]
                .copy_from_slice(&data[offset..offset + write_length]);
            self.upto = slice_offset + write_length;
            offset += write_length;
        }
    }

    /// Write an integer in variable-length encoding (1-5 bytes, 7 bits per byte).
    pub fn write_vint<A: Allocator>(&mut self, pool: &mut ByteBlockPool<A>, mut i: i32) {
        while (i & !0x7F) != 0 {
            self.write_byte(pool, ((i & 0x7F) | 0x80) as u8);
            i = ((i as u32) >> 7) as i32;
        }
        self.write_byte(pool, i as u8);
    }

    /// Current global write address.
    pub fn address(&self) -> usize {
        self.upto + self.buffer_index * BYTE_BLOCK_SIZE
    }
}

// ---------------------------------------------------------------------------
// ByteSliceReader
// ---------------------------------------------------------------------------

/// Read cursor that follows the forwarding chain of byte slices.
///
/// Borrows the [`ByteBlockPool`] immutably for its lifetime. Created after
/// all writes are complete.
pub struct ByteSliceReader<'a, A: Allocator> {
    pool: &'a ByteBlockPool<A>,
    /// Current buffer index in the pool.
    buffer_upto: usize,
    /// Current read position within the buffer.
    upto: usize,
    /// Limit within the current buffer (exclusive) — data bytes only.
    limit: usize,
    /// Current slice level (used to determine next level on forwarding).
    level: usize,
    /// Global offset of the start of the current buffer.
    buffer_offset: usize,
    /// Global end offset of all data across all slices.
    end_index: usize,
}

impl<'a, A: Allocator> ByteSliceReader<'a, A> {
    /// Create a reader over slices starting at `start_index` (global offset of
    /// first slice) through `end_index` (global offset of last written byte + 1).
    pub fn new(pool: &'a ByteBlockPool<A>, start_index: usize, end_index: usize) -> Self {
        debug_assert!(end_index >= start_index);

        let buffer_upto = start_index / BYTE_BLOCK_SIZE;
        let buffer_offset = buffer_upto * BYTE_BLOCK_SIZE;
        let upto = start_index & BYTE_BLOCK_MASK;

        let limit = if start_index + FIRST_LEVEL_SIZE >= end_index {
            // Single slice — limit is the end
            end_index & BYTE_BLOCK_MASK
        } else {
            // Multiple slices — first slice data ends 4 bytes before the end
            // (last 4 bytes are forwarding address + level marker)
            upto + FIRST_LEVEL_SIZE - 4
        };

        Self {
            pool,
            buffer_upto,
            upto,
            limit,
            level: 0,
            buffer_offset,
            end_index,
        }
    }

    /// Returns true if all bytes have been read.
    pub fn eof(&self) -> bool {
        self.upto + self.buffer_offset == self.end_index
    }

    /// Read a single byte, advancing the cursor.
    pub fn read_byte(&mut self) -> u8 {
        debug_assert!(!self.eof());
        if self.upto == self.limit {
            self.next_slice();
        }
        let b = self.pool.buffers[self.buffer_upto][self.upto];
        self.upto += 1;
        b
    }

    /// Read `dest.len()` bytes into `dest`.
    pub fn read_bytes(&mut self, dest: &mut [u8]) {
        let mut offset = 0;
        let mut remaining = dest.len();
        while remaining > 0 {
            let available = self.limit - self.upto;
            if available < remaining {
                dest[offset..offset + available].copy_from_slice(
                    &self.pool.buffers[self.buffer_upto][self.upto..self.upto + available],
                );
                offset += available;
                remaining -= available;
                self.next_slice();
            } else {
                dest[offset..offset + remaining].copy_from_slice(
                    &self.pool.buffers[self.buffer_upto][self.upto..self.upto + remaining],
                );
                self.upto += remaining;
                break;
            }
        }
    }

    /// Skip `n` bytes forward.
    pub fn skip_bytes(&mut self, mut n: usize) {
        while n > 0 {
            let available = self.limit - self.upto;
            if available < n {
                n -= available;
                self.next_slice();
            } else {
                self.upto += n;
                break;
            }
        }
    }

    /// Write all remaining data to the given writer. Returns bytes written.
    pub fn write_to(&mut self, out: &mut dyn io::Write) -> io::Result<usize> {
        let mut size = 0;
        loop {
            if self.limit + self.buffer_offset == self.end_index {
                // Final slice
                let count = self.limit - self.upto;
                out.write_all(&self.pool.buffers[self.buffer_upto][self.upto..self.upto + count])?;
                size += count;
                self.upto = self.limit;
                break;
            } else {
                let count = self.limit - self.upto;
                out.write_all(&self.pool.buffers[self.buffer_upto][self.upto..self.upto + count])?;
                size += count;
                self.next_slice();
            }
        }
        Ok(size)
    }

    /// Follow the forwarding address to the next slice.
    fn next_slice(&mut self) {
        // Read LE i32 forwarding address from the limit position
        let buf = &self.pool.buffers[self.buffer_upto];
        let next_index = i32::from_le_bytes([
            buf[self.limit],
            buf[self.limit + 1],
            buf[self.limit + 2],
            buf[self.limit + 3],
        ]) as usize;

        self.level = NEXT_LEVEL_ARRAY[self.level];
        let new_size = LEVEL_SIZE_ARRAY[self.level];

        self.buffer_upto = next_index / BYTE_BLOCK_SIZE;
        self.buffer_offset = self.buffer_upto * BYTE_BLOCK_SIZE;
        self.upto = next_index & BYTE_BLOCK_MASK;

        if next_index + new_size >= self.end_index {
            // Final slice
            self.limit = self.end_index - self.buffer_offset;
        } else {
            // Not final — reserve 4 bytes for forwarding address
            self.limit = self.upto + new_size - 4;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;

    // Ported from:
    //   org.apache.lucene.util.TestByteBlockPool
    //   org.apache.lucene.index.TestByteSliceReader
    //   org.apache.lucene.util.TestRecyclingByteBlockAllocator

    #[test]
    fn test_append_and_read_roundtrip() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        let data: Vec<u8> = (0..=255).collect();
        pool.append(&data);

        let mut result = vec![0u8; 256];
        pool.read_bytes(0, &mut result);
        assert_eq!(data, result);
    }

    #[test]
    fn test_read_byte() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        pool.append(&[0xAB, 0xCD, 0xEF]);
        assert_eq!(pool.read_byte(0), 0xAB);
        assert_eq!(pool.read_byte(1), 0xCD);
        assert_eq!(pool.read_byte(2), 0xEF);
    }

    #[test]
    fn test_append_across_block_boundary() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        // Fill most of the first block
        let fill = vec![0xAA; BYTE_BLOCK_SIZE - 10];
        pool.append(&fill);
        assert_eq!(pool.position(), BYTE_BLOCK_SIZE - 10);

        // Write 20 bytes that span the boundary
        let cross: Vec<u8> = (0..20).collect();
        pool.append(&cross);
        assert_eq!(pool.position(), BYTE_BLOCK_SIZE + 10);

        // Read them back
        let mut result = vec![0u8; 20];
        pool.read_bytes(BYTE_BLOCK_SIZE - 10, &mut result);
        assert_eq!(cross, result);
    }

    #[test]
    fn test_large_random_blocks() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        let mut items: Vec<Vec<u8>> = Vec::new();
        let mut total: usize = 0;

        for i in 0..50 {
            let size = if i % 2 == 0 { 500 + i * 10 } else { 60000 };
            let data: Vec<u8> = (0..size).map(|j| (j % 256) as u8).collect();
            pool.append(&data);
            total += size;
            assert_eq!(pool.position(), total);
            items.push(data);
        }

        let mut pos = 0;
        for expected in &items {
            let mut actual = vec![0u8; expected.len()];
            pool.read_bytes(pos, &mut actual);
            assert_eq!(expected, &actual);
            pos += expected.len();
        }
    }

    #[test]
    fn test_position() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();
        assert_eq!(pool.position(), 0);

        pool.append(&[1, 2, 3]);
        assert_eq!(pool.position(), 3);
    }

    #[test]
    fn test_reset_reuse_first() {
        let counter = Rc::new(Cell::new(0usize));
        let mut pool = ByteBlockPool::new(DirectTrackingAllocator::new(counter.clone()));
        pool.next_buffer();
        assert_eq!(counter.get(), BYTE_BLOCK_SIZE);

        pool.append(&[1, 2, 3]);
        pool.reset(true, true);

        assert_eq!(counter.get(), BYTE_BLOCK_SIZE);
        assert_eq!(pool.position(), 0);
        // Buffer should be zeroed
        assert_eq!(pool.read_byte(0), 0);
    }

    #[test]
    fn test_reset_no_reuse() {
        let counter = Rc::new(Cell::new(0usize));
        let mut pool = ByteBlockPool::new(DirectTrackingAllocator::new(counter.clone()));
        pool.next_buffer();
        assert_eq!(counter.get(), BYTE_BLOCK_SIZE);

        pool.reset(false, false);
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_tracking_allocator_multi_block() {
        let counter = Rc::new(Cell::new(0usize));
        let mut pool = ByteBlockPool::new(DirectTrackingAllocator::new(counter.clone()));
        pool.next_buffer();
        pool.next_buffer();
        pool.next_buffer();
        assert_eq!(counter.get(), 3 * BYTE_BLOCK_SIZE);

        pool.reset(false, true);
        // Reused first, recycled 2
        assert_eq!(counter.get(), BYTE_BLOCK_SIZE);
    }

    #[test]
    fn test_overflow_detection() {
        // Use a minimal allocator to test overflow without wasting memory
        struct TinyAllocator;
        impl Allocator for TinyAllocator {
            fn get_byte_block(&mut self) -> Vec<u8> {
                Vec::new()
            }
            fn recycle_byte_blocks(&mut self, _blocks: &mut [Vec<u8>]) {}
        }

        let mut pool = ByteBlockPool::new(TinyAllocator);
        pool.next_buffer();

        let mut overflowed = false;
        // i32::MAX / BYTE_BLOCK_SIZE + 1 iterations to overflow byte_offset
        for _ in 0..i32::MAX as usize / BYTE_BLOCK_SIZE + 1 {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pool.next_buffer();
            }));
            if result.is_err() {
                overflowed = true;
                break;
            }
        }
        assert!(overflowed, "byte_offset should overflow");
    }

    #[test]
    fn test_new_slice_writes_level_marker() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        let offset = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);
        assert_eq!(offset, 0);
        // Level marker at end of slice: 0x10 = level 0
        assert_eq!(pool.buffers[0][FIRST_LEVEL_SIZE - 1], 0x10);
    }

    #[test]
    fn test_level_progression() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        let offset = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);

        // Write 1 data byte (first slice has 1 usable byte before marker)
        pool.buffers[0][offset] = 0x42;

        // Now grow: upto points to the level marker
        let (data_start, usable) =
            ByteSlicePool::alloc_known_size_slice(&mut pool, 0, offset + FIRST_LEVEL_SIZE - 1);

        // Level 1 has size 14, usable = 14 - 3 = 11
        assert_eq!(usable, 11);
        assert_gt!(data_start, 0);
    }

    #[test]
    fn test_forwarding_chain() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        // Allocate initial slice
        let _offset = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);

        // Grow through several levels
        let mut buf_idx: usize = 0;
        let mut upto = FIRST_LEVEL_SIZE - 1; // level marker position

        for expected_level in 1..=5 {
            let new_upto = ByteSlicePool::alloc_slice(&mut pool, buf_idx, upto);
            buf_idx = pool.buffer_upto as usize;
            // The new slice should have the correct level marker at its end
            let new_size = LEVEL_SIZE_ARRAY[expected_level];
            let marker_pos = new_upto + new_size - 3 - 1;
            assert_eq!(
                pool.buffers[buf_idx][marker_pos] & 0x0F,
                expected_level as u8
            );
            upto = marker_pos;
        }
    }

    #[test]
    fn test_cross_block_slice_allocation() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        // Fill up most of the block
        let fill_size = BYTE_BLOCK_SIZE - 3;
        pool.byte_upto = fill_size;

        // Allocate a slice that won't fit — should trigger new block
        let offset = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);
        assert_eq!(pool.buffer_upto, 1);
        assert_eq!(offset, 0); // Start of new block
    }

    #[test]
    fn test_writer_single_bytes() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        let offset = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);
        let mut writer = ByteSliceWriter::new(&pool, offset);

        // First slice (level 0, size 5): 1 data byte usable
        writer.write_byte(&mut pool, 0xAA);
        // This write should trigger slice growth
        writer.write_byte(&mut pool, 0xBB);

        // Verify by reading the slice chain
        let end = writer.address();
        let mut reader = ByteSliceReader::new(&pool, offset + pool.byte_offset as usize, end);
        assert_eq!(reader.read_byte(), 0xAA);
        assert_eq!(reader.read_byte(), 0xBB);
        assert!(reader.eof());
    }

    #[test]
    fn test_writer_bulk_write() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        let offset = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);
        let mut writer = ByteSliceWriter::new(&pool, offset);

        let data: Vec<u8> = (0..100).collect();
        writer.write_bytes(&mut pool, &data);

        let end = writer.address();
        let start = offset + pool.byte_offset as usize;

        // Verify we didn't truncate: the start should be 0 since byte_offset
        // was BYTE_BLOCK_SIZE after next_buffer for a fresh pool
        // Actually, after next_buffer, byte_offset = 0
        let mut reader = ByteSliceReader::new(&pool, start, end);
        let mut result = vec![0u8; 100];
        reader.read_bytes(&mut result);
        assert_eq!(data, result);
        assert!(reader.eof());
    }

    #[test]
    fn test_writer_vint_roundtrip() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        let offset = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);
        let mut writer = ByteSliceWriter::new(&pool, offset);

        let values = [0, 1, 127, 128, 16383, 16384, 2097151, i32::MAX];
        for &v in &values {
            writer.write_vint(&mut pool, v);
        }

        let end = writer.address();
        let start = offset + pool.byte_offset as usize;
        let mut reader = ByteSliceReader::new(&pool, start, end);

        for &expected in &values {
            let actual = read_vint(&mut reader);
            assert_eq!(expected, actual, "vint mismatch");
        }
        assert!(reader.eof());
    }

    #[test]
    fn test_two_interleaved_writers() {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        let offset1 = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);
        let mut writer1 = ByteSliceWriter::new(&pool, offset1);
        let start1 = offset1 + pool.byte_offset as usize;

        let offset2 = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);
        let mut writer2 = ByteSliceWriter::new(&pool, offset2);
        let start2 = offset2 + pool.byte_offset as usize;

        // Interleave writes
        for i in 0u8..50 {
            writer1.write_byte(&mut pool, i);
            writer2.write_byte(&mut pool, 200 + i);
        }

        let end1 = writer1.address();
        let end2 = writer2.address();

        // Read back stream 1
        let mut reader1 = ByteSliceReader::new(&pool, start1, end1);
        for i in 0u8..50 {
            assert_eq!(reader1.read_byte(), i);
        }
        assert!(reader1.eof());

        // Read back stream 2
        let mut reader2 = ByteSliceReader::new(&pool, start2, end2);
        for i in 0u8..50 {
            assert_eq!(reader2.read_byte(), 200 + i);
        }
        assert!(reader2.eof());
    }

    #[test]
    fn test_reader_byte_by_byte() {
        let data: Vec<u8> = (0..200).map(|i| (i % 256) as u8).collect();
        let (pool, start, end) = write_slice_data(&data);

        let mut reader = ByteSliceReader::new(&pool, start, end);
        for &expected in &data {
            assert!(!reader.eof());
            assert_eq!(reader.read_byte(), expected);
        }
        assert!(reader.eof());
    }

    #[test]
    fn test_reader_bulk_read() {
        let data: Vec<u8> = (0..300).map(|i| (i % 256) as u8).collect();
        let (pool, start, end) = write_slice_data(&data);

        let mut reader = ByteSliceReader::new(&pool, start, end);
        let mut result = vec![0u8; data.len()];
        reader.read_bytes(&mut result);
        assert_eq!(data, result);
        assert!(reader.eof());
    }

    #[test]
    fn test_reader_skip_bytes() {
        let data: Vec<u8> = (0..100).collect();
        let (pool, start, end) = write_slice_data(&data);

        let mut reader = ByteSliceReader::new(&pool, start, end);
        reader.skip_bytes(10);
        assert_eq!(reader.read_byte(), 10);

        reader.skip_bytes(20);
        assert_eq!(reader.read_byte(), 31);
    }

    #[test]
    fn test_reader_write_to() {
        let data: Vec<u8> = (0..500).map(|i| (i % 256) as u8).collect();
        let (pool, start, end) = write_slice_data(&data);

        let mut reader = ByteSliceReader::new(&pool, start, end);
        let mut output = Vec::new();
        let bytes_written = reader.write_to(&mut output).unwrap();
        assert_len_eq_x!(&data, bytes_written);
        assert_eq!(data, output);
    }

    #[test]
    fn test_reader_eof_empty_data() {
        // Write zero data bytes — just allocate a slice
        let data: Vec<u8> = Vec::new();
        let (pool, start, end) = write_slice_data(&data);

        let reader = ByteSliceReader::new(&pool, start, end);
        assert!(reader.eof());
    }

    #[test]
    fn test_reader_matches_java_write_pattern() {
        // Mimics the Java test's pattern of writing bytes using the low-level
        // buffer[upto] check approach (same as TermsHashPerField).
        let data: Vec<u8> = (0..150).map(|i| (i * 7 + 3) as u8).collect();

        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        let upto_start = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);
        let mut buf_idx: usize = 0;
        let mut upto = upto_start;

        for &b in &data {
            if (pool.buffers[buf_idx][upto] & 16) != 0 {
                upto = ByteSlicePool::alloc_slice(&mut pool, buf_idx, upto);
                buf_idx = pool.buffer_upto as usize;
            }
            pool.buffers[buf_idx][upto] = b;
            upto += 1;
        }

        let end = upto + buf_idx * BYTE_BLOCK_SIZE;

        // Read byte by byte
        let mut reader = ByteSliceReader::new(&pool, upto_start, end);
        for &expected in &data {
            assert_eq!(reader.read_byte(), expected);
        }
        assert!(reader.eof());
    }

    /// Write data into a slice chain using ByteSliceWriter and return the pool
    /// along with start/end indices for reading.
    fn write_slice_data(data: &[u8]) -> (ByteBlockPool<DirectAllocator>, usize, usize) {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();

        let offset = ByteSlicePool::new_slice(&mut pool, FIRST_LEVEL_SIZE);
        let start = offset + pool.byte_offset as usize;
        let mut writer = ByteSliceWriter::new(&pool, offset);

        for &b in data {
            writer.write_byte(&mut pool, b);
        }

        let end = writer.address();
        (pool, start, end)
    }

    /// Read a VInt from a ByteSliceReader (for test verification).
    fn read_vint<A: Allocator>(reader: &mut ByteSliceReader<A>) -> i32 {
        let mut b = reader.read_byte();
        let mut result = (b & 0x7F) as i32;
        let mut shift = 7;
        while (b & 0x80) != 0 {
            b = reader.read_byte();
            result |= ((b & 0x7F) as i32) << shift;
            shift += 7;
        }
        result
    }

    #[test]
    fn test_recycling_alloc_reuses_blocks() {
        let counter = Rc::new(Cell::new(0usize));
        let mut pool = ByteBlockPool::new(RecyclingByteBlockAllocator::new(2, counter.clone()));
        pool.next_buffer();
        pool.next_buffer();
        assert_eq!(counter.get(), 2 * BYTE_BLOCK_SIZE);

        // Reset with reuse_first=false — both blocks recycled into free list
        pool.reset(false, false);
        assert_eq!(pool.allocator.num_buffered_blocks(), 2);
        // Counter unchanged: blocks are recycled, not freed
        assert_eq!(counter.get(), 2 * BYTE_BLOCK_SIZE);

        // Allocate again — should reuse recycled blocks, no new allocation
        pool.next_buffer();
        pool.next_buffer();
        assert_eq!(counter.get(), 2 * BYTE_BLOCK_SIZE);
        assert_eq!(pool.allocator.num_buffered_blocks(), 0);
    }

    #[test]
    fn test_recycling_exceeds_max_buffered() {
        let counter = Rc::new(Cell::new(0usize));
        let max_buffered = 2;
        let mut pool = ByteBlockPool::new(RecyclingByteBlockAllocator::new(
            max_buffered,
            counter.clone(),
        ));

        // Allocate 4 blocks
        pool.next_buffer();
        pool.next_buffer();
        pool.next_buffer();
        pool.next_buffer();
        assert_eq!(counter.get(), 4 * BYTE_BLOCK_SIZE);

        // Reset — only 2 fit in the free list, 2 are dropped
        pool.reset(false, false);
        assert_eq!(pool.allocator.num_buffered_blocks(), max_buffered);
        // 2 blocks dropped, counter decremented for those
        assert_eq!(counter.get(), 2 * BYTE_BLOCK_SIZE);
    }

    #[test]
    fn test_recycling_free_blocks() {
        let counter = Rc::new(Cell::new(0usize));
        let mut alloc = RecyclingByteBlockAllocator::new(10, counter.clone());

        // Allocate and recycle 5 blocks
        let mut blocks: Vec<Vec<u8>> = (0..5).map(|_| alloc.get_byte_block()).collect();
        assert_eq!(counter.get(), 5 * BYTE_BLOCK_SIZE);

        alloc.recycle_byte_blocks(&mut blocks);
        assert_eq!(alloc.num_buffered_blocks(), 5);
        assert_eq!(counter.get(), 5 * BYTE_BLOCK_SIZE);

        // Free 3 of them
        let freed = alloc.free_blocks(3);
        assert_eq!(freed, 3);
        assert_eq!(alloc.num_buffered_blocks(), 2);
        assert_eq!(counter.get(), 2 * BYTE_BLOCK_SIZE);

        // Free more than available
        let freed = alloc.free_blocks(10);
        assert_eq!(freed, 2);
        assert_eq!(alloc.num_buffered_blocks(), 0);
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_recycling_zeroed_on_reuse() {
        let counter = Rc::new(Cell::new(0usize));
        let mut pool = ByteBlockPool::new(RecyclingByteBlockAllocator::new(4, counter.clone()));
        pool.next_buffer();

        // Write data, then reset with zero_fill
        pool.append(&[0xFF; 100]);
        pool.reset(true, false);

        // Reuse recycled block — it should be zeroed
        pool.next_buffer();
        for i in 0..100 {
            assert_eq!(pool.read_byte(i), 0, "byte at offset {i} should be zero");
        }
    }

    #[test]
    fn test_recycling_with_defaults() {
        let counter = Rc::new(Cell::new(0usize));
        let alloc = RecyclingByteBlockAllocator::with_defaults(counter);
        assert_eq!(alloc.num_buffered_blocks(), 0);
        assert_eq!(alloc.bytes_used(), 0);
    }

    #[test]
    fn test_recycling_data_integrity_across_reuse() {
        let counter = Rc::new(Cell::new(0usize));
        let mut pool = ByteBlockPool::new(RecyclingByteBlockAllocator::new(4, counter.clone()));

        for round in 0u8..3 {
            pool.next_buffer();
            let data: Vec<u8> = (0..500)
                .map(|i| (i as u8).wrapping_add(round * 37))
                .collect();
            pool.append(&data);

            let mut result = vec![0u8; 500];
            pool.read_bytes(0, &mut result);
            assert_eq!(data, result, "round {round} data mismatch");

            pool.reset(true, false);
        }
    }
}
