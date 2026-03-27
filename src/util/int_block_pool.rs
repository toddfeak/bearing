// SPDX-License-Identifier: Apache-2.0

//! A pool for int blocks similar to [`ByteBlockPool`](super::byte_block_pool::ByteBlockPool).

/// Shift for block size (2^13 = 8192).
pub const INT_BLOCK_SHIFT: u32 = 13;
/// Number of ints per block.
pub const INT_BLOCK_SIZE: usize = 1 << INT_BLOCK_SHIFT;
/// Mask for extracting position within a block.
pub const INT_BLOCK_MASK: usize = INT_BLOCK_SIZE - 1;

/// Arena allocator for `i32` values in fixed-size blocks.
///
/// Blocks of 8192 ints are allocated on demand. The pool provides
/// a growing flat address space over the block array.
#[derive(Debug)]
pub struct IntBlockPool {
    /// Array of buffers currently used in the pool.
    pub buffers: Vec<Vec<i32>>,
    /// Index into buffers pointing to the current head buffer.
    buffer_upto: i32,
    /// Pointer to the current position in the head buffer.
    pub int_upto: usize,
    /// Current head offset (global offset of the start of the current buffer).
    pub int_offset: i32,
}

impl IntBlockPool {
    /// Creates a new empty `IntBlockPool`.
    pub fn new() -> Self {
        Self {
            buffers: Vec::with_capacity(10),
            buffer_upto: -1,
            int_upto: INT_BLOCK_SIZE,
            int_offset: -(INT_BLOCK_SIZE as i32),
        }
    }

    /// Advances the pool to its next buffer.
    ///
    /// This method should be called once after the constructor to initialize
    /// the pool. After a [`reset`](Self::reset) call with `reuse_first=true`,
    /// the pool is already positioned at the first buffer.
    pub fn next_buffer(&mut self) {
        let next_index = (self.buffer_upto + 1) as usize;
        if next_index == self.buffers.len() {
            self.buffers.push(vec![0i32; INT_BLOCK_SIZE]);
        } else {
            // Buffer slot already exists (from prior use), ensure it's allocated
            if self.buffers[next_index].is_empty() {
                self.buffers[next_index] = vec![0i32; INT_BLOCK_SIZE];
            }
        }
        self.buffer_upto += 1;

        self.int_upto = 0;
        self.int_offset = self
            .int_offset
            .checked_add(INT_BLOCK_SIZE as i32)
            .expect("int_offset overflow");
    }

    /// Resets the pool to its initial state.
    ///
    /// - `zero_fill_buffers`: if `true`, buffers are filled with 0 before recycling.
    /// - `reuse_first`: if `true`, the first buffer is reused and the pool is
    ///   positioned at it.
    pub fn reset(&mut self, zero_fill_buffers: bool, reuse_first: bool) {
        if self.buffer_upto != -1 {
            if zero_fill_buffers {
                for i in 0..self.buffer_upto as usize {
                    self.buffers[i].fill(0);
                }
                // Partial zero fill the final buffer
                let upto = self.int_upto;
                self.buffers[self.buffer_upto as usize][..upto].fill(0);
            }

            if self.buffer_upto > 0 || !reuse_first {
                let offset = if reuse_first { 1 } else { 0 };
                // Drop recycled buffers
                for i in offset..=self.buffer_upto as usize {
                    self.buffers[i] = Vec::new();
                }
            }
            if reuse_first {
                self.buffer_upto = 0;
                self.int_upto = 0;
                self.int_offset = 0;
            } else {
                self.buffer_upto = -1;
                self.int_upto = INT_BLOCK_SIZE;
                self.int_offset = -(INT_BLOCK_SIZE as i32);
            }
        }
    }

    /// Returns the current head buffer, or panics if no buffer has been allocated.
    pub fn buffer(&self) -> &[i32] {
        &self.buffers[self.buffer_upto as usize]
    }

    /// Returns the current head buffer mutably.
    pub fn buffer_mut(&mut self) -> &mut [i32] {
        &mut self.buffers[self.buffer_upto as usize]
    }
}

impl Default for IntBlockPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;

    #[test]
    fn test_new_pool() {
        let pool = IntBlockPool::new();
        assert_eq!(pool.buffer_upto, -1);
        assert_eq!(pool.int_upto, INT_BLOCK_SIZE);
        assert_eq!(pool.int_offset, -(INT_BLOCK_SIZE as i32));
    }

    #[test]
    fn test_next_buffer() {
        let mut pool = IntBlockPool::new();
        pool.next_buffer();

        assert_eq!(pool.buffer_upto, 0);
        assert_eq!(pool.int_upto, 0);
        assert_eq!(pool.int_offset, 0);
        assert_len_eq_x!(pool.buffer(), INT_BLOCK_SIZE);
    }

    #[test]
    fn test_multiple_buffers() {
        let mut pool = IntBlockPool::new();
        pool.next_buffer();
        assert_eq!(pool.int_offset, 0);

        pool.int_upto = INT_BLOCK_SIZE; // simulate filling first buffer
        pool.next_buffer();
        assert_eq!(pool.buffer_upto, 1);
        assert_eq!(pool.int_offset, INT_BLOCK_SIZE as i32);
        assert_eq!(pool.int_upto, 0);
    }

    #[test]
    fn test_write_and_read_across_blocks() {
        let mut pool = IntBlockPool::new();
        pool.next_buffer();

        // Write values across a block boundary
        let total = INT_BLOCK_SIZE + 100;
        for i in 0..total {
            if pool.int_upto == INT_BLOCK_SIZE {
                pool.next_buffer();
            }
            let upto = pool.int_upto;
            pool.buffer_mut()[upto] = i as i32;
            pool.int_upto += 1;
        }

        // Read back from first block
        for i in 0..INT_BLOCK_SIZE {
            assert_eq!(pool.buffers[0][i], i as i32);
        }
        // Read back from second block
        for i in 0..100 {
            assert_eq!(pool.buffers[1][i], (INT_BLOCK_SIZE + i) as i32);
        }
    }

    #[test]
    fn test_reset_reuse_first() {
        let mut pool = IntBlockPool::new();
        pool.next_buffer();
        pool.buffer_mut()[0] = 42;
        pool.int_upto = 1;
        pool.next_buffer();

        pool.reset(true, true);

        assert_eq!(pool.buffer_upto, 0);
        assert_eq!(pool.int_upto, 0);
        assert_eq!(pool.int_offset, 0);
        // First buffer should be zeroed
        assert_eq!(pool.buffer()[0], 0);
    }

    #[test]
    fn test_reset_no_reuse() {
        let mut pool = IntBlockPool::new();
        pool.next_buffer();
        pool.int_upto = 10;

        pool.reset(false, false);

        assert_eq!(pool.buffer_upto, -1);
        assert_eq!(pool.int_upto, INT_BLOCK_SIZE);
        assert_eq!(pool.int_offset, -(INT_BLOCK_SIZE as i32));
    }

    #[test]
    fn test_constants() {
        assert_eq!(INT_BLOCK_SIZE, 8192);
        assert_eq!(INT_BLOCK_MASK, 8191);
        assert_eq!(INT_BLOCK_SHIFT, 13);
    }
}
