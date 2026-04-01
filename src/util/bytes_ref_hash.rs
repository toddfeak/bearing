// SPDX-License-Identifier: Apache-2.0

//! Hash map for deduplicating byte sequences into compact integer IDs.
//!
//! Byte data is stored in a contiguous pool (length-prefixed), and the hash
//! table maps byte sequences to sequential integer IDs using open addressing
//! with linear probing. High bits of the hash code are stored alongside the
//! ID for fast rejection during probing.

use crate::util::byte_block_pool::{
    BYTE_BLOCK_MASK, BYTE_BLOCK_SHIFT, BYTE_BLOCK_SIZE, ByteBlockPool, DirectAllocator,
};

/// Default initial capacity of the hash table.
pub const DEFAULT_CAPACITY: usize = 16;

/// Hash map that interns byte sequences into a [`ByteBlockPool`] and assigns
/// sequential integer IDs.
///
/// Each unique byte sequence gets an ID starting at 0. The bytes are stored
/// length-prefixed in the pool (1 byte for lengths < 128, 2 bytes otherwise).
#[derive(mem_dbg::MemSize)]
pub struct BytesRefHash {
    pool: ByteBlockPool<DirectAllocator>,
    bytes_start: Vec<i32>,
    hash_size: usize,
    hash_half_size: usize,
    hash_mask: i32,
    high_mask: i32,
    count: usize,
    last_count: i32,
    ids: Vec<i32>,
}

impl BytesRefHash {
    /// Creates a new `BytesRefHash` with the given pool and initial capacity.
    ///
    /// The `BytesRefHash` takes ownership of the pool and uses it to store
    /// length-prefixed term bytes. In Java, this pool is the shared `termBytePool`
    /// passed from `TermsHash`.
    ///
    /// # Panics
    /// Panics if `capacity` is not a positive power of two.
    pub fn new(pool: ByteBlockPool<DirectAllocator>, capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");
        assert!(
            capacity.is_power_of_two(),
            "capacity must be a power of two, got {capacity}"
        );
        let hash_size = capacity;
        let hash_mask = (hash_size - 1) as i32;
        Self {
            pool,
            bytes_start: Vec::new(),
            hash_size,
            hash_half_size: hash_size >> 1,
            hash_mask,
            high_mask: !hash_mask,
            count: 0,
            last_count: -1,
            ids: vec![-1i32; hash_size],
        }
    }

    /// Creates a new `BytesRefHash` with the given pool and default capacity (16).
    pub fn with_pool(pool: ByteBlockPool<DirectAllocator>) -> Self {
        Self::new(pool, DEFAULT_CAPACITY)
    }

    /// Creates a new `BytesRefHash` that creates its own pool.
    pub fn with_default_capacity() -> Self {
        let mut pool = ByteBlockPool::new(DirectAllocator);
        pool.next_buffer();
        Self::new(pool, DEFAULT_CAPACITY)
    }

    /// Returns the number of unique byte sequences in this hash.
    pub fn size(&self) -> usize {
        self.count
    }

    /// Returns the bytes for the given ID.
    ///
    /// # Panics
    /// Panics if `bytes_id` is out of range.
    pub fn get(&self, bytes_id: usize) -> &[u8] {
        let start = self.bytes_start[bytes_id] as usize;
        self.read_bytes_at(start)
    }

    /// Adds a byte sequence. Returns the new ID (>= 0) if the bytes are new,
    /// or `-(existing_id) - 1` if already present.
    pub fn add(&mut self, bytes: &[u8]) -> i32 {
        let hashcode = do_hash(bytes);
        let hash_pos = self.find_hash(bytes, hashcode);
        let e = self.ids[hash_pos];

        if e == -1 {
            // new entry
            if self.count >= self.bytes_start.len() {
                self.bytes_start
                    .resize(grow_size(self.bytes_start.len().max(1)), 0);
            }
            self.bytes_start[self.count] = self.add_bytes_to_pool(bytes);
            let new_id = self.count as i32;
            self.count += 1;
            assert!(self.ids[hash_pos] == -1);
            self.ids[hash_pos] = new_id | (hashcode & self.high_mask);

            if self.count == self.hash_half_size {
                self.rehash(2 * self.hash_size, true);
            }
            return new_id;
        }
        let existing_id = e & self.hash_mask;
        -(existing_id + 1)
    }

    /// Looks up a byte sequence. Returns the ID if found, or -1 if not present.
    pub fn find(&self, bytes: &[u8]) -> i32 {
        let hashcode = do_hash(bytes);
        let id = self.ids[self.find_hash(bytes, hashcode)];
        if id == -1 { -1 } else { id & self.hash_mask }
    }

    /// Returns the `bytes_start` offset for the given ID.
    pub fn byte_start(&self, bytes_id: usize) -> i32 {
        assert!(bytes_id < self.count);
        self.bytes_start[bytes_id]
    }

    /// Compacts the IDs array and returns a slice of term IDs in arbitrary order.
    /// Valid IDs are at indices `0..size()`.
    ///
    /// This is a destructive operation. [`clear`](Self::clear) must be called
    /// before reusing.
    pub fn compact(&mut self) -> &[i32] {
        let mut upto = 0;
        for i in 0..self.hash_size {
            if self.ids[i] != -1 {
                self.ids[upto] = self.ids[i] & self.hash_mask;
                if upto < i {
                    self.ids[i] = -1;
                }
                upto += 1;
            }
        }
        assert_eq!(upto, self.count);
        self.last_count = self.count as i32;
        &self.ids[..self.count]
    }

    /// Returns term IDs sorted by the referenced byte values (lexicographic).
    ///
    /// This is a destructive operation. [`clear`](Self::clear) must be called
    /// before reusing.
    pub fn sort(&mut self) -> Vec<i32> {
        let compact = self.compact().to_vec();
        let mut sorted = compact;
        sorted.sort_by(|&a, &b| {
            let a_bytes = self.get(a as usize);
            let b_bytes = self.get(b as usize);
            a_bytes.cmp(b_bytes)
        });
        sorted
    }

    /// Clears the hash for reuse.
    pub fn clear(&mut self, reset_pool: bool) {
        self.last_count = self.count as i32;
        self.count = 0;
        if reset_pool {
            self.pool.reset(false, false);
        }
        self.bytes_start.clear();
        if self.last_count != -1 && self.shrink(self.last_count as usize) {
            return;
        }
        self.ids.fill(-1);
    }

    /// Returns a reference to the underlying byte pool (the `termBytePool`).
    pub fn pool(&self) -> &ByteBlockPool<DirectAllocator> {
        &self.pool
    }

    /// Returns the estimated RAM usage in bytes.
    pub fn ram_bytes_used(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.bytes_start.len() * std::mem::size_of::<i32>()
            + self.ids.len() * std::mem::size_of::<i32>()
            + self.pool.ram_bytes_used()
    }

    // --- Internal methods ---

    fn find_hash(&self, bytes: &[u8], hashcode: i32) -> usize {
        let mut code = hashcode;
        let mut hash_pos = (code & self.hash_mask) as usize;
        let mut e = self.ids[hash_pos];
        let high_bits = hashcode & self.high_mask;

        while e != -1
            && ((e & self.high_mask) != high_bits
                || !self.pool_bytes_equal(self.bytes_start[(e & self.hash_mask) as usize], bytes))
        {
            code = code.wrapping_add(1);
            hash_pos = (code & self.hash_mask) as usize;
            e = self.ids[hash_pos];
        }

        hash_pos
    }

    fn rehash(&mut self, new_size: usize, hash_on_data: bool) {
        let new_mask = (new_size - 1) as i32;
        let new_high_mask = !new_mask;
        let mut new_hash = vec![-1i32; new_size];

        for i in 0..self.hash_size {
            let e0 = self.ids[i];
            if e0 != -1 {
                let e0 = e0 & self.hash_mask;
                let (hashcode, mut code);
                if hash_on_data {
                    let start = self.bytes_start[e0 as usize] as usize;
                    let bytes = self.read_bytes_at(start);
                    hashcode = do_hash(bytes);
                    code = hashcode;
                } else {
                    code = self.bytes_start[e0 as usize];
                    hashcode = 0;
                }

                let mut hash_pos = (code & new_mask) as usize;
                assert!(hash_pos < new_size);

                while new_hash[hash_pos] != -1 {
                    code = code.wrapping_add(1);
                    hash_pos = (code & new_mask) as usize;
                }

                new_hash[hash_pos] = e0 | (hashcode & new_high_mask);
            }
        }

        self.hash_mask = new_mask;
        self.high_mask = new_high_mask;
        self.ids = new_hash;
        self.hash_size = new_size;
        self.hash_half_size = new_size / 2;
    }

    fn shrink(&mut self, target_size: usize) -> bool {
        let mut new_size = self.hash_size;
        while new_size >= 8 && new_size / 4 > target_size {
            new_size /= 2;
        }
        if new_size != self.hash_size {
            self.hash_size = new_size;
            self.ids = vec![-1i32; new_size];
            self.hash_half_size = new_size / 2;
            self.hash_mask = (new_size - 1) as i32;
            self.high_mask = !self.hash_mask;
            true
        } else {
            false
        }
    }

    /// Adds bytes to the pool with length prefix. Returns the start offset.
    fn add_bytes_to_pool(&mut self, bytes: &[u8]) -> i32 {
        let length = bytes.len();
        let len2 = 2 + length;
        if len2 + self.pool.byte_upto > BYTE_BLOCK_SIZE {
            if len2 > BYTE_BLOCK_SIZE {
                panic!(
                    "bytes can be at most {} in length; got {}",
                    BYTE_BLOCK_SIZE - 2,
                    length
                );
            }
            self.pool.next_buffer();
        }
        let buffer_upto = self.pool.byte_upto;
        let text_start = buffer_upto as i32 + self.pool.byte_offset;

        if length < 128 {
            // 1 byte to store length
            self.pool.current_buffer_mut()[buffer_upto] = length as u8;
            self.pool.current_buffer_mut()[buffer_upto + 1..buffer_upto + 1 + length]
                .copy_from_slice(bytes);
            self.pool.byte_upto += length + 1;
        } else {
            // 2 bytes to store length (big-endian with high bit set)
            let encoded = (length as u16) | 0x8000;
            self.pool.current_buffer_mut()[buffer_upto] = (encoded >> 8) as u8;
            self.pool.current_buffer_mut()[buffer_upto + 1] = encoded as u8;
            self.pool.current_buffer_mut()[buffer_upto + 2..buffer_upto + 2 + length]
                .copy_from_slice(bytes);
            self.pool.byte_upto += length + 2;
        }

        text_start
    }

    /// Reads the length-prefixed bytes at the given pool offset.
    fn read_bytes_at(&self, start: usize) -> &[u8] {
        let buffer_index = start >> BYTE_BLOCK_SHIFT;
        let pos = start & BYTE_BLOCK_MASK;
        let buffer = &self.pool.buffers[buffer_index];

        if (buffer[pos] & 0x80) == 0 {
            let length = buffer[pos] as usize;
            &buffer[pos + 1..pos + 1 + length]
        } else {
            let length = (((buffer[pos] as usize) << 8) | (buffer[pos + 1] as usize)) & 0x7FFF;
            &buffer[pos + 2..pos + 2 + length]
        }
    }

    /// Compares the bytes at `start` in the pool with `other`.
    fn pool_bytes_equal(&self, start: i32, other: &[u8]) -> bool {
        self.read_bytes_at(start as usize) == other
    }
}

impl std::fmt::Debug for BytesRefHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BytesRefHash")
            .field("count", &self.count)
            .field("hash_size", &self.hash_size)
            .finish()
    }
}

/// Computes the hash of a byte slice using MurmurHash3 x86 32-bit.
pub fn do_hash(bytes: &[u8]) -> i32 {
    murmurhash3_x86_32(bytes, GOOD_FAST_HASH_SEED)
}

/// Seed for hashing. Fixed value for deterministic behavior.
const GOOD_FAST_HASH_SEED: i32 = 0;

/// MurmurHash3 x86 32-bit implementation.
///
/// Matches Java's `StringHelper.murmurhash3_x86_32`.
fn murmurhash3_x86_32(data: &[u8], seed: i32) -> i32 {
    let c1: i32 = 0xcc9e2d51_u32 as i32;
    let c2: i32 = 0x1b873593_u32 as i32;

    let mut h1 = seed;
    let len = data.len();
    let rounded_end = len & !3; // round down to 4 byte block

    let mut i = 0;
    while i < rounded_end {
        // little endian load order
        let k1_raw = i32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
        let mut k1 = k1_raw;
        k1 = k1.wrapping_mul(c1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(c2);

        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.wrapping_mul(5).wrapping_add(0xe6546b64_u32 as i32);
        i += 4;
    }

    // tail
    let mut k1: i32;
    match len & 3 {
        3 => {
            k1 = (data[rounded_end + 2] as i32 & 0xff) << 16;
            k1 |= (data[rounded_end + 1] as i32 & 0xff) << 8;
            k1 |= data[rounded_end] as i32 & 0xff;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(15);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        2 => {
            k1 = (data[rounded_end + 1] as i32 & 0xff) << 8;
            k1 |= data[rounded_end] as i32 & 0xff;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(15);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        1 => {
            k1 = data[rounded_end] as i32 & 0xff;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(15);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        _ => {}
    }

    // finalization
    h1 ^= len as i32;

    // fmix
    h1 ^= (h1 as u32 >> 16) as i32;
    h1 = h1.wrapping_mul(0x85ebca6b_u32 as i32);
    h1 ^= (h1 as u32 >> 13) as i32;
    h1 = h1.wrapping_mul(0xc2b2ae35_u32 as i32);
    h1 ^= (h1 as u32 >> 16) as i32;

    h1
}

/// Computes a grow size (approximately 1.5x).
fn grow_size(current: usize) -> usize {
    current + (current >> 1).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;

    #[test]
    fn test_add_and_find() {
        let mut hash = BytesRefHash::with_default_capacity();
        let id0 = hash.add(b"hello");
        assert_eq!(id0, 0);
        let id1 = hash.add(b"world");
        assert_eq!(id1, 1);

        // Adding same bytes returns negative
        let dup = hash.add(b"hello");
        assert_eq!(dup, -1); // -(id)-1 = -(0)-1 = -1

        assert_eq!(hash.find(b"hello"), 0);
        assert_eq!(hash.find(b"world"), 1);
        assert_eq!(hash.find(b"missing"), -1);
    }

    #[test]
    fn test_get_bytes() {
        let mut hash = BytesRefHash::with_default_capacity();
        hash.add(b"foo");
        hash.add(b"bar");
        hash.add(b"baz");

        assert_eq!(hash.get(0), b"foo");
        assert_eq!(hash.get(1), b"bar");
        assert_eq!(hash.get(2), b"baz");
    }

    #[test]
    fn test_size() {
        let mut hash = BytesRefHash::with_default_capacity();
        assert_eq!(hash.size(), 0);
        hash.add(b"a");
        assert_eq!(hash.size(), 1);
        hash.add(b"b");
        assert_eq!(hash.size(), 2);
        // Duplicate doesn't increase size
        hash.add(b"a");
        assert_eq!(hash.size(), 2);
    }

    #[test]
    fn test_many_entries_triggers_rehash() {
        let mut hash = BytesRefHash::with_default_capacity();
        // Add enough entries to trigger multiple rehashes
        for i in 0i32..1000 {
            let key = format!("key_{i:04}");
            let id = hash.add(key.as_bytes());
            assert_eq!(id, i);
        }
        assert_eq!(hash.size(), 1000);

        // Verify all can be found
        for i in 0i32..1000 {
            let key = format!("key_{i:04}");
            assert_eq!(hash.find(key.as_bytes()), i);
        }
    }

    #[test]
    fn test_sort_order() {
        let mut hash = BytesRefHash::with_default_capacity();
        hash.add(b"cherry");
        hash.add(b"apple");
        hash.add(b"banana");

        let sorted = hash.sort();
        assert_len_eq_x!(&sorted, 3);

        // Verify sorted order by checking the bytes
        assert_eq!(hash.get(sorted[0] as usize), b"apple");
        assert_eq!(hash.get(sorted[1] as usize), b"banana");
        assert_eq!(hash.get(sorted[2] as usize), b"cherry");
    }

    #[test]
    fn test_compact() {
        let mut hash = BytesRefHash::with_default_capacity();
        hash.add(b"x");
        hash.add(b"y");
        hash.add(b"z");

        let compact = hash.compact().to_vec();
        assert_len_eq_x!(&compact, 3);

        // Should contain IDs 0, 1, 2 in some order
        let mut sorted = compact;
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2]);
    }

    #[test]
    fn test_clear_and_reuse() {
        let mut hash = BytesRefHash::with_default_capacity();
        hash.add(b"first");
        hash.add(b"second");
        assert_eq!(hash.size(), 2);

        hash.clear(true);
        assert_eq!(hash.size(), 0);
        assert_eq!(hash.find(b"first"), -1);

        // Can add new entries after clear
        let id = hash.add(b"new");
        assert_eq!(id, 0);
        assert_eq!(hash.size(), 1);
    }

    #[test]
    fn test_empty_bytes() {
        let mut hash = BytesRefHash::with_default_capacity();
        let id = hash.add(b"");
        assert_eq!(id, 0);
        assert_eq!(hash.get(0), b"");
        assert_eq!(hash.find(b""), 0);
    }

    #[test]
    fn test_long_bytes() {
        let mut hash = BytesRefHash::with_default_capacity();
        // Test bytes longer than 127 (requires 2-byte length encoding)
        let long_bytes = vec![b'x'; 200];
        let id = hash.add(&long_bytes);
        assert_eq!(id, 0);
        assert_eq!(hash.get(0), long_bytes.as_slice());
    }

    #[test]
    fn test_murmurhash3_empty() {
        let h = murmurhash3_x86_32(b"", 0);
        // Known value for empty input with seed 0
        assert_eq!(h, 0);
    }

    #[test]
    fn test_murmurhash3_deterministic() {
        let h1 = murmurhash3_x86_32(b"test", 42);
        let h2 = murmurhash3_x86_32(b"test", 42);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_murmurhash3_different_inputs() {
        let h1 = murmurhash3_x86_32(b"abc", 0);
        let h2 = murmurhash3_x86_32(b"abd", 0);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_byte_start() {
        let mut hash = BytesRefHash::with_default_capacity();
        hash.add(b"hello");
        let start = hash.byte_start(0);
        assert_ge!(start, 0);
    }

    #[test]
    fn test_ram_bytes_used() {
        let hash = BytesRefHash::with_default_capacity();
        assert_gt!(hash.ram_bytes_used(), 0);
    }
}
