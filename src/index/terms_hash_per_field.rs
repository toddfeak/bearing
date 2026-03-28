// SPDX-License-Identifier: Apache-2.0

//! Per-field term processing for the inverted index.
//!
//! `TermsHashPerField` deduplicates terms via `BytesRefHash`, allocates byte
//! stream slices for posting data, and provides low-level write methods
//! (`write_byte`, `write_bytes`, `write_v_int`) that write to shared pools.
//!
//! `FreqProxTermsWriterPerField` extends this with concrete doc/freq/position/
//! offset encoding logic.

use std::io;

use crate::document::IndexOptions;
use crate::index::indexing_chain::FreqProxPostingsArray;
use crate::util::byte_block_pool::{
    BYTE_BLOCK_MASK, BYTE_BLOCK_SHIFT, BYTE_BLOCK_SIZE, ByteBlockPool, ByteSlicePool,
    DirectAllocator, FIRST_LEVEL_SIZE,
};
use crate::util::bytes_ref_hash::BytesRefHash;
use crate::util::int_block_pool::{INT_BLOCK_MASK, INT_BLOCK_SHIFT, INT_BLOCK_SIZE, IntBlockPool};

/// Initial capacity for the BytesRefHash.
const HASH_INIT_SIZE: usize = 4;

/// Per-field term processing for the inverted index.
///
/// Deduplicates terms, allocates byte stream slices, and provides write methods.
/// Stream addresses are stored in `IntBlockPool` entries. Each term gets
/// `stream_count` consecutive int slots holding the current write address for
/// each stream.
///
// TODO(lucene-alignment): Pool ownership — Java's TermsHash owns intPool and
// bytePool and shares them across all TermsHashPerField instances. Currently
// this struct owns its pools. Move ownership to TermsHash when it is ported
// (backlog step 2.5).
pub struct TermsHashPerField {
    // TODO(lucene-alignment): nextPerField — Java chains a secondary
    // TermsHashPerField (e.g. TermVectorsConsumerPerField) via this field.
    // Add when TermVectorsConsumer is ported as a TermsHash (Phase 3).
    int_pool: IntBlockPool,
    byte_pool: ByteBlockPool<DirectAllocator>,
    // termStreamAddressBuffer: index into int_pool.buffers identifying the
    // current buffer holding stream addresses for the most recently accessed term.
    term_stream_address_buffer_index: usize,
    stream_address_offset: usize,
    stream_count: usize,
    field_name: String,
    index_options: IndexOptions,
    bytes_hash: BytesRefHash,
    sorted_term_ids: Option<Vec<i32>>,
    last_doc_id: i32, // assertion-only
}

impl TermsHashPerField {
    /// Creates a new `TermsHashPerField`.
    ///
    /// `stream_count` is the number of byte streams per term: 1 for doc(+freq),
    /// 2 when positions/offsets are also indexed.
    pub fn new(stream_count: usize, field_name: String, index_options: IndexOptions) -> Self {
        assert!(index_options != IndexOptions::None);

        let mut int_pool = IntBlockPool::new();
        int_pool.next_buffer();

        let mut byte_pool = ByteBlockPool::new(DirectAllocator);
        byte_pool.next_buffer();

        let mut term_byte_pool = ByteBlockPool::new(DirectAllocator);
        term_byte_pool.next_buffer();

        let bytes_hash = BytesRefHash::new(term_byte_pool, HASH_INIT_SIZE);

        Self {
            int_pool,
            byte_pool,
            term_stream_address_buffer_index: 0,
            stream_address_offset: 0,
            stream_count,
            field_name,
            index_options,
            bytes_hash,
            sorted_term_ids: None,
            last_doc_id: -1,
        }
    }

    /// Clears the term hash and resets state for reuse.
    pub fn reset(&mut self) {
        self.bytes_hash.clear(false);
        self.sorted_term_ids = None;
        // TODO(lucene-alignment): nextPerField — Java recurses:
        // if (nextPerField != null) { nextPerField.reset(); }
    }

    /// Initializes a `ByteSliceReader` for the given term and stream.
    ///
    /// Returns `(start, end)` global offsets for constructing a reader.
    /// `start` is the beginning of the stream data, `end` is the current
    /// write position.
    pub fn get_stream_range(&self, term_id: usize, stream: usize) -> (usize, usize) {
        assert!(stream < self.stream_count);
        let stream_start_offset = self.postings_array_address_offset(term_id);
        let buffer_index = (stream_start_offset as usize) >> INT_BLOCK_SHIFT;
        let offset_in_buffer = (stream_start_offset as usize) & INT_BLOCK_MASK;
        let end = self.int_pool.buffers[buffer_index][offset_in_buffer + stream] as usize;
        let start = self.postings_array_byte_starts(term_id) as usize + stream * FIRST_LEVEL_SIZE;
        (start, end)
    }

    /// Collapses the hash table and sorts term IDs lexicographically.
    ///
    /// Must not be called twice without a [`reset`](Self::reset) in between.
    pub fn sort_terms(&mut self) {
        assert!(self.sorted_term_ids.is_none());
        self.sorted_term_ids = Some(self.bytes_hash.sort());
    }

    /// Returns the sorted term IDs. [`sort_terms`](Self::sort_terms) must be
    /// called first.
    pub fn sorted_term_ids(&self) -> &[i32] {
        self.sorted_term_ids
            .as_ref()
            .expect("sort_terms not called")
    }

    /// Returns the number of unique terms.
    pub fn num_terms(&self) -> usize {
        self.bytes_hash.size()
    }

    /// Returns the field name.
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Returns the index options.
    pub fn index_options(&self) -> IndexOptions {
        self.index_options
    }

    /// Returns the bytes for a given term ID (from the term byte pool).
    pub fn term_bytes(&self, term_id: usize) -> &[u8] {
        self.bytes_hash.get(term_id)
    }

    /// Returns a reference to the byte pool (for constructing readers at flush).
    pub fn byte_pool(&self) -> &ByteBlockPool<DirectAllocator> {
        &self.byte_pool
    }

    /// Primary entry point: add a term for a document.
    ///
    /// Returns the term ID (positive for new terms, the decoded positive ID
    /// for existing terms). Calls `new_term_callback` for new terms or
    /// `add_term_callback` for existing terms.
    pub fn add(
        &mut self,
        term_bytes: &[u8],
        doc_id: i32,
        new_term_callback: &mut dyn FnMut(&mut TermsHashPerField, usize, i32),
        add_term_callback: &mut dyn FnMut(&mut TermsHashPerField, usize, i32),
    ) -> usize {
        debug_assert!(doc_id >= self.last_doc_id);
        self.last_doc_id = doc_id;

        let term_id = self.bytes_hash.add(term_bytes);
        if term_id >= 0 {
            // New term
            let tid = term_id as usize;
            self.init_stream_slices(tid);
            new_term_callback(self, tid, doc_id);
            tid
        } else {
            // Existing term
            let tid = self.position_stream_slice(term_id);
            add_term_callback(self, tid, doc_id);
            tid
        }

        // TODO(lucene-alignment): nextPerField — Java chains:
        // if (doNextCall) {
        //     nextPerField.add(postingsArray.textStarts[termID], docID);
        // }
        // Add when term vectors are ported via TermsHash chain (Phase 3).
    }

    /// Finish adding all instances of this field to the current document.
    pub fn finish(&self) {
        // TODO(lucene-alignment): nextPerField — Java recurses:
        // if (nextPerField != null) { nextPerField.finish(); }
    }

    /// Write a single byte to the given stream for the current term.
    pub fn write_byte(&mut self, stream: usize, b: u8) {
        let stream_address = self.stream_address_offset + stream;
        let upto =
            self.int_pool.buffers[self.term_stream_address_buffer_index][stream_address] as usize;
        let buffer_index = upto >> BYTE_BLOCK_SHIFT;
        let offset = upto & BYTE_BLOCK_MASK;
        if self.byte_pool.buffers[buffer_index][offset] != 0 {
            // End of slice; allocate a new one
            let new_offset = ByteSlicePool::alloc_slice(&mut self.byte_pool, buffer_index, offset);
            let new_buf_idx = self.byte_pool.buffers.len() - 1;
            self.int_pool.buffers[self.term_stream_address_buffer_index][stream_address] =
                (new_offset as i32) + self.byte_pool.byte_offset;
            self.byte_pool.buffers[new_buf_idx][new_offset] = b;
            self.int_pool.buffers[self.term_stream_address_buffer_index][stream_address] += 1;
        } else {
            self.byte_pool.buffers[buffer_index][offset] = b;
            self.int_pool.buffers[self.term_stream_address_buffer_index][stream_address] += 1;
        }
    }

    /// Write multiple bytes to the given stream for the current term.
    pub fn write_bytes(&mut self, stream: usize, data: &[u8]) {
        let end = data.len();
        let stream_address = self.stream_address_offset + stream;
        let upto =
            self.int_pool.buffers[self.term_stream_address_buffer_index][stream_address] as usize;
        let mut buffer_index = upto >> BYTE_BLOCK_SHIFT;
        let mut slice_offset = upto & BYTE_BLOCK_MASK;
        let mut offset = 0;

        // Write into current slice while there's room
        while self.byte_pool.buffers[buffer_index][slice_offset] == 0 && offset < end {
            self.byte_pool.buffers[buffer_index][slice_offset] = data[offset];
            slice_offset += 1;
            offset += 1;
            self.int_pool.buffers[self.term_stream_address_buffer_index][stream_address] += 1;
        }

        // If we still have data, grow slices as needed
        while offset < end {
            let (new_slice_offset, slice_length) = ByteSlicePool::alloc_known_size_slice(
                &mut self.byte_pool,
                buffer_index,
                slice_offset,
            );
            buffer_index = self.byte_pool.buffers.len() - 1;
            let write_length = (slice_length - 1).min(end - offset);
            self.byte_pool.buffers[buffer_index][new_slice_offset..new_slice_offset + write_length]
                .copy_from_slice(&data[offset..offset + write_length]);
            slice_offset = new_slice_offset + write_length;
            offset += write_length;
            self.int_pool.buffers[self.term_stream_address_buffer_index][stream_address] =
                (slice_offset as i32) + self.byte_pool.byte_offset;
        }
    }

    /// Write a variable-length encoded integer to the given stream.
    pub fn write_v_int(&mut self, stream: usize, mut i: i32) {
        assert!(stream < self.stream_count);
        while (i & !0x7F) != 0 {
            self.write_byte(stream, ((i & 0x7F) | 0x80) as u8);
            i = ((i as u32) >> 7) as i32;
        }
        self.write_byte(stream, i as u8);
    }

    // --- Postings array accessors ---
    // These provide access to the FreqProxPostingsArray fields via the
    // term ID. The actual array is managed by FreqProxTermsWriterPerField.

    fn postings_array_address_offset(&self, _term_id: usize) -> i32 {
        // This is accessed via the FreqProxTermsWriterPerField which holds the array.
        // For now, panic — callers use FreqProxTermsWriterPerField's array directly.
        panic!("postings_array_address_offset called on base TermsHashPerField")
    }

    fn postings_array_byte_starts(&self, _term_id: usize) -> i32 {
        panic!("postings_array_byte_starts called on base TermsHashPerField")
    }

    // --- Internal methods ---

    /// Allocate stream slices for a new term.
    ///
    fn init_stream_slices(&mut self, _term_id: usize) {
        // Ensure IntBlockPool has room for streamCount entries
        if self.stream_count + self.int_pool.int_upto > INT_BLOCK_SIZE {
            self.int_pool.next_buffer();
        }

        // Ensure ByteBlockPool has room for initial slices
        if BYTE_BLOCK_SIZE - self.byte_pool.byte_upto < (2 * self.stream_count) * FIRST_LEVEL_SIZE {
            self.byte_pool.next_buffer();
        }

        // termStreamAddressBuffer = intPool.buffer (current buffer)
        self.term_stream_address_buffer_index = self.int_pool.buffers.len() - 1;
        self.stream_address_offset = self.int_pool.int_upto;
        self.int_pool.int_upto += self.stream_count;

        // Note: postingsArray.addressOffset[termID] and byteStarts[termID]
        // are set by FreqProxTermsWriterPerField which manages the array.

        // Allocate a first-level slice for each stream
        for i in 0..self.stream_count {
            let upto = ByteSlicePool::new_slice(&mut self.byte_pool, FIRST_LEVEL_SIZE);
            self.int_pool.buffers[self.term_stream_address_buffer_index]
                [self.stream_address_offset + i] = (upto as i32) + self.byte_pool.byte_offset;
        }
    }

    /// Get the address stored for stream 0 of the current term (the byte stream start).
    fn current_stream_0_address(&self) -> i32 {
        self.int_pool.buffers[self.term_stream_address_buffer_index][self.stream_address_offset]
    }

    /// The global int offset where the current term's stream addresses are stored.
    fn current_address_offset(&self) -> i32 {
        self.stream_address_offset as i32 + self.int_pool.int_offset
    }

    /// Reposition to the streams of an existing term.
    ///
    /// `raw_term_id` is the negative value returned by `BytesRefHash.add()`.
    fn position_stream_slice(&mut self, raw_term_id: i32) -> usize {
        // The address offset is retrieved from FreqProxTermsWriterPerField's
        // postings array. Since this is the base class, the caller
        // (FreqProxTermsWriterPerField) sets up the addressing before calling
        // add_term_callback.
        // Return the decoded term_id. The caller is responsible for setting
        // term_stream_address_buffer_index and stream_address_offset from
        // the postings array before the callback.
        ((-raw_term_id) - 1) as usize
    }

    /// Set up stream addressing from a postings array address offset.
    ///
    /// Called by FreqProxTermsWriterPerField before addTerm to position
    /// the write cursors for an existing term.
    pub fn position_from_address_offset(&mut self, address_offset: i32) {
        let int_start = address_offset as usize;
        self.term_stream_address_buffer_index = int_start >> INT_BLOCK_SHIFT;
        self.stream_address_offset = int_start & INT_BLOCK_MASK;
    }
}

impl std::fmt::Debug for TermsHashPerField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TermsHashPerField")
            .field("field_name", &self.field_name)
            .field("stream_count", &self.stream_count)
            .field("num_terms", &self.bytes_hash.size())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// FreqProxTermsWriterPerField
// ---------------------------------------------------------------------------

/// Concrete per-field posting writer for frequency and proximity data.
///
/// Extends `TermsHashPerField` via composition. Implements the `newTerm` and
/// `addTerm` logic that encodes doc IDs, frequencies, positions, and offsets
/// into the byte pool streams.
///
/// Stream 0: doc codes and frequencies
/// Stream 1: position codes, offsets, and payloads (when has_prox)
pub struct FreqProxTermsWriterPerField {
    /// Base term hash functionality (owns pools, BytesRefHash).
    pub base: TermsHashPerField,
    /// Per-term posting metadata arrays.
    pub postings_array: FreqProxPostingsArray,
    /// Whether this field indexes term frequencies.
    pub has_freq: bool,
    /// Whether this field indexes positions.
    pub has_prox: bool,
    /// Whether this field indexes offsets.
    pub has_offsets: bool,
    /// Whether any token had a payload in the current segment.
    pub saw_payloads: bool,
    /// Tracks max term frequency across all terms for the current document.
    pub max_term_frequency: i32,
    /// Tracks unique term count for the current document.
    pub unique_term_count: i32,
}

impl FreqProxTermsWriterPerField {
    /// Creates a new `FreqProxTermsWriterPerField`.
    pub fn new(field_name: String, index_options: IndexOptions) -> Self {
        let has_freq = index_options >= IndexOptions::DocsAndFreqs;
        let has_prox = index_options >= IndexOptions::DocsAndFreqsAndPositions;
        let has_offsets = index_options >= IndexOptions::DocsAndFreqsAndPositionsAndOffsets;

        let stream_count = if has_prox { 2 } else { 1 };

        let base = TermsHashPerField::new(stream_count, field_name, index_options);

        let postings_array = FreqProxPostingsArray::new(2, has_freq, has_prox, has_offsets);

        Self {
            base,
            postings_array,
            has_freq,
            has_prox,
            has_offsets,
            saw_payloads: false,
            max_term_frequency: 0,
            unique_term_count: 0,
        }
    }

    /// Add a term occurrence for the given document.
    ///
    /// `position` is the token position (absolute, not delta).
    /// `start_offset` and `end_offset` are character offsets (only used if has_offsets).
    pub fn add_term(
        &mut self,
        term_bytes: &[u8],
        doc_id: i32,
        position: i32,
        start_offset: i32,
        end_offset: i32,
    ) -> io::Result<usize> {
        let has_freq = self.has_freq;
        let has_prox = self.has_prox;
        let has_offsets = self.has_offsets;

        // We need to pass closures that capture position/offset state.
        // Since the closures need &mut self (via TermsHashPerField), and
        // FreqProxPostingsArray is separate, we split the borrow.
        let postings = &mut self.postings_array as *mut FreqProxPostingsArray;
        let max_tf = &mut self.max_term_frequency as *mut i32;
        let unique_tc = &mut self.unique_term_count as *mut i32;

        let mut new_term_fn = |base: &mut TermsHashPerField, term_id: usize, doc_id: i32| {
            // SAFETY: postings_array is not accessed through base
            let postings = unsafe { &mut *postings };
            let max_tf = unsafe { &mut *max_tf };
            let unique_tc = unsafe { &mut *unique_tc };

            // Ensure postings array is large enough
            while term_id >= postings.size() {
                let grown = postings.grow();
                *postings = grown;
            }

            // Store address info from the just-allocated streams
            postings.base.address_offset[term_id] = base.current_address_offset();
            postings.base.byte_starts[term_id] = base.current_stream_0_address();
            postings.base.text_starts[term_id] = base.bytes_hash.byte_start(term_id);

            postings.last_doc_ids[term_id] = doc_id;
            if !has_freq {
                postings.last_doc_codes[term_id] = doc_id;
                *max_tf = (*max_tf).max(1);
            } else {
                postings.last_doc_codes[term_id] = doc_id << 1;
                postings.term_freqs.as_mut().unwrap()[term_id] = 1;
                // TODO(lucene-alignment): getTermFreq() — Java supports custom
                // TermFrequencyAttribute. Always 1 for now. Add when FeatureField
                // or custom freq is wired through this path.
                if has_prox {
                    // writeProx(termID, position) — no payload support yet
                    // TODO(lucene-alignment): PayloadAttribute — Java checks
                    // payloadAttribute for payload data. Add when payload indexing
                    // is supported.
                    base.write_v_int(1, position << 1);
                    postings.last_positions.as_mut().unwrap()[term_id] = position;
                    if has_offsets {
                        postings.last_offsets.as_mut().unwrap()[term_id] = 0;
                        // writeOffsets
                        // TODO(lucene-alignment): OffsetAttribute — Java reads
                        // offsets from analyzer attributes. Using direct params for now.
                        // Add proper attribute integration in step 2.6 (PerField port).
                        base.write_v_int(1, start_offset);
                        base.write_v_int(1, end_offset - start_offset);
                        postings.last_offsets.as_mut().unwrap()[term_id] = start_offset;
                    }
                }
                *max_tf = (*max_tf).max(postings.term_freqs.as_ref().unwrap()[term_id]);
            }
            *unique_tc += 1;
        };

        let mut add_term_fn = |base: &mut TermsHashPerField, term_id: usize, doc_id: i32| {
            // SAFETY: postings_array is not accessed through base
            let postings = unsafe { &mut *postings };
            let max_tf = unsafe { &mut *max_tf };
            let unique_tc = unsafe { &mut *unique_tc };

            // Position the stream cursors from the stored address
            base.position_from_address_offset(postings.base.address_offset[term_id]);

            if !has_freq {
                if doc_id != postings.last_doc_ids[term_id] {
                    assert!(doc_id > postings.last_doc_ids[term_id]);
                    base.write_v_int(0, postings.last_doc_codes[term_id]);
                    postings.last_doc_codes[term_id] = doc_id - postings.last_doc_ids[term_id];
                    postings.last_doc_ids[term_id] = doc_id;
                    *unique_tc += 1;
                }
            } else if doc_id != postings.last_doc_ids[term_id] {
                assert!(doc_id > postings.last_doc_ids[term_id]);
                // Flush previous doc: encode doc code + freq
                if postings.term_freqs.as_ref().unwrap()[term_id] == 1 {
                    base.write_v_int(0, postings.last_doc_codes[term_id] | 1);
                } else {
                    base.write_v_int(0, postings.last_doc_codes[term_id]);
                    base.write_v_int(0, postings.term_freqs.as_ref().unwrap()[term_id]);
                }

                // Init freq for current document
                postings.term_freqs.as_mut().unwrap()[term_id] = 1;
                *max_tf = (*max_tf).max(1);
                postings.last_doc_codes[term_id] = (doc_id - postings.last_doc_ids[term_id]) << 1;
                postings.last_doc_ids[term_id] = doc_id;
                if has_prox {
                    // writeProx
                    base.write_v_int(1, position << 1);
                    postings.last_positions.as_mut().unwrap()[term_id] = position;
                    if has_offsets {
                        postings.last_offsets.as_mut().unwrap()[term_id] = 0;
                        base.write_v_int(1, start_offset);
                        base.write_v_int(1, end_offset - start_offset);
                        postings.last_offsets.as_mut().unwrap()[term_id] = start_offset;
                    }
                }
                *unique_tc += 1;
            } else {
                // Same document — increment frequency
                let freq = postings.term_freqs.as_mut().unwrap();
                freq[term_id] = freq[term_id]
                    .checked_add(1)
                    .expect("term frequency overflow");
                *max_tf = (*max_tf).max(freq[term_id]);
                if has_prox {
                    let last_pos = postings.last_positions.as_ref().unwrap()[term_id];
                    base.write_v_int(1, (position - last_pos) << 1);
                    postings.last_positions.as_mut().unwrap()[term_id] = position;
                    if has_offsets {
                        let last_offset = postings.last_offsets.as_ref().unwrap()[term_id];
                        base.write_v_int(1, start_offset - last_offset);
                        base.write_v_int(1, end_offset - start_offset);
                        postings.last_offsets.as_mut().unwrap()[term_id] = start_offset;
                    }
                }
            }
        };

        let tid = self
            .base
            .add(term_bytes, doc_id, &mut new_term_fn, &mut add_term_fn);
        Ok(tid)
    }

    /// Returns the number of unique terms.
    pub fn num_terms(&self) -> usize {
        self.base.num_terms()
    }

    /// Sort terms lexicographically.
    pub fn sort_terms(&mut self) {
        self.base.sort_terms();
    }

    /// Returns sorted term IDs.
    pub fn sorted_term_ids(&self) -> &[i32] {
        self.base.sorted_term_ids()
    }

    /// Returns the bytes for a given term ID.
    pub fn term_bytes(&self, term_id: usize) -> &[u8] {
        self.base.term_bytes(term_id)
    }

    /// Returns the stream range for constructing a reader.
    pub fn get_stream_range(&self, term_id: usize, stream: usize) -> (usize, usize) {
        assert!(stream < self.base.stream_count);
        let address_offset = self.postings_array.base.address_offset[term_id];
        let buffer_index = (address_offset as usize) >> INT_BLOCK_SHIFT;
        let offset_in_buffer = (address_offset as usize) & INT_BLOCK_MASK;
        let end = self.base.int_pool.buffers[buffer_index][offset_in_buffer + stream] as usize;
        let start =
            self.postings_array.base.byte_starts[term_id] as usize + stream * FIRST_LEVEL_SIZE;
        (start, end)
    }
}

impl std::fmt::Debug for FreqProxTermsWriterPerField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FreqProxTermsWriterPerField")
            .field("field_name", &self.base.field_name)
            .field("num_terms", &self.base.bytes_hash.size())
            .field("has_freq", &self.has_freq)
            .field("has_prox", &self.has_prox)
            .field("has_offsets", &self.has_offsets)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store;
    use crate::util::byte_block_pool::ByteSliceReader;
    use assertables::*;

    /// Helper to read a VInt from a byte slice reader.
    fn read_vint<A: crate::util::byte_block_pool::Allocator>(
        reader: &mut ByteSliceReader<'_, A>,
    ) -> i32 {
        store::read_vint(reader).unwrap()
    }

    #[test]
    fn test_single_term_single_doc() {
        let mut field =
            FreqProxTermsWriterPerField::new("body".to_string(), IndexOptions::DocsAndFreqs);

        field.add_term(b"hello", 0, 0, 0, 5).unwrap();

        assert_eq!(field.num_terms(), 1);
        assert_eq!(field.term_bytes(0), b"hello");
    }

    #[test]
    fn test_duplicate_term_same_doc() {
        let mut field = FreqProxTermsWriterPerField::new(
            "body".to_string(),
            IndexOptions::DocsAndFreqsAndPositions,
        );

        field.add_term(b"hello", 0, 0, 0, 5).unwrap();
        field.add_term(b"hello", 0, 1, 6, 11).unwrap();

        assert_eq!(field.num_terms(), 1);
        // Freq should be 2
        assert_eq!(field.postings_array.term_freqs.as_ref().unwrap()[0], 2);
    }

    #[test]
    fn test_multiple_terms() {
        let mut field =
            FreqProxTermsWriterPerField::new("body".to_string(), IndexOptions::DocsAndFreqs);

        field.add_term(b"hello", 0, 0, 0, 5).unwrap();
        field.add_term(b"world", 0, 1, 6, 11).unwrap();
        field.add_term(b"hello", 0, 2, 12, 17).unwrap();

        assert_eq!(field.num_terms(), 2);
        // "hello" freq should be 2
        let hello_id = field.base.bytes_hash.find(b"hello");
        assert_ge!(hello_id, 0);
        assert_eq!(
            field.postings_array.term_freqs.as_ref().unwrap()[hello_id as usize],
            2
        );
    }

    #[test]
    fn test_term_across_documents() {
        let mut field = FreqProxTermsWriterPerField::new(
            "body".to_string(),
            IndexOptions::DocsAndFreqsAndPositions,
        );

        // Doc 0: "hello world"
        field.add_term(b"hello", 0, 0, 0, 5).unwrap();
        field.add_term(b"world", 0, 1, 6, 11).unwrap();

        // Doc 1: "hello"
        field.add_term(b"hello", 1, 0, 0, 5).unwrap();

        assert_eq!(field.num_terms(), 2);

        // Read back the doc/freq stream for "hello" (stream 0)
        let hello_id = field.base.bytes_hash.find(b"hello") as usize;
        let (start, end) = field.get_stream_range(hello_id, 0);
        let mut reader = ByteSliceReader::new(&field.base.byte_pool, start, end);

        // First doc: code = 0 << 1 = 0, freq = 1 → encoded as (0 | 1) = 1
        let code = read_vint(&mut reader);
        assert_eq!(code, 1); // doc_delta=0 << 1 | 1 (freq=1 packed)

        // Second doc hasn't been flushed yet (it's the current pending doc)
        // so we should only see the first doc in the stream
        assert!(reader.eof());
    }

    #[test]
    fn test_sort_terms_lexicographic() {
        let mut field =
            FreqProxTermsWriterPerField::new("body".to_string(), IndexOptions::DocsAndFreqs);

        field.add_term(b"cherry", 0, 0, 0, 6).unwrap();
        field.add_term(b"apple", 0, 1, 7, 12).unwrap();
        field.add_term(b"banana", 0, 2, 13, 19).unwrap();

        field.sort_terms();
        let sorted = field.sorted_term_ids();
        assert_len_eq_x!(sorted, 3);

        assert_eq!(field.term_bytes(sorted[0] as usize), b"apple");
        assert_eq!(field.term_bytes(sorted[1] as usize), b"banana");
        assert_eq!(field.term_bytes(sorted[2] as usize), b"cherry");
    }

    #[test]
    fn test_docs_only_no_freq() {
        let mut field = FreqProxTermsWriterPerField::new("tags".to_string(), IndexOptions::Docs);

        field.add_term(b"tag1", 0, 0, 0, 4).unwrap();
        field.add_term(b"tag1", 1, 0, 0, 4).unwrap();
        field.add_term(b"tag1", 2, 0, 0, 4).unwrap();

        assert_eq!(field.num_terms(), 1);
        assert!(!field.has_freq);

        // Read stream 0 — should have doc codes
        let tid = field.base.bytes_hash.find(b"tag1") as usize;
        let (start, end) = field.get_stream_range(tid, 0);
        let mut reader = ByteSliceReader::new(&field.base.byte_pool, start, end);

        // Doc 0 encoded, doc 1 delta encoded (1-0=1)
        let code0 = read_vint(&mut reader);
        assert_eq!(code0, 0); // doc 0
        let code1 = read_vint(&mut reader);
        assert_eq!(code1, 1); // delta: 1-0=1
        // Doc 2 is still pending (not flushed)
    }

    #[test]
    fn test_positions_stream() {
        let mut field = FreqProxTermsWriterPerField::new(
            "body".to_string(),
            IndexOptions::DocsAndFreqsAndPositions,
        );

        // "hello" appears at positions 0 and 3 in doc 0
        field.add_term(b"hello", 0, 0, 0, 5).unwrap();
        field.add_term(b"other", 0, 1, 6, 11).unwrap();
        field.add_term(b"stuff", 0, 2, 12, 17).unwrap();
        field.add_term(b"hello", 0, 3, 18, 23).unwrap();

        let hello_id = field.base.bytes_hash.find(b"hello") as usize;

        // Read position stream (stream 1)
        let (start, end) = field.get_stream_range(hello_id, 1);
        let mut reader = ByteSliceReader::new(&field.base.byte_pool, start, end);

        // Position 0: proxCode = 0 << 1 = 0 (no payload)
        let pos0 = read_vint(&mut reader);
        assert_eq!(pos0, 0); // position 0 << 1

        // Position 3: proxCode = (3 - 0) << 1 = 6 (delta from last position)
        let pos1 = read_vint(&mut reader);
        assert_eq!(pos1, 6); // delta 3 << 1
    }

    #[test]
    fn test_multi_doc_freq_encoding() {
        let mut field =
            FreqProxTermsWriterPerField::new("body".to_string(), IndexOptions::DocsAndFreqs);

        // Doc 0: "hello" x3
        field.add_term(b"hello", 0, 0, 0, 5).unwrap();
        field.add_term(b"hello", 0, 1, 6, 11).unwrap();
        field.add_term(b"hello", 0, 2, 12, 17).unwrap();

        // Doc 1: "hello" x1
        field.add_term(b"hello", 1, 0, 0, 5).unwrap();

        // Doc 2: "hello" x2
        field.add_term(b"hello", 2, 0, 0, 5).unwrap();
        field.add_term(b"hello", 2, 1, 6, 11).unwrap();

        let hello_id = field.base.bytes_hash.find(b"hello") as usize;
        let (start, end) = field.get_stream_range(hello_id, 0);
        let mut reader = ByteSliceReader::new(&field.base.byte_pool, start, end);

        // Doc 0: delta=0, freq=3 → docCode = 0<<1 = 0, then freq=3
        let doc0_code = read_vint(&mut reader);
        assert_eq!(doc0_code, 0); // 0 << 1
        let doc0_freq = read_vint(&mut reader);
        assert_eq!(doc0_freq, 3);

        // Doc 1: delta=1, freq=1 → docCode = 1<<1|1 = 3 (freq packed)
        let doc1_code = read_vint(&mut reader);
        assert_eq!(doc1_code, 3); // (1 << 1) | 1

        // Doc 2 is still pending (current doc, not yet flushed)
        assert!(reader.eof());
    }

    #[test]
    fn test_max_term_frequency_tracking() {
        let mut field =
            FreqProxTermsWriterPerField::new("body".to_string(), IndexOptions::DocsAndFreqs);

        field.add_term(b"a", 0, 0, 0, 1).unwrap();
        field.add_term(b"b", 0, 1, 2, 3).unwrap();
        field.add_term(b"a", 0, 2, 4, 5).unwrap();
        field.add_term(b"a", 0, 3, 6, 7).unwrap();

        assert_eq!(field.max_term_frequency, 3); // "a" appeared 3 times
    }

    #[test]
    fn test_unique_term_count_tracking() {
        let mut field =
            FreqProxTermsWriterPerField::new("body".to_string(), IndexOptions::DocsAndFreqs);

        field.add_term(b"a", 0, 0, 0, 1).unwrap();
        field.add_term(b"b", 0, 1, 2, 3).unwrap();
        field.add_term(b"a", 0, 2, 4, 5).unwrap();

        assert_eq!(field.unique_term_count, 2);
    }
}
