// SPDX-License-Identifier: Apache-2.0

//! Per-field term vector processing.
//!
//! `TermVectorsConsumerPerField` is the secondary per-field writer in the
//! TermsHash chain. It is attached to `FreqProxTermsWriterPerField` via the
//! `nextPerField` link and processes the same terms for term vector storage.
//!
//! Stream 0: positions + payloads
//! Stream 1: offsets

use std::io;

use crate::newindex::codecs::term_vectors::TermVectorChunkWriter;
use crate::newindex::terms_hash::{
    BYTES_PER_POSTING, ParallelPostingsArray, TermsHash, TermsHashPerField, TermsHashPerFieldTrait,
    oversize,
};
use crate::util::byte_block_pool::{
    ByteBlockPool, ByteSliceReader, DirectAllocator, FIRST_LEVEL_SIZE,
};
use crate::util::bytes_ref_hash::BytesRefHash;
use crate::util::int_block_pool::{INT_BLOCK_MASK, INT_BLOCK_SHIFT, IntBlockPool};

use crate::document::IndexOptions;

/// Per-field term vector writer.
///
/// Implements `TermsHashPerFieldTrait`, using `add_by_pool_offset` for term
/// deduplication — the term bytes are already interned in the primary
/// (FreqProx) `BytesRefHash`.
///
/// Pools are owned externally in a `TermsHash` stored on
/// `TermVectorsConsumer`.
pub(crate) struct TermVectorsConsumerPerField {
    /// Base term hash functionality (BytesRefHash, stream cursors).
    pub(crate) base: TermsHashPerField,

    /// Per-term TV metadata.
    pub(crate) postings_array: TermVectorsPostingsArray,

    /// Whether this field is storing term vectors for the current document.
    pub(crate) do_vectors: bool,
    /// Whether to store positions in term vectors.
    pub(crate) do_vector_positions: bool,
    /// Whether to store offsets in term vectors.
    pub(crate) do_vector_offsets: bool,
    /// Whether to store payloads in term vectors.
    pub(crate) do_vector_payloads: bool,
    /// Whether we actually saw any payloads for this field.
    pub(crate) has_payloads: bool,
    // Current token state — set by FreqProx's add() before calling add_by_text_start.
    pub(crate) current_position: i32,
    pub(crate) current_start_offset: i32,
    pub(crate) current_end_offset: i32,
}

impl TermVectorsConsumerPerField {
    /// Creates a new `TermVectorsConsumerPerField`.
    ///
    /// Always uses 2 streams (positions + offsets), matching Java.
    pub(crate) fn new(field_name: String) -> Self {
        let stream_count = 2;
        let base = TermsHashPerField::new(stream_count, field_name, IndexOptions::DocsAndFreqs);

        Self {
            base,
            postings_array: TermVectorsPostingsArray::new(2),
            do_vectors: false,
            do_vector_positions: false,
            do_vector_offsets: false,
            do_vector_payloads: false,
            has_payloads: false,
            current_position: 0,
            current_start_offset: 0,
            current_end_offset: 0,
        }
    }

    /// Called after all tokens for this field in the current document.
    #[expect(dead_code)]
    pub(crate) fn finish(&self) {
        // No-op; finishDocument is driven externally.
    }

    /// Returns true if this field has term vector data to flush.
    pub(crate) fn has_data(&self) -> bool {
        self.do_vectors && self.num_terms() > 0
    }

    /// Writes this field's term vector data to the codec writer, reading
    /// term text from the primary byte pool and position/offset data from
    /// the TV pools via `ByteSliceReader`.
    ///
    /// `primary_hash` / `primary_byte_pool` provide term text lookup.
    /// `tv_terms_hash` provides the TV int/byte pools for position/offset streams.
    #[expect(dead_code)]
    pub(crate) fn finish_document(
        &mut self,
        field_number: u32,
        primary_hash: &BytesRefHash,
        primary_byte_pool: &ByteBlockPool<DirectAllocator>,
        tv_terms_hash: &TermsHash,
        writer: &mut TermVectorChunkWriter,
    ) -> io::Result<()> {
        if !self.do_vectors {
            return Ok(());
        }
        self.do_vectors = false;

        let num_terms = self.num_terms();

        self.base.sort_terms(primary_byte_pool);
        let sorted_ids = self.base.sorted_term_ids();

        writer.start_field(
            field_number,
            num_terms as i32,
            self.do_vector_positions,
            self.do_vector_offsets,
            self.has_payloads,
        );

        for &sorted_id in &sorted_ids[..num_terms] {
            let term_id = sorted_id as usize;
            let freq = self.postings_array.freqs[term_id];

            let text_start = self.postings_array.base.text_starts[term_id] as usize;
            let term_bytes = primary_hash.get_by_offset(primary_byte_pool, text_start);

            writer.start_term(term_bytes, freq);

            if self.do_vector_positions || self.do_vector_offsets {
                let mut pos_reader = if self.do_vector_positions {
                    let (start, end) = self.get_stream_range(&tv_terms_hash.int_pool, term_id, 0);
                    Some(ByteSliceReader::new(&tv_terms_hash.byte_pool, start, end))
                } else {
                    None
                };
                let mut off_reader = if self.do_vector_offsets {
                    let (start, end) = self.get_stream_range(&tv_terms_hash.int_pool, term_id, 1);
                    Some(ByteSliceReader::new(&tv_terms_hash.byte_pool, start, end))
                } else {
                    None
                };
                writer.add_prox(freq, pos_reader.as_mut(), off_reader.as_mut());
            }
            writer.finish_term();
        }
        writer.finish_field();

        self.reset();
        Ok(())
    }

    /// Writes this field's term vector data to the codec writer when the
    /// TV per-field owns its own term bytes (used when terms were added
    /// via `add()` rather than `add_by_text_start()`).
    ///
    /// `tv_terms_hash` provides both the byte pool (for term text AND
    /// position/offset streams) and the int pool (for stream addresses).
    pub(crate) fn finish_document_self_owned(
        &mut self,
        field_number: u32,
        tv_terms_hash: &TermsHash,
        writer: &mut TermVectorChunkWriter,
    ) -> io::Result<()> {
        if !self.do_vectors {
            return Ok(());
        }
        self.do_vectors = false;

        let num_terms = self.num_terms();

        self.base.sort_terms(&tv_terms_hash.byte_pool);
        let sorted_ids = self.base.sorted_term_ids();

        writer.start_field(
            field_number,
            num_terms as i32,
            self.do_vector_positions,
            self.do_vector_offsets,
            self.has_payloads,
        );

        for &sorted_id in &sorted_ids[..num_terms] {
            let term_id = sorted_id as usize;
            let freq = self.postings_array.freqs[term_id];

            let term_bytes = self.base.bytes_hash.get(&tv_terms_hash.byte_pool, term_id);

            writer.start_term(term_bytes, freq);

            if self.do_vector_positions || self.do_vector_offsets {
                let mut pos_reader = if self.do_vector_positions {
                    let (start, end) = self.get_stream_range(&tv_terms_hash.int_pool, term_id, 0);
                    Some(ByteSliceReader::new(&tv_terms_hash.byte_pool, start, end))
                } else {
                    None
                };
                let mut off_reader = if self.do_vector_offsets {
                    let (start, end) = self.get_stream_range(&tv_terms_hash.int_pool, term_id, 1);
                    Some(ByteSliceReader::new(&tv_terms_hash.byte_pool, start, end))
                } else {
                    None
                };
                writer.add_prox(freq, pos_reader.as_mut(), off_reader.as_mut());
            }
            writer.finish_term();
        }
        writer.finish_field();

        self.reset();
        Ok(())
    }

    /// Returns the number of unique terms in the TV hash.
    pub(crate) fn num_terms(&self) -> usize {
        self.base.bytes_hash.size()
    }

    /// Returns the field name.
    #[expect(dead_code)]
    pub(crate) fn field_name(&self) -> &str {
        self.base.field_name()
    }

    /// Clears the term hash and resets state for reuse between documents.
    pub(crate) fn reset(&mut self) {
        self.base.reset();
    }

    /// Returns the stream range `(start, end)` for the given term and stream.
    pub(crate) fn get_stream_range(
        &self,
        int_pool: &IntBlockPool,
        term_id: usize,
        stream: usize,
    ) -> (usize, usize) {
        assert!(stream < self.base.stream_count);
        let address_offset = self.postings_array.base.address_offset[term_id];
        let buffer_index = (address_offset as usize) >> INT_BLOCK_SHIFT;
        let offset_in_buffer = (address_offset as usize) & INT_BLOCK_MASK;
        let end = int_pool.buffers[buffer_index][offset_in_buffer + stream] as usize;
        let start =
            self.postings_array.base.byte_starts[term_id] as usize + stream * FIRST_LEVEL_SIZE;
        (start, end)
    }

    /// Write position and/or offset data for a term occurrence.
    fn write_prox(&mut self, terms_hash: &mut TermsHash, term_id: usize) {
        if self.do_vector_offsets {
            let last_offset = self.postings_array.last_offsets[term_id];
            let start_offset = self.current_start_offset;
            let end_offset = self.current_end_offset;
            self.base
                .write_v_int(terms_hash, 1, start_offset - last_offset);
            self.base
                .write_v_int(terms_hash, 1, end_offset - start_offset);
            self.postings_array.last_offsets[term_id] = end_offset;
        }

        if self.do_vector_positions {
            let pos = self.current_position - self.postings_array.last_positions[term_id];
            self.base.write_v_int(terms_hash, 0, pos << 1);
            self.postings_array.last_positions[term_id] = self.current_position;
        }
    }
}

impl std::fmt::Debug for TermVectorsConsumerPerField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TermVectorsConsumerPerField")
            .field("field_name", &self.base.field_name())
            .field("num_terms", &self.base.bytes_hash.size())
            .field("do_vectors", &self.do_vectors)
            .field("do_vector_positions", &self.do_vector_positions)
            .field("do_vector_offsets", &self.do_vector_offsets)
            .finish()
    }
}

impl TermsHashPerFieldTrait for TermVectorsConsumerPerField {
    fn base(&self) -> &TermsHashPerField {
        &self.base
    }

    fn base_mut(&mut self) -> &mut TermsHashPerField {
        &mut self.base
    }

    fn postings_array_base(&self) -> &ParallelPostingsArray {
        &self.postings_array.base
    }

    fn postings_array_base_mut(&mut self) -> &mut ParallelPostingsArray {
        &mut self.postings_array.base
    }

    fn ensure_postings_capacity(&mut self, term_id: usize) {
        while term_id >= self.postings_array.size() {
            let grown = self.postings_array.grow();
            self.postings_array = grown;
        }
    }

    fn new_term(&mut self, terms_hash: &mut TermsHash, term_id: usize, _doc_id: i32) {
        let postings = &mut self.postings_array;

        postings.freqs[term_id] = 1;
        postings.last_offsets[term_id] = 0;
        postings.last_positions[term_id] = 0;

        self.write_prox(terms_hash, term_id);
    }

    fn add_term(&mut self, terms_hash: &mut TermsHash, term_id: usize, _doc_id: i32) {
        self.postings_array.freqs[term_id] += 1;

        self.write_prox(terms_hash, term_id);
    }
}

// ---------------------------------------------------------------------------
// TermVectorsPostingsArray
// ---------------------------------------------------------------------------

/// Per-term posting metadata for term vectors.
///
/// Simpler than `FreqProxPostingsArray` — no document tracking since
/// term vectors are per-document. Tracks frequency, last position, and
/// last offset within a single document.
#[derive(Debug)]
pub(crate) struct TermVectorsPostingsArray {
    /// Base arrays (text starts, address offsets, byte starts).
    pub base: ParallelPostingsArray,
    /// How many times each term occurred in the current document.
    pub freqs: Vec<i32>,
    /// Last offset we saw for each term.
    pub last_offsets: Vec<i32>,
    /// Last position where each term occurred.
    pub last_positions: Vec<i32>,
}

impl TermVectorsPostingsArray {
    /// Creates a new array with the given initial capacity.
    pub(crate) fn new(size: usize) -> Self {
        Self {
            base: ParallelPostingsArray::new(size),
            freqs: vec![0; size],
            last_offsets: vec![0; size],
            last_positions: vec![0; size],
        }
    }

    /// Returns the current capacity.
    pub(crate) fn size(&self) -> usize {
        self.base.size()
    }

    /// Returns bytes per posting (base + 3 int fields).
    pub(crate) fn bytes_per_posting(&self) -> usize {
        BYTES_PER_POSTING + 3 * std::mem::size_of::<i32>()
    }

    /// Grows the arrays to accommodate at least one more entry.
    pub(crate) fn grow(&self) -> Self {
        let new_size = oversize(self.size() + 1, self.bytes_per_posting());
        let mut new_array = Self::new(new_size);
        self.copy_to(&mut new_array, self.size());
        new_array
    }

    /// Copies `num_to_copy` elements from `self` into `to_array`.
    pub(crate) fn copy_to(&self, to_array: &mut TermVectorsPostingsArray, num_to_copy: usize) {
        self.base.copy_to(&mut to_array.base, num_to_copy);
        to_array.freqs[..num_to_copy].copy_from_slice(&self.freqs[..num_to_copy]);
        to_array.last_offsets[..num_to_copy].copy_from_slice(&self.last_offsets[..num_to_copy]);
        to_array.last_positions[..num_to_copy].copy_from_slice(&self.last_positions[..num_to_copy]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store;
    use assertables::*;

    /// Helper to read a VInt from a byte slice reader.
    fn read_vint<A: crate::util::byte_block_pool::Allocator>(
        reader: &mut ByteSliceReader<'_, A>,
    ) -> i32 {
        store::read_vint(reader).unwrap()
    }

    #[test]
    fn test_tv_new_term_positions() {
        let mut tv_th = TermsHash::new();
        let mut tv = TermVectorsConsumerPerField::new("body".to_string());
        tv.do_vectors = true;
        tv.do_vector_positions = true;

        tv.current_position = 0;
        tv.current_start_offset = 0;
        tv.current_end_offset = 5;
        TermsHashPerFieldTrait::add_by_text_start(&mut tv, &mut tv_th, 100, 0);

        assert_eq!(tv.num_terms(), 1);
        assert_eq!(tv.postings_array.freqs[0], 1);
        assert_eq!(tv.postings_array.last_positions[0], 0);

        // Read position stream (stream 0)
        let (start, end) = tv.get_stream_range(&tv_th.int_pool, 0, 0);
        let mut reader = ByteSliceReader::new(&tv_th.byte_pool, start, end);
        let pos_code = read_vint(&mut reader);
        assert_eq!(pos_code, 0);
    }

    #[test]
    fn test_tv_add_term_positions() {
        let mut tv_th = TermsHash::new();
        let mut tv = TermVectorsConsumerPerField::new("body".to_string());
        tv.do_vectors = true;
        tv.do_vector_positions = true;

        // Same term at positions 0 and 3
        tv.current_position = 0;
        tv.current_start_offset = 0;
        tv.current_end_offset = 5;
        TermsHashPerFieldTrait::add_by_text_start(&mut tv, &mut tv_th, 100, 0);

        tv.current_position = 3;
        tv.current_start_offset = 18;
        tv.current_end_offset = 23;
        TermsHashPerFieldTrait::add_by_text_start(&mut tv, &mut tv_th, 100, 0);

        assert_eq!(tv.num_terms(), 1);
        assert_eq!(tv.postings_array.freqs[0], 2);

        let (start, end) = tv.get_stream_range(&tv_th.int_pool, 0, 0);
        let mut reader = ByteSliceReader::new(&tv_th.byte_pool, start, end);

        let pos0 = read_vint(&mut reader);
        assert_eq!(pos0, 0);
        let pos1 = read_vint(&mut reader);
        assert_eq!(pos1, 6); // delta 3 << 1
    }

    #[test]
    fn test_tv_offsets() {
        let mut tv_th = TermsHash::new();
        let mut tv = TermVectorsConsumerPerField::new("body".to_string());
        tv.do_vectors = true;
        tv.do_vector_offsets = true;

        tv.current_position = 0;
        tv.current_start_offset = 0;
        tv.current_end_offset = 5;
        TermsHashPerFieldTrait::add_by_text_start(&mut tv, &mut tv_th, 100, 0);

        tv.current_position = 1;
        tv.current_start_offset = 10;
        tv.current_end_offset = 15;
        TermsHashPerFieldTrait::add_by_text_start(&mut tv, &mut tv_th, 100, 0);

        assert_eq!(tv.num_terms(), 1);
        assert_eq!(tv.postings_array.freqs[0], 2);

        // Read offset stream (stream 1)
        let (start, end) = tv.get_stream_range(&tv_th.int_pool, 0, 1);
        let mut reader = ByteSliceReader::new(&tv_th.byte_pool, start, end);

        let start_delta_0 = read_vint(&mut reader);
        assert_eq!(start_delta_0, 0);
        let length_0 = read_vint(&mut reader);
        assert_eq!(length_0, 5);

        let start_delta_1 = read_vint(&mut reader);
        assert_eq!(start_delta_1, 5); // 10 - 5
        let length_1 = read_vint(&mut reader);
        assert_eq!(length_1, 5);
    }

    #[test]
    fn test_tv_multiple_terms() {
        let mut tv_th = TermsHash::new();
        let mut tv = TermVectorsConsumerPerField::new("body".to_string());
        tv.do_vectors = true;
        tv.do_vector_positions = true;

        tv.current_position = 0;
        tv.current_start_offset = 0;
        tv.current_end_offset = 5;
        TermsHashPerFieldTrait::add_by_text_start(&mut tv, &mut tv_th, 100, 0);

        tv.current_position = 1;
        tv.current_start_offset = 6;
        tv.current_end_offset = 11;
        TermsHashPerFieldTrait::add_by_text_start(&mut tv, &mut tv_th, 200, 0);

        tv.current_position = 2;
        tv.current_start_offset = 12;
        tv.current_end_offset = 17;
        TermsHashPerFieldTrait::add_by_text_start(&mut tv, &mut tv_th, 100, 0);

        assert_eq!(tv.num_terms(), 2);
        assert_eq!(tv.postings_array.freqs[0], 2);
        assert_eq!(tv.postings_array.freqs[1], 1);
    }

    #[test]
    fn test_tv_reset_clears_between_docs() {
        let mut tv_th = TermsHash::new();
        let mut tv = TermVectorsConsumerPerField::new("body".to_string());
        tv.do_vectors = true;
        tv.do_vector_positions = true;

        tv.current_position = 0;
        tv.current_start_offset = 0;
        tv.current_end_offset = 5;
        TermsHashPerFieldTrait::add_by_text_start(&mut tv, &mut tv_th, 100, 0);
        assert_eq!(tv.num_terms(), 1);

        tv.reset();
        assert_eq!(tv.num_terms(), 0);

        tv.current_position = 0;
        tv.current_start_offset = 0;
        tv.current_end_offset = 5;
        TermsHashPerFieldTrait::add_by_text_start(&mut tv, &mut tv_th, 200, 1);
        assert_eq!(tv.num_terms(), 1);
    }

    #[test]
    fn test_tv_postings_array_grow() {
        let arr = TermVectorsPostingsArray::new(2);
        assert_eq!(arr.size(), 2);
        let grown = arr.grow();
        assert_gt!(grown.size(), 2);
    }
}
