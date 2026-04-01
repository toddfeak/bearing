// SPDX-License-Identifier: Apache-2.0

// DEBT: parallel to index::indexing_chain::PostingsArray — reconcile after switchover.

//! Per-field term and postings accumulation.
//!
//! Each indexed field gets one `PerFieldPostings` instance that deduplicates
//! terms via [`BytesRefHash`] and encodes doc delta/freq and position delta
//! data into shared [`ByteBlockPool`]s owned by the
//! [`PostingsConsumer`](super::postings_consumer::PostingsConsumer).

use std::fmt;
use std::io;
use std::str;

use crate::store;
use crate::util::byte_block_pool::{
    ByteBlockPool, ByteSlicePool, ByteSliceReader, ByteSliceWriter, DirectAllocator,
    FIRST_LEVEL_SIZE,
};
use crate::util::bytes_ref_hash::BytesRefHash;

/// Per-field postings accumulation state.
///
/// Holds a term hash for deduplication and parallel arrays of per-term
/// metadata. Actual postings bytes (doc deltas, freqs, position deltas)
/// are written into shared pools owned by the caller.
pub struct PerFieldPostings {
    /// Term deduplication hash. Owns its own `ByteBlockPool` for term bytes.
    terms: BytesRefHash,

    // Per-term parallel arrays (indexed by term ID from `terms`)
    /// Global offset where each term's doc/freq byte slice starts (for reading).
    byte_stream_starts: Vec<u32>,
    /// Current global write address for each term's doc/freq byte slice.
    byte_stream_addrs: Vec<u32>,
    /// Last doc ID written per term (for delta encoding).
    last_doc_ids: Vec<i32>,
    /// Current in-progress doc ID per term (-1 = no pending doc).
    current_doc_ids: Vec<i32>,
    /// Current frequency count per term for the in-progress doc.
    current_freqs: Vec<i32>,

    // Position tracking (empty when !has_positions)
    /// Global offset where each term's position byte slice starts.
    positions_stream_starts: Vec<u32>,
    /// Current global write address for each term's position byte slice.
    positions_stream_addrs: Vec<u32>,
    /// Last position written per term (for delta encoding).
    last_positions: Vec<i32>,

    has_freqs: bool,
    has_positions: bool,
}

impl fmt::Debug for PerFieldPostings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PerFieldPostings")
            .field("term_count", &self.terms.size())
            .field("has_freqs", &self.has_freqs)
            .field("has_positions", &self.has_positions)
            .finish()
    }
}

impl PerFieldPostings {
    /// Creates empty per-field state.
    /// Creates empty per-field state.
    ///
    /// `has_freqs` controls whether term frequencies are recorded (false for DOCS-only).
    /// `has_positions` controls whether position deltas are written.
    pub fn new(has_freqs: bool, has_positions: bool) -> Self {
        Self {
            terms: BytesRefHash::with_default_capacity(),
            byte_stream_starts: Vec::new(),
            byte_stream_addrs: Vec::new(),
            last_doc_ids: Vec::new(),
            current_doc_ids: Vec::new(),
            current_freqs: Vec::new(),
            positions_stream_starts: Vec::new(),
            positions_stream_addrs: Vec::new(),
            last_positions: Vec::new(),
            has_freqs,
            has_positions,
        }
    }

    /// Hashes a term, allocating byte slices for new terms. Returns the term ID.
    ///
    /// If the term already exists, returns its existing ID. For new terms,
    /// allocates slices in the shared pools and initializes parallel arrays.
    pub fn add_term(
        &mut self,
        term: &[u8],
        byte_pool: &mut ByteBlockPool<DirectAllocator>,
        positions_pool: Option<&mut ByteBlockPool<DirectAllocator>>,
    ) -> usize {
        let raw_id = self.terms.add(term);
        if raw_id >= 0 {
            // New term — allocate slices and initialize arrays
            let tid = raw_id as usize;
            let local_offset = ByteSlicePool::new_slice(byte_pool, FIRST_LEVEL_SIZE);
            let global_offset = local_offset as u32 + byte_pool.byte_offset() as u32;
            self.byte_stream_starts.push(global_offset);
            let writer = ByteSliceWriter::new(byte_pool, local_offset);
            self.byte_stream_addrs.push(writer.address() as u32);
            self.last_doc_ids.push(0);
            self.current_doc_ids.push(-1);
            self.current_freqs.push(0);

            if self.has_positions {
                let pool = positions_pool.expect("positions_pool required when has_positions");
                let local_offset = ByteSlicePool::new_slice(pool, FIRST_LEVEL_SIZE);
                let global_offset = local_offset as u32 + pool.byte_offset() as u32;
                self.positions_stream_starts.push(global_offset);
                let writer = ByteSliceWriter::new(pool, local_offset);
                self.positions_stream_addrs.push(writer.address() as u32);
                self.last_positions.push(0);
            }

            tid
        } else {
            // Existing term
            (-(raw_id + 1)) as usize
        }
    }

    /// Records a token occurrence for the given term in the given document.
    ///
    /// Handles new-doc detection (finalizes previous doc, resets freq).
    /// When positions are enabled, writes position deltas immediately.
    #[inline]
    pub fn record_occurrence(
        &mut self,
        tid: usize,
        doc_id: i32,
        position: i32,
        byte_pool: &mut ByteBlockPool<DirectAllocator>,
        positions_pool: Option<&mut ByteBlockPool<DirectAllocator>>,
    ) {
        if self.current_doc_ids[tid] != doc_id {
            // New document for this term
            if self.current_doc_ids[tid] >= 0 {
                self.finalize_doc(tid, byte_pool);
            }
            self.current_doc_ids[tid] = doc_id;
            self.current_freqs[tid] = 1;
            if self.has_positions {
                self.last_positions[tid] = 0;
            }
        } else {
            self.current_freqs[tid] += 1;
        }

        if self.has_positions {
            let pos_delta = position - self.last_positions[tid];
            let pool = positions_pool.expect("positions_pool required when has_positions");
            let mut writer =
                ByteSliceWriter::from_address(self.positions_stream_addrs[tid] as usize);
            writer.write_vint(pool, pos_delta);
            self.positions_stream_addrs[tid] = writer.address() as u32;
            self.last_positions[tid] = position;
        }
    }

    /// Records a term occurrence with an explicit frequency for the given document.
    ///
    /// Used by FeatureField where the frequency encodes a feature value rather
    /// than being incremented per token. No positions are written.
    #[inline]
    pub fn record_occurrence_with_freq(
        &mut self,
        tid: usize,
        doc_id: i32,
        freq: i32,
        byte_pool: &mut ByteBlockPool<DirectAllocator>,
    ) {
        if self.current_doc_ids[tid] != doc_id {
            if self.current_doc_ids[tid] >= 0 {
                self.finalize_doc(tid, byte_pool);
            }
            self.current_doc_ids[tid] = doc_id;
            self.current_freqs[tid] = freq;
        } else {
            self.current_freqs[tid] = freq;
        }
    }

    /// Encodes the pending document for a term into the doc/freq byte stream.
    ///
    /// Writes doc_delta and freq as vInts. Position data was already written
    /// during `record_occurrence`.
    pub fn finalize_doc(&mut self, tid: usize, byte_pool: &mut ByteBlockPool<DirectAllocator>) {
        if self.current_doc_ids[tid] < 0 {
            return;
        }

        let delta = self.current_doc_ids[tid] - self.last_doc_ids[tid];
        let mut writer = ByteSliceWriter::from_address(self.byte_stream_addrs[tid] as usize);
        writer.write_vint(byte_pool, delta);
        if self.has_freqs {
            writer.write_vint(byte_pool, self.current_freqs[tid]);
        }
        self.byte_stream_addrs[tid] = writer.address() as u32;

        self.last_doc_ids[tid] = self.current_doc_ids[tid];
        self.current_doc_ids[tid] = -1;
        self.current_freqs[tid] = 0;
    }

    /// Finalizes all terms with pending documents.
    pub fn finalize_all(&mut self, byte_pool: &mut ByteBlockPool<DirectAllocator>) {
        for tid in 0..self.current_doc_ids.len() {
            self.finalize_doc(tid, byte_pool);
        }
    }

    /// Returns terms sorted by byte order with their term IDs.
    pub fn sort_terms(&mut self) -> Vec<(String, usize)> {
        let sorted_ids = self.terms.sort();
        sorted_ids
            .iter()
            .map(|&id| {
                let bytes = self.terms.get(id as usize);
                let text = str::from_utf8(bytes)
                    .expect("term bytes must be valid UTF-8")
                    .to_string();
                (text, id as usize)
            })
            .collect()
    }

    /// Decodes one term's postings from the shared pools.
    ///
    /// Returns a list of (doc_id, freq, positions) tuples suitable for
    /// passing to `PostingsWriter::write_term`.
    pub fn decode_term(
        &self,
        tid: usize,
        byte_pool: &ByteBlockPool<DirectAllocator>,
        positions_pool: Option<&ByteBlockPool<DirectAllocator>>,
    ) -> io::Result<Vec<(i32, i32, Vec<i32>)>> {
        let start = self.byte_stream_starts[tid] as usize;
        let end = self.byte_stream_addrs[tid] as usize;
        let mut reader = ByteSliceReader::new(byte_pool, start, end);

        let mut pos_reader = if self.has_positions {
            let pool = positions_pool.expect("positions_pool required when has_positions");
            Some(ByteSliceReader::new(
                pool,
                self.positions_stream_starts[tid] as usize,
                self.positions_stream_addrs[tid] as usize,
            ))
        } else {
            None
        };

        let mut result = Vec::new();
        let mut last_doc_id = 0;

        while !reader.eof() {
            let doc_delta = store::read_vint(&mut reader)?;
            let doc_id = last_doc_id + doc_delta;
            last_doc_id = doc_id;

            let freq = if self.has_freqs {
                store::read_vint(&mut reader)?
            } else {
                1
            };

            let positions = if let Some(ref mut pos_r) = pos_reader {
                let mut positions = Vec::with_capacity(freq as usize);
                let mut last_pos = 0;
                for _ in 0..freq {
                    let pos_delta = store::read_vint(pos_r)?;
                    let pos = last_pos + pos_delta;
                    positions.push(pos);
                    last_pos = pos;
                }
                positions
            } else {
                Vec::new()
            };

            result.push((doc_id, freq, positions));
        }

        Ok(result)
    }

    /// Returns the number of unique terms.
    pub fn term_count(&self) -> usize {
        self.terms.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;

    /// Creates shared pools for testing.
    fn make_pools(
        has_positions: bool,
    ) -> (
        ByteBlockPool<DirectAllocator>,
        Option<ByteBlockPool<DirectAllocator>>,
    ) {
        let mut byte_pool = ByteBlockPool::new(DirectAllocator);
        byte_pool.next_buffer();
        let positions_pool = if has_positions {
            let mut pool = ByteBlockPool::new(DirectAllocator);
            pool.next_buffer();
            Some(pool)
        } else {
            None
        };
        (byte_pool, positions_pool)
    }

    #[test]
    fn single_term_single_doc() {
        let (mut byte_pool, mut positions_pool) = make_pools(true);
        let mut pfp = PerFieldPostings::new(true, true);

        let tid = pfp.add_term(b"hello", &mut byte_pool, positions_pool.as_mut());
        assert_eq!(tid, 0);

        pfp.record_occurrence(tid, 0, 0, &mut byte_pool, positions_pool.as_mut());
        pfp.finalize_all(&mut byte_pool);

        let postings = pfp
            .decode_term(tid, &byte_pool, positions_pool.as_ref())
            .unwrap();
        assert_len_eq_x!(&postings, 1);
        let (doc_id, freq, ref positions) = postings[0];
        assert_eq!(doc_id, 0);
        assert_eq!(freq, 1);
        assert_eq!(positions, &[0]);
    }

    #[test]
    fn single_term_multiple_docs() {
        let (mut byte_pool, mut positions_pool) = make_pools(true);
        let mut pfp = PerFieldPostings::new(true, true);

        let tid = pfp.add_term(b"hello", &mut byte_pool, positions_pool.as_mut());

        // Doc 0: 2 occurrences at positions 0, 3
        pfp.record_occurrence(tid, 0, 0, &mut byte_pool, positions_pool.as_mut());
        pfp.record_occurrence(tid, 0, 3, &mut byte_pool, positions_pool.as_mut());

        // Doc 1: 1 occurrence at position 5
        pfp.record_occurrence(tid, 1, 5, &mut byte_pool, positions_pool.as_mut());

        pfp.finalize_all(&mut byte_pool);

        let postings = pfp
            .decode_term(tid, &byte_pool, positions_pool.as_ref())
            .unwrap();
        assert_len_eq_x!(&postings, 2);

        assert_eq!(postings[0], (0, 2, vec![0, 3]));
        assert_eq!(postings[1], (1, 1, vec![5]));
    }

    #[test]
    fn multiple_terms_multiple_docs() {
        let (mut byte_pool, mut positions_pool) = make_pools(true);
        let mut pfp = PerFieldPostings::new(true, true);

        let tid_hello = pfp.add_term(b"hello", &mut byte_pool, positions_pool.as_mut());
        let tid_world = pfp.add_term(b"world", &mut byte_pool, positions_pool.as_mut());
        assert_ne!(tid_hello, tid_world);

        // Doc 0: "hello world"
        pfp.record_occurrence(tid_hello, 0, 0, &mut byte_pool, positions_pool.as_mut());
        pfp.record_occurrence(tid_world, 0, 1, &mut byte_pool, positions_pool.as_mut());

        // Doc 1: "hello hello"
        pfp.record_occurrence(tid_hello, 1, 0, &mut byte_pool, positions_pool.as_mut());
        pfp.record_occurrence(tid_hello, 1, 1, &mut byte_pool, positions_pool.as_mut());

        pfp.finalize_all(&mut byte_pool);

        let hello_postings = pfp
            .decode_term(tid_hello, &byte_pool, positions_pool.as_ref())
            .unwrap();
        assert_len_eq_x!(&hello_postings, 2);
        assert_eq!(hello_postings[0], (0, 1, vec![0]));
        assert_eq!(hello_postings[1], (1, 2, vec![0, 1]));

        let world_postings = pfp
            .decode_term(tid_world, &byte_pool, positions_pool.as_ref())
            .unwrap();
        assert_len_eq_x!(&world_postings, 1);
        assert_eq!(world_postings[0], (0, 1, vec![1]));
    }

    #[test]
    fn term_sorting() {
        let (mut byte_pool, mut positions_pool) = make_pools(false);
        let mut pfp = PerFieldPostings::new(true, false);

        pfp.add_term(b"zebra", &mut byte_pool, positions_pool.as_mut());
        pfp.add_term(b"apple", &mut byte_pool, positions_pool.as_mut());
        pfp.add_term(b"mango", &mut byte_pool, positions_pool.as_mut());

        let sorted = pfp.sort_terms();
        let names: Vec<&str> = sorted.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["apple", "mango", "zebra"]);
    }

    #[test]
    fn existing_term_returns_same_id() {
        let (mut byte_pool, mut positions_pool) = make_pools(false);
        let mut pfp = PerFieldPostings::new(true, false);

        let id1 = pfp.add_term(b"hello", &mut byte_pool, positions_pool.as_mut());
        let id2 = pfp.add_term(b"hello", &mut byte_pool, positions_pool.as_mut());
        assert_eq!(id1, id2);
        assert_eq!(pfp.term_count(), 1);
    }

    #[test]
    fn frequency_counting() {
        let (mut byte_pool, mut positions_pool) = make_pools(false);
        let mut pfp = PerFieldPostings::new(true, false);

        let tid = pfp.add_term(b"the", &mut byte_pool, positions_pool.as_mut());

        // Doc 0: 5 occurrences
        for _ in 0..5 {
            pfp.record_occurrence(tid, 0, 0, &mut byte_pool, positions_pool.as_mut());
        }
        pfp.finalize_all(&mut byte_pool);

        let postings = pfp
            .decode_term(tid, &byte_pool, positions_pool.as_ref())
            .unwrap();
        assert_len_eq_x!(&postings, 1);
        assert_eq!(postings[0].0, 0); // doc_id
        assert_eq!(postings[0].1, 5); // freq
    }

    #[test]
    fn position_delta_encoding() {
        let (mut byte_pool, mut positions_pool) = make_pools(true);
        let mut pfp = PerFieldPostings::new(true, true);

        let tid = pfp.add_term(b"test", &mut byte_pool, positions_pool.as_mut());

        // Positions: 2, 7, 15 → deltas: 2, 5, 8
        pfp.record_occurrence(tid, 0, 2, &mut byte_pool, positions_pool.as_mut());
        pfp.record_occurrence(tid, 0, 7, &mut byte_pool, positions_pool.as_mut());
        pfp.record_occurrence(tid, 0, 15, &mut byte_pool, positions_pool.as_mut());

        pfp.finalize_all(&mut byte_pool);

        let postings = pfp
            .decode_term(tid, &byte_pool, positions_pool.as_ref())
            .unwrap();
        assert_eq!(postings[0], (0, 3, vec![2, 7, 15]));
    }

    #[test]
    fn docs_only_no_freqs_no_positions() {
        let (mut byte_pool, _) = make_pools(false);
        let mut pfp = PerFieldPostings::new(false, false);

        let tid = pfp.add_term(b"hello", &mut byte_pool, None);

        // Two docs, single occurrence each
        pfp.record_occurrence(tid, 0, 0, &mut byte_pool, None);
        pfp.record_occurrence(tid, 1, 0, &mut byte_pool, None);
        pfp.finalize_all(&mut byte_pool);

        let postings = pfp.decode_term(tid, &byte_pool, None).unwrap();
        assert_len_eq_x!(&postings, 2);
        // freq defaults to 1, no positions
        assert_eq!(postings[0], (0, 1, vec![]));
        assert_eq!(postings[1], (1, 1, vec![]));
    }

    #[test]
    fn explicit_freq_no_positions() {
        let (mut byte_pool, _) = make_pools(false);
        let mut pfp = PerFieldPostings::new(true, false);

        let tid = pfp.add_term(b"score", &mut byte_pool, None);

        // 0.95f32 bits = 0x3F733333, >> 15 = 0x7EE6 = 32486
        let freq = (f32::to_bits(0.95) >> 15) as i32;
        pfp.record_occurrence_with_freq(tid, 0, freq, &mut byte_pool);
        pfp.record_occurrence_with_freq(tid, 1, freq, &mut byte_pool);
        pfp.finalize_all(&mut byte_pool);

        let postings = pfp.decode_term(tid, &byte_pool, None).unwrap();
        assert_len_eq_x!(&postings, 2);
        assert_eq!(postings[0], (0, freq, vec![]));
        assert_eq!(postings[1], (1, freq, vec![]));
    }
}
