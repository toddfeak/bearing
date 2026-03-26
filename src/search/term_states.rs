// SPDX-License-Identifier: Apache-2.0

//! Per-reader cache of [`IntBlockTermState`] for a single term.
//!
//! Maintains a view across all leaf readers, caching the per-segment term metadata
//! and aggregating term statistics (docFreq, totalTermFreq). This avoids repeated
//! trie navigation and block scanning when the same term is looked up multiple times
//! across segments.

use std::fmt;
use std::io;

use crate::codecs::lucene103::postings_format::IntBlockTermState;
use crate::search::index_searcher::IndexSearcher;

/// Maintains per-leaf [`IntBlockTermState`] for a single term across all segments.
///
/// Built once during weight creation, then reused by scorer suppliers to avoid
/// redundant trie + seek_exact I/O per segment.
pub struct TermStates {
    /// Per-leaf cached state, indexed by leaf ordinal. `None` if the term
    /// does not exist in that segment.
    states: Box<[Option<IntBlockTermState>]>,
    /// Accumulated document frequency across all segments.
    doc_freq: i32,
    /// Accumulated total term frequency across all segments.
    total_term_freq: i64,
}

impl fmt::Debug for TermStates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TermStates")
            .field("doc_freq", &self.doc_freq)
            .field("total_term_freq", &self.total_term_freq)
            .field("num_segments", &self.states.len())
            .finish()
    }
}

impl TermStates {
    /// Creates an empty `TermStates` with the given number of leaf slots.
    fn new(num_leaves: usize) -> Self {
        Self {
            states: vec![None; num_leaves].into_boxed_slice(),
            doc_freq: 0,
            total_term_freq: 0,
        }
    }

    /// Builds a `TermStates` by visiting all leaf readers and collecting term metadata.
    ///
    /// For each segment, navigates the trie and seeks the term. If found, caches the
    /// `IntBlockTermState` and accumulates statistics.
    pub fn build(searcher: &IndexSearcher, field: &str, term: &[u8]) -> io::Result<Self> {
        let leaves = searcher.get_reader().leaves();
        let mut term_states = Self::new(leaves.len());

        for leaf in leaves {
            let terms = match leaf.reader.terms(field) {
                Some(t) => t,
                None => continue,
            };

            let mut terms_enum = terms.iterator()?;
            if !terms_enum.seek_exact(term)? {
                continue;
            }

            let state = terms_enum.term_state()?;
            let doc_freq = terms_enum.doc_freq()?;
            let ttf = terms_enum.total_term_freq()?;
            let ttf = if ttf > 0 { ttf } else { doc_freq as i64 };
            term_states.register(state, leaf.ord, doc_freq, ttf);
        }

        Ok(term_states)
    }

    /// Registers a [`IntBlockTermState`] for the given leaf ordinal and accumulates
    /// statistics.
    fn register(
        &mut self,
        state: IntBlockTermState,
        ord: usize,
        doc_freq: i32,
        total_term_freq: i64,
    ) {
        debug_assert!(ord < self.states.len());
        debug_assert!(self.states[ord].is_none());
        self.states[ord] = Some(state);
        self.accumulate_statistics(doc_freq, total_term_freq);
    }

    /// Accumulates term statistics from a single segment.
    fn accumulate_statistics(&mut self, doc_freq: i32, total_term_freq: i64) {
        debug_assert!(doc_freq >= 0);
        debug_assert!(total_term_freq >= 0);
        debug_assert!(doc_freq as i64 <= total_term_freq);
        self.doc_freq += doc_freq;
        self.total_term_freq += total_term_freq;
    }

    /// Returns the cached [`IntBlockTermState`] for the given leaf ordinal, or `None`
    /// if the term does not exist in that segment.
    pub fn get(&self, ord: usize) -> Option<IntBlockTermState> {
        debug_assert!(ord < self.states.len());
        self.states[ord]
    }

    /// Returns the accumulated document frequency across all segments.
    pub fn doc_freq(&self) -> i32 {
        self.doc_freq
    }

    /// Returns the accumulated total term frequency across all segments.
    pub fn total_term_freq(&self) -> i64 {
        self.total_term_freq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document;
    use crate::index::directory_reader::DirectoryReader;
    use crate::index::{IndexWriter, IndexWriterConfig};
    use crate::store::{Directory, MemoryDirectory};
    use assertables::*;

    fn build_single_segment_index() -> (Box<dyn Directory>, DirectoryReader) {
        let config = IndexWriterConfig::new().set_use_compound_file(false);
        let writer = IndexWriter::with_config(config);

        let mut doc = document::Document::new();
        doc.add(document::text_field("content", "hello world"));
        writer.add_document(doc).unwrap();

        let mut doc = document::Document::new();
        doc.add(document::text_field("content", "hello there"));
        writer.add_document(doc).unwrap();

        let mut doc = document::Document::new();
        doc.add(document::text_field("content", "world peace"));
        writer.add_document(doc).unwrap();

        let result = writer.commit().unwrap();
        let seg_files = result.into_segment_files().unwrap();

        let mut mem_dir = MemoryDirectory::new();
        for sf in &seg_files {
            mem_dir.write_file(&sf.name, &sf.data).unwrap();
        }
        let dir = Box::new(mem_dir) as Box<dyn Directory>;
        let reader = DirectoryReader::open(dir.as_ref()).unwrap();
        (dir, reader)
    }

    #[test]
    fn test_build_existing_term() {
        let (_dir, reader) = build_single_segment_index();
        let searcher = IndexSearcher::new(&reader);

        // "hello" appears in 2 of 3 docs
        let ts = TermStates::build(&searcher, "content", b"hello").unwrap();
        assert_eq!(ts.doc_freq(), 2);
        assert_ge!(ts.total_term_freq(), 2);
    }

    #[test]
    fn test_build_nonexistent_term() {
        let (_dir, reader) = build_single_segment_index();
        let searcher = IndexSearcher::new(&reader);

        let ts = TermStates::build(&searcher, "content", b"nonexistent").unwrap();
        assert_eq!(ts.doc_freq(), 0);
        assert_eq!(ts.total_term_freq(), 0);
    }

    #[test]
    fn test_build_nonexistent_field() {
        let (_dir, reader) = build_single_segment_index();
        let searcher = IndexSearcher::new(&reader);

        let ts = TermStates::build(&searcher, "no_such_field", b"hello").unwrap();
        assert_eq!(ts.doc_freq(), 0);
        assert_eq!(ts.total_term_freq(), 0);
    }

    #[test]
    fn test_get_returns_state_for_existing_term() {
        let (_dir, reader) = build_single_segment_index();
        let searcher = IndexSearcher::new(&reader);

        let ts = TermStates::build(&searcher, "content", b"hello").unwrap();

        // Single segment index — leaf 0 should have the state
        let state = ts.get(0);
        let state = assert_some!(state);
        assert_eq!(state.doc_freq, 2);
    }

    #[test]
    fn test_get_returns_none_for_missing_term() {
        let (_dir, reader) = build_single_segment_index();
        let searcher = IndexSearcher::new(&reader);

        let ts = TermStates::build(&searcher, "content", b"nonexistent").unwrap();
        assert_none!(ts.get(0));
    }

    #[test]
    fn test_single_doc_term() {
        let (_dir, reader) = build_single_segment_index();
        let searcher = IndexSearcher::new(&reader);

        // "peace" appears in only 1 doc
        let ts = TermStates::build(&searcher, "content", b"peace").unwrap();
        assert_eq!(ts.doc_freq(), 1);
        assert_ge!(ts.total_term_freq(), 1);

        let state = ts.get(0).unwrap();
        assert_eq!(state.doc_freq, 1);
    }
}
