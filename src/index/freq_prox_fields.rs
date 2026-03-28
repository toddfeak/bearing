// SPDX-License-Identifier: Apache-2.0

//! In-memory posting data presented through the codec writer traits.
//!
//! `FreqProxFields` wraps one or more `FreqProxTermsWriterPerField` instances
//! and presents their buffered posting data through the `FieldsWriter` /
//! `TermsWriter` / `TermsEnumWriter` / `PostingsEnumWriter` traits for
//! consumption by `BlockTreeTermsWriter` during flush.

use std::collections::BTreeMap;
use std::io;

use crate::codecs::fields_writer::{
    FieldsWriter, NO_MORE_DOCS, PostingsEnumWriter, TermsEnumWriter, TermsWriter,
};
use crate::index::terms_hash_per_field::FreqProxTermsWriterPerField;
use crate::store;
use crate::util::byte_block_pool::{ByteSliceReader, DirectAllocator};

/// Wraps a set of `FreqProxTermsWriterPerField` instances for codec consumption.
pub struct FreqProxFields<'a> {
    fields: BTreeMap<String, &'a FreqProxTermsWriterPerField>,
}

impl<'a> FreqProxFields<'a> {
    /// Creates a new `FreqProxFields` from a sorted list of per-field writers.
    pub fn new(field_list: &[&'a FreqProxTermsWriterPerField]) -> Self {
        let mut fields = BTreeMap::new();
        for field in field_list {
            fields.insert(field.base.field_name().to_string(), *field);
        }
        Self { fields }
    }
}

impl FieldsWriter for FreqProxFields<'_> {
    fn field_names(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
    }

    fn terms(&self, field: &str) -> Option<Box<dyn TermsWriter + '_>> {
        self.fields
            .get(field)
            .map(|f| Box::new(FreqProxTerms { terms: f }) as Box<dyn TermsWriter + '_>)
    }
}

/// Terms access for a single field, backed by a `FreqProxTermsWriterPerField`.
struct FreqProxTerms<'a> {
    terms: &'a FreqProxTermsWriterPerField,
}

impl TermsWriter for FreqProxTerms<'_> {
    fn iterator(&self) -> io::Result<Box<dyn TermsEnumWriter + '_>> {
        Ok(Box::new(FreqProxTermsEnum::new(self.terms)))
    }

    fn has_freqs(&self) -> bool {
        self.terms.has_freq
    }

    fn has_positions(&self) -> bool {
        self.terms.has_prox
    }

    fn has_offsets(&self) -> bool {
        self.terms.has_offsets
    }

    fn has_payloads(&self) -> bool {
        self.terms.saw_payloads
    }
}

/// Iterates terms in sorted lexicographic order for a field.
struct FreqProxTermsEnum<'a> {
    terms: &'a FreqProxTermsWriterPerField,
    sorted_term_ids: Vec<i32>,
    num_terms: usize,
    ord: i32,
}

impl<'a> FreqProxTermsEnum<'a> {
    fn new(terms: &'a FreqProxTermsWriterPerField) -> Self {
        // Terms must have been sorted before creating the enum
        let sorted_term_ids = terms.sorted_term_ids().to_vec();
        let num_terms = terms.num_terms();
        Self {
            terms,
            sorted_term_ids,
            num_terms,
            ord: -1,
        }
    }
}

impl<'a> TermsEnumWriter for FreqProxTermsEnum<'a> {
    fn next(&mut self) -> io::Result<Option<&[u8]>> {
        self.ord += 1;
        if self.ord as usize >= self.num_terms {
            Ok(None)
        } else {
            let term_id = self.sorted_term_ids[self.ord as usize] as usize;
            Ok(Some(self.terms.term_bytes(term_id)))
        }
    }

    fn postings(&mut self) -> io::Result<Box<dyn PostingsEnumWriter + '_>> {
        let term_id = self.sorted_term_ids[self.ord as usize] as usize;

        if self.terms.has_prox {
            // Positions (and possibly offsets) — use FreqProxPostingsEnum
            let (doc_start, doc_end) = self.terms.get_stream_range(term_id, 0);
            let (pos_start, pos_end) = self.terms.get_stream_range(term_id, 1);
            let doc_reader = ByteSliceReader::new(self.terms.base.byte_pool(), doc_start, doc_end);
            let pos_reader = ByteSliceReader::new(self.terms.base.byte_pool(), pos_start, pos_end);
            Ok(Box::new(FreqProxPostingsEnum::new(
                doc_reader,
                pos_reader,
                &self.terms.postings_array,
                term_id,
                self.terms.has_offsets,
            )))
        } else {
            // Docs only or docs+freqs — use FreqProxDocsEnum
            let (start, end) = self.terms.get_stream_range(term_id, 0);
            let reader = ByteSliceReader::new(self.terms.base.byte_pool(), start, end);
            Ok(Box::new(FreqProxDocsEnum::new(
                reader,
                &self.terms.postings_array,
                term_id,
                self.terms.has_freq,
            )))
        }
    }
}

/// Iterates documents and frequencies for a term (no positions).
///
/// Used when `IndexOptions` is `Docs` or `DocsAndFreqs`.
struct FreqProxDocsEnum<'a> {
    reader: ByteSliceReader<'a, DirectAllocator>,
    postings_array: &'a crate::index::indexing_chain::FreqProxPostingsArray,
    read_term_freq: bool,
    doc_id: i32,
    freq: i32,
    ended: bool,
    term_id: usize,
}

impl<'a> FreqProxDocsEnum<'a> {
    fn new(
        reader: ByteSliceReader<'a, DirectAllocator>,
        postings_array: &'a crate::index::indexing_chain::FreqProxPostingsArray,
        term_id: usize,
        read_term_freq: bool,
    ) -> Self {
        Self {
            reader,
            postings_array,
            read_term_freq,
            doc_id: -1,
            freq: 0,
            ended: false,
            term_id,
        }
    }
}

impl PostingsEnumWriter for FreqProxDocsEnum<'_> {
    fn next_doc(&mut self) -> io::Result<i32> {
        if self.doc_id == -1 {
            self.doc_id = 0;
        }

        if self.reader.eof() {
            if self.ended {
                return Ok(NO_MORE_DOCS);
            } else {
                // Return the last doc whose data is still in the postings array
                // (not yet flushed to the byte stream)
                self.ended = true;
                self.doc_id = self.postings_array.last_doc_ids[self.term_id];
                if self.read_term_freq {
                    self.freq = self.postings_array.term_freqs.as_ref().unwrap()[self.term_id];
                }
            }
        } else {
            let code = store::read_vint(&mut self.reader)?;
            if !self.read_term_freq {
                self.doc_id += code;
            } else {
                self.doc_id += code >> 1;
                if (code & 1) != 0 {
                    self.freq = 1;
                } else {
                    self.freq = store::read_vint(&mut self.reader)?;
                }
            }

            assert!(self.doc_id != self.postings_array.last_doc_ids[self.term_id]);
        }

        Ok(self.doc_id)
    }

    fn freq(&self) -> i32 {
        if !self.read_term_freq {
            panic!("freq was not indexed");
        }
        self.freq
    }

    fn next_position(&mut self) -> io::Result<i32> {
        Ok(-1)
    }

    fn start_offset(&self) -> i32 {
        -1
    }

    fn end_offset(&self) -> i32 {
        -1
    }

    fn payload(&self) -> Option<&[u8]> {
        None
    }
}

/// Iterates documents, frequencies, positions, and offsets for a term.
///
/// Used when `IndexOptions` is `DocsAndFreqsAndPositions` or higher.
struct FreqProxPostingsEnum<'a> {
    reader: ByteSliceReader<'a, DirectAllocator>,
    pos_reader: ByteSliceReader<'a, DirectAllocator>,
    postings_array: &'a crate::index::indexing_chain::FreqProxPostingsArray,
    read_offsets: bool,
    doc_id: i32,
    freq: i32,
    pos: i32,
    start_offset_val: i32,
    end_offset_val: i32,
    pos_left: i32,
    term_id: usize,
    ended: bool,
    // TODO(lucene-alignment): PayloadAttribute — Java reads payload bytes from
    // the position stream. Add when payload indexing is supported.
}

impl<'a> FreqProxPostingsEnum<'a> {
    fn new(
        reader: ByteSliceReader<'a, DirectAllocator>,
        pos_reader: ByteSliceReader<'a, DirectAllocator>,
        postings_array: &'a crate::index::indexing_chain::FreqProxPostingsArray,
        term_id: usize,
        read_offsets: bool,
    ) -> Self {
        Self {
            reader,
            pos_reader,
            postings_array,
            read_offsets,
            doc_id: -1,
            freq: 0,
            pos: 0,
            start_offset_val: 0,
            end_offset_val: 0,
            pos_left: 0,
            term_id,
            ended: false,
        }
    }
}

impl PostingsEnumWriter for FreqProxPostingsEnum<'_> {
    fn next_doc(&mut self) -> io::Result<i32> {
        if self.doc_id == -1 {
            self.doc_id = 0;
        }
        // Consume any remaining positions from the previous doc
        while self.pos_left != 0 {
            self.next_position()?;
        }

        if self.reader.eof() {
            if self.ended {
                return Ok(NO_MORE_DOCS);
            } else {
                self.ended = true;
                self.doc_id = self.postings_array.last_doc_ids[self.term_id];
                self.freq = self.postings_array.term_freqs.as_ref().unwrap()[self.term_id];
            }
        } else {
            let code = store::read_vint(&mut self.reader)?;
            self.doc_id += code >> 1;
            if (code & 1) != 0 {
                self.freq = 1;
            } else {
                self.freq = store::read_vint(&mut self.reader)?;
            }

            assert!(self.doc_id != self.postings_array.last_doc_ids[self.term_id]);
        }

        self.pos_left = self.freq;
        self.pos = 0;
        self.start_offset_val = 0;
        Ok(self.doc_id)
    }

    fn freq(&self) -> i32 {
        self.freq
    }

    fn next_position(&mut self) -> io::Result<i32> {
        assert!(self.pos_left > 0);
        self.pos_left -= 1;
        let code = store::read_vint(&mut self.pos_reader)?;
        self.pos += code >> 1;
        // TODO(lucene-alignment): PayloadAttribute — Java checks (code & 1) for
        // payload presence and reads payload length + bytes. Add when payload
        // indexing is supported.

        if self.read_offsets {
            self.start_offset_val += store::read_vint(&mut self.pos_reader)?;
            self.end_offset_val = self.start_offset_val + store::read_vint(&mut self.pos_reader)?;
        }

        Ok(self.pos)
    }

    fn start_offset(&self) -> i32 {
        self.start_offset_val
    }

    fn end_offset(&self) -> i32 {
        self.end_offset_val
    }

    fn payload(&self) -> Option<&[u8]> {
        // TODO(lucene-alignment): PayloadAttribute — return payload bytes when
        // payload indexing is supported.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::fields_writer::NO_MORE_DOCS;
    use crate::document::IndexOptions;

    fn make_field(name: &str, options: IndexOptions) -> FreqProxTermsWriterPerField {
        FreqProxTermsWriterPerField::new(name.to_string(), options)
    }

    #[test]
    fn test_single_field_single_term() {
        let mut field = make_field("body", IndexOptions::DocsAndFreqs);
        field.add_term(b"hello", 0, 0, 0, 5).unwrap();
        field.sort_terms();

        let fields = FreqProxFields::new(&[&field]);
        assert_eq!(fields.field_names(), vec!["body"]);

        let terms = fields.terms("body").unwrap();
        assert!(terms.has_freqs());
        assert!(!terms.has_positions());

        let mut iter = terms.iterator().unwrap();
        let term = iter.next().unwrap().unwrap();
        assert_eq!(term, b"hello");

        {
            let mut postings = iter.postings().unwrap();
            let doc = postings.next_doc().unwrap();
            assert_eq!(doc, 0);
            assert_eq!(postings.freq(), 1);

            let end = postings.next_doc().unwrap();
            assert_eq!(end, NO_MORE_DOCS);
        }

        assert!(iter.next().unwrap().is_none());
    }

    #[test]
    fn test_multi_term_sorted_iteration() {
        let mut field = make_field("body", IndexOptions::DocsAndFreqs);
        field.add_term(b"cherry", 0, 0, 0, 6).unwrap();
        field.add_term(b"apple", 0, 1, 7, 12).unwrap();
        field.add_term(b"banana", 0, 2, 13, 19).unwrap();
        field.sort_terms();

        let fields = FreqProxFields::new(&[&field]);
        let terms = fields.terms("body").unwrap();
        let mut iter = terms.iterator().unwrap();

        assert_eq!(iter.next().unwrap().unwrap(), b"apple");
        assert_eq!(iter.next().unwrap().unwrap(), b"banana");
        assert_eq!(iter.next().unwrap().unwrap(), b"cherry");
        assert!(iter.next().unwrap().is_none());
    }

    #[test]
    fn test_multi_doc_freq_encoding() {
        let mut field = make_field("body", IndexOptions::DocsAndFreqs);

        // Doc 0: "hello" x3
        field.add_term(b"hello", 0, 0, 0, 5).unwrap();
        field.add_term(b"hello", 0, 1, 6, 11).unwrap();
        field.add_term(b"hello", 0, 2, 12, 17).unwrap();

        // Doc 1: "hello" x1
        field.add_term(b"hello", 1, 0, 0, 5).unwrap();

        // Doc 2: "hello" x2
        field.add_term(b"hello", 2, 0, 0, 5).unwrap();
        field.add_term(b"hello", 2, 1, 6, 11).unwrap();

        field.sort_terms();

        let fields = FreqProxFields::new(&[&field]);
        let terms = fields.terms("body").unwrap();
        let mut iter = terms.iterator().unwrap();
        iter.next().unwrap(); // "hello"

        let mut postings = iter.postings().unwrap();

        // Doc 0, freq 3
        assert_eq!(postings.next_doc().unwrap(), 0);
        assert_eq!(postings.freq(), 3);

        // Doc 1, freq 1
        assert_eq!(postings.next_doc().unwrap(), 1);
        assert_eq!(postings.freq(), 1);

        // Doc 2, freq 2 (the last/unflushed doc)
        assert_eq!(postings.next_doc().unwrap(), 2);
        assert_eq!(postings.freq(), 2);

        assert_eq!(postings.next_doc().unwrap(), NO_MORE_DOCS);
    }

    #[test]
    fn test_positions_stream() {
        let mut field = make_field("body", IndexOptions::DocsAndFreqsAndPositions);

        // "hello" at positions 0 and 3 in doc 0
        field.add_term(b"hello", 0, 0, 0, 5).unwrap();
        field.add_term(b"other", 0, 1, 6, 11).unwrap();
        field.add_term(b"hello", 0, 3, 18, 23).unwrap();

        field.sort_terms();

        let fields = FreqProxFields::new(&[&field]);
        let terms = fields.terms("body").unwrap();
        assert!(terms.has_positions());

        let mut iter = terms.iterator().unwrap();
        // "hello" comes first alphabetically
        let term = iter.next().unwrap().unwrap();
        assert_eq!(term, b"hello");

        let mut postings = iter.postings().unwrap();
        assert_eq!(postings.next_doc().unwrap(), 0);
        assert_eq!(postings.freq(), 2);

        assert_eq!(postings.next_position().unwrap(), 0);
        assert_eq!(postings.next_position().unwrap(), 3);

        assert_eq!(postings.next_doc().unwrap(), NO_MORE_DOCS);
    }

    #[test]
    fn test_docs_only_no_freq() {
        let mut field = make_field("tags", IndexOptions::Docs);
        field.add_term(b"tag1", 0, 0, 0, 4).unwrap();
        field.add_term(b"tag1", 1, 0, 0, 4).unwrap();
        field.add_term(b"tag1", 2, 0, 0, 4).unwrap();
        field.sort_terms();

        let fields = FreqProxFields::new(&[&field]);
        let terms = fields.terms("tags").unwrap();
        assert!(!terms.has_freqs());

        let mut iter = terms.iterator().unwrap();
        iter.next().unwrap();
        let mut postings = iter.postings().unwrap();

        assert_eq!(postings.next_doc().unwrap(), 0);
        assert_eq!(postings.next_doc().unwrap(), 1);
        assert_eq!(postings.next_doc().unwrap(), 2);
        assert_eq!(postings.next_doc().unwrap(), NO_MORE_DOCS);
    }

    #[test]
    fn test_multiple_fields() {
        let mut field_a = make_field("alpha", IndexOptions::DocsAndFreqs);
        let mut field_b = make_field("beta", IndexOptions::DocsAndFreqs);

        field_a.add_term(b"hello", 0, 0, 0, 5).unwrap();
        field_b.add_term(b"world", 0, 0, 0, 5).unwrap();

        field_a.sort_terms();
        field_b.sort_terms();

        let fields = FreqProxFields::new(&[&field_a, &field_b]);
        let names = fields.field_names();
        assert_eq!(names, vec!["alpha", "beta"]);

        // Both fields accessible
        assert!(fields.terms("alpha").is_some());
        assert!(fields.terms("beta").is_some());
        assert!(fields.terms("missing").is_none());
    }

    #[test]
    fn test_unflushed_last_doc() {
        let mut field = make_field("body", IndexOptions::DocsAndFreqs);

        // Only one doc — never flushed to byte stream, only in postings array
        field.add_term(b"hello", 0, 0, 0, 5).unwrap();
        field.sort_terms();

        let fields = FreqProxFields::new(&[&field]);
        let terms = fields.terms("body").unwrap();
        let mut iter = terms.iterator().unwrap();
        iter.next().unwrap();

        let mut postings = iter.postings().unwrap();
        assert_eq!(postings.next_doc().unwrap(), 0);
        assert_eq!(postings.freq(), 1);
        assert_eq!(postings.next_doc().unwrap(), NO_MORE_DOCS);
    }
}
