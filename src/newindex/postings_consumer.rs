// SPDX-License-Identifier: Apache-2.0

// DEBT: parallel to the postings accumulation + flush in index::indexing_chain —
// reconcile after switchover.

//! [`FieldConsumer`] that accumulates terms and postings, then flushes via
//! the block tree terms dictionary and postings codecs.

use std::collections::HashMap;
use std::io;

use crate::codecs::competitive_impact::NormsLookup;
use crate::newindex::analyzer::Token;
use crate::newindex::codecs::blocktree_writer::{BlockTreeTermsWriter, FieldWriteContext};
use crate::newindex::consumer::{FieldConsumer, TokenInterest};
use crate::newindex::field::Field;
use crate::newindex::per_field_postings::PerFieldPostings;
use crate::newindex::segment_accumulator::SegmentAccumulator;
use crate::newindex::segment_context::SegmentContext;
use crate::util::byte_block_pool::{ByteBlockPool, DirectAllocator};

/// Accumulates postings (terms, frequencies, positions) for indexed fields
/// and writes them at flush time via the block tree + postings codecs.
///
/// Owns shared byte pools that all per-field states write into.
pub struct PostingsConsumer {
    /// field_id → per-field postings state
    per_field: HashMap<u32, PerFieldState>,
    /// Shared pool for doc delta + freq byte slices (all fields).
    byte_pool: ByteBlockPool<DirectAllocator>,
    /// Shared pool for position delta byte slices (all fields).
    positions_pool: ByteBlockPool<DirectAllocator>,
    current_doc_id: i32,
    current_position: i32,
    doc_count: i32,
}

/// Per-field state tracked by PostingsConsumer.
struct PerFieldState {
    postings: PerFieldPostings,
    field_name: String,
    field_number: u32,
    /// Set of term IDs that appeared in the current document for this field.
    /// Used to finalize all active terms when the field ends.
    active_term_ids: Vec<usize>,
}

impl std::fmt::Debug for PostingsConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostingsConsumer")
            .field("field_count", &self.per_field.len())
            .field("doc_count", &self.doc_count)
            .finish()
    }
}

impl PostingsConsumer {
    /// Creates a new consumer with empty pools.
    pub fn new() -> Self {
        let mut byte_pool = ByteBlockPool::new(DirectAllocator);
        byte_pool.next_buffer();
        let mut positions_pool = ByteBlockPool::new(DirectAllocator);
        positions_pool.next_buffer();

        Self {
            per_field: HashMap::new(),
            byte_pool,
            positions_pool,
            current_doc_id: 0,
            current_position: 0,
            doc_count: 0,
        }
    }
}

impl Default for PostingsConsumer {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldConsumer for PostingsConsumer {
    fn start_document(&mut self, doc_id: i32) -> io::Result<()> {
        self.current_doc_id = doc_id;
        Ok(())
    }

    fn start_field(
        &mut self,
        field_id: u32,
        field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<TokenInterest> {
        if !field.field_type().tokenized {
            return Ok(TokenInterest::NoTokens);
        }

        self.current_position = 0;

        self.per_field
            .entry(field_id)
            .or_insert_with(|| PerFieldState {
                postings: PerFieldPostings::new(true),
                field_name: field.name().to_string(),
                field_number: field_id,
                active_term_ids: Vec::new(),
            });

        Ok(TokenInterest::WantsTokens)
    }

    fn add_token(
        &mut self,
        field_id: u32,
        _field: &Field,
        token: &Token<'_>,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        self.current_position += token.position_increment;
        let position = self.current_position - 1;

        let state = self
            .per_field
            .get_mut(&field_id)
            .expect("start_field must precede add_token");
        let tid = state.postings.add_term(
            token.text.as_bytes(),
            &mut self.byte_pool,
            Some(&mut self.positions_pool),
        );

        state.postings.record_occurrence(
            tid,
            self.current_doc_id,
            position,
            &mut self.byte_pool,
            Some(&mut self.positions_pool),
        );

        if !state.active_term_ids.contains(&tid) {
            state.active_term_ids.push(tid);
        }

        Ok(())
    }

    fn finish_field(
        &mut self,
        field_id: u32,
        _field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        if let Some(state) = self.per_field.get_mut(&field_id) {
            state.active_term_ids.clear();
        }
        Ok(())
    }

    fn finish_document(
        &mut self,
        _doc_id: i32,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        self.doc_count += 1;
        Ok(())
    }

    fn flush(
        &mut self,
        context: &SegmentContext,
        accumulator: &SegmentAccumulator,
    ) -> io::Result<Vec<String>> {
        if self.per_field.is_empty() {
            return Ok(vec![]);
        }

        // Finalize all pending docs across all fields
        for state in self.per_field.values_mut() {
            state.postings.finalize_all(&mut self.byte_pool);
        }

        // Sort fields by field number for deterministic output
        let mut field_ids: Vec<u32> = self.per_field.keys().copied().collect();
        field_ids.sort();

        // Determine if any field has positions
        let has_positions = true; // all tokenized fields get DOCS_AND_FREQS_AND_POSITIONS

        // Suffix must match PerFieldPostingsFormat.suffix written in .fnm attributes
        let per_field_suffix = "Lucene103_0";

        let mut writer = BlockTreeTermsWriter::new(
            &context.directory,
            &context.segment_name,
            per_field_suffix,
            &context.segment_id,
            has_positions,
        )?;

        let norms_data = accumulator.norms();

        for &field_id in &field_ids {
            let state = self.per_field.get_mut(&field_id).unwrap();

            if state.postings.term_count() == 0 {
                continue;
            }

            let sorted = state.postings.sort_terms();
            let sorted_refs: Vec<(&str, usize)> =
                sorted.iter().map(|(s, id)| (s.as_str(), *id)).collect();

            let norms = if let Some(field_norms) = norms_data.get(&field_id) {
                NormsLookup::new(&field_norms.values, &field_norms.docs)
            } else {
                NormsLookup::no_norms()
            };

            let field_ctx = FieldWriteContext {
                field_name: state.field_name.clone(),
                field_number: state.field_number,
                write_freqs: true,
                write_positions: true,
            };

            writer.write_field(
                &field_ctx,
                &sorted_refs,
                &state.postings,
                &self.byte_pool,
                Some(&self.positions_pool),
                &norms,
            )?;
        }

        writer.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::newindex::field::text_field;
    use crate::store::{MemoryDirectory, SharedDirectory};
    use assertables::*;
    use std::sync::Arc;

    fn test_context() -> SegmentContext {
        SegmentContext {
            directory: Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new()))),
            segment_name: "_0".to_string(),
            segment_id: [0u8; 16],
        }
    }

    fn make_token(text: &str) -> Token<'_> {
        Token {
            text,
            start_offset: 0,
            end_offset: text.len() as i32,
            position_increment: 1,
        }
    }

    /// Process a single document with one text field through the consumer.
    fn process_doc(
        consumer: &mut PostingsConsumer,
        doc_id: i32,
        field_id: u32,
        field: &Field,
        tokens: &[&str],
        acc: &mut SegmentAccumulator,
    ) {
        consumer.start_document(doc_id).unwrap();
        consumer.start_field(field_id, field, acc).unwrap();
        for &t in tokens {
            let token = make_token(t);
            consumer.add_token(field_id, field, &token, acc).unwrap();
        }
        consumer.finish_field(field_id, field, acc).unwrap();
        consumer.finish_document(doc_id, acc).unwrap();
    }

    #[test]
    fn single_doc_produces_postings_files() {
        let ctx = test_context();
        let mut consumer = PostingsConsumer::new();
        let mut acc = SegmentAccumulator::new();
        let field = text_field("body", "ignored");

        process_doc(&mut consumer, 0, 0, &field, &["hello", "world"], &mut acc);

        let names = consumer.flush(&ctx, &acc).unwrap();

        assert_len_eq_x!(&names, 6);
        assert!(names.contains(&"_0_Lucene103_0.tim".to_string()));
        assert!(names.contains(&"_0_Lucene103_0.tip".to_string()));
        assert!(names.contains(&"_0_Lucene103_0.tmd".to_string()));
        assert!(names.contains(&"_0_Lucene103_0.doc".to_string()));
        assert!(names.contains(&"_0_Lucene103_0.pos".to_string()));
        assert!(names.contains(&"_0_Lucene103_0.psm".to_string()));
    }

    #[test]
    fn multiple_docs_produce_postings() {
        let ctx = test_context();
        let mut consumer = PostingsConsumer::new();
        let mut acc = SegmentAccumulator::new();
        let field = text_field("body", "ignored");

        process_doc(&mut consumer, 0, 0, &field, &["hello", "world"], &mut acc);
        process_doc(&mut consumer, 1, 0, &field, &["hello", "rust"], &mut acc);
        process_doc(&mut consumer, 2, 0, &field, &["world", "rust"], &mut acc);

        let names = consumer.flush(&ctx, &acc).unwrap();
        assert_ge!(names.len(), 5);
    }

    #[test]
    fn stored_only_field_ignored() {
        let ctx = test_context();
        let mut consumer = PostingsConsumer::new();
        let mut acc = SegmentAccumulator::new();
        let field = crate::newindex::field::stored_field("title", "hello");

        consumer.start_document(0).unwrap();
        let interest = consumer.start_field(0, &field, &mut acc).unwrap();
        assert_eq!(interest, TokenInterest::NoTokens);
        consumer.finish_field(0, &field, &mut acc).unwrap();
        consumer.finish_document(0, &mut acc).unwrap();

        let names = consumer.flush(&ctx, &acc).unwrap();
        assert_is_empty!(&names);
    }

    #[test]
    fn multiple_fields_produce_postings() {
        let ctx = test_context();
        let mut consumer = PostingsConsumer::new();
        let mut acc = SegmentAccumulator::new();
        let title = text_field("title", "ignored");
        let body = text_field("body", "ignored");

        consumer.start_document(0).unwrap();

        consumer.start_field(0, &title, &mut acc).unwrap();
        let token = make_token("hello");
        consumer.add_token(0, &title, &token, &mut acc).unwrap();
        consumer.finish_field(0, &title, &mut acc).unwrap();

        consumer.start_field(1, &body, &mut acc).unwrap();
        let token = make_token("world");
        consumer.add_token(1, &body, &token, &mut acc).unwrap();
        consumer.finish_field(1, &body, &mut acc).unwrap();

        consumer.finish_document(0, &mut acc).unwrap();

        let names = consumer.flush(&ctx, &acc).unwrap();
        assert_ge!(names.len(), 5);
    }

    #[test]
    fn no_tokenized_fields_produces_no_files() {
        let ctx = test_context();
        let mut consumer = PostingsConsumer::new();
        let acc = SegmentAccumulator::new();

        let names = consumer.flush(&ctx, &acc).unwrap();
        assert_is_empty!(&names);
    }
}
