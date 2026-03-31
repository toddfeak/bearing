// SPDX-License-Identifier: Apache-2.0

//! [`FieldConsumer`] that computes norms from token counts and writes `.nvm`, `.nvd`.

use std::collections::HashMap;
use std::io;

use crate::newindex::analyzer::Token;
use crate::newindex::codecs::norms::{self, NormsFieldData};
use crate::newindex::consumer::{FieldConsumer, TokenInterest};
use crate::newindex::field::Field;
use crate::newindex::segment_accumulator::SegmentAccumulator;
use crate::newindex::segment_context::SegmentContext;
use crate::util::small_float;

/// Per-field accumulated norms.
struct PerFieldNorms {
    field_name: String,
    norms: Vec<i64>,
    docs: Vec<i32>,
}

/// Computes and writes per-field norms from token counts.
///
/// For each tokenized field that has norms enabled, counts tokens via
/// `add_token` and computes a SmallFloat-encoded norm in `finish_field`.
/// At flush time, writes `.nvm` and `.nvd` via the norms codec.
#[derive(Default)]
pub struct NormsConsumer {
    per_field: HashMap<u32, PerFieldNorms>,
    current_token_count: i32,
    current_has_norms: bool,
    current_doc_id: i32,
    doc_count: i32,
}

impl std::fmt::Debug for NormsConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NormsConsumer")
            .field("field_count", &self.per_field.len())
            .finish()
    }
}

impl NormsConsumer {
    /// Creates a new consumer.
    pub fn new() -> Self {
        Self {
            per_field: HashMap::new(),
            current_token_count: 0,
            current_has_norms: false,
            current_doc_id: 0,
            doc_count: 0,
        }
    }
}

/// Computes the BM25 norm value for a field.
///
/// Encodes the field length (token count) as a single byte using
/// SmallFloat, then sign-extends through i8 to match Java's widening.
fn compute_norm(field_length: i32) -> i64 {
    small_float::int_to_byte4(field_length) as i8 as i64
}

impl FieldConsumer for NormsConsumer {
    fn start_document(&mut self, doc_id: i32) -> io::Result<()> {
        self.current_doc_id = doc_id;
        Ok(())
    }

    fn start_field(
        &mut self,
        _field_id: u32,
        field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<TokenInterest> {
        let ft = field.field_type();
        self.current_has_norms = ft.tokenized && !ft.omit_norms;
        self.current_token_count = 0;

        if self.current_has_norms {
            Ok(TokenInterest::WantsTokens)
        } else {
            Ok(TokenInterest::NoTokens)
        }
    }

    fn add_token(
        &mut self,
        _field_id: u32,
        _field: &Field,
        token: &Token<'_>,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        self.current_token_count += token.position_increment;
        Ok(())
    }

    fn finish_field(
        &mut self,
        field_id: u32,
        field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        if self.current_has_norms && self.current_token_count > 0 {
            let norm = compute_norm(self.current_token_count);
            let entry = self
                .per_field
                .entry(field_id)
                .or_insert_with(|| PerFieldNorms {
                    field_name: field.name().to_string(),
                    norms: Vec::new(),
                    docs: Vec::new(),
                });
            entry.norms.push(norm);
            entry.docs.push(self.current_doc_id);
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
        _accumulator: &SegmentAccumulator,
    ) -> io::Result<Vec<String>> {
        if self.per_field.is_empty() {
            return Ok(vec![]);
        }

        // Build sorted field data for the codec writer
        let mut fields: Vec<NormsFieldData> = self
            .per_field
            .drain()
            .map(|(field_number, pf)| NormsFieldData {
                field_name: pf.field_name,
                field_number,
                norms: pf.norms,
                docs: pf.docs,
            })
            .collect();
        fields.sort_by_key(|f| f.field_number);

        norms::write(
            &context.directory,
            &context.segment_name,
            "",
            &context.segment_id,
            &fields,
            self.doc_count,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::newindex::field::{FieldBuilder, FieldType, text_field};
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

    /// Simulates the worker calling start_field → add_token (N times) → finish_field
    /// for a tokenized field with the given token count.
    fn process_tokenized_field(
        consumer: &mut NormsConsumer,
        field_id: u32,
        field: &Field,
        token_count: i32,
        acc: &mut SegmentAccumulator,
    ) {
        consumer.start_field(field_id, field, acc).unwrap();
        let mut buf = String::new();
        for _ in 0..token_count {
            buf.clear();
            buf.push_str("token");
            let token = Token {
                text: &buf,
                start_offset: 0,
                end_offset: 5,
                position_increment: 1,
            };
            consumer.add_token(field_id, field, &token, acc).unwrap();
        }
        consumer.finish_field(field_id, field, acc).unwrap();
    }

    #[test]
    fn computes_norms_from_token_count() {
        let mut consumer = NormsConsumer::new();
        let mut acc = SegmentAccumulator::new();
        let field = text_field("body", "ignored");

        // Doc 0: 3 tokens, Doc 1: 10 tokens, Doc 2: 1 token
        for (doc_id, count) in [(0, 3), (1, 10), (2, 1)] {
            consumer.start_document(doc_id).unwrap();
            process_tokenized_field(&mut consumer, 0, &field, count, &mut acc);
            consumer.finish_document(doc_id, &mut acc).unwrap();
        }

        let ctx = test_context();
        let names = consumer.flush(&ctx, &acc).unwrap();
        assert_len_eq_x!(&names, 2);
        assert_eq!(names[0], "_0.nvm");
        assert_eq!(names[1], "_0.nvd");
    }

    #[test]
    fn omit_norms_produces_no_files() {
        let mut consumer = NormsConsumer::new();
        let mut acc = SegmentAccumulator::new();
        let field = FieldBuilder::new("body")
            .field_type(FieldType {
                tokenized: true,
                omit_norms: true,
                ..Default::default()
            })
            .string_value("ignored")
            .build();

        consumer.start_document(0).unwrap();
        process_tokenized_field(&mut consumer, 0, &field, 5, &mut acc);
        consumer.finish_document(0, &mut acc).unwrap();

        let ctx = test_context();
        let names = consumer.flush(&ctx, &acc).unwrap();
        assert_is_empty!(&names);
    }

    #[test]
    fn non_tokenized_produces_no_files() {
        let mut consumer = NormsConsumer::new();
        let mut acc = SegmentAccumulator::new();
        let field = FieldBuilder::new("title")
            .field_type(FieldType {
                stored: true,
                ..Default::default()
            })
            .string_value("ignored")
            .build();

        consumer.start_document(0).unwrap();
        consumer.start_field(0, &field, &mut acc).unwrap();
        consumer.finish_field(0, &field, &mut acc).unwrap();
        consumer.finish_document(0, &mut acc).unwrap();

        let ctx = test_context();
        let names = consumer.flush(&ctx, &acc).unwrap();
        assert_is_empty!(&names);
    }

    #[test]
    fn zero_tokens_produces_no_norm_for_that_doc() {
        let mut consumer = NormsConsumer::new();
        let mut acc = SegmentAccumulator::new();
        let field = text_field("body", "ignored");

        // Doc 0: 3 tokens (gets norm), Doc 1: 0 tokens (no norm)
        consumer.start_document(0).unwrap();
        process_tokenized_field(&mut consumer, 0, &field, 3, &mut acc);
        consumer.finish_document(0, &mut acc).unwrap();

        consumer.start_document(1).unwrap();
        process_tokenized_field(&mut consumer, 0, &field, 0, &mut acc);
        consumer.finish_document(1, &mut acc).unwrap();

        let ctx = test_context();
        let names = consumer.flush(&ctx, &acc).unwrap();
        // Should still write files (1 doc has norms)
        assert_len_eq_x!(&names, 2);
    }

    #[test]
    fn compute_norm_matches_expected_values() {
        // SmallFloat encoding: small values are identity, larger are lossy
        assert_eq!(compute_norm(1), 1);
        assert_eq!(compute_norm(2), 2);
        assert_eq!(compute_norm(3), 3);

        // Larger values get compressed — verify sign extension
        let norm_100 = compute_norm(100);
        assert_ne!(norm_100, 100); // lossy
        assert_gt!(norm_100, 0); // positive field length → positive norm
    }
}
