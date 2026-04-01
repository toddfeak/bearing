// SPDX-License-Identifier: Apache-2.0

//! [`FieldConsumer`] that accumulates per-document term vector data and
//! writes `.tvd`/`.tvx`/`.tvm` files at flush time.

use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::newindex::analyzer::Token;
use crate::newindex::codecs::term_vectors::{
    self, OffsetBuffers, TermVectorDoc, TermVectorField, TermVectorTerm,
};
use crate::newindex::consumer::{FieldConsumer, TokenInterest};
use crate::newindex::field::Field;
use crate::newindex::segment_accumulator::SegmentAccumulator;
use crate::newindex::segment_context::SegmentContext;

/// Accumulates per-document term vector data during indexing and writes
/// `.tvd`/`.tvx`/`.tvm` files at flush time via the DEBT codec copy.
#[derive(Default)]
pub struct TermVectorsConsumer {
    /// Completed term vector documents, in document order.
    /// Only includes documents that have at least one TV field.
    docs: Vec<TermVectorDoc>,
    /// Fields being accumulated for the current document.
    current_fields: Vec<TermVectorField>,
    /// Per-field accumulation state during the current document's current field.
    current_field: Option<TvFieldAccum>,
}

/// Accumulation state for a single field within a single document.
struct TvFieldAccum {
    field_number: u32,
    has_positions: bool,
    has_offsets: bool,
    has_payloads: bool,
    /// Unique terms seen in this field, keyed by term text.
    terms: HashMap<String, TvTermAccum>,
    /// Current absolute position (accumulated from position_increment).
    current_position: i32,
}

/// Accumulation state for a single term within a field.
struct TvTermAccum {
    freq: i32,
    positions: Vec<i32>,
    start_offsets: Vec<i32>,
    end_offsets: Vec<i32>,
}

impl fmt::Debug for TermVectorsConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TermVectorsConsumer")
            .field("doc_count", &self.docs.len())
            .finish()
    }
}

impl TermVectorsConsumer {
    /// Creates a new consumer with no accumulated data.
    pub fn new() -> Self {
        Self::default()
    }
}

impl FieldConsumer for TermVectorsConsumer {
    fn start_document(&mut self, _doc_id: i32) -> io::Result<()> {
        self.current_fields.clear();
        Ok(())
    }

    fn start_field(
        &mut self,
        field_id: u32,
        field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<TokenInterest> {
        let Some(opts) = field.field_type().term_vector_options() else {
            self.current_field = None;
            return Ok(TokenInterest::NoTokens);
        };

        self.current_field = Some(TvFieldAccum {
            field_number: field_id,
            has_positions: opts.has_positions(),
            has_offsets: opts.has_offsets(),
            has_payloads: opts.has_payloads(),
            terms: HashMap::new(),
            current_position: -1,
        });
        Ok(TokenInterest::WantsTokens)
    }

    fn add_token(
        &mut self,
        _field_id: u32,
        _field: &Field,
        token: &Token<'_>,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        let accum = self
            .current_field
            .as_mut()
            .expect("add_token called without active TV field");

        accum.current_position += token.position_increment;

        let term_accum = accum
            .terms
            .entry(token.text.to_string())
            .or_insert_with(|| TvTermAccum {
                freq: 0,
                positions: Vec::new(),
                start_offsets: Vec::new(),
                end_offsets: Vec::new(),
            });

        term_accum.freq += 1;

        if accum.has_positions {
            term_accum.positions.push(accum.current_position);
        }

        if accum.has_offsets {
            term_accum.start_offsets.push(token.start_offset);
            term_accum.end_offsets.push(token.end_offset);
        }

        Ok(())
    }

    fn finish_field(
        &mut self,
        _field_id: u32,
        _field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        let Some(accum) = self.current_field.take() else {
            return Ok(());
        };

        if accum.terms.is_empty() {
            return Ok(());
        }

        // Sort terms by byte order
        let mut sorted_terms: Vec<(String, TvTermAccum)> = accum.terms.into_iter().collect();
        sorted_terms.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));

        let terms = sorted_terms
            .into_iter()
            .map(|(text, ta)| {
                let offsets = if accum.has_offsets {
                    Some(Box::new(OffsetBuffers {
                        start_offsets: ta.start_offsets,
                        end_offsets: ta.end_offsets,
                    }))
                } else {
                    None
                };
                TermVectorTerm {
                    term: text,
                    freq: ta.freq,
                    positions: ta.positions,
                    offsets,
                }
            })
            .collect();

        self.current_fields.push(TermVectorField {
            field_number: accum.field_number,
            has_positions: accum.has_positions,
            has_offsets: accum.has_offsets,
            has_payloads: accum.has_payloads,
            terms,
        });

        Ok(())
    }

    fn finish_document(
        &mut self,
        _doc_id: i32,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        if !self.current_fields.is_empty() {
            let fields = std::mem::take(&mut self.current_fields);
            self.docs.push(TermVectorDoc { fields });
        }
        Ok(())
    }

    fn flush(
        &mut self,
        context: &SegmentContext,
        accumulator: &SegmentAccumulator,
    ) -> io::Result<Vec<String>> {
        if self.docs.is_empty() {
            return Ok(Vec::new());
        }

        term_vectors::write(
            &context.directory,
            &context.segment_name,
            "",
            &context.segment_id,
            &self.docs,
            accumulator.doc_count(),
        )
    }
}

#[cfg(test)]
mod tests {
    use assertables::*;

    use super::*;
    use crate::newindex::field::{TermVectorOptions, text};

    #[test]
    fn test_no_tv_fields_returns_no_tokens() {
        let mut consumer = TermVectorsConsumer::new();
        let mut accum = SegmentAccumulator::new();
        let field = text("contents").value("hello world");
        let interest = consumer.start_field(0, &field, &mut accum).unwrap();
        assert_eq!(interest, TokenInterest::NoTokens);
    }

    #[test]
    fn test_tv_field_returns_wants_tokens() {
        let mut consumer = TermVectorsConsumer::new();
        let mut accum = SegmentAccumulator::new();
        let field = text("contents")
            .with_term_vectors(TermVectorOptions::PositionsAndOffsets)
            .value("hello world");
        let interest = consumer.start_field(0, &field, &mut accum).unwrap();
        assert_eq!(interest, TokenInterest::WantsTokens);
    }

    #[test]
    fn test_single_doc_accumulation() {
        let mut consumer = TermVectorsConsumer::new();
        let mut accum = SegmentAccumulator::new();

        consumer.start_document(0).unwrap();

        let field = text("contents")
            .with_term_vectors(TermVectorOptions::PositionsAndOffsets)
            .value("hello world");
        consumer.start_field(0, &field, &mut accum).unwrap();

        let token1 = Token {
            text: "hello",
            start_offset: 0,
            end_offset: 5,
            position_increment: 1,
        };
        consumer.add_token(0, &field, &token1, &mut accum).unwrap();

        let token2 = Token {
            text: "world",
            start_offset: 6,
            end_offset: 11,
            position_increment: 1,
        };
        consumer.add_token(0, &field, &token2, &mut accum).unwrap();

        consumer.finish_field(0, &field, &mut accum).unwrap();
        consumer.finish_document(0, &mut accum).unwrap();

        assert_len_eq_x!(&consumer.docs, 1);
        assert_len_eq_x!(&consumer.docs[0].fields, 1);
        let tv_field = &consumer.docs[0].fields[0];
        assert!(tv_field.has_positions);
        assert!(tv_field.has_offsets);
        assert!(!tv_field.has_payloads);
        // Terms sorted by byte order: "hello" < "world"
        assert_len_eq_x!(&tv_field.terms, 2);
        assert_eq!(tv_field.terms[0].term, "hello");
        assert_eq!(tv_field.terms[0].freq, 1);
        assert_eq!(tv_field.terms[0].positions, vec![0]);
        assert_eq!(tv_field.terms[1].term, "world");
        assert_eq!(tv_field.terms[1].freq, 1);
        assert_eq!(tv_field.terms[1].positions, vec![1]);
    }

    #[test]
    fn test_repeated_term_accumulation() {
        let mut consumer = TermVectorsConsumer::new();
        let mut accum = SegmentAccumulator::new();

        consumer.start_document(0).unwrap();

        let field = text("contents")
            .with_term_vectors(TermVectorOptions::Positions)
            .value("the cat sat on the mat");
        consumer.start_field(0, &field, &mut accum).unwrap();

        // "the" appears twice at positions 0 and 4
        for (text, pos_inc, start, end) in [
            ("the", 1, 0, 3),
            ("cat", 1, 4, 7),
            ("sat", 1, 8, 11),
            ("on", 1, 12, 14),
            ("the", 1, 15, 18),
            ("mat", 1, 19, 22),
        ] {
            let token = Token {
                text,
                start_offset: start,
                end_offset: end,
                position_increment: pos_inc,
            };
            consumer.add_token(0, &field, &token, &mut accum).unwrap();
        }

        consumer.finish_field(0, &field, &mut accum).unwrap();
        consumer.finish_document(0, &mut accum).unwrap();

        let tv_field = &consumer.docs[0].fields[0];
        // Find "the" — should have freq=2 and positions [0, 4]
        let the_term = tv_field.terms.iter().find(|t| t.term == "the").unwrap();
        assert_eq!(the_term.freq, 2);
        assert_eq!(the_term.positions, vec![0, 4]);
    }

    #[test]
    fn test_doc_without_tv_not_accumulated() {
        let mut consumer = TermVectorsConsumer::new();
        let mut accum = SegmentAccumulator::new();

        // Doc 0: no TV fields
        consumer.start_document(0).unwrap();
        let field = text("contents").value("hello");
        consumer.start_field(0, &field, &mut accum).unwrap();
        consumer.finish_field(0, &field, &mut accum).unwrap();
        consumer.finish_document(0, &mut accum).unwrap();

        assert!(consumer.docs.is_empty());
    }
}
