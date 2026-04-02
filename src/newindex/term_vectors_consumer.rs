// SPDX-License-Identifier: Apache-2.0

//! [`FieldConsumer`] that accumulates per-document term vector data and
//! writes `.tvd`/`.tvx`/`.tvm` files at flush time.
//!
//! Uses pool-based `TermVectorsConsumerPerField` instances to accumulate
//! per-document term vector data, then streams it to the codec writer
//! at `finish_document` time. Pools are reset per-document.

use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::newindex::analyzer::Token;
use crate::newindex::codecs::term_vectors::{self, TermVectorChunkWriter};
use crate::newindex::consumer::{FieldConsumer, TokenInterest};
use crate::newindex::field::Field;
use crate::newindex::segment_accumulator::SegmentAccumulator;
use crate::newindex::segment_context::SegmentContext;
use crate::newindex::term_vectors_consumer_per_field::TermVectorsConsumerPerField;
use crate::newindex::terms_hash::{TermsHash, TermsHashPerFieldTrait};

/// Accumulates per-document term vector data during indexing and writes
/// `.tvd`/`.tvx`/`.tvm` files at flush time via the DEBT codec copy.
///
/// Owns a per-document `TermsHash` (TV pools) that is reset after each
/// document is written to the codec writer.
pub struct TermVectorsConsumer {
    /// Lazy-initialized codec writer.
    writer: Option<TermVectorChunkWriter>,
    /// Shared TV pools, reset per-document.
    tv_terms_hash: TermsHash,
    /// Per-field TV consumers, keyed by field_id.
    per_field: HashMap<u32, TvPerFieldState>,
    /// Fields that have TV data in the current document.
    active_field_ids: Vec<u32>,
    /// Whether the current document has any term vector fields.
    has_vectors: bool,
    /// Number of documents processed (for gap-filling non-TV docs).
    num_docs: i32,
    /// Last doc ID written to the TV writer.
    tv_last_doc_id: i32,
    /// Current doc ID being processed.
    current_doc_id: i32,
}

/// Per-field state for term vector processing.
struct TvPerFieldState {
    tv_pf: TermVectorsConsumerPerField,
    field_number: u32,
    /// Accumulated absolute position for the current field in the current document.
    current_position: i32,
}

impl mem_dbg::MemSize for TermVectorsConsumer {
    fn mem_size_rec(
        &self,
        _flags: mem_dbg::SizeFlags,
        _refs: &mut mem_dbg::HashMap<usize, usize>,
    ) -> usize {
        // TV pools are reset per-document, so baseline is small
        std::mem::size_of::<Self>()
    }
}

impl fmt::Debug for TermVectorsConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TermVectorsConsumer")
            .field("field_count", &self.per_field.len())
            .field("has_vectors", &self.has_vectors)
            .finish()
    }
}

impl Default for TermVectorsConsumer {
    fn default() -> Self {
        Self::new()
    }
}

impl TermVectorsConsumer {
    /// Creates a new consumer with no accumulated data.
    pub fn new() -> Self {
        Self {
            writer: None,
            tv_terms_hash: TermsHash::new(),
            per_field: HashMap::new(),
            active_field_ids: Vec::new(),
            has_vectors: false,
            num_docs: 0,
            tv_last_doc_id: 0,
            current_doc_id: 0,
        }
    }
}

impl FieldConsumer for TermVectorsConsumer {
    fn start_document(&mut self, doc_id: i32) -> io::Result<()> {
        self.current_doc_id = doc_id;
        self.has_vectors = false;
        self.active_field_ids.clear();
        Ok(())
    }

    fn start_field(
        &mut self,
        field_id: u32,
        field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<TokenInterest> {
        let Some(opts) = field.field_type().term_vector_options() else {
            return Ok(TokenInterest::NoTokens);
        };

        self.has_vectors = true;

        let state = self
            .per_field
            .entry(field_id)
            .or_insert_with(|| TvPerFieldState {
                tv_pf: TermVectorsConsumerPerField::new(field.name().to_string()),
                field_number: field_id,
                current_position: -1,
            });

        // Reset position tracking for this field in this document
        state.current_position = -1;

        // Configure for this document's field
        state.tv_pf.do_vectors = true;
        state.tv_pf.do_vector_positions = opts.has_positions();
        state.tv_pf.do_vector_offsets = opts.has_offsets();
        state.tv_pf.do_vector_payloads = opts.has_payloads();

        self.active_field_ids.push(field_id);

        Ok(TokenInterest::WantsTokens)
    }

    fn add_token(
        &mut self,
        field_id: u32,
        _field: &Field,
        token: &Token<'_>,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        let state = self
            .per_field
            .get_mut(&field_id)
            .expect("add_token called without active TV field");

        // Accumulate absolute position from position_increment
        state.current_position += token.position_increment;

        state.tv_pf.current_position = state.current_position;
        state.tv_pf.current_start_offset = token.start_offset;
        state.tv_pf.current_end_offset = token.end_offset;

        // Intern term bytes directly in the TV byte pool via add()
        TermsHashPerFieldTrait::add(
            &mut state.tv_pf,
            &mut self.tv_terms_hash,
            token.text.as_bytes(),
            self.current_doc_id,
        );

        Ok(())
    }

    fn finish_field(
        &mut self,
        _field_id: u32,
        _field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        // Data is in the pools — nothing to do here
        Ok(())
    }

    fn finish_document(
        &mut self,
        doc_id: i32,
        _accumulator: &mut SegmentAccumulator,
        context: &SegmentContext,
    ) -> io::Result<()> {
        self.num_docs += 1;

        if !self.has_vectors {
            return Ok(());
        }

        // Lazy-create the writer
        if self.writer.is_none() {
            let tvd_name = crate::newindex::index_file_names::segment_file_name(
                &context.segment_name,
                "",
                term_vectors::VECTORS_EXTENSION,
            );
            let tvd = {
                let mut dir = context.directory.lock().unwrap();
                dir.create_output(&tvd_name)?
            };
            self.writer = Some(TermVectorChunkWriter::new(tvd, &context.segment_id, "")?);
        }

        let writer = self.writer.as_mut().unwrap();

        // Fill gaps for docs without vectors
        while self.tv_last_doc_id < doc_id {
            writer.start_document(0);
            writer.finish_document()?;
            self.tv_last_doc_id += 1;
        }

        // Count active fields
        let num_vector_fields = self.active_field_ids.len() as i32;
        writer.start_document(num_vector_fields);

        // Sort active fields by field number for deterministic output
        self.active_field_ids.sort();

        for &field_id in &self.active_field_ids.clone() {
            let state = self.per_field.get_mut(&field_id).unwrap();
            if state.tv_pf.has_data() {
                state.tv_pf.finish_document_self_owned(
                    state.field_number,
                    &self.tv_terms_hash,
                    writer,
                )?;
            }
        }

        writer.finish_document()?;
        self.tv_last_doc_id = doc_id + 1;

        // Reset TV pools for next document
        self.tv_terms_hash.reset();
        for state in self.per_field.values_mut() {
            state.tv_pf.reset();
        }

        Ok(())
    }

    fn flush(
        &mut self,
        context: &SegmentContext,
        accumulator: &SegmentAccumulator,
    ) -> io::Result<Vec<String>> {
        let Some(writer) = self.writer.take() else {
            return Ok(Vec::new());
        };

        // Fill remaining gap docs
        // The writer's finish will flush any remaining pending docs
        writer.finish(
            &context.directory,
            &context.segment_name,
            "",
            &context.segment_id,
            accumulator.doc_count(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assertables::*;

    use super::*;
    use crate::newindex::field::{TermVectorOptions, text};
    use crate::store::{MemoryDirectory, SharedDirectory};

    fn test_context() -> SegmentContext {
        SegmentContext {
            directory: Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new()))),
            segment_name: "_0".to_string(),
            segment_id: [0u8; 16],
        }
    }

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
    fn test_doc_without_tv_not_written() {
        let context = test_context();
        let mut consumer = TermVectorsConsumer::new();
        let mut accum = SegmentAccumulator::new();

        consumer.start_document(0).unwrap();
        let field = text("contents").value("hello");
        consumer.start_field(0, &field, &mut accum).unwrap();
        consumer.finish_field(0, &field, &mut accum).unwrap();
        consumer.finish_document(0, &mut accum, &context).unwrap();

        // No writer created
        assert!(consumer.writer.is_none());
    }

    #[test]
    fn mem_size_empty_is_small() {
        use mem_dbg::{MemSize, SizeFlags};
        let consumer = TermVectorsConsumer::new();
        assert_lt!(consumer.mem_size(SizeFlags::CAPACITY), 1000);
    }
}
