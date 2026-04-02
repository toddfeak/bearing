// SPDX-License-Identifier: Apache-2.0

//! [`FieldConsumer`] that tracks field metadata and writes `.fnm`.
//!
//! Registered last in the consumer list so that all other consumers
//! have finished processing before field infos are written.

use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::newindex::analyzer::Token;
use crate::newindex::codecs::field_infos::{self, FieldInfo, FieldInfos};
use crate::newindex::consumer::{FieldConsumer, TokenInterest};
use crate::newindex::field::{Field, PointsValue};
use crate::newindex::segment_accumulator::SegmentAccumulator;
use crate::newindex::segment_context::SegmentContext;

/// Tracks field metadata from `start_field` calls and writes the `.fnm`
/// file at flush time.
#[derive(Default, mem_dbg::MemSize)]
pub struct FieldInfosConsumer {
    fields: HashMap<u32, FieldInfo>,
}

impl fmt::Debug for FieldInfosConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FieldInfosConsumer")
            .field("field_count", &self.fields.len())
            .finish()
    }
}

impl FieldInfosConsumer {
    /// Creates a new consumer.
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }
}

impl FieldConsumer for FieldInfosConsumer {
    fn start_document(&mut self, _doc_id: i32) -> io::Result<()> {
        Ok(())
    }

    fn start_field(
        &mut self,
        field_id: u32,
        field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<TokenInterest> {
        let (pdim, pidx, pbytes) = match field.field_type().points() {
            None => (0, 0, 0),
            Some(PointsValue::Single {
                bytes_per_dim,
                encoded,
            }) => {
                let dims = (encoded.len() / bytes_per_dim) as u32;
                (dims, dims, *bytes_per_dim as u32)
            }
            Some(PointsValue::Range {
                dims,
                bytes_per_dim,
                ..
            }) => {
                let d = (*dims * 2) as u32;
                (d, d, *bytes_per_dim as u32)
            }
        };
        self.fields.entry(field_id).or_insert_with(|| FieldInfo {
            name: field.name().to_string(),
            number: field_id,
            store_term_vectors: field.field_type().term_vector_options().is_some(),
            has_norms: field.field_type().has_norms(),
            index_options: field.field_type().index_options() as u8,
            doc_values_type: field.field_type().doc_values_type(),
            point_dimension_count: pdim,
            point_index_dimension_count: pidx,
            point_num_bytes: pbytes,
        });
        Ok(TokenInterest::NoTokens)
    }

    fn add_token(
        &mut self,
        _field_id: u32,
        _field: &Field,
        _token: &Token<'_>,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        Ok(())
    }

    fn finish_field(
        &mut self,
        _field_id: u32,
        _field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<()> {
        Ok(())
    }

    fn finish_document(
        &mut self,
        _doc_id: i32,
        _accumulator: &mut SegmentAccumulator,
        _context: &SegmentContext,
    ) -> io::Result<()> {
        Ok(())
    }

    fn flush(
        &mut self,
        context: &SegmentContext,
        _accumulator: &SegmentAccumulator,
    ) -> io::Result<Vec<String>> {
        let mut fields: Vec<FieldInfo> = self.fields.values().cloned().collect();
        fields.sort_by_key(|f| f.number);
        let fis = FieldInfos::new(fields);
        let name = field_infos::write(
            &context.directory,
            &context.segment_name,
            "",
            &context.segment_id,
            &fis,
        )?;
        Ok(vec![name])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::newindex::field::stored;
    use crate::store::{MemoryDirectory, SharedDirectory};
    use std::sync::Arc;

    fn test_context() -> SegmentContext {
        SegmentContext {
            directory: Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new()))),
            segment_name: "_0".to_string(),
            segment_id: [0u8; 16],
        }
    }

    #[test]
    fn flush_produces_fnm_file() {
        let context = test_context();
        let mut consumer = FieldInfosConsumer::new();
        let mut acc = SegmentAccumulator::new();

        consumer.start_document(0).unwrap();
        let f = stored("title").string("hello");
        consumer.start_field(0, &f, &mut acc).unwrap();
        consumer.finish_field(0, &f, &mut acc).unwrap();
        consumer.finish_document(0, &mut acc, &context).unwrap();

        let names = consumer.flush(&context, &acc).unwrap();
        assert_eq!(names, vec!["_0.fnm"]);
    }

    #[test]
    fn tracks_multiple_fields() {
        let context = test_context();
        let mut consumer = FieldInfosConsumer::new();
        let mut acc = SegmentAccumulator::new();

        consumer.start_document(0).unwrap();

        let f1 = stored("title").string("t");
        consumer.start_field(0, &f1, &mut acc).unwrap();
        consumer.finish_field(0, &f1, &mut acc).unwrap();

        let f2 = stored("body").string("b");
        consumer.start_field(1, &f2, &mut acc).unwrap();
        consumer.finish_field(1, &f2, &mut acc).unwrap();

        consumer.finish_document(0, &mut acc, &context).unwrap();

        let names = consumer.flush(&context, &acc).unwrap();
        assert_eq!(names, vec!["_0.fnm"]);

        // Verify file has content
        let guard = context.directory.lock().unwrap();
        let data = guard.read_file("_0.fnm").unwrap();

        // Header magic
        assert_eq!(&data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);

        // Footer magic
        let footer_start = data.len() - 16;
        assert_eq!(
            &data[footer_start..footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
    }

    #[test]
    fn deduplicates_same_field_across_docs() {
        let context = test_context();
        let mut consumer = FieldInfosConsumer::new();
        let mut acc = SegmentAccumulator::new();

        for doc_id in 0..3 {
            consumer.start_document(doc_id).unwrap();
            let f = stored("title").string(format!("t{doc_id}"));
            consumer.start_field(0, &f, &mut acc).unwrap();
            consumer.finish_field(0, &f, &mut acc).unwrap();
            consumer
                .finish_document(doc_id, &mut acc, &context)
                .unwrap();
        }

        // Should only have 1 field registered despite 3 documents
        assert_eq!(consumer.fields.len(), 1);

        let names = consumer.flush(&context, &acc).unwrap();
        assert_eq!(names, vec!["_0.fnm"]);
    }

    #[test]
    fn mem_size_is_struct_size() {
        use mem_dbg::{MemSize, SizeFlags};
        let consumer = FieldInfosConsumer::new();
        assert_eq!(
            consumer.mem_size(SizeFlags::CAPACITY),
            std::mem::size_of::<FieldInfosConsumer>()
        );
    }
}
