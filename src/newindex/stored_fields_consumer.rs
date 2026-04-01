// SPDX-License-Identifier: Apache-2.0

//! [`FieldConsumer`] that buffers stored field data and writes `.fdt`, `.fdx`, `.fdm`.

use std::io;
use std::mem;

use crate::document::StoredValue as CodecStoredValue;
use crate::newindex::analyzer::Token;
use crate::newindex::codecs::stored_fields::{self, StoredDoc};
use crate::newindex::consumer::{FieldConsumer, TokenInterest};
use crate::newindex::field::{Field, StoredValue};
use crate::newindex::segment_accumulator::SegmentAccumulator;
use crate::newindex::segment_context::SegmentContext;

/// Converts a newindex [`StoredValue`] to the codec's [`CodecStoredValue`].
// DEBT: remove after switchover when codec uses newindex types directly
fn to_codec_stored_value(sv: &StoredValue) -> CodecStoredValue {
    match sv {
        StoredValue::String(s) => CodecStoredValue::String(s.clone()),
        StoredValue::Bytes(b) => CodecStoredValue::Bytes(b.clone()),
        StoredValue::Int(v) => CodecStoredValue::Int(*v),
        StoredValue::Long(v) => CodecStoredValue::Long(*v),
        StoredValue::Float(v) => CodecStoredValue::Float(*v),
        StoredValue::Double(v) => CodecStoredValue::Double(*v),
    }
}

/// Buffers stored field values per document and flushes them as
/// compressed stored fields files via the codec writer.
#[derive(Debug, Default, mem_dbg::MemSize)]
pub struct StoredFieldsConsumer {
    docs: Vec<StoredDoc>,
    current_doc_fields: Vec<(u32, CodecStoredValue)>,
}

impl StoredFieldsConsumer {
    /// Creates a new consumer.
    pub fn new() -> Self {
        Self {
            docs: Vec::new(),
            current_doc_fields: Vec::new(),
        }
    }
}

impl FieldConsumer for StoredFieldsConsumer {
    fn start_document(&mut self, _doc_id: i32) -> io::Result<()> {
        self.current_doc_fields.clear();
        Ok(())
    }

    fn start_field(
        &mut self,
        field_id: u32,
        field: &Field,
        _accumulator: &mut SegmentAccumulator,
    ) -> io::Result<TokenInterest> {
        if let Some(sv) = field.field_type().stored() {
            self.current_doc_fields
                .push((field_id, to_codec_stored_value(sv)));
        }
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
    ) -> io::Result<()> {
        let fields = mem::take(&mut self.current_doc_fields);
        self.docs.push(StoredDoc { fields });
        Ok(())
    }

    fn flush(
        &mut self,
        context: &SegmentContext,
        _accumulator: &SegmentAccumulator,
    ) -> io::Result<Vec<String>> {
        let num_docs = self.docs.len() as i32;
        stored_fields::write(
            &context.directory,
            &context.segment_name,
            "",
            &context.segment_id,
            &self.docs,
            num_docs,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use super::*;
    use crate::newindex::field::{stored, text};
    use crate::store::{MemoryDirectory, SharedDirectory};

    fn test_context() -> SegmentContext {
        SegmentContext {
            directory: Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new()))),
            segment_name: "_0".to_string(),
            segment_id: [0u8; 16],
        }
    }

    #[test]
    fn flush_produces_three_files() {
        let ctx = test_context();
        let mut consumer = StoredFieldsConsumer::new();
        let mut acc = SegmentAccumulator::new();

        consumer.start_document(0).unwrap();
        let field = stored("title").string("hello");
        consumer.start_field(0, &field, &mut acc).unwrap();
        consumer.finish_field(0, &field, &mut acc).unwrap();
        consumer.finish_document(0, &mut acc).unwrap();

        let names = consumer.flush(&ctx, &acc).unwrap();
        assert_eq!(names.len(), 3);
        assert_eq!(names[0], "_0.fdt");
        assert_eq!(names[1], "_0.fdx");
        assert_eq!(names[2], "_0.fdm");
    }

    #[test]
    fn non_stored_field_is_ignored() {
        let ctx = test_context();
        let mut consumer = StoredFieldsConsumer::new();
        let mut acc = SegmentAccumulator::new();

        consumer.start_document(0).unwrap();
        let field = text("not_stored").reader(Cursor::new(b"invisible".to_vec()));
        consumer.start_field(0, &field, &mut acc).unwrap();
        consumer.finish_field(0, &field, &mut acc).unwrap();
        consumer.finish_document(0, &mut acc).unwrap();

        // Flush should still produce files (empty stored fields)
        let names = consumer.flush(&ctx, &acc).unwrap();
        assert_eq!(names.len(), 3);
    }

    #[test]
    fn multiple_docs_multiple_fields() {
        let ctx = test_context();
        let mut consumer = StoredFieldsConsumer::new();
        let mut acc = SegmentAccumulator::new();

        for doc_id in 0..3 {
            consumer.start_document(doc_id).unwrap();

            let f1 = stored("title").string(format!("title {doc_id}"));
            consumer.start_field(0, &f1, &mut acc).unwrap();
            consumer.finish_field(0, &f1, &mut acc).unwrap();

            let f2 = stored("body").string(format!("body {doc_id}"));
            consumer.start_field(1, &f2, &mut acc).unwrap();
            consumer.finish_field(1, &f2, &mut acc).unwrap();

            consumer.finish_document(doc_id, &mut acc).unwrap();
        }

        let names = consumer.flush(&ctx, &acc).unwrap();
        assert_eq!(names.len(), 3);

        // Verify files have content
        let guard = ctx.directory.lock().unwrap();
        for name in &names {
            let data = guard.read_file(name).unwrap();
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn mem_size_empty_is_small() {
        use mem_dbg::{MemSize, SizeFlags};
        let consumer = StoredFieldsConsumer::new();
        // Derived MemSize includes size_of::<Self>() for the stack portion.
        assert_lt!(consumer.mem_size(SizeFlags::CAPACITY), 200);
    }

    #[test]
    fn mem_size_grows_with_documents() {
        use mem_dbg::{MemSize, SizeFlags};
        let mut consumer = StoredFieldsConsumer::new();
        let mut acc = SegmentAccumulator::new();

        for doc_id in 0..10 {
            consumer.start_document(doc_id).unwrap();
            let field = stored("title").string(format!("doc {doc_id}"));
            consumer.start_field(0, &field, &mut acc).unwrap();
            consumer.finish_field(0, &field, &mut acc).unwrap();
            consumer.finish_document(doc_id, &mut acc).unwrap();
        }

        assert_gt!(consumer.mem_size(SizeFlags::CAPACITY), 0);
    }
}
