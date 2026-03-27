// SPDX-License-Identifier: Apache-2.0

//! Streams stored field data directly to codec during document processing.
//!
//! Instead of buffering `StoredDoc` structs in memory, this consumer writes
//! stored fields to the codec writer incrementally as documents are indexed.
//! This eliminates stored field RAM accumulation entirely.

use std::io;

use crate::document::StoredValue;
use crate::index::FieldInfo;

/// Trait for a codec-level stored fields writer that receives streaming data.
///
/// Implementations write stored field data to output files incrementally.
/// The lifecycle is: `start_document` -> N x `write_field` -> `finish_document`,
/// repeated for each document.
pub trait StoredFieldsWriter {
    /// Begins a new document.
    fn start_document(&mut self) -> io::Result<()>;

    /// Writes a single stored field value.
    fn write_field(&mut self, field_info: &FieldInfo, value: &StoredValue) -> io::Result<()>;

    /// Finishes the current document.
    fn finish_document(&mut self) -> io::Result<()>;

    /// Finalizes the writer after all documents have been written.
    fn finish(&mut self, num_docs: i32) -> io::Result<()>;
}

/// Consumes stored fields during document processing, delegating to a
/// [`StoredFieldsWriter`] for incremental writes.
///
/// Handles gap-filling for documents that have no stored fields by writing
/// empty documents to maintain document ID alignment.
pub struct StoredFieldsConsumer<W: StoredFieldsWriter> {
    writer: W,
    last_doc: i32,
}

impl<W: StoredFieldsWriter> StoredFieldsConsumer<W> {
    /// Creates a new consumer with the given writer.
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            last_doc: -1,
        }
    }

    /// Begins storing fields for the given document.
    ///
    /// If there are gaps (documents between `last_doc` and `doc_id` with
    /// no stored fields), empty documents are written to fill the gap.
    pub fn start_document(&mut self, doc_id: i32) -> io::Result<()> {
        assert!(self.last_doc < doc_id);
        // Fill gap documents
        while self.last_doc + 1 < doc_id {
            self.last_doc += 1;
            self.writer.start_document()?;
            self.writer.finish_document()?;
        }
        self.last_doc = doc_id;
        self.writer.start_document()
    }

    /// Writes a single stored field.
    pub fn write_field(&mut self, field_info: &FieldInfo, value: &StoredValue) -> io::Result<()> {
        self.writer.write_field(field_info, value)
    }

    /// Finishes the current document.
    pub fn finish_document(&mut self) -> io::Result<()> {
        self.writer.finish_document()
    }

    /// Fills remaining documents up to `max_doc` and finalizes the writer.
    pub fn finish(&mut self, max_doc: i32) -> io::Result<()> {
        while self.last_doc < max_doc - 1 {
            self.start_document(self.last_doc + 1)?;
            self.finish_document()?;
        }
        self.writer.finish(max_doc)
    }
}

impl<W: StoredFieldsWriter + std::fmt::Debug> std::fmt::Debug for StoredFieldsConsumer<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredFieldsConsumer")
            .field("last_doc", &self.last_doc)
            .field("writer", &self.writer)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test writer that records all calls for verification.
    #[derive(Debug, Default)]
    struct MockWriter {
        events: Vec<String>,
    }

    impl StoredFieldsWriter for MockWriter {
        fn start_document(&mut self) -> io::Result<()> {
            self.events.push("start".to_string());
            Ok(())
        }

        fn write_field(&mut self, field_info: &FieldInfo, _value: &StoredValue) -> io::Result<()> {
            self.events.push(format!("field:{}", field_info.name()));
            Ok(())
        }

        fn finish_document(&mut self) -> io::Result<()> {
            self.events.push("finish".to_string());
            Ok(())
        }

        fn finish(&mut self, num_docs: i32) -> io::Result<()> {
            self.events.push(format!("done:{num_docs}"));
            Ok(())
        }
    }

    fn make_field_info(name: &str, number: u32) -> FieldInfo {
        use crate::document::{DocValuesType, IndexOptions};
        use crate::index::PointDimensionConfig;
        FieldInfo::new(
            name.to_string(),
            number,
            false,
            false,
            IndexOptions::None,
            DocValuesType::None,
            PointDimensionConfig::default(),
        )
    }

    #[test]
    fn test_sequential_documents() {
        let writer = MockWriter::default();
        let mut consumer = StoredFieldsConsumer::new(writer);

        consumer.start_document(0).unwrap();
        let fi = make_field_info("title", 0);
        consumer
            .write_field(&fi, &StoredValue::String("hello".to_string()))
            .unwrap();
        consumer.finish_document().unwrap();

        consumer.start_document(1).unwrap();
        consumer.finish_document().unwrap();

        assert_eq!(
            consumer.writer.events,
            vec!["start", "field:title", "finish", "start", "finish"]
        );
    }

    #[test]
    fn test_gap_filling() {
        let writer = MockWriter::default();
        let mut consumer = StoredFieldsConsumer::new(writer);

        // Skip docs 0, 1, 2 — go straight to doc 3
        consumer.start_document(3).unwrap();
        consumer.finish_document().unwrap();

        // Should have filled gaps: start/finish for docs 0, 1, 2, then start for doc 3
        assert_eq!(
            consumer.writer.events,
            vec![
                "start", "finish", // doc 0 (gap)
                "start", "finish", // doc 1 (gap)
                "start", "finish", // doc 2 (gap)
                "start", "finish", // doc 3 (actual)
            ]
        );
    }

    #[test]
    fn test_finish_fills_remaining() {
        let writer = MockWriter::default();
        let mut consumer = StoredFieldsConsumer::new(writer);

        consumer.start_document(0).unwrap();
        consumer.finish_document().unwrap();

        // Finish with max_doc=3, should fill docs 1, 2
        consumer.finish(3).unwrap();

        assert_eq!(
            consumer.writer.events,
            vec![
                "start", "finish", // doc 0
                "start", "finish", // doc 1 (fill)
                "start", "finish", // doc 2 (fill)
                "done:3",
            ]
        );
    }
}
