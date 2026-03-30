// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the newindex indexing pipeline.
//!
//! Verifies the full lifecycle: IndexWriter → add documents → commit →
//! correct segments returned with expected file lists.

use bearing::newindex::config::IndexWriterConfig;
use bearing::newindex::document::DocumentBuilder;
use bearing::newindex::field::{FieldBuilder, FieldType};
use bearing::newindex::writer::IndexWriter;
use bearing::store::MemoryDirectory;

fn stored_type() -> FieldType {
    FieldType { stored: true }
}

#[test]
fn single_segment_stored_fields() {
    let writer = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );

    for i in 0..5 {
        let doc = DocumentBuilder::new()
            .add_field(
                FieldBuilder::new("title")
                    .field_type(stored_type())
                    .string_value(format!("Document {i}"))
                    .build(),
            )
            .add_field(
                FieldBuilder::new("body")
                    .field_type(stored_type())
                    .string_value(format!("Body text for document {i}"))
                    .build(),
            )
            .build();
        writer.add_document(doc).unwrap();
    }

    let segments = writer.commit().unwrap();

    // Single-threaded config produces one segment
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].doc_count, 5);

    // Verify expected files are in the segment's file list
    let files = &segments[0].file_names;
    assert!(files.iter().any(|f| f.ends_with(".fdt")));
    assert!(files.iter().any(|f| f.ends_with(".fdx")));
    assert!(files.iter().any(|f| f.ends_with(".fdm")));
    assert!(files.iter().any(|f| f.ends_with(".fnm")));
    assert!(files.iter().any(|f| f.ends_with(".si")));
}

#[test]
fn empty_commit_produces_no_segments() {
    let writer = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );
    let segments = writer.commit().unwrap();
    assert!(segments.is_empty());
}

#[test]
fn segment_file_names_use_segment_prefix() {
    let writer = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );

    let doc = DocumentBuilder::new()
        .add_field(
            FieldBuilder::new("title")
                .field_type(stored_type())
                .string_value("hello")
                .build(),
        )
        .build();
    writer.add_document(doc).unwrap();

    let segments = writer.commit().unwrap();
    let seg = &segments[0];

    for file_name in &seg.file_names {
        assert!(
            file_name.starts_with(&seg.segment_id.name),
            "file {file_name} should start with segment name {}",
            seg.segment_id.name
        );
    }
}
