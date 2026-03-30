// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the newindex indexing pipeline.
//!
//! Verifies the full lifecycle: IndexWriter → add documents → commit →
//! correct segments returned with expected file lists.

use std::collections::HashSet;

use assertables::*;
use bearing::newindex::config::IndexWriterConfig;
use bearing::newindex::document::DocumentBuilder;
use bearing::newindex::field::{FieldBuilder, FieldType};
use bearing::newindex::writer::IndexWriter;
use bearing::store::MemoryDirectory;

fn stored_type() -> FieldType {
    FieldType { stored: true }
}

fn add_stored_docs(writer: &IndexWriter, count: usize) {
    for i in 0..count {
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
    assert_any!(files.iter(), |f: &String| f.ends_with(".fdt"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".fdx"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".fdm"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".fnm"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".si"));
}

#[test]
fn empty_commit_produces_no_segments() {
    let writer = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );
    let segments = writer.commit().unwrap();
    assert_is_empty!(segments);
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
        assert_starts_with!(file_name, seg.segment_id.name.as_str());
    }
}

#[test]
fn max_buffered_docs_creates_multiple_segments() {
    let config = IndexWriterConfig {
        max_buffered_docs: 5,
        ..Default::default()
    };
    let writer = IndexWriter::new(config, Box::new(MemoryDirectory::new()));

    add_stored_docs(&writer, 12);

    let segments = writer.commit().unwrap();

    // 12 docs with max_buffered_docs=5 → 3 segments (5 + 5 + 2)
    assert_eq!(segments.len(), 3);
    let total_docs: i32 = segments.iter().map(|s| s.doc_count).sum();
    assert_eq!(total_docs, 12);
    assert_eq!(segments[0].doc_count, 5);
    assert_eq!(segments[1].doc_count, 5);
    assert_eq!(segments[2].doc_count, 2);

    // All segment names must be unique
    let names: HashSet<_> = segments.iter().map(|s| &s.segment_id.name).collect();
    assert_eq!(names.len(), 3);

    // Each segment has stored field files
    for seg in &segments {
        assert_any!(seg.file_names.iter(), |f: &String| f.ends_with(".fdt"));
        assert_any!(seg.file_names.iter(), |f: &String| f.ends_with(".si"));
        assert_any!(seg.file_names.iter(), |f: &String| f.ends_with(".fnm"));
    }
}

#[test]
fn multi_thread_produces_independent_segments() {
    let config = IndexWriterConfig {
        num_threads: 2,
        max_buffered_docs: 1,
        ..Default::default()
    };
    let writer = IndexWriter::new(config, Box::new(MemoryDirectory::new()));

    // With max_buffered_docs=1, every doc triggers a flush. The flush
    // I/O gives the other thread time to pick up work from the channel.
    // 100 docs is enough that both threads will participate.
    add_stored_docs(&writer, 100);

    let segments = writer.commit().unwrap();

    // 100 docs with max_buffered_docs=1 → 100 segments across 2 threads
    assert_eq!(segments.len(), 100);
    let total_docs: i32 = segments.iter().map(|s| s.doc_count).sum();
    assert_eq!(total_docs, 100);

    // Every segment has exactly 1 doc
    for seg in &segments {
        assert_eq!(seg.doc_count, 1);
    }

    // All segment names unique
    let names: HashSet<_> = segments.iter().map(|s| &s.segment_id.name).collect();
    assert_eq!(names.len(), 100);
}

#[test]
fn multi_thread_with_mid_flush() {
    let config = IndexWriterConfig {
        num_threads: 2,
        max_buffered_docs: 3,
        ..Default::default()
    };
    let writer = IndexWriter::new(config, Box::new(MemoryDirectory::new()));

    add_stored_docs(&writer, 10);

    let segments = writer.commit().unwrap();

    // Multiple segments from flush + final flush across 2 threads
    assert_ge!(segments.len(), 3);
    let total_docs: i32 = segments.iter().map(|s| s.doc_count).sum();
    assert_eq!(total_docs, 10);

    // All segment names unique
    let names: HashSet<_> = segments.iter().map(|s| &s.segment_id.name).collect();
    assert_eq!(names.len(), segments.len());
}

#[test]
fn compound_file_packaging() {
    let config = IndexWriterConfig {
        use_compound_file: true,
        ..Default::default()
    };
    let dir = Box::new(MemoryDirectory::new());
    let writer = IndexWriter::new(config, dir);

    add_stored_docs(&writer, 5);

    let segments = writer.commit().unwrap();

    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].doc_count, 5);

    // Compound packaging replaces sub-files with .cfs/.cfe
    let files = &segments[0].file_names;
    assert_eq!(files.len(), 3);
    assert_any!(files.iter(), |f: &String| f.ends_with(".si"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".cfs"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".cfe"));
}

#[test]
fn non_compound_keeps_individual_files() {
    let config = IndexWriterConfig {
        use_compound_file: false,
        ..Default::default()
    };
    let writer = IndexWriter::new(config, Box::new(MemoryDirectory::new()));

    add_stored_docs(&writer, 5);

    let segments = writer.commit().unwrap();

    let files = &segments[0].file_names;
    // Individual stored field files + .fnm + .si
    assert_ge!(files.len(), 5);
    assert_any!(files.iter(), |f: &String| f.ends_with(".fdt"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".fdx"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".fdm"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".fnm"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".si"));
    // No compound files
    assert!(!files.iter().any(|f| f.ends_with(".cfs")));
    assert!(!files.iter().any(|f| f.ends_with(".cfe")));
}

#[test]
fn compound_with_multi_segment() {
    let config = IndexWriterConfig {
        use_compound_file: true,
        max_buffered_docs: 5,
        ..Default::default()
    };
    let writer = IndexWriter::new(config, Box::new(MemoryDirectory::new()));

    add_stored_docs(&writer, 12);

    let segments = writer.commit().unwrap();

    assert_eq!(segments.len(), 3);
    let total_docs: i32 = segments.iter().map(|s| s.doc_count).sum();
    assert_eq!(total_docs, 12);

    // Every segment should be compound
    for seg in &segments {
        assert_eq!(seg.file_names.len(), 3);
        assert_any!(seg.file_names.iter(), |f: &String| f.ends_with(".cfs"));
        assert_any!(seg.file_names.iter(), |f: &String| f.ends_with(".cfe"));
        assert_any!(seg.file_names.iter(), |f: &String| f.ends_with(".si"));
    }
}
