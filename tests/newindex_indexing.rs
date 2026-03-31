// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the newindex indexing pipeline.
//!
//! Verifies the full lifecycle: IndexWriter → add documents → commit →
//! correct segments returned with expected file lists.

use std::collections::HashSet;
use std::fs;
use std::io::Cursor;
use std::path::Path;

use assertables::*;
use bearing::newindex::config::IndexWriterConfig;
use bearing::newindex::document::DocumentBuilder;
use bearing::newindex::field::{stored, string, text};
use bearing::newindex::writer::IndexWriter;
use bearing::store::MemoryDirectory;

fn add_stored_docs(writer: &IndexWriter, count: usize) {
    for i in 0..count {
        let doc = DocumentBuilder::new()
            .add_field(stored("title").string(format!("Document {i}")))
            .add_field(stored("body").string(format!("Body text for document {i}")))
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
            .add_field(stored("title").string(format!("Document {i}")))
            .add_field(stored("body").string(format!("Body text for document {i}")))
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
        .add_field(stored("title").string("hello"))
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

// --- Text field (tokenized + norms + postings) tests ---

fn add_text_docs_from_testdata(writer: &IndexWriter) {
    let docs_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/docs");
    for entry in fs::read_dir(&docs_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let contents = fs::read_to_string(&path).unwrap();
        let doc = DocumentBuilder::new()
            .add_field(stored("path").string(&name))
            .add_field(text("contents").stored().value(contents))
            .build();
        writer.add_document(doc).unwrap();
    }
}

#[test]
fn stored_tokenized_fields_produce_norms_and_postings_files() {
    let writer = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );

    add_text_docs_from_testdata(&writer);

    let segments = writer.commit().unwrap();
    assert_eq!(segments.len(), 1);

    let files = &segments[0].file_names;
    // Norms
    assert!(files.contains(&"_0.nvm".to_string()));
    assert!(files.contains(&"_0.nvd".to_string()));
    // Stored fields
    assert!(files.contains(&"_0.fdt".to_string()));
    assert!(files.contains(&"_0.fdx".to_string()));
    assert!(files.contains(&"_0.fdm".to_string()));
    // Field infos + segment info
    assert!(files.contains(&"_0.fnm".to_string()));
    assert!(files.contains(&"_0.si".to_string()));
    // Postings — per-field suffix must match PerFieldPostingsFormat attributes in .fnm
    assert!(files.contains(&"_0_Lucene103_0.tim".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.tip".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.tmd".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.doc".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.pos".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.psm".to_string()));
}

#[test]
fn stored_tokenized_fields_multi_segment() {
    let config = IndexWriterConfig {
        max_buffered_docs: 2,
        ..Default::default()
    };
    let writer = IndexWriter::new(config, Box::new(MemoryDirectory::new()));

    add_text_docs_from_testdata(&writer);

    let segments = writer.commit().unwrap();

    // 4 docs with max_buffered_docs=2 → 2 segments
    assert_eq!(segments.len(), 2);
    let total_docs: i32 = segments.iter().map(|s| s.doc_count).sum();
    assert_eq!(total_docs, 4);

    // Each segment has norms and postings files
    for seg in &segments {
        assert_any!(seg.file_names.iter(), |f: &String| f.ends_with(".nvm"));
        assert_any!(seg.file_names.iter(), |f: &String| f.ends_with(".nvd"));
        assert_any!(seg.file_names.iter(), |f: &String| f
            .ends_with("_Lucene103_0.tim"));
        assert_any!(seg.file_names.iter(), |f: &String| f
            .ends_with("_Lucene103_0.doc"));
    }
}

#[test]
fn text_only_fields_produce_postings_without_stored() {
    let writer = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );

    // Tokenized, not stored
    for i in 0..3 {
        let doc = DocumentBuilder::new()
            .add_field(text("body").reader(Cursor::new(
                format!("hello world document {i}").into_bytes(),
            )))
            .build();
        writer.add_document(doc).unwrap();
    }

    let segments = writer.commit().unwrap();
    assert_eq!(segments.len(), 1);

    let files = &segments[0].file_names;
    // Postings present
    assert!(files.contains(&"_0_Lucene103_0.tim".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.doc".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.pos".to_string()));
    // Norms present (TEXT type has norms)
    assert!(files.contains(&"_0.nvm".to_string()));
}

#[test]
fn mixed_stored_and_stored_tokenized_fields() {
    let writer = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );

    for i in 0..5 {
        let doc = DocumentBuilder::new()
            .add_field(stored("id").string(format!("{i}")))
            .add_field(text("body").stored().value(format!("quick brown fox {i}")))
            .build();
        writer.add_document(doc).unwrap();
    }

    let segments = writer.commit().unwrap();
    assert_eq!(segments.len(), 1);

    let files = &segments[0].file_names;
    // Both stored fields and postings
    assert!(files.contains(&"_0.fdt".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.tim".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.doc".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.pos".to_string()));
    assert!(files.contains(&"_0.nvm".to_string()));
}

#[test]
fn tokenized_field_produces_same_postings_as_string() {
    use std::io::BufReader;

    let docs_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/docs");

    // Index with tokenized_field (streaming)
    let writer_reader = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );
    for entry in fs::read_dir(&docs_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let file = fs::File::open(&path).unwrap();
        let doc = DocumentBuilder::new()
            .add_field(text("contents").reader(BufReader::new(file)))
            .build();
        writer_reader.add_document(doc).unwrap();
    }
    let segments_reader = writer_reader.commit().unwrap();

    // Index with stored_tokenized_field (string)
    let writer_string = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );
    for entry in fs::read_dir(&docs_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let contents = fs::read_to_string(&path).unwrap();
        let doc = DocumentBuilder::new()
            .add_field(text("contents").stored().value(contents))
            .build();
        writer_string.add_document(doc).unwrap();
    }
    let segments_string = writer_string.commit().unwrap();

    // Both should produce the same number of segments and docs
    assert_eq!(segments_reader.len(), segments_string.len());
    let total_reader: i32 = segments_reader.iter().map(|s| s.doc_count).sum();
    let total_string: i32 = segments_string.iter().map(|s| s.doc_count).sum();
    assert_eq!(total_reader, total_string);

    // Reader-based index should have postings and norms
    let files = &segments_reader[0].file_names;
    assert!(files.contains(&"_0_Lucene103_0.tim".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.doc".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.pos".to_string()));
    assert!(files.contains(&"_0.nvm".to_string()));
}

#[test]
fn reader_field_not_stored() {
    let writer = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );

    let doc = DocumentBuilder::new()
        .add_field(stored("title").string("test"))
        .add_field(text("contents").reader(Cursor::new(b"hello world document".to_vec())))
        .build();
    writer.add_document(doc).unwrap();

    let segments = writer.commit().unwrap();
    assert_eq!(segments.len(), 1);

    let files = &segments[0].file_names;
    // "title" is stored → .fdt exists
    assert!(files.contains(&"_0.fdt".to_string()));
    // "contents" via reader → postings exist
    assert!(files.contains(&"_0_Lucene103_0.tim".to_string()));
    // Norms exist for "contents"
    assert!(files.contains(&"_0.nvm".to_string()));
}

#[test]
fn string_field_produces_docs_only_postings() {
    let writer = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );

    for i in 0..3 {
        let doc = DocumentBuilder::new()
            .add_field(string("title").stored().value(format!("doc_{i}")))
            .build();
        writer.add_document(doc).unwrap();
    }

    let segments = writer.commit().unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].doc_count, 3);

    let files = &segments[0].file_names;
    // Postings files exist (DOCS-only terms)
    assert!(files.contains(&"_0_Lucene103_0.tim".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.doc".to_string()));
    // Stored fields exist
    assert!(files.contains(&"_0.fdt".to_string()));
    // No norms (StringField omits norms)
    assert!(!files.iter().any(|f| f.ends_with(".nvm")));
    // No positions file for DOCS-only fields
    assert!(!files.iter().any(|f| f.ends_with(".pos")));
}

#[test]
fn mixed_string_and_text_fields() {
    let writer = IndexWriter::new(
        IndexWriterConfig::default(),
        Box::new(MemoryDirectory::new()),
    );

    for i in 0..3 {
        let doc = DocumentBuilder::new()
            .add_field(string("title").stored().value(format!("doc_{i}")))
            .add_field(text("body").stored().value(format!("quick brown fox {i}")))
            .build();
        writer.add_document(doc).unwrap();
    }

    let segments = writer.commit().unwrap();
    assert_eq!(segments.len(), 1);

    let files = &segments[0].file_names;
    // Both fields have postings
    assert!(files.contains(&"_0_Lucene103_0.tim".to_string()));
    assert!(files.contains(&"_0_Lucene103_0.doc".to_string()));
    // Positions exist (from tokenized "body" field)
    assert!(files.contains(&"_0_Lucene103_0.pos".to_string()));
    // Norms exist (from tokenized "body" field)
    assert!(files.contains(&"_0.nvm".to_string()));
    // Stored fields exist
    assert!(files.contains(&"_0.fdt".to_string()));
}
