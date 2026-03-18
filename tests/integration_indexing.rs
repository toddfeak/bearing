//! Integration tests for the bearing indexing public API.
//!
//! These tests exercise the public API as an external consumer would,
//! verifying that IndexWriter, Document, Field types, and Directory
//! implementations work correctly together.

use std::io;
use std::thread;

use bearing::document::{
    Document, FieldValue, binary_doc_values_field, double_field, float_field, int_field,
    keyword_field, long_field, numeric_doc_values_field, sorted_doc_values_field,
    sorted_numeric_doc_values_field, sorted_set_doc_values_field, stored_bytes_field,
    stored_double_field, stored_float_field, stored_int_field, stored_long_field,
    stored_string_field, string_field, text_field,
};
use bearing::index::{IndexWriter, IndexWriterConfig};
use bearing::store::{Directory, FSDirectory, MemoryDirectory};

/// Helper: creates a document with path, modified, and contents fields.
fn make_simple_doc(path: &str, modified: i64, contents: &str) -> Document {
    let mut doc = Document::new();
    doc.add(text_field("body", contents));
    doc.add(keyword_field("id", path));
    doc.add(long_field("ts", modified));
    doc
}

/// Helper: creates a document with all 8 field types.
fn make_all_fields_doc() -> Document {
    let mut doc = Document::new();
    doc.add(text_field("body", "the quick brown fox"));
    doc.add(keyword_field("category", "animals"));
    doc.add(string_field("id", "doc-1", true));
    doc.add(long_field("timestamp", 1_700_000_000));
    doc.add(int_field("count", 42, true));
    doc.add(float_field("score", 3.125, true));
    doc.add(double_field("price", 99.99, true));
    doc.add(stored_string_field("author", "Todd"));
    doc
}

// ---------------------------------------------------------------------------
// Single-threaded indexing
// ---------------------------------------------------------------------------

#[test]
fn single_threaded_index_and_commit() -> io::Result<()> {
    let writer = IndexWriter::new();

    for i in 0..10 {
        let mut doc = Document::new();
        doc.add(text_field("body", &format!("document number {i}")));
        doc.add(keyword_field("id", &format!("doc-{i}")));
        writer.add_document(doc)?;
    }

    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), 10);

    let files = result.into_segment_files()?;
    assert!(!files.is_empty(), "commit should produce segment files");

    // Should have a segments_N file
    assert!(
        files.iter().any(|f| f.name.starts_with("segments_")),
        "should contain a segments file"
    );
    // Should have compound file entries (.cfs, .cfe)
    assert!(
        files.iter().any(|f| f.name.ends_with(".cfs")),
        "should contain .cfs compound file"
    );
    assert!(
        files.iter().any(|f| f.name.ends_with(".cfe")),
        "should contain .cfe compound file entry"
    );
    // Should have segment info (.si)
    assert!(
        files.iter().any(|f| f.name.ends_with(".si")),
        "should contain .si segment info"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Multi-threaded indexing
// ---------------------------------------------------------------------------

#[test]
fn multi_threaded_indexing() -> io::Result<()> {
    let writer = IndexWriter::new();
    let num_threads = 4;
    let docs_per_thread = 25;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let w = writer.clone();
            thread::spawn(move || {
                for i in 0..docs_per_thread {
                    let mut doc = Document::new();
                    doc.add(text_field("body", &format!("thread {t} doc {i}")));
                    doc.add(keyword_field("id", &format!("t{t}-d{i}")));
                    w.add_document(doc).expect("add_document failed");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), num_threads * docs_per_thread);

    let files = result.into_segment_files()?;
    assert!(
        files.iter().any(|f| f.name.starts_with("segments_")),
        "should contain a segments file"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// IndexWriterConfig
// ---------------------------------------------------------------------------

#[test]
fn config_max_buffered_docs() -> io::Result<()> {
    let config = IndexWriterConfig::new().set_max_buffered_docs(5);
    assert_eq!(config.max_buffered_docs(), 5);

    let writer = IndexWriter::with_config(config);

    // Add more than max_buffered_docs to trigger at least one flush
    for i in 0..20 {
        let mut doc = Document::new();
        doc.add(text_field("body", &format!("document {i}")));
        writer.add_document(doc)?;
    }

    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), 20);

    let files = result.into_segment_files()?;
    assert!(!files.is_empty());

    Ok(())
}

#[test]
fn config_ram_buffer_size() {
    let config = IndexWriterConfig::new().set_ram_buffer_size_mb(32.0);
    assert!((config.ram_buffer_size_mb() - 32.0).abs() < f64::EPSILON);
}

#[test]
fn config_defaults() {
    let config = IndexWriterConfig::new();
    assert!((config.ram_buffer_size_mb() - 16.0).abs() < f64::EPSILON);
    assert_eq!(config.max_buffered_docs(), -1);
}

// ---------------------------------------------------------------------------
// Empty commit
// ---------------------------------------------------------------------------

#[test]
fn empty_commit() -> io::Result<()> {
    let writer = IndexWriter::new();
    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), 0);

    let files = result.into_segment_files()?;
    // Even an empty commit should produce a segments file
    assert!(files.iter().any(|f| f.name.starts_with("segments_")));

    Ok(())
}

// ---------------------------------------------------------------------------
// All field factory functions
// ---------------------------------------------------------------------------

#[test]
fn all_field_types_commit_successfully() -> io::Result<()> {
    let writer = IndexWriter::new();
    let doc = make_all_fields_doc();
    writer.add_document(doc)?;

    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), 1);

    let files = result.into_segment_files()?;
    assert!(!files.is_empty());

    Ok(())
}

#[test]
fn stored_only_fields_commit_successfully() -> io::Result<()> {
    let writer = IndexWriter::new();

    let mut doc = Document::new();
    doc.add(stored_string_field("s", "hello"));
    doc.add(stored_int_field("i", 42));
    doc.add(stored_long_field("l", 123_456_789));
    doc.add(stored_float_field("f", 2.75));
    doc.add(stored_double_field("d", 3.125));
    doc.add(stored_bytes_field("b", vec![0xDE, 0xAD, 0xBE, 0xEF]));
    // Need at least one indexed field for the document to be valid
    doc.add(keyword_field("id", "stored-only"));
    writer.add_document(doc)?;

    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), 1);

    let files = result.into_segment_files()?;
    assert!(!files.is_empty());

    Ok(())
}

// ---------------------------------------------------------------------------
// Document & Field accessors
// ---------------------------------------------------------------------------

#[test]
fn field_accessors() {
    let f = text_field("body", "hello world");
    assert_eq!(f.name(), "body");
    assert_eq!(f.string_value(), Some("hello world"));
    assert_eq!(f.numeric_value(), None);

    let f = long_field("ts", 12345);
    assert_eq!(f.name(), "ts");
    assert_eq!(f.numeric_value(), Some(12345));

    let f = int_field("count", 7, false);
    assert_eq!(f.name(), "count");
    assert!(matches!(f.value(), FieldValue::Int(7)));

    let f = float_field("score", 1.5, false);
    assert_eq!(f.name(), "score");
    assert!(matches!(f.value(), FieldValue::Float(v) if (*v - 1.5).abs() < f32::EPSILON));

    let f = double_field("price", 99.99, false);
    assert_eq!(f.name(), "price");
    assert!(matches!(f.value(), FieldValue::Double(v) if (*v - 99.99).abs() < f64::EPSILON));

    let f = keyword_field("tag", "rust");
    assert_eq!(f.name(), "tag");
    assert_eq!(f.string_value(), Some("rust"));

    let f = string_field("id", "abc", true);
    assert_eq!(f.name(), "id");
    assert_eq!(f.string_value(), Some("abc"));
}

#[test]
fn document_construction() {
    let mut doc = Document::new();
    assert!(doc.fields.is_empty());

    doc.add(text_field("a", "hello"));
    doc.add(keyword_field("b", "world"));
    assert_eq!(doc.fields.len(), 2);
    assert_eq!(doc.fields[0].name(), "a");
    assert_eq!(doc.fields[1].name(), "b");
}

// ---------------------------------------------------------------------------
// In-memory round-trip via MemoryDirectory
// ---------------------------------------------------------------------------

#[test]
fn memory_directory_round_trip() -> io::Result<()> {
    let writer = IndexWriter::new();

    for i in 0..5 {
        let mut doc = Document::new();
        doc.add(text_field("body", &format!("memory test {i}")));
        doc.add(keyword_field("id", &format!("mem-{i}")));
        writer.add_document(doc)?;
    }

    let result = writer.commit()?;
    let mut dir = MemoryDirectory::new();
    let file_names = result.write_to_directory(&mut dir)?;

    assert!(!file_names.is_empty());
    assert!(file_names.iter().any(|n| n.starts_with("segments_")));

    // Verify all files are accessible through the directory
    let listed = dir.list_all()?;
    for name in &file_names {
        assert!(
            listed.contains(name),
            "file '{name}' written but not listed in directory"
        );
        let len = dir.file_length(name)?;
        assert!(len > 0, "file '{name}' should have non-zero length");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Segment file naming conventions
// ---------------------------------------------------------------------------

#[test]
fn segment_file_names_follow_lucene_conventions() -> io::Result<()> {
    let writer = IndexWriter::new();

    let mut doc = Document::new();
    doc.add(text_field("body", "naming test"));
    writer.add_document(doc)?;

    let result = writer.commit()?;
    let files = result.into_segment_files()?;

    for file in &files {
        let name = &file.name;
        // segments_N or _N.ext or _N_xxx.ext
        assert!(
            name.starts_with("segments_") || name.starts_with('_'),
            "unexpected file name pattern: {name}"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Filesystem round-trip via FSDirectory
// ---------------------------------------------------------------------------

#[test]
fn fs_directory_round_trip() -> io::Result<()> {
    let tmp_dir = std::env::temp_dir().join("bearing_integration_test_fs");
    // Clean up from any previous run
    if tmp_dir.exists() {
        std::fs::remove_dir_all(&tmp_dir)?;
    }

    let fs_dir = FSDirectory::open(&tmp_dir)?;
    let writer = IndexWriter::with_directory(Box::new(fs_dir));
    writer.add_document(make_simple_doc(
        "fs-1",
        1_000_000,
        "filesystem test document",
    ))?;
    let result = writer.commit()?;

    // Files are already on disk — verify via file_names()
    let file_names = result.file_names();
    assert!(!file_names.is_empty());
    assert!(file_names.iter().any(|n| n.starts_with("segments_")));

    for name in file_names {
        let path = tmp_dir.join(name);
        assert!(
            path.exists(),
            "file should exist on disk: {}",
            path.display()
        );
        let meta = std::fs::metadata(&path)?;
        assert!(meta.len() > 0, "file should be non-empty: {name}");
    }

    // Clean up
    std::fs::remove_dir_all(&tmp_dir)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Multiple segments via max_buffered_docs
// ---------------------------------------------------------------------------

#[test]
fn multiple_segments_via_flush() -> io::Result<()> {
    let config = IndexWriterConfig::new().set_max_buffered_docs(3);
    let writer = IndexWriter::with_config(config);

    // Add 10 docs with max_buffered_docs=3 -> should produce multiple segments
    for i in 0..10 {
        let mut doc = Document::new();
        doc.add(text_field("body", &format!("segment test doc {i}")));
        doc.add(keyword_field("id", &format!("seg-{i}")));
        writer.add_document(doc)?;
    }

    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), 10);

    let files = result.into_segment_files()?;

    // With max_buffered_docs=3 and 10 docs, we should have multiple .si files
    let si_count = files.iter().filter(|f| f.name.ends_with(".si")).count();
    assert!(
        si_count > 1,
        "expected multiple segments, got {si_count} .si files"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Mixed field types in single document
// ---------------------------------------------------------------------------

#[test]
fn all_field_types_in_single_document() -> io::Result<()> {
    let writer = IndexWriter::new();
    let doc = make_all_fields_doc();

    // Verify the document has the expected number of fields
    assert_eq!(doc.fields.len(), 8);

    writer.add_document(doc)?;
    let result = writer.commit()?;

    let mut dir = MemoryDirectory::new();
    let file_names = result.write_to_directory(&mut dir)?;

    // Should produce valid index files
    assert!(file_names.iter().any(|n| n.starts_with("segments_")));
    assert!(file_names.iter().any(|n| n.ends_with(".si")));
    assert!(file_names.iter().any(|n| n.ends_with(".cfs")));

    Ok(())
}

// ---------------------------------------------------------------------------
// Large batch indexing
// ---------------------------------------------------------------------------

#[test]
fn large_batch_indexing() -> io::Result<()> {
    let writer = IndexWriter::new();

    for i in 0..1000 {
        let mut doc = Document::new();
        doc.add(text_field(
            "body",
            &format!("bulk document number {i} with some extra text for variety"),
        ));
        doc.add(keyword_field(
            "category",
            if i % 2 == 0 { "even" } else { "odd" },
        ));
        doc.add(long_field("id", i));
        writer.add_document(doc)?;
    }

    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), 1000);

    let files = result.into_segment_files()?;
    assert!(!files.is_empty());

    Ok(())
}

// ---------------------------------------------------------------------------
// Non-compound file mode
// ---------------------------------------------------------------------------

#[test]
fn non_compound_mode() -> io::Result<()> {
    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    for i in 0..5 {
        writer.add_document(make_simple_doc(
            &format!("nc-{i}"),
            i as i64,
            &format!("non-compound test {i}"),
        ))?;
    }

    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), 5);

    let file_names = result.file_names().to_vec();

    // No compound files
    assert!(!file_names.iter().any(|n| n.ends_with(".cfs")));
    assert!(!file_names.iter().any(|n| n.ends_with(".cfe")));

    // Individual sub-files present
    assert!(file_names.iter().any(|n| n.ends_with(".fnm")));
    assert!(file_names.iter().any(|n| n.ends_with(".si")));

    // Can still read files via into_segment_files
    let files = result.into_segment_files()?;
    assert!(!files.is_empty());
    assert!(
        files.len() > 4,
        "non-compound should have more than 4 files"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// FSDirectory with non-compound mode
// ---------------------------------------------------------------------------

#[test]
fn fs_directory_non_compound() -> io::Result<()> {
    let tmp_dir = std::env::temp_dir().join("bearing_integration_test_fs_nc");
    if tmp_dir.exists() {
        std::fs::remove_dir_all(&tmp_dir)?;
    }

    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let fs_dir = FSDirectory::open(&tmp_dir)?;
    let writer = IndexWriter::with_config_and_directory(config, Box::new(fs_dir));

    writer.add_document(make_simple_doc("fs-nc-1", 100, "non-compound fs test"))?;
    let result = writer.commit()?;

    let file_names = result.file_names();
    assert!(!file_names.iter().any(|n| n.ends_with(".cfs")));
    assert!(file_names.iter().any(|n| n.ends_with(".fnm")));

    // Verify files on disk
    for name in file_names {
        let path = tmp_dir.join(name);
        assert!(path.exists(), "file should exist: {}", path.display());
    }

    std::fs::remove_dir_all(&tmp_dir)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Doc-values-only field types
// ---------------------------------------------------------------------------

#[test]
fn doc_values_only_fields() -> io::Result<()> {
    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    for i in 0..10 {
        let mut doc = Document::new();
        doc.add(text_field("body", &format!("doc values test {i}")));
        doc.add(numeric_doc_values_field("count", i * 10));
        doc.add(binary_doc_values_field(
            "hash",
            vec![(i as u8) * 11, (i as u8) * 22],
        ));
        doc.add(sorted_doc_values_field(
            "category",
            format!("cat-{}", i % 3).as_bytes(),
        ));
        doc.add(sorted_set_doc_values_field(
            "tag",
            &format!("tag-{}", i % 5),
        ));
        doc.add(sorted_numeric_doc_values_field("priority", (i % 4) as i64));
        writer.add_document(doc)?;
    }

    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), 10);

    let mut dir = MemoryDirectory::new();
    let file_names = result.write_to_directory(&mut dir)?;

    // Doc values files should exist
    assert!(
        file_names.iter().any(|n| n.ends_with(".dvm")),
        "expected .dvm file"
    );
    assert!(
        file_names.iter().any(|n| n.ends_with(".dvd")),
        "expected .dvd file"
    );

    // Verify files have content
    for name in &file_names {
        if name.ends_with(".dvm") || name.ends_with(".dvd") {
            let len = dir.file_length(name)?;
            assert!(len > 0, "{name} should have non-zero length");
        }
    }

    Ok(())
}

#[test]
fn doc_values_only_fields_non_compound() -> io::Result<()> {
    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    for i in 0..5 {
        let mut doc = Document::new();
        doc.add(text_field("body", &format!("non-compound dv test {i}")));
        doc.add(numeric_doc_values_field("count", i * 100));
        doc.add(binary_doc_values_field("payload", vec![i as u8; 4]));
        doc.add(sorted_doc_values_field("status", b"active"));
        doc.add(sorted_set_doc_values_field("region", "us-east"));
        doc.add(sorted_numeric_doc_values_field("score", i * 10));
        writer.add_document(doc)?;
    }

    let result = writer.commit()?;
    let file_names = result.file_names().to_vec();

    // Non-compound: .dvm and .dvd should be individual files
    assert!(file_names.iter().any(|n| n.ends_with(".dvm")));
    assert!(file_names.iter().any(|n| n.ends_with(".dvd")));

    Ok(())
}

#[test]
fn mixed_doc_values_and_regular_fields() -> io::Result<()> {
    let writer = IndexWriter::new();

    for i in 0..5 {
        let mut doc = Document::new();
        // Regular fields
        doc.add(keyword_field("id", &format!("doc-{i}")));
        doc.add(long_field("timestamp", 1_700_000_000 + i));
        doc.add(text_field("body", &format!("mixed field test {i}")));
        // Doc-values-only fields
        doc.add(numeric_doc_values_field("weight", i * 100));
        doc.add(sorted_doc_values_field("tier", b"gold"));
        doc.add(binary_doc_values_field("blob", vec![0xFF; 8]));
        writer.add_document(doc)?;
    }

    let result = writer.commit()?;
    assert_eq!(writer.num_docs(), 5);

    let files = result.into_segment_files()?;
    assert!(!files.is_empty());

    Ok(())
}
