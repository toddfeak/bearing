// SPDX-License-Identifier: Apache-2.0
//! Integration test for term vectors.
//!
//! When RUST_TV_INDEX_DIR is set, writes the index to that directory for
//! cross-validation with the Java IndexTermVectors utility.

#[macro_use]
extern crate assertables;

use std::io;

use bearing::document::{Document, string_field, text_field, text_field_with_term_vectors};
use bearing::index::{IndexWriter, IndexWriterConfig};
use bearing::store::FSDirectory;

/// Creates 10 documents with deterministic content matching the Java
/// IndexTermVectors program.
fn make_term_vector_docs() -> Vec<Document> {
    (0..10)
        .map(|i| {
            let mut doc = Document::new();
            let doc_id = format!("doc-{i:03}");
            doc.add(string_field("id", &doc_id, true));
            doc.add(text_field("body", &format!("doc values test {i}")));
            doc.add(text_field_with_term_vectors(
                "contents",
                &format!("the quick brown fox jumps over the lazy dog number {i}"),
            ));
            doc
        })
        .collect()
}

#[test]
fn write_term_vectors_index() -> io::Result<()> {
    let index_dir = match std::env::var("RUST_TV_INDEX_DIR") {
        Ok(dir) => dir,
        Err(_) => return Ok(()), // Skip if env var not set
    };

    let path = std::path::Path::new(&index_dir);
    if path.exists() {
        std::fs::remove_dir_all(path)?;
    }
    std::fs::create_dir_all(path)?;

    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let fs_dir = FSDirectory::open(path)?;
    let writer = IndexWriter::with_config_and_directory(config, Box::new(fs_dir));

    for doc in make_term_vector_docs() {
        writer.add_document(doc)?;
    }

    let result = writer.commit()?;
    let file_names = result.file_names();

    // Verify term vector files exist
    assert!(
        file_names.iter().any(|n| n.ends_with(".tvd")),
        "expected .tvd file in {file_names:?}"
    );
    assert!(
        file_names.iter().any(|n| n.ends_with(".tvx")),
        "expected .tvx file in {file_names:?}"
    );
    assert!(
        file_names.iter().any(|n| n.ends_with(".tvm")),
        "expected .tvm file in {file_names:?}"
    );

    // Verify files on disk are non-empty
    for name in file_names {
        let file_path = path.join(name);
        assert!(
            file_path.exists(),
            "file should exist: {}",
            file_path.display()
        );
        let meta = std::fs::metadata(&file_path)?;
        assert_gt!(meta.len(), 0, "file should be non-empty: {name}");
    }

    Ok(())
}
