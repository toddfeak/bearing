// SPDX-License-Identifier: Apache-2.0
//! Integration test for doc-values-only field types.
//!
//! When RUST_DV_INDEX_DIR is set, writes the index to that directory for
//! cross-validation with the Java IndexDocValues utility.

#[macro_use]
extern crate assertables;

use std::io;

use bearing::document::{
    Document, binary_doc_values_field, numeric_doc_values_field, sorted_doc_values_field,
    sorted_numeric_doc_values_field, sorted_set_doc_values_field, text_field,
};
use bearing::index::{IndexWriter, IndexWriterConfig};
use bearing::store::FSDirectory;

/// Creates documents matching the Java IndexDocValues program.
fn make_doc_values_docs() -> Vec<Document> {
    (0..10)
        .map(|i| {
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
            doc
        })
        .collect()
}

#[test]
fn write_doc_values_index() -> io::Result<()> {
    let index_dir = match std::env::var("RUST_DV_INDEX_DIR") {
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

    for doc in make_doc_values_docs() {
        writer.add_document(doc)?;
    }

    let result = writer.commit()?;
    let file_names = result.file_names();

    // Verify doc values files exist
    assert!(
        file_names.iter().any(|n| n.ends_with(".dvm")),
        "expected .dvm file in {file_names:?}"
    );
    assert!(
        file_names.iter().any(|n| n.ends_with(".dvd")),
        "expected .dvd file in {file_names:?}"
    );

    // Verify files on disk
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
