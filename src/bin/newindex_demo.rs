// SPDX-License-Identifier: Apache-2.0

//! Writes a stored-fields-only index via the newindex pipeline.
//!
//! Used by e2e tests to produce an on-disk index for Java CheckIndex validation.

use std::path::PathBuf;

use bearing::newindex::config::IndexWriterConfig;
use bearing::newindex::document::DocumentBuilder;
use bearing::newindex::field::{FieldBuilder, FieldType};
use bearing::newindex::writer::IndexWriter;
use bearing::store::FSDirectory;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: newindex_demo <index-dir>");
        std::process::exit(1);
    }
    let index_path = PathBuf::from(&args[1]);

    let fs_dir = FSDirectory::open_with_file_handles(&index_path).unwrap();
    let writer = IndexWriter::new(IndexWriterConfig::default(), Box::new(fs_dir));

    let stored_type = FieldType { stored: true };

    for i in 0..10 {
        let doc = DocumentBuilder::new()
            .add_field(
                FieldBuilder::new("title")
                    .field_type(stored_type.clone())
                    .string_value(format!("Document {i}"))
                    .build(),
            )
            .add_field(
                FieldBuilder::new("body")
                    .field_type(stored_type.clone())
                    .string_value(format!("This is the body text for document number {i}."))
                    .build(),
            )
            .build();
        writer.add_document(doc).unwrap();
    }

    let segments = writer.commit().unwrap();
    println!(
        "Wrote {} segment(s) with {} total docs to {}",
        segments.len(),
        segments.iter().map(|s| s.doc_count).sum::<i32>(),
        index_path.display()
    );
}
