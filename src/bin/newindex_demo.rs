// SPDX-License-Identifier: Apache-2.0

//! Writes a stored-fields-only index via the newindex pipeline.
//!
//! Used by e2e tests to produce an on-disk index for Java CheckIndex validation.

use std::path::PathBuf;

use bearing::newindex::config::IndexWriterConfig;
use bearing::newindex::document::DocumentBuilder;
use bearing::newindex::field::stored_field;
use bearing::newindex::writer::IndexWriter;
use bearing::store::FSDirectory;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut index_path = None;
    let mut doc_count: usize = 10;
    let mut max_buffered_docs: i32 = -1;
    let mut num_threads: usize = 1;
    let mut use_compound_file = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--doc-count" => {
                i += 1;
                doc_count = args[i].parse().expect("invalid --doc-count");
            }
            "--max-buffered-docs" => {
                i += 1;
                max_buffered_docs = args[i].parse().expect("invalid --max-buffered-docs");
            }
            "--threads" => {
                i += 1;
                num_threads = args[i].parse().expect("invalid --threads");
            }
            "--compound" => {
                use_compound_file = true;
            }
            arg if !arg.starts_with('-') && index_path.is_none() => {
                index_path = Some(PathBuf::from(arg));
            }
            _ => {
                eprintln!(
                    "Usage: newindex_demo <index-dir> [--doc-count N] [--max-buffered-docs N] [--threads N]"
                );
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let index_path = index_path.unwrap_or_else(|| {
        eprintln!(
            "Usage: newindex_demo <index-dir> [--doc-count N] [--max-buffered-docs N] [--threads N]"
        );
        std::process::exit(1);
    });

    let config = IndexWriterConfig {
        num_threads,
        max_buffered_docs,
        use_compound_file,
        ..Default::default()
    };

    let fs_dir = FSDirectory::open_with_file_handles(&index_path).unwrap();
    let writer = IndexWriter::new(config, Box::new(fs_dir));

    for i in 0..doc_count {
        let doc = DocumentBuilder::new()
            .add_field(stored_field("title", format!("Document {i}")))
            .add_field(stored_field(
                "body",
                format!("This is the body text for document number {i}."),
            ))
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
