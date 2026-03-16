// SPDX-License-Identifier: Apache-2.0

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::{Instant, UNIX_EPOCH};

use log::{error, warn};

use bearing::document;
use bearing::document::Document;
use bearing::index::{IndexWriter, IndexWriterConfig};
use bearing::store::{Directory, FSDirectory};

struct CliArgs {
    index_path: String,
    docs_path: String,
    max_buffered_docs: i32,
    ram_buffer_size_mb: f64,
    num_threads: usize,
}

fn parse_args() -> CliArgs {
    let args: Vec<String> = env::args().collect();

    let mut index_path = String::from("index");
    let mut docs_path: Option<String> = None;
    let mut max_buffered_docs: i32 = -1;
    let mut ram_buffer_size_mb: f64 = -1.0; // -1.0 = use default (16 MB)
    let mut num_threads: usize = 1;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-index" => {
                i += 1;
                if i >= args.len() {
                    error!("-index requires a value");
                    process::exit(1);
                }
                index_path = args[i].clone();
            }
            "-docs" => {
                i += 1;
                if i >= args.len() {
                    error!("-docs requires a value");
                    process::exit(1);
                }
                docs_path = Some(args[i].clone());
            }
            "--max-buffered-docs" => {
                i += 1;
                if i >= args.len() {
                    error!("--max-buffered-docs requires a value");
                    process::exit(1);
                }
                max_buffered_docs = match args[i].parse() {
                    Ok(n) => n,
                    Err(_) => {
                        error!("--max-buffered-docs must be an integer");
                        process::exit(1);
                    }
                };
            }
            "--ram-buffer-size" => {
                i += 1;
                if i >= args.len() {
                    error!("--ram-buffer-size requires a value");
                    process::exit(1);
                }
                ram_buffer_size_mb = match args[i].parse() {
                    Ok(n) => n,
                    Err(_) => {
                        error!("--ram-buffer-size must be a number (MB)");
                        process::exit(1);
                    }
                };
            }
            "--threads" => {
                i += 1;
                if i >= args.len() {
                    error!("--threads requires a value");
                    process::exit(1);
                }
                num_threads = match args[i].parse() {
                    Ok(n) if n >= 1 => n,
                    _ => {
                        error!("--threads must be a positive integer");
                        process::exit(1);
                    }
                };
            }
            other => {
                error!("Unknown parameter: {other}");
                print_usage();
                process::exit(1);
            }
        }
        i += 1;
    }

    let docs_path = match docs_path {
        Some(p) => p,
        None => {
            print_usage();
            process::exit(1);
        }
    };

    CliArgs {
        index_path,
        docs_path,
        max_buffered_docs,
        ram_buffer_size_mb,
        num_threads,
    }
}

fn main() {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Warn)
        .env()
        .init()
        .unwrap();

    let args = parse_args();

    let doc_dir = PathBuf::from(&args.docs_path);
    if !doc_dir.is_dir() {
        error!(
            "Document directory '{}' does not exist or is not readable",
            doc_dir.display()
        );
        process::exit(1);
    }

    // Remove any existing index files in the output directory
    let index_dir = Path::new(&args.index_path);
    if index_dir.is_dir()
        && let Err(e) = remove_index_files(index_dir)
    {
        error!("Error cleaning index directory '{}': {e}", args.index_path);
        process::exit(1);
    }

    println!(
        "Creating index in '{}' from documents in '{}'...",
        args.index_path, args.docs_path
    );
    if args.num_threads > 1 {
        println!("Using {} threads", args.num_threads);
    }

    let start = Instant::now();

    // Collect document paths
    let mut doc_paths = Vec::new();
    walk_docs(&doc_dir, &mut doc_paths);

    // Create IndexWriter — default config uses 16 MB RAM buffer (matching Java Lucene)
    let mut config = IndexWriterConfig::new();
    if args.max_buffered_docs > 0 {
        config = config.set_max_buffered_docs(args.max_buffered_docs);
    }
    if args.ram_buffer_size_mb >= 0.0 {
        config = config.set_ram_buffer_size_mb(args.ram_buffer_size_mb);
    }
    let writer = IndexWriter::with_config(config);

    if args.num_threads <= 1 {
        // Single-threaded indexing
        for path in &doc_paths {
            let doc = make_document(path);
            if let Err(e) = writer.add_document(doc) {
                error!("Error indexing '{}': {e}", path.display());
                process::exit(1);
            }
            println!("  indexed: {}", path.display());
        }
    } else {
        // Multi-threaded indexing
        let chunk_size = doc_paths.len().div_ceil(args.num_threads);
        thread::scope(|s| {
            for chunk in doc_paths.chunks(chunk_size) {
                let w = writer.clone();
                s.spawn(move || {
                    for path in chunk {
                        let doc = make_document(path);
                        if let Err(e) = w.add_document(doc) {
                            error!("Error indexing '{}': {e}", path.display());
                        }
                    }
                });
            }
        });
        for path in &doc_paths {
            println!("  indexed: {}", path.display());
        }
    }

    // Commit and write index files to disk
    let commit = match writer.commit() {
        Ok(c) => c,
        Err(e) => {
            error!("Error committing index: {e}");
            process::exit(1);
        }
    };

    let elapsed = start.elapsed();

    println!();
    println!("Indexed {} documents in {elapsed:.2?}", writer.num_docs());

    let mut fs_dir = match FSDirectory::open(Path::new(&args.index_path)) {
        Ok(d) => d,
        Err(e) => {
            error!("Error opening index directory '{}': {e}", args.index_path);
            process::exit(1);
        }
    };

    let written_files = match commit.write_to_directory(&mut fs_dir) {
        Ok(files) => files,
        Err(e) => {
            error!("Error writing index to '{}': {e}", args.index_path);
            process::exit(1);
        }
    };

    println!("Produced {} index files:", written_files.len());
    for name in &written_files {
        match fs_dir.file_length(name) {
            Ok(len) => println!("  {name}: {len} bytes"),
            Err(_) => println!("  {name}"),
        }
    }

    println!("\nIndex written to '{}'", args.index_path);
}

/// Creates a Document from a file path, matching Java's IndexAllFields:
/// - "path": KeywordField (stored)
/// - "modified": LongField (milliseconds since epoch)
/// - "contents": TextField (tokenized, not stored)
/// - "title": StringField (stored) — filename without extension
/// - "size": IntField (stored) — file size in bytes
/// - "score": FloatField (stored) — (file_size % 100) / 10.0
/// - "rating": DoubleField (stored) — file_size * 1.5
/// - "notes": StoredField(String) — "indexed by Rust"
/// - "extra_int": StoredField(int) — file_size % 1000
/// - "extra_float": StoredField(float) — (file_size % 100) / 3.0
/// - "extra_double": StoredField(double) — file_size * 0.123
fn make_document(path: &Path) -> Document {
    let mut doc = Document::new();

    let path_str = path.to_string_lossy();
    let metadata = fs::metadata(path).ok();
    let file_size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);

    // "path" field — the file path as a keyword
    doc.add(document::keyword_field("path", &path_str));

    // "modified" field — last modified time in milliseconds since epoch
    let modified = metadata
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    doc.add(document::long_field("modified", modified));

    // "contents" field — the file text
    let contents = fs::read_to_string(path).unwrap_or_default();
    doc.add(document::text_field("contents", &contents));

    // "title" field — filename without extension
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let title = match file_name.rfind('.') {
        Some(dot) => &file_name[..dot],
        None => &file_name,
    };
    doc.add(document::string_field("title", title, true));

    // "size" field — file size in bytes
    doc.add(document::int_field("size", file_size as i32, true));

    // "score" field — (file_size % 100) / 10.0
    doc.add(document::float_field(
        "score",
        (file_size % 100) as f32 / 10.0,
        true,
    ));

    // "rating" field — file_size * 1.5
    doc.add(document::double_field(
        "rating",
        file_size as f64 * 1.5,
        true,
    ));

    // Stored-only fields
    doc.add(document::stored_string_field("notes", "indexed by Rust"));
    // extra_int = file_size % 1000
    doc.add(document::stored_int_field(
        "extra_int",
        (file_size % 1000) as i32,
    ));
    // extra_float = (file_size % 100) / 3.0
    doc.add(document::stored_float_field(
        "extra_float",
        (file_size % 100) as f32 / 3.0,
    ));
    // extra_double = file_size * 0.123
    doc.add(document::stored_double_field(
        "extra_double",
        file_size as f64 * 0.123,
    ));

    doc
}

/// Recursively collects file paths from a directory.
fn walk_docs(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            warn!("Error reading directory '{}': {e}", dir.display());
            return;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                warn!("Error reading entry: {e}");
                continue;
            }
        };

        let path = entry.path();
        if path.is_dir() {
            walk_docs(&path, out);
        } else {
            out.push(path);
        }
    }
}

/// Removes known Lucene index files from a directory.
/// Only removes files that look like index files (segments_*, _*.si, _*.cfs, _*.cfe, etc.).
fn remove_index_files(dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        if is_index_file(name) {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Returns true if the filename looks like a Lucene index file.
fn is_index_file(name: &str) -> bool {
    // segments_N, pending_segments_N
    if name.starts_with("segments_") || name.starts_with("pending_segments_") {
        return true;
    }
    // Per-segment files: _0.si, _0.cfs, _0.cfe, _0.fnm, _0.fdt, etc.
    if name.starts_with('_') && name.contains('.') {
        return true;
    }
    false
}

fn print_usage() {
    eprintln!(
        "Usage: indexfiles [-index INDEX_PATH] -docs DOCS_PATH [--max-buffered-docs N] [--ram-buffer-size MB] [--threads N]\n\n\
         Indexes the documents in DOCS_PATH, creating a Lucene index\n\
         in INDEX_PATH that can be searched with SearchFiles.\n\
         Any existing index files in INDEX_PATH are removed first.\n\n\
         Options:\n\
         \t--max-buffered-docs N  Flush after N docs per segment (-1 = disabled)\n\
         \t--ram-buffer-size MB   RAM buffer size in MB (default: 16.0)\n\
         \t--threads N            Number of indexing threads (default: 1)"
    );
}
