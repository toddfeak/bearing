// SPDX-License-Identifier: Apache-2.0

//! Integration tests for store-level read functionality (DataInput, IndexInput, Directory::open_input).

#[macro_use]
extern crate assertables;

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;

use bearing::encoding::read_encoding::ReadEncoding;
use bearing::store::{CompoundDirectory, Directory, FSDirectory, MemoryDirectory, SharedDirectory};

#[test]
fn test_memory_directory_write_then_read() {
    let mut dir = MemoryDirectory::new();

    // Write via create_output
    {
        let mut out = dir.create_output("test.bin").unwrap();
        out.write_le_int(0x04030201).unwrap();
        out.write_string("hello").unwrap();
        out.write_vint(42).unwrap();
        out.write_be_long(0x0807060504030201).unwrap();
        out.write_zint(-100).unwrap();
    }

    // Read via open_input
    let mut input = dir.open_input("test.bin").unwrap();
    assert_eq!(input.read_le_int().unwrap(), 0x04030201);
    assert_eq!(input.read_string().unwrap(), "hello");
    assert_eq!(input.read_vint().unwrap(), 42);
    assert_eq!(input.read_be_long().unwrap(), 0x0807060504030201);
    assert_eq!(input.read_zint().unwrap(), -100);
}

#[test]
fn test_fs_directory_write_then_read() {
    let dir_path = temp_dir("integration_store_input");
    let mut dir = FSDirectory::open(&dir_path).unwrap();
    let _cleanup = DirCleanup(&dir_path);

    {
        let mut out = dir.create_output("test.bin").unwrap();
        out.write_le_int(0x04030201).unwrap();
        out.write_string("world").unwrap();
        out.write_vlong(123456789).unwrap();
    }

    let mut input = dir.open_input("test.bin").unwrap();
    assert_eq!(input.read_le_int().unwrap(), 0x04030201);
    assert_eq!(input.read_string().unwrap(), "world");
    assert_eq!(input.read_vlong().unwrap(), 123456789);
}

#[test]
fn test_open_input_seek_and_reread() {
    let mut dir = MemoryDirectory::new();

    {
        let mut out = dir.create_output("seek.bin").unwrap();
        out.write_bytes(&[10, 20, 30, 40, 50]).unwrap();
    }

    let mut input = dir.open_input("seek.bin").unwrap();
    assert_eq!(input.length(), 5);

    // Read first two bytes
    assert_eq!(input.read_byte().unwrap(), 10);
    assert_eq!(input.read_byte().unwrap(), 20);
    assert_eq!(input.file_pointer(), 2);

    // Seek back to start and re-read
    input.seek(0).unwrap();
    assert_eq!(input.read_byte().unwrap(), 10);

    // Seek to end-1
    input.seek(4).unwrap();
    assert_eq!(input.read_byte().unwrap(), 50);
}

#[test]
fn test_open_input_skip_bytes() {
    let mut dir = MemoryDirectory::new();

    {
        let mut out = dir.create_output("skip.bin").unwrap();
        out.write_bytes(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
    }

    let mut input = dir.open_input("skip.bin").unwrap();
    input.skip_bytes(5).unwrap();
    assert_eq!(input.file_pointer(), 5);
    assert_eq!(input.read_byte().unwrap(), 6);
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = env::temp_dir().join(format!("bearing_test_{name}_{}", process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

struct DirCleanup<'a>(&'a Path);

impl Drop for DirCleanup<'_> {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(self.0);
    }
}

#[test]
fn test_index_writer_files_readable() {
    use bearing::document::DocumentBuilder;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::{keyword, text};
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default();
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    let doc = DocumentBuilder::new()
        .add_field(text("body").value("the quick brown fox"))
        .add_field(keyword("category").value("animals"))
        .build();
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();

    // Every file should be openable and have non-zero length
    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    assert_not_empty!(files);

    for file in &files {
        let input = dir.open_input(file).unwrap();
        assert_gt!(input.length(), 0, "file {file} has zero length");
    }
}

#[test]
fn test_index_writer_codec_files_have_valid_headers() {
    use bearing::document::DocumentBuilder;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::{keyword, text};
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default();
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    let doc = DocumentBuilder::new()
        .add_field(text("body").value("the quick brown fox"))
        .add_field(keyword("category").value("animals"))
        .build();
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();

    // Codec files (not segments_N) start with CODEC_MAGIC (0x3FD76C17 BE)
    let codec_magic_bytes: [u8; 4] = [0x3F, 0xD7, 0x6C, 0x17];
    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();

    for file in &files {
        if file.starts_with("segments") {
            continue; // segments_N has its own format
        }

        let mut input = dir.open_input(file).unwrap();
        let mut header = [0u8; 4];
        input.read_exact(&mut header).unwrap();
        assert_eq!(
            header, codec_magic_bytes,
            "file {file} does not start with CODEC_MAGIC"
        );

        // Read past magic, read codec name string
        let codec_name = input.read_string().unwrap();
        assert_not_empty!(codec_name, "file {file} has empty codec name");
    }
}

#[test]
fn test_read_segments_from_index_writer() {
    use bearing::document::DocumentBuilder;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::{keyword, text};
    use bearing::index::segment_infos;
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default();
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    let doc = DocumentBuilder::new()
        .add_field(text("body").value("the quick brown fox"))
        .add_field(keyword("category").value("animals"))
        .build();
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();

    // Find the segments_N file
    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();

    // Read segments_N
    let infos = segment_infos::read(&**dir, segments_file).unwrap();

    // Should have exactly 1 segment
    assert_eq!(infos.segments.len(), 1);
    assert_eq!(infos.segments[0].name, "_0");
    assert_eq!(infos.segments[0].codec, "Lucene103");
    assert_eq!(infos.counter, 1);
}

#[test]
fn test_read_segments_multiple_docs() {
    use bearing::document::DocumentBuilder;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::{keyword, long_field, text};
    use bearing::index::segment_infos;
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default();
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    for i in 0..5 {
        let doc = DocumentBuilder::new()
            .add_field(text("body").value(format!("document number {i}")))
            .add_field(keyword("id").value(format!("doc_{i}")))
            .add_field(long_field("modified").value(1000 + i as i64))
            .build();
        writer.add_document(doc).unwrap();
    }

    writer.commit().unwrap();

    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&**dir, segments_file).unwrap();

    assert_eq!(infos.segments.len(), 1);
    assert_eq!(infos.segments[0].name, "_0");
    assert_eq!(infos.segments[0].codec, "Lucene103");
}

#[test]
fn test_read_segments_memory_directory() {
    use bearing::document::DocumentBuilder;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::{keyword, text};
    use bearing::index::segment_infos;
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default();
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    let doc = DocumentBuilder::new()
        .add_field(text("body").value("hello world"))
        .add_field(keyword("tag").value("test"))
        .build();
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();

    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&**dir, segments_file).unwrap();

    assert_eq!(infos.segments.len(), 1);
    assert_eq!(infos.segments[0].name, "_0");
}

// --- Compound file tests ---

#[test]
fn test_read_segments_compound_mode() {
    use bearing::document::DocumentBuilder;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::{keyword, text};
    use bearing::index::segment_infos;
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default().use_compound_file(true);
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    let doc = DocumentBuilder::new()
        .add_field(text("body").value("compound file test"))
        .add_field(keyword("tag").value("test"))
        .build();
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();

    // segments_N should be readable even in compound mode
    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&**dir, segments_file).unwrap();

    assert_eq!(infos.segments.len(), 1);
    assert_eq!(infos.segments[0].name, "_0");

    // Should have .cfs and .cfe files
    assert_any!(files.iter(), |f: &String| f.ends_with(".cfs"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".cfe"));
}

#[test]
fn test_compound_directory_list_files() {
    use bearing::document::DocumentBuilder;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::{keyword, text};
    use bearing::index::segment_infos;
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default().use_compound_file(true);
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    let doc = DocumentBuilder::new()
        .add_field(text("body").value("compound listing test"))
        .add_field(keyword("tag").value("test"))
        .build();
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();

    // Read segments to get segment ID
    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&**dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    // Open compound directory
    let compound_dir = CompoundDirectory::open(&**dir, &seg.name, &seg.id).unwrap();
    let compound_files = compound_dir.list_all().unwrap();

    // Should contain segment files like .fnm, .fdt, .fdm, etc.
    assert_not_empty!(compound_files);
    assert_any!(compound_files.iter(), |f: &String| f.ends_with(".fnm"));
}

#[test]
fn test_compound_directory_read_embedded_file() {
    use bearing::document::DocumentBuilder;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::{keyword, text};
    use bearing::index::segment_infos;
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default().use_compound_file(true);
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    let doc = DocumentBuilder::new()
        .add_field(text("body").value("embedded file test"))
        .add_field(keyword("tag").value("test"))
        .build();
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();

    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&**dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    let compound_dir = CompoundDirectory::open(&**dir, &seg.name, &seg.id).unwrap();

    // Read a .fnm file from compound — should start with codec magic
    let mut input = compound_dir.open_input(".fnm").unwrap();
    let magic = input.read_be_int().unwrap();
    assert_eq!(
        magic, 0x3FD76C17_u32 as i32,
        ".fnm in compound should start with CODEC_MAGIC"
    );

    // Read codec name after magic
    let codec_name = input.read_string().unwrap();
    assert_eq!(codec_name, "Lucene94FieldInfos");
}

#[test]
fn test_compound_directory_memory() {
    use bearing::document::DocumentBuilder;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::{keyword, text};
    use bearing::index::segment_infos;
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default().use_compound_file(true);
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    let doc = DocumentBuilder::new()
        .add_field(text("body").value("memory compound test"))
        .add_field(keyword("tag").value("test"))
        .build();
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();

    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&**dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    let compound_dir = CompoundDirectory::open(&**dir, &seg.name, &seg.id).unwrap();
    let compound_files = compound_dir.list_all().unwrap();
    assert_not_empty!(compound_files);

    // Verify an embedded file is readable
    let mut input = compound_dir.open_input(".fnm").unwrap();
    let magic = input.read_be_int().unwrap();
    assert_eq!(magic, 0x3FD76C17_u32 as i32);
}

// ============================================================
// Stored fields reader integration tests
// ============================================================

#[test]
fn test_stored_fields_reader_round_trip() {
    use bearing::codecs::lucene90::stored_fields_reader::StoredFieldsReader;
    use bearing::document::DocumentBuilder;
    use bearing::document::StoredValue;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::stored;
    use bearing::index::segment_infos;
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default();
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    let doc = DocumentBuilder::new()
        .add_field(stored("title").string("Hello World"))
        .add_field(stored("count").int(42))
        .build();
    writer.add_document(doc).unwrap();

    let doc2 = DocumentBuilder::new()
        .add_field(stored("title").string("Second Doc"))
        .add_field(stored("count").int(99))
        .build();
    writer.add_document(doc2).unwrap();

    writer.commit().unwrap();

    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    let segments_file = files
        .iter()
        .find(|f| f.starts_with("segments_"))
        .expect("no segments file");
    let infos = segment_infos::read(&**dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    let mut reader = StoredFieldsReader::open(&**dir, &seg.name, "", &seg.id).unwrap();

    // Doc 0
    let fields = reader.document(0).unwrap();
    assert!(
        fields
            .iter()
            .any(|f| matches!(&f.value, StoredValue::String(s) if s == "Hello World")),
        "missing 'Hello World'"
    );
    assert!(
        fields
            .iter()
            .any(|f| matches!(&f.value, StoredValue::Int(42))),
        "missing Int(42)"
    );

    // Doc 1
    let fields1 = reader.document(1).unwrap();
    assert!(
        fields1
            .iter()
            .any(|f| matches!(&f.value, StoredValue::String(s) if s == "Second Doc")),
        "missing 'Second Doc'"
    );
    assert!(
        fields1
            .iter()
            .any(|f| matches!(&f.value, StoredValue::Int(99))),
        "missing Int(99)"
    );
}

#[test]
fn test_stored_fields_reader_all_types() {
    use bearing::codecs::lucene90::stored_fields_reader::StoredFieldsReader;
    use bearing::document::DocumentBuilder;
    use bearing::document::StoredValue;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::stored;
    use bearing::index::segment_infos;
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default();
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    let doc = DocumentBuilder::new()
        .add_field(stored("s").string("text value"))
        .add_field(stored("i").int(12345))
        .add_field(stored("l").long(9876543210))
        .add_field(stored("f").float(3.125))
        .add_field(stored("d").double(2.7))
        .add_field(stored("b").bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]))
        .build();
    writer.add_document(doc).unwrap();

    writer.commit().unwrap();

    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    let segments_file = files
        .iter()
        .find(|f| f.starts_with("segments_"))
        .expect("no segments file");
    let infos = segment_infos::read(&**dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    let mut reader = StoredFieldsReader::open(&**dir, &seg.name, "", &seg.id).unwrap();
    let fields = reader.document(0).unwrap();

    assert!(
        fields
            .iter()
            .any(|f| matches!(&f.value, StoredValue::String(s) if s == "text value")),
        "missing string"
    );
    assert!(
        fields
            .iter()
            .any(|f| matches!(&f.value, StoredValue::Int(12345))),
        "missing int"
    );
    assert!(
        fields
            .iter()
            .any(|f| matches!(&f.value, StoredValue::Long(9876543210))),
        "missing long"
    );
    assert!(
        fields
            .iter()
            .any(|f| matches!(&f.value, StoredValue::Float(v) if (*v - 3.125).abs() < 0.001)),
        "missing float"
    );
    assert!(
        fields
            .iter()
            .any(|f| matches!(&f.value, StoredValue::Double(v) if (*v - 2.7).abs() < 0.001)),
        "missing double"
    );
    assert!(
        fields
            .iter()
            .any(|f| matches!(&f.value, StoredValue::Bytes(b) if b == &[0xDE, 0xAD, 0xBE, 0xEF])),
        "missing bytes"
    );
}

#[test]
fn test_stored_fields_reader_many_docs() {
    use bearing::codecs::lucene90::stored_fields_reader::StoredFieldsReader;
    use bearing::document::DocumentBuilder;
    use bearing::document::StoredValue;
    use bearing::index::config::IndexWriterConfig;
    use bearing::index::field::stored;
    use bearing::index::segment_infos;
    use bearing::index::writer::IndexWriter;

    let config = IndexWriterConfig::default();
    let directory = Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())));
    let writer = IndexWriter::new(config, Arc::clone(&directory));

    for i in 0..50 {
        let doc = DocumentBuilder::new()
            .add_field(stored("title").string(format!("Document {i}")))
            .add_field(stored("id").int(i))
            .build();
        writer.add_document(doc).unwrap();
    }

    writer.commit().unwrap();

    let dir = directory.lock().unwrap();
    let files = dir.list_all().unwrap();
    let segments_file = files
        .iter()
        .find(|f| f.starts_with("segments_"))
        .expect("no segments file");
    let infos = segment_infos::read(&**dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    let mut reader = StoredFieldsReader::open(&**dir, &seg.name, "", &seg.id).unwrap();

    for i in 0..50 {
        let fields = reader.document(i).unwrap();
        let title = format!("Document {i}");
        assert!(
            fields
                .iter()
                .any(|f| matches!(&f.value, StoredValue::String(s) if s == &title)),
            "missing title for doc {i}"
        );
        assert!(
            fields
                .iter()
                .any(|f| matches!(&f.value, StoredValue::Int(v) if *v == i as i32)),
            "missing id for doc {i}"
        );
    }
}
