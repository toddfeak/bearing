// SPDX-License-Identifier: Apache-2.0

//! Integration tests for store-level read functionality (DataInput, IndexInput, Directory::open_input).

#[macro_use]
extern crate assertables;

use std::path::Path;

use bearing::store::{CompoundDirectory, Directory, FSDirectory, MemoryDirectory};

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

#[test]
fn test_index_writer_files_readable() {
    use bearing::document::{Document, keyword_field, text_field};
    use bearing::index::IndexWriter;

    let writer = IndexWriter::new();

    let mut doc = Document::new();
    doc.add(text_field("body", "the quick brown fox"));
    doc.add(keyword_field("category", "animals"));
    writer.add_document(doc).unwrap();

    let result = writer.commit().unwrap();
    let mut dir = MemoryDirectory::new();
    result.write_to_directory(&mut dir).unwrap();

    // Every file should be openable and have non-zero length
    let files = dir.list_all().unwrap();
    assert_not_empty!(files);

    for file in &files {
        let input = dir.open_input(file).unwrap();
        assert_gt!(input.length(), 0, "file {file} has zero length");
    }
}

#[test]
fn test_index_writer_codec_files_have_valid_headers() {
    use bearing::document::{Document, keyword_field, text_field};
    use bearing::index::IndexWriter;

    let writer = IndexWriter::new();

    let mut doc = Document::new();
    doc.add(text_field("body", "the quick brown fox"));
    doc.add(keyword_field("category", "animals"));
    writer.add_document(doc).unwrap();

    let result = writer.commit().unwrap();
    let mut dir = MemoryDirectory::new();
    result.write_to_directory(&mut dir).unwrap();

    // Codec files (not segments_N) start with CODEC_MAGIC (0x3FD76C17 BE)
    let codec_magic_bytes: [u8; 4] = [0x3F, 0xD7, 0x6C, 0x17];
    let files = dir.list_all().unwrap();

    for file in &files {
        if file.starts_with("segments") {
            continue; // segments_N has its own format
        }

        let mut input = dir.open_input(file).unwrap();
        let mut header = [0u8; 4];
        input.read_bytes(&mut header).unwrap();
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
    use bearing::document::{Document, keyword_field, text_field};
    use bearing::index::{IndexWriter, IndexWriterConfig, segment_infos};

    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    let mut doc = Document::new();
    doc.add(text_field("body", "the quick brown fox"));
    doc.add(keyword_field("category", "animals"));
    writer.add_document(doc).unwrap();

    let result = writer.commit().unwrap();
    let mut dir = MemoryDirectory::new();
    result.write_to_directory(&mut dir).unwrap();

    // Find the segments_N file
    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();

    // Read segments_N
    let infos = segment_infos::read(&dir, segments_file).unwrap();

    // Should have exactly 1 segment
    assert_eq!(infos.segments.len(), 1);
    assert_eq!(infos.segments[0].name, "_0");
    assert_eq!(infos.segments[0].codec, "Lucene103");
    assert_eq!(infos.counter, 1);
}

#[test]
fn test_read_segments_multiple_docs() {
    use bearing::document::{Document, keyword_field, long_field, text_field};
    use bearing::index::{IndexWriter, IndexWriterConfig, segment_infos};

    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    for i in 0..5 {
        let mut doc = Document::new();
        doc.add(text_field("body", &format!("document number {i}")));
        doc.add(keyword_field("id", &format!("doc_{i}")));
        doc.add(long_field("modified", 1000 + i as i64));
        writer.add_document(doc).unwrap();
    }

    let result = writer.commit().unwrap();
    let mut dir = MemoryDirectory::new();
    result.write_to_directory(&mut dir).unwrap();

    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&dir, segments_file).unwrap();

    assert_eq!(infos.segments.len(), 1);
    assert_eq!(infos.segments[0].name, "_0");
    assert_eq!(infos.segments[0].codec, "Lucene103");
}

#[test]
fn test_read_segments_fs_directory() {
    use bearing::document::{Document, keyword_field, text_field};
    use bearing::index::{IndexWriter, IndexWriterConfig, segment_infos};

    let dir_path = temp_dir("read_segments_fs");
    let _cleanup = DirCleanup(&dir_path);

    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    let mut doc = Document::new();
    doc.add(text_field("body", "hello world"));
    doc.add(keyword_field("tag", "test"));
    writer.add_document(doc).unwrap();

    let result = writer.commit().unwrap();
    let mut fs_dir = FSDirectory::open(&dir_path).unwrap();
    result.write_to_directory(&mut fs_dir).unwrap();

    let files = fs_dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&fs_dir, segments_file).unwrap();

    assert_eq!(infos.segments.len(), 1);
    assert_eq!(infos.segments[0].name, "_0");
}

// --- Compound file tests ---

#[test]
fn test_read_segments_compound_mode() {
    use bearing::document::{Document, keyword_field, text_field};
    use bearing::index::{IndexWriter, segment_infos};

    // Default config uses compound files
    let writer = IndexWriter::new();

    let mut doc = Document::new();
    doc.add(text_field("body", "compound file test"));
    doc.add(keyword_field("tag", "test"));
    writer.add_document(doc).unwrap();

    let result = writer.commit().unwrap();
    let mut dir = MemoryDirectory::new();
    result.write_to_directory(&mut dir).unwrap();

    // segments_N should be readable even in compound mode
    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&dir, segments_file).unwrap();

    assert_eq!(infos.segments.len(), 1);
    assert_eq!(infos.segments[0].name, "_0");

    // Should have .cfs and .cfe files
    assert_any!(files.iter(), |f: &String| f.ends_with(".cfs"));
    assert_any!(files.iter(), |f: &String| f.ends_with(".cfe"));
}

#[test]
fn test_compound_directory_list_files() {
    use bearing::document::{Document, keyword_field, text_field};
    use bearing::index::{IndexWriter, segment_infos};

    let writer = IndexWriter::new();

    let mut doc = Document::new();
    doc.add(text_field("body", "compound listing test"));
    doc.add(keyword_field("tag", "test"));
    writer.add_document(doc).unwrap();

    let result = writer.commit().unwrap();
    let mut dir = MemoryDirectory::new();
    result.write_to_directory(&mut dir).unwrap();

    // Read segments to get segment ID
    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    // Open compound directory
    let compound_dir = CompoundDirectory::open(&dir, &seg.name, &seg.id).unwrap();
    let compound_files = compound_dir.list_all().unwrap();

    // Should contain segment files like .fnm, .fdt, .fdm, etc.
    assert_not_empty!(compound_files);
    assert_any!(compound_files.iter(), |f: &String| f.ends_with(".fnm"));
}

#[test]
fn test_compound_directory_read_embedded_file() {
    use bearing::document::{Document, keyword_field, text_field};
    use bearing::index::{IndexWriter, segment_infos};

    let writer = IndexWriter::new();

    let mut doc = Document::new();
    doc.add(text_field("body", "embedded file test"));
    doc.add(keyword_field("tag", "test"));
    writer.add_document(doc).unwrap();

    let result = writer.commit().unwrap();
    let mut dir = MemoryDirectory::new();
    result.write_to_directory(&mut dir).unwrap();

    let files = dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    let compound_dir = CompoundDirectory::open(&dir, &seg.name, &seg.id).unwrap();

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
fn test_compound_directory_fs() {
    use bearing::document::{Document, keyword_field, text_field};
    use bearing::index::{IndexWriter, segment_infos};

    let dir_path = temp_dir("compound_fs");
    let _cleanup = DirCleanup(&dir_path);

    let writer = IndexWriter::new();

    let mut doc = Document::new();
    doc.add(text_field("body", "fs compound test"));
    doc.add(keyword_field("tag", "test"));
    writer.add_document(doc).unwrap();

    let result = writer.commit().unwrap();
    let mut fs_dir = FSDirectory::open(&dir_path).unwrap();
    result.write_to_directory(&mut fs_dir).unwrap();

    let files = fs_dir.list_all().unwrap();
    let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
    let infos = segment_infos::read(&fs_dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    // Open compound from FSDirectory — exercises FSIndexInput::slice()
    let compound_dir = CompoundDirectory::open(&fs_dir, &seg.name, &seg.id).unwrap();
    let compound_files = compound_dir.list_all().unwrap();
    assert_not_empty!(compound_files);

    // Verify an embedded file is readable
    let mut input = compound_dir.open_input(".fnm").unwrap();
    let magic = input.read_be_int().unwrap();
    assert_eq!(magic, 0x3FD76C17_u32 as i32);
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("bearing_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

struct DirCleanup<'a>(&'a Path);

impl Drop for DirCleanup<'_> {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0);
    }
}

// ============================================================
// Stored fields reader integration tests
// ============================================================

#[test]
fn test_stored_fields_reader_round_trip() {
    use bearing::codecs::lucene90::compressing_stored_fields_reader::CompressingStoredFieldsReader;
    use bearing::document::{Document, StoredValue, stored_int_field, stored_string_field};
    use bearing::index::{IndexWriter, IndexWriterConfig, segment_infos};

    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    let mut doc = Document::new();
    doc.add(stored_string_field("title", "Hello World"));
    doc.add(stored_int_field("count", 42));
    writer.add_document(doc).unwrap();

    let mut doc2 = Document::new();
    doc2.add(stored_string_field("title", "Second Doc"));
    doc2.add(stored_int_field("count", 99));
    writer.add_document(doc2).unwrap();

    let result = writer.commit().unwrap();

    let mut mem_dir = MemoryDirectory::new();
    for seg_file in result.into_segment_files().unwrap() {
        mem_dir.write_file(&seg_file.name, &seg_file.data).unwrap();
    }

    let files = mem_dir.list_all().unwrap();
    let segments_file = files
        .iter()
        .find(|f| f.starts_with("segments_"))
        .expect("no segments file");
    let infos = segment_infos::read(&mem_dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    let mut reader = CompressingStoredFieldsReader::open(&mem_dir, &seg.name, "", &seg.id).unwrap();

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
    use bearing::codecs::lucene90::compressing_stored_fields_reader::CompressingStoredFieldsReader;
    use bearing::document::{
        Document, StoredValue, stored_bytes_field, stored_double_field, stored_float_field,
        stored_int_field, stored_long_field, stored_string_field,
    };
    use bearing::index::{IndexWriter, IndexWriterConfig, segment_infos};

    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    let mut doc = Document::new();
    doc.add(stored_string_field("s", "text value"));
    doc.add(stored_int_field("i", 12345));
    doc.add(stored_long_field("l", 9876543210));
    doc.add(stored_float_field("f", 3.125));
    doc.add(stored_double_field("d", 2.7));
    doc.add(stored_bytes_field("b", vec![0xDE, 0xAD, 0xBE, 0xEF]));
    writer.add_document(doc).unwrap();

    let result = writer.commit().unwrap();

    let mut mem_dir = MemoryDirectory::new();
    for seg_file in result.into_segment_files().unwrap() {
        mem_dir.write_file(&seg_file.name, &seg_file.data).unwrap();
    }

    let files = mem_dir.list_all().unwrap();
    let segments_file = files
        .iter()
        .find(|f| f.starts_with("segments_"))
        .expect("no segments file");
    let infos = segment_infos::read(&mem_dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    let mut reader = CompressingStoredFieldsReader::open(&mem_dir, &seg.name, "", &seg.id).unwrap();
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
    use bearing::codecs::lucene90::compressing_stored_fields_reader::CompressingStoredFieldsReader;
    use bearing::document::{Document, StoredValue, stored_int_field, stored_string_field};
    use bearing::index::{IndexWriter, IndexWriterConfig, segment_infos};

    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    for i in 0..50 {
        let mut doc = Document::new();
        doc.add(stored_string_field("title", &format!("Document {i}")));
        doc.add(stored_int_field("id", i));
        writer.add_document(doc).unwrap();
    }

    let result = writer.commit().unwrap();

    let mut mem_dir = MemoryDirectory::new();
    for seg_file in result.into_segment_files().unwrap() {
        mem_dir.write_file(&seg_file.name, &seg_file.data).unwrap();
    }

    let files = mem_dir.list_all().unwrap();
    let segments_file = files
        .iter()
        .find(|f| f.starts_with("segments_"))
        .expect("no segments file");
    let infos = segment_infos::read(&mem_dir, segments_file).unwrap();
    let seg = &infos.segments[0];

    let mut reader = CompressingStoredFieldsReader::open(&mem_dir, &seg.name, "", &seg.id).unwrap();

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
