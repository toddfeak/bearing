// SPDX-License-Identifier: Apache-2.0

//! Integration tests for store-level read functionality (DataInput, IndexInput, Directory::open_input).

use std::path::Path;

use bearing::store::{DataInput, DataOutput, Directory, FSDirectory, IndexInput, MemoryDirectory};

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
    assert!(!files.is_empty());

    for file in &files {
        let input = dir.open_input(file).unwrap();
        assert!(input.length() > 0, "file {file} has zero length");
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
        assert!(!codec_name.is_empty(), "file {file} has empty codec name");
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

    // Should have exactly 1 segment with 1 document
    assert_eq!(infos.segments.len(), 1);
    assert_eq!(infos.segments[0].info.max_doc, 1);
    assert_eq!(infos.segments[0].info.name, "_0");

    // Field infos should contain our fields
    let fis = &infos.segments[0].field_infos;
    assert!(fis.len() >= 2); // at least "body" and "category"

    let body = fis.iter().find(|f| f.name() == "body");
    assert!(body.is_some());

    let category = fis.iter().find(|f| f.name() == "category");
    assert!(category.is_some());
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
    assert_eq!(infos.segments[0].info.max_doc, 5);

    // Verify fields
    let fis = &infos.segments[0].field_infos;
    assert!(fis.iter().any(|f| f.name() == "body"));
    assert!(fis.iter().any(|f| f.name() == "id"));
    assert!(fis.iter().any(|f| f.name() == "modified"));
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
    assert_eq!(infos.segments[0].info.max_doc, 1);
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
