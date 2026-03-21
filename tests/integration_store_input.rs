// SPDX-License-Identifier: Apache-2.0

//! Integration tests for store-level read functionality (DataInput, IndexInput, Directory::open_input).

use std::path::Path;

use bearing::store::{
    CompoundDirectory, DataInput, DataOutput, Directory, FSDirectory, IndexInput, MemoryDirectory,
};

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
    assert!(files.iter().any(|f| f.ends_with(".cfs")));
    assert!(files.iter().any(|f| f.ends_with(".cfe")));
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
    assert!(!compound_files.is_empty());
    assert!(
        compound_files.iter().any(|f| f.ends_with(".fnm")),
        "expected .fnm in compound files: {compound_files:?}"
    );
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
    assert!(!compound_files.is_empty());

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
