// SPDX-License-Identifier: Apache-2.0

//! Postings metadata reader for the Lucene103 postings format.
//!
//! Reads `.psm` (metadata), `.doc` (document IDs), and `.pos` (positions) files
//! written by [`super::postings_writer::PostingsWriter`]. Only metadata and file
//! headers are read during construction; posting list data is not accessed.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::lucene103::postings_format::{
    DOC_CODEC, DOC_EXTENSION, META_CODEC, META_EXTENSION, POS_CODEC, POS_EXTENSION,
    VERSION_CURRENT, VERSION_START,
};
use crate::index::{FieldInfos, index_file_names};
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory};

/// Reads postings metadata for a segment.
///
/// Opens `.psm`, `.doc`, and `.pos` files during construction. Only the `.psm`
/// metadata (impact statistics) is read; `.doc` and `.pos` are validated but not
/// read beyond their headers. Matches the constructor pattern of Java's
/// `Lucene103PostingsReader`.
///
/// When lazy posting list reads are added, this struct will also hold:
/// - `doc_in: Box<dyn IndexInput>` — `.doc` file handle
/// - `pos_in: Option<Box<dyn IndexInput>>` — `.pos` file handle
pub struct PostingsReader {
    /// Maximum number of competitive impacts at skip level 0.
    max_num_impacts_at_level0: i32,
    /// Maximum bytes for encoded impacts at skip level 0.
    max_impact_num_bytes_at_level0: i32,
    /// Maximum number of competitive impacts at skip level 1.
    max_num_impacts_at_level1: i32,
    /// Maximum bytes for encoded impacts at skip level 1.
    max_impact_num_bytes_at_level1: i32,
}

impl PostingsReader {
    /// Opens postings files (`.psm`, `.doc`, `.pos`) for the given segment.
    pub fn open(
        directory: &dyn Directory,
        segment_name: &str,
        segment_suffix: &str,
        segment_id: &[u8; codec_util::ID_LENGTH],
        field_infos: &FieldInfos,
    ) -> io::Result<Self> {
        // Open .psm (metadata) with checksum validation
        let psm_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);
        let meta_input = directory.open_input(&psm_name)?;
        let mut meta_in = ChecksumIndexInput::new(meta_input);

        codec_util::check_index_header(
            &mut meta_in,
            META_CODEC,
            VERSION_START,
            VERSION_CURRENT,
            segment_id,
            segment_suffix,
        )?;

        // Read impact statistics
        let max_num_impacts_at_level0 = meta_in.read_le_int()?;
        let max_impact_num_bytes_at_level0 = meta_in.read_le_int()?;
        let max_num_impacts_at_level1 = meta_in.read_le_int()?;
        let max_impact_num_bytes_at_level1 = meta_in.read_le_int()?;

        // Read expected file lengths
        let _expected_doc_file_length = meta_in.read_le_long()?;
        if field_infos.has_prox() {
            let _expected_pos_file_length = meta_in.read_le_long()?;
        }

        codec_util::check_footer(&mut meta_in)?;

        // Validate .doc header
        let doc_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, DOC_EXTENSION);
        let mut doc_in = directory.open_input(&doc_name)?;
        codec_util::check_index_header(
            doc_in.as_mut(),
            DOC_CODEC,
            VERSION_START,
            VERSION_CURRENT,
            segment_id,
            segment_suffix,
        )?;

        // Validate .pos header if positions exist
        if field_infos.has_prox() {
            let pos_name =
                index_file_names::segment_file_name(segment_name, segment_suffix, POS_EXTENSION);
            let mut pos_in = directory.open_input(&pos_name)?;
            codec_util::check_index_header(
                pos_in.as_mut(),
                POS_CODEC,
                VERSION_START,
                VERSION_CURRENT,
                segment_id,
                segment_suffix,
            )?;
        }

        debug!(
            "postings_reader: opened for segment {segment_name}, \
             impacts=[{max_num_impacts_at_level0}, {max_impact_num_bytes_at_level0}, \
             {max_num_impacts_at_level1}, {max_impact_num_bytes_at_level1}]"
        );

        Ok(Self {
            max_num_impacts_at_level0,
            max_impact_num_bytes_at_level0,
            max_num_impacts_at_level1,
            max_impact_num_bytes_at_level1,
        })
    }

    /// Returns the maximum number of competitive impacts at skip level 0.
    pub fn max_num_impacts_at_level0(&self) -> i32 {
        self.max_num_impacts_at_level0
    }

    /// Returns the maximum impact byte size at skip level 0.
    pub fn max_impact_num_bytes_at_level0(&self) -> i32 {
        self.max_impact_num_bytes_at_level0
    }

    /// Returns the maximum number of competitive impacts at skip level 1.
    pub fn max_num_impacts_at_level1(&self) -> i32 {
        self.max_num_impacts_at_level1
    }

    /// Returns the maximum impact byte size at skip level 1.
    pub fn max_impact_num_bytes_at_level1(&self) -> i32 {
        self.max_impact_num_bytes_at_level1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{self, Document};
    use crate::index::{IndexWriter, IndexWriterConfig};
    use crate::store::MemoryDirectory;
    use assertables::*;

    #[test]
    fn test_open_postings_reader() {
        // Write a segment with indexed fields via IndexWriter (non-compound for direct file access)
        let config = IndexWriterConfig::new().set_use_compound_file(false);
        let writer = IndexWriter::with_config(config);
        let mut doc = Document::new();
        doc.add(document::text_field("content", "hello world"));
        writer.add_document(doc).unwrap();

        let result = writer.commit().unwrap();
        let seg_files = result.into_segment_files().unwrap();

        // Build a MemoryDirectory from the segment files
        let mut mem_dir = MemoryDirectory::new();
        for sf in &seg_files {
            mem_dir.write_file(&sf.name, &sf.data).unwrap();
        }
        let dir = Box::new(mem_dir) as Box<dyn Directory>;

        // Find segment info
        let files = dir.list_all().unwrap();
        let segments_file = files.iter().find(|f| f.starts_with("segments_")).unwrap();
        let infos = crate::index::segment_infos::read(dir.as_ref(), segments_file).unwrap();
        let seg = &infos.segments[0];

        let si =
            crate::codecs::lucene99::segment_info_format::read(dir.as_ref(), &seg.name, &seg.id)
                .unwrap();
        let field_infos =
            crate::codecs::lucene94::field_infos_format::read(dir.as_ref(), &si, "").unwrap();

        // Find postings suffix from field attributes
        let suffix = field_infos
            .iter()
            .find_map(|fi| {
                let format = fi.get_attribute("PerFieldPostingsFormat.format")?;
                let sfx = fi.get_attribute("PerFieldPostingsFormat.suffix")?;
                Some(format!("{format}_{sfx}"))
            })
            .unwrap();

        // Open the postings reader
        let reader =
            PostingsReader::open(dir.as_ref(), &seg.name, &suffix, &seg.id, &field_infos).unwrap();

        // Impact stats should be non-negative
        assert_ge!(reader.max_num_impacts_at_level0(), 0);
        assert_ge!(reader.max_impact_num_bytes_at_level0(), 0);
        assert_ge!(reader.max_num_impacts_at_level1(), 0);
        assert_ge!(reader.max_impact_num_bytes_at_level1(), 0);
    }
}
