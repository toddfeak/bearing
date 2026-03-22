// SPDX-License-Identifier: Apache-2.0

//! Points/BKD metadata reader for the Lucene90 points format.
//!
//! Reads `.kdm` (metadata) and validates `.kdi` (index) and `.kdd` (data) files
//! written by [`super::points::write`]. Metadata is read eagerly during construction;
//! tree and leaf data are not accessed.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::lucene90::points::{
    BKD_CODEC, BKD_VERSION, DATA_CODEC, DATA_EXTENSION, FORMAT_VERSION, INDEX_CODEC,
    INDEX_EXTENSION, META_CODEC, META_EXTENSION,
};
use crate::index::{FieldInfos, index_file_names};
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory};

/// Per-field BKD tree metadata.
///
/// Stores statistics read eagerly from `.kdm`. When lazy tree reads are added,
/// this struct will also hold:
/// - `num_dims`, `num_index_dims`, `bytes_per_dim` — BKD config (also in FieldInfos)
/// - `min_packed_value`, `max_packed_value` — per-dimension bounds
/// - `num_index_bytes`, `data_start_fp`, `index_start_fp` — file pointers
#[derive(Clone)]
struct BkdEntry {
    num_leaves: u32,
    point_count: i64,
    doc_count: i32,
}

/// Reads points/BKD metadata for a segment.
///
/// Opens `.kdm`, `.kdi`, and `.kdd` files during construction. Per-field BKD
/// metadata is read eagerly from `.kdm`; tree and leaf data in `.kdi`/`.kdd`
/// are not accessed. Matches the constructor pattern of Java's
/// `Lucene90PointsReader` + `BKDReader`.
pub struct PointsReader {
    /// Per-field BKD metadata indexed by field number. `None` for fields without points.
    entries: Box<[Option<BkdEntry>]>,
}

impl PointsReader {
    /// Opens points files (`.kdm`, `.kdi`, `.kdd`) for the given segment.
    pub fn open(
        directory: &dyn Directory,
        segment_name: &str,
        segment_suffix: &str,
        segment_id: &[u8; codec_util::ID_LENGTH],
        field_infos: &FieldInfos,
    ) -> io::Result<Self> {
        // Open .kdm (metadata) with checksum validation
        let kdm_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);
        let meta_input = directory.open_input(&kdm_name)?;
        let mut meta_in = ChecksumIndexInput::new(meta_input);

        codec_util::check_index_header(
            &mut meta_in,
            META_CODEC,
            FORMAT_VERSION,
            FORMAT_VERSION,
            segment_id,
            segment_suffix,
        )?;

        let entries = read_fields(&mut meta_in, field_infos)?;

        // index and data file lengths (for future integrity checks)
        let _index_length = meta_in.read_le_long()?;
        let _data_length = meta_in.read_le_long()?;

        codec_util::check_footer(&mut meta_in)?;

        // Validate .kdi and .kdd headers
        let kdi_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, INDEX_EXTENSION);
        let mut kdi = directory.open_input(&kdi_name)?;
        codec_util::check_index_header(
            kdi.as_mut(),
            INDEX_CODEC,
            FORMAT_VERSION,
            FORMAT_VERSION,
            segment_id,
            segment_suffix,
        )?;

        let kdd_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, DATA_EXTENSION);
        let mut kdd = directory.open_input(&kdd_name)?;
        codec_util::check_index_header(
            kdd.as_mut(),
            DATA_CODEC,
            FORMAT_VERSION,
            FORMAT_VERSION,
            segment_id,
            segment_suffix,
        )?;

        debug!(
            "points_reader: opened {} entries for segment {segment_name}",
            entries.iter().filter(|e| e.is_some()).count()
        );

        Ok(Self { entries })
    }

    /// Returns the total number of indexed points for a field.
    pub fn point_count(&self, field_number: u32) -> Option<i64> {
        self.entry(field_number).map(|e| e.point_count)
    }

    /// Returns the number of documents with points for a field.
    pub fn doc_count(&self, field_number: u32) -> Option<i32> {
        self.entry(field_number).map(|e| e.doc_count)
    }

    /// Returns the number of leaf nodes in the BKD tree for a field.
    pub fn num_leaves(&self, field_number: u32) -> Option<u32> {
        self.entry(field_number).map(|e| e.num_leaves)
    }

    fn entry(&self, field_number: u32) -> Option<&BkdEntry> {
        self.entries
            .get(field_number as usize)
            .and_then(|opt| opt.as_ref())
    }
}

/// Reads all per-field BKD metadata entries from `.kdm`.
fn read_fields(
    meta: &mut dyn DataInput,
    field_infos: &FieldInfos,
) -> io::Result<Box<[Option<BkdEntry>]>> {
    let mut entries: Vec<Option<BkdEntry>> = vec![None; field_infos.len()];

    loop {
        let field_number = meta.read_le_int()?;
        if field_number == -1 {
            break;
        }

        let field_number = field_number as u32;
        let _info = field_infos
            .field_info_by_number(field_number)
            .ok_or_else(|| io::Error::other(format!("invalid field number: {field_number}")))?;

        let entry = read_bkd_entry(meta)?;
        entries[field_number as usize] = Some(entry);
    }

    Ok(entries.into_boxed_slice())
}

/// Reads a single BKD metadata entry (one per point field).
fn read_bkd_entry(meta: &mut dyn DataInput) -> io::Result<BkdEntry> {
    // BKD simple header (not index header)
    codec_util::check_header(meta, BKD_CODEC, BKD_VERSION, BKD_VERSION)?;

    let _num_dims = meta.read_vint()? as u32;
    let num_index_dims = meta.read_vint()? as u32;
    let _max_points_in_leaf = meta.read_vint()?;
    let bytes_per_dim = meta.read_vint()? as u32;
    let num_leaves = meta.read_vint()? as u32;

    // Skip min/max packed values (numIndexDims × bytesPerDim bytes each)
    let packed_len = (num_index_dims * bytes_per_dim) as u64;
    meta.skip_bytes(packed_len * 2)?;

    let point_count = meta.read_vlong()?;
    let doc_count = meta.read_vint()?;

    // Skip file pointers (needed for future tree reads)
    let _num_index_bytes = meta.read_vint()?;
    let _data_start_fp = meta.read_le_long()?;
    let _index_start_fp = meta.read_le_long()?;

    Ok(BkdEntry {
        num_leaves,
        point_count,
        doc_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::lucene90::points;
    use crate::document::{DocValuesType, IndexOptions};
    use crate::index::indexing_chain::PerFieldData;
    use crate::index::{FieldInfo, FieldInfos, PointDimensionConfig};
    use crate::store::{MemoryDirectory, SharedDirectory};
    use assertables::*;
    use std::collections::HashMap;

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    fn make_point_field(
        name: &str,
        number: u32,
        dims: u32,
        index_dims: u32,
        num_bytes: u32,
    ) -> FieldInfo {
        FieldInfo::new(
            name.to_string(),
            number,
            false,
            true,
            IndexOptions::None,
            DocValuesType::None,
            PointDimensionConfig {
                dimension_count: dims,
                index_dimension_count: index_dims,
                num_bytes,
            },
        )
    }

    fn make_point_data(values: Vec<(i32, Vec<u8>)>) -> PerFieldData {
        let mut pfd = PerFieldData::new();
        pfd.points = values;
        pfd
    }

    fn write_and_read(
        field_infos: &FieldInfos,
        per_field: &HashMap<String, PerFieldData>,
        num_docs: i32,
    ) -> PointsReader {
        let segment_id = [0u8; 16];
        let dir = test_directory();
        points::write(
            &dir,
            "_0",
            "",
            &segment_id,
            field_infos,
            per_field,
            num_docs,
        )
        .unwrap();
        let guard = dir.lock().unwrap();
        PointsReader::open(guard.as_ref(), "_0", "", &segment_id, field_infos).unwrap()
    }

    #[test]
    fn test_1d_int_field() {
        // 3 docs with 4-byte int points
        let fi = make_point_field("size", 0, 1, 1, 4);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "size".to_string(),
            make_point_data(vec![
                (0, 100i32.to_be_bytes().to_vec()),
                (1, 200i32.to_be_bytes().to_vec()),
                (2, 300i32.to_be_bytes().to_vec()),
            ]),
        );

        let reader = write_and_read(&field_infos, &per_field, 3);
        assert_eq!(reader.point_count(0), Some(3));
        assert_eq!(reader.doc_count(0), Some(3));
    }

    #[test]
    fn test_2d_latlon_field() {
        // 2 docs with 2D 4-byte points (like LatLonPoint)
        let fi = make_point_field("location", 0, 2, 2, 4);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        let mut point1 = Vec::new();
        point1.extend_from_slice(&10i32.to_be_bytes()); // lat
        point1.extend_from_slice(&20i32.to_be_bytes()); // lon
        let mut point2 = Vec::new();
        point2.extend_from_slice(&30i32.to_be_bytes());
        point2.extend_from_slice(&40i32.to_be_bytes());

        per_field.insert(
            "location".to_string(),
            make_point_data(vec![(0, point1), (1, point2)]),
        );

        let reader = write_and_read(&field_infos, &per_field, 2);
        assert_eq!(reader.point_count(0), Some(2));
        assert_eq!(reader.doc_count(0), Some(2));
        assert_eq!(reader.num_leaves(0), Some(1));
    }

    #[test]
    fn test_multiple_fields() {
        let fi_size = make_point_field("size", 0, 1, 1, 4);
        let fi_loc = make_point_field("location", 1, 2, 2, 4);
        let field_infos = FieldInfos::new(vec![fi_size, fi_loc]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "size".to_string(),
            make_point_data(vec![
                (0, 100i32.to_be_bytes().to_vec()),
                (1, 200i32.to_be_bytes().to_vec()),
            ]),
        );

        let mut loc_point = Vec::new();
        loc_point.extend_from_slice(&10i32.to_be_bytes());
        loc_point.extend_from_slice(&20i32.to_be_bytes());
        per_field.insert(
            "location".to_string(),
            make_point_data(vec![(0, loc_point)]),
        );

        let reader = write_and_read(&field_infos, &per_field, 2);
        assert_eq!(reader.point_count(0), Some(2));
        assert_eq!(reader.doc_count(0), Some(2));
        assert_eq!(reader.point_count(1), Some(1));
        assert_eq!(reader.doc_count(1), Some(1));
    }

    #[test]
    fn test_nonexistent_field() {
        let fi = make_point_field("size", 0, 1, 1, 4);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "size".to_string(),
            make_point_data(vec![(0, 42i32.to_be_bytes().to_vec())]),
        );

        let reader = write_and_read(&field_infos, &per_field, 1);
        assert_none!(reader.point_count(99));
        assert_none!(reader.doc_count(99));
    }

    #[test]
    fn test_8byte_long_field() {
        // 8-byte points (LongField)
        let fi = make_point_field("modified", 0, 1, 1, 8);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "modified".to_string(),
            make_point_data(vec![
                (0, 1000i64.to_be_bytes().to_vec()),
                (1, 2000i64.to_be_bytes().to_vec()),
            ]),
        );

        let reader = write_and_read(&field_infos, &per_field, 2);
        assert_eq!(reader.point_count(0), Some(2));
        assert_eq!(reader.doc_count(0), Some(2));
        assert_eq!(reader.num_leaves(0), Some(1));
    }
}
