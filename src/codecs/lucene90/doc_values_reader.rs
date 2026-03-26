// SPDX-License-Identifier: Apache-2.0

//! Doc values metadata reader for the Lucene90 doc values format.
//!
//! Reads `.dvm` (metadata) and `.dvd` (data) files written by [`super::doc_values::write`].
//! Metadata is read eagerly during construction; value data is read lazily from
//! the `.dvd` data file on demand.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::lucene90::doc_values::{
    BINARY, DATA_CODEC, DATA_EXTENSION, DIRECT_MONOTONIC_BLOCK_SHIFT, META_CODEC, META_EXTENSION,
    NUMERIC, SORTED, SORTED_NUMERIC, SORTED_SET, VERSION,
};
use crate::index::{FieldInfos, index_file_names};
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory, IndexInput};

// ---------------------------------------------------------------------------
// Entry types — one per doc values type, stored eagerly in memory
// ---------------------------------------------------------------------------

/// Per-field doc values metadata.
///
/// Stores only the document count for now. Additional fields (offsets, encoding
/// params) will be added when lazy value reads are implemented.
#[derive(Clone, Copy)]
struct DocValuesEntry {
    num_docs_with_field: i32,
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Reads doc values metadata for a segment.
///
/// Opens `.dvm` and `.dvd` files during construction. All metadata is read
/// eagerly from `.dvm`; the `.dvd` data file handle is kept open for future
/// lazy value reads.
pub struct DocValuesReader {
    /// Per-field metadata indexed by field number. `None` for fields without doc values.
    entries: Box<[Option<DocValuesEntry>]>,
    /// Open handle to the `.dvd` data file for lazy value reads.
    data: Box<dyn IndexInput>,
}

impl DocValuesReader {
    /// Opens doc values files (`.dvm`, `.dvd`) for the given segment.
    pub fn open(
        directory: &dyn Directory,
        segment_name: &str,
        segment_suffix: &str,
        segment_id: &[u8; codec_util::ID_LENGTH],
        field_infos: &FieldInfos,
    ) -> io::Result<Self> {
        // Open .dvm (metadata) with checksum validation
        let dvm_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);
        let meta_input = directory.open_input(&dvm_name)?;
        let mut meta_in = ChecksumIndexInput::new(meta_input);

        let version = codec_util::check_index_header(
            &mut meta_in,
            META_CODEC,
            VERSION,
            VERSION,
            segment_id,
            segment_suffix,
        )?;

        let entries = read_fields(&mut meta_in, field_infos)?;

        codec_util::check_footer(&mut meta_in)?;

        // Open .dvd (data) and validate header
        let dvd_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, DATA_EXTENSION);
        let mut data = directory.open_input(&dvd_name)?;
        let data_version = codec_util::check_index_header(
            data.as_mut(),
            DATA_CODEC,
            VERSION,
            VERSION,
            segment_id,
            segment_suffix,
        )?;

        if version != data_version {
            return Err(io::Error::other(format!(
                "format version mismatch: meta={version}, data={data_version}"
            )));
        }

        codec_util::retrieve_checksum(data.as_mut())?;

        debug!(
            "doc_values_reader: opened {} entries for segment {segment_name}",
            entries.iter().filter(|e| e.is_some()).count()
        );

        Ok(Self { entries, data })
    }

    /// Returns a reference to the `.dvd` data input for lazy value reads.
    pub fn data(&self) -> &dyn IndexInput {
        self.data.as_ref()
    }

    /// Returns the number of documents that have values for the given field.
    pub fn num_docs_with_field(&self, field_number: u32) -> Option<i32> {
        self.entries
            .get(field_number as usize)
            .and_then(|opt| opt.as_ref())
            .map(|e| e.num_docs_with_field)
    }
}

// ---------------------------------------------------------------------------
// Metadata parsing
// ---------------------------------------------------------------------------

/// Reads all doc values metadata entries from the `.dvm` file.
fn read_fields(
    meta: &mut dyn DataInput,
    field_infos: &FieldInfos,
) -> io::Result<Box<[Option<DocValuesEntry>]>> {
    let mut entries: Vec<Option<DocValuesEntry>> = vec![None; field_infos.len()];

    loop {
        let field_number = meta.read_le_int()?;
        if field_number == -1 {
            break;
        }

        let field_number = field_number as u32;
        let info = field_infos
            .field_info_by_number(field_number)
            .ok_or_else(|| io::Error::other(format!("invalid field number: {field_number}")))?;

        let type_byte = meta.read_byte()?;

        // Skip-index metadata must be read BEFORE type-specific metadata
        if info.doc_values_skip_index_type != 0 {
            read_doc_values_skipper_meta(meta)?;
        }

        let entry = match type_byte {
            NUMERIC => read_numeric(meta)?,
            BINARY => read_binary(meta)?,
            SORTED => read_sorted(meta)?,
            SORTED_SET => read_sorted_set(meta)?,
            SORTED_NUMERIC => read_sorted_numeric(meta)?,
            _ => {
                return Err(io::Error::other(format!(
                    "invalid doc values type: {type_byte}"
                )));
            }
        };

        entries[field_number as usize] = Some(entry);
    }

    Ok(entries.into_boxed_slice())
}

/// Reads doc values skipper metadata to keep the stream in sync.
///
/// Java stores these values in a `DocValuesSkipperEntry` for use by
/// `getDocValuesSkipper()`. We read and discard them for now since skip
/// index queries are not yet implemented.
fn read_doc_values_skipper_meta(meta: &mut dyn DataInput) -> io::Result<()> {
    meta.read_le_long()?; // offset
    meta.read_le_long()?; // length
    meta.read_le_long()?; // maxValue
    meta.read_le_long()?; // minValue
    meta.read_le_int()?; // docCount
    meta.read_le_int()?; // maxDocId
    Ok(())
}

/// Skips the docs-with-field block written by `write_values()`.
fn skip_docs_with_field(meta: &mut dyn DataInput) -> io::Result<()> {
    meta.read_le_long()?; // offset
    meta.read_le_long()?; // length
    meta.read_le_short()?; // jump_table_entry_count
    meta.read_byte()?; // dense_rank_power
    Ok(())
}

/// Reads the `write_values()` block, returning `num_values`.
/// Skips all encoding metadata (table, bpv, min, gcd, offsets).
fn read_values_num_values(meta: &mut dyn DataInput) -> io::Result<i64> {
    skip_docs_with_field(meta)?;

    let num_values = meta.read_le_long()?;

    let table_size = meta.read_le_int()?;
    if table_size > 0 {
        meta.skip_bytes(table_size as u64 * 8)?; // table entries
    }

    meta.read_byte()?; // bits_per_value
    meta.read_le_long()?; // min_value
    meta.read_le_long()?; // gcd
    meta.read_le_long()?; // values_offset
    meta.read_le_long()?; // values_length
    meta.read_le_long()?; // jump_table_offset

    Ok(num_values)
}

fn read_numeric(meta: &mut dyn DataInput) -> io::Result<DocValuesEntry> {
    let num_values = read_values_num_values(meta)?;
    Ok(DocValuesEntry {
        num_docs_with_field: num_values as i32,
    })
}

fn read_binary(meta: &mut dyn DataInput) -> io::Result<DocValuesEntry> {
    meta.read_le_long()?; // data_offset
    meta.read_le_long()?; // data_length
    skip_docs_with_field(meta)?;
    let num_docs_with_field = meta.read_le_int()?;
    let min_length = meta.read_le_int()?;
    let max_length = meta.read_le_int()?;

    if max_length > min_length {
        skip_direct_monotonic_addresses(meta, num_docs_with_field as i64 + 1)?;
    }

    Ok(DocValuesEntry {
        num_docs_with_field,
    })
}

fn read_sorted(meta: &mut dyn DataInput) -> io::Result<DocValuesEntry> {
    let num_values = read_values_num_values(meta)?;
    skip_terms_dict(meta)?;
    Ok(DocValuesEntry {
        num_docs_with_field: num_values as i32,
    })
}

fn read_sorted_numeric(meta: &mut dyn DataInput) -> io::Result<DocValuesEntry> {
    let num_values = read_values_num_values(meta)?;
    let num_docs_with_field = meta.read_le_int()?;

    if num_values > num_docs_with_field as i64 {
        skip_direct_monotonic_addresses(meta, num_docs_with_field as i64 + 1)?;
    }

    Ok(DocValuesEntry {
        num_docs_with_field,
    })
}

fn read_sorted_set(meta: &mut dyn DataInput) -> io::Result<DocValuesEntry> {
    let is_multi_valued = meta.read_byte()?;

    if is_multi_valued == 0 {
        // Single-valued: like SORTED
        let num_values = read_values_num_values(meta)?;
        skip_terms_dict(meta)?;
        Ok(DocValuesEntry {
            num_docs_with_field: num_values as i32,
        })
    } else {
        // Multi-valued: like SORTED_NUMERIC + terms dict
        let num_values = read_values_num_values(meta)?;
        let num_docs_with_field = meta.read_le_int()?;

        if num_values > num_docs_with_field as i64 {
            skip_direct_monotonic_addresses(meta, num_docs_with_field as i64 + 1)?;
        }

        skip_terms_dict(meta)?;

        Ok(DocValuesEntry {
            num_docs_with_field,
        })
    }
}

// ---------------------------------------------------------------------------
// Skip helpers — advance past metadata we don't need yet
// ---------------------------------------------------------------------------

/// Skips DirectMonotonicReader metadata blocks in the meta stream.
///
/// Each block is 21 bytes: min(i64) + avgInc(i32) + offset(i64) + bpv(u8).
fn skip_direct_monotonic_meta_blocks(
    meta: &mut dyn DataInput,
    num_values: i64,
    block_shift: u32,
) -> io::Result<()> {
    let block_size = 1i64 << block_shift;
    let num_blocks = (num_values + block_size - 1) / block_size;
    // 21 bytes per block: i64(8) + i32(4) + i64(8) + u8(1)
    meta.skip_bytes(num_blocks as u64 * 21)?;
    Ok(())
}

/// Skips DirectMonotonic addresses metadata: offset, blockShift vint,
/// DM meta blocks, and length.
fn skip_direct_monotonic_addresses(meta: &mut dyn DataInput, num_values: i64) -> io::Result<()> {
    let _addresses_offset = meta.read_le_long()?;
    let block_shift = meta.read_vint()? as u32;
    skip_direct_monotonic_meta_blocks(meta, num_values, block_shift)?;
    let _addresses_length = meta.read_le_long()?;
    Ok(())
}

/// Skips the terms dictionary metadata written by `add_terms_dict()`.
fn skip_terms_dict(meta: &mut dyn DataInput) -> io::Result<()> {
    let num_terms = meta.read_vlong()?;
    let block_shift = meta.read_le_int()? as u32;

    // Terms block addresses: DirectMonotonic meta blocks
    let terms_block_size = 1i64 << block_shift;
    let num_term_blocks = (num_terms + terms_block_size - 1) / terms_block_size;
    // DM writer adds entries at block boundaries, so num DM values = num_term_blocks
    skip_direct_monotonic_meta_blocks(meta, num_term_blocks, DIRECT_MONOTONIC_BLOCK_SHIFT)?;

    let _max_term_length = meta.read_le_int()?;
    let _max_block_length = meta.read_le_int()?;
    let _terms_data_offset = meta.read_le_long()?;
    let _terms_data_length = meta.read_le_long()?;
    let _terms_addresses_offset = meta.read_le_long()?;
    let _terms_addresses_length = meta.read_le_long()?;

    // Reverse index
    let reverse_index_shift = meta.read_le_int()? as u32;
    let reverse_block_size = 1i64 << reverse_index_shift;
    let num_reverse_blocks = (num_terms + reverse_block_size - 1) / reverse_block_size;
    // Reverse index DM writer adds entries at reverse index boundaries + final entry
    skip_direct_monotonic_meta_blocks(meta, num_reverse_blocks + 1, DIRECT_MONOTONIC_BLOCK_SHIFT)?;

    let _reverse_index_offset = meta.read_le_long()?;
    let _reverse_index_length = meta.read_le_long()?;
    let _reverse_addresses_offset = meta.read_le_long()?;
    let _reverse_addresses_length = meta.read_le_long()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::lucene90::doc_values;
    use crate::document::{DocValuesType, IndexOptions};
    use crate::index::indexing_chain::{DocValuesAccumulator, PerFieldData};
    use crate::index::{FieldInfo, FieldInfos};
    use crate::store::{MemoryDirectory, SharedDirectory};
    use crate::util::BytesRef;
    use assertables::*;
    use std::collections::HashMap;

    fn make_field_info(name: &str, number: u32, dv_type: DocValuesType) -> FieldInfo {
        crate::test_util::make_field_info(name, number, true, IndexOptions::None, dv_type)
    }

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    fn make_per_field_numeric(values: Vec<(i32, i64)>) -> PerFieldData {
        let mut pfd = PerFieldData::new();
        pfd.doc_values = DocValuesAccumulator::Numeric(values);
        pfd
    }

    fn make_per_field_binary(values: Vec<(i32, Vec<u8>)>) -> PerFieldData {
        let mut pfd = PerFieldData::new();
        pfd.doc_values = DocValuesAccumulator::Binary(values);
        pfd
    }

    fn make_per_field_sorted(values: Vec<(i32, BytesRef)>) -> PerFieldData {
        let mut pfd = PerFieldData::new();
        pfd.doc_values = DocValuesAccumulator::Sorted(values);
        pfd
    }

    fn make_per_field_sorted_numeric(values: Vec<(i32, Vec<i64>)>) -> PerFieldData {
        let mut pfd = PerFieldData::new();
        pfd.doc_values = DocValuesAccumulator::SortedNumeric(values);
        pfd
    }

    fn make_per_field_sorted_set(values: Vec<(i32, Vec<BytesRef>)>) -> PerFieldData {
        let mut pfd = PerFieldData::new();
        pfd.doc_values = DocValuesAccumulator::SortedSet(values);
        pfd
    }

    /// Writes doc values and opens a reader.
    fn write_and_read(
        field_infos: &FieldInfos,
        per_field: &HashMap<String, PerFieldData>,
        num_docs: i32,
        suffix: &str,
    ) -> DocValuesReader {
        let segment_id = [0u8; 16];
        let dir = test_directory();
        doc_values::write(
            &dir,
            "_0",
            suffix,
            &segment_id,
            field_infos,
            per_field,
            num_docs,
        )
        .unwrap();
        let guard = dir.lock().unwrap();
        DocValuesReader::open(guard.as_ref(), "_0", suffix, &segment_id, field_infos).unwrap()
    }

    #[test]
    fn test_numeric_all_docs() {
        let fi = make_field_info("count", 0, DocValuesType::Numeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "count".to_string(),
            make_per_field_numeric(vec![(0, 10), (1, 20), (2, 30)]),
        );

        let reader = write_and_read(&field_infos, &per_field, 3, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(3));
        assert_gt!(reader.data().length(), 0);
    }

    #[test]
    fn test_numeric_sparse() {
        let fi = make_field_info("count", 0, DocValuesType::Numeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "count".to_string(),
            make_per_field_numeric(vec![(1, 10), (3, 20)]),
        );

        let reader = write_and_read(&field_infos, &per_field, 5, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(2));
    }

    #[test]
    fn test_numeric_empty() {
        let fi = make_field_info("count", 0, DocValuesType::Numeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert("count".to_string(), make_per_field_numeric(vec![]));

        let reader = write_and_read(&field_infos, &per_field, 5, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(0));
    }

    #[test]
    fn test_binary_all_docs() {
        let fi = make_field_info("hash", 0, DocValuesType::Binary);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "hash".to_string(),
            make_per_field_binary(vec![
                (0, b"abc".to_vec()),
                (1, b"def".to_vec()),
                (2, b"ghi".to_vec()),
            ]),
        );

        let reader = write_and_read(&field_infos, &per_field, 3, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(3));
    }

    #[test]
    fn test_binary_variable_length() {
        let fi = make_field_info("data", 0, DocValuesType::Binary);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "data".to_string(),
            make_per_field_binary(vec![
                (0, b"short".to_vec()),
                (1, b"a longer value here".to_vec()),
            ]),
        );

        let reader = write_and_read(&field_infos, &per_field, 2, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(2));
    }

    #[test]
    fn test_binary_sparse() {
        let fi = make_field_info("data", 0, DocValuesType::Binary);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "data".to_string(),
            make_per_field_binary(vec![(1, b"abc".to_vec()), (3, b"def".to_vec())]),
        );

        let reader = write_and_read(&field_infos, &per_field, 5, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(2));
    }

    #[test]
    fn test_sorted() {
        let fi = make_field_info("category", 0, DocValuesType::Sorted);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "category".to_string(),
            make_per_field_sorted(vec![
                (0, BytesRef::new(b"alpha".to_vec())),
                (1, BytesRef::new(b"beta".to_vec())),
                (2, BytesRef::new(b"alpha".to_vec())),
            ]),
        );

        let reader = write_and_read(&field_infos, &per_field, 3, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(3));
    }

    #[test]
    fn test_sorted_numeric_single_valued() {
        let fi = make_field_info("priority", 0, DocValuesType::SortedNumeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "priority".to_string(),
            make_per_field_sorted_numeric(vec![(0, vec![100]), (1, vec![200]), (2, vec![300])]),
        );

        let reader = write_and_read(&field_infos, &per_field, 3, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(3));
    }

    #[test]
    fn test_sorted_numeric_multi_valued() {
        let fi = make_field_info("tags", 0, DocValuesType::SortedNumeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "tags".to_string(),
            make_per_field_sorted_numeric(vec![(0, vec![1, 2, 3]), (1, vec![4]), (2, vec![5, 6])]),
        );

        let reader = write_and_read(&field_infos, &per_field, 3, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(3));
    }

    #[test]
    fn test_sorted_numeric_sparse() {
        let fi = make_field_info("tags", 0, DocValuesType::SortedNumeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "tags".to_string(),
            make_per_field_sorted_numeric(vec![(1, vec![10, 20]), (3, vec![30])]),
        );

        let reader = write_and_read(&field_infos, &per_field, 5, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(2));
    }

    #[test]
    fn test_sorted_set_single_valued() {
        let fi = make_field_info("tag", 0, DocValuesType::SortedSet);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "tag".to_string(),
            make_per_field_sorted_set(vec![
                (0, vec![BytesRef::new(b"a".to_vec())]),
                (1, vec![BytesRef::new(b"b".to_vec())]),
                (2, vec![BytesRef::new(b"c".to_vec())]),
            ]),
        );

        let reader = write_and_read(&field_infos, &per_field, 3, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(3));
    }

    #[test]
    fn test_sorted_set_multi_valued() {
        let fi = make_field_info("tags", 0, DocValuesType::SortedSet);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "tags".to_string(),
            make_per_field_sorted_set(vec![
                (
                    0,
                    vec![BytesRef::new(b"a".to_vec()), BytesRef::new(b"b".to_vec())],
                ),
                (1, vec![BytesRef::new(b"c".to_vec())]),
                (
                    2,
                    vec![BytesRef::new(b"a".to_vec()), BytesRef::new(b"d".to_vec())],
                ),
            ]),
        );

        let reader = write_and_read(&field_infos, &per_field, 3, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(3));
    }

    #[test]
    fn test_multiple_fields_mixed_types() {
        let fi_num = make_field_info("count", 0, DocValuesType::Numeric);
        let fi_bin = make_field_info("hash", 1, DocValuesType::Binary);
        let fi_sn = make_field_info("priority", 2, DocValuesType::SortedNumeric);
        let field_infos = FieldInfos::new(vec![fi_num, fi_bin, fi_sn]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "count".to_string(),
            make_per_field_numeric(vec![(0, 10), (1, 20), (2, 30)]),
        );
        per_field.insert(
            "hash".to_string(),
            make_per_field_binary(vec![(0, b"abc".to_vec()), (1, b"def".to_vec())]),
        );
        per_field.insert(
            "priority".to_string(),
            make_per_field_sorted_numeric(vec![(0, vec![1]), (2, vec![3])]),
        );

        let reader = write_and_read(&field_infos, &per_field, 3, "Lucene90_0");
        assert_eq!(reader.num_docs_with_field(0), Some(3));
        assert_eq!(reader.num_docs_with_field(1), Some(2));
        assert_eq!(reader.num_docs_with_field(2), Some(2));
    }

    #[test]
    fn test_nonexistent_field() {
        let fi = make_field_info("count", 0, DocValuesType::Numeric);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert("count".to_string(), make_per_field_numeric(vec![(0, 10)]));

        let reader = write_and_read(&field_infos, &per_field, 1, "Lucene90_0");
        assert_none!(reader.num_docs_with_field(99));
    }
}
