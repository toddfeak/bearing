// SPDX-License-Identifier: Apache-2.0

//! Norms reader for the Lucene90 norms format.
//!
//! Reads `.nvm` (metadata) and `.nvd` (data) files written by [`super::norms::write`].
//! Metadata is read eagerly during construction; norm values are read lazily from
//! the `.nvd` data file on demand.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::lucene90::indexed_disi::IndexedDISI;
use crate::codecs::lucene90::norms::{
    DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION, VERSION,
};
use crate::index::{FieldInfos, index_file_names};
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory, IndexInput};

/// Per-field norms metadata entry read from `.nvm`.
#[derive(Clone)]
struct NormsEntry {
    /// Offset of the IndexedDISI bitset in `.nvd`, or -1 (ALL) / -2 (EMPTY).
    docs_with_field_offset: i64,
    /// Byte length of the IndexedDISI bitset in `.nvd`.
    docs_with_field_length: i64,
    /// Number of jump table entries for the IndexedDISI.
    jump_table_entry_count: i16,
    /// Dense rank power for the IndexedDISI.
    dense_rank_power: u8,
    /// Number of documents that have a norm value for this field.
    num_docs_with_field: i32,
    /// Bytes per norm value: 0 (constant), 1, 2, 4, or 8.
    bytes_per_norm: u8,
    /// If `bytes_per_norm == 0`: the constant norm value.
    /// Otherwise: byte offset into `.nvd` where norm values start.
    norms_offset: i64,
}

/// Reads norms for a segment.
///
/// Opens `.nvm` and `.nvd` files during construction. Metadata entries are read
/// eagerly and stored as an immutable slice; the `.nvd` data file handle is kept
/// open for lazy value reads via [`get`](Self::get).
pub struct NormsReader {
    /// Per-field metadata indexed by field number. `None` for fields without norms.
    entries: Box<[Option<NormsEntry>]>,
    /// Maximum document ID + 1 for this segment.
    max_doc: i32,
    /// Open handle to the `.nvd` data file.
    data: Box<dyn IndexInput>,
}

impl NormsReader {
    /// Opens norms files (`.nvm`, `.nvd`) for the given segment.
    ///
    /// Reads all metadata eagerly from `.nvm`; the `.nvd` data file is opened but
    /// no data is read until [`get`](Self::get) is called.
    pub fn open(
        directory: &dyn Directory,
        segment_name: &str,
        segment_suffix: &str,
        segment_id: &[u8; codec_util::ID_LENGTH],
        field_infos: &FieldInfos,
        max_doc: i32,
    ) -> io::Result<Self> {
        // Open .nvm (metadata) with checksum validation
        let nvm_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, META_EXTENSION);
        let meta_input = directory.open_input(&nvm_name)?;
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

        // Open .nvd (data) and validate header
        let nvd_name =
            index_file_names::segment_file_name(segment_name, segment_suffix, DATA_EXTENSION);
        let mut data = directory.open_input(&nvd_name)?;
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

        debug!(
            "norms_reader: opened {} entries for segment {segment_name}",
            entries.len()
        );

        Ok(Self {
            entries,
            max_doc,
            data,
        })
    }

    /// Returns the number of documents that have norms for the given field.
    ///
    /// Returns `None` if no norms entry exists for this field number.
    pub fn num_docs_with_field(&self, field_number: u32) -> Option<i32> {
        self.entry(field_number).map(|e| e.num_docs_with_field)
    }

    /// Returns the norm value for a document.
    ///
    /// Returns `Ok(None)` if the field has no norms entry (EMPTY pattern) or if
    /// the document doesn't have a norm for this field (SPARSE pattern, doc not in DISI).
    pub fn get(&mut self, field_number: u32, doc_id: i32) -> io::Result<Option<i64>> {
        if doc_id < 0 || doc_id >= self.max_doc {
            return Err(io::Error::other(format!(
                "doc_id {doc_id} out of range [0, {})",
                self.max_doc
            )));
        }

        let entry = match self.entry(field_number) {
            Some(e) => e,
            None => return Ok(None),
        };

        if entry.docs_with_field_offset == -2 {
            return Ok(None);
        }

        // Copy what we need before borrowing &mut self for data reads
        let norms_offset = entry.norms_offset;
        let bytes_per_norm = entry.bytes_per_norm;
        let docs_with_field_offset = entry.docs_with_field_offset;

        if docs_with_field_offset == -1 {
            // ALL: every document has a norm
            if bytes_per_norm == 0 {
                return Ok(Some(norms_offset));
            }
            return self.read_norm_value(norms_offset, bytes_per_norm, doc_id as i64);
        }

        // SPARSE: use IndexedDISI to check presence and get ordinal
        let docs_with_field_length = entry.docs_with_field_length;
        let jump_table_entry_count = entry.jump_table_entry_count;
        let dense_rank_power = entry.dense_rank_power;
        let num_docs_with_field = entry.num_docs_with_field;

        let mut disi = IndexedDISI::new(
            self.data.as_ref(),
            docs_with_field_offset,
            docs_with_field_length,
            jump_table_entry_count,
            dense_rank_power,
            num_docs_with_field as i64,
        )?;

        if !disi.advance_exact(doc_id)? {
            return Ok(None);
        }

        if bytes_per_norm == 0 {
            return Ok(Some(norms_offset));
        }
        self.read_norm_value(norms_offset, bytes_per_norm, disi.index() as i64)
    }

    /// Reads a single norm value from the data file at the given position.
    fn read_norm_value(
        &mut self,
        base_offset: i64,
        bytes_per_norm: u8,
        index: i64,
    ) -> io::Result<Option<i64>> {
        let offset = base_offset as u64 + (index as u64) * (bytes_per_norm as u64);
        self.data.seek(offset)?;
        let value = match bytes_per_norm {
            1 => self.data.read_byte()? as i8 as i64,
            2 => self.data.read_le_short()? as i64,
            4 => self.data.read_le_int()? as i64,
            8 => self.data.read_le_long()?,
            _ => {
                return Err(io::Error::other(format!(
                    "invalid bytes_per_norm: {bytes_per_norm}"
                )));
            }
        };
        Ok(Some(value))
    }

    /// Returns the norms entry for a field number, or `None` if absent.
    fn entry(&self, field_number: u32) -> Option<&NormsEntry> {
        self.entries
            .get(field_number as usize)
            .and_then(|opt| opt.as_ref())
    }
}

/// Reads all norms metadata entries from the `.nvm` file.
///
/// Returns a `Box<[Option<NormsEntry>]>` indexed by field number.
fn read_fields(
    meta: &mut dyn DataInput,
    field_infos: &FieldInfos,
) -> io::Result<Box<[Option<NormsEntry>]>> {
    let mut entries: Vec<Option<NormsEntry>> = vec![None; field_infos.len()];

    loop {
        let field_number = meta.read_le_int()?;
        if field_number == -1 {
            break; // EOF marker
        }

        let field_number = field_number as u32;
        let info = field_infos
            .field_info_by_number(field_number)
            .ok_or_else(|| io::Error::other(format!("invalid field number: {field_number}")))?;

        if !info.has_norms() {
            return Err(io::Error::other(format!(
                "invalid field: {} (no norms)",
                info.name()
            )));
        }

        let docs_with_field_offset = meta.read_le_long()?;
        let docs_with_field_length = meta.read_le_long()?;
        let jump_table_entry_count = meta.read_le_short()?;
        let dense_rank_power = meta.read_byte()?;
        let num_docs_with_field = meta.read_le_int()?;
        let bytes_per_norm = meta.read_byte()?;

        match bytes_per_norm {
            0 | 1 | 2 | 4 | 8 => {}
            _ => {
                return Err(io::Error::other(format!(
                    "invalid bytesPerNorm: {bytes_per_norm}, field: {}",
                    info.name()
                )));
            }
        }

        let norms_offset = meta.read_le_long()?;

        entries[field_number as usize] = Some(NormsEntry {
            docs_with_field_offset,
            docs_with_field_length,
            jump_table_entry_count,
            dense_rank_power,
            num_docs_with_field,
            bytes_per_norm,
            norms_offset,
        });
    }

    Ok(entries.into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::lucene90::norms;
    use crate::document::{DocValuesType, IndexOptions};
    use crate::index::indexing_chain::PerFieldData;
    use crate::index::{FieldInfo, FieldInfos};
    use crate::store::{MemoryDirectory, SharedDirectory};
    use assertables::*;
    use std::collections::HashMap;

    fn make_field_info(name: &str, number: u32, has_norms: bool) -> FieldInfo {
        crate::test_util::make_field_info(
            name,
            number,
            !has_norms,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::None,
        )
    }

    fn make_per_field_data(norms_vals: Vec<i64>, norms_docs: Vec<i32>) -> PerFieldData {
        let mut pfd = PerFieldData::new();
        pfd.norms = norms_vals;
        pfd.norms_docs = norms_docs;
        pfd
    }

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    /// Writes norms and opens a reader, returning the reader.
    fn write_and_read(
        field_infos: &FieldInfos,
        per_field: &HashMap<String, PerFieldData>,
        num_docs: i32,
    ) -> NormsReader {
        let segment_id = [0u8; 16];
        let dir = test_directory();
        norms::write(
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
        NormsReader::open(guard.as_ref(), "_0", "", &segment_id, field_infos, num_docs).unwrap()
    }

    // Ported from org.apache.lucene.codecs.lucene90.TestLucene90NormsFormat

    #[test]
    fn test_all_constant() {
        // ALL pattern with constant norms (bytes_per_norm=0)
        let fi = make_field_info("contents", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "contents".to_string(),
            make_per_field_data(vec![42, 42, 42], vec![0, 1, 2]),
        );

        let mut reader = write_and_read(&field_infos, &per_field, 3);

        assert_eq!(reader.num_docs_with_field(0), Some(3));

        // All docs return the constant value
        for doc in 0..3 {
            assert_eq!(reader.get(0, doc).unwrap(), Some(42), "doc {doc}");
        }
    }

    #[test]
    fn test_all_variable_1byte() {
        // ALL pattern with 1-byte variable norms
        let fi = make_field_info("contents", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "contents".to_string(),
            make_per_field_data(vec![12, 8, 10], vec![0, 1, 2]),
        );

        let mut reader = write_and_read(&field_infos, &per_field, 3);

        assert_eq!(reader.num_docs_with_field(0), Some(3));
        assert_eq!(reader.get(0, 0).unwrap(), Some(12));
        assert_eq!(reader.get(0, 1).unwrap(), Some(8));
        assert_eq!(reader.get(0, 2).unwrap(), Some(10));
    }

    #[test]
    fn test_all_variable_2byte() {
        // ALL pattern with 2-byte norms (values outside i8 range)
        let fi = make_field_info("f", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "f".to_string(),
            make_per_field_data(vec![1000, -500, 32000], vec![0, 1, 2]),
        );

        let mut reader = write_and_read(&field_infos, &per_field, 3);

        assert_eq!(reader.get(0, 0).unwrap(), Some(1000));
        assert_eq!(reader.get(0, 1).unwrap(), Some(-500));
        assert_eq!(reader.get(0, 2).unwrap(), Some(32000));
    }

    #[test]
    fn test_all_variable_4byte() {
        // ALL pattern with 4-byte norms
        let fi = make_field_info("f", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "f".to_string(),
            make_per_field_data(vec![100_000, -100_000], vec![0, 1]),
        );

        let mut reader = write_and_read(&field_infos, &per_field, 2);

        assert_eq!(reader.get(0, 0).unwrap(), Some(100_000));
        assert_eq!(reader.get(0, 1).unwrap(), Some(-100_000));
    }

    #[test]
    fn test_all_variable_8byte() {
        // ALL pattern with 8-byte norms
        let fi = make_field_info("f", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "f".to_string(),
            make_per_field_data(vec![i64::MAX, i64::MIN], vec![0, 1]),
        );

        let mut reader = write_and_read(&field_infos, &per_field, 2);

        assert_eq!(reader.get(0, 0).unwrap(), Some(i64::MAX));
        assert_eq!(reader.get(0, 1).unwrap(), Some(i64::MIN));
    }

    #[test]
    fn test_empty_pattern() {
        // EMPTY: field has norms but no documents contributed
        let fi = make_field_info("contents", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert("contents".to_string(), make_per_field_data(vec![], vec![]));

        let mut reader = write_and_read(&field_infos, &per_field, 3);

        assert_eq!(reader.num_docs_with_field(0), Some(0));
        assert_none!(reader.get(0, 0).unwrap());
        assert_none!(reader.get(0, 1).unwrap());
    }

    #[test]
    fn test_sparse_variable() {
        // SPARSE: 2 of 5 docs have norms
        let fi = make_field_info("contents", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "contents".to_string(),
            make_per_field_data(vec![12, 8], vec![1, 3]),
        );

        let mut reader = write_and_read(&field_infos, &per_field, 5);

        assert_eq!(reader.num_docs_with_field(0), Some(2));
        assert_none!(reader.get(0, 0).unwrap());
        assert_eq!(reader.get(0, 1).unwrap(), Some(12));
        assert_none!(reader.get(0, 2).unwrap());
        assert_eq!(reader.get(0, 3).unwrap(), Some(8));
        assert_none!(reader.get(0, 4).unwrap());
    }

    #[test]
    fn test_sparse_constant() {
        // SPARSE with constant value: 3 of 5 docs, all same norm
        let fi = make_field_info("title", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "title".to_string(),
            make_per_field_data(vec![42, 42, 42], vec![0, 2, 4]),
        );

        let mut reader = write_and_read(&field_infos, &per_field, 5);

        assert_eq!(reader.num_docs_with_field(0), Some(3));
        assert_eq!(reader.get(0, 0).unwrap(), Some(42));
        assert_none!(reader.get(0, 1).unwrap());
        assert_eq!(reader.get(0, 2).unwrap(), Some(42));
        assert_none!(reader.get(0, 3).unwrap());
        assert_eq!(reader.get(0, 4).unwrap(), Some(42));
    }

    #[test]
    fn test_multiple_fields_mixed_patterns() {
        // Field 0: ALL constant, Field 1: SPARSE variable
        let fi_a = make_field_info("alpha", 0, true);
        let fi_b = make_field_info("beta", 1, true);
        let field_infos = FieldInfos::new(vec![fi_a, fi_b]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "alpha".to_string(),
            make_per_field_data(vec![5, 5, 5], vec![0, 1, 2]),
        );
        per_field.insert(
            "beta".to_string(),
            make_per_field_data(vec![10, 20], vec![0, 2]),
        );

        let mut reader = write_and_read(&field_infos, &per_field, 3);

        // alpha: ALL constant
        assert_eq!(reader.num_docs_with_field(0), Some(3));
        assert_eq!(reader.get(0, 0).unwrap(), Some(5));
        assert_eq!(reader.get(0, 1).unwrap(), Some(5));
        assert_eq!(reader.get(0, 2).unwrap(), Some(5));

        // beta: SPARSE variable
        assert_eq!(reader.num_docs_with_field(1), Some(2));
        assert_eq!(reader.get(1, 0).unwrap(), Some(10));
        assert_none!(reader.get(1, 1).unwrap());
        assert_eq!(reader.get(1, 2).unwrap(), Some(20));
    }

    #[test]
    fn test_nonexistent_field() {
        let fi = make_field_info("contents", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "contents".to_string(),
            make_per_field_data(vec![10], vec![0]),
        );

        let mut reader = write_and_read(&field_infos, &per_field, 1);

        assert_none!(reader.num_docs_with_field(99));
        assert_none!(reader.get(99, 0).unwrap());
    }

    #[test]
    fn test_negative_norm_values() {
        // Norms can be negative (signed byte range)
        let fi = make_field_info("f", 0, true);
        let field_infos = FieldInfos::new(vec![fi]);

        let mut per_field = HashMap::new();
        per_field.insert(
            "f".to_string(),
            make_per_field_data(vec![-128, -1, 0, 127], vec![0, 1, 2, 3]),
        );

        let mut reader = write_and_read(&field_infos, &per_field, 4);

        assert_eq!(reader.get(0, 0).unwrap(), Some(-128));
        assert_eq!(reader.get(0, 1).unwrap(), Some(-1));
        assert_eq!(reader.get(0, 2).unwrap(), Some(0));
        assert_eq!(reader.get(0, 3).unwrap(), Some(127));
    }
}
