// SPDX-License-Identifier: Apache-2.0

//! Norms reader for the Lucene90 norms format.
//!
//! Reads `.nvm` (metadata) and `.nvd` (data) files written by the norms writer.
//! Metadata is read eagerly during construction; norm values are read lazily from
//! the `.nvd` data file on demand.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::lucene90::indexed_disi::IndexedDISI;
use crate::codecs::lucene90::norms::{
    DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION, VERSION,
};
use crate::index::numeric_doc_values::NumericDocValues;
use crate::index::{FieldInfos, index_file_names};
use crate::store::checksum_input::ChecksumIndexInput;
use crate::store::{DataInput, Directory, IndexInput, RandomAccessInput};

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
/// open for lazy value reads via [`get_norms`](Self::get_norms).
pub struct NormsProducer {
    /// Per-field metadata indexed by field number. `None` for fields without norms.
    entries: Box<[Option<NormsEntry>]>,
    /// Maximum document ID + 1 for this segment.
    max_doc: i32,
    /// Open handle to the `.nvd` data file.
    data: Box<dyn IndexInput>,
}

impl NormsProducer {
    /// Opens norms files (`.nvm`, `.nvd`) for the given segment.
    ///
    /// Reads all metadata eagerly from `.nvm`; the `.nvd` data file is opened but
    /// no data is read until [`get_norms`](Self::get_norms) is called.
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

        codec_util::retrieve_checksum(data.as_mut())?;

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

    /// Returns a [`NumericDocValues`] for the given field, or `None` if the field
    /// has no norms (EMPTY pattern or no entry).
    ///
    /// For dense norms (all docs have a value), the returned iterator does a single
    /// random-access read per `long_value()` call — no seeking. For sparse norms,
    /// it wraps an [`IndexedDISI`] for presence checks.
    pub fn get_norms(
        &self,
        field_info: &crate::index::FieldInfo,
    ) -> io::Result<Option<Box<dyn NumericDocValues>>> {
        let entry = match self.entry(field_info.number()) {
            Some(e) => e,
            None => return Ok(None),
        };

        if entry.docs_with_field_offset == -2 {
            // EMPTY: no documents have norms for this field
            return Ok(None);
        } else if entry.docs_with_field_offset == -1 {
            // DENSE: every document has a norm
            if entry.bytes_per_norm == 0 {
                return Ok(Some(Box::new(DenseNormsIterator {
                    doc: -1,
                    max_doc: self.max_doc,
                    slice: None,
                    bytes_per_norm: 0,
                    constant_value: entry.norms_offset,
                })));
            }
            let data_length = entry.num_docs_with_field as u64 * entry.bytes_per_norm as u64;
            let slice_input =
                self.data
                    .slice("norms data", entry.norms_offset as u64, data_length)?;
            let slice = slice_input.random_access()?;
            return Ok(Some(Box::new(DenseNormsIterator {
                doc: -1,
                max_doc: self.max_doc,
                slice: Some(slice),
                bytes_per_norm: entry.bytes_per_norm,
                constant_value: 0,
            })));
        }

        // SPARSE: use IndexedDISI to check presence and get ordinal
        let disi = IndexedDISI::new(
            self.data.as_ref(),
            entry.docs_with_field_offset,
            entry.docs_with_field_length,
            entry.jump_table_entry_count,
            entry.dense_rank_power,
            entry.num_docs_with_field as i64,
        )?;

        if entry.bytes_per_norm == 0 {
            return Ok(Some(Box::new(SparseNormsIterator {
                disi,
                slice: None,
                bytes_per_norm: 0,
                constant_value: entry.norms_offset,
            })));
        }
        let data_length = entry.num_docs_with_field as u64 * entry.bytes_per_norm as u64;
        let slice_input = self
            .data
            .slice("norms data", entry.norms_offset as u64, data_length)?;
        let slice = slice_input.random_access()?;
        Ok(Some(Box::new(SparseNormsIterator {
            disi,
            slice: Some(slice),
            bytes_per_norm: entry.bytes_per_norm,
            constant_value: 0,
        })))
    }

    /// Returns the norms entry for a field number, or `None` if absent.
    fn entry(&self, field_number: u32) -> Option<&NormsEntry> {
        self.entries
            .get(field_number as usize)
            .and_then(|opt| opt.as_ref())
    }
}

// ---------------------------------------------------------------------------
// DenseNormsIterator — all documents have a norm value
// ---------------------------------------------------------------------------

/// Dense norms iterator where every document has a norm value.
///
/// For `bytes_per_norm > 0`, reads values via random access into the `.nvd`
/// data slice. For `bytes_per_norm == 0`, returns a constant value.
struct DenseNormsIterator {
    doc: i32,
    #[expect(dead_code)]
    max_doc: i32,
    slice: Option<Box<dyn RandomAccessInput>>,
    bytes_per_norm: u8,
    constant_value: i64,
}

impl NumericDocValues for DenseNormsIterator {
    fn advance_exact(&mut self, target: i32) -> io::Result<bool> {
        self.doc = target;
        Ok(true)
    }

    fn long_value(&self) -> io::Result<i64> {
        if self.bytes_per_norm == 0 {
            return Ok(self.constant_value);
        }
        let slice = self.slice.as_ref().unwrap();
        match self.bytes_per_norm {
            1 => Ok(slice.read_byte_at(self.doc as u64)? as i8 as i64),
            2 => Ok(slice.read_le_short_at((self.doc as u64) << 1)? as i64),
            4 => Ok(slice.read_le_int_at((self.doc as u64) << 2)? as i64),
            8 => slice.read_le_long_at((self.doc as u64) << 3),
            _ => Err(io::Error::other(format!(
                "invalid bytes_per_norm: {}",
                self.bytes_per_norm
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// SparseNormsIterator — only some documents have a norm value
// ---------------------------------------------------------------------------

/// Sparse norms iterator backed by an [`IndexedDISI`].
///
/// `advance_exact` delegates to the DISI to check document presence.
/// `long_value` reads from the data slice at the DISI's ordinal index.
struct SparseNormsIterator {
    disi: IndexedDISI,
    slice: Option<Box<dyn RandomAccessInput>>,
    bytes_per_norm: u8,
    constant_value: i64,
}

impl NumericDocValues for SparseNormsIterator {
    fn advance_exact(&mut self, target: i32) -> io::Result<bool> {
        self.disi.advance_exact(target)
    }

    fn long_value(&self) -> io::Result<i64> {
        if self.bytes_per_norm == 0 {
            return Ok(self.constant_value);
        }
        let index = self.disi.index() as u64;
        let slice = self.slice.as_ref().unwrap();
        match self.bytes_per_norm {
            1 => Ok(slice.read_byte_at(index)? as i8 as i64),
            2 => Ok(slice.read_le_short_at(index << 1)? as i64),
            4 => Ok(slice.read_le_int_at(index << 2)? as i64),
            8 => slice.read_le_long_at(index << 3),
            _ => Err(io::Error::other(format!(
                "invalid bytes_per_norm: {}",
                self.bytes_per_norm
            ))),
        }
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
    use crate::codecs::lucene90::norms::{self, NormsFieldData};
    use crate::document::{DocValuesType, IndexOptions};
    use crate::index::{FieldInfo, FieldInfos};
    use crate::store::{MemoryDirectory, SharedDirectory};
    use assertables::*;

    fn make_field_info(name: &str, number: u32, has_norms: bool) -> FieldInfo {
        crate::test_util::make_field_info(
            name,
            number,
            !has_norms,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::None,
        )
    }

    fn make_norms_field(
        name: &str,
        number: u32,
        norms_vals: Vec<i64>,
        norms_docs: Vec<i32>,
    ) -> NormsFieldData {
        NormsFieldData {
            field_name: name.to_string(),
            field_number: number,
            norms: norms_vals,
            docs: norms_docs,
        }
    }

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    /// Writes norms and opens a reader, returning the reader.
    fn write_and_read(
        fields: &[NormsFieldData],
        field_infos: &FieldInfos,
        num_docs: i32,
    ) -> NormsProducer {
        let segment_id = [0u8; 16];
        let dir = test_directory();
        norms::write(&dir, "_0", "", &segment_id, fields, num_docs).unwrap();
        let guard = dir.lock().unwrap();
        NormsProducer::open(guard.as_ref(), "_0", "", &segment_id, field_infos, num_docs).unwrap()
    }

    /// Helper: get a single norm value using get_norms + advance_exact + long_value.
    fn get_norm(reader: &NormsProducer, fi: &FieldInfo, doc_id: i32) -> io::Result<Option<i64>> {
        let mut norms = match reader.get_norms(fi)? {
            Some(n) => n,
            None => return Ok(None),
        };
        if norms.advance_exact(doc_id)? {
            Ok(Some(norms.long_value()?))
        } else {
            Ok(None)
        }
    }

    // Ported from org.apache.lucene.codecs.lucene90.TestLucene90NormsFormat

    #[test]
    fn test_all_constant() {
        // ALL pattern with constant norms (bytes_per_norm=0)
        let fields = vec![make_norms_field(
            "contents",
            0,
            vec![42, 42, 42],
            vec![0, 1, 2],
        )];
        let field_infos = FieldInfos::new(vec![make_field_info("contents", 0, true)]);

        let fi = field_infos.field_info_by_number(0).unwrap();
        let reader = write_and_read(&fields, &field_infos, 3);

        assert_eq!(reader.num_docs_with_field(0), Some(3));

        // All docs return the constant value
        for doc in 0..3 {
            assert_eq!(get_norm(&reader, fi, doc).unwrap(), Some(42), "doc {doc}");
        }
    }

    #[test]
    fn test_all_variable_1byte() {
        // ALL pattern with 1-byte variable norms
        let fields = vec![make_norms_field(
            "contents",
            0,
            vec![12, 8, 10],
            vec![0, 1, 2],
        )];
        let field_infos = FieldInfos::new(vec![make_field_info("contents", 0, true)]);

        let fi = field_infos.field_info_by_number(0).unwrap();
        let reader = write_and_read(&fields, &field_infos, 3);

        assert_eq!(reader.num_docs_with_field(0), Some(3));
        assert_eq!(get_norm(&reader, fi, 0).unwrap(), Some(12));
        assert_eq!(get_norm(&reader, fi, 1).unwrap(), Some(8));
        assert_eq!(get_norm(&reader, fi, 2).unwrap(), Some(10));
    }

    #[test]
    fn test_all_variable_2byte() {
        // ALL pattern with 2-byte norms (values outside i8 range)
        let fields = vec![make_norms_field(
            "f",
            0,
            vec![1000, -500, 32000],
            vec![0, 1, 2],
        )];
        let field_infos = FieldInfos::new(vec![make_field_info("f", 0, true)]);

        let fi = field_infos.field_info_by_number(0).unwrap();
        let reader = write_and_read(&fields, &field_infos, 3);

        assert_eq!(get_norm(&reader, fi, 0).unwrap(), Some(1000));
        assert_eq!(get_norm(&reader, fi, 1).unwrap(), Some(-500));
        assert_eq!(get_norm(&reader, fi, 2).unwrap(), Some(32000));
    }

    #[test]
    fn test_all_variable_4byte() {
        // ALL pattern with 4-byte norms
        let fields = vec![make_norms_field(
            "f",
            0,
            vec![100_000, -100_000],
            vec![0, 1],
        )];
        let field_infos = FieldInfos::new(vec![make_field_info("f", 0, true)]);

        let fi = field_infos.field_info_by_number(0).unwrap();
        let reader = write_and_read(&fields, &field_infos, 2);

        assert_eq!(get_norm(&reader, fi, 0).unwrap(), Some(100_000));
        assert_eq!(get_norm(&reader, fi, 1).unwrap(), Some(-100_000));
    }

    #[test]
    fn test_all_variable_8byte() {
        // ALL pattern with 8-byte norms
        let fields = vec![make_norms_field(
            "f",
            0,
            vec![i64::MAX, i64::MIN],
            vec![0, 1],
        )];
        let field_infos = FieldInfos::new(vec![make_field_info("f", 0, true)]);

        let fi = field_infos.field_info_by_number(0).unwrap();
        let reader = write_and_read(&fields, &field_infos, 2);

        assert_eq!(get_norm(&reader, fi, 0).unwrap(), Some(i64::MAX));
        assert_eq!(get_norm(&reader, fi, 1).unwrap(), Some(i64::MIN));
    }

    #[test]
    fn test_empty_pattern() {
        // EMPTY: field has norms but no documents contributed
        let fields = vec![make_norms_field("contents", 0, vec![], vec![])];
        let field_infos = FieldInfos::new(vec![make_field_info("contents", 0, true)]);

        let fi = field_infos.field_info_by_number(0).unwrap();
        let reader = write_and_read(&fields, &field_infos, 3);

        assert_eq!(reader.num_docs_with_field(0), Some(0));
        assert_none!(get_norm(&reader, fi, 0).unwrap());
        assert_none!(get_norm(&reader, fi, 1).unwrap());
    }

    #[test]
    fn test_sparse_variable() {
        // SPARSE: 2 of 5 docs have norms
        let fields = vec![make_norms_field("contents", 0, vec![12, 8], vec![1, 3])];
        let field_infos = FieldInfos::new(vec![make_field_info("contents", 0, true)]);

        let fi = field_infos.field_info_by_number(0).unwrap();
        let reader = write_and_read(&fields, &field_infos, 5);

        assert_eq!(reader.num_docs_with_field(0), Some(2));
        assert_none!(get_norm(&reader, fi, 0).unwrap());
        assert_eq!(get_norm(&reader, fi, 1).unwrap(), Some(12));
        assert_none!(get_norm(&reader, fi, 2).unwrap());
        assert_eq!(get_norm(&reader, fi, 3).unwrap(), Some(8));
        assert_none!(get_norm(&reader, fi, 4).unwrap());
    }

    #[test]
    fn test_sparse_constant() {
        // SPARSE with constant value: 3 of 5 docs, all same norm
        let fields = vec![make_norms_field(
            "title",
            0,
            vec![42, 42, 42],
            vec![0, 2, 4],
        )];
        let field_infos = FieldInfos::new(vec![make_field_info("title", 0, true)]);

        let fi = field_infos.field_info_by_number(0).unwrap();
        let reader = write_and_read(&fields, &field_infos, 5);

        assert_eq!(reader.num_docs_with_field(0), Some(3));
        assert_eq!(get_norm(&reader, fi, 0).unwrap(), Some(42));
        assert_none!(get_norm(&reader, fi, 1).unwrap());
        assert_eq!(get_norm(&reader, fi, 2).unwrap(), Some(42));
        assert_none!(get_norm(&reader, fi, 3).unwrap());
        assert_eq!(get_norm(&reader, fi, 4).unwrap(), Some(42));
    }

    #[test]
    fn test_multiple_fields_mixed_patterns() {
        // Field 0: ALL constant, Field 1: SPARSE variable
        let fields = vec![
            make_norms_field("alpha", 0, vec![5, 5, 5], vec![0, 1, 2]),
            make_norms_field("beta", 1, vec![10, 20], vec![0, 2]),
        ];
        let field_infos = FieldInfos::new(vec![
            make_field_info("alpha", 0, true),
            make_field_info("beta", 1, true),
        ]);

        let fi = field_infos.field_info_by_number(0).unwrap();
        let fi1 = field_infos.field_info_by_number(1).unwrap();
        let reader = write_and_read(&fields, &field_infos, 3);

        // alpha: ALL constant
        assert_eq!(reader.num_docs_with_field(0), Some(3));
        assert_eq!(get_norm(&reader, fi, 0).unwrap(), Some(5));
        assert_eq!(get_norm(&reader, fi, 1).unwrap(), Some(5));
        assert_eq!(get_norm(&reader, fi, 2).unwrap(), Some(5));

        // beta: SPARSE variable
        assert_eq!(reader.num_docs_with_field(1), Some(2));
        assert_eq!(get_norm(&reader, fi1, 0).unwrap(), Some(10));
        assert_none!(get_norm(&reader, fi1, 1).unwrap());
        assert_eq!(get_norm(&reader, fi1, 2).unwrap(), Some(20));
    }

    #[test]
    fn test_nonexistent_field() {
        let fields = vec![make_norms_field("contents", 0, vec![10], vec![0])];
        let field_infos = FieldInfos::new(vec![make_field_info("contents", 0, true)]);

        let reader = write_and_read(&fields, &field_infos, 1);

        assert_none!(reader.num_docs_with_field(99));
    }

    #[test]
    fn test_negative_norm_values() {
        // Norms can be negative (signed byte range)
        let fields = vec![make_norms_field(
            "f",
            0,
            vec![-128, -1, 0, 127],
            vec![0, 1, 2, 3],
        )];
        let field_infos = FieldInfos::new(vec![make_field_info("f", 0, true)]);

        let fi = field_infos.field_info_by_number(0).unwrap();
        let reader = write_and_read(&fields, &field_infos, 4);

        assert_eq!(get_norm(&reader, fi, 0).unwrap(), Some(-128));
        assert_eq!(get_norm(&reader, fi, 1).unwrap(), Some(-1));
        assert_eq!(get_norm(&reader, fi, 2).unwrap(), Some(0));
        assert_eq!(get_norm(&reader, fi, 3).unwrap(), Some(127));
    }
}
