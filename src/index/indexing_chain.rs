// SPDX-License-Identifier: Apache-2.0
//! Indexing chain that processes document fields into postings, stored fields, and doc values.

use std::collections::HashMap;
use std::io;

use mem_dbg::MemSize;

use crate::analysis::{Analyzer, TokenRef};
use crate::document::{DocValuesType, Document, Field, FieldValue, IndexOptions, StoredValue};
use crate::index::{FieldInfo, FieldInfos, PointDimensionConfig};
use crate::util::BytesRef;

/// Lightweight summary of a field's metadata, extracted from FieldInfo.
/// Used to avoid cloning the full FieldInfo (which contains a HashMap) on every field visit.
#[derive(Clone, Copy, Debug)]
struct FieldMeta {
    number: u32,
    index_options: IndexOptions,
    doc_values_type: DocValuesType,
    omit_norms: bool,
    has_point_values: bool,
}

impl From<&FieldInfo> for FieldMeta {
    fn from(fi: &FieldInfo) -> Self {
        Self {
            number: fi.number(),
            index_options: fi.index_options(),
            doc_values_type: fi.doc_values_type(),
            omit_norms: fi.omit_norms(),
            has_point_values: fi.has_point_values(),
        }
    }
}

/// Writes a variable-length integer (1-5 bytes) into a `Vec<u8>`.
/// High bit = continuation. Mirrors `DataOutput::write_vint`.
fn write_vint(buf: &mut Vec<u8>, val: i32) {
    let mut v = val as u32;
    while (v & !0x7F) != 0 {
        buf.push(((v & 0x7F) | 0x80) as u8);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Reads a variable-length integer from a byte slice at the given offset.
/// Advances `offset` past the consumed bytes.
fn read_vint(data: &[u8], offset: &mut usize) -> i32 {
    let mut result = 0i32;
    let mut shift = 0;
    loop {
        let b = data[*offset] as i32;
        *offset += 1;
        result |= (b & 0x7F) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

/// A decoded posting, produced by `PostingList::decode()` at flush time.
#[derive(Clone, Debug)]
pub struct DecodedPosting {
    pub doc_id: i32,
    pub freq: i32,
    pub positions: Vec<i32>,
}

/// Start and end offset buffers for offset-indexed fields.
///
/// Boxed and optional in `PostingList` to avoid 48 bytes of Vec overhead
/// per term in the common case where offsets are not indexed.
#[derive(Clone, Debug, MemSize)]
struct OffsetBuffers {
    start_offsets: Vec<i32>,
    end_offsets: Vec<i32>,
}

/// Per-term posting list across all documents.
///
/// Stores finalized document postings as a compact vInt-encoded byte stream,
/// using ~3-5 bytes per posting instead of ~80 bytes with the old Vec<Posting>
/// approach. Only the current in-progress document uses temporary Vecs.
#[derive(Clone, Debug, MemSize)]
pub struct PostingList {
    /// Compact storage: all finalized docs encoded as vInt byte stream.
    byte_stream: Vec<u8>,
    total_term_freq: i64,
    doc_freq: i32,
    /// Last doc_id written to byte_stream (for delta encoding).
    last_doc_id: i32,
    /// Current in-progress document's doc_id (-1 if none pending).
    current_doc_id: i32,
    current_freq: i32,
    current_positions: Vec<i32>,
    current_offsets: Option<Box<OffsetBuffers>>,
    /// Field's index options (needed for encoding decisions).
    has_freqs: bool,
    has_positions: bool,
    has_offsets: bool,
}

impl PostingList {
    /// Creates a new empty posting list with the given index options.
    pub fn new(has_freqs: bool, has_positions: bool, has_offsets: bool) -> Self {
        Self {
            byte_stream: Vec::new(),
            total_term_freq: 0,
            doc_freq: 0,
            last_doc_id: 0,
            current_doc_id: -1,
            current_freq: 0,
            current_positions: Vec::new(),
            current_offsets: if has_offsets {
                Some(Box::new(OffsetBuffers {
                    start_offsets: Vec::new(),
                    end_offsets: Vec::new(),
                }))
            } else {
                None
            },
            has_freqs,
            has_positions,
            has_offsets,
        }
    }

    /// Finalizes the previous pending document (if any) and starts a new one.
    pub fn start_doc(&mut self, doc_id: i32) {
        if self.current_doc_id >= 0 {
            self.finalize_current_doc();
        }
        self.current_doc_id = doc_id;
        self.current_freq = 1;
        self.doc_freq += 1;
    }

    /// Increments the frequency for the current in-progress document.
    pub fn increment_freq(&mut self) {
        self.current_freq += 1;
    }

    /// Records a position for the current document.
    pub fn add_position(&mut self, position: i32) {
        self.current_positions.push(position);
    }

    /// Records an offset pair for the current document.
    pub fn add_offset(&mut self, start: i32, end: i32) {
        let offsets = self
            .current_offsets
            .as_mut()
            .expect("add_offset called on non-offset field");
        offsets.start_offsets.push(start);
        offsets.end_offsets.push(end);
    }

    /// Records a token occurrence for the given document: starts a new doc
    /// or increments frequency, and records position/offset data as configured.
    #[inline]
    pub fn record_occurrence(
        &mut self,
        doc_id: i32,
        position: i32,
        start_offset: i32,
        end_offset: i32,
    ) {
        if self.current_doc_id == doc_id {
            if self.has_freqs {
                self.increment_freq();
            }
            if self.has_positions {
                self.add_position(position);
            }
            if self.has_offsets {
                self.add_offset(start_offset, end_offset);
            }
        } else {
            self.start_doc(doc_id);
            if self.has_positions {
                self.add_position(position);
            }
            if self.has_offsets {
                self.add_offset(start_offset, end_offset);
            }
        }
    }

    /// Encodes the current pending document into the byte_stream and clears
    /// the temporary state. Reuses the temp Vec capacity across documents.
    pub fn finalize_current_doc(&mut self) {
        if self.current_doc_id < 0 {
            return;
        }

        self.total_term_freq += self.current_freq as i64;

        // doc_id delta
        let delta = self.current_doc_id - self.last_doc_id;
        write_vint(&mut self.byte_stream, delta);

        // freq (if field has freqs)
        if self.has_freqs {
            write_vint(&mut self.byte_stream, self.current_freq);
        }

        // positions + optional offsets
        if self.has_positions {
            let offsets = self.current_offsets.as_ref();
            let mut last_pos = 0;
            for (i, &pos) in self.current_positions.iter().enumerate() {
                let pos_delta = pos - last_pos;
                write_vint(&mut self.byte_stream, pos_delta);
                last_pos = pos;

                if let Some(ob) = offsets {
                    write_vint(&mut self.byte_stream, ob.start_offsets[i]);
                    write_vint(&mut self.byte_stream, ob.end_offsets[i]);
                }
            }
        }

        self.last_doc_id = self.current_doc_id;
        self.current_doc_id = -1;
        self.current_freq = 0;
        self.current_positions.clear();
        if let Some(ob) = self.current_offsets.as_mut() {
            ob.start_offsets.clear();
            ob.end_offsets.clear();
        }
    }

    /// Decodes the entire byte stream into a Vec of DecodedPosting.
    /// Used at flush time when the codec writers need structured access.
    pub fn decode(&self) -> Vec<DecodedPosting> {
        let mut result = Vec::with_capacity(self.doc_freq as usize);
        let mut offset = 0;
        let mut last_doc_id = 0;

        while offset < self.byte_stream.len() {
            let doc_delta = read_vint(&self.byte_stream, &mut offset);
            let doc_id = last_doc_id + doc_delta;
            last_doc_id = doc_id;

            let freq = if self.has_freqs {
                read_vint(&self.byte_stream, &mut offset)
            } else {
                1
            };

            let mut positions = if self.has_positions {
                Vec::with_capacity(freq as usize)
            } else {
                Vec::new()
            };
            if self.has_positions {
                let mut last_pos = 0;
                for _ in 0..freq {
                    let pos_delta = read_vint(&self.byte_stream, &mut offset);
                    let pos = last_pos + pos_delta;
                    positions.push(pos);
                    last_pos = pos;

                    if self.has_offsets {
                        // Consume offset data from the byte stream (not exposed in DecodedPosting)
                        read_vint(&self.byte_stream, &mut offset);
                        read_vint(&self.byte_stream, &mut offset);
                    }
                }
            }

            result.push(DecodedPosting {
                doc_id,
                freq,
                positions,
            });
        }

        result
    }
}

/// Per-field accumulated data from all documents.
///
/// Postings are stored as a separate term-id lookup (`term_ids`) and a
/// contiguous `Vec<PostingList>` indexed by term id. During HashMap rehash
/// only the lightweight `(String, u32)` entries are copied (~29 bytes each)
/// instead of the full `(String, PostingList)` entries (~145+ bytes each).
#[derive(Clone, Debug, MemSize)]
pub struct PerFieldData {
    /// Term string -> index into `posting_lists`.
    term_ids: HashMap<String, u32>,
    /// PostingLists indexed by term id.
    posting_lists: Vec<PostingList>,
    /// Doc values accumulated per document.
    pub doc_values: DocValuesAccumulator,
    /// Norm values per document (only for fields with norms).
    pub norms: Vec<i64>,
    /// Docs that have a norm value for this field.
    pub norms_docs: Vec<i32>,
    /// Point values per document (only for point fields).
    pub points: Vec<(i32, Vec<u8>)>,
}

impl PerFieldData {
    /// Creates a new empty `PerFieldData`.
    pub fn new() -> Self {
        Self {
            term_ids: HashMap::new(),
            posting_lists: Vec::new(),
            doc_values: DocValuesAccumulator::None,
            norms: Vec::new(),
            norms_docs: Vec::new(),
            points: Vec::new(),
        }
    }

    /// Looks up or inserts a term, returning `&mut PostingList`.
    fn get_or_insert_posting_list(
        &mut self,
        term: &str,
        has_freqs: bool,
        has_positions: bool,
        has_offsets: bool,
    ) -> &mut PostingList {
        let posting_lists = &mut self.posting_lists;
        let id = *self.term_ids.entry(term.to_string()).or_insert_with(|| {
            let id = posting_lists.len() as u32;
            posting_lists.push(PostingList::new(has_freqs, has_positions, has_offsets));
            id
        });
        &mut self.posting_lists[id as usize]
    }

    /// Returns terms and posting lists in byte-sorted order for codec writing.
    pub fn sorted_postings(&self) -> Vec<(&str, &PostingList)> {
        let mut pairs: Vec<(&str, &PostingList)> = self
            .term_ids
            .iter()
            .map(|(term, &id)| (term.as_str(), &self.posting_lists[id as usize]))
            .collect();
        pairs.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
        pairs
    }

    /// Returns true if this field has any postings.
    pub fn has_postings(&self) -> bool {
        !self.term_ids.is_empty()
    }
}

#[cfg(test)]
impl PerFieldData {
    /// Number of unique terms.
    pub fn num_terms(&self) -> usize {
        self.term_ids.len()
    }

    /// Looks up a posting list by term.
    pub fn posting_list(&self, term: &str) -> Option<&PostingList> {
        self.term_ids
            .get(term)
            .map(|&id| &self.posting_lists[id as usize])
    }
}

/// Accumulated doc values, type-specific.
#[derive(Clone, Debug, MemSize)]
pub enum DocValuesAccumulator {
    None,
    /// NUMERIC: single long value per doc.
    Numeric(Vec<(i32, i64)>),
    /// BINARY: per-doc byte array.
    Binary(Vec<(i32, Vec<u8>)>),
    /// SORTED: single byte[] value per doc (ordinal-mapped).
    Sorted(Vec<(i32, BytesRef)>),
    /// SORTED_NUMERIC: per-doc list of long values.
    SortedNumeric(Vec<(i32, Vec<i64>)>),
    /// SORTED_SET: per-doc list of byte[] values (terms).
    SortedSet(Vec<(i32, Vec<BytesRef>)>),
}

/// A stored field for a single document.
#[derive(Clone, Debug, MemSize)]
pub struct StoredDoc {
    pub fields: Vec<(u32, StoredValue)>, // (field_number, value)
}

/// The indexing chain: processes documents into in-memory data structures.
///
/// Simplified for single-threaded, single-segment indexing.
#[derive(MemSize)]
pub struct IndexingChain {
    /// Per-field accumulated data, keyed by field name.
    per_field: HashMap<String, PerFieldData>,
    /// Stored fields per document, in doc order.
    stored_docs: Vec<StoredDoc>,
    /// Field number assignment counter.
    field_number_counter: u32,
    /// FieldInfo registry: field name -> FieldInfo.
    field_infos: HashMap<String, FieldInfo>,
    /// Number of documents processed.
    num_docs: i32,
    /// Global field name -> number map, shared across segments to ensure
    /// consistent field numbering. Cloned from IndexWriter at chain creation.
    global_field_numbers: HashMap<String, u32>,
    /// Reusable buffer for zero-allocation analyze_to() path.
    lowercase_buf: String,
}

impl Default for IndexingChain {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexingChain {
    pub fn new() -> Self {
        Self {
            per_field: HashMap::new(),
            stored_docs: Vec::new(),
            field_number_counter: 0,
            field_infos: HashMap::new(),
            num_docs: 0,
            global_field_numbers: HashMap::new(),
            lowercase_buf: String::new(),
        }
    }

    /// Creates a new chain with pre-assigned global field numbers.
    ///
    /// Field names present in `global_field_numbers` will always use the
    /// pre-assigned number. New fields will be assigned numbers starting
    /// from `next_field_number`.
    pub fn with_global_field_numbers(
        global_field_numbers: HashMap<String, u32>,
        next_field_number: u32,
    ) -> Self {
        Self {
            per_field: HashMap::new(),
            stored_docs: Vec::new(),
            field_number_counter: next_field_number,
            field_infos: HashMap::new(),
            num_docs: 0,
            global_field_numbers,
            lowercase_buf: String::new(),
        }
    }

    /// Returns the global field number mappings (including any new fields
    /// discovered during document processing).
    pub fn field_number_mappings(&self) -> impl Iterator<Item = (&str, u32)> {
        self.field_infos
            .iter()
            .map(|(name, fi)| (name.as_str(), fi.number()))
    }

    pub fn per_field(&self) -> &HashMap<String, PerFieldData> {
        &self.per_field
    }

    pub fn stored_docs(&self) -> &[StoredDoc] {
        &self.stored_docs
    }

    pub fn num_docs(&self) -> i32 {
        self.num_docs
    }

    /// Finalizes all pending (not yet encoded) postings across all fields.
    /// Must be called before flush so the last document in each posting list
    /// is encoded into the byte stream.
    pub fn finalize_pending_postings(&mut self) {
        for pf in self.per_field.values_mut() {
            for pl in &mut pf.posting_lists {
                pl.finalize_current_doc();
            }
        }
    }

    /// Returns the RAM bytes used by this chain's buffered data.
    ///
    /// Uses `mem_dbg` with `CAPACITY` flag to measure actual allocated memory,
    /// including unused HashMap buckets and Vec capacity. This ensures the
    /// flush policy sees the true memory footprint, not just the used portion.
    pub fn ram_bytes_used(&self) -> usize {
        self.mem_size(mem_dbg::SizeFlags::CAPACITY)
    }

    /// Processes a single document, extracting all field data.
    pub fn process_document(&mut self, doc: Document, analyzer: &dyn Analyzer) -> io::Result<()> {
        let doc_id = self.num_docs;
        let mut stored_fields: Vec<(u32, StoredValue)> = Vec::new();
        // Extract reusable buffer so we can pass &mut to process_indexed_field
        let mut lowercase_buf = std::mem::take(&mut self.lowercase_buf);

        for mut field in doc.fields {
            let meta = self.get_or_create_field_meta(&field);

            // Avoid allocating an owned key on the hot path: get_mut borrows the key,
            // falling back to entry() only on first occurrence of a new field name.
            let per_field = if let Some(pf) = self.per_field.get_mut(field.name()) {
                pf
            } else {
                self.per_field
                    .entry(field.name().to_string())
                    .or_insert_with(PerFieldData::new)
            };

            // Process indexed field (postings)
            if meta.index_options != IndexOptions::None {
                Self::process_indexed_field(
                    per_field,
                    &meta,
                    &mut field,
                    doc_id,
                    analyzer,
                    &mut lowercase_buf,
                )?;
            }

            // Process stored field
            if let Some(stored) = field.stored_value() {
                stored_fields.push((meta.number, stored));
            }

            // Process doc values
            if meta.doc_values_type != DocValuesType::None {
                Self::process_doc_values(per_field, &meta, &field, doc_id);
            }

            // Process point values
            if meta.has_point_values
                && let Some(point_bytes) = field.point_bytes()
            {
                per_field.points.push((doc_id, point_bytes));
            }
        }

        // Restore reusable buffer
        self.lowercase_buf = lowercase_buf;

        self.stored_docs.push(StoredDoc {
            fields: stored_fields,
        });

        self.num_docs += 1;
        Ok(())
    }

    /// Gets or creates a FieldInfo for the given field, returning a lightweight FieldMeta.
    /// On existing fields this copies 5 scalars instead of cloning the entire FieldInfo.
    ///
    /// Uses global field numbers when available to ensure consistent field
    /// numbering across segments.
    fn get_or_create_field_meta(&mut self, field: &Field) -> FieldMeta {
        if let Some(fi) = self.field_infos.get(field.name()) {
            return FieldMeta::from(fi);
        }

        // Check global field numbers first for cross-segment consistency
        let number = if let Some(&num) = self.global_field_numbers.get(field.name()) {
            num
        } else {
            let num = self.field_number_counter;
            self.field_number_counter += 1;
            num
        };

        let ft = field.field_type();
        let point_config = PointDimensionConfig {
            dimension_count: ft.point_dimension_count(),
            index_dimension_count: ft.point_index_dimension_count(),
            num_bytes: ft.point_num_bytes(),
        };

        let mut fi = FieldInfo::new(
            field.name().to_string(),
            number,
            ft.store_term_vectors(),
            ft.omit_norms(),
            ft.index_options(),
            ft.doc_values_type(),
            point_config,
        );

        // Set PerField codec attributes so Java's PerFieldPostingsFormat/PerFieldDocValuesFormat
        // readers know which codec to use for each field.
        if ft.index_options() != IndexOptions::None {
            fi.put_attribute(
                "PerFieldPostingsFormat.format".to_string(),
                "Lucene103".to_string(),
            );
            fi.put_attribute("PerFieldPostingsFormat.suffix".to_string(), "0".to_string());
        }
        if ft.doc_values_type() != DocValuesType::None {
            fi.put_attribute(
                "PerFieldDocValuesFormat.format".to_string(),
                "Lucene90".to_string(),
            );
            fi.put_attribute(
                "PerFieldDocValuesFormat.suffix".to_string(),
                "0".to_string(),
            );
        }

        let meta = FieldMeta::from(&fi);
        self.field_infos.insert(field.name().to_string(), fi);
        meta
    }

    /// Processes an indexed field: tokenize and build posting lists.
    fn process_indexed_field(
        per_field: &mut PerFieldData,
        meta: &FieldMeta,
        field: &mut Field,
        doc_id: i32,
        analyzer: &dyn Analyzer,
        buf: &mut String,
    ) -> io::Result<()> {
        let has_positions = meta.index_options >= IndexOptions::DocsAndFreqsAndPositions;
        let has_offsets = meta.index_options >= IndexOptions::DocsAndFreqsAndPositionsAndOffsets;
        let has_freqs = meta.index_options >= IndexOptions::DocsAndFreqs;

        let mut position: i32 = -1;
        let mut field_length: i32 = 0;

        let mut record_token = |per_field: &mut PerFieldData, token_ref: TokenRef<'_>| {
            position += token_ref.position_increment as i32;
            field_length += 1;

            let posting_list = per_field.get_or_insert_posting_list(
                token_ref.text,
                has_freqs,
                has_positions,
                has_offsets,
            );

            posting_list.record_occurrence(
                doc_id,
                position,
                token_ref.start_offset as i32,
                token_ref.end_offset as i32,
            );
        };

        if field.field_type().tokenized() {
            match field.value() {
                FieldValue::Text(text) => {
                    analyzer.analyze_to(text, buf, &mut |tr| record_token(per_field, tr));
                }
                FieldValue::Reader(_) => {
                    let FieldValue::Reader(mut reader) =
                        std::mem::replace(field.value_mut(), FieldValue::Text(String::new()))
                    else {
                        unreachable!()
                    };
                    analyzer.analyze_reader(&mut *reader, buf, &mut |tr| {
                        record_token(per_field, tr);
                    })?;
                }
                _ => return Ok(()),
            }
        } else {
            let text = match field.value() {
                FieldValue::Text(text) => text,
                _ => return Ok(()),
            };

            // Keyword field: single token with the exact value, no allocation needed
            position += 1;
            field_length = 1;

            let posting_list =
                per_field.get_or_insert_posting_list(text, has_freqs, has_positions, has_offsets);

            posting_list.record_occurrence(doc_id, position, 0, text.len() as i32);
        }

        // Compute and store norms if this field has norms
        // has_norms = is_indexed && !omit_norms; is_indexed is guaranteed here
        if !meta.omit_norms && field_length > 0 {
            let norm = compute_norm(field_length);
            per_field.norms.push(norm);
            per_field.norms_docs.push(doc_id);
        }

        Ok(())
    }

    /// Processes doc values for a field.
    fn process_doc_values(
        per_field: &mut PerFieldData,
        meta: &FieldMeta,
        field: &Field,
        doc_id: i32,
    ) {
        match meta.doc_values_type {
            DocValuesType::Numeric => {
                if let Some(v) = field.numeric_value() {
                    if let DocValuesAccumulator::Numeric(ref mut vals) = per_field.doc_values {
                        vals.push((doc_id, v));
                    } else {
                        per_field.doc_values = DocValuesAccumulator::Numeric(vec![(doc_id, v)]);
                    }
                }
            }
            DocValuesType::Binary => {
                if let FieldValue::Bytes(b) = field.value() {
                    if let DocValuesAccumulator::Binary(ref mut vals) = per_field.doc_values {
                        vals.push((doc_id, b.clone()));
                    } else {
                        per_field.doc_values =
                            DocValuesAccumulator::Binary(vec![(doc_id, b.clone())]);
                    }
                }
            }
            DocValuesType::Sorted => match field.value() {
                FieldValue::Bytes(b) => {
                    let term = BytesRef::new(b.clone());
                    if let DocValuesAccumulator::Sorted(ref mut vals) = per_field.doc_values {
                        vals.push((doc_id, term));
                    } else {
                        per_field.doc_values = DocValuesAccumulator::Sorted(vec![(doc_id, term)]);
                    }
                }
                FieldValue::Text(s) => {
                    let term = BytesRef::from_utf8(s);
                    if let DocValuesAccumulator::Sorted(ref mut vals) = per_field.doc_values {
                        vals.push((doc_id, term));
                    } else {
                        per_field.doc_values = DocValuesAccumulator::Sorted(vec![(doc_id, term)]);
                    }
                }
                _ => {}
            },
            DocValuesType::SortedNumeric => {
                if let Some(v) = field.numeric_value() {
                    if let DocValuesAccumulator::SortedNumeric(ref mut vals) = per_field.doc_values
                    {
                        vals.push((doc_id, vec![v]));
                    } else {
                        per_field.doc_values =
                            DocValuesAccumulator::SortedNumeric(vec![(doc_id, vec![v])]);
                    }
                }
            }
            DocValuesType::SortedSet => {
                if let FieldValue::Text(s) = field.value() {
                    let term = BytesRef::from_utf8(s);
                    if let DocValuesAccumulator::SortedSet(ref mut vals) = per_field.doc_values {
                        vals.push((doc_id, vec![term]));
                    } else {
                        per_field.doc_values =
                            DocValuesAccumulator::SortedSet(vec![(doc_id, vec![term])]);
                    }
                }
            }
            DocValuesType::None => {}
        }
    }

    /// Builds the final FieldInfos from all processed fields.
    /// The fields are sorted by field number.
    pub fn build_field_infos(&self) -> FieldInfos {
        let mut fields: Vec<FieldInfo> = self.field_infos.values().cloned().collect();
        fields.sort_by_key(|fi| fi.number());
        FieldInfos::new(fields)
    }
}

/// Computes the BM25 norm value for a field.
///
/// The norm encodes the field length as a single byte using a
/// float-to-byte compression scheme compatible with SmallFloat.
fn compute_norm(field_length: i32) -> i64 {
    // Sign-extend to match Java's byte → long widening.
    // Java's SmallFloat.intToByte4 returns a byte (signed, -128..127),
    // which is widened to long with sign extension. Rust's int_to_byte4
    // returns u8 (0..255), so we must cast through i8 to match.
    encode_norm_value(field_length) as i8 as i64
}

/// Encodes a field length into a single norm byte.
///
/// This matches Lucene's BM25Similarity.computeNorm which calls
/// SmallFloat.intToByte4(invertState.getLength()).
fn encode_norm_value(length: i32) -> u8 {
    // SmallFloat.intToByte4: encodes an int as a byte with 4 bits mantissa, 3 bits exponent
    int_to_byte4(length)
}

/// Float-like encoding for positive longs that preserves ordering and 4 significant bits.
/// Matches Java's `SmallFloat.longToInt4`.
fn long_to_int4(i: i64) -> i32 {
    assert!(i >= 0);
    let num_bits = 64 - (i as u64).leading_zeros();
    if num_bits < 4 {
        // subnormal value
        i as i32
    } else {
        // normal value
        let shift = num_bits - 4;
        // only keep the 5 most significant bits
        let mut encoded = (i as u64 >> shift) as i32;
        // clear the most significant bit, which is implicit
        encoded &= 0x07;
        // encode the shift, adding 1 because 0 is reserved for subnormal values
        encoded |= (shift as i32 + 1) << 3;
        encoded
    }
}

/// `longToInt4(Integer.MAX_VALUE)`
const MAX_INT4: u32 = {
    // longToInt4(i32::MAX) = longToInt4(2147483647)
    // numBits = 31, shift = 27, encoded = (2147483647 >> 27) & 0x07 = 7, | (28 << 3) = 224+7 = 231
    231
};

/// Number of values encoded directly (without `longToInt4`), matching Java's `NUM_FREE_VALUES`.
const NUM_FREE_VALUES: u32 = 255 - MAX_INT4; // 24

/// Encodes an integer to a byte using SmallFloat.intToByte4 format.
///
/// Matches Java's `SmallFloat.intToByte4` from Lucene 10.3.2, which uses
/// `longToInt4` with `NUM_FREE_VALUES` offset for accurate encoding of
/// small values.
fn int_to_byte4(i: i32) -> u8 {
    if i < 0 {
        return 0;
    }
    if (i as u32) < NUM_FREE_VALUES {
        i as u8
    } else {
        (NUM_FREE_VALUES + long_to_int4(i as i64 - NUM_FREE_VALUES as i64) as u32) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::standard::StandardAnalyzer;

    impl PostingList {
        fn doc_freq(&self) -> i32 {
            self.doc_freq
        }
    }
    use crate::document;

    fn make_analyzer() -> StandardAnalyzer {
        StandardAnalyzer::new()
    }

    #[test]
    fn test_process_single_document() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/foo/bar.txt"));
        doc.add(document::long_field("modified", 1000));
        doc.add(document::text_field("contents", "hello world"));

        chain.process_document(doc, &analyzer).unwrap();

        assert_eq!(chain.num_docs(), 1);
        assert_eq!(chain.stored_docs().len(), 1);

        // Finalize pending postings before decoding
        chain.finalize_pending_postings();

        // Check "path" field postings (keyword, single token)
        let path_data = chain.per_field().get("path").unwrap();
        assert_eq!(path_data.num_terms(), 1); // one unique term
        let posting_list = path_data.posting_list("/foo/bar.txt").unwrap();
        assert_eq!(posting_list.doc_freq(), 1);
        let decoded = posting_list.decode();
        assert_eq!(decoded[0].doc_id, 0);

        // Check "contents" field postings (tokenized)
        let contents_data = chain.per_field().get("contents").unwrap();
        assert_eq!(contents_data.num_terms(), 2); // "hello", "world"
        assert!(contents_data.posting_list("hello").is_some());
        assert!(contents_data.posting_list("world").is_some());

        // Check norms exist for "contents" (has norms)
        assert_eq!(contents_data.norms.len(), 1);
        assert_eq!(contents_data.norms_docs[0], 0);

        // Check no norms for "path" (omit_norms=true)
        assert!(path_data.norms.is_empty());

        // Check "modified" points
        let modified_data = chain.per_field().get("modified").unwrap();
        assert_eq!(modified_data.points.len(), 1);
        assert_eq!(modified_data.points[0].0, 0); // doc_id

        // Check stored fields
        assert_eq!(chain.stored_docs()[0].fields.len(), 1); // only "path" is stored
    }

    #[test]
    fn test_process_multiple_documents() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc1 = Document::new();
        doc1.add(document::keyword_field("path", "/a.txt"));
        doc1.add(document::text_field("contents", "hello world"));

        let mut doc2 = Document::new();
        doc2.add(document::keyword_field("path", "/b.txt"));
        doc2.add(document::text_field("contents", "hello rust"));

        chain.process_document(doc1, &analyzer).unwrap();
        chain.process_document(doc2, &analyzer).unwrap();
        chain.finalize_pending_postings();

        assert_eq!(chain.num_docs(), 2);

        // "hello" appears in both docs
        let contents_data = chain.per_field().get("contents").unwrap();
        let hello_list = contents_data.posting_list("hello").unwrap();
        assert_eq!(hello_list.doc_freq(), 2);
        let hello_decoded = hello_list.decode();
        assert_eq!(hello_decoded[0].doc_id, 0);
        assert_eq!(hello_decoded[1].doc_id, 1);

        // "world" only in doc 0
        let world_list = contents_data.posting_list("world").unwrap();
        assert_eq!(world_list.doc_freq(), 1);
        assert_eq!(world_list.decode()[0].doc_id, 0);

        // "rust" only in doc 1
        let rust_list = contents_data.posting_list("rust").unwrap();
        assert_eq!(rust_list.doc_freq(), 1);
        assert_eq!(rust_list.decode()[0].doc_id, 1);
    }

    #[test]
    fn test_positions_tracked() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::text_field("contents", "hello world hello"));

        chain.process_document(doc, &analyzer).unwrap();
        chain.finalize_pending_postings();

        let contents = chain.per_field().get("contents").unwrap();
        let hello_list = contents.posting_list("hello").unwrap();
        let decoded = hello_list.decode();
        assert_eq!(decoded[0].freq, 2);
        assert_eq!(decoded[0].positions.len(), 2);
        assert_eq!(decoded[0].positions[0], 0);
        assert_eq!(decoded[0].positions[1], 2);
    }

    #[test]
    fn test_field_infos_built() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/foo.txt"));
        doc.add(document::long_field("modified", 100));
        doc.add(document::text_field("contents", "hello"));

        chain.process_document(doc, &analyzer).unwrap();

        let fis = chain.build_field_infos();
        assert_eq!(fis.len(), 3);
        assert!(fis.has_postings());
        assert!(fis.has_doc_values());
        assert!(fis.has_point_values());
        assert!(fis.has_norms());
    }

    #[test]
    fn test_doc_values_sorted_numeric() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::long_field("modified", 42));
        chain.process_document(doc, &analyzer).unwrap();

        let modified = chain.per_field().get("modified").unwrap();
        if let DocValuesAccumulator::SortedNumeric(ref vals) = modified.doc_values {
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0], (0, vec![42]));
        } else {
            panic!("expected SortedNumeric");
        }
    }

    #[test]
    fn test_doc_values_sorted_set() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/foo.txt"));
        chain.process_document(doc, &analyzer).unwrap();

        let path_data = chain.per_field().get("path").unwrap();
        if let DocValuesAccumulator::SortedSet(ref vals) = path_data.doc_values {
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0].0, 0);
            assert_eq!(vals[0].1[0], BytesRef::from_utf8("/foo.txt"));
        } else {
            panic!("expected SortedSet");
        }
    }

    #[test]
    fn test_field_infos_have_per_field_codec_attributes() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/foo.txt"));
        doc.add(document::long_field("modified", 1000));
        doc.add(document::text_field("contents", "hello world"));
        chain.process_document(doc, &analyzer).unwrap();

        let fis = chain.build_field_infos();

        // "path" is indexed → needs PerFieldPostingsFormat attributes
        let path_fi = fis.field_info_by_name("path").unwrap();
        assert_eq!(
            path_fi.get_attribute("PerFieldPostingsFormat.format"),
            Some("Lucene103"),
        );
        assert_eq!(
            path_fi.get_attribute("PerFieldPostingsFormat.suffix"),
            Some("0"),
        );

        // "modified" has doc values but no index → needs PerFieldDocValuesFormat, no postings attrs
        let mod_fi = fis.field_info_by_name("modified").unwrap();
        assert_eq!(
            mod_fi.get_attribute("PerFieldDocValuesFormat.format"),
            Some("Lucene90"),
        );
        assert!(
            mod_fi
                .get_attribute("PerFieldPostingsFormat.format")
                .is_none(),
            "non-indexed field should not have PerFieldPostingsFormat attribute"
        );

        // "contents" is indexed → needs PerFieldPostingsFormat attributes
        let cont_fi = fis.field_info_by_name("contents").unwrap();
        assert_eq!(
            cont_fi.get_attribute("PerFieldPostingsFormat.format"),
            Some("Lucene103"),
        );
    }

    #[test]
    fn test_encode_norm_value() {
        // SmallFloat.intToByte4 known values:
        // 1 -> some byte, 2 -> some byte, etc.
        // The exact values depend on the encoding.
        // Just verify it produces non-zero for positive lengths.
        assert_ne!(encode_norm_value(1), 0);
        assert_ne!(encode_norm_value(10), 0);
        assert_ne!(encode_norm_value(100), 0);
        // Zero length gives zero norm
        assert_eq!(encode_norm_value(0), 0);
    }

    #[test]
    fn test_ram_bytes_used_increases_with_docs() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let ram_empty = chain.ram_bytes_used();

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/foo.txt"));
        doc.add(document::long_field("modified", 1000));
        doc.add(document::text_field("contents", "hello world"));
        chain.process_document(doc, &analyzer).unwrap();

        let ram_after_one = chain.ram_bytes_used();
        assert!(
            ram_after_one > ram_empty,
            "RAM should grow after one doc: empty={ram_empty}, after_one={ram_after_one}"
        );

        // Add more docs and verify RAM grows
        for i in 0..10 {
            let mut doc = Document::new();
            doc.add(document::keyword_field("path", &format!("/{i}.txt")));
            doc.add(document::long_field("modified", i as i64 * 100));
            doc.add(document::text_field(
                "contents",
                &format!("document number {i} with some text content"),
            ));
            chain.process_document(doc, &analyzer).unwrap();
        }

        let ram_after_many = chain.ram_bytes_used();
        assert!(
            ram_after_many > ram_after_one,
            "RAM should grow with more docs: after_one={ram_after_one}, after_many={ram_after_many}"
        );
    }

    #[test]
    fn test_vint_encode_decode_roundtrip() {
        let test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 0x7FFF_FFFF, -1];
        for &val in &test_values {
            let mut buf = Vec::new();
            write_vint(&mut buf, val);
            let mut offset = 0;
            let decoded = read_vint(&buf, &mut offset);
            assert_eq!(decoded, val, "round-trip failed for {val}");
            assert_eq!(offset, buf.len(), "not all bytes consumed for {val}");
        }
    }

    #[test]
    fn test_vint_encoding_sizes() {
        // 0..127 should encode as 1 byte
        let mut buf = Vec::new();
        write_vint(&mut buf, 0);
        assert_eq!(buf.len(), 1);

        buf.clear();
        write_vint(&mut buf, 127);
        assert_eq!(buf.len(), 1);

        // 128 should encode as 2 bytes
        buf.clear();
        write_vint(&mut buf, 128);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_posting_list_byte_stream_compact() {
        // Build a PostingList with 10 docs via the API, then verify the
        // byte_stream is much smaller than old Posting-based storage would be.
        let mut pl = PostingList::new(true, true, false);
        for doc_id in 0..10 {
            pl.start_doc(doc_id);
            pl.add_position(0);
            pl.add_position(5);
            pl.increment_freq();
        }
        pl.finalize_current_doc();

        // Decode and verify correctness
        let decoded = pl.decode();
        assert_eq!(decoded.len(), 10);
        for (i, dp) in decoded.iter().enumerate() {
            assert_eq!(dp.doc_id, i as i32);
            assert_eq!(dp.freq, 2);
            assert_eq!(dp.positions, vec![0, 5]);
        }

        // Byte stream should be very compact: per doc ~4 bytes
        // (1 byte doc_delta + 1 byte freq + 1 byte pos0 + 1 byte pos1)
        // = ~40 bytes for 10 docs.
        // Old approach: 10 * 80 bytes (Posting struct) + heap = 800+ bytes
        assert!(
            pl.byte_stream.len() < 100,
            "byte stream should be compact, got {} bytes",
            pl.byte_stream.len()
        );
    }

    #[test]
    fn test_process_int_field() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::int_field("size", 42, true));
        chain.process_document(doc, &analyzer).unwrap();

        let size_data = chain.per_field().get("size").unwrap();
        // Points: 4-byte sortable encoding
        assert_eq!(size_data.points.len(), 1);
        assert_eq!(size_data.points[0].1.len(), 4);
        // Doc values via numeric_value()
        if let DocValuesAccumulator::SortedNumeric(ref vals) = size_data.doc_values {
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0].1, vec![42]);
        } else {
            panic!("expected SortedNumeric for IntField");
        }
        // Stored
        assert_eq!(chain.stored_docs()[0].fields.len(), 1);
    }

    #[test]
    fn test_process_float_field() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::float_field("score", 1.5, true));
        chain.process_document(doc, &analyzer).unwrap();

        let score_data = chain.per_field().get("score").unwrap();
        assert_eq!(score_data.points.len(), 1);
        assert_eq!(score_data.points[0].1.len(), 4);
        if let DocValuesAccumulator::SortedNumeric(ref vals) = score_data.doc_values {
            assert_eq!(vals.len(), 1);
            let expected = crate::util::numeric_utils::float_to_sortable_int(1.5) as i64;
            assert_eq!(vals[0].1, vec![expected]);
        } else {
            panic!("expected SortedNumeric for FloatField");
        }
    }

    #[test]
    fn test_process_double_field() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::double_field("rating", 9.87, false));
        chain.process_document(doc, &analyzer).unwrap();

        let rating_data = chain.per_field().get("rating").unwrap();
        assert_eq!(rating_data.points.len(), 1);
        assert_eq!(rating_data.points[0].1.len(), 8);
        if let DocValuesAccumulator::SortedNumeric(ref vals) = rating_data.doc_values {
            assert_eq!(vals.len(), 1);
            let expected = crate::util::numeric_utils::double_to_sortable_long(9.87);
            assert_eq!(vals[0].1, vec![expected]);
        } else {
            panic!("expected SortedNumeric for DoubleField");
        }
        // Not stored
        assert_eq!(chain.stored_docs()[0].fields.len(), 0);
    }

    #[test]
    fn test_process_string_field() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::string_field("title", "hello world", true));
        chain.process_document(doc, &analyzer).unwrap();
        chain.finalize_pending_postings();

        let title_data = chain.per_field().get("title").unwrap();
        // StringField is not tokenized, so the whole value is one term
        assert_eq!(title_data.num_terms(), 1);
        assert!(title_data.posting_list("hello world").is_some());
        // No doc values
        assert!(matches!(title_data.doc_values, DocValuesAccumulator::None));
        // Stored
        assert_eq!(chain.stored_docs()[0].fields.len(), 1);
    }

    #[test]
    fn test_process_stored_only_fields() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::stored_string_field("notes", "test"));
        doc.add(document::stored_int_field("extra_int", 99));
        doc.add(document::stored_float_field("extra_float", 1.5));
        doc.add(document::stored_double_field("extra_double", 2.5));
        chain.process_document(doc, &analyzer).unwrap();

        // All should be stored, nothing indexed
        assert_eq!(chain.stored_docs()[0].fields.len(), 4);
        for field_name in &["notes", "extra_int", "extra_float", "extra_double"] {
            let data = chain.per_field().get(*field_name).unwrap();
            assert!(!data.has_postings());
            assert!(data.points.is_empty());
            assert!(matches!(data.doc_values, DocValuesAccumulator::None));
        }
    }

    #[test]
    fn test_reader_field_produces_same_postings_as_text_field() {
        let text = "the quick brown fox jumps over the lazy dog";

        // Index with text_field
        let mut chain_text = IndexingChain::new();
        let analyzer = make_analyzer();
        let mut doc = Document::new();
        doc.add(document::text_field("contents", text));
        chain_text.process_document(doc, &analyzer).unwrap();
        chain_text.finalize_pending_postings();

        // Index with text_field_reader
        let mut chain_reader = IndexingChain::new();
        let mut doc = Document::new();
        doc.add(document::text_field_reader(
            "contents",
            std::io::Cursor::new(text.as_bytes().to_vec()),
        ));
        chain_reader.process_document(doc, &analyzer).unwrap();
        chain_reader.finalize_pending_postings();

        let pf_text = chain_text.per_field().get("contents").unwrap();
        let pf_reader = chain_reader.per_field().get("contents").unwrap();

        assert_eq!(pf_text.num_terms(), pf_reader.num_terms());

        for (term, pl_text) in pf_text.sorted_postings() {
            let pl_reader = pf_reader.posting_list(term).unwrap_or_else(|| {
                panic!("reader chain missing term: {term}");
            });
            let decoded_text = pl_text.decode();
            let decoded_reader = pl_reader.decode();
            assert_eq!(
                decoded_text.len(),
                decoded_reader.len(),
                "doc count mismatch for term: {term}"
            );
            for (dt, dr) in decoded_text.iter().zip(&decoded_reader) {
                assert_eq!(dt.doc_id, dr.doc_id, "doc_id mismatch for term: {term}");
                assert_eq!(dt.freq, dr.freq, "freq mismatch for term: {term}");
                assert_eq!(
                    dt.positions, dr.positions,
                    "positions mismatch for term: {term}"
                );
            }
        }

        assert_eq!(pf_text.norms, pf_reader.norms);
    }

    #[test]
    fn test_large_reader_field_multi_chunk() {
        // 40 KB of text — exercises multiple 8 KB chunks in analyze_reader
        let text = "the quick brown fox jumps over the lazy dog ".repeat(1000);
        assert!(text.len() > 32_000);

        let mut chain_text = IndexingChain::new();
        let analyzer = make_analyzer();
        let mut doc = Document::new();
        doc.add(document::text_field("contents", &text));
        chain_text.process_document(doc, &analyzer).unwrap();
        chain_text.finalize_pending_postings();

        let mut chain_reader = IndexingChain::new();
        let mut doc = Document::new();
        doc.add(document::text_field_reader(
            "contents",
            std::io::Cursor::new(text.as_bytes().to_vec()),
        ));
        chain_reader.process_document(doc, &analyzer).unwrap();
        chain_reader.finalize_pending_postings();

        let pf_text = chain_text.per_field().get("contents").unwrap();
        let pf_reader = chain_reader.per_field().get("contents").unwrap();

        assert_eq!(pf_text.num_terms(), pf_reader.num_terms());

        for (term, pl_text) in pf_text.sorted_postings() {
            let pl_reader = pf_reader.posting_list(term).unwrap_or_else(|| {
                panic!("reader chain missing term: {term}");
            });
            let decoded_text = pl_text.decode();
            let decoded_reader = pl_reader.decode();
            assert_eq!(
                decoded_text.len(),
                decoded_reader.len(),
                "doc count mismatch for term: {term}"
            );
            for (dt, dr) in decoded_text.iter().zip(&decoded_reader) {
                assert_eq!(dt.doc_id, dr.doc_id, "doc_id mismatch for term: {term}");
                assert_eq!(dt.freq, dr.freq, "freq mismatch for term: {term}");
                assert_eq!(
                    dt.positions, dr.positions,
                    "positions mismatch for term: {term}"
                );
            }
        }

        assert_eq!(pf_text.norms, pf_reader.norms);
    }
}
