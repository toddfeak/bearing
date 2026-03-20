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

/// Start and end offset buffers for offset-indexed fields.
///
/// Boxed and optional in `TermVectorTerm` to avoid
/// 48 bytes of Vec overhead per instance when offsets are not stored.
#[derive(Clone, Debug, MemSize)]
pub struct OffsetBuffers {
    /// Absolute start offsets.
    pub start_offsets: Vec<i32>,
    /// Absolute end offsets.
    pub end_offsets: Vec<i32>,
}

/// Compact struct-of-arrays storage for all terms' posting data in a field.
///
/// Instead of one `PostingList` struct (~143 bytes) per term, stores per-term
/// data in parallel Vecs indexed by term_id. This eliminates struct padding
/// and per-Vec overhead, reducing per-term cost from ~193 bytes to ~33 bytes.
#[derive(Clone, Debug, MemSize)]
pub struct PostingsArray {
    // Per-term encoded data: doc_delta + freq as vInt byte streams
    pub(crate) byte_streams: Vec<Vec<u8>>,
    total_term_freqs: Vec<i64>,
    doc_freqs: Vec<i32>,
    last_doc_ids: Vec<i32>,

    // Current in-progress document state (per-term)
    current_doc_ids: Vec<i32>,
    current_freqs: Vec<i32>,

    // Per-term position/offset stream: position deltas + optional offset deltas
    // written immediately during tokenization (only allocated when has_positions)
    prox_streams: Option<Vec<Vec<u8>>>,
    /// Last position written per term (for delta encoding).
    last_positions: Vec<i32>,
    /// Last end offset written per term (for offset delta encoding).
    last_end_offsets: Vec<i32>,

    // TV state (only allocated when tv enabled)
    tv_freqs: Option<Vec<i32>>,
    tv_positions: Option<Vec<Vec<i32>>>,
    tv_start_offsets: Option<Vec<Vec<i32>>>,
    tv_end_offsets: Option<Vec<Vec<i32>>>,

    // Field-level config
    has_freqs: bool,
    has_positions: bool,
    has_offsets: bool,
}

impl PostingsArray {
    /// Creates a new empty postings array with the given field configuration.
    pub fn new(
        has_freqs: bool,
        has_positions: bool,
        has_offsets: bool,
        tv_positions: bool,
        tv_offsets: bool,
    ) -> Self {
        Self {
            byte_streams: Vec::new(),
            total_term_freqs: Vec::new(),
            doc_freqs: Vec::new(),
            last_doc_ids: Vec::new(),
            current_doc_ids: Vec::new(),
            current_freqs: Vec::new(),
            prox_streams: if has_positions {
                Some(Vec::new())
            } else {
                None
            },
            last_positions: Vec::new(),
            last_end_offsets: Vec::new(),
            tv_freqs: if tv_positions || tv_offsets {
                Some(Vec::new())
            } else {
                None
            },
            tv_positions: if tv_positions { Some(Vec::new()) } else { None },
            tv_start_offsets: if tv_offsets { Some(Vec::new()) } else { None },
            tv_end_offsets: if tv_offsets { Some(Vec::new()) } else { None },
            has_freqs,
            has_positions,
            has_offsets,
        }
    }

    /// Allocates a new slot for a term, returning its term_id.
    pub fn add_term(&mut self) -> usize {
        let tid = self.byte_streams.len();
        self.byte_streams.push(Vec::new());
        self.total_term_freqs.push(0);
        self.doc_freqs.push(0);
        self.last_doc_ids.push(0);
        self.current_doc_ids.push(-1);
        self.current_freqs.push(0);
        if let Some(ref mut v) = self.prox_streams {
            v.push(Vec::new());
        }
        if self.has_positions {
            self.last_positions.push(0);
        }
        if self.has_offsets {
            self.last_end_offsets.push(0);
        }
        if let Some(ref mut v) = self.tv_freqs {
            v.push(0);
        }
        if let Some(ref mut v) = self.tv_positions {
            v.push(Vec::new());
        }
        if let Some(ref mut v) = self.tv_start_offsets {
            v.push(Vec::new());
        }
        if let Some(ref mut v) = self.tv_end_offsets {
            v.push(Vec::new());
        }
        tid
    }

    /// Records a token occurrence for the given term and document.
    ///
    /// When positions are enabled, position deltas (and optional offset deltas)
    /// are written immediately to `prox_streams` as vInts, avoiding the need to
    /// buffer positions in a `Vec<i32>` per term.
    #[inline]
    pub fn record_occurrence(
        &mut self,
        tid: usize,
        doc_id: i32,
        position: i32,
        start_offset: i32,
        end_offset: i32,
    ) {
        if self.current_doc_ids[tid] != doc_id {
            self.start_doc(tid, doc_id);
        } else if self.has_freqs {
            self.current_freqs[tid] += 1;
        }

        if self.has_positions {
            let pos_delta = position - self.last_positions[tid];
            let prox = &mut self.prox_streams.as_mut().unwrap()[tid];
            write_vint(prox, pos_delta);
            self.last_positions[tid] = position;

            if self.has_offsets {
                write_vint(prox, start_offset - self.last_end_offsets[tid]);
                write_vint(prox, end_offset - start_offset);
                self.last_end_offsets[tid] = end_offset;
            }
        }
    }

    /// Starts a new document for the given term, finalizing any pending doc first.
    fn start_doc(&mut self, tid: usize, doc_id: i32) {
        if self.current_doc_ids[tid] >= 0 {
            self.finalize_current_doc(tid);
        }
        self.current_doc_ids[tid] = doc_id;
        self.current_freqs[tid] = 1;
        self.doc_freqs[tid] += 1;
        if self.has_positions {
            self.last_positions[tid] = 0;
        }
        if self.has_offsets {
            self.last_end_offsets[tid] = 0;
        }
    }

    /// Starts a new document for the given term (explicit API for FeatureField).
    pub fn start_doc_explicit(&mut self, tid: usize, doc_id: i32) {
        self.start_doc(tid, doc_id);
    }

    /// Sets the frequency for the given term's current document.
    pub fn set_freq(&mut self, tid: usize, freq: i32) {
        self.current_freqs[tid] = freq;
    }

    /// Records a term vector occurrence for the given term.
    #[inline]
    pub fn record_tv_occurrence(
        &mut self,
        tid: usize,
        position: i32,
        start_offset: i32,
        end_offset: i32,
    ) {
        self.tv_freqs.as_mut().unwrap()[tid] += 1;
        if let Some(ref mut positions) = self.tv_positions {
            positions[tid].push(position);
        }
        if let Some(ref mut start_offsets) = self.tv_start_offsets {
            start_offsets[tid].push(start_offset);
        }
        if let Some(ref mut end_offsets) = self.tv_end_offsets {
            end_offsets[tid].push(end_offset);
        }
    }

    /// Encodes the current pending document for one term into its byte_stream.
    ///
    /// Only writes doc_delta and freq — position/offset data was already written
    /// directly to `prox_streams` during `record_occurrence`.
    pub fn finalize_current_doc(&mut self, tid: usize) {
        if self.current_doc_ids[tid] < 0 {
            return;
        }

        self.total_term_freqs[tid] += self.current_freqs[tid] as i64;

        // doc_id delta
        let delta = self.current_doc_ids[tid] - self.last_doc_ids[tid];
        write_vint(&mut self.byte_streams[tid], delta);

        // freq (if field has freqs)
        if self.has_freqs {
            write_vint(&mut self.byte_streams[tid], self.current_freqs[tid]);
        }

        self.last_doc_ids[tid] = self.current_doc_ids[tid];
        self.current_doc_ids[tid] = -1;
        self.current_freqs[tid] = 0;
    }

    /// Finalizes all terms with pending documents.
    pub fn finalize_all(&mut self) {
        for tid in 0..self.current_doc_ids.len() {
            self.finalize_current_doc(tid);
        }
    }

    /// Decodes one term's data into a reusable `PostingsBuffer`.
    ///
    /// Reads doc_delta + freq from `byte_streams` and position/offset deltas
    /// from `prox_streams` in parallel.
    pub fn decode_into(&self, tid: usize, buf: &mut PostingsBuffer) {
        buf.clear();
        let data = &self.byte_streams[tid];
        let mut doc_offset = 0;
        let mut last_doc_id = 0;

        // Prox stream state (only when has_positions)
        let prox_data = self.prox_streams.as_ref().map(|v| v[tid].as_slice());
        let mut prox_offset = 0;

        while doc_offset < data.len() {
            let doc_delta = read_vint(data, &mut doc_offset);
            let doc_id = last_doc_id + doc_delta;
            last_doc_id = doc_id;

            let freq = if self.has_freqs {
                read_vint(data, &mut doc_offset)
            } else {
                1
            };

            buf.doc_ids.push(doc_id);
            buf.freqs.push(freq);

            if let Some(prox) = prox_data {
                let start = buf.positions.len();
                let mut last_pos = 0;
                for _ in 0..freq {
                    let pos_delta = read_vint(prox, &mut prox_offset);
                    let pos = last_pos + pos_delta;
                    buf.positions.push(pos);
                    last_pos = pos;

                    if self.has_offsets {
                        // Consume offset data (not exposed in PostingsBuffer)
                        read_vint(prox, &mut prox_offset);
                        read_vint(prox, &mut prox_offset);
                    }
                }
                buf.position_starts.push(start);
            }
        }

        buf.doc_freq = self.doc_freqs[tid];
        buf.total_term_freq = self.total_term_freqs[tid];
    }

    /// Returns the number of terms in this array.
    pub fn len(&self) -> usize {
        self.byte_streams.len()
    }
}

/// Reusable buffer for decoded posting data, used at flush time.
///
/// Instead of allocating a `Vec<DecodedPosting>` per term, a single
/// `PostingsBuffer` is reused across all terms in a field.
#[derive(Debug)]
pub struct PostingsBuffer {
    /// Decoded doc IDs in order.
    pub doc_ids: Vec<i32>,
    /// Decoded frequencies (parallel with doc_ids).
    pub freqs: Vec<i32>,
    /// All positions concatenated across all docs.
    pub positions: Vec<i32>,
    /// Start index into `positions` for each doc (parallel with doc_ids).
    pub position_starts: Vec<usize>,
    /// Total term freq (sum of freqs).
    pub total_term_freq: i64,
    /// Number of documents containing this term.
    pub doc_freq: i32,
}

impl PostingsBuffer {
    /// Creates a new empty buffer.
    pub fn new() -> Self {
        Self {
            doc_ids: Vec::new(),
            freqs: Vec::new(),
            positions: Vec::new(),
            position_starts: Vec::new(),
            total_term_freq: 0,
            doc_freq: 0,
        }
    }

    /// Clears the buffer for reuse.
    pub fn clear(&mut self) {
        self.doc_ids.clear();
        self.freqs.clear();
        self.positions.clear();
        self.position_starts.clear();
        self.total_term_freq = 0;
        self.doc_freq = 0;
    }

    /// Returns postings data in the format expected by `PostingsWriter::write_term`.
    pub fn as_postings_data(&self) -> Vec<(i32, i32, &[i32])> {
        self.doc_ids
            .iter()
            .enumerate()
            .map(|(i, &doc_id)| {
                let freq = self.freqs[i];
                let positions = if self.position_starts.is_empty() {
                    &[] as &[i32]
                } else {
                    let start = self.position_starts[i];
                    let end = if i + 1 < self.position_starts.len() {
                        self.position_starts[i + 1]
                    } else {
                        self.positions.len()
                    };
                    &self.positions[start..end]
                };
                (doc_id, freq, positions)
            })
            .collect()
    }
}

/// Per-field accumulated data from all documents.
///
/// Postings are stored as a separate term-id lookup (`term_ids`) and a
/// compact struct-of-arrays `PostingsArray` indexed by term id. During
/// HashMap rehash only the lightweight `(String, u32)` entries are copied
/// (~29 bytes each).
#[derive(Clone, Debug, MemSize)]
pub struct PerFieldData {
    /// Term string -> index into `postings`.
    term_ids: HashMap<String, u32>,
    /// Struct-of-arrays postings storage.
    pub postings: PostingsArray,
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
            postings: PostingsArray::new(false, false, false, false, false),
            doc_values: DocValuesAccumulator::None,
            norms: Vec::new(),
            norms_docs: Vec::new(),
            points: Vec::new(),
        }
    }

    /// Looks up or inserts a term, returning its term_id.
    ///
    /// On first call for this field, initializes the `PostingsArray` with the
    /// given field configuration. Subsequent calls must use the same config.
    fn get_or_insert_term(
        &mut self,
        term: &str,
        has_freqs: bool,
        has_positions: bool,
        has_offsets: bool,
        tv_positions: bool,
        tv_offsets: bool,
    ) -> usize {
        if let Some(&id) = self.term_ids.get(term) {
            return id as usize;
        }
        // Initialize PostingsArray config on first term
        if self.postings.len() == 0 {
            self.postings = PostingsArray::new(
                has_freqs,
                has_positions,
                has_offsets,
                tv_positions,
                tv_offsets,
            );
        }
        let tid = self.postings.add_term();
        self.term_ids.insert(term.to_string(), tid as u32);
        tid
    }

    /// Returns terms and term_ids in byte-sorted order for codec writing.
    pub fn sorted_postings(&self) -> Vec<(&str, usize)> {
        let mut pairs: Vec<(&str, usize)> = self
            .term_ids
            .iter()
            .map(|(term, &id)| (term.as_str(), id as usize))
            .collect();
        pairs.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
        pairs
    }

    /// Returns true if this field has any postings.
    pub fn has_postings(&self) -> bool {
        !self.term_ids.is_empty()
    }

    /// Extracts term vector data from the postings array and returns a `TermVectorField`.
    ///
    /// Iterates all terms, collecting those with `tv_freq > 0`, drains their TV
    /// accumulators (preserving Vec capacity), and returns a sorted `TermVectorField`.
    /// Returns `None` if no term has TV data for the current document.
    fn take_term_vector_data(
        &mut self,
        field_number: u32,
        tv_positions: bool,
        tv_offsets: bool,
        tv_payloads: bool,
    ) -> Option<TermVectorField> {
        let mut terms: Vec<TermVectorTerm> = Vec::new();

        let tv_freqs = self.postings.tv_freqs.as_mut()?;

        for (term_text, &id) in &self.term_ids {
            let tid = id as usize;
            if tv_freqs[tid] > 0 {
                let positions = self
                    .postings
                    .tv_positions
                    .as_mut()
                    .map(|v| std::mem::take(&mut v[tid]))
                    .unwrap_or_default();
                let offsets = if let (Some(ref mut starts), Some(ref mut ends)) = (
                    self.postings.tv_start_offsets.as_mut(),
                    self.postings.tv_end_offsets.as_mut(),
                ) {
                    Some(Box::new(OffsetBuffers {
                        start_offsets: std::mem::take(&mut starts[tid]),
                        end_offsets: std::mem::take(&mut ends[tid]),
                    }))
                } else {
                    None
                };
                terms.push(TermVectorTerm {
                    term: term_text.clone(),
                    freq: tv_freqs[tid],
                    positions,
                    offsets,
                });
                tv_freqs[tid] = 0;
            }
        }

        if terms.is_empty() {
            return None;
        }

        terms.sort_by(|a, b| a.term.as_bytes().cmp(b.term.as_bytes()));
        Some(TermVectorField {
            field_number,
            has_positions: tv_positions,
            has_offsets: tv_offsets,
            has_payloads: tv_payloads,
            terms,
        })
    }
}

#[cfg(test)]
impl PerFieldData {
    /// Number of unique terms.
    pub fn num_terms(&self) -> usize {
        self.term_ids.len()
    }

    /// Looks up a term_id by term text.
    pub fn term_id(&self, term: &str) -> Option<usize> {
        self.term_ids.get(term).map(|&id| id as usize)
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

/// A single term's term vector data for one document.
#[derive(Clone, Debug, MemSize)]
pub struct TermVectorTerm {
    /// The term's UTF-8 bytes.
    pub term: String,
    /// Term frequency in this document.
    pub freq: i32,
    /// Absolute positions. Empty if positions not stored.
    pub positions: Vec<i32>,
    /// Start and end offsets. `None` if offsets not stored.
    pub offsets: Option<Box<OffsetBuffers>>,
}

/// Term vector data for one field in one document.
#[derive(Clone, Debug, MemSize)]
pub struct TermVectorField {
    /// Field number (matches FieldInfo.number).
    pub field_number: u32,
    /// Whether positions are stored.
    pub has_positions: bool,
    /// Whether offsets are stored.
    pub has_offsets: bool,
    /// Whether payloads are stored (structurally supported, always false for now).
    pub has_payloads: bool,
    /// Terms sorted by UTF-8 byte order.
    pub terms: Vec<TermVectorTerm>,
}

/// All term vector fields for one document.
#[derive(Clone, Debug, MemSize)]
pub struct TermVectorDoc {
    /// Term vector fields, sorted by field number. Empty if no TV fields.
    pub fields: Vec<TermVectorField>,
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
    /// Term vector data per document, in doc order.
    term_vector_docs: Vec<TermVectorDoc>,
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
            term_vector_docs: Vec::new(),
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
            term_vector_docs: Vec::new(),
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

    /// Returns the term vector data for all documents, in doc order.
    pub fn term_vector_docs(&self) -> &[TermVectorDoc] {
        &self.term_vector_docs
    }

    /// Takes the most recently added term vector doc, leaving `None` in its place.
    /// Used by `SegmentWorker` to stream TV data to the chunk writer instead of
    /// accumulating in memory.
    pub fn take_last_tv_doc(&mut self) -> Option<TermVectorDoc> {
        self.term_vector_docs.pop()
    }

    pub fn num_docs(&self) -> i32 {
        self.num_docs
    }

    /// Finalizes all pending (not yet encoded) postings across all fields.
    /// Must be called before flush so the last document in each posting list
    /// is encoded into the byte stream.
    pub fn finalize_pending_postings(&mut self) {
        for pf in self.per_field.values_mut() {
            pf.postings.finalize_all();
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

        let mut tv_fields: Vec<TermVectorField> = Vec::new();

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

            // Process indexed field (postings + optional term vectors)
            if meta.index_options != IndexOptions::None
                && let Some(tvf) = Self::process_indexed_field(
                    per_field,
                    &meta,
                    &mut field,
                    doc_id,
                    analyzer,
                    &mut lowercase_buf,
                )?
            {
                tv_fields.push(tvf);
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
        self.term_vector_docs
            .push(TermVectorDoc { fields: tv_fields });

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
    ///
    /// Returns a [`TermVectorField`] if term vectors are enabled for this field.
    fn process_indexed_field(
        per_field: &mut PerFieldData,
        meta: &FieldMeta,
        field: &mut Field,
        doc_id: i32,
        analyzer: &dyn Analyzer,
        buf: &mut String,
    ) -> io::Result<Option<TermVectorField>> {
        let has_positions = meta.index_options >= IndexOptions::DocsAndFreqsAndPositions;
        let has_offsets = meta.index_options >= IndexOptions::DocsAndFreqsAndPositionsAndOffsets;
        let has_freqs = meta.index_options >= IndexOptions::DocsAndFreqs;

        // Term vector flags come from FieldType, not FieldMeta
        let store_tv = field.field_type().store_term_vectors();
        let tv_positions = field.field_type().store_term_vector_positions();
        let tv_offsets = field.field_type().store_term_vector_offsets();
        let tv_payloads = field.field_type().store_term_vector_payloads();

        let mut position: i32 = -1;
        let mut field_length: i32 = 0;

        let mut record_token = |per_field: &mut PerFieldData, token_ref: TokenRef<'_>| {
            position += token_ref.position_increment as i32;
            field_length += 1;

            let tid = per_field.get_or_insert_term(
                token_ref.text,
                has_freqs,
                has_positions,
                has_offsets,
                tv_positions,
                tv_offsets,
            );

            per_field.postings.record_occurrence(
                tid,
                doc_id,
                position,
                token_ref.start_offset as i32,
                token_ref.end_offset as i32,
            );

            if store_tv {
                per_field.postings.record_tv_occurrence(
                    tid,
                    position,
                    token_ref.start_offset as i32,
                    token_ref.end_offset as i32,
                );
            }
        };

        // Feature fields: single term with explicit frequency, no positions/norms
        if let FieldValue::Feature { term, freq } = field.value() {
            let tid = per_field.get_or_insert_term(term, has_freqs, false, false, false, false);
            per_field.postings.start_doc_explicit(tid, doc_id);
            per_field.postings.set_freq(tid, *freq);
            return Ok(None);
        }

        if field.field_type().tokenized() {
            match field.value() {
                FieldValue::Text(text) => {
                    analyzer.analyze_to(text, buf, &mut |tr| {
                        record_token(per_field, tr);
                    });
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
                _ => return Ok(None),
            }
        } else {
            let text = match field.value() {
                FieldValue::Text(text) => text,
                _ => return Ok(None),
            };

            // Keyword field: single token with the exact value, no allocation needed
            position += 1;
            field_length = 1;

            let tid = per_field.get_or_insert_term(
                text,
                has_freqs,
                has_positions,
                has_offsets,
                tv_positions,
                tv_offsets,
            );

            per_field
                .postings
                .record_occurrence(tid, doc_id, position, 0, text.len() as i32);

            if store_tv {
                per_field
                    .postings
                    .record_tv_occurrence(tid, position, 0, text.len() as i32);
            }
        }

        // Compute and store norms if this field has norms
        // has_norms = is_indexed && !omit_norms; is_indexed is guaranteed here
        if !meta.omit_norms && field_length > 0 {
            let norm = compute_norm(field_length);
            per_field.norms.push(norm);
            per_field.norms_docs.push(doc_id);
        }

        if store_tv {
            return Ok(per_field.take_term_vector_data(
                meta.number,
                tv_positions,
                tv_offsets,
                tv_payloads,
            ));
        }
        Ok(None)
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
                        if let Some(last) = vals.last_mut().filter(|(id, _)| *id == doc_id) {
                            last.1.push(v);
                        } else {
                            vals.push((doc_id, vec![v]));
                        }
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
                        if let Some(last) = vals.last_mut().filter(|(id, _)| *id == doc_id) {
                            last.1.push(term);
                        } else {
                            vals.push((doc_id, vec![term]));
                        }
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
    use crate::document;

    fn make_analyzer() -> StandardAnalyzer {
        StandardAnalyzer::new()
    }

    /// Decode a single term from PerFieldData into a PostingsBuffer.
    fn decode_term(pfd: &PerFieldData, term: &str) -> PostingsBuffer {
        let tid = pfd
            .term_id(term)
            .unwrap_or_else(|| panic!("term not found: {term}"));
        let mut buf = PostingsBuffer::new();
        pfd.postings.decode_into(tid, &mut buf);
        buf
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
        assert_eq!(path_data.num_terms(), 1);
        let buf = decode_term(path_data, "/foo/bar.txt");
        assert_eq!(buf.doc_freq, 1);
        assert_eq!(buf.doc_ids[0], 0);

        // Check "contents" field postings (tokenized)
        let contents_data = chain.per_field().get("contents").unwrap();
        assert_eq!(contents_data.num_terms(), 2);
        assert_some!(contents_data.term_id("hello"));
        assert_some!(contents_data.term_id("world"));

        // Check norms exist for "contents" (has norms)
        assert_eq!(contents_data.norms.len(), 1);
        assert_eq!(contents_data.norms_docs[0], 0);

        // Check no norms for "path" (omit_norms=true)
        assert_is_empty!(path_data.norms);

        // Check "modified" points
        let modified_data = chain.per_field().get("modified").unwrap();
        assert_eq!(modified_data.points.len(), 1);
        assert_eq!(modified_data.points[0].0, 0);

        // Check stored fields
        assert_eq!(chain.stored_docs()[0].fields.len(), 1);
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
        let hello = decode_term(contents_data, "hello");
        assert_eq!(hello.doc_freq, 2);
        assert_eq!(hello.doc_ids[0], 0);
        assert_eq!(hello.doc_ids[1], 1);

        // "world" only in doc 0
        let world = decode_term(contents_data, "world");
        assert_eq!(world.doc_freq, 1);
        assert_eq!(world.doc_ids[0], 0);

        // "rust" only in doc 1
        let rust_buf = decode_term(contents_data, "rust");
        assert_eq!(rust_buf.doc_freq, 1);
        assert_eq!(rust_buf.doc_ids[0], 1);
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
        let hello = decode_term(contents, "hello");
        assert_eq!(hello.freqs[0], 2);
        assert_len_eq_x!(&hello.positions, 2);
        assert_eq!(hello.positions[0], 0);
        assert_eq!(hello.positions[1], 2);
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
    fn test_doc_values_numeric() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::numeric_doc_values_field("count", 99));
        chain.process_document(doc, &analyzer).unwrap();

        let count_data = chain.per_field().get("count").unwrap();
        if let DocValuesAccumulator::Numeric(ref vals) = count_data.doc_values {
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0], (0, 99));
        } else {
            panic!("expected Numeric");
        }
    }

    #[test]
    fn test_doc_values_binary() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::binary_doc_values_field("payload", vec![1, 2, 3]));
        chain.process_document(doc, &analyzer).unwrap();

        let payload_data = chain.per_field().get("payload").unwrap();
        if let DocValuesAccumulator::Binary(ref vals) = payload_data.doc_values {
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0], (0, vec![1, 2, 3]));
        } else {
            panic!("expected Binary");
        }
    }

    #[test]
    fn test_doc_values_sorted() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::sorted_doc_values_field("category", b"animals"));
        chain.process_document(doc, &analyzer).unwrap();

        let cat_data = chain.per_field().get("category").unwrap();
        if let DocValuesAccumulator::Sorted(ref vals) = cat_data.doc_values {
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0].0, 0);
            assert_eq!(vals[0].1, BytesRef::new(b"animals".to_vec()));
        } else {
            panic!("expected Sorted");
        }
    }

    #[test]
    fn test_doc_values_numeric_multiple_docs() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        for i in 0..3 {
            let mut doc = Document::new();
            doc.add(document::numeric_doc_values_field("count", i * 10));
            chain.process_document(doc, &analyzer).unwrap();
        }

        let count_data = chain.per_field().get("count").unwrap();
        if let DocValuesAccumulator::Numeric(ref vals) = count_data.doc_values {
            assert_eq!(vals.len(), 3);
            assert_eq!(vals[0], (0, 0));
            assert_eq!(vals[1], (1, 10));
            assert_eq!(vals[2], (2, 20));
        } else {
            panic!("expected Numeric");
        }
    }

    #[test]
    fn test_postings_array_with_offsets_roundtrip() {
        let mut pa = PostingsArray::new(true, true, true, false, false);
        let tid = pa.add_term();

        // Doc 0: two occurrences
        pa.record_occurrence(tid, 0, 0, 0, 5);
        pa.record_occurrence(tid, 0, 1, 6, 11);

        // Doc 1: one occurrence
        pa.record_occurrence(tid, 1, 0, 0, 3);

        pa.finalize_current_doc(tid);

        let mut buf = PostingsBuffer::new();
        pa.decode_into(tid, &mut buf);
        assert_len_eq_x!(&buf.doc_ids, 2);
        assert_eq!(buf.doc_ids[0], 0);
        assert_eq!(buf.freqs[0], 2);
        assert_eq!(buf.positions, vec![0, 1, 0]);
        assert_eq!(buf.doc_ids[1], 1);
        assert_eq!(buf.freqs[1], 1);

        // Verify offset data was encoded (prox_stream is larger with offsets)
        let mut pa_no_offsets = PostingsArray::new(true, true, false, false, false);
        let tid2 = pa_no_offsets.add_term();
        pa_no_offsets.record_occurrence(tid2, 0, 0, 0, 5);
        pa_no_offsets.record_occurrence(tid2, 0, 1, 6, 11);
        pa_no_offsets.record_occurrence(tid2, 1, 0, 0, 3);
        pa_no_offsets.finalize_current_doc(tid2);
        let prox_with = pa.prox_streams.as_ref().unwrap()[tid].len();
        let prox_without = pa_no_offsets.prox_streams.as_ref().unwrap()[tid2].len();
        assert_gt!(
            prox_with,
            prox_without,
            "prox stream with offsets should be larger"
        );
    }

    #[test]
    fn test_indexing_chain_default() {
        let chain = IndexingChain::default();
        assert_eq!(chain.num_docs(), 0);
    }

    #[test]
    fn test_doc_values_numeric_appends_to_existing() {
        // Exercises the append-to-existing-accumulator branch (not just initial creation)
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::numeric_doc_values_field("count", 10));
        chain.process_document(doc, &analyzer).unwrap();

        let mut doc = Document::new();
        doc.add(document::numeric_doc_values_field("count", 20));
        chain.process_document(doc, &analyzer).unwrap();

        let data = chain.per_field().get("count").unwrap();
        if let DocValuesAccumulator::Numeric(ref vals) = data.doc_values {
            assert_eq!(vals, &[(0, 10), (1, 20)]);
        } else {
            panic!("expected Numeric");
        }
    }

    #[test]
    fn test_doc_values_binary_appends_to_existing() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::binary_doc_values_field("payload", vec![1]));
        chain.process_document(doc, &analyzer).unwrap();

        let mut doc = Document::new();
        doc.add(document::binary_doc_values_field("payload", vec![2]));
        chain.process_document(doc, &analyzer).unwrap();

        let data = chain.per_field().get("payload").unwrap();
        if let DocValuesAccumulator::Binary(ref vals) = data.doc_values {
            assert_eq!(vals.len(), 2);
            assert_eq!(vals[1], (1, vec![2]));
        } else {
            panic!("expected Binary");
        }
    }

    #[test]
    fn test_doc_values_sorted_appends_to_existing() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::sorted_doc_values_field("cat", b"a"));
        chain.process_document(doc, &analyzer).unwrap();

        let mut doc = Document::new();
        doc.add(document::sorted_doc_values_field("cat", b"b"));
        chain.process_document(doc, &analyzer).unwrap();

        let data = chain.per_field().get("cat").unwrap();
        if let DocValuesAccumulator::Sorted(ref vals) = data.doc_values {
            assert_eq!(vals.len(), 2);
        } else {
            panic!("expected Sorted");
        }
    }

    #[test]
    fn test_doc_values_sorted_set_appends_to_existing() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::sorted_set_doc_values_field("tag", "rust"));
        chain.process_document(doc, &analyzer).unwrap();

        let mut doc = Document::new();
        doc.add(document::sorted_set_doc_values_field("tag", "java"));
        chain.process_document(doc, &analyzer).unwrap();

        let data = chain.per_field().get("tag").unwrap();
        if let DocValuesAccumulator::SortedSet(ref vals) = data.doc_values {
            assert_eq!(vals.len(), 2);
        } else {
            panic!("expected SortedSet");
        }
    }

    #[test]
    fn test_doc_values_sorted_numeric_appends_to_existing() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::sorted_numeric_doc_values_field("ts", 100));
        chain.process_document(doc, &analyzer).unwrap();

        let mut doc = Document::new();
        doc.add(document::sorted_numeric_doc_values_field("ts", 200));
        chain.process_document(doc, &analyzer).unwrap();

        let data = chain.per_field().get("ts").unwrap();
        if let DocValuesAccumulator::SortedNumeric(ref vals) = data.doc_values {
            assert_eq!(vals.len(), 2);
        } else {
            panic!("expected SortedNumeric");
        }
    }

    #[test]
    fn test_encode_norm_subnormal_and_negative() {
        // field_length=0 → intToByte4(0) = 0
        assert_eq!(encode_norm_value(0), 0);
        // field_length < 0 → intToByte4 returns 0
        assert_eq!(encode_norm_value(-1), 0);
        // Small field lengths (1-15) exercise the subnormal path in long_to_int4
        for len in 1..=15 {
            assert_ne!(encode_norm_value(len), 0);
        }
    }

    #[test]
    fn test_long_to_int4_subnormal() {
        // Values 0-7 have fewer than 4 significant bits → subnormal path
        assert_eq!(long_to_int4(0), 0);
        assert_eq!(long_to_int4(1), 1);
        assert_eq!(long_to_int4(7), 7);
        // Value 8 has exactly 4 bits → normal path
        let normal = long_to_int4(8);
        assert_gt!(normal, 7);
    }

    #[test]
    fn test_process_doc_values_sorted_text_variant() {
        // Exercises the FieldValue::Text branch for DocValuesType::Sorted
        // by calling process_doc_values directly.
        let meta = FieldMeta {
            number: 0,
            index_options: IndexOptions::None,
            doc_values_type: DocValuesType::Sorted,
            omit_norms: true,
            has_point_values: false,
        };

        let field1 = crate::document::Field::new(
            "category".to_string(),
            crate::document::FieldTypeBuilder::new().build(),
            crate::document::FieldValue::Text("animals".to_string()),
        );
        let field2 = crate::document::Field::new(
            "category".to_string(),
            crate::document::FieldTypeBuilder::new().build(),
            crate::document::FieldValue::Text("plants".to_string()),
        );

        let mut per_field = PerFieldData::new();
        IndexingChain::process_doc_values(&mut per_field, &meta, &field1, 0);
        IndexingChain::process_doc_values(&mut per_field, &meta, &field2, 1);

        if let DocValuesAccumulator::Sorted(ref vals) = per_field.doc_values {
            assert_eq!(vals.len(), 2);
            assert_eq!(vals[0].1, BytesRef::from_utf8("animals"));
            assert_eq!(vals[1].1, BytesRef::from_utf8("plants"));
        } else {
            panic!("expected Sorted");
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
    fn test_postings_array_byte_stream_compact() {
        let mut pa = PostingsArray::new(true, true, false, false, false);
        let tid = pa.add_term();
        for doc_id in 0..10 {
            pa.record_occurrence(tid, doc_id, 0, 0, 0);
            pa.record_occurrence(tid, doc_id, 5, 0, 0);
        }
        pa.finalize_current_doc(tid);

        let mut buf = PostingsBuffer::new();
        pa.decode_into(tid, &mut buf);
        assert_len_eq_x!(&buf.doc_ids, 10);
        for (i, &doc_id) in buf.doc_ids.iter().enumerate() {
            assert_eq!(doc_id, i as i32);
            assert_eq!(buf.freqs[i], 2);
        }

        // byte_streams now only has doc_delta + freq (~20 bytes for 10 docs)
        // prox_streams has position deltas (~20 bytes for 10 docs × 2 positions)
        let total_bytes = pa.byte_streams[tid].len() + pa.prox_streams.as_ref().unwrap()[tid].len();
        assert!(
            total_bytes < 100,
            "combined streams should be compact, got {total_bytes} bytes",
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
        assert_some!(title_data.term_id("hello world"));
        // No doc values
        assert_matches!(title_data.doc_values, DocValuesAccumulator::None);
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
            assert_is_empty!(data.points);
            assert_matches!(data.doc_values, DocValuesAccumulator::None);
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

        let mut buf_text = PostingsBuffer::new();
        let mut buf_reader = PostingsBuffer::new();
        for (term, tid_text) in pf_text.sorted_postings() {
            let tid_reader = pf_reader.term_id(term).unwrap_or_else(|| {
                panic!("reader chain missing term: {term}");
            });
            pf_text.postings.decode_into(tid_text, &mut buf_text);
            pf_reader.postings.decode_into(tid_reader, &mut buf_reader);
            assert_len_eq!(
                &buf_text.doc_ids,
                &buf_reader.doc_ids,
                "doc count mismatch for term: {term}"
            );
            for i in 0..buf_text.doc_ids.len() {
                assert_eq!(
                    buf_text.doc_ids[i], buf_reader.doc_ids[i],
                    "doc_id mismatch for term: {term}"
                );
                assert_eq!(
                    buf_text.freqs[i], buf_reader.freqs[i],
                    "freq mismatch for term: {term}"
                );
            }
            assert_eq!(
                buf_text.positions, buf_reader.positions,
                "positions mismatch for term: {term}"
            );
        }

        assert_eq!(pf_text.norms, pf_reader.norms);
    }

    #[test]
    fn test_large_reader_field_multi_chunk() {
        // 40 KB of text — exercises multiple 8 KB chunks in analyze_reader
        let text = "the quick brown fox jumps over the lazy dog ".repeat(1000);
        assert_gt!(text.len(), 32_000);

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

        let mut buf_text = PostingsBuffer::new();
        let mut buf_reader = PostingsBuffer::new();
        for (term, tid_text) in pf_text.sorted_postings() {
            let tid_reader = pf_reader.term_id(term).unwrap_or_else(|| {
                panic!("reader chain missing term: {term}");
            });
            pf_text.postings.decode_into(tid_text, &mut buf_text);
            pf_reader.postings.decode_into(tid_reader, &mut buf_reader);
            assert_len_eq!(
                &buf_text.doc_ids,
                &buf_reader.doc_ids,
                "doc count mismatch for term: {term}"
            );
            for i in 0..buf_text.doc_ids.len() {
                assert_eq!(
                    buf_text.doc_ids[i], buf_reader.doc_ids[i],
                    "doc_id mismatch for term: {term}"
                );
                assert_eq!(
                    buf_text.freqs[i], buf_reader.freqs[i],
                    "freq mismatch for term: {term}"
                );
            }
            assert_eq!(
                buf_text.positions, buf_reader.positions,
                "positions mismatch for term: {term}"
            );
        }

        assert_eq!(pf_text.norms, pf_reader.norms);
    }

    #[test]
    fn test_term_vectors_basic() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::text_field_with_term_vectors(
            "contents",
            "hello world",
        ));
        chain.process_document(doc, &analyzer).unwrap();

        let tv_docs = chain.term_vector_docs();
        assert_eq!(tv_docs.len(), 1);
        let tv_doc = &tv_docs[0];
        assert_eq!(tv_doc.fields.len(), 1);

        let tv_field = &tv_doc.fields[0];
        assert!(tv_field.has_positions);
        assert!(tv_field.has_offsets);
        assert!(!tv_field.has_payloads);
        assert_eq!(tv_field.terms.len(), 2);

        let hello = &tv_field.terms[0];
        assert_eq!(hello.term, "hello");
        assert_eq!(hello.freq, 1);
        assert_eq!(hello.positions, vec![0]);
        let hello_offsets = hello.offsets.as_ref().unwrap();
        assert_eq!(hello_offsets.start_offsets, vec![0]);
        assert_eq!(hello_offsets.end_offsets, vec![5]);

        let world = &tv_field.terms[1];
        assert_eq!(world.term, "world");
        assert_eq!(world.freq, 1);
        assert_eq!(world.positions, vec![1]);
        let world_offsets = world.offsets.as_ref().unwrap();
        assert_eq!(world_offsets.start_offsets, vec![6]);
        assert_eq!(world_offsets.end_offsets, vec![11]);
    }

    #[test]
    fn test_term_vectors_sorted_by_utf8_bytes() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::text_field_with_term_vectors(
            "contents",
            "banana apple cherry",
        ));
        chain.process_document(doc, &analyzer).unwrap();

        let terms = &chain.term_vector_docs()[0].fields[0].terms;
        let term_texts: Vec<&str> = terms.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(term_texts, vec!["apple", "banana", "cherry"]);
    }

    #[test]
    fn test_term_vectors_repeated_term() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::text_field_with_term_vectors(
            "contents",
            "hello world hello",
        ));
        chain.process_document(doc, &analyzer).unwrap();

        let terms = &chain.term_vector_docs()[0].fields[0].terms;
        let hello = terms.iter().find(|t| t.term == "hello").unwrap();
        assert_eq!(hello.freq, 2);
        assert_eq!(hello.positions, vec![0, 2]);
        let hello_offsets = hello.offsets.as_ref().unwrap();
        assert_eq!(hello_offsets.start_offsets, vec![0, 12]);
        assert_eq!(hello_offsets.end_offsets, vec![5, 17]);
    }

    #[test]
    fn test_term_vectors_no_tv_fields_empty_doc() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::text_field("contents", "hello world"));
        chain.process_document(doc, &analyzer).unwrap();

        let tv_docs = chain.term_vector_docs();
        assert_eq!(tv_docs.len(), 1);
        assert_is_empty!(tv_docs[0].fields);
    }

    #[test]
    fn test_term_vectors_multi_doc_alignment() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        // Doc 0: has TV
        let mut doc = Document::new();
        doc.add(document::text_field_with_term_vectors("contents", "hello"));
        chain.process_document(doc, &analyzer).unwrap();

        // Doc 1: no TV
        let mut doc = Document::new();
        doc.add(document::text_field("contents", "world"));
        chain.process_document(doc, &analyzer).unwrap();

        // Doc 2: has TV
        let mut doc = Document::new();
        doc.add(document::text_field_with_term_vectors("contents", "rust"));
        chain.process_document(doc, &analyzer).unwrap();

        let tv_docs = chain.term_vector_docs();
        assert_eq!(tv_docs.len(), 3);
        assert_eq!(tv_docs[0].fields.len(), 1);
        assert_is_empty!(tv_docs[1].fields);
        assert_eq!(tv_docs[2].fields.len(), 1);
    }

    #[test]
    fn test_term_vectors_feature_field_excluded() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::feature_field("features", "pagerank", 1.0));
        chain.process_document(doc, &analyzer).unwrap();

        let tv_docs = chain.term_vector_docs();
        assert_eq!(tv_docs.len(), 1);
        assert_is_empty!(tv_docs[0].fields);
    }

    #[test]
    fn test_term_vectors_positions_only() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        // Build a field type with TV + positions but no offsets
        let ft = crate::document::FieldTypeBuilder::new()
            .index_options(IndexOptions::DocsAndFreqsAndPositions)
            .tokenized(true)
            .store_term_vectors(true)
            .store_term_vector_positions(true)
            .build();
        let field = crate::document::Field::new(
            "contents".to_string(),
            ft,
            crate::document::FieldValue::Text("hello world".to_string()),
        );

        let mut doc = Document::new();
        doc.add(field);
        chain.process_document(doc, &analyzer).unwrap();

        let tv_field = &chain.term_vector_docs()[0].fields[0];
        assert!(tv_field.has_positions);
        assert!(!tv_field.has_offsets);

        let hello = &tv_field.terms[0];
        assert_eq!(hello.term, "hello");
        assert_eq!(hello.positions, vec![0]);
        assert_none!(hello.offsets);
    }

    #[test]
    fn test_term_vectors_keyword_with_tv() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        // Build a keyword-like field type with TV enabled
        let ft = crate::document::FieldTypeBuilder::new()
            .index_options(IndexOptions::DocsAndFreqs)
            .tokenized(false)
            .omit_norms(true)
            .store_term_vectors(true)
            .store_term_vector_positions(true)
            .store_term_vector_offsets(true)
            .build();
        let field = crate::document::Field::new(
            "tag".to_string(),
            ft,
            crate::document::FieldValue::Text("/foo/bar.txt".to_string()),
        );

        let mut doc = Document::new();
        doc.add(field);
        chain.process_document(doc, &analyzer).unwrap();

        let tv_field = &chain.term_vector_docs()[0].fields[0];
        assert_eq!(tv_field.terms.len(), 1);

        let term = &tv_field.terms[0];
        assert_eq!(term.term, "/foo/bar.txt");
        assert_eq!(term.freq, 1);
        assert_eq!(term.positions, vec![0]);
        let term_offsets = term.offsets.as_ref().unwrap();
        assert_eq!(term_offsets.start_offsets, vec![0]);
        assert_eq!(term_offsets.end_offsets, vec![12]);
    }

    #[test]
    fn test_term_vectors_mixed_fields() {
        let mut chain = IndexingChain::new();
        let analyzer = make_analyzer();

        let mut doc = Document::new();
        doc.add(document::text_field_with_term_vectors("body", "hello"));
        doc.add(document::text_field("title", "world"));
        chain.process_document(doc, &analyzer).unwrap();

        let tv_doc = &chain.term_vector_docs()[0];
        assert_eq!(tv_doc.fields.len(), 1);
        // Only "body" has TV — verify by field number
        let body_fi = chain.build_field_infos();
        let body_number = body_fi.field_info_by_name("body").unwrap().number();
        assert_eq!(tv_doc.fields[0].field_number, body_number);
    }

    #[test]
    fn measure_tv_ram_overhead() {
        let analyzer = make_analyzer();
        let text = "hello world foo bar baz alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau upsilon phi chi psi omega ".repeat(60);

        for n in [1, 10, 50] {
            let mut chain_no_tv = IndexingChain::new();
            let mut chain_tv = IndexingChain::new();
            for _ in 0..n {
                let mut doc = Document::new();
                doc.add(document::text_field("contents", &text));
                chain_no_tv.process_document(doc, &analyzer).unwrap();

                let mut doc2 = Document::new();
                doc2.add(document::text_field_with_term_vectors("contents", &text));
                chain_tv.process_document(doc2, &analyzer).unwrap();
            }
            let no_tv = chain_no_tv.ram_bytes_used();
            let tv = chain_tv.ram_bytes_used();
            eprintln!(
                "{n:>3} docs: no_tv={no_tv:>8}, tv={tv:>8}, overhead={:>8} ({:.1}x)",
                tv - no_tv,
                tv as f64 / no_tv as f64
            );
        }
    }
}
