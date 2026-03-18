// SPDX-License-Identifier: Apache-2.0

//! Document model and field types.
//!
//! A [`Document`] is a collection of [`Field`]s, each with a name, [`FieldType`],
//! and value. Factory functions like [`text_field`], [`keyword_field`], and
//! [`long_field`] create fields with common configurations. Specialized point
//! fields ([`lat_lon_point`], [`int_range_field`], [`long_range_field`],
//! [`float_range_field`], [`double_range_field`]) encode multi-dimensional
//! data for BKD tree indexing. [`feature_field`] stores feature name/value
//! pairs as term frequencies for static scoring. Doc-values-only factories
//! ([`numeric_doc_values_field`], [`binary_doc_values_field`],
//! [`sorted_doc_values_field`], [`sorted_set_doc_values_field`],
//! [`sorted_numeric_doc_values_field`]) create fields for sorting and faceting
//! without indexing or storing. The [`text_field_reader`] factory accepts a
//! [`Read`] source for streaming tokenization without buffering the entire
//! text in memory.

use std::fmt;
use std::io::Read;

use mem_dbg::MemSize;

/// Specifies what information is stored in the index for a field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, MemSize)]
#[mem_size_flat]
pub enum IndexOptions {
    /// Not indexed.
    None = 0,
    /// Only documents are indexed: term frequencies and positions are omitted.
    Docs = 1,
    /// Documents and term frequencies are indexed. Positions are omitted.
    DocsAndFreqs = 2,
    /// Documents, frequencies, and positions are indexed.
    DocsAndFreqsAndPositions = 3,
    /// Documents, frequencies, positions, and offsets are indexed.
    DocsAndFreqsAndPositionsAndOffsets = 4,
}

impl IndexOptions {
    pub fn has_freqs(self) -> bool {
        self >= IndexOptions::DocsAndFreqs
    }

    pub fn has_positions(self) -> bool {
        self >= IndexOptions::DocsAndFreqsAndPositions
    }
}

/// Specifies the type of doc values stored for a field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, MemSize)]
#[mem_size_flat]
pub enum DocValuesType {
    /// No doc values.
    None = 0,
    /// A per-document numeric value.
    Numeric = 1,
    /// A per-document byte[].
    Binary = 2,
    /// A pre-sorted byte[]. Only a single value per document is allowed.
    Sorted = 3,
    /// A pre-sorted numeric value. Multiple values per document are allowed.
    SortedNumeric = 4,
    /// A pre-sorted Set<byte[]>. Multiple values per document are allowed.
    SortedSet = 5,
}

/// Describes the properties of a field type.
#[derive(Clone, Debug)]
pub struct FieldType {
    stored: bool,
    tokenized: bool,
    omit_norms: bool,
    index_options: IndexOptions,
    doc_values_type: DocValuesType,
    store_term_vectors: bool,
    store_term_vector_offsets: bool,
    store_term_vector_positions: bool,
    store_term_vector_payloads: bool,
    /// Number of dimensions for point values (0 = not a point field).
    point_dimension_count: u32,
    /// Number of dimensions used for the index (must be <= point_dimension_count).
    point_index_dimension_count: u32,
    /// Number of bytes per point dimension.
    point_num_bytes: u32,
}

impl FieldType {
    pub fn new() -> Self {
        Self {
            stored: false,
            tokenized: false,
            omit_norms: false,
            index_options: IndexOptions::None,
            doc_values_type: DocValuesType::None,
            store_term_vectors: false,
            store_term_vector_offsets: false,
            store_term_vector_positions: false,
            store_term_vector_payloads: false,
            point_dimension_count: 0,
            point_index_dimension_count: 0,
            point_num_bytes: 0,
        }
    }

    pub fn stored(&self) -> bool {
        self.stored
    }

    pub fn tokenized(&self) -> bool {
        self.tokenized
    }

    pub fn omit_norms(&self) -> bool {
        self.omit_norms
    }

    pub fn index_options(&self) -> IndexOptions {
        self.index_options
    }

    pub fn doc_values_type(&self) -> DocValuesType {
        self.doc_values_type
    }

    pub fn store_term_vectors(&self) -> bool {
        self.store_term_vectors
    }

    pub fn store_term_vector_offsets(&self) -> bool {
        self.store_term_vector_offsets
    }

    pub fn store_term_vector_positions(&self) -> bool {
        self.store_term_vector_positions
    }

    pub fn store_term_vector_payloads(&self) -> bool {
        self.store_term_vector_payloads
    }

    pub fn point_dimension_count(&self) -> u32 {
        self.point_dimension_count
    }

    pub fn point_index_dimension_count(&self) -> u32 {
        self.point_index_dimension_count
    }

    pub fn point_num_bytes(&self) -> u32 {
        self.point_num_bytes
    }

    pub fn is_indexed(&self) -> bool {
        self.index_options != IndexOptions::None
    }

    pub fn has_points(&self) -> bool {
        self.point_dimension_count > 0
    }

    pub fn has_doc_values(&self) -> bool {
        self.doc_values_type != DocValuesType::None
    }

    pub fn has_norms(&self) -> bool {
        self.is_indexed() && !self.omit_norms
    }
}

impl Default for FieldType {
    fn default() -> Self {
        Self::new()
    }
}

/// The stored value for a field.
pub enum FieldValue {
    /// A text string (for keyword, text, and stored string fields).
    Text(String),
    /// A 32-bit integer (for IntField).
    Int(i32),
    /// A long integer (for numeric fields like LongField).
    Long(i64),
    /// A 32-bit float (for FloatField).
    Float(f32),
    /// A 64-bit double (for DoubleField).
    Double(f64),
    /// Raw bytes (for binary fields).
    Bytes(Vec<u8>),
    /// A streaming text source (for large text fields).
    ///
    /// Reader fields are tokenized and indexed but cannot be stored or used
    /// for doc values or point lookups. Use [`text_field_reader`] to create
    /// a field with this variant.
    Reader(Box<dyn Read + Send>),
    /// A feature field that stores a term with an explicit frequency.
    ///
    /// Used by [`feature_field`] to encode feature name/value pairs in the
    /// posting list without tokenization.
    Feature {
        /// The feature name, stored as the indexed term.
        term: String,
        /// The encoded frequency (feature_value bits >> 15).
        freq: i32,
    },
}

impl fmt::Debug for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldValue::Text(s) => f.debug_tuple("Text").field(s).finish(),
            FieldValue::Int(v) => f.debug_tuple("Int").field(v).finish(),
            FieldValue::Long(v) => f.debug_tuple("Long").field(v).finish(),
            FieldValue::Float(v) => f.debug_tuple("Float").field(v).finish(),
            FieldValue::Double(v) => f.debug_tuple("Double").field(v).finish(),
            FieldValue::Bytes(b) => f.debug_tuple("Bytes").field(b).finish(),
            FieldValue::Reader(_) => f.debug_tuple("Reader").field(&"...").finish(),
            FieldValue::Feature { term, freq } => f
                .debug_struct("Feature")
                .field("term", term)
                .field("freq", freq)
                .finish(),
        }
    }
}

/// A field in a document.
#[derive(Debug)]
pub struct Field {
    name: String,
    field_type: FieldType,
    value: FieldValue,
}

impl Field {
    pub fn new(name: String, field_type: FieldType, value: FieldValue) -> Self {
        Self {
            name,
            field_type,
            value,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn field_type(&self) -> &FieldType {
        &self.field_type
    }

    pub fn value(&self) -> &FieldValue {
        &self.value
    }

    /// Returns a mutable reference to the field value.
    pub(crate) fn value_mut(&mut self) -> &mut FieldValue {
        &mut self.value
    }

    /// Returns the string value, if this field holds text.
    pub fn string_value(&self) -> Option<&str> {
        match &self.value {
            FieldValue::Text(s) => Some(s),
            FieldValue::Feature { term, .. } => Some(term),
            _ => None,
        }
    }

    /// Returns the numeric value as i64, if this field holds a numeric type.
    /// Int and Float are widened/converted to match Java's behavior.
    pub fn numeric_value(&self) -> Option<i64> {
        match &self.value {
            FieldValue::Int(v) => Some(*v as i64),
            FieldValue::Long(v) => Some(*v),
            FieldValue::Float(v) => {
                Some(crate::util::numeric_utils::float_to_sortable_int(*v) as i64)
            }
            FieldValue::Double(v) => Some(crate::util::numeric_utils::double_to_sortable_long(*v)),
            _ => None,
        }
    }

    /// Returns the bytes for point indexing, if applicable.
    pub fn point_bytes(&self) -> Option<Vec<u8>> {
        if !self.field_type.has_points() {
            return None;
        }
        match &self.value {
            FieldValue::Int(v) => {
                Some(crate::util::numeric_utils::int_to_sortable_bytes(*v).to_vec())
            }
            FieldValue::Long(v) => {
                Some(crate::util::numeric_utils::long_to_sortable_bytes(*v).to_vec())
            }
            FieldValue::Float(v) => {
                Some(crate::util::numeric_utils::float_to_sortable_bytes(*v).to_vec())
            }
            FieldValue::Double(v) => {
                Some(crate::util::numeric_utils::double_to_sortable_bytes(*v).to_vec())
            }
            FieldValue::Bytes(b) => Some(b.clone()),
            _ => None,
        }
    }

    /// Returns the bytes to store, if this field is stored.
    pub fn stored_value(&self) -> Option<StoredValue> {
        if !self.field_type.stored() {
            return None;
        }
        match &self.value {
            FieldValue::Text(s) => Some(StoredValue::String(s.clone())),
            FieldValue::Int(v) => Some(StoredValue::Int(*v)),
            FieldValue::Long(v) => Some(StoredValue::Long(*v)),
            FieldValue::Float(v) => Some(StoredValue::Float(*v)),
            FieldValue::Double(v) => Some(StoredValue::Double(*v)),
            FieldValue::Bytes(b) => Some(StoredValue::Bytes(b.clone())),
            FieldValue::Reader(_) | FieldValue::Feature { .. } => None,
        }
    }
}

/// A stored value that can be read back from the index.
#[derive(Clone, Debug, MemSize)]
pub enum StoredValue {
    String(String),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
}

/// A document to be indexed.
#[derive(Debug, Default)]
pub struct Document {
    pub fields: Vec<Field>,
}

impl Document {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn add(&mut self, field: Field) {
        self.fields.push(field);
    }
}

/// Creates a stored keyword field with an inverted index and sorted-set doc values.
///
/// Indexed with `DOCS` only (no freqs/positions), norms omitted, not tokenized.
pub fn keyword_field(name: &str, value: &str) -> Field {
    let mut ft = FieldType::new();
    ft.stored = true;
    ft.index_options = IndexOptions::Docs;
    ft.omit_norms = true;
    ft.tokenized = false;
    ft.doc_values_type = DocValuesType::SortedSet;
    Field::new(name.to_string(), ft, FieldValue::Text(value.to_string()))
}

/// Creates an unstored long field with point indexing and sorted-numeric doc values.
///
/// Points: 1 dimension, 8 bytes. No posting list.
pub fn long_field(name: &str, value: i64) -> Field {
    let mut ft = FieldType::new();
    ft.point_dimension_count = 1;
    ft.point_index_dimension_count = 1;
    ft.point_num_bytes = 8;
    ft.doc_values_type = DocValuesType::SortedNumeric;
    Field::new(name.to_string(), ft, FieldValue::Long(value))
}

/// Creates an unstored, tokenized text field with positions.
pub fn text_field(name: &str, value: &str) -> Field {
    let mut ft = FieldType::new();
    ft.index_options = IndexOptions::DocsAndFreqsAndPositions;
    ft.tokenized = true;
    Field::new(name.to_string(), ft, FieldValue::Text(value.to_string()))
}

/// Creates a tokenized text field backed by a [`Read`] source.
///
/// The reader is consumed during indexing, tokenizing in chunks without
/// buffering the entire content in memory. Reader fields are indexed but
/// cannot be stored.
pub fn text_field_reader(name: &str, reader: impl Read + Send + 'static) -> Field {
    let mut ft = FieldType::new();
    ft.index_options = IndexOptions::DocsAndFreqsAndPositions;
    ft.tokenized = true;
    Field::new(name.to_string(), ft, FieldValue::Reader(Box::new(reader)))
}

/// Creates an inverted-only string field (no doc values).
///
/// Indexed with `DOCS` only, norms omitted, not tokenized.
pub fn string_field(name: &str, value: &str, stored: bool) -> Field {
    let mut ft = FieldType::new();
    ft.stored = stored;
    ft.index_options = IndexOptions::Docs;
    ft.omit_norms = true;
    ft.tokenized = false;
    Field::new(name.to_string(), ft, FieldValue::Text(value.to_string()))
}

/// Creates an int field with point indexing and sorted-numeric doc values.
///
/// Points: 1 dimension, 4 bytes.
pub fn int_field(name: &str, value: i32, stored: bool) -> Field {
    let mut ft = FieldType::new();
    ft.stored = stored;
    ft.point_dimension_count = 1;
    ft.point_index_dimension_count = 1;
    ft.point_num_bytes = 4;
    ft.doc_values_type = DocValuesType::SortedNumeric;
    Field::new(name.to_string(), ft, FieldValue::Int(value))
}

/// Creates a float field with point indexing and sorted-numeric doc values.
///
/// Points: 1 dimension, 4 bytes.
pub fn float_field(name: &str, value: f32, stored: bool) -> Field {
    let mut ft = FieldType::new();
    ft.stored = stored;
    ft.point_dimension_count = 1;
    ft.point_index_dimension_count = 1;
    ft.point_num_bytes = 4;
    ft.doc_values_type = DocValuesType::SortedNumeric;
    Field::new(name.to_string(), ft, FieldValue::Float(value))
}

/// Creates a double field with point indexing and sorted-numeric doc values.
///
/// Points: 1 dimension, 8 bytes.
pub fn double_field(name: &str, value: f64, stored: bool) -> Field {
    let mut ft = FieldType::new();
    ft.stored = stored;
    ft.point_dimension_count = 1;
    ft.point_index_dimension_count = 1;
    ft.point_num_bytes = 8;
    ft.doc_values_type = DocValuesType::SortedNumeric;
    Field::new(name.to_string(), ft, FieldValue::Double(value))
}

/// Creates a per-document numeric value for sorting and faceting (doc-values-only).
///
/// Not stored, not indexed, no points.
pub fn numeric_doc_values_field(name: &str, value: i64) -> Field {
    let mut ft = FieldType::new();
    ft.doc_values_type = DocValuesType::Numeric;
    Field::new(name.to_string(), ft, FieldValue::Long(value))
}

/// Creates a per-document byte array for arbitrary binary data (doc-values-only).
///
/// Not stored, not indexed, no points.
pub fn binary_doc_values_field(name: &str, value: Vec<u8>) -> Field {
    let mut ft = FieldType::new();
    ft.doc_values_type = DocValuesType::Binary;
    Field::new(name.to_string(), ft, FieldValue::Bytes(value))
}

/// Creates a per-document ordinal-mapped byte array (doc-values-only).
///
/// Values are deduplicated and ordinal-encoded. Not stored, not indexed, no points.
pub fn sorted_doc_values_field(name: &str, value: &[u8]) -> Field {
    let mut ft = FieldType::new();
    ft.doc_values_type = DocValuesType::Sorted;
    Field::new(name.to_string(), ft, FieldValue::Bytes(value.to_vec()))
}

/// Creates a sorted-set doc values field (doc-values-only).
///
/// Unlike [`keyword_field`], this has no inverted index or stored value.
pub fn sorted_set_doc_values_field(name: &str, value: &str) -> Field {
    let mut ft = FieldType::new();
    ft.doc_values_type = DocValuesType::SortedSet;
    Field::new(name.to_string(), ft, FieldValue::Text(value.to_string()))
}

/// Creates a sorted-numeric doc values field (doc-values-only).
///
/// Unlike [`long_field`], this has no point index or stored value.
pub fn sorted_numeric_doc_values_field(name: &str, value: i64) -> Field {
    let mut ft = FieldType::new();
    ft.doc_values_type = DocValuesType::SortedNumeric;
    Field::new(name.to_string(), ft, FieldValue::Long(value))
}

/// Creates a stored-only string field (no indexing).
pub fn stored_string_field(name: &str, value: &str) -> Field {
    let mut ft = FieldType::new();
    ft.stored = true;
    Field::new(name.to_string(), ft, FieldValue::Text(value.to_string()))
}

/// Creates a stored-only int field (no indexing).
pub fn stored_int_field(name: &str, value: i32) -> Field {
    let mut ft = FieldType::new();
    ft.stored = true;
    Field::new(name.to_string(), ft, FieldValue::Int(value))
}

/// Creates a stored-only long field (no indexing).
pub fn stored_long_field(name: &str, value: i64) -> Field {
    let mut ft = FieldType::new();
    ft.stored = true;
    Field::new(name.to_string(), ft, FieldValue::Long(value))
}

/// Creates a stored-only float field (no indexing).
pub fn stored_float_field(name: &str, value: f32) -> Field {
    let mut ft = FieldType::new();
    ft.stored = true;
    Field::new(name.to_string(), ft, FieldValue::Float(value))
}

/// Creates a stored-only double field (no indexing).
pub fn stored_double_field(name: &str, value: f64) -> Field {
    let mut ft = FieldType::new();
    ft.stored = true;
    Field::new(name.to_string(), ft, FieldValue::Double(value))
}

/// Creates a stored-only bytes field (no indexing).
pub fn stored_bytes_field(name: &str, value: Vec<u8>) -> Field {
    let mut ft = FieldType::new();
    ft.stored = true;
    Field::new(name.to_string(), ft, FieldValue::Bytes(value))
}

/// Creates an indexed lat/lon point field.
///
/// Points: 2 dimensions, 4 bytes each. Not stored, no doc values.
/// Latitude must be in [-90, 90], longitude in [-180, 180].
pub fn lat_lon_point(name: &str, lat: f64, lon: f64) -> Field {
    let encoded_lat = crate::util::numeric_utils::encode_latitude(lat);
    let encoded_lon = crate::util::numeric_utils::encode_longitude(lon);
    let mut bytes = Vec::with_capacity(8);
    bytes.extend_from_slice(&crate::util::numeric_utils::int_to_sortable_bytes(
        encoded_lat,
    ));
    bytes.extend_from_slice(&crate::util::numeric_utils::int_to_sortable_bytes(
        encoded_lon,
    ));
    let mut ft = FieldType::new();
    ft.point_dimension_count = 2;
    ft.point_index_dimension_count = 2;
    ft.point_num_bytes = 4;
    Field::new(name.to_string(), ft, FieldValue::Bytes(bytes))
}

/// Creates an integer range field for indexing a range per dimension.
///
/// Points: `dims*2` dimensions, 4 bytes each. Layout: `[min1..minN, max1..maxN]`.
pub fn int_range_field(name: &str, mins: &[i32], maxs: &[i32]) -> Field {
    let bytes = crate::util::numeric_utils::encode_int_range(mins, maxs);
    let dims = (mins.len() * 2) as u32;
    let mut ft = FieldType::new();
    ft.point_dimension_count = dims;
    ft.point_index_dimension_count = dims;
    ft.point_num_bytes = 4;
    Field::new(name.to_string(), ft, FieldValue::Bytes(bytes))
}

/// Creates a long range field for indexing a range per dimension.
///
/// Points: `dims*2` dimensions, 8 bytes each. Layout: `[min1..minN, max1..maxN]`.
pub fn long_range_field(name: &str, mins: &[i64], maxs: &[i64]) -> Field {
    let bytes = crate::util::numeric_utils::encode_long_range(mins, maxs);
    let dims = (mins.len() * 2) as u32;
    let mut ft = FieldType::new();
    ft.point_dimension_count = dims;
    ft.point_index_dimension_count = dims;
    ft.point_num_bytes = 8;
    Field::new(name.to_string(), ft, FieldValue::Bytes(bytes))
}

/// Creates a float range field for indexing a range per dimension.
///
/// Points: `dims*2` dimensions, 4 bytes each. Layout: `[min1..minN, max1..maxN]`.
pub fn float_range_field(name: &str, mins: &[f32], maxs: &[f32]) -> Field {
    let bytes = crate::util::numeric_utils::encode_float_range(mins, maxs);
    let dims = (mins.len() * 2) as u32;
    let mut ft = FieldType::new();
    ft.point_dimension_count = dims;
    ft.point_index_dimension_count = dims;
    ft.point_num_bytes = 4;
    Field::new(name.to_string(), ft, FieldValue::Bytes(bytes))
}

/// Creates a double range field for indexing a range per dimension.
///
/// Points: `dims*2` dimensions, 8 bytes each. Layout: `[min1..minN, max1..maxN]`.
pub fn double_range_field(name: &str, mins: &[f64], maxs: &[f64]) -> Field {
    let bytes = crate::util::numeric_utils::encode_double_range(mins, maxs);
    let dims = (mins.len() * 2) as u32;
    let mut ft = FieldType::new();
    ft.point_dimension_count = dims;
    ft.point_index_dimension_count = dims;
    ft.point_num_bytes = 8;
    Field::new(name.to_string(), ft, FieldValue::Bytes(bytes))
}

/// Creates a feature field that stores a feature name/value pair in the posting list.
///
/// The feature value is encoded as a term frequency: `float_bits >> 15`.
/// FieldType: not tokenized, omit_norms, DocsAndFreqs.
/// The feature name becomes the indexed term.
pub fn feature_field(name: &str, feature_name: &str, feature_value: f32) -> Field {
    assert!(
        feature_value.is_finite() && feature_value > 0.0,
        "feature_value must be finite and positive, got {feature_value}"
    );
    let freq = (f32::to_bits(feature_value) >> 15) as i32;
    let mut ft = FieldType::new();
    ft.tokenized = false;
    ft.omit_norms = true;
    ft.index_options = IndexOptions::DocsAndFreqs;
    Field::new(
        name.to_string(),
        ft,
        FieldValue::Feature {
            term: feature_name.to_string(),
            freq,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_field() {
        let f = keyword_field("path", "/foo/bar.txt");
        assert_eq!(f.name(), "path");
        assert_eq!(f.field_type().index_options(), IndexOptions::Docs);
        assert!(f.field_type().omit_norms());
        assert!(!f.field_type().tokenized());
        assert!(f.field_type().stored());
        assert_eq!(f.field_type().doc_values_type(), DocValuesType::SortedSet);
        assert_eq!(f.string_value(), Some("/foo/bar.txt"));
    }

    #[test]
    fn test_long_field() {
        let f = long_field("modified", 1234567890);
        assert_eq!(f.name(), "modified");
        assert_eq!(f.field_type().index_options(), IndexOptions::None);
        assert!(!f.field_type().stored());
        assert_eq!(
            f.field_type().doc_values_type(),
            DocValuesType::SortedNumeric
        );
        assert_eq!(f.field_type().point_dimension_count(), 1);
        assert_eq!(f.field_type().point_num_bytes(), 8);
        assert_eq!(f.numeric_value(), Some(1234567890));
    }

    #[test]
    fn test_text_field() {
        let f = text_field("contents", "hello world");
        assert_eq!(f.name(), "contents");
        assert_eq!(
            f.field_type().index_options(),
            IndexOptions::DocsAndFreqsAndPositions
        );
        assert!(f.field_type().tokenized());
        assert!(!f.field_type().stored());
        assert!(!f.field_type().omit_norms());
        assert_eq!(f.field_type().doc_values_type(), DocValuesType::None);
        assert_eq!(f.string_value(), Some("hello world"));
    }

    #[test]
    fn test_document() {
        let mut doc = Document::new();
        doc.add(keyword_field("path", "/foo.txt"));
        doc.add(long_field("modified", 100));
        doc.add(text_field("contents", "hello"));
        assert_eq!(doc.fields.len(), 3);
    }

    #[test]
    fn test_point_bytes() {
        let f = long_field("modified", 42);
        let pb = f.point_bytes().unwrap();
        assert_eq!(pb.len(), 8);
        // 42 with sign-flip: 0x800000000000002A
        assert_eq!(pb, [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A]);
    }

    #[test]
    fn test_stored_value() {
        let f = keyword_field("path", "/foo.txt");
        assert_some!(f.stored_value());

        let f = text_field("contents", "hello");
        assert_none!(f.stored_value()); // not stored
    }

    #[test]
    fn test_index_options_ordering() {
        assert_lt!(IndexOptions::None, IndexOptions::Docs);
        assert_lt!(IndexOptions::Docs, IndexOptions::DocsAndFreqs);
        assert_lt!(
            IndexOptions::DocsAndFreqs,
            IndexOptions::DocsAndFreqsAndPositions
        );
        assert_lt!(
            IndexOptions::DocsAndFreqsAndPositions,
            IndexOptions::DocsAndFreqsAndPositionsAndOffsets
        );
    }

    #[test]
    fn test_field_type_helpers() {
        let ft_keyword = keyword_field("x", "y").field_type().clone();
        assert!(ft_keyword.is_indexed());
        assert!(!ft_keyword.has_points());
        assert!(ft_keyword.has_doc_values());
        assert!(!ft_keyword.has_norms()); // omit_norms=true

        let ft_long = long_field("x", 1).field_type().clone();
        assert!(!ft_long.is_indexed());
        assert!(ft_long.has_points());
        assert!(ft_long.has_doc_values());

        let ft_text = text_field("x", "y").field_type().clone();
        assert!(ft_text.is_indexed());
        assert!(!ft_text.has_points());
        assert!(!ft_text.has_doc_values());
        assert!(ft_text.has_norms());
    }

    #[test]
    fn test_string_field() {
        let f = string_field("title", "hello", true);
        assert_eq!(f.name(), "title");
        assert_eq!(f.field_type().index_options(), IndexOptions::Docs);
        assert!(f.field_type().omit_norms());
        assert!(!f.field_type().tokenized());
        assert!(f.field_type().stored());
        assert_eq!(f.field_type().doc_values_type(), DocValuesType::None);
        assert_eq!(f.string_value(), Some("hello"));

        let f_unstored = string_field("tag", "rust", false);
        assert!(!f_unstored.field_type().stored());
        assert_none!(f_unstored.stored_value());
    }

    #[test]
    fn test_int_field() {
        let f = int_field("size", 42, true);
        assert_eq!(f.name(), "size");
        assert!(f.field_type().stored());
        assert_eq!(f.field_type().point_dimension_count(), 1);
        assert_eq!(f.field_type().point_num_bytes(), 4);
        assert_eq!(
            f.field_type().doc_values_type(),
            DocValuesType::SortedNumeric
        );
        assert_eq!(f.numeric_value(), Some(42));

        let pb = f.point_bytes().unwrap();
        assert_eq!(pb.len(), 4);
        assert_eq!(
            pb,
            crate::util::numeric_utils::int_to_sortable_bytes(42).to_vec()
        );

        if let Some(StoredValue::Int(v)) = f.stored_value() {
            assert_eq!(v, 42);
        } else {
            panic!("expected StoredValue::Int");
        }
    }

    #[test]
    fn test_float_field() {
        let f = float_field("score", 1.5, true);
        assert_eq!(f.name(), "score");
        assert!(f.field_type().stored());
        assert_eq!(f.field_type().point_dimension_count(), 1);
        assert_eq!(f.field_type().point_num_bytes(), 4);
        assert_eq!(
            f.field_type().doc_values_type(),
            DocValuesType::SortedNumeric
        );
        assert_eq!(
            f.numeric_value(),
            Some(crate::util::numeric_utils::float_to_sortable_int(1.5) as i64)
        );

        let pb = f.point_bytes().unwrap();
        assert_eq!(pb.len(), 4);
        assert_eq!(
            pb,
            crate::util::numeric_utils::float_to_sortable_bytes(1.5).to_vec()
        );

        if let Some(StoredValue::Float(v)) = f.stored_value() {
            assert_eq!(v, 1.5);
        } else {
            panic!("expected StoredValue::Float");
        }
    }

    #[test]
    fn test_double_field() {
        let f = double_field("rating", 9.87, true);
        assert_eq!(f.name(), "rating");
        assert!(f.field_type().stored());
        assert_eq!(f.field_type().point_dimension_count(), 1);
        assert_eq!(f.field_type().point_num_bytes(), 8);
        assert_eq!(
            f.field_type().doc_values_type(),
            DocValuesType::SortedNumeric
        );
        assert_eq!(
            f.numeric_value(),
            Some(crate::util::numeric_utils::double_to_sortable_long(9.87))
        );

        let pb = f.point_bytes().unwrap();
        assert_eq!(pb.len(), 8);
        assert_eq!(
            pb,
            crate::util::numeric_utils::double_to_sortable_bytes(9.87).to_vec()
        );

        if let Some(StoredValue::Double(v)) = f.stored_value() {
            assert_eq!(v, 9.87);
        } else {
            panic!("expected StoredValue::Double");
        }
    }

    #[test]
    fn test_stored_field_variants() {
        let f = stored_string_field("notes", "hello");
        assert!(f.field_type().stored());
        assert!(!f.field_type().is_indexed());
        assert!(!f.field_type().has_points());
        if let Some(StoredValue::String(s)) = f.stored_value() {
            assert_eq!(s, "hello");
        } else {
            panic!("expected StoredValue::String");
        }

        let f = stored_int_field("count", 99);
        if let Some(StoredValue::Int(v)) = f.stored_value() {
            assert_eq!(v, 99);
        } else {
            panic!("expected StoredValue::Int");
        }

        let f = stored_long_field("big", 123456789);
        if let Some(StoredValue::Long(v)) = f.stored_value() {
            assert_eq!(v, 123456789);
        } else {
            panic!("expected StoredValue::Long");
        }

        let f = stored_float_field("ratio", 1.5);
        if let Some(StoredValue::Float(v)) = f.stored_value() {
            assert_eq!(v, 1.5);
        } else {
            panic!("expected StoredValue::Float");
        }

        let f = stored_double_field("precise", 7.654);
        if let Some(StoredValue::Double(v)) = f.stored_value() {
            assert_eq!(v, 7.654);
        } else {
            panic!("expected StoredValue::Double");
        }

        let f = stored_bytes_field("raw", vec![1, 2, 3]);
        if let Some(StoredValue::Bytes(b)) = f.stored_value() {
            assert_eq!(b, vec![1, 2, 3]);
        } else {
            panic!("expected StoredValue::Bytes");
        }
    }

    #[test]
    fn test_numeric_doc_values_field() {
        let f = numeric_doc_values_field("count", 42);
        assert_eq!(f.name(), "count");
        assert_eq!(f.field_type().doc_values_type(), DocValuesType::Numeric);
        assert!(!f.field_type().stored());
        assert!(!f.field_type().is_indexed());
        assert!(!f.field_type().has_points());
        assert_eq!(f.numeric_value(), Some(42));
    }

    #[test]
    fn test_binary_doc_values_field() {
        let f = binary_doc_values_field("payload", vec![1, 2, 3]);
        assert_eq!(f.name(), "payload");
        assert_eq!(f.field_type().doc_values_type(), DocValuesType::Binary);
        assert!(!f.field_type().stored());
        assert!(!f.field_type().is_indexed());
        assert!(!f.field_type().has_points());
        if let FieldValue::Bytes(b) = f.value() {
            assert_eq!(b, &[1, 2, 3]);
        } else {
            panic!("expected FieldValue::Bytes");
        }
    }

    #[test]
    fn test_sorted_doc_values_field() {
        let f = sorted_doc_values_field("category", b"animals");
        assert_eq!(f.name(), "category");
        assert_eq!(f.field_type().doc_values_type(), DocValuesType::Sorted);
        assert!(!f.field_type().stored());
        assert!(!f.field_type().is_indexed());
        assert!(!f.field_type().has_points());
    }

    #[test]
    fn test_sorted_set_doc_values_field() {
        let f = sorted_set_doc_values_field("tag", "rust");
        assert_eq!(f.name(), "tag");
        assert_eq!(f.field_type().doc_values_type(), DocValuesType::SortedSet);
        assert!(!f.field_type().stored());
        assert!(!f.field_type().is_indexed());
        assert!(!f.field_type().has_points());
    }

    #[test]
    fn test_sorted_numeric_doc_values_field() {
        let f = sorted_numeric_doc_values_field("timestamp", 1000);
        assert_eq!(f.name(), "timestamp");
        assert_eq!(
            f.field_type().doc_values_type(),
            DocValuesType::SortedNumeric
        );
        assert!(!f.field_type().stored());
        assert!(!f.field_type().is_indexed());
        assert!(!f.field_type().has_points());
        assert_eq!(f.numeric_value(), Some(1000));
    }

    #[test]
    fn test_field_type_default() {
        let ft = FieldType::default();
        assert!(!ft.stored());
        assert!(!ft.tokenized());
        assert!(!ft.omit_norms());
        assert_eq!(ft.index_options(), IndexOptions::None);
        assert_eq!(ft.doc_values_type(), DocValuesType::None);
        assert!(!ft.store_term_vectors());
        assert!(!ft.store_term_vector_offsets());
        assert!(!ft.store_term_vector_positions());
        assert!(!ft.store_term_vector_payloads());
        assert_eq!(ft.point_dimension_count(), 0);
        assert_eq!(ft.point_index_dimension_count(), 0);
        assert_eq!(ft.point_num_bytes(), 0);
    }

    #[test]
    fn test_numeric_value_non_numeric() {
        let f = keyword_field("path", "/foo");
        assert_none!(f.numeric_value());
    }

    #[test]
    fn test_point_bytes_non_point() {
        let f = text_field("contents", "hello");
        assert_none!(f.point_bytes());
    }

    #[test]
    fn test_point_bytes_bytes_field() {
        // A field type with points and a Bytes value
        let mut ft = FieldType::new();
        ft.point_dimension_count = 1;
        ft.point_index_dimension_count = 1;
        ft.point_num_bytes = 4;
        let f = Field::new(
            "raw_point".to_string(),
            ft,
            FieldValue::Bytes(vec![0x80, 0x00, 0x00, 0x2A]),
        );
        let pb = f.point_bytes().unwrap();
        assert_eq!(pb, vec![0x80, 0x00, 0x00, 0x2A]);
    }

    #[test]
    fn test_field_value_debug_all_variants() {
        // Exercise Debug for all FieldValue variants
        let cases: Vec<FieldValue> = vec![
            FieldValue::Text("hello".to_string()),
            FieldValue::Int(42),
            FieldValue::Long(100),
            FieldValue::Float(1.5),
            FieldValue::Double(2.5),
            FieldValue::Bytes(vec![1, 2]),
            FieldValue::Reader(Box::new(std::io::Cursor::new(vec![]))),
            FieldValue::Feature {
                term: "feat".to_string(),
                freq: 100,
            },
        ];
        for val in &cases {
            let s = format!("{:?}", val);
            assert_not_empty!(s);
        }
    }

    #[test]
    fn test_int_field_not_stored() {
        let f = int_field("x", 10, false);
        assert!(!f.field_type().stored());
        assert_none!(f.stored_value());
        assert_some!(f.point_bytes());
    }

    #[test]
    fn test_text_field_reader() {
        let f = text_field_reader("contents", std::io::Cursor::new(b"hello world".to_vec()));
        assert_eq!(f.name(), "contents");
        assert_eq!(
            f.field_type().index_options(),
            IndexOptions::DocsAndFreqsAndPositions
        );
        assert!(f.field_type().tokenized());
        assert!(!f.field_type().stored());
        assert_matches!(f.value(), FieldValue::Reader(_));
        assert_none!(f.string_value());
        assert_none!(f.stored_value());
        assert_none!(f.point_bytes());
    }

    #[test]
    fn test_field_value_debug() {
        let reader_val = FieldValue::Reader(Box::new(std::io::Cursor::new(vec![])));
        let debug_str = format!("{:?}", reader_val);
        assert_contains!(debug_str, "Reader");
    }

    #[test]
    fn test_lat_lon_point() {
        let f = lat_lon_point("location", 40.7128, -74.006);
        assert_eq!(f.name(), "location");
        assert_eq!(f.field_type().point_dimension_count(), 2);
        assert_eq!(f.field_type().point_index_dimension_count(), 2);
        assert_eq!(f.field_type().point_num_bytes(), 4);
        assert!(!f.field_type().stored());
        assert_eq!(f.field_type().doc_values_type(), DocValuesType::None);
        assert!(!f.field_type().is_indexed());
        assert!(f.field_type().has_points());

        let pb = f.point_bytes().unwrap();
        assert_len_eq_x!(&pb, 8);

        // Verify encoded bytes match expected values
        let expected_lat = crate::util::numeric_utils::int_to_sortable_bytes(
            crate::util::numeric_utils::encode_latitude(40.7128),
        );
        let expected_lon = crate::util::numeric_utils::int_to_sortable_bytes(
            crate::util::numeric_utils::encode_longitude(-74.006),
        );
        assert_eq!(&pb[0..4], &expected_lat);
        assert_eq!(&pb[4..8], &expected_lon);
    }

    #[test]
    fn test_int_range_field() {
        let f = int_range_field("range", &[10, 20], &[30, 40]);
        assert_eq!(f.name(), "range");
        assert_eq!(f.field_type().point_dimension_count(), 4);
        assert_eq!(f.field_type().point_index_dimension_count(), 4);
        assert_eq!(f.field_type().point_num_bytes(), 4);
        assert!(!f.field_type().stored());

        let pb = f.point_bytes().unwrap();
        assert_len_eq_x!(&pb, 16); // 2 dims * 2 * 4 bytes
    }

    #[test]
    fn test_long_range_field() {
        let f = long_range_field("range", &[100], &[200]);
        assert_eq!(f.field_type().point_dimension_count(), 2);
        assert_eq!(f.field_type().point_num_bytes(), 8);

        let pb = f.point_bytes().unwrap();
        assert_len_eq_x!(&pb, 16); // 1 dim * 2 * 8 bytes
    }

    #[test]
    fn test_float_range_field() {
        let f = float_range_field("range", &[1.0], &[2.0]);
        assert_eq!(f.field_type().point_dimension_count(), 2);
        assert_eq!(f.field_type().point_num_bytes(), 4);

        let pb = f.point_bytes().unwrap();
        assert_len_eq_x!(&pb, 8);
    }

    #[test]
    fn test_double_range_field() {
        let f = double_range_field("range", &[1.0, 2.0], &[3.0, 4.0]);
        assert_eq!(f.field_type().point_dimension_count(), 4);
        assert_eq!(f.field_type().point_num_bytes(), 8);

        let pb = f.point_bytes().unwrap();
        assert_len_eq_x!(&pb, 32); // 2 dims * 2 * 8 bytes
    }

    #[test]
    fn test_feature_field() {
        let f = feature_field("features", "pagerank", 1.0);
        assert_eq!(f.name(), "features");
        assert!(!f.field_type().tokenized());
        assert!(f.field_type().omit_norms());
        assert_eq!(f.field_type().index_options(), IndexOptions::DocsAndFreqs);
        assert!(!f.field_type().stored());
        assert!(!f.field_type().has_points());

        // 1.0f32 bits = 0x3F800000, >> 15 = 0x7F00 = 32512
        assert_matches!(f.value(), FieldValue::Feature { term, freq }
            if term == "pagerank" && *freq == 32512);

        assert_eq!(f.string_value(), Some("pagerank"));
        assert_none!(f.numeric_value());
        assert_none!(f.stored_value());
        assert_none!(f.point_bytes());
    }

    #[test]
    fn test_feature_field_encoding_known_values() {
        // 0.5f32 bits = 0x3F000000, >> 15 = 0x7E00 = 32256
        let f = feature_field("f", "x", 0.5);
        assert_matches!(f.value(), FieldValue::Feature { freq, .. } if *freq == 32256);

        // 10.0f32 bits = 0x41200000, >> 15 = 0x8240 = 33344
        let f = feature_field("f", "x", 10.0);
        assert_matches!(f.value(), FieldValue::Feature { freq, .. } if *freq == 33344);
    }

    #[test]
    #[should_panic(expected = "finite and positive")]
    fn test_feature_field_zero_value() {
        feature_field("f", "x", 0.0);
    }

    #[test]
    #[should_panic(expected = "finite and positive")]
    fn test_feature_field_negative_value() {
        feature_field("f", "x", -1.0);
    }

    #[test]
    #[should_panic(expected = "finite and positive")]
    fn test_feature_field_nan_value() {
        feature_field("f", "x", f32::NAN);
    }

    #[test]
    fn test_feature_field_debug() {
        let f = feature_field("features", "pagerank", 1.0);
        let debug_str = format!("{:?}", f.value());
        assert_contains!(debug_str, "Feature");
        assert_contains!(debug_str, "pagerank");
    }
}
