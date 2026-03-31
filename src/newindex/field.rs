// SPDX-License-Identifier: Apache-2.0

//! Field types and builders for the newindex pipeline.
//!
//! Fields are the unit of data added to a document. Each field has a name and
//! a [`FieldType`] that describes what axes of the index it participates in:
//! storage, inverted index (postings), doc values, and points.
//!
//! Fields are constructed through builder functions that enforce valid
//! combinations at compile time. The first call determines the field type and
//! constrains what follows.
//!
//! # Examples
//!
//! ```ignore
//! // Tokenized text field, stored
//! let f = text("body").stored().value("hello world");
//!
//! // Tokenized text field, reader-backed, not stored
//! let f = text("body").reader(file);
//!
//! // Tokenized text field, not stored
//! let f = text("body").value("hello world");
//! ```

use std::fmt;
use std::io::Read;

use crate::document::{DocValuesType, IndexOptions};

/// Value stored in the stored fields segment for later retrieval.
///
/// Each variant carries a single typed value. The type is fixed at field
/// creation time and determines how the value is serialized.
// LOCKED
#[derive(Clone, PartialEq)]
pub enum StoredValue {
    /// UTF-8 string value.
    String(String),
    /// Raw byte array.
    Bytes(Vec<u8>),
    /// 32-bit signed integer.
    Int(i32),
    /// 64-bit signed integer.
    Long(i64),
    /// 32-bit floating point.
    Float(f32),
    /// 64-bit floating point.
    Double(f64),
}

impl fmt::Debug for StoredValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoredValue::String(s) => f.debug_tuple("String").field(s).finish(),
            StoredValue::Bytes(b) => f.debug_tuple("Bytes.len()").field(&b.len()).finish(),
            StoredValue::Int(v) => f.debug_tuple("Int").field(v).finish(),
            StoredValue::Long(v) => f.debug_tuple("Long").field(v).finish(),
            StoredValue::Float(v) => f.debug_tuple("Float").field(v).finish(),
            StoredValue::Double(v) => f.debug_tuple("Double").field(v).finish(),
        }
    }
}

/// How a field enters the inverted index (postings).
///
/// Determines the [`IndexOptions`] level (docs, freqs, positions) and whether
/// the field is run through an analyzer.
// LOCKED
pub enum InvertableValue {
    /// Streaming reader run through an analyzer. Produces a token stream
    /// with docs, freqs, and positions. Has norms. Cannot be stored.
    Tokenized(Box<dyn Read + Send>),
    /// String run through an analyzer. Produces a token stream with docs,
    /// freqs, and positions. Has norms. Can be stored (the string is
    /// accessible before tokenization consumes it).
    TokenizedString(String),
    /// Single exact-match term indexed at the DOCS level only. Omits norms.
    ExactMatch(String),
    /// Term with a feature value encoded as term frequency. Indexed at
    /// DOCS_AND_FREQS level. Omits norms. Used by FeatureField.
    Feature(String, f32),
}

impl fmt::Debug for InvertableValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvertableValue::Tokenized(_) => f.debug_tuple("Tokenized").field(&"...").finish(),
            InvertableValue::TokenizedString(s) => {
                f.debug_tuple("TokenizedString").field(s).finish()
            }
            InvertableValue::ExactMatch(s) => f.debug_tuple("ExactMatch").field(s).finish(),
            InvertableValue::Feature(name, val) => {
                f.debug_tuple("Feature").field(name).field(val).finish()
            }
        }
    }
}

/// Columnar per-document value for sorting, faceting, and aggregation.
///
/// Doc values are written to separate files (.dvd, .dvm) and accessed
// LOCKED
/// during queries without touching the inverted index.
#[derive(Clone, PartialEq)]
pub enum DocValue {
    /// Single numeric value per document.
    Numeric(i64),
    /// Single binary value per document.
    Binary(Vec<u8>),
    /// Single sorted byte sequence per document.
    Sorted(Vec<u8>),
    /// Multiple sorted byte sequences per document.
    SortedSet(Vec<Vec<u8>>),
    /// Multiple sorted numeric values per document.
    SortedNumeric(Vec<i64>),
}

impl fmt::Debug for DocValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocValue::Numeric(v) => f.debug_tuple("Numeric").field(v).finish(),
            DocValue::Binary(b) => f.debug_tuple("Binary.len()").field(&b.len()).finish(),
            DocValue::Sorted(b) => f.debug_tuple("Sorted.len()").field(&b.len()).finish(),
            DocValue::SortedSet(v) => f.debug_tuple("SortedSet.len()").field(&v.len()).finish(),
            DocValue::SortedNumeric(v) => f
                .debug_tuple("SortedNumeric.len()")
                .field(&v.len())
                .finish(),
        }
    }
}

/// Dimensional point data for range queries.
///
// LOCKED
/// Points are indexed in a BKD tree structure for efficient multi-dimensional
/// range queries.
#[derive(Clone, PartialEq)]
pub enum PointsValue {
    /// Single point: 1 dimension, N bytes per dimension.
    Single {
        /// Number of bytes per dimension.
        bytes_per_dim: usize,
        /// Encoded point data.
        encoded: Vec<u8>,
    },
    /// Range: min/max pairs. N dimensions, M bytes per value.
    Range {
        /// Number of dimensions.
        dims: usize,
        /// Number of bytes per dimension value.
        bytes_per_dim: usize,
        /// Encoded min/max data laid out as [min1..minN, max1..maxN].
        encoded: Vec<u8>,
    },
}

impl fmt::Debug for PointsValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PointsValue::Single {
                bytes_per_dim,
                encoded,
            } => f
                .debug_struct("Single")
                .field("bytes_per_dim", bytes_per_dim)
                .field("encoded_len", &encoded.len())
                .finish(),
            PointsValue::Range {
                dims,
                bytes_per_dim,
                encoded,
            } => f
                .debug_struct("Range")
                .field("dims", dims)
                .field("bytes_per_dim", bytes_per_dim)
                .field("encoded_len", &encoded.len())
                .finish(),
        }
    }
}

/// Describes what axes of the index a field participates in.
///
/// Each axis is optional and independent. A field can participate in any valid
/// combination of storage, inverted index, doc values, and points. The axes
/// are assembled by builder functions that enforce valid combinations.
///
/// `FieldType` fields are private — instances can only be created through
/// the builder DSL ([`text`], etc.).
// LOCKED
pub struct FieldType {
    stored: Option<StoredValue>,
    invertable: Option<InvertableValue>,
    doc: Option<DocValue>,
    points: Option<PointsValue>,
}

impl FieldType {
    /// Returns the stored value, if this field is stored.
    pub fn stored(&self) -> Option<&StoredValue> {
        self.stored.as_ref()
    }

    /// Returns the invertable value, if this field participates in the
    /// inverted index.
    pub fn invertable(&self) -> Option<&InvertableValue> {
        self.invertable.as_ref()
    }

    /// Takes ownership of the invertable value, leaving `None` in its place.
    ///
    /// Used by the postings consumer to consume reader-backed values during
    /// tokenization.
    pub fn take_invertable(&mut self) -> Option<InvertableValue> {
        self.invertable.take()
    }

    /// Returns the doc value, if this field has doc values.
    pub fn doc_value(&self) -> Option<&DocValue> {
        self.doc.as_ref()
    }

    /// Returns the points value, if this field has point data.
    pub fn points(&self) -> Option<&PointsValue> {
        self.points.as_ref()
    }

    /// Returns the index options for this field, derived from the invertable
    /// value.
    pub fn index_options(&self) -> IndexOptions {
        match &self.invertable {
            None => IndexOptions::None,
            Some(InvertableValue::Tokenized(_) | InvertableValue::TokenizedString(_)) => {
                IndexOptions::DocsAndFreqsAndPositions
            }
            Some(InvertableValue::ExactMatch(_)) => IndexOptions::Docs,
            Some(InvertableValue::Feature(_, _)) => IndexOptions::DocsAndFreqs,
        }
    }

    /// Whether this field is tokenized (run through an analyzer).
    pub fn is_tokenized(&self) -> bool {
        matches!(
            &self.invertable,
            Some(InvertableValue::Tokenized(_) | InvertableValue::TokenizedString(_))
        )
    }

    /// Whether this field computes and stores norms. Only tokenized fields
    /// have norms.
    pub fn has_norms(&self) -> bool {
        self.is_tokenized()
    }

    /// Returns the doc values type for this field, derived from the doc value.
    pub fn doc_values_type(&self) -> DocValuesType {
        match &self.doc {
            None => DocValuesType::None,
            Some(DocValue::Numeric(_)) => DocValuesType::Numeric,
            Some(DocValue::Binary(_)) => DocValuesType::Binary,
            Some(DocValue::Sorted(_)) => DocValuesType::Sorted,
            Some(DocValue::SortedSet(_)) => DocValuesType::SortedSet,
            Some(DocValue::SortedNumeric(_)) => DocValuesType::SortedNumeric,
        }
    }
}

impl fmt::Debug for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FieldType")
            .field("stored", &self.stored)
            .field("invertable", &self.invertable)
            .field("doc", &self.doc)
            .field("points", &self.points)
            .finish()
    }
}

/// An immutable field within a document.
///
/// Created via builder functions ([`text`], etc.). Holds a name and a
/// [`FieldType`] that describes what axes of the index the field participates
/// in.
// LOCKED
#[derive(Debug)]
pub struct Field {
    name: String,
    field_type: FieldType,
}

impl Field {
    /// Returns the field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the field type.
    pub fn field_type(&self) -> &FieldType {
        &self.field_type
    }

    /// Returns a mutable reference to the field type.
    ///
    /// Used by consumers that need to take ownership of values (e.g.,
    /// [`FieldType::take_invertable`] during tokenization).
    pub fn field_type_mut(&mut self) -> &mut FieldType {
        &mut self.field_type
    }
}

// ---------------------------------------------------------------------------
// Builder DSL
// ---------------------------------------------------------------------------

/// Creates a [`TextFieldBuilder`] for a tokenized text field.
///
/// Text fields are run through an analyzer to produce a token stream with
/// docs, freqs, and positions. They support optional storage.
///
/// # Examples
///
/// ```ignore
/// let f = text("body").value("hello world");          // not stored
/// let f = text("body").stored().value("hello world");  // stored
/// let f = text("body").reader(file);                   // reader-backed
/// ```
pub fn text(name: &str) -> TextFieldBuilder {
    TextFieldBuilder {
        name: name.to_string(),
    }
}

/// Builder for a tokenized text field.
///
/// Supports three terminal methods:
/// - [`.value()`](TextFieldBuilder::value) — string value, not stored
/// - [`.reader()`](TextFieldBuilder::reader) — reader-backed, not stored
/// - [`.stored()`](TextFieldBuilder::stored) — returns a
///   [`StoredTextFieldBuilder`] that only accepts string values
pub struct TextFieldBuilder {
    name: String,
}

impl TextFieldBuilder {
    /// Marks this text field as stored.
    ///
    /// Returns a [`StoredTextFieldBuilder`] that accepts only string values
    /// (readers cannot be stored).
    pub fn stored(self) -> StoredTextFieldBuilder {
        StoredTextFieldBuilder { name: self.name }
    }

    /// Sets the string value and builds the field. Not stored.
    pub fn value(self, text: impl Into<String>) -> Field {
        let s = text.into();
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: Some(InvertableValue::TokenizedString(s)),
                doc: None,
                points: None,
            },
        }
    }

    /// Sets a streaming reader value and builds the field. Not stored.
    ///
    /// The reader is consumed during indexing. Tokenization proceeds in
    /// chunks without buffering the entire content in memory.
    pub fn reader(self, reader: impl Read + Send + 'static) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: Some(InvertableValue::Tokenized(Box::new(reader))),
                doc: None,
                points: None,
            },
        }
    }
}

/// Builder for a stored tokenized text field.
///
/// Created by [`TextFieldBuilder::stored()`]. Only accepts string values
/// because reader-backed fields cannot be stored.
pub struct StoredTextFieldBuilder {
    name: String,
}

impl StoredTextFieldBuilder {
    /// Sets the string value and builds the stored text field.
    ///
    /// The string is both stored for retrieval and tokenized for searching.
    pub fn value(self, text: impl Into<String>) -> Field {
        let s = text.into();
        Field {
            name: self.name,
            field_type: FieldType {
                stored: Some(StoredValue::String(s.clone())),
                invertable: Some(InvertableValue::TokenizedString(s)),
                doc: None,
                points: None,
            },
        }
    }
}

/// Creates a [`KeywordFieldBuilder`] for an exact-match indexed field with
/// SortedSet doc values.
///
/// Keyword fields are indexed as a single term (DOCS only), have SortedSet doc
/// values for sorting/faceting, and support optional storage. Norms are omitted.
///
/// # Examples
///
/// ```ignore
/// let f = keyword("tag").value("rust");
/// let f = keyword("tag").stored().value("rust");
/// ```
pub fn keyword(name: &str) -> KeywordFieldBuilder {
    KeywordFieldBuilder {
        name: name.to_string(),
        stored: false,
    }
}

/// Builder for a keyword field (exact-match indexed + SortedSet DV).
pub struct KeywordFieldBuilder {
    name: String,
    stored: bool,
}

impl KeywordFieldBuilder {
    /// Marks this keyword field as stored.
    pub fn stored(mut self) -> Self {
        self.stored = true;
        self
    }

    /// Sets the value and builds the field.
    pub fn value(self, text: impl Into<String>) -> Field {
        let s = text.into();
        Field {
            name: self.name,
            field_type: FieldType {
                stored: if self.stored {
                    Some(StoredValue::String(s.clone()))
                } else {
                    None
                },
                invertable: Some(InvertableValue::ExactMatch(s.clone())),
                doc: Some(DocValue::SortedSet(vec![s.into_bytes()])),
                points: None,
            },
        }
    }
}

/// Creates a builder for a [`FeatureField`](InvertableValue::Feature).
///
/// Feature fields encode a float value as term frequency for static ranking
/// signals. Never stored.
///
/// # Examples
///
/// ```ignore
/// let f = feature("pagerank").value("score", 0.95);
/// ```
pub fn feature(name: &str) -> FeatureFieldBuilder {
    FeatureFieldBuilder {
        name: name.to_string(),
    }
}

/// Builder for a feature field (freq-encoded float value).
pub struct FeatureFieldBuilder {
    name: String,
}

impl FeatureFieldBuilder {
    /// Sets the feature name and value, and builds the field.
    ///
    /// Panics if `feature_value` is not positive and finite.
    pub fn value(self, feature_name: impl Into<String>, feature_value: f32) -> Field {
        assert!(
            feature_value.is_finite() && feature_value > 0.0,
            "feature value must be positive and finite"
        );
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: Some(InvertableValue::Feature(feature_name.into(), feature_value)),
                doc: None,
                points: None,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Numeric field builders (points + SortedNumeric DV)
// ---------------------------------------------------------------------------

/// Creates a builder for an IntField (points + SortedNumeric DV, optional storage).
///
/// # Examples
///
/// ```ignore
/// let f = int_field("count").value(42);
/// let f = int_field("count").stored().value(42);
/// ```
pub fn int_field(name: &str) -> IntFieldBuilder {
    IntFieldBuilder {
        name: name.to_string(),
        stored: false,
    }
}

/// Builder for an int field (1D point + SortedNumeric DV).
pub struct IntFieldBuilder {
    name: String,
    stored: bool,
}

impl IntFieldBuilder {
    /// Marks this field as stored.
    pub fn stored(mut self) -> Self {
        self.stored = true;
        self
    }

    /// Sets the value and builds the field.
    pub fn value(self, v: i32) -> Field {
        use crate::encoding::sortable_bytes;
        Field {
            name: self.name,
            field_type: FieldType {
                stored: if self.stored {
                    Some(StoredValue::Int(v))
                } else {
                    None
                },
                invertable: None,
                doc: Some(DocValue::SortedNumeric(vec![v as i64])),
                points: Some(PointsValue::Single {
                    bytes_per_dim: 4,
                    encoded: sortable_bytes::from_int(v).to_vec(),
                }),
            },
        }
    }
}

/// Creates a builder for a LongField (points + SortedNumeric DV, optional storage).
///
/// # Examples
///
/// ```ignore
/// let f = long_field("timestamp").value(1234567890);
/// let f = long_field("timestamp").stored().value(1234567890);
/// ```
pub fn long_field(name: &str) -> LongFieldBuilder {
    LongFieldBuilder {
        name: name.to_string(),
        stored: false,
    }
}

/// Builder for a long field (1D point + SortedNumeric DV).
pub struct LongFieldBuilder {
    name: String,
    stored: bool,
}

impl LongFieldBuilder {
    /// Marks this field as stored.
    pub fn stored(mut self) -> Self {
        self.stored = true;
        self
    }

    /// Sets the value and builds the field.
    pub fn value(self, v: i64) -> Field {
        use crate::encoding::sortable_bytes;
        Field {
            name: self.name,
            field_type: FieldType {
                stored: if self.stored {
                    Some(StoredValue::Long(v))
                } else {
                    None
                },
                invertable: None,
                doc: Some(DocValue::SortedNumeric(vec![v])),
                points: Some(PointsValue::Single {
                    bytes_per_dim: 8,
                    encoded: sortable_bytes::from_long(v).to_vec(),
                }),
            },
        }
    }
}

/// Creates a builder for a FloatField (points + SortedNumeric DV, optional storage).
///
/// # Examples
///
/// ```ignore
/// let f = float_field("score").value(0.95);
/// let f = float_field("score").stored().value(0.95);
/// ```
pub fn float_field(name: &str) -> FloatFieldBuilder {
    FloatFieldBuilder {
        name: name.to_string(),
        stored: false,
    }
}

/// Builder for a float field (1D point + SortedNumeric DV).
pub struct FloatFieldBuilder {
    name: String,
    stored: bool,
}

impl FloatFieldBuilder {
    /// Marks this field as stored.
    pub fn stored(mut self) -> Self {
        self.stored = true;
        self
    }

    /// Sets the value and builds the field.
    pub fn value(self, v: f32) -> Field {
        use crate::encoding::sortable_bytes;
        Field {
            name: self.name,
            field_type: FieldType {
                stored: if self.stored {
                    Some(StoredValue::Float(v))
                } else {
                    None
                },
                invertable: None,
                doc: Some(DocValue::SortedNumeric(vec![
                    sortable_bytes::float_to_int(v) as i64,
                ])),
                points: Some(PointsValue::Single {
                    bytes_per_dim: 4,
                    encoded: sortable_bytes::from_float(v).to_vec(),
                }),
            },
        }
    }
}

/// Creates a builder for a DoubleField (points + SortedNumeric DV, optional storage).
///
/// # Examples
///
/// ```ignore
/// let f = double_field("weight").value(9.81);
/// let f = double_field("weight").stored().value(9.81);
/// ```
pub fn double_field(name: &str) -> DoubleFieldBuilder {
    DoubleFieldBuilder {
        name: name.to_string(),
        stored: false,
    }
}

/// Builder for a double field (1D point + SortedNumeric DV).
pub struct DoubleFieldBuilder {
    name: String,
    stored: bool,
}

impl DoubleFieldBuilder {
    /// Marks this field as stored.
    pub fn stored(mut self) -> Self {
        self.stored = true;
        self
    }

    /// Sets the value and builds the field.
    pub fn value(self, v: f64) -> Field {
        use crate::encoding::sortable_bytes;
        Field {
            name: self.name,
            field_type: FieldType {
                stored: if self.stored {
                    Some(StoredValue::Double(v))
                } else {
                    None
                },
                invertable: None,
                doc: Some(DocValue::SortedNumeric(vec![
                    sortable_bytes::double_to_long(v),
                ])),
                points: Some(PointsValue::Single {
                    bytes_per_dim: 8,
                    encoded: sortable_bytes::from_double(v).to_vec(),
                }),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Spatial and range field builders (points only)
// ---------------------------------------------------------------------------

/// Creates a builder for a LatLonPoint (2D point, not stored).
///
/// # Examples
///
/// ```ignore
/// let f = lat_lon("location").value(40.7128, -74.0060);
/// ```
pub fn lat_lon(name: &str) -> LatLonBuilder {
    LatLonBuilder {
        name: name.to_string(),
    }
}

/// Builder for a lat/lon point field.
pub struct LatLonBuilder {
    name: String,
}

impl LatLonBuilder {
    /// Sets the latitude and longitude, and builds the field.
    pub fn value(self, lat: f64, lon: f64) -> Field {
        use crate::encoding::{geo, sortable_bytes};
        let encoded_lat = geo::encode_latitude(lat);
        let encoded_lon = geo::encode_longitude(lon);
        let mut encoded = Vec::with_capacity(8);
        encoded.extend_from_slice(&sortable_bytes::from_int(encoded_lat));
        encoded.extend_from_slice(&sortable_bytes::from_int(encoded_lon));
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: None,
                doc: None,
                points: Some(PointsValue::Single {
                    bytes_per_dim: 4,
                    encoded,
                }),
            },
        }
    }
}

/// Creates a builder for an IntRange field (multi-dim points, not stored).
///
/// # Examples
///
/// ```ignore
/// let f = int_range("age_range").value(&[18], &[65]);
/// ```
pub fn int_range(name: &str) -> IntRangeBuilder {
    IntRangeBuilder {
        name: name.to_string(),
    }
}

/// Builder for an int range field.
pub struct IntRangeBuilder {
    name: String,
}

impl IntRangeBuilder {
    /// Sets the min/max values per dimension and builds the field.
    ///
    /// Panics if `mins` and `maxs` have different lengths or are empty.
    pub fn value(self, mins: &[i32], maxs: &[i32]) -> Field {
        assert_eq!(
            mins.len(),
            maxs.len(),
            "mins and maxs must have same length"
        );
        assert!(!mins.is_empty(), "must have at least one dimension");
        use crate::encoding::range;
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: None,
                doc: None,
                points: Some(PointsValue::Range {
                    dims: mins.len(),
                    bytes_per_dim: 4,
                    encoded: range::encode_int(mins, maxs),
                }),
            },
        }
    }
}

/// Creates a builder for a LongRange field (multi-dim points, not stored).
///
/// # Examples
///
/// ```ignore
/// let f = long_range("time_range").value(&[start], &[end]);
/// ```
pub fn long_range(name: &str) -> LongRangeBuilder {
    LongRangeBuilder {
        name: name.to_string(),
    }
}

/// Builder for a long range field.
pub struct LongRangeBuilder {
    name: String,
}

impl LongRangeBuilder {
    /// Sets the min/max values per dimension and builds the field.
    ///
    /// Panics if `mins` and `maxs` have different lengths or are empty.
    pub fn value(self, mins: &[i64], maxs: &[i64]) -> Field {
        assert_eq!(
            mins.len(),
            maxs.len(),
            "mins and maxs must have same length"
        );
        assert!(!mins.is_empty(), "must have at least one dimension");
        use crate::encoding::range;
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: None,
                doc: None,
                points: Some(PointsValue::Range {
                    dims: mins.len(),
                    bytes_per_dim: 8,
                    encoded: range::encode_long(mins, maxs),
                }),
            },
        }
    }
}

/// Creates a builder for a FloatRange field (multi-dim points, not stored).
///
/// # Examples
///
/// ```ignore
/// let f = float_range("temp_range").value(&[0.0], &[100.0]);
/// ```
pub fn float_range(name: &str) -> FloatRangeBuilder {
    FloatRangeBuilder {
        name: name.to_string(),
    }
}

/// Builder for a float range field.
pub struct FloatRangeBuilder {
    name: String,
}

impl FloatRangeBuilder {
    /// Sets the min/max values per dimension and builds the field.
    ///
    /// Panics if `mins` and `maxs` have different lengths or are empty.
    pub fn value(self, mins: &[f32], maxs: &[f32]) -> Field {
        assert_eq!(
            mins.len(),
            maxs.len(),
            "mins and maxs must have same length"
        );
        assert!(!mins.is_empty(), "must have at least one dimension");
        use crate::encoding::range;
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: None,
                doc: None,
                points: Some(PointsValue::Range {
                    dims: mins.len(),
                    bytes_per_dim: 4,
                    encoded: range::encode_float(mins, maxs),
                }),
            },
        }
    }
}

/// Creates a builder for a DoubleRange field (multi-dim points, not stored).
///
/// # Examples
///
/// ```ignore
/// let f = double_range("coord_range").value(&[0.0], &[1.0]);
/// ```
pub fn double_range(name: &str) -> DoubleRangeBuilder {
    DoubleRangeBuilder {
        name: name.to_string(),
    }
}

/// Builder for a double range field.
pub struct DoubleRangeBuilder {
    name: String,
}

impl DoubleRangeBuilder {
    /// Sets the min/max values per dimension and builds the field.
    ///
    /// Panics if `mins` and `maxs` have different lengths or are empty.
    pub fn value(self, mins: &[f64], maxs: &[f64]) -> Field {
        assert_eq!(
            mins.len(),
            maxs.len(),
            "mins and maxs must have same length"
        );
        assert!(!mins.is_empty(), "must have at least one dimension");
        use crate::encoding::range;
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: None,
                doc: None,
                points: Some(PointsValue::Range {
                    dims: mins.len(),
                    bytes_per_dim: 8,
                    encoded: range::encode_double(mins, maxs),
                }),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Doc-values-only field builders
// ---------------------------------------------------------------------------

/// Creates a builder for a numeric doc values field (doc values only).
///
/// # Examples
///
/// ```ignore
/// let f = numeric_dv("count").value(42);
/// ```
pub fn numeric_dv(name: &str) -> NumericDvBuilder {
    NumericDvBuilder {
        name: name.to_string(),
    }
}

/// Builder for a numeric doc values field.
pub struct NumericDvBuilder {
    name: String,
}

impl NumericDvBuilder {
    /// Sets the numeric value and builds the field.
    pub fn value(self, v: i64) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: None,
                doc: Some(DocValue::Numeric(v)),
                points: None,
            },
        }
    }
}

/// Creates a builder for a binary doc values field (doc values only).
///
/// # Examples
///
/// ```ignore
/// let f = binary_dv("hash").value(vec![0xAA, 0xBB]);
/// ```
pub fn binary_dv(name: &str) -> BinaryDvBuilder {
    BinaryDvBuilder {
        name: name.to_string(),
    }
}

/// Builder for a binary doc values field.
pub struct BinaryDvBuilder {
    name: String,
}

impl BinaryDvBuilder {
    /// Sets the binary value and builds the field.
    pub fn value(self, v: Vec<u8>) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: None,
                doc: Some(DocValue::Binary(v)),
                points: None,
            },
        }
    }
}

/// Creates a builder for a sorted doc values field (doc values only).
///
/// # Examples
///
/// ```ignore
/// let f = sorted_dv("category").value(b"alpha".to_vec());
/// ```
pub fn sorted_dv(name: &str) -> SortedDvBuilder {
    SortedDvBuilder {
        name: name.to_string(),
    }
}

/// Builder for a sorted doc values field.
pub struct SortedDvBuilder {
    name: String,
}

impl SortedDvBuilder {
    /// Sets the sorted byte value and builds the field.
    pub fn value(self, v: Vec<u8>) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: None,
                doc: Some(DocValue::Sorted(v)),
                points: None,
            },
        }
    }
}

/// Creates a builder for a sorted set doc values field (doc values only).
///
/// # Examples
///
/// ```ignore
/// let f = sorted_set_dv("tags").value(vec![b"a".to_vec(), b"b".to_vec()]);
/// ```
pub fn sorted_set_dv(name: &str) -> SortedSetDvBuilder {
    SortedSetDvBuilder {
        name: name.to_string(),
    }
}

/// Builder for a sorted set doc values field.
pub struct SortedSetDvBuilder {
    name: String,
}

impl SortedSetDvBuilder {
    /// Sets the sorted set values and builds the field.
    pub fn value(self, values: Vec<Vec<u8>>) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: None,
                doc: Some(DocValue::SortedSet(values)),
                points: None,
            },
        }
    }
}

/// Creates a builder for a sorted numeric doc values field (doc values only).
///
/// # Examples
///
/// ```ignore
/// let f = sorted_numeric_dv("timestamps").value(vec![100, 200]);
/// ```
pub fn sorted_numeric_dv(name: &str) -> SortedNumericDvBuilder {
    SortedNumericDvBuilder {
        name: name.to_string(),
    }
}

/// Builder for a sorted numeric doc values field.
pub struct SortedNumericDvBuilder {
    name: String,
}

impl SortedNumericDvBuilder {
    /// Sets the sorted numeric values and builds the field.
    pub fn value(self, values: Vec<i64>) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: None,
                invertable: None,
                doc: Some(DocValue::SortedNumeric(values)),
                points: None,
            },
        }
    }
}

/// Creates a [`StoredFieldBuilder`] for a stored-only field.
///
/// Stored fields are retrievable but not searchable. The terminal method
/// determines the value type.
///
/// # Examples
///
/// ```ignore
/// let f = stored("title").string("hello");
/// let f = stored("size").long(1024);
/// let f = stored("raw").bytes(vec![0xDE, 0xAD]);
/// ```
pub fn stored(name: &str) -> StoredFieldBuilder {
    StoredFieldBuilder {
        name: name.to_string(),
    }
}

/// Builder for a stored-only field.
///
/// The terminal method determines the stored value type. No indexing,
/// doc values, or points.
pub struct StoredFieldBuilder {
    name: String,
}

impl StoredFieldBuilder {
    /// Stores a string value.
    pub fn string(self, v: impl Into<String>) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: Some(StoredValue::String(v.into())),
                invertable: None,
                doc: None,
                points: None,
            },
        }
    }

    /// Stores a raw byte array.
    pub fn bytes(self, v: Vec<u8>) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: Some(StoredValue::Bytes(v)),
                invertable: None,
                doc: None,
                points: None,
            },
        }
    }

    /// Stores a 32-bit integer.
    pub fn int(self, v: i32) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: Some(StoredValue::Int(v)),
                invertable: None,
                doc: None,
                points: None,
            },
        }
    }

    /// Stores a 64-bit integer.
    pub fn long(self, v: i64) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: Some(StoredValue::Long(v)),
                invertable: None,
                doc: None,
                points: None,
            },
        }
    }

    /// Stores a 32-bit float.
    pub fn float(self, v: f32) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: Some(StoredValue::Float(v)),
                invertable: None,
                doc: None,
                points: None,
            },
        }
    }

    /// Stores a 64-bit float.
    pub fn double(self, v: f64) -> Field {
        Field {
            name: self.name,
            field_type: FieldType {
                stored: Some(StoredValue::Double(v)),
                invertable: None,
                doc: None,
                points: None,
            },
        }
    }
}

/// Creates a [`StringFieldBuilder`] for an exact-match indexed field.
///
/// String fields are indexed as a single term at the DOCS level without
/// tokenization. Norms are omitted. Supports optional storage.
///
/// # Examples
///
/// ```ignore
/// let f = string("status").value("active");
/// let f = string("status").stored().value("active");
/// ```
pub fn string(name: &str) -> StringFieldBuilder {
    StringFieldBuilder {
        name: name.to_string(),
        stored: false,
    }
}

/// Builder for an exact-match indexed field (StringField).
///
/// Supports optional [`.stored()`](StringFieldBuilder::stored) before the
/// terminal [`.value()`](StringFieldBuilder::value) call.
pub struct StringFieldBuilder {
    name: String,
    stored: bool,
}

impl StringFieldBuilder {
    /// Marks this string field as stored.
    pub fn stored(mut self) -> Self {
        self.stored = true;
        self
    }

    /// Sets the exact-match value and builds the field.
    pub fn value(self, text: impl Into<String>) -> Field {
        let s = text.into();
        Field {
            name: self.name,
            field_type: FieldType {
                stored: if self.stored {
                    Some(StoredValue::String(s.clone()))
                } else {
                    None
                },
                invertable: Some(InvertableValue::ExactMatch(s)),
                doc: None,
                points: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use assertables::*;

    use super::*;

    // -----------------------------------------------------------------------
    // text() builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn text_field_not_stored() {
        let field = text("body").value("hello world");
        assert_eq!(field.name(), "body");
        assert_none!(field.field_type().stored());
        assert!(field.field_type().invertable().is_some());
        assert_eq!(
            field.field_type().index_options(),
            IndexOptions::DocsAndFreqsAndPositions
        );
        assert!(field.field_type().is_tokenized());
        assert!(field.field_type().has_norms());
        assert_none!(field.field_type().doc_value());
        assert_none!(field.field_type().points());
    }

    #[test]
    fn text_field_stored() {
        let field = text("body").stored().value("hello world");
        assert_eq!(field.name(), "body");
        assert_eq!(
            field.field_type().stored(),
            Some(&StoredValue::String("hello world".to_string()))
        );
        assert!(field.field_type().invertable().is_some());
        assert_eq!(
            field.field_type().index_options(),
            IndexOptions::DocsAndFreqsAndPositions
        );
        assert!(field.field_type().is_tokenized());
        assert!(field.field_type().has_norms());
    }

    #[test]
    fn text_field_reader() {
        let field = text("body").reader(Cursor::new(b"hello world".to_vec()));
        assert_eq!(field.name(), "body");
        assert_none!(field.field_type().stored());
        assert!(matches!(
            field.field_type().invertable(),
            Some(InvertableValue::Tokenized(_))
        ));
        assert_eq!(
            field.field_type().index_options(),
            IndexOptions::DocsAndFreqsAndPositions
        );
        assert!(field.field_type().is_tokenized());
    }

    #[test]
    fn text_field_take_invertable() {
        let mut field = text("body").value("hello");
        assert!(field.field_type().invertable().is_some());

        let taken = field.field_type_mut().take_invertable();
        assert!(taken.is_some());
        assert_none!(field.field_type().invertable());
    }

    #[test]
    fn text_field_reader_take_and_read() {
        let mut field = text("body").reader(Cursor::new(b"streaming content".to_vec()));
        let invertable = field.field_type_mut().take_invertable().unwrap();
        match invertable {
            InvertableValue::Tokenized(mut reader) => {
                let mut buf = String::new();
                reader.read_to_string(&mut buf).unwrap();
                assert_eq!(buf, "streaming content");
            }
            _ => panic!("expected Tokenized"),
        }
    }

    // -----------------------------------------------------------------------
    // FieldType derived property tests
    // -----------------------------------------------------------------------

    #[test]
    fn field_type_no_axes() {
        let ft = FieldType {
            stored: None,
            invertable: None,
            doc: None,
            points: None,
        };
        assert_eq!(ft.index_options(), IndexOptions::None);
        assert!(!ft.is_tokenized());
        assert!(!ft.has_norms());
        assert_eq!(ft.doc_values_type(), DocValuesType::None);
    }

    #[test]
    fn field_type_exact_match_properties() {
        let ft = FieldType {
            stored: None,
            invertable: Some(InvertableValue::ExactMatch("term".to_string())),
            doc: None,
            points: None,
        };
        assert_eq!(ft.index_options(), IndexOptions::Docs);
        assert!(!ft.is_tokenized());
        assert!(!ft.has_norms());
    }

    #[test]
    fn field_type_feature_properties() {
        let ft = FieldType {
            stored: None,
            invertable: Some(InvertableValue::Feature("score".to_string(), 0.5)),
            doc: None,
            points: None,
        };
        assert_eq!(ft.index_options(), IndexOptions::DocsAndFreqs);
        assert!(!ft.is_tokenized());
        assert!(!ft.has_norms());
    }

    #[test]
    fn field_type_doc_values_type_numeric() {
        let ft = FieldType {
            stored: None,
            invertable: None,
            doc: Some(DocValue::Numeric(42)),
            points: None,
        };
        assert_eq!(ft.doc_values_type(), DocValuesType::Numeric);
    }

    #[test]
    fn field_type_doc_values_type_binary() {
        let ft = FieldType {
            stored: None,
            invertable: None,
            doc: Some(DocValue::Binary(vec![1, 2, 3])),
            points: None,
        };
        assert_eq!(ft.doc_values_type(), DocValuesType::Binary);
    }

    #[test]
    fn field_type_doc_values_type_sorted() {
        let ft = FieldType {
            stored: None,
            invertable: None,
            doc: Some(DocValue::Sorted(b"alpha".to_vec())),
            points: None,
        };
        assert_eq!(ft.doc_values_type(), DocValuesType::Sorted);
    }

    #[test]
    fn field_type_doc_values_type_sorted_set() {
        let ft = FieldType {
            stored: None,
            invertable: None,
            doc: Some(DocValue::SortedSet(vec![b"a".to_vec(), b"b".to_vec()])),
            points: None,
        };
        assert_eq!(ft.doc_values_type(), DocValuesType::SortedSet);
    }

    #[test]
    fn field_type_doc_values_type_sorted_numeric() {
        let ft = FieldType {
            stored: None,
            invertable: None,
            doc: Some(DocValue::SortedNumeric(vec![10, 20])),
            points: None,
        };
        assert_eq!(ft.doc_values_type(), DocValuesType::SortedNumeric);
    }

    // -----------------------------------------------------------------------
    // Debug formatting tests
    // -----------------------------------------------------------------------

    #[test]
    fn field_debug_formatting() {
        let field = text("body").value("hello");
        let debug = format!("{field:?}");
        assert!(debug.contains("body"));
        assert!(debug.contains("FieldType"));
    }

    #[test]
    fn stored_value_debug() {
        let sv = StoredValue::String("hello".to_string());
        let debug = format!("{sv:?}");
        assert!(debug.contains("String"));
        assert!(debug.contains("hello"));
    }

    #[test]
    fn doc_value_debug() {
        let dv = DocValue::Numeric(42);
        let debug = format!("{dv:?}");
        assert!(debug.contains("Numeric"));
        assert!(debug.contains("42"));
    }

    #[test]
    fn points_value_debug() {
        let pv = PointsValue::Single {
            bytes_per_dim: 8,
            encoded: vec![0, 0, 0, 0, 0, 0, 0, 42],
        };
        let debug = format!("{pv:?}");
        assert!(debug.contains("Single"));
        assert!(debug.contains("bytes_per_dim"));
    }

    // -----------------------------------------------------------------------
    // stored() builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn stored_field_string() {
        let field = stored("title").string("hello");
        assert_eq!(field.name(), "title");
        assert_eq!(
            field.field_type().stored(),
            Some(&StoredValue::String("hello".to_string()))
        );
        assert_none!(field.field_type().invertable());
        assert_eq!(field.field_type().index_options(), IndexOptions::None);
        assert!(!field.field_type().is_tokenized());
        assert!(!field.field_type().has_norms());
        assert_eq!(field.field_type().doc_values_type(), DocValuesType::None);
    }

    #[test]
    fn stored_field_long() {
        let field = stored("size").long(1024);
        assert_eq!(field.field_type().stored(), Some(&StoredValue::Long(1024)));
        assert_none!(field.field_type().invertable());
    }

    #[test]
    fn stored_field_bytes() {
        let field = stored("raw").bytes(vec![0xDE, 0xAD]);
        assert_eq!(
            field.field_type().stored(),
            Some(&StoredValue::Bytes(vec![0xDE, 0xAD]))
        );
    }

    #[test]
    fn stored_field_int() {
        let field = stored("count").int(42);
        assert_eq!(field.field_type().stored(), Some(&StoredValue::Int(42)));
    }

    #[test]
    fn stored_field_float() {
        let field = stored("ratio").float(0.5);
        assert_eq!(field.field_type().stored(), Some(&StoredValue::Float(0.5)));
    }

    #[test]
    fn stored_field_double() {
        let field = stored("weight").double(1.234);
        assert_eq!(
            field.field_type().stored(),
            Some(&StoredValue::Double(1.234))
        );
    }

    // -----------------------------------------------------------------------
    // string() builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn string_field_not_stored() {
        let field = string("status").value("active");
        assert_eq!(field.name(), "status");
        assert_none!(field.field_type().stored());
        assert!(matches!(
            field.field_type().invertable(),
            Some(InvertableValue::ExactMatch(_))
        ));
        assert_eq!(field.field_type().index_options(), IndexOptions::Docs);
        assert!(!field.field_type().is_tokenized());
        assert!(!field.field_type().has_norms());
    }

    #[test]
    fn string_field_stored() {
        let field = string("status").stored().value("active");
        assert_eq!(field.name(), "status");
        assert_eq!(
            field.field_type().stored(),
            Some(&StoredValue::String("active".to_string()))
        );
        assert!(matches!(
            field.field_type().invertable(),
            Some(InvertableValue::ExactMatch(_))
        ));
        assert_eq!(field.field_type().index_options(), IndexOptions::Docs);
    }

    // -----------------------------------------------------------------------
    // keyword() builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn keyword_field_not_stored() {
        let field = keyword("tag").value("rust");
        assert_eq!(field.name(), "tag");
        assert_none!(field.field_type().stored());
        assert!(matches!(
            field.field_type().invertable(),
            Some(InvertableValue::ExactMatch(_))
        ));
        assert_eq!(field.field_type().index_options(), IndexOptions::Docs);
        assert!(!field.field_type().is_tokenized());
        assert!(!field.field_type().has_norms());
        assert_eq!(
            field.field_type().doc_values_type(),
            DocValuesType::SortedSet
        );
        assert_none!(field.field_type().points());
    }

    #[test]
    fn keyword_field_stored() {
        let field = keyword("tag").stored().value("rust");
        assert_eq!(
            field.field_type().stored(),
            Some(&StoredValue::String("rust".to_string()))
        );
        assert!(field.field_type().invertable().is_some());
        assert!(field.field_type().doc_value().is_some());
    }

    // -----------------------------------------------------------------------
    // feature() builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn feature_field() {
        let field = feature("pagerank").value("score", 0.95);
        assert_eq!(field.name(), "pagerank");
        assert_none!(field.field_type().stored());
        assert!(matches!(
            field.field_type().invertable(),
            Some(InvertableValue::Feature(_, _))
        ));
        assert_eq!(
            field.field_type().index_options(),
            IndexOptions::DocsAndFreqs
        );
        assert!(!field.field_type().is_tokenized());
        assert!(!field.field_type().has_norms());
        assert_none!(field.field_type().doc_value());
        assert_none!(field.field_type().points());
    }

    #[test]
    #[should_panic(expected = "feature value must be positive and finite")]
    fn feature_field_rejects_negative() {
        feature("f").value("s", -1.0);
    }

    #[test]
    #[should_panic(expected = "feature value must be positive and finite")]
    fn feature_field_rejects_nan() {
        feature("f").value("s", f32::NAN);
    }

    // -----------------------------------------------------------------------
    // int_field() builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn int_field_not_stored() {
        let field = int_field("count").value(42);
        assert_eq!(field.name(), "count");
        assert_none!(field.field_type().stored());
        assert_none!(field.field_type().invertable());
        assert_eq!(
            field.field_type().doc_values_type(),
            DocValuesType::SortedNumeric
        );
        assert!(field.field_type().points().is_some());
        if let Some(PointsValue::Single {
            bytes_per_dim,
            encoded,
        }) = field.field_type().points()
        {
            assert_eq!(*bytes_per_dim, 4);
            assert_eq!(encoded.len(), 4);
        } else {
            panic!("expected Single point");
        }
    }

    #[test]
    fn int_field_stored() {
        let field = int_field("count").stored().value(42);
        assert_eq!(field.field_type().stored(), Some(&StoredValue::Int(42)));
    }

    // -----------------------------------------------------------------------
    // long_field() builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn long_field_not_stored() {
        let field = long_field("timestamp").value(1234567890);
        assert_eq!(field.name(), "timestamp");
        assert_none!(field.field_type().stored());
        assert_eq!(
            field.field_type().doc_values_type(),
            DocValuesType::SortedNumeric
        );
        if let Some(PointsValue::Single {
            bytes_per_dim,
            encoded,
        }) = field.field_type().points()
        {
            assert_eq!(*bytes_per_dim, 8);
            assert_eq!(encoded.len(), 8);
        } else {
            panic!("expected Single point");
        }
    }

    #[test]
    fn long_field_stored() {
        let field = long_field("ts").stored().value(99);
        assert_eq!(field.field_type().stored(), Some(&StoredValue::Long(99)));
    }

    // -----------------------------------------------------------------------
    // float_field() builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn float_field_not_stored() {
        let field = float_field("score").value(0.5);
        assert_eq!(field.name(), "score");
        assert_none!(field.field_type().stored());
        assert_eq!(
            field.field_type().doc_values_type(),
            DocValuesType::SortedNumeric
        );
        if let Some(PointsValue::Single {
            bytes_per_dim,
            encoded,
        }) = field.field_type().points()
        {
            assert_eq!(*bytes_per_dim, 4);
            assert_eq!(encoded.len(), 4);
        } else {
            panic!("expected Single point");
        }
    }

    #[test]
    fn float_field_stored() {
        let field = float_field("s").stored().value(0.5);
        assert_eq!(field.field_type().stored(), Some(&StoredValue::Float(0.5)));
    }

    // -----------------------------------------------------------------------
    // double_field() builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn double_field_not_stored() {
        let field = double_field("weight").value(9.81);
        assert_eq!(field.name(), "weight");
        assert_none!(field.field_type().stored());
        assert_eq!(
            field.field_type().doc_values_type(),
            DocValuesType::SortedNumeric
        );
        if let Some(PointsValue::Single {
            bytes_per_dim,
            encoded,
        }) = field.field_type().points()
        {
            assert_eq!(*bytes_per_dim, 8);
            assert_eq!(encoded.len(), 8);
        } else {
            panic!("expected Single point");
        }
    }

    #[test]
    fn double_field_stored() {
        let field = double_field("w").stored().value(9.81);
        assert_eq!(
            field.field_type().stored(),
            Some(&StoredValue::Double(9.81))
        );
    }

    // -----------------------------------------------------------------------
    // lat_lon() builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn lat_lon_field() {
        let field = lat_lon("location").value(40.7128, -74.0060);
        assert_eq!(field.name(), "location");
        assert_none!(field.field_type().stored());
        assert_none!(field.field_type().invertable());
        assert_none!(field.field_type().doc_value());
        if let Some(PointsValue::Single {
            bytes_per_dim,
            encoded,
        }) = field.field_type().points()
        {
            assert_eq!(*bytes_per_dim, 4);
            assert_eq!(encoded.len(), 8); // 2 dimensions x 4 bytes
        } else {
            panic!("expected Single point");
        }
    }

    // -----------------------------------------------------------------------
    // Range field builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn int_range_field() {
        let field = int_range("age_range").value(&[18], &[65]);
        assert_eq!(field.name(), "age_range");
        assert_none!(field.field_type().stored());
        assert_none!(field.field_type().invertable());
        assert_none!(field.field_type().doc_value());
        if let Some(PointsValue::Range {
            dims,
            bytes_per_dim,
            encoded,
        }) = field.field_type().points()
        {
            assert_eq!(*dims, 1);
            assert_eq!(*bytes_per_dim, 4);
            assert_eq!(encoded.len(), 8); // 2 values x 4 bytes
        } else {
            panic!("expected Range point");
        }
    }

    #[test]
    fn int_range_multi_dim() {
        let field = int_range("r").value(&[1, 2], &[10, 20]);
        if let Some(PointsValue::Range {
            dims,
            bytes_per_dim,
            encoded,
        }) = field.field_type().points()
        {
            assert_eq!(*dims, 2);
            assert_eq!(*bytes_per_dim, 4);
            assert_eq!(encoded.len(), 16); // 4 values x 4 bytes
        } else {
            panic!("expected Range point");
        }
    }

    #[test]
    #[should_panic(expected = "mins and maxs must have same length")]
    fn int_range_mismatched_dims() {
        int_range("r").value(&[1, 2], &[10]);
    }

    #[test]
    fn long_range_field() {
        let field = long_range("time_range").value(&[100], &[200]);
        if let Some(PointsValue::Range {
            dims,
            bytes_per_dim,
            ..
        }) = field.field_type().points()
        {
            assert_eq!(*dims, 1);
            assert_eq!(*bytes_per_dim, 8);
        } else {
            panic!("expected Range point");
        }
    }

    #[test]
    fn float_range_field() {
        let field = float_range("temp_range").value(&[0.0], &[100.0]);
        if let Some(PointsValue::Range {
            dims,
            bytes_per_dim,
            ..
        }) = field.field_type().points()
        {
            assert_eq!(*dims, 1);
            assert_eq!(*bytes_per_dim, 4);
        } else {
            panic!("expected Range point");
        }
    }

    #[test]
    fn double_range_field() {
        let field = double_range("coord_range").value(&[0.0], &[1.0]);
        if let Some(PointsValue::Range {
            dims,
            bytes_per_dim,
            ..
        }) = field.field_type().points()
        {
            assert_eq!(*dims, 1);
            assert_eq!(*bytes_per_dim, 8);
        } else {
            panic!("expected Range point");
        }
    }

    // -----------------------------------------------------------------------
    // DV-only builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn numeric_dv_field() {
        let field = numeric_dv("count").value(42);
        assert_eq!(field.name(), "count");
        assert_none!(field.field_type().stored());
        assert_none!(field.field_type().invertable());
        assert_none!(field.field_type().points());
        assert_eq!(field.field_type().doc_values_type(), DocValuesType::Numeric);
        assert_eq!(field.field_type().doc_value(), Some(&DocValue::Numeric(42)));
    }

    #[test]
    fn binary_dv_field() {
        let field = binary_dv("hash").value(vec![0xAA, 0xBB]);
        assert_eq!(field.name(), "hash");
        assert_eq!(field.field_type().doc_values_type(), DocValuesType::Binary);
        assert_none!(field.field_type().stored());
        assert_none!(field.field_type().invertable());
    }

    #[test]
    fn sorted_dv_field() {
        let field = sorted_dv("category").value(b"alpha".to_vec());
        assert_eq!(field.name(), "category");
        assert_eq!(field.field_type().doc_values_type(), DocValuesType::Sorted);
    }

    #[test]
    fn sorted_set_dv_field() {
        let field = sorted_set_dv("tags").value(vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(field.name(), "tags");
        assert_eq!(
            field.field_type().doc_values_type(),
            DocValuesType::SortedSet
        );
    }

    #[test]
    fn sorted_numeric_dv_field() {
        let field = sorted_numeric_dv("timestamps").value(vec![100, 200]);
        assert_eq!(field.name(), "timestamps");
        assert_eq!(
            field.field_type().doc_values_type(),
            DocValuesType::SortedNumeric
        );
    }
}
