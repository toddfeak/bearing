// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::io::{self, Read};

/// The kind of field, combining type and value in a single enum.
///
/// Each variant carries exactly the data it needs. Invalid combinations
/// (e.g., a stored reader) are unrepresentable.
// LOCKED
pub enum FieldKind {
    /// Stored only, not indexed.
    Stored(String),
    /// Tokenized and indexed via streaming reader, not stored.
    Tokenized(Box<dyn Read + Send>),
    /// Tokenized, indexed, and stored.
    StoredTokenized(String),
}

/// Not indexed.
const INDEX_OPTIONS_NONE: u8 = 0;
/// Indexed with docs, frequencies, and positions.
const INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS: u8 = 3;

impl FieldKind {
    /// Index options for `.fnm`.
    pub fn index_options(&self) -> u8 {
        match self {
            FieldKind::Stored(_) => INDEX_OPTIONS_NONE,
            FieldKind::Tokenized(_) | FieldKind::StoredTokenized(_) => {
                INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS
            }
        }
    }

    /// Whether this field computes and stores norms.
    pub fn has_norms(&self) -> bool {
        match self {
            FieldKind::Stored(_) => false,
            FieldKind::Tokenized(_) | FieldKind::StoredTokenized(_) => true,
        }
    }

    /// Consumes the value and returns a boxed reader for tokenization.
    ///
    /// - `Stored` and `StoredTokenized` string values are converted to an in-memory `Cursor`.
    /// - `Tokenized` reader values are returned directly.
    pub fn into_reader(self) -> Box<dyn Read + Send> {
        match self {
            FieldKind::Stored(s) | FieldKind::StoredTokenized(s) => {
                Box::new(io::Cursor::new(s.into_bytes()))
            }
            FieldKind::Tokenized(r) => r,
        }
    }
}

impl fmt::Debug for FieldKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldKind::Stored(s) => f.debug_tuple("Stored").field(s).finish(),
            FieldKind::Tokenized(_) => f.debug_tuple("Tokenized").field(&"...").finish(),
            FieldKind::StoredTokenized(s) => f.debug_tuple("StoredTokenized").field(s).finish(),
        }
    }
}

/// An immutable field within a document.
///
/// Created via convenience functions [`stored_field`], [`tokenized_field`],
/// or [`stored_tokenized_field`].
// LOCKED
#[derive(Debug)]
pub struct Field {
    name: String,
    kind: FieldKind,
}

impl Field {
    /// Returns the field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the field kind.
    pub fn kind(&self) -> &FieldKind {
        &self.kind
    }

    /// Returns a mutable reference to the field kind.
    ///
    /// Used to consume reader-backed values via [`std::mem::replace`]
    /// during tokenization.
    pub fn kind_mut(&mut self) -> &mut FieldKind {
        &mut self.kind
    }
}

/// Creates a stored-only field. Not indexed, not tokenized.
pub fn stored_field(name: &str, value: impl Into<String>) -> Field {
    Field {
        name: name.to_string(),
        kind: FieldKind::Stored(value.into()),
    }
}

/// Creates a tokenized field backed by a streaming reader.
///
/// The reader is consumed during indexing, tokenizing in chunks without
/// buffering the entire content in memory. Not stored.
pub fn tokenized_field(name: &str, reader: impl Read + Send + 'static) -> Field {
    Field {
        name: name.to_string(),
        kind: FieldKind::Tokenized(Box::new(reader)),
    }
}

/// Creates a tokenized field that is also stored.
///
/// The string is available for stored field retrieval. During tokenization,
/// it is consumed and wrapped in a reader internally.
pub fn stored_tokenized_field(name: &str, value: impl Into<String>) -> Field {
    Field {
        name: name.to_string(),
        kind: FieldKind::StoredTokenized(value.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;
    use io::Cursor;

    #[test]
    fn stored_field_kind() {
        let field = stored_field("title", "hello");
        assert_eq!(field.name(), "title");
        assert!(matches!(field.kind(), FieldKind::Stored(_)));
        assert_eq!(field.kind().index_options(), 0);
        assert!(!field.kind().has_norms());
    }

    #[test]
    fn tokenized_field_kind() {
        let field = tokenized_field("contents", Cursor::new(b"hello world".to_vec()));
        assert_eq!(field.name(), "contents");
        assert!(matches!(field.kind(), FieldKind::Tokenized(_)));
        assert_eq!(field.kind().index_options(), 3);
        assert!(field.kind().has_norms());
    }

    #[test]
    fn stored_tokenized_field_kind() {
        let field = stored_tokenized_field("body", "hello world");
        assert_eq!(field.name(), "body");
        assert!(matches!(field.kind(), FieldKind::StoredTokenized(_)));
        assert_eq!(field.kind().index_options(), 3);
        assert!(field.kind().has_norms());
    }

    #[test]
    fn into_reader_from_stored_tokenized() {
        let kind = FieldKind::StoredTokenized("streaming content".to_string());
        let mut reader = kind.into_reader();
        let mut buf = String::new();
        reader.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "streaming content");
    }

    #[test]
    fn into_reader_from_tokenized() {
        let kind = FieldKind::Tokenized(Box::new(Cursor::new(b"from reader".to_vec())));
        let mut reader = kind.into_reader();
        let mut buf = String::new();
        reader.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "from reader");
    }

    #[test]
    fn reader_value_is_consumable() {
        let mut field = tokenized_field("body", Cursor::new(b"streaming content".to_vec()));
        assert!(matches!(field.kind(), FieldKind::Tokenized(_)));

        let kind = mem::replace(field.kind_mut(), FieldKind::Stored(String::new()));
        let mut reader = kind.into_reader();
        let mut buf = String::new();
        reader.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "streaming content");

        // After consumption, kind is the replacement
        assert!(matches!(field.kind(), FieldKind::Stored(_)));
    }

    #[test]
    fn debug_formatting() {
        let stored = FieldKind::Stored("hello".to_string());
        assert!(format!("{stored:?}").contains("Stored"));

        let tokenized = FieldKind::Tokenized(Box::new(Cursor::new(b"test".to_vec())));
        let debug = format!("{tokenized:?}");
        assert!(debug.contains("Tokenized"));
        assert!(!debug.contains("test"));

        let stored_tok = FieldKind::StoredTokenized("world".to_string());
        assert!(format!("{stored_tok:?}").contains("StoredTokenized"));
    }
}
