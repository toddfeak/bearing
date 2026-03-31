// SPDX-License-Identifier: Apache-2.0

use std::io::Read;

/// Configuration flags for how a field is indexed, stored, and searched.
///
/// Reusable across fields that share the same configuration.
// LOCKED
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldType {
    /// Whether the field's value is stored verbatim for retrieval.
    pub stored: bool,
    /// Whether the field's value is tokenized for indexing.
    pub tokenized: bool,
    /// Whether to skip norm computation for this field.
    pub omit_norms: bool,
}

impl FieldType {
    /// Stored-only field. Not indexed, not tokenized.
    pub const STORED: FieldType = FieldType {
        stored: true,
        tokenized: false,
        omit_norms: false,
    };

    /// Indexed and tokenized, with norms, and stored.
    pub const TEXT_STORED: FieldType = FieldType {
        stored: true,
        tokenized: true,
        omit_norms: false,
    };

    /// Indexed and tokenized, with norms, not stored.
    pub const TEXT: FieldType = FieldType {
        stored: false,
        tokenized: true,
        omit_norms: false,
    };
}

/// Creates a stored-only field.
pub fn stored_field(name: &str, value: impl Into<String>) -> Field {
    Field {
        name: name.to_string(),
        field_type: FieldType::STORED,
        value: FieldValue::String(value.into()),
    }
}

/// Creates a tokenized field backed by a streaming reader.
///
/// The reader is consumed during indexing, tokenizing in chunks without
/// buffering the entire content in memory. Not stored.
pub fn tokenized_field(name: &str, reader: impl Read + Send + 'static) -> Field {
    Field {
        name: name.to_string(),
        field_type: FieldType::TEXT,
        value: FieldValue::Reader(Box::new(reader)),
    }
}

/// Creates a tokenized field that is also stored.
///
/// The string is available for stored field retrieval. During tokenization,
/// it is consumed and wrapped in a reader internally.
pub fn stored_tokenized_field(name: &str, value: impl Into<String>) -> Field {
    Field {
        name: name.to_string(),
        field_type: FieldType::TEXT_STORED,
        value: FieldValue::String(value.into()),
    }
}

/// The value carried by a field.
pub enum FieldValue {
    /// A UTF-8 string value.
    String(String),
    /// A streaming text source for large text fields.
    ///
    /// Reader fields are tokenized and indexed but cannot be stored.
    /// The reader is consumed during indexing — it can only be read once.
    Reader(Box<dyn Read + Send>),
}

impl FieldValue {
    /// Consumes the value and returns a boxed reader for tokenization.
    ///
    /// - `String` values are converted to an in-memory `Cursor`.
    /// - `Reader` values are returned directly.
    pub fn into_reader(self) -> Box<dyn Read + Send> {
        match self {
            FieldValue::String(s) => Box::new(std::io::Cursor::new(s.into_bytes())),
            FieldValue::Reader(r) => r,
        }
    }
}

impl std::fmt::Debug for FieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::String(s) => f.debug_tuple("String").field(s).finish(),
            FieldValue::Reader(_) => f.debug_tuple("Reader").field(&"...").finish(),
        }
    }
}

/// An immutable field within a document.
///
/// Created via [`FieldBuilder`]. Once built, the field is read-only.
// LOCKED
#[derive(Debug)]
pub struct Field {
    name: String,
    field_type: FieldType,
    value: FieldValue,
}

impl Field {
    /// Returns the field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the field type configuration.
    pub fn field_type(&self) -> &FieldType {
        &self.field_type
    }

    /// Returns the field value.
    pub fn value(&self) -> &FieldValue {
        &self.value
    }

    /// Returns a mutable reference to the field value.
    ///
    /// Used to consume reader-backed values via [`std::mem::replace`]
    /// during tokenization.
    pub fn value_mut(&mut self) -> &mut FieldValue {
        &mut self.value
    }
}

/// Builds a [`Field`].
#[derive(Debug)]
pub struct FieldBuilder {
    name: String,
    field_type: Option<FieldType>,
    value: Option<FieldValue>,
}

impl FieldBuilder {
    /// Creates a builder with the given field name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            field_type: None,
            value: None,
        }
    }

    /// Sets the field type.
    pub fn field_type(mut self, field_type: FieldType) -> Self {
        self.field_type = Some(field_type);
        self
    }

    /// Sets the field value to a string.
    pub fn string_value(mut self, value: impl Into<String>) -> Self {
        self.value = Some(FieldValue::String(value.into()));
        self
    }

    /// Sets the field value to a streaming reader.
    pub fn reader_value(mut self, reader: impl Read + Send + 'static) -> Self {
        self.value = Some(FieldValue::Reader(Box::new(reader)));
        self
    }

    /// Consumes the builder and produces an immutable [`Field`].
    ///
    /// # Panics
    ///
    /// - If `field_type` or value was not set.
    /// - If a `Reader` value is used with a non-tokenized field type.
    /// - If a `Reader` value is used with a stored field type.
    pub fn build(self) -> Field {
        let field_type = self.field_type.expect("field_type is required");
        let value = self.value.expect("value is required");

        if matches!(value, FieldValue::Reader(_)) {
            assert!(
                field_type.tokenized,
                "reader-backed fields must be tokenized"
            );
            assert!(!field_type.stored, "reader-backed fields cannot be stored");
        }

        Field {
            name: self.name,
            field_type,
            value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn tokenized_field_creates_text_type() {
        let field = tokenized_field("contents", Cursor::new(b"hello world".to_vec()));
        assert_eq!(field.name(), "contents");
        assert_eq!(*field.field_type(), FieldType::TEXT);
        assert!(matches!(field.value(), FieldValue::Reader(_)));
    }

    #[test]
    fn stored_tokenized_field_creates_text_stored_type() {
        let field = stored_tokenized_field("contents", "hello world");
        assert_eq!(field.name(), "contents");
        assert_eq!(*field.field_type(), FieldType::TEXT_STORED);
        assert!(matches!(field.value(), FieldValue::String(_)));
    }

    #[test]
    fn reader_value_is_consumable() {
        let mut field = tokenized_field("body", Cursor::new(b"streaming content".to_vec()));
        assert!(matches!(field.value(), FieldValue::Reader(_)));

        // Consume via std::mem::replace, same pattern as segment_worker
        let FieldValue::Reader(mut reader) =
            std::mem::replace(field.value_mut(), FieldValue::String(String::new()))
        else {
            panic!("expected Reader");
        };

        let mut buf = String::new();
        reader.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "streaming content");

        // After consumption, field value is the replacement
        assert!(matches!(field.value(), FieldValue::String(_)));
    }

    #[test]
    fn field_value_debug_reader() {
        let val = FieldValue::Reader(Box::new(Cursor::new(b"test".to_vec())));
        let debug = format!("{val:?}");
        assert!(debug.contains("Reader"));
        assert!(!debug.contains("test"));
    }

    #[test]
    fn field_value_debug_string() {
        let val = FieldValue::String("hello".to_string());
        let debug = format!("{val:?}");
        assert!(debug.contains("hello"));
    }

    #[test]
    fn builder_with_reader_value() {
        let field = FieldBuilder::new("body")
            .field_type(FieldType::TEXT)
            .reader_value(Cursor::new(b"built via builder".to_vec()))
            .build();
        assert_eq!(field.name(), "body");
        assert!(matches!(field.value(), FieldValue::Reader(_)));
    }

    #[test]
    #[should_panic(expected = "reader-backed fields must be tokenized")]
    fn reader_with_non_tokenized_panics() {
        FieldBuilder::new("path")
            .field_type(FieldType::STORED)
            .reader_value(Cursor::new(b"bad".to_vec()))
            .build();
    }

    #[test]
    #[should_panic(expected = "reader-backed fields cannot be stored")]
    fn reader_with_stored_panics() {
        FieldBuilder::new("body")
            .field_type(FieldType::TEXT_STORED)
            .reader_value(Cursor::new(b"bad".to_vec()))
            .build();
    }
}
