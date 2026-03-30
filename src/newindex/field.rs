// SPDX-License-Identifier: Apache-2.0

/// Configuration flags for how a field is indexed, stored, and searched.
///
/// Reusable across fields that share the same configuration.
// LOCKED
#[derive(Debug, Clone)]
pub struct FieldType {}

/// An immutable field within a document.
///
/// Created via [`FieldBuilder`]. Once built, the field is read-only.
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

    /// Returns the field type configuration.
    pub fn field_type(&self) -> &FieldType {
        &self.field_type
    }
}

/// Builds a [`Field`].
#[derive(Debug)]
pub struct FieldBuilder {
    name: String,
    field_type: Option<FieldType>,
}

impl FieldBuilder {
    /// Creates a builder with the given field name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            field_type: None,
        }
    }

    /// Sets the field type.
    pub fn field_type(mut self, field_type: FieldType) -> Self {
        self.field_type = Some(field_type);
        self
    }

    /// Consumes the builder and produces an immutable [`Field`].
    ///
    /// Panics if `field_type` was not set.
    pub fn build(self) -> Field {
        Field {
            name: self.name,
            field_type: self.field_type.expect("field_type is required"),
        }
    }
}
