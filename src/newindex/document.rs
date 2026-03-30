// SPDX-License-Identifier: Apache-2.0

use crate::newindex::field::Field;

/// An immutable collection of fields to be indexed.
///
/// Created via [`DocumentBuilder`]. Once built, the document is read-only
/// and consumed by the indexing pipeline.
// LOCKED
#[derive(Debug)]
pub struct Document {
    fields: Vec<Field>,
}

impl Document {
    /// Returns the fields in this document.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
}

/// Builds a [`Document`] by accumulating fields.
#[derive(Debug, Default)]
pub struct DocumentBuilder {
    fields: Vec<Field>,
}

impl DocumentBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a field to the document.
    pub fn add_field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    /// Consumes the builder and produces an immutable [`Document`].
    pub fn build(self) -> Document {
        Document {
            fields: self.fields,
        }
    }
}
