// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::document::{DocValuesType, IndexOptions};
use crate::newindex::field::FieldType;

/// Captures the structural identity of a field type for conflict detection.
///
/// Two fields with the same name must have the same shape within a segment.
/// The shape captures which axes are active and their derived properties,
/// but not the actual values.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldShape {
    is_stored: bool,
    index_options: IndexOptions,
    has_norms: bool,
    doc_values_type: DocValuesType,
    has_points: bool,
}

impl FieldShape {
    fn from_field_type(ft: &FieldType) -> Self {
        Self {
            is_stored: ft.stored().is_some(),
            index_options: ft.index_options(),
            has_norms: ft.has_norms(),
            doc_values_type: ft.doc_values_type(),
            has_points: ft.points().is_some(),
        }
    }
}

/// A field that has been registered in this segment.
#[derive(Debug, Clone)]
pub struct RegisteredField {
    /// The field name.
    pub name: String,
    /// The assigned field number (unique within the segment).
    pub number: u32,
    /// The structural shape of the field type at registration time.
    shape: FieldShape,
}

/// Per-segment registry of field metadata.
///
/// Owned by a single worker thread. Assigns field numbers on first
/// occurrence of a field name and returns the existing entry on
/// subsequent calls. Validates that the same field name always has
/// consistent options within the segment.
// LOCKED
#[derive(Default)]
pub struct FieldInfoRegistry {
    fields: Vec<RegisteredField>,
    by_name: HashMap<String, usize>,
}

impl fmt::Debug for FieldInfoRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FieldInfoRegistry")
            .field("field_count", &self.fields.len())
            .finish()
    }
}

impl FieldInfoRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the field number for the given name, registering it on
    /// first occurrence with the provided [`FieldType`].
    ///
    /// If the field was already registered, validates that the new
    /// field type shape is consistent with the existing one. Returns
    /// an error if there is a conflict.
    pub fn get_or_register(&mut self, name: &str, field_type: &FieldType) -> io::Result<u32> {
        let shape = FieldShape::from_field_type(field_type);
        if let Some(&idx) = self.by_name.get(name) {
            let existing = &self.fields[idx];
            if existing.shape != shape {
                return Err(io::Error::other(format!(
                    "field '{}' registered with {:?}, but now seen with {:?}",
                    name, existing.shape, shape
                )));
            }
            Ok(existing.number)
        } else {
            let number = self.fields.len() as u32;
            let idx = self.fields.len();
            self.fields.push(RegisteredField {
                name: name.to_string(),
                number,
                shape,
            });
            self.by_name.insert(name.to_string(), idx);
            Ok(number)
        }
    }

    /// Returns the registered fields in registration order.
    pub fn registered_fields(&self) -> &[RegisteredField] {
        &self.fields
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::newindex::field::{stored, text};

    #[test]
    fn register_new_field_assigns_zero() {
        let mut reg = FieldInfoRegistry::new();
        let field = stored("title").string("hello");
        let num = reg.get_or_register("title", field.field_type()).unwrap();
        assert_eq!(num, 0);
    }

    #[test]
    fn same_field_returns_same_number() {
        let mut reg = FieldInfoRegistry::new();
        let f1 = stored("title").string("a");
        let f2 = stored("title").string("b");
        let n1 = reg.get_or_register("title", f1.field_type()).unwrap();
        let n2 = reg.get_or_register("title", f2.field_type()).unwrap();
        assert_eq!(n1, n2);
    }

    #[test]
    fn different_fields_get_sequential_numbers() {
        let mut reg = FieldInfoRegistry::new();
        let f0 = stored("title").string("a");
        let f1 = stored("body").string("b");
        let f2 = stored("author").string("c");
        let n0 = reg.get_or_register("title", f0.field_type()).unwrap();
        let n1 = reg.get_or_register("body", f1.field_type()).unwrap();
        let n2 = reg.get_or_register("author", f2.field_type()).unwrap();
        assert_eq!(n0, 0);
        assert_eq!(n1, 1);
        assert_eq!(n2, 2);
    }

    #[test]
    fn conflicting_kind_returns_error() {
        let mut reg = FieldInfoRegistry::new();
        let s = stored("title").string("a");
        reg.get_or_register("title", s.field_type()).unwrap();

        let t = text("title").stored().value("a");
        let result = reg.get_or_register("title", t.field_type());
        assert!(result.is_err());
    }

    #[test]
    fn registered_fields_returns_in_order() {
        let mut reg = FieldInfoRegistry::new();
        let fa = stored("a").string("x");
        let fb = stored("b").string("y");
        reg.get_or_register("a", fa.field_type()).unwrap();
        reg.get_or_register("b", fb.field_type()).unwrap();
        let fields = reg.registered_fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "a");
        assert_eq!(fields[0].number, 0);
        assert_eq!(fields[1].name, "b");
        assert_eq!(fields[1].number, 1);
    }

    #[test]
    fn stored_vs_tokenized_conflicts() {
        let mut reg = FieldInfoRegistry::new();
        let s = stored("body").string("x");
        reg.get_or_register("body", s.field_type()).unwrap();

        let t = text("body").reader(Cursor::new(b"y".to_vec()));
        let result = reg.get_or_register("body", t.field_type());
        assert!(result.is_err());
    }
}
