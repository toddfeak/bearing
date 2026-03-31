// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::mem::{self, Discriminant};

use crate::newindex::field::FieldKind;

/// A field that has been registered in this segment.
#[derive(Debug, Clone)]
pub struct RegisteredField {
    /// The field name.
    pub name: String,
    /// The assigned field number (unique within the segment).
    pub number: u32,
    /// The discriminant of the field kind at registration time.
    pub kind_discriminant: Discriminant<FieldKind>,
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
    /// first occurrence with the provided `FieldKind`.
    ///
    /// If the field was already registered, validates that the new
    /// `FieldKind` discriminant is consistent with the existing one.
    /// Returns an error if there is a conflict.
    pub fn get_or_register(&mut self, name: &str, kind: &FieldKind) -> io::Result<u32> {
        let disc = mem::discriminant(kind);
        if let Some(&idx) = self.by_name.get(name) {
            let existing = &self.fields[idx];
            if existing.kind_discriminant != disc {
                return Err(io::Error::other(format!(
                    "field '{}' registered with {:?}, but now seen with {:?}",
                    name, existing.kind_discriminant, disc
                )));
            }
            Ok(existing.number)
        } else {
            let number = self.fields.len() as u32;
            let idx = self.fields.len();
            self.fields.push(RegisteredField {
                name: name.to_string(),
                number,
                kind_discriminant: disc,
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
    use crate::newindex::field::{stored_field, stored_tokenized_field, tokenized_field};

    #[test]
    fn register_new_field_assigns_zero() {
        let mut reg = FieldInfoRegistry::new();
        let field = stored_field("title", "hello");
        let num = reg.get_or_register("title", field.kind()).unwrap();
        assert_eq!(num, 0);
    }

    #[test]
    fn same_field_returns_same_number() {
        let mut reg = FieldInfoRegistry::new();
        let f1 = stored_field("title", "a");
        let f2 = stored_field("title", "b");
        let n1 = reg.get_or_register("title", f1.kind()).unwrap();
        let n2 = reg.get_or_register("title", f2.kind()).unwrap();
        assert_eq!(n1, n2);
    }

    #[test]
    fn different_fields_get_sequential_numbers() {
        let mut reg = FieldInfoRegistry::new();
        let f0 = stored_field("title", "a");
        let f1 = stored_field("body", "b");
        let f2 = stored_field("author", "c");
        let n0 = reg.get_or_register("title", f0.kind()).unwrap();
        let n1 = reg.get_or_register("body", f1.kind()).unwrap();
        let n2 = reg.get_or_register("author", f2.kind()).unwrap();
        assert_eq!(n0, 0);
        assert_eq!(n1, 1);
        assert_eq!(n2, 2);
    }

    #[test]
    fn conflicting_kind_returns_error() {
        let mut reg = FieldInfoRegistry::new();
        let stored = stored_field("title", "a");
        reg.get_or_register("title", stored.kind()).unwrap();

        let tokenized = stored_tokenized_field("title", "a");
        let result = reg.get_or_register("title", tokenized.kind());
        assert!(result.is_err());
    }

    #[test]
    fn registered_fields_returns_in_order() {
        let mut reg = FieldInfoRegistry::new();
        let fa = stored_field("a", "x");
        let fb = stored_field("b", "y");
        reg.get_or_register("a", fa.kind()).unwrap();
        reg.get_or_register("b", fb.kind()).unwrap();
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
        let stored = stored_field("body", "x");
        reg.get_or_register("body", stored.kind()).unwrap();

        let tok = tokenized_field("body", Cursor::new(b"y".to_vec()));
        let result = reg.get_or_register("body", tok.kind());
        assert!(result.is_err());
    }
}
