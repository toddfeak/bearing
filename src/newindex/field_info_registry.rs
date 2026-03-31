// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::io;

use crate::newindex::field::FieldType;

/// A field that has been registered in this segment.
#[derive(Debug, Clone)]
pub struct RegisteredField {
    /// The field name.
    pub name: String,
    /// The assigned field number (unique within the segment).
    pub number: u32,
    /// The field type configuration at registration time.
    pub field_type: FieldType,
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

impl std::fmt::Debug for FieldInfoRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    /// first occurrence with the provided `FieldType`.
    ///
    /// If the field was already registered, validates that the new
    /// `FieldType` is consistent with the existing one. Returns an
    /// error if there is a conflict.
    pub fn get_or_register(&mut self, name: &str, field_type: &FieldType) -> io::Result<u32> {
        if let Some(&idx) = self.by_name.get(name) {
            let existing = &self.fields[idx];
            if existing.field_type != *field_type {
                return Err(io::Error::other(format!(
                    "field '{}' registered with {:?}, but now seen with {:?}",
                    name, existing.field_type, field_type
                )));
            }
            Ok(existing.number)
        } else {
            let number = self.fields.len() as u32;
            let idx = self.fields.len();
            self.fields.push(RegisteredField {
                name: name.to_string(),
                number,
                field_type: field_type.clone(),
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
    use super::*;

    #[test]
    fn register_new_field_assigns_zero() {
        let mut reg = FieldInfoRegistry::new();
        let ft = FieldType {
            stored: true,
            ..Default::default()
        };
        let num = reg.get_or_register("title", &ft).unwrap();
        assert_eq!(num, 0);
    }

    #[test]
    fn same_field_returns_same_number() {
        let mut reg = FieldInfoRegistry::new();
        let ft = FieldType {
            stored: true,
            ..Default::default()
        };
        let n1 = reg.get_or_register("title", &ft).unwrap();
        let n2 = reg.get_or_register("title", &ft).unwrap();
        assert_eq!(n1, n2);
    }

    #[test]
    fn different_fields_get_sequential_numbers() {
        let mut reg = FieldInfoRegistry::new();
        let ft = FieldType {
            stored: true,
            ..Default::default()
        };
        let n0 = reg.get_or_register("title", &ft).unwrap();
        let n1 = reg.get_or_register("body", &ft).unwrap();
        let n2 = reg.get_or_register("author", &ft).unwrap();
        assert_eq!(n0, 0);
        assert_eq!(n1, 1);
        assert_eq!(n2, 2);
    }

    #[test]
    fn conflicting_type_returns_error() {
        let mut reg = FieldInfoRegistry::new();
        reg.get_or_register(
            "title",
            &FieldType {
                stored: true,
                ..Default::default()
            },
        )
        .unwrap();
        let result = reg.get_or_register(
            "title",
            &FieldType {
                stored: false,
                ..Default::default()
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn registered_fields_returns_in_order() {
        let mut reg = FieldInfoRegistry::new();
        let ft = FieldType {
            stored: true,
            ..Default::default()
        };
        reg.get_or_register("a", &ft).unwrap();
        reg.get_or_register("b", &ft).unwrap();
        let fields = reg.registered_fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "a");
        assert_eq!(fields[0].number, 0);
        assert_eq!(fields[1].name, "b");
        assert_eq!(fields[1].number, 1);
    }

    #[test]
    fn conflicting_tokenized_returns_error() {
        let mut reg = FieldInfoRegistry::new();
        reg.get_or_register(
            "body",
            &FieldType {
                tokenized: true,
                ..Default::default()
            },
        )
        .unwrap();
        let result = reg.get_or_register(
            "body",
            &FieldType {
                tokenized: false,
                ..Default::default()
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn conflicting_omit_norms_returns_error() {
        let mut reg = FieldInfoRegistry::new();
        reg.get_or_register(
            "body",
            &FieldType {
                tokenized: true,
                omit_norms: false,
                ..Default::default()
            },
        )
        .unwrap();
        let result = reg.get_or_register(
            "body",
            &FieldType {
                tokenized: true,
                omit_norms: true,
                ..Default::default()
            },
        );
        assert!(result.is_err());
    }
}
