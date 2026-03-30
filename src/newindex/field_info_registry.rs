// SPDX-License-Identifier: Apache-2.0

use std::io;

use crate::newindex::field::FieldType;

/// Per-segment registry of field metadata.
///
/// Owned by a single worker thread. Assigns field numbers on first
/// occurrence of a field name and returns the existing entry on
/// subsequent calls. Validates that the same field name always has
/// consistent options within the segment.
// LOCKED
#[derive(Default)]
pub struct FieldInfoRegistry {}

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
    pub fn get_or_register(&mut self, _name: &str, _field_type: &FieldType) -> io::Result<u32> {
        todo!()
    }
}
