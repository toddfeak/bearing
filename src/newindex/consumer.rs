// SPDX-License-Identifier: Apache-2.0

use std::io;

use crate::newindex::analyzer::Token;
use crate::newindex::field::Field;

/// Lifecycle trait for processing document data during indexing.
///
/// Each consumer handles one aspect of field data (postings, stored
/// fields, norms, term vectors, doc values, points). The SegmentWorker
/// calls these methods in order for every document — the consumer
/// decides internally whether to act on a given field or ignore it.
///
/// This trait is the core of the indexing pipeline. All data flows
/// through these lifecycle methods.
///
/// # Call sequence per document
///
/// ```text
/// start_document(doc_id)
///   for each field:
///     add_field(field_id, field)
///     if tokenized and wants_tokens(field_id, field):
///       for each token from analyzer:
///         add_token(field_id, field, token)
/// finish_document(doc_id)
/// ```
///
/// # Flush
///
/// After one or more documents have been processed, `flush` is called
/// to write accumulated data to segment files. The consumer is then
/// dropped along with the worker.
// LOCKED
pub trait FieldConsumer {
    /// A new document is beginning.
    fn start_document(&mut self, doc_id: i32) -> io::Result<()>;

    /// A field from the current document. The consumer decides whether
    /// this field is relevant and processes it accordingly.
    ///
    /// Called once per field, before any tokens for that field.
    fn add_field(&mut self, field_id: u32, field: &Field) -> io::Result<()>;

    /// Whether this consumer wants to receive tokens for the given field.
    ///
    /// Called once per tokenized field to build the filtered consumer
    /// list for the token loop. FieldConsumers that don't process tokens
    /// (e.g., stored fields, points) return false.
    fn wants_tokens(&self, field_id: u32, field: &Field) -> bool;

    /// A single token from a tokenized field. Only called on consumers
    /// that returned true from `wants_tokens` for this field.
    ///
    /// The field reference is provided so the consumer has full context
    /// without needing to track "current field" state.
    fn add_token(&mut self, field_id: u32, field: &Field, token: &Token<'_>) -> io::Result<()>;

    /// The current document is complete. FieldConsumers may finalize
    /// per-document state (e.g., flush term vectors, store norms).
    fn finish_document(&mut self, doc_id: i32) -> io::Result<()>;

    /// Write all accumulated data to segment files.
    /// Returns the names of the files written.
    fn flush(&mut self) -> io::Result<Vec<String>>;
}
