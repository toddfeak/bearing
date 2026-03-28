// SPDX-License-Identifier: Apache-2.0

//! Codec writer traits for the flush and merge write paths.
//!
//! These traits define the interface between the indexing chain and the codec
//! writers. At flush time, `FreqProxFields` implements these traits to read
//! from in-memory byte pool streams. At merge time, different implementations
//! will wrap multiple segment readers.
//!
//! The trait names use a `Writer` suffix to distinguish them from the read-path
//! traits in `index::terms`.

use std::io;

/// Sentinel value returned by [`PostingsEnumWriter::next_doc`] when iteration
/// is exhausted.
pub const NO_MORE_DOCS: i32 = i32::MAX;

/// Access to all fields in a segment for codec writing.
///
/// Consumed by `BlockTreeTermsWriter::write()` during flush and merge.
pub trait FieldsWriter {
    /// Returns field names in the order they should be written.
    fn field_names(&self) -> Vec<String>;

    /// Returns the terms for the given field, or `None` if the field has no terms.
    fn terms(&self, field: &str) -> Option<Box<dyn TermsWriter + '_>>;
}

/// Access to the terms of a single field for codec writing.
///
/// Consumed by `BlockTreeTermsWriter` to get a term iterator and field metadata.
pub trait TermsWriter {
    /// Returns an iterator over all terms in sorted order.
    fn iterator(&self) -> io::Result<Box<dyn TermsEnumWriter + '_>>;

    /// Whether this field indexes term frequencies.
    fn has_freqs(&self) -> bool;

    /// Whether this field indexes positions.
    fn has_positions(&self) -> bool;

    /// Whether this field indexes offsets.
    fn has_offsets(&self) -> bool;

    /// Whether any token in this field had a payload.
    fn has_payloads(&self) -> bool;
}

/// Iterator over terms in sorted order for codec writing.
///
/// The codec calls `next()` to advance to each term, then `postings()` to get
/// the posting data for that term.
pub trait TermsEnumWriter {
    /// Advances to the next term. Returns the term bytes, or `None` at end.
    fn next(&mut self) -> io::Result<Option<&[u8]>>;

    /// Returns a postings iterator for the current term.
    ///
    /// Must be called after a successful `next()`.
    fn postings(&mut self) -> io::Result<Box<dyn PostingsEnumWriter + '_>>;

    // TODO(lucene-alignment): seekCeil() — not called by codec during flush.
    // Needed for merge path. Add when segment merging is implemented.
}

/// Iterator over postings (doc/freq/position/offset/payload) for a single term.
///
/// Consumed by `PushPostingsWriterBase::writeTerm()` (and its subclass
/// `Lucene103PostingsWriter`) during flush and merge.
///
/// Call pattern: `next_doc()` in a loop, for each doc optionally call `freq()`,
/// then `next_position()` × freq times, each optionally followed by
/// `start_offset()`, `end_offset()`, `payload()`.
pub trait PostingsEnumWriter {
    /// Advances to the next document. Returns the doc ID, or
    /// [`NO_MORE_DOCS`] when exhausted.
    fn next_doc(&mut self) -> io::Result<i32>;

    /// Returns the frequency of the current term in the current document.
    fn freq(&self) -> i32;

    /// Advances to the next position within the current document.
    /// Returns the position.
    fn next_position(&mut self) -> io::Result<i32>;

    /// Returns the start character offset of the current position.
    fn start_offset(&self) -> i32;

    /// Returns the end character offset of the current position.
    fn end_offset(&self) -> i32;

    /// Returns the payload at the current position, if any.
    fn payload(&self) -> Option<&[u8]>;
}
