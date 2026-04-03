// SPDX-License-Identifier: Apache-2.0

//! Text analysis pipeline: tokenizers and analyzers.
//!
//! The [`Analyzer`] trait is the main entry point — a stateful, pull-based
//! token iterator. [`StandardAnalyzer`] provides Unicode-aware tokenization
//! with lowercase normalization, matching Lucene's `StandardAnalyzer`.

use std::io::{self, Read};

pub mod standard;

pub use standard::StandardAnalyzer;

/// A single token produced by the analyzer during tokenization.
#[derive(Debug)]
pub struct Token<'a> {
    /// The token text, borrowed from the analyzer's buffer.
    pub text: &'a str,
    /// Start character offset in the original input.
    pub start_offset: i32,
    /// End character offset in the original input.
    pub end_offset: i32,
    /// Position increment (distance from previous token). Usually 1.
    pub position_increment: i32,
}

/// Breaks text into a stream of tokens for indexing.
///
/// The caller drives the loop by calling `next_token` repeatedly.
/// The analyzer reads from the provided `Reader` incrementally and
/// writes each token's text into a caller-owned buffer. The returned
/// `Token` borrows from that buffer, avoiding per-token allocation.
///
/// Each implementation holds its own internal parsing state (offsets,
/// leftover bytes, etc.) which is reset when a new field begins.
pub trait Analyzer: Send {
    /// Reads the next token from `reader`, writing token text into `buf`.
    ///
    /// Returns `None` when the input is exhausted. The returned `Token`
    /// borrows its text from `buf`. The caller must let the token drop
    /// before calling `next_token` again (which the natural loop does).
    fn next_token<'b>(
        &mut self,
        reader: &mut dyn Read,
        buf: &'b mut String,
    ) -> io::Result<Option<Token<'b>>>;

    /// Resets internal state for processing a new field.
    fn reset(&mut self);
}
