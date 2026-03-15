// SPDX-License-Identifier: Apache-2.0

//! Text analysis pipeline: tokenizers, token filters, and analyzers.
//!
//! The [`Analyzer`] trait is the main entry point. [`StandardAnalyzer`] provides
//! Unicode-aware tokenization with lowercase normalization, matching Lucene's
//! `StandardAnalyzer`.

pub mod standard;

pub use standard::{LowerCaseFilter, StandardAnalyzer, StandardTokenizer};

/// A token produced by a tokenizer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token {
    /// The token text.
    pub text: String,
    /// Start character offset in the original input.
    pub start_offset: usize,
    /// End character offset in the original input.
    pub end_offset: usize,
    /// Position increment (distance from previous token). Usually 1.
    pub position_increment: u32,
}

/// Trait for tokenizers that split text into tokens.
pub trait Tokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token>;
}

/// Trait for token filters that transform a stream of tokens.
pub trait TokenFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token>;
}

/// A borrowed token reference, used by the zero-allocation `analyze_to()` path.
/// Borrows its text from a pre-lowercased buffer rather than allocating a new String.
pub struct TokenRef<'a> {
    pub text: &'a str,
    pub start_offset: usize,
    pub end_offset: usize,
    pub position_increment: u32,
}

/// Trait for analyzers that combine a tokenizer and filters.
pub trait Analyzer: Send + Sync {
    fn analyze(&self, text: &str) -> Vec<Token>;

    /// Zero-allocation analysis path. Lowercases `text` into `buf`, then
    /// invokes `callback` for each token borrowing from `buf`.
    ///
    /// The default implementation falls back to `analyze()` and wraps
    /// each owned `Token` in a `TokenRef`.
    fn analyze_to(&self, text: &str, buf: &mut String, callback: &mut dyn FnMut(TokenRef<'_>)) {
        let _ = buf; // unused in default impl
        for token in self.analyze(text) {
            callback(TokenRef {
                text: &token.text,
                start_offset: token.start_offset,
                end_offset: token.end_offset,
                position_increment: token.position_increment,
            });
        }
    }
}
