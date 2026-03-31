// SPDX-License-Identifier: Apache-2.0

//! Standard analyzer adapter for the newindex pipeline.
//!
//! Bridges the push-based `analysis::Analyzer` (callback with `TokenRef`)
//! to the pull-based `newindex::Analyzer` trait (`next_token` iteration).

use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Read};

use crate::analysis;
use crate::analysis::Analyzer as AnalysisTrait;
use crate::newindex::analyzer::{Analyzer, Token};

/// Buffered token from a single `analyze_to` pass.
struct BufferedToken {
    text: String,
    start_offset: i32,
    end_offset: i32,
    position_increment: i32,
}

/// Standard text analyzer adapter.
///
/// Wraps the existing `analysis::StandardAnalyzer` (push-based callback API)
/// and adapts it to the pull-based `newindex::Analyzer` trait. On the first
/// `next_token` call for a field, reads all input, tokenizes via `analyze_to`,
/// and buffers the tokens. Subsequent calls pop from the buffer.
#[derive(Default)]
pub struct StandardAnalyzer {
    inner: analysis::StandardAnalyzer,
    tokens: VecDeque<BufferedToken>,
    consumed: bool,
}

impl fmt::Debug for StandardAnalyzer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StandardAnalyzer")
            .field("buffered_tokens", &self.tokens.len())
            .finish()
    }
}

impl Analyzer for StandardAnalyzer {
    fn next_token<'b>(
        &mut self,
        reader: &mut dyn Read,
        buf: &'b mut String,
    ) -> io::Result<Option<Token<'b>>> {
        if !self.consumed {
            // Read all input and tokenize in one pass
            let mut input = String::new();
            reader.read_to_string(&mut input)?;
            self.consumed = true;

            let mut analyze_buf = String::new();
            self.inner.analyze_to(&input, &mut analyze_buf, &mut |tr| {
                self.tokens.push_back(BufferedToken {
                    text: tr.text.to_string(),
                    start_offset: tr.start_offset as i32,
                    end_offset: tr.end_offset as i32,
                    position_increment: tr.position_increment as i32,
                });
            });
        }

        match self.tokens.pop_front() {
            Some(bt) => {
                buf.clear();
                buf.push_str(&bt.text);
                Ok(Some(Token {
                    text: buf,
                    start_offset: bt.start_offset,
                    end_offset: bt.end_offset,
                    position_increment: bt.position_increment,
                }))
            }
            None => Ok(None),
        }
    }

    fn reset(&mut self) {
        self.tokens.clear();
        self.consumed = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use assertables::*;

    fn collect_tokens(text: &str) -> Vec<(String, i32, i32, i32)> {
        let mut analyzer = StandardAnalyzer::default();
        let mut reader: &[u8] = text.as_bytes();
        let mut buf = String::new();
        let mut result = Vec::new();

        while let Some(token) = analyzer.next_token(&mut reader, &mut buf).unwrap() {
            result.push((
                token.text.to_string(),
                token.start_offset,
                token.end_offset,
                token.position_increment,
            ));
        }
        result
    }

    #[test]
    fn tokenizes_simple_text() {
        let tokens = collect_tokens("hello world");
        assert_len_eq_x!(&tokens, 2);
        assert_eq!(tokens[0].0, "hello");
        assert_eq!(tokens[1].0, "world");
    }

    #[test]
    fn lowercases_tokens() {
        let tokens = collect_tokens("Hello WORLD");
        assert_eq!(tokens[0].0, "hello");
        assert_eq!(tokens[1].0, "world");
    }

    #[test]
    fn empty_string_produces_no_tokens() {
        let tokens = collect_tokens("");
        assert_is_empty!(&tokens);
    }

    #[test]
    fn preserves_contractions() {
        let tokens = collect_tokens("don't");
        assert_len_eq_x!(&tokens, 1);
        assert_eq!(tokens[0].0, "don't");
    }

    #[test]
    fn offsets_are_correct() {
        let tokens = collect_tokens("hello world");
        // "hello" at 0..5
        assert_eq!(tokens[0].1, 0); // start_offset
        assert_eq!(tokens[0].2, 5); // end_offset
        // "world" at 6..11
        assert_eq!(tokens[1].1, 6);
        assert_eq!(tokens[1].2, 11);
    }

    #[test]
    fn position_increments_are_one() {
        let tokens = collect_tokens("one two three");
        for t in &tokens {
            assert_eq!(t.3, 1);
        }
    }

    #[test]
    fn reset_allows_reuse() {
        let mut analyzer = StandardAnalyzer::default();
        let mut buf = String::new();

        // First field
        let mut reader: &[u8] = b"hello";
        let token = analyzer.next_token(&mut reader, &mut buf).unwrap();
        assert_some!(&token);
        let none = analyzer.next_token(&mut reader, &mut buf).unwrap();
        assert_none!(&none);

        // Reset and process second field
        analyzer.reset();
        let mut reader: &[u8] = b"world";
        let token = analyzer.next_token(&mut reader, &mut buf).unwrap();
        assert_some!(&token);
        assert_eq!(buf, "world");
    }
}
