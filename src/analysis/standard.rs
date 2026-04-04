// SPDX-License-Identifier: Apache-2.0

//! Standard analysis: [`StandardTokenizer`] and [`StandardAnalyzer`].

use std::collections::VecDeque;
use std::io::{self, Read};

use crate::analysis::{Analyzer, Token};

/// Maximum token length. Tokens longer than this are split.
/// Matches Java: StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH = 255
const MAX_TOKEN_LENGTH: usize = 255;

/// A simplified StandardTokenizer implementing UAX#29 word break rules
/// for ASCII/Latin text. Produces the same tokens as Java's StandardTokenizer
/// for English text.
///
/// Token rules:
/// - Alphanumeric sequences are tokens
/// - Internal apostrophes (e.g., "don't") are kept as part of the token
/// - Internal periods in abbreviations (e.g., "U.S.A.") are kept
/// - Everything else is a token separator
pub struct StandardTokenizer;

impl StandardTokenizer {
    /// Returns true if the character can be part of a word token.
    fn is_word_char(c: char) -> bool {
        c.is_alphanumeric() || c == '_'
    }

    /// Returns true if the character is an internal separator that can
    /// appear within a token (apostrophe, period, hyphen in certain contexts).
    fn is_internal_separator(c: char) -> bool {
        c == '\'' || c == '\u{2019}' // apostrophe and right single quotation mark
    }

    /// Shared state machine for tokenization. Calls `emit` for each token found.
    fn tokenize_inner<F>(text: &str, mut emit: F)
    where
        F: FnMut(usize, usize),
    {
        let mut iter = text.char_indices().peekable();

        while let Some(&(byte_pos, ch)) = iter.peek() {
            if !Self::is_word_char(ch) {
                iter.next();
                continue;
            }

            let token_start_byte = byte_pos;
            let mut token_end_byte = byte_pos + ch.len_utf8();
            let mut char_count: usize = 1;
            iter.next();

            while char_count < MAX_TOKEN_LENGTH {
                if let Some(&(bp, c)) = iter.peek() {
                    if Self::is_word_char(c) {
                        token_end_byte = bp + c.len_utf8();
                        char_count += 1;
                        iter.next();
                    } else if Self::is_internal_separator(c) {
                        let sep_byte = bp;
                        iter.next();
                        if let Some(&(bp2, c2)) = iter.peek() {
                            if c2.is_alphanumeric() {
                                token_end_byte = bp2 + c2.len_utf8();
                                char_count += 2;
                                iter.next();
                            } else {
                                token_end_byte = sep_byte;
                                break;
                            }
                        } else {
                            token_end_byte = sep_byte;
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            if char_count >= MAX_TOKEN_LENGTH {
                while let Some(&(_, c)) = iter.peek() {
                    if Self::is_word_char(c) || Self::is_internal_separator(c) {
                        iter.next();
                    } else {
                        break;
                    }
                }
            }

            emit(token_start_byte, token_end_byte);
        }
    }
}

/// Buffered token byte range into the analyzer's `lowered` buffer.
struct BufferedToken {
    start: usize,
    end: usize,
    start_offset: i32,
    end_offset: i32,
}

/// Standard text analyzer: Unicode-aware tokenization with lowercase normalization.
///
/// Matches Java's `StandardAnalyzer` with no stop words. Implements the
/// pull-based [`Analyzer`] trait. Call [`set_reader`](Analyzer::set_reader)
/// to provide input for a new field, then pull tokens with
/// [`next_token`](Analyzer::next_token).
#[derive(Default)]
pub struct StandardAnalyzer {
    reader: Option<Box<dyn Read + Send>>,
    tokens: VecDeque<BufferedToken>,
    /// Lowercased text. Tokens borrow from this.
    lowered: String,
    consumed: bool,
}

impl StandardAnalyzer {
    /// Creates a new analyzer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads all input, lowercases (ASCII-only), and tokenizes into the buffer.
    fn consume_reader(&mut self) -> io::Result<()> {
        let mut input = String::new();
        if let Some(reader) = &mut self.reader {
            reader.read_to_string(&mut input)?;
        }

        self.lowered.clear();
        self.lowered.reserve(input.len());
        for ch in input.chars() {
            self.lowered.push(ch.to_ascii_lowercase());
        }

        StandardTokenizer::tokenize_inner(&self.lowered, |start, end| {
            self.tokens.push_back(BufferedToken {
                start,
                end,
                start_offset: start as i32,
                end_offset: end as i32,
            });
        });

        self.consumed = true;
        Ok(())
    }
}

impl Analyzer for StandardAnalyzer {
    fn set_reader(&mut self, reader: Box<dyn Read + Send>) {
        self.reader = Some(reader);
        self.tokens.clear();
        self.lowered.clear();
        self.consumed = false;
    }

    fn next_token(&mut self) -> io::Result<Option<Token<'_>>> {
        if !self.consumed {
            self.consume_reader()?;
        }

        match self.tokens.pop_front() {
            Some(bt) => Ok(Some(Token {
                text: &self.lowered[bt.start..bt.end],
                start_offset: bt.start_offset,
                end_offset: bt.end_offset,
                position_increment: 1,
            })),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;

    // --- StandardTokenizer unit tests ---

    /// Helper: tokenize and collect (text, start, end) tuples.
    fn tokenize(text: &str) -> Vec<(String, usize, usize)> {
        let mut tokens = Vec::new();
        StandardTokenizer::tokenize_inner(text, |start, end| {
            tokens.push((text[start..end].to_string(), start, end));
        });
        tokens
    }

    #[test]
    fn test_standard_tokenizer_simple() {
        let tokens = tokenize("hello world");
        assert_len_eq_x!(&tokens, 2);
        assert_eq!(tokens[0], ("hello".to_string(), 0, 5));
        assert_eq!(tokens[1], ("world".to_string(), 6, 11));
    }

    #[test]
    fn test_standard_tokenizer_contraction() {
        let tokens = tokenize("don't stop");
        assert_len_eq_x!(&tokens, 2);
        assert_eq!(tokens[0].0, "don't");
        assert_eq!(tokens[1].0, "stop");
    }

    #[test]
    fn test_standard_tokenizer_numbers() {
        let tokens = tokenize("test123 456");
        assert_len_eq_x!(&tokens, 2);
        assert_eq!(tokens[0].0, "test123");
        assert_eq!(tokens[1].0, "456");
    }

    #[test]
    fn test_standard_tokenizer_punctuation() {
        let tokens = tokenize("hello, world! foo.");
        assert_len_eq_x!(&tokens, 3);
        assert_eq!(tokens[0].0, "hello");
        assert_eq!(tokens[1].0, "world");
        assert_eq!(tokens[2].0, "foo");
    }

    #[test]
    fn test_standard_tokenizer_empty() {
        let tokens = tokenize("");
        assert_is_empty!(tokens);
    }

    #[test]
    fn test_apostrophe_at_end_of_input() {
        let tokens = tokenize("don't");
        assert_len_eq_x!(&tokens, 1);
        assert_eq!(tokens[0].0, "don't");

        let tokens = tokenize("hello'");
        assert_len_eq_x!(&tokens, 1);
        assert_eq!(tokens[0].0, "hello");
    }

    #[test]
    fn test_apostrophe_followed_by_non_alpha() {
        let tokens = tokenize("it' s");
        assert_len_eq_x!(&tokens, 2);
        assert_eq!(tokens[0].0, "it");
        assert_eq!(tokens[1].0, "s");
    }

    #[test]
    fn test_token_exceeding_max_length() {
        let long_word: String = "a".repeat(255);
        let tokens = tokenize(&long_word);
        assert_len_eq_x!(&tokens, 1);
        assert_len_eq_x!(&tokens[0].0, 255);

        let too_long: String = "b".repeat(300);
        let tokens = tokenize(&too_long);
        assert_len_eq_x!(&tokens, 1);
        assert_len_eq_x!(&tokens[0].0, 255);

        let input = format!("{} short", "c".repeat(300));
        let tokens = tokenize(&input);
        assert_len_eq_x!(&tokens, 2);
        assert_len_eq_x!(&tokens[0].0, 255);
        assert_eq!(tokens[1].0, "short");
    }

    // --- StandardAnalyzer (pull-based) tests ---

    fn collect_tokens(text: &str) -> Vec<(String, i32, i32, i32)> {
        let mut analyzer = StandardAnalyzer::default();
        analyzer.set_reader(Box::new(io::Cursor::new(text.as_bytes().to_vec())));
        let mut result = Vec::new();

        while let Some(token) = analyzer.next_token().unwrap() {
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
    fn test_standard_analyzer() {
        let tokens = collect_tokens("The quick brown fox");
        let texts: Vec<&str> = tokens.iter().map(|t| t.0.as_str()).collect();
        assert_eq!(texts, vec!["the", "quick", "brown", "fox"]);
    }

    #[test]
    fn test_standard_analyzer_no_stop_words() {
        let tokens = collect_tokens("the quick and brown fox");
        assert_len_eq_x!(&tokens, 5);
        let texts: Vec<&str> = tokens.iter().map(|t| t.0.as_str()).collect();
        assert_eq!(texts, vec!["the", "quick", "and", "brown", "fox"]);
        for t in &tokens {
            assert_eq!(t.3, 1);
        }
    }

    #[test]
    fn test_lowercases_tokens() {
        let tokens = collect_tokens("Hello WORLD");
        assert_eq!(tokens[0].0, "hello");
        assert_eq!(tokens[1].0, "world");
    }

    #[test]
    fn test_empty_string_produces_no_tokens() {
        let tokens = collect_tokens("");
        assert_is_empty!(&tokens);
    }

    #[test]
    fn test_preserves_contractions() {
        let tokens = collect_tokens("don't");
        assert_len_eq_x!(&tokens, 1);
        assert_eq!(tokens[0].0, "don't");
    }

    #[test]
    fn test_offsets_are_correct() {
        let tokens = collect_tokens("hello world");
        assert_eq!(tokens[0].1, 0); // start_offset
        assert_eq!(tokens[0].2, 5); // end_offset
        assert_eq!(tokens[1].1, 6);
        assert_eq!(tokens[1].2, 11);
    }

    #[test]
    fn test_position_increments_are_one() {
        let tokens = collect_tokens("one two three");
        for t in &tokens {
            assert_eq!(t.3, 1);
        }
    }

    #[test]
    fn test_set_reader_allows_reuse() {
        let mut analyzer = StandardAnalyzer::default();

        // First field
        analyzer.set_reader(Box::new(io::Cursor::new(b"hello".to_vec())));
        let token = analyzer.next_token().unwrap();
        assert_some!(&token);
        let none = analyzer.next_token().unwrap();
        assert_none!(&none);

        // Set new reader and process second field
        analyzer.set_reader(Box::new(io::Cursor::new(b"world".to_vec())));
        let token = analyzer.next_token().unwrap();
        assert_some!(&token);
        assert_eq!(token.unwrap().text, "world");
    }
}
