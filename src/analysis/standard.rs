// SPDX-License-Identifier: Apache-2.0

//! Standard analysis components: [`StandardTokenizer`], [`LowerCaseFilter`],
//! and [`StandardAnalyzer`].

use std::io::{self, Read};

use crate::analysis::{Analyzer, Token, TokenFilter, TokenRef, Tokenizer};

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
}

impl StandardTokenizer {
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

    /// Zero-allocation tokenization: emits `TokenRef` borrowing from `text`
    /// instead of allocating a `String` per token.
    pub fn tokenize_callback<'a>(text: &'a str, callback: &mut dyn FnMut(TokenRef<'a>)) {
        Self::tokenize_inner(text, |start, end| {
            callback(TokenRef {
                text: &text[start..end],
                start_offset: start,
                end_offset: end,
                position_increment: 1,
            });
        });
    }
}

impl Tokenizer for StandardTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        Self::tokenize_inner(text, |start, end| {
            tokens.push(Token {
                text: text[start..end].to_string(),
                start_offset: start,
                end_offset: end,
                position_increment: 1,
            });
        });
        tokens
    }
}

/// Lowercases all token text.
pub struct LowerCaseFilter;

impl TokenFilter for LowerCaseFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .map(|mut t| {
                t.text.make_ascii_lowercase();
                t
            })
            .collect()
    }
}

/// StandardAnalyzer: StandardTokenizer + LowerCaseFilter.
///
/// Java's default StandardAnalyzer() constructor uses NO stop words
/// (CharArraySet.EMPTY_SET). The pipeline is just StandardTokenizer + LowerCaseFilter.
pub struct StandardAnalyzer;

impl StandardAnalyzer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for StandardAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer for StandardAnalyzer {
    fn analyze(&self, text: &str) -> Vec<Token> {
        let tokenizer = StandardTokenizer;
        let tokens = tokenizer.tokenize(text);
        LowerCaseFilter.filter(tokens)
    }

    fn analyze_to(&self, text: &str, buf: &mut String, callback: &mut dyn FnMut(TokenRef<'_>)) {
        // Lowercase entire input into buf (ASCII-only, byte-length preserving)
        buf.clear();
        buf.reserve(text.len());
        for b in text.bytes() {
            buf.push(if b.is_ascii_uppercase() {
                (b + 32) as char
            } else {
                b as char
            });
        }
        // Tokenize the pre-lowercased buffer, emitting borrowed slices
        StandardTokenizer::tokenize_callback(buf, callback);
    }

    fn analyze_reader(
        &self,
        reader: &mut dyn Read,
        buf: &mut String,
        callback: &mut dyn FnMut(TokenRef<'_>),
    ) -> io::Result<()> {
        const CHUNK_SIZE: usize = 8192;
        let mut read_buf = [0u8; CHUNK_SIZE];
        // Bytes that formed an incomplete UTF-8 sequence at the end of the last read
        let mut utf8_carry = [0u8; 4];
        let mut utf8_carry_len: usize = 0;
        // Cumulative byte offset of buf[0] in the original stream
        let mut base_offset: usize = 0;

        buf.clear();

        loop {
            // Read a chunk
            let bytes_read = reader.read(&mut read_buf)?;
            let eof = bytes_read == 0;

            if !eof {
                // Prepend any carried-over incomplete UTF-8 bytes
                let raw = if utf8_carry_len > 0 {
                    let mut combined = Vec::with_capacity(utf8_carry_len + bytes_read);
                    combined.extend_from_slice(&utf8_carry[..utf8_carry_len]);
                    combined.extend_from_slice(&read_buf[..bytes_read]);
                    utf8_carry_len = 0;
                    combined
                } else {
                    read_buf[..bytes_read].to_vec()
                };

                // Find the last valid UTF-8 boundary
                let valid_len = find_utf8_boundary(&raw);
                if valid_len < raw.len() {
                    let leftover = raw.len() - valid_len;
                    utf8_carry[..leftover].copy_from_slice(&raw[valid_len..]);
                    utf8_carry_len = leftover;
                }

                // Lowercase and append valid bytes to buf
                for &b in &raw[..valid_len] {
                    buf.push(if b.is_ascii_uppercase() {
                        (b + 32) as char
                    } else {
                        b as char
                    });
                }
            }

            if buf.is_empty() {
                if eof {
                    break;
                }
                continue;
            }

            if eof {
                // Emit all remaining tokens
                StandardTokenizer::tokenize_callback(buf, &mut |mut tr| {
                    tr.start_offset += base_offset;
                    tr.end_offset += base_offset;
                    callback(tr);
                });
                break;
            }

            // Tokenize buf, but hold back the last token if it ends at buf's
            // boundary (it might continue into the next chunk).
            let mut last_token: Option<(usize, usize)> = None;
            StandardTokenizer::tokenize_inner(buf, |start, end| {
                // Emit the *previous* last_token since we now know it's complete
                if let Some((ls, le)) = last_token {
                    callback(TokenRef {
                        text: &buf[ls..le],
                        start_offset: base_offset + ls,
                        end_offset: base_offset + le,
                        position_increment: 1,
                    });
                }
                last_token = Some((start, end));
            });

            if let Some((ls, le)) = last_token {
                if le == buf.len() {
                    // Last token touches the buffer boundary — carry it over
                    let carried = buf[ls..le].to_string();
                    base_offset += ls;
                    buf.clear();
                    buf.push_str(&carried);
                } else {
                    // Last token doesn't touch the end — safe to emit
                    callback(TokenRef {
                        text: &buf[ls..le],
                        start_offset: base_offset + ls,
                        end_offset: base_offset + le,
                        position_increment: 1,
                    });
                    base_offset += buf.len();
                    buf.clear();
                }
            } else {
                // No tokens found in this chunk
                base_offset += buf.len();
                buf.clear();
            }
        }

        Ok(())
    }
}

/// Finds the largest prefix of `bytes` that is valid UTF-8.
/// Returns `bytes.len()` if all bytes are valid.
fn find_utf8_boundary(bytes: &[u8]) -> usize {
    match std::str::from_utf8(bytes) {
        Ok(_) => bytes.len(),
        Err(e) => e.valid_up_to(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_tokenizer_simple() {
        let tokenizer = StandardTokenizer;
        let tokens = tokenizer.tokenize("hello world");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[0].start_offset, 0);
        assert_eq!(tokens[0].end_offset, 5);
        assert_eq!(tokens[1].text, "world");
        assert_eq!(tokens[1].start_offset, 6);
        assert_eq!(tokens[1].end_offset, 11);
    }

    #[test]
    fn test_standard_tokenizer_contraction() {
        let tokenizer = StandardTokenizer;
        let tokens = tokenizer.tokenize("don't stop");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "don't");
        assert_eq!(tokens[1].text, "stop");
    }

    #[test]
    fn test_standard_tokenizer_numbers() {
        let tokenizer = StandardTokenizer;
        let tokens = tokenizer.tokenize("test123 456");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "test123");
        assert_eq!(tokens[1].text, "456");
    }

    #[test]
    fn test_standard_tokenizer_punctuation() {
        let tokenizer = StandardTokenizer;
        let tokens = tokenizer.tokenize("hello, world! foo.");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[1].text, "world");
        assert_eq!(tokens[2].text, "foo");
    }

    #[test]
    fn test_standard_tokenizer_empty() {
        let tokenizer = StandardTokenizer;
        let tokens = tokenizer.tokenize("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_standard_tokenizer_position_increments() {
        let tokenizer = StandardTokenizer;
        let tokens = tokenizer.tokenize("a b c");
        for t in &tokens {
            assert_eq!(t.position_increment, 1);
        }
    }

    #[test]
    fn test_lowercase_filter() {
        let tokenizer = StandardTokenizer;
        let tokens = tokenizer.tokenize("Hello WORLD FooBar");
        let tokens = LowerCaseFilter.filter(tokens);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[1].text, "world");
        assert_eq!(tokens[2].text, "foobar");
    }

    #[test]
    fn test_standard_analyzer() {
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("The quick brown fox");
        // Java's default StandardAnalyzer has no stop words
        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(texts, vec!["the", "quick", "brown", "fox"]);
    }

    #[test]
    fn test_standard_analyzer_no_stop_words() {
        // Ported from StandardAnalyzer default constructor: CharArraySet.EMPTY_SET
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("the quick and brown fox");
        assert_eq!(tokens.len(), 5);
        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(texts, vec!["the", "quick", "and", "brown", "fox"]);
        // All position increments should be 1
        for t in &tokens {
            assert_eq!(t.position_increment, 1);
        }
    }

    #[test]
    fn test_tokenize_callback_matches_tokenize() {
        let test_cases = [
            "hello world",
            "don't stop",
            "test123 456",
            "hello, world! foo.",
            "",
            "a b c",
            "The Quick BROWN Fox",
            "  leading   trailing  ",
            "multiple   spaces   between",
        ];

        let tokenizer = StandardTokenizer;
        for input in &test_cases {
            let owned_tokens = tokenizer.tokenize(input);
            let mut callback_tokens = Vec::new();
            StandardTokenizer::tokenize_callback(input, &mut |tr| {
                callback_tokens.push((
                    tr.text.to_string(),
                    tr.start_offset,
                    tr.end_offset,
                    tr.position_increment,
                ));
            });

            assert_eq!(
                owned_tokens.len(),
                callback_tokens.len(),
                "token count mismatch for input: {:?}",
                input,
            );

            for (owned, cb) in owned_tokens.iter().zip(&callback_tokens) {
                assert_eq!(owned.text, cb.0, "text mismatch for input: {:?}", input);
                assert_eq!(
                    owned.start_offset, cb.1,
                    "start_offset mismatch for input: {:?}",
                    input,
                );
                assert_eq!(
                    owned.end_offset, cb.2,
                    "end_offset mismatch for input: {:?}",
                    input,
                );
                assert_eq!(
                    owned.position_increment, cb.3,
                    "position_increment mismatch for input: {:?}",
                    input,
                );
            }
        }
    }

    #[test]
    fn test_analyze_reader_matches_analyze_to() {
        let analyzer = StandardAnalyzer::new();
        let test_cases = [
            "The Quick BROWN Fox",
            "don't STOP believing",
            "Hello, World! Foo.",
            "",
            "UPPERCASE lowercase MiXeD",
            "test123 ABC456 xyz",
            "a single word",
            "  leading   trailing  ",
        ];

        for input in &test_cases {
            let mut buf1 = String::new();
            let mut tokens_to = Vec::new();
            analyzer.analyze_to(input, &mut buf1, &mut |tr| {
                tokens_to.push((
                    tr.text.to_string(),
                    tr.start_offset,
                    tr.end_offset,
                    tr.position_increment,
                ));
            });

            let mut buf2 = String::new();
            let mut tokens_reader = Vec::new();
            let mut cursor = std::io::Cursor::new(input.as_bytes());
            analyzer
                .analyze_reader(&mut cursor, &mut buf2, &mut |tr| {
                    tokens_reader.push((
                        tr.text.to_string(),
                        tr.start_offset,
                        tr.end_offset,
                        tr.position_increment,
                    ));
                })
                .unwrap();

            assert_eq!(tokens_to, tokens_reader, "mismatch for input: {:?}", input,);
        }
    }

    #[test]
    fn test_analyze_reader_word_spanning_chunk_boundary() {
        let analyzer = StandardAnalyzer::new();
        // Create text where a word spans the 8 KB chunk boundary
        let padding = "x ".repeat(4094); // 8188 bytes, next word starts near boundary
        let text = format!("{padding}foxy lady");

        let mut buf_to = String::new();
        let mut tokens_to = Vec::new();
        analyzer.analyze_to(&text, &mut buf_to, &mut |tr| {
            tokens_to.push((tr.text.to_string(), tr.start_offset, tr.end_offset));
        });

        let mut buf_reader = String::new();
        let mut tokens_reader = Vec::new();
        let mut cursor = std::io::Cursor::new(text.as_bytes());
        analyzer
            .analyze_reader(&mut cursor, &mut buf_reader, &mut |tr| {
                tokens_reader.push((tr.text.to_string(), tr.start_offset, tr.end_offset));
            })
            .unwrap();

        assert_eq!(tokens_to, tokens_reader);
    }

    /// Reader that yields exactly one byte at a time, stressing carry-over logic.
    struct OneByteReader<'a> {
        data: &'a [u8],
        pos: usize,
    }
    impl<'a> std::io::Read for OneByteReader<'a> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.pos >= self.data.len() {
                return Ok(0);
            }
            buf[0] = self.data[self.pos];
            self.pos += 1;
            Ok(1)
        }
    }

    #[test]
    fn test_analyze_reader_one_byte_at_a_time() {
        let analyzer = StandardAnalyzer::new();
        let text = "Hello World";

        let mut buf1 = String::new();
        let mut tokens_to = Vec::new();
        analyzer.analyze_to(text, &mut buf1, &mut |tr| {
            tokens_to.push((tr.text.to_string(), tr.start_offset, tr.end_offset));
        });

        let mut buf2 = String::new();
        let mut tokens_reader = Vec::new();
        let mut reader = OneByteReader {
            data: text.as_bytes(),
            pos: 0,
        };
        analyzer
            .analyze_reader(&mut reader, &mut buf2, &mut |tr| {
                tokens_reader.push((tr.text.to_string(), tr.start_offset, tr.end_offset));
            })
            .unwrap();

        assert_eq!(tokens_to, tokens_reader);
    }

    #[test]
    fn test_analyze_reader_contraction_at_chunk_boundary() {
        let analyzer = StandardAnalyzer::new();
        // Place "don't" so "don" ends the first 8192-byte chunk and "'t" is in the next.
        // We need exactly 8189 bytes of padding (8192 - 3 for "don"), then "don't stop".
        let mut padding = String::with_capacity(8189);
        while padding.len() < 8189 - 1 {
            padding.push_str("z ");
        }
        // Fill remaining bytes with spaces
        while padding.len() < 8189 {
            padding.push(' ');
        }
        let text = format!("{padding}don't stop");

        let mut buf1 = String::new();
        let mut tokens_to = Vec::new();
        analyzer.analyze_to(&text, &mut buf1, &mut |tr| {
            tokens_to.push((tr.text.to_string(), tr.start_offset, tr.end_offset));
        });

        let mut buf2 = String::new();
        let mut tokens_reader = Vec::new();
        let mut cursor = std::io::Cursor::new(text.as_bytes());
        analyzer
            .analyze_reader(&mut cursor, &mut buf2, &mut |tr| {
                tokens_reader.push((tr.text.to_string(), tr.start_offset, tr.end_offset));
            })
            .unwrap();

        assert_eq!(tokens_to, tokens_reader);
    }

    #[test]
    fn test_analyze_reader_empty() {
        let analyzer = StandardAnalyzer::new();
        let mut buf = String::new();
        let mut tokens = Vec::new();
        let mut cursor = std::io::Cursor::new(b"");
        analyzer
            .analyze_reader(&mut cursor, &mut buf, &mut |tr| {
                tokens.push(tr.text.to_string());
            })
            .unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_analyze_reader_multibyte_utf8_at_chunk_boundary() {
        let analyzer = StandardAnalyzer::new();
        // Place a multi-byte character right at the 8192 boundary
        let padding = "a ".repeat(4095); // 8190 bytes
        // "ä" is 2 bytes (0xC3 0xA4), so it starts at byte 8190 and first byte
        // is in chunk 1, second in chunk 2
        let text = format!("{padding}ä hello");

        let mut buf1 = String::new();
        let mut tokens_to = Vec::new();
        analyzer.analyze_to(&text, &mut buf1, &mut |tr| {
            tokens_to.push((tr.text.to_string(), tr.start_offset, tr.end_offset));
        });

        let mut buf2 = String::new();
        let mut tokens_reader = Vec::new();
        let mut cursor = std::io::Cursor::new(text.as_bytes());
        analyzer
            .analyze_reader(&mut cursor, &mut buf2, &mut |tr| {
                tokens_reader.push((tr.text.to_string(), tr.start_offset, tr.end_offset));
            })
            .unwrap();

        assert_eq!(tokens_to, tokens_reader);
    }

    #[test]
    fn test_apostrophe_at_end_of_input() {
        // Apostrophe at end of word at end of input (line 76-77)
        let tokenizer = StandardTokenizer;
        let tokens = tokenizer.tokenize("don't");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text, "don't");

        // Trailing apostrophe with no following char
        let tokens = tokenizer.tokenize("hello'");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text, "hello");
    }

    #[test]
    fn test_apostrophe_followed_by_non_alpha() {
        // Apostrophe followed by non-alphanumeric (line 72-73)
        let tokenizer = StandardTokenizer;
        let tokens = tokenizer.tokenize("it' s");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "it");
        assert_eq!(tokens[1].text, "s");

        // Apostrophe followed by space
        let tokens = tokenizer.tokenize("don' t");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "don");
        assert_eq!(tokens[1].text, "t");
    }

    #[test]
    fn test_analyze_reader_all_whitespace_chunk() {
        // A chunk of only whitespace produces no tokens (lines 287-291)
        let analyzer = StandardAnalyzer::new();
        let spaces = " ".repeat(8192); // exactly one chunk of spaces
        let text = format!("{spaces}hello");

        let mut buf1 = String::new();
        let mut tokens_to = Vec::new();
        analyzer.analyze_to(&text, &mut buf1, &mut |tr| {
            tokens_to.push((tr.text.to_string(), tr.start_offset, tr.end_offset));
        });

        let mut buf2 = String::new();
        let mut tokens_reader = Vec::new();
        let mut cursor = std::io::Cursor::new(text.as_bytes());
        analyzer
            .analyze_reader(&mut cursor, &mut buf2, &mut |tr| {
                tokens_reader.push((tr.text.to_string(), tr.start_offset, tr.end_offset));
            })
            .unwrap();

        assert_eq!(tokens_to, tokens_reader);
    }

    #[test]
    fn test_analyze_reader_utf8_carry_over() {
        // Force a multi-byte UTF-8 char to be split across read boundaries
        // by using a reader that delivers exactly N bytes at a time.
        // "ä" is 0xC3 0xA4 (2 bytes). Place it so byte 0xC3 is the last
        // byte of one read and 0xA4 is the first byte of the next.
        let analyzer = StandardAnalyzer::new();

        // Use a custom reader that yields exactly 3 bytes at a time.
        // Input: "ab" (2 bytes) + "ä" (2 bytes) + "cd" (2 bytes) = 6 bytes total.
        // Reads: [a, b, 0xC3] then [0xA4, c, d] — splits the ä.
        struct ChunkedReader {
            data: Vec<u8>,
            pos: usize,
            chunk_size: usize,
        }
        impl std::io::Read for ChunkedReader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.pos >= self.data.len() {
                    return Ok(0);
                }
                let end = (self.pos + self.chunk_size)
                    .min(self.data.len())
                    .min(self.pos + buf.len());
                let n = end - self.pos;
                buf[..n].copy_from_slice(&self.data[self.pos..end]);
                self.pos += n;
                Ok(n)
            }
        }

        let input = "abäcd";
        let mut buf_to = String::new();
        let mut tokens_to = Vec::new();
        analyzer.analyze_to(input, &mut buf_to, &mut |tr| {
            tokens_to.push(tr.text.to_string());
        });

        let mut buf_reader = String::new();
        let mut tokens_reader = Vec::new();
        let mut reader = ChunkedReader {
            data: input.as_bytes().to_vec(),
            pos: 0,
            chunk_size: 3, // splits the 2-byte ä across reads
        };
        analyzer
            .analyze_reader(&mut reader, &mut buf_reader, &mut |tr| {
                tokens_reader.push(tr.text.to_string());
            })
            .unwrap();

        assert_eq!(tokens_to, tokens_reader);
    }

    #[test]
    fn test_standard_analyzer_default() {
        let analyzer = StandardAnalyzer::default();
        let tokens = analyzer.analyze("hello");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text, "hello");
    }

    #[test]
    fn test_token_exceeding_max_length() {
        let tokenizer = StandardTokenizer;
        // A single token of exactly MAX_TOKEN_LENGTH (255) chars should be kept
        let long_word: String = "a".repeat(255);
        let tokens = tokenizer.tokenize(&long_word);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text.len(), 255);

        // A token exceeding MAX_TOKEN_LENGTH (256+ chars) is truncated to 255
        let too_long: String = "b".repeat(300);
        let tokens = tokenizer.tokenize(&too_long);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text.len(), 255);

        // A long token followed by a separator and another token
        let input = format!("{} short", "c".repeat(300));
        let tokens = tokenizer.tokenize(&input);
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text.len(), 255);
        assert_eq!(tokens[1].text, "short");
    }

    #[test]
    fn test_analyze_to_matches_analyze() {
        let analyzer = StandardAnalyzer::new();
        let test_cases = [
            "The Quick BROWN Fox",
            "don't STOP believing",
            "Hello, World! Foo.",
            "",
            "UPPERCASE lowercase MiXeD",
            "test123 ABC456 xyz",
        ];

        for input in &test_cases {
            let owned_tokens = analyzer.analyze(input);
            let mut buf = String::new();
            let mut callback_tokens = Vec::new();
            analyzer.analyze_to(input, &mut buf, &mut |tr| {
                callback_tokens.push((
                    tr.text.to_string(),
                    tr.start_offset,
                    tr.end_offset,
                    tr.position_increment,
                ));
            });

            assert_eq!(
                owned_tokens.len(),
                callback_tokens.len(),
                "token count mismatch for input: {:?}",
                input,
            );

            for (owned, cb) in owned_tokens.iter().zip(&callback_tokens) {
                assert_eq!(owned.text, cb.0, "text mismatch for input: {:?}", input);
                assert_eq!(
                    owned.start_offset, cb.1,
                    "start_offset mismatch for input: {:?}",
                    input,
                );
                assert_eq!(
                    owned.end_offset, cb.2,
                    "end_offset mismatch for input: {:?}",
                    input,
                );
                assert_eq!(
                    owned.position_increment, cb.3,
                    "position_increment mismatch for input: {:?}",
                    input,
                );
            }
        }
    }
}
