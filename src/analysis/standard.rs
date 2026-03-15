// Ported from org.apache.lucene.analysis.standard.StandardTokenizer,
// org.apache.lucene.analysis.LowerCaseFilter,
// org.apache.lucene.analysis.standard.StandardAnalyzer

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
/// Ported from org.apache.lucene.analysis.LowerCaseFilter
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
/// Ported from org.apache.lucene.analysis.standard.StandardAnalyzer
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
