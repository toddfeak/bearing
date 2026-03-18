//! Integration tests for the bearing analysis public API.
//!
//! Tests the StandardAnalyzer, tokenization, and the zero-allocation
//! analyze_to callback API.

#[macro_use]
extern crate assertables;

use bearing::analysis::{Analyzer, StandardAnalyzer, Token, TokenRef};

// ---------------------------------------------------------------------------
// StandardAnalyzer tokenization
// ---------------------------------------------------------------------------

#[test]
fn standard_analyzer_basic_tokenization() {
    let analyzer = StandardAnalyzer::new();
    let tokens = analyzer.analyze("the quick brown fox");

    assert_eq!(tokens.len(), 4);
    assert_eq!(tokens[0].text, "the");
    assert_eq!(tokens[1].text, "quick");
    assert_eq!(tokens[2].text, "brown");
    assert_eq!(tokens[3].text, "fox");
}

#[test]
fn standard_analyzer_lowercases() {
    let analyzer = StandardAnalyzer::new();
    let tokens = analyzer.analyze("Hello WORLD FoO");

    assert_eq!(tokens.len(), 3);
    assert_eq!(tokens[0].text, "hello");
    assert_eq!(tokens[1].text, "world");
    assert_eq!(tokens[2].text, "foo");
}

#[test]
fn standard_analyzer_empty_input() {
    let analyzer = StandardAnalyzer::new();
    let tokens = analyzer.analyze("");
    assert_is_empty!(tokens);
}

#[test]
fn standard_analyzer_whitespace_only() {
    let analyzer = StandardAnalyzer::new();
    let tokens = analyzer.analyze("   \t\n  ");
    assert_is_empty!(tokens);
}

#[test]
fn standard_analyzer_punctuation_splitting() {
    let analyzer = StandardAnalyzer::new();
    let tokens = analyzer.analyze("hello, world! how are you?");

    let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
    assert_eq!(texts, vec!["hello", "world", "how", "are", "you"]);
}

#[test]
fn standard_analyzer_offsets() {
    let analyzer = StandardAnalyzer::new();
    let tokens = analyzer.analyze("hello world");

    assert_eq!(tokens[0].start_offset, 0);
    assert_eq!(tokens[0].end_offset, 5);
    assert_eq!(tokens[1].start_offset, 6);
    assert_eq!(tokens[1].end_offset, 11);
}

#[test]
fn standard_analyzer_position_increments() {
    let analyzer = StandardAnalyzer::new();
    let tokens = analyzer.analyze("one two three");

    // All tokens should have position_increment = 1
    for token in &tokens {
        assert_eq!(token.position_increment, 1);
    }
}

// ---------------------------------------------------------------------------
// analyze_to callback API
// ---------------------------------------------------------------------------

#[test]
fn analyze_to_produces_same_results() {
    let analyzer = StandardAnalyzer::new();
    let text = "The Quick Brown Fox Jumps";

    let owned_tokens = analyzer.analyze(text);

    let mut callback_tokens = Vec::new();
    let mut buf = String::new();
    analyzer.analyze_to(text, &mut buf, &mut |token_ref: TokenRef<'_>| {
        callback_tokens.push(Token {
            text: token_ref.text.to_string(),
            start_offset: token_ref.start_offset,
            end_offset: token_ref.end_offset,
            position_increment: token_ref.position_increment,
        });
    });

    assert_eq!(owned_tokens.len(), callback_tokens.len());
    for (owned, callback) in owned_tokens.iter().zip(callback_tokens.iter()) {
        assert_eq!(owned.text, callback.text);
        assert_eq!(owned.start_offset, callback.start_offset);
        assert_eq!(owned.end_offset, callback.end_offset);
        assert_eq!(owned.position_increment, callback.position_increment);
    }
}

#[test]
fn analyze_to_reuses_buffer() {
    let analyzer = StandardAnalyzer::new();
    let mut buf = String::new();
    let mut count = 0;

    analyzer.analyze_to("hello world", &mut buf, &mut |_| {
        count += 1;
    });
    assert_eq!(count, 2);

    // Reuse the same buffer for a second call
    count = 0;
    analyzer.analyze_to("another test here", &mut buf, &mut |_| {
        count += 1;
    });
    assert_eq!(count, 3);
}

// ---------------------------------------------------------------------------
// Unicode handling
// ---------------------------------------------------------------------------

#[test]
fn unicode_basic_latin_extended() {
    let analyzer = StandardAnalyzer::new();
    let tokens = analyzer.analyze("café résumé naïve");

    // StandardAnalyzer should handle accented characters as part of words
    assert_eq!(tokens.len(), 3);
    // Lowercasing is ASCII-only per the known limitations
    assert_eq!(tokens[0].text, "café");
    assert_eq!(tokens[1].text, "résumé");
    assert_eq!(tokens[2].text, "naïve");
}

#[test]
fn unicode_cjk_characters() {
    let analyzer = StandardAnalyzer::new();
    // CJK characters — StandardTokenizer behavior may vary,
    // but should not panic
    let tokens = analyzer.analyze("hello 世界");
    assert_not_empty!(tokens);
    assert_eq!(tokens[0].text, "hello");
}

#[test]
fn unicode_emoji() {
    let analyzer = StandardAnalyzer::new();
    // Should not panic on emoji input
    let _tokens = analyzer.analyze("hello 🌍 world");
}

#[test]
fn single_character_tokens() {
    let analyzer = StandardAnalyzer::new();
    let tokens = analyzer.analyze("a b c");

    assert_eq!(tokens.len(), 3);
    assert_eq!(tokens[0].text, "a");
    assert_eq!(tokens[1].text, "b");
    assert_eq!(tokens[2].text, "c");
}

#[test]
fn numeric_text_tokenization() {
    let analyzer = StandardAnalyzer::new();
    let tokens = analyzer.analyze("version 3.14 release 42");

    let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
    // Numbers should be tokenized as words
    assert_contains!(texts, &"version");
    assert_contains!(texts, &"release");
    assert_contains!(texts, &"42");
}
