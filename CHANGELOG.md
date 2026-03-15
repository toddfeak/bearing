# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Multi-threaded `IndexWriter` with DWPT pool and configurable flush policies
- Eight field types: `KeywordField`, `LongField`, `TextField`, `StringField`, `IntField`, `FloatField`, `DoubleField`, `StoredField`
- Lucene103 codec with all required file formats
- `StandardAnalyzer` with `LowerCaseFilter` and `WhitespaceTokenizer`
- `FSDirectory` for on-disk indexes and in-memory `Directory` for testing
- `indexfiles` CLI binary modeled after Lucene's `IndexFiles` demo
- RAM buffer and max-buffered-docs flush policies
- End-to-end validation via Java Lucene's `VerifyIndex`
