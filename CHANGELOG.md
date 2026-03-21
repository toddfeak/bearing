# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Multi-threaded `IndexWriter` with SegmentWorker pool and configurable flush policies
- Fourteen field types: `KeywordField`, `LongField`, `TextField`, `StringField`, `IntField`, `FloatField`, `DoubleField`, `StoredField`, `LatLonPoint`, `FeatureField`, `IntRange`, `LongRange`, `FloatRange`, `DoubleRange`
- Five doc-values-only field types: `NumericDocValuesField`, `BinaryDocValuesField`, `SortedDocValuesField`, `SortedSetDocValuesField`, `SortedNumericDocValuesField`
- Sparse doc values and norms — fields not present in every document
- Multi-valued fields — `SORTED_SET` and `SORTED_NUMERIC` with multiple values per document
- Term vectors with positions and offsets (`.tvd`/`.tvx`/`.tvm`)
- `FeatureField` with competitive impact encoding for BM25 boosting
- `LatLonPoint` for geo-point indexing via BKD tree
- Range fields (`IntRange`, `LongRange`, `FloatRange`, `DoubleRange`) for range queries
- Lucene103 codec with all required file formats
- `StandardAnalyzer` with `LowerCaseFilter` and `WhitespaceTokenizer`
- `FSDirectory` for on-disk indexes and in-memory `Directory` for testing
- `indexfiles` CLI binary modeled after Lucene's `IndexFiles` demo
- RAM buffer and max-buffered-docs flush policies
- End-to-end validation via Java Lucene's `VerifyIndex`
- `encoding` module: varint, zigzag, LZ4, packed integers, group-varint, sortable bytes, geo encoding
- `ByteBlockPool` arena for memory-efficient byte stream storage

### Changed

- Streaming term vector writes — per-chunk lifecycle reduces memory from segment-level to chunk-level
- Struct-of-arrays `PostingsArray` replaces per-term `PostingList` structs, reducing postings memory ~18%
- Position and byte streams use `ByteBlockPool` arena, reducing position memory ~39%
- Streaming tokenizer eliminates full-document-in-memory during analysis
