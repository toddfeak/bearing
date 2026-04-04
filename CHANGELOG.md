# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0-alpha.3]

### Changed

- Reworked the indexing pipeline with improved lifecycle and ownership — replaced the old /index pacakge with a unified write path using idiomatic Rust ownership, builder patterns, and clear module layering
- Restructured `index` module: public API types at the top level, 19 internal pipeline modules moved into `index/pipeline/` as `pub(crate)`
- Extracted I/O traits from `store.rs` into `data_output.rs`, `data_input.rs`, `index_output.rs`, `index_input.rs`
- Reduced public API surface: internal modules in `util` and `store` are now `pub(crate)`
- Added `prelude` module with convenience re-exports for common types
- Added rustdoc examples for `IndexWriter`, `DocumentBuilder`, and `SharedDirectory`
- Updated `lib.rs` quick start and `indexfiles` CLI to use prelude imports


## [0.1.0-alpha.2]

### Added

- **Read path**: `DirectoryReader`, `SegmentReader`, `MmapDirectory` for reading Lucene indexes
- **TermQuery**: Single-term BM25 scoring with competitive skipping via impacts
- **BooleanQuery**: All 1-2 term combinations — MUST, SHOULD, MUST_NOT, and mixed
  - `BooleanScorer` for pure disjunction (window-based bulk scoring)
  - `BlockMaxConjunctionBulkScorer` for conjunction with dynamic pruning
  - `ReqExclBulkScorer` for MUST_NOT exclusion
  - `ReqOptSumScorer` for mixed MUST+SHOULD with TOP_SCORES competitive skipping
- `IndexSearcher` with `TopScoreDocCollector` for top-k result collection
- `BM25Similarity` with `SimScorer` / `BulkSimScorer`
- Codec readers: postings, stored fields, doc values, norms, term vectors, points, compound files
- `Terms` / `TermsEnum` traits for term dictionary navigation
- `DocIdSetIterator` trait with `fill_bit_set` for bulk bitset loading

## [0.1.0-alpha.1]

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
