# Bearing — Roadmap

## Performance Summary

Benchmark: 2000 docs, 149 MB corpus, release build.

| Metric | Rust | Java (Lucene 10.3.2) | Ratio |
|---|---|---|---|
| 1 thread | 1.36s | 2.72s | **2.0x faster** |
| 12 threads | 0.61s | 2.72s | **4.4x faster** |

Rust single-threaded indexing is 2x faster than Java. With 12 threads, it scales to 4.4x faster (Java's IndexWriter was benchmarked single-threaded as the baseline).

---

## Current State

### Indexing (functional, not feature complete)

- Multi-segment, multi-threaded indexing with SegmentWorker pool
- Comprehensive test suite, validated by Java Lucene VerifyIndex
- Lucene103 codec with all required file formats
- Fourteen field types: `KeywordField`, `LongField`, `TextField`, `StringField`, `IntField`, `FloatField`, `DoubleField`, `StoredField`, `LatLonPoint`, `FeatureField`, `IntRange`, `LongRange`, `FloatRange`, `DoubleRange`
- All five doc values types: `NumericDocValuesField`, `BinaryDocValuesField`, `SortedDocValuesField`, `SortedSetDocValuesField`, `SortedNumericDocValuesField`
- Sparse doc values and norms (fields not present in every document)
- Multi-valued fields (`SORTED_SET`, `SORTED_NUMERIC`)
- Term vectors with positions and offsets (`.tvd`/`.tvx`/`.tvm`)
- Streaming term vector writes (per-chunk lifecycle, not segment-level buffering)
- Memory-optimized postings: struct-of-arrays `PostingsArray`, ByteBlockPool-based position/byte streams

### Next Phase: Read Path & Querying

The write path is functional for the target field types but not feature complete — some indexing features are deferred (low priority or high complexity) and others require the read path (merging, deletes, updates). The next major phase is building the read/query path, which unblocks both querying and those remaining write-side features.

---

## Phase 1 — Read Path Foundation

### 1. IndexInput & DataInput
Random-access file reading with seek, position tracking, and primitive decoding (readVInt, readLong, etc.). Mirror of the existing DataOutput/IndexOutput write traits. Everything else depends on this.

**Testable:** Write bytes with existing DataOutput, read them back with DataInput. Round-trip tests for every primitive type.

### 2. Codec Readers
Format-specific readers for each codec version. Each reader has its own read-side in-memory data structures optimized for seeking and iteration — they do NOT share internal structures with the writers. What they share is the file format and encoding utilities (VInt, PFor, LZ4, etc.).

Readers to build (order flexible, each is independently testable):
- Segment info reader (lucene99)
- Field infos reader (lucene94)
- Stored fields reader (lucene90)
- Norms reader (lucene90)
- Doc values reader (lucene90)
- Points/BKD reader (lucene90)
- Postings reader + block tree terms reader (lucene103)
- Term vectors reader (lucene90)

FOR/PFOR decode functions go in `codecs::lucene103::for_util` alongside the existing encode functions — format-version-specific, not a general encoding utility.

**Testable:** Each reader independently — write a segment with existing writers, read it back with the corresponding reader, verify data matches.

### 3. Index Reader Hierarchy
The public-facing reader API:
- `LeafReader` / `SegmentReader` — opens a single segment, wires up all codec readers
- `DirectoryReader` — opens an index directory, enumerates segments, provides `LeafReader` per segment
- Public iterator traits: `Terms`, `TermsEnum`, `PostingsEnum`, doc values accessors

**Testable:** Open an index written by the existing IndexWriter, enumerate segments, iterate terms, look up stored fields.

## Phase 2 — Search

### 4. Search Infrastructure
Query execution:
- `IndexSearcher` — entry point, holds a `DirectoryReader`
- `Query` / `Weight` / `Scorer` / `BulkScorer` abstractions
- `Collector` / `TopDocs` — result gathering
- Basic similarity/scoring (BM25)

**Testable:** Once readers exist, even a basic TermQuery can be tested end-to-end.

### 5. Core Query Types
Start with the essentials:
- `TermQuery` (single term lookup)
- `BooleanQuery` (AND/OR/NOT composition)
- Then expand: phrase, range, wildcard, etc.

**Testable:** Index docs, search, verify correct doc IDs returned.

## Phase 3 — Index Lifecycle (unblocked by read path)

These features were deferred from the write path because they require reading existing segments.

- **Segment merging** — `MergePolicy`, `MergeScheduler`, compact segments post-flush
- **Delete documents** — live docs bitset, delete-by-term, delete-by-query
- **Document updates** — `updateDocument`, `deleteDocuments`

## Phase 4 — Query Types

Expand the query vocabulary.

- **PhraseQuery** — positional matching
- **WildcardQuery / PrefixQuery** — pattern-based term matching via automaton
- **FuzzyQuery** — edit-distance matching
- **Numeric range queries** — point-based range filtering

## Phase 5 — Analyzers & Text Processing

Richer text analysis pipeline.

- **Stop words** — port Lucene's English stop word list into `StandardAnalyzer`
- **More analyzers** — `SimpleAnalyzer`, `KeywordAnalyzer`, language-specific analyzers
- **Token filters** — stemming, synonyms, n-grams
- **Custom analysis chains** — user-defined tokenizer + filter pipelines

## Phase 6 — Advanced Features

Higher-level search functionality.

- **Faceting** — taxonomy-based and doc-values-based facets
- **Suggesters** — auto-complete and "did you mean" support
- **Spatial** — geo-point and geo-shape indexing/search
- **Highlighting** — hit highlighting in search results

## Phase 7 — Quality & Hardening

- **Custom error type** — `BearingError` enum to distinguish I/O, format, and logic errors
- **Fuzzing / property tests** — randomized testing a la Lucene's `RandomIndexWriter`
- **Performance optimization** — profiling-driven improvements, faster hashing, allocation reduction

---

## Deferred Indexing Work

These write-path features are not prioritized but remain in the backlog:

- **Payloads** — `.pay` file support (niche feature)
- **Posting offsets** — character offsets in postings for highlighter support
- **Index-time sorting** — pre-sorted segments for early query termination
- **Flush control improvements** — accurate per-thread memory measurement, smarter flush policy (see `docs/backlog/`)
- **Compact in-memory encoding** — encode stored fields, doc values, norms compactly at insertion time (see `docs/backlog/`)
- **KNN vector fields** — HNSW graph indexing
- **Shape fields** — polygon/line tessellation

## Non-Goals

- **Not a drop-in Java API replacement** — Bearing uses Rust idioms (traits, `Result`, ownership) rather than mirroring Lucene's Java API surface.
- **No distributed search** — Bearing is a single-node library. Distributed coordination is out of scope.
- **Single crate** — no workspace split planned unless complexity demands it.
- **Older index formats** — current target is Lucene103 for both reading and writing. Support for older Lucene index formats is a deferred future possibility, not in scope.

## Version Compatibility

| Bearing | Lucene | Codec | Rust Edition |
|---|---|---|---|
| 0.1.x | 10.3.2 | Lucene103 | 2024 |
