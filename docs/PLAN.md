# Bearing — Roadmap

## Performance Summary

Benchmark: 2000 docs, 149 MB corpus, release build.

| Metric | Rust | Java (Lucene 10.3.2) | Ratio |
|---|---|---|---|
| 1 thread | 1.36s | 2.72s | **2.0x faster** |
| 12 threads | 0.61s | 2.72s | **4.4x faster** |

Rust single-threaded indexing is 2x faster than Java. With 12 threads, it scales to 4.4x faster (Java's IndexWriter was benchmarked single-threaded as the baseline).

---

## Current State (MVP Complete)

- Multi-segment, multi-threaded indexing with DWPT pool
- Comprehensive test suite, validated by Java Lucene VerifyIndex
- Eight field types: `KeywordField`, `LongField`, `TextField`, `StringField`, `IntField`, `FloatField`, `DoubleField`, `StoredField`
- Lucene103 codec with all required file formats

---

## Tier 1 — Expand Field Coverage

Handle edge cases and gaps in existing field type support.

1. **Sparse doc values / norms** — handle documents with missing fields (currently returns `Err`)
2. **Multi-valued fields** — `SORTED_SET` and `SORTED_NUMERIC` with multiple values per doc
3. **Stop words** — port Lucene's English stop word list into `StandardAnalyzer`

## Tier 2 — Index Lifecycle

Add the write-side operations needed for a production index.

4. **Segment merging** — `MergePolicy`, `MergeScheduler`, compact segments post-flush
5. **Delete documents** — live docs bitset, delete-by-term, delete-by-query
6. **Term vectors** — `.tvd`/`.tvx` file writing
7. **Payloads** — `.pay` file support

## Tier 3 — Reading & Search (Core)

Build the read path and fundamental query types so Rust can consume its own indexes.

8. **IndexReader** — `DirectoryReader`, `SegmentReader`, codec readers for all formats
9. **IndexSearcher** — query execution, collection, scoring infrastructure
10. **TermQuery** — single-term lookup with BM25 scoring
11. **BooleanQuery** — `MUST`, `SHOULD`, `MUST_NOT` clause composition

## Tier 4 — Query Types

Expand the query vocabulary.

12. **PhraseQuery** — positional matching
13. **WildcardQuery / PrefixQuery** — pattern-based term matching via automaton
14. **FuzzyQuery** — edit-distance matching
15. **Numeric range queries** — point-based range filtering

## Tier 5 — Analyzers & Text Processing

Richer text analysis pipeline.

16. **More analyzers** — `SimpleAnalyzer`, `KeywordAnalyzer`, language-specific analyzers
17. **Token filters** — stemming, synonyms, n-grams
18. **Custom analysis chains** — user-defined tokenizer + filter pipelines

## Tier 6 — Advanced Features

Higher-level search functionality.

19. **Faceting** — taxonomy-based and doc-values-based facets
20. **Suggesters** — auto-complete and "did you mean" support
21. **Spatial** — geo-point and geo-shape indexing/search
22. **Highlighting** — hit highlighting in search results

## Tier 7 — Quality & Hardening

23. **Custom error type** — `BearingError` enum to distinguish I/O, format, and logic errors
24. **Fuzzing / property tests** — randomized testing a la Lucene's `RandomIndexWriter`
25. **Performance optimization** — profiling-driven improvements, faster hashing, allocation reduction

---

## Non-Goals

- **Not a drop-in Java API replacement** — Bearing uses Rust idioms (traits, `Result`, ownership) rather than mirroring Lucene's Java API surface.
- **No distributed search** — Bearing is a single-node library. Distributed coordination is out of scope.
- **Single crate** — no workspace split planned unless complexity demands it.
- **Older index formats** — current target is Lucene103 for both reading and writing. Support for older Lucene index formats is a deferred future possibility, not in scope.

## Version Compatibility

| Bearing | Lucene | Codec | Rust Edition |
|---|---|---|---|
| 0.1.x | 10.3.2 | Lucene103 | 2024 |
