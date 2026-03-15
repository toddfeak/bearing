# Rust Lucene Indexer — Roadmap

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
- 288 tests passing, Java Lucene VerifyIndex validates output
- Three field types: KeywordField, LongField, TextField
- Lucene103 codec with all required file formats
- No `todo!()` or `unimplemented!()` remaining

---

## Tier 1 — Expand Field Coverage

Broaden the set of indexable field types and handle edge cases in existing ones.

1. **More field types** — `StringField`, `IntField`, `FloatField`, `DoubleField`, `StoredField`
2. **Sparse doc values / norms** — handle documents with missing fields (currently returns `Err`)
3. **Multi-valued fields** — `SORTED_SET` and `SORTED_NUMERIC` with multiple values per doc
4. **Stop words** — port Lucene's English stop word list into `StandardAnalyzer`

## Tier 2 — Index Lifecycle

Add the write-side operations needed for a production index.

5. **Segment merging** — `MergePolicy`, `MergeScheduler`, compact segments post-flush
6. **Delete documents** — live docs bitset, delete-by-term, delete-by-query
7. **Term vectors** — `.tvd`/`.tvx` file writing
8. **Payloads** — `.pay` file support

## Tier 3 — Reading & Search

Build the read path so Rust can consume its own indexes.

9. **IndexReader** — `DirectoryReader`, `SegmentReader`, codec readers for all formats
10. **Search** — `IndexSearcher`, `TermQuery`, `BooleanQuery`, scoring

## Tier 4 — Quality & Hardening

11. **Custom error type** — `LuceneError` enum to distinguish I/O, format, and logic errors
12. **Fuzzing / property tests** — randomized testing a la Lucene's `RandomIndexWriter`
13. **Faster hashing** — `ahash` showed ~10% single-threaded gain; revisit when dependency policy allows
