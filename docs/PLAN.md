# Bearing — Roadmap

## Performance Summary

**Indexing** (2000 docs, 149 MB corpus, release build):

| Metric | Bearing | Lucene 10.3.2 | Ratio |
|---|---|---|---|
| 1 thread | 1.36s | 2.72s | **2.0x faster** |
| 12 threads | 0.61s | 2.72s | **4.4x faster** |

**Querying** (2000 docs, 60M corpus, 2000 queries — all boolean query types with 2 terms or less):

| Metric | Bearing | Lucene 10.3.2 | Ratio |
|---|---|---|---|
| Avg query time | 39 µs | 126 µs | **3.2x faster** |
| Peak RSS | 10 MB | 101 MB | **10x less memory** |

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

### 1. IndexInput & DataInput (done)
`DataInput` trait with default implementations delegating to encoding functions via `DataInputReader` adapter. `IndexInput` trait with seek/slice. `ByteSliceIndexInput` (in-memory), `FSIndexInput` (file-backed with slice support), `ChecksumIndexInput` (CRC32-wrapping). `Directory::open_input` on both `FSDirectory` and `MemoryDirectory`. Encoding read functions for varint, zigzag, string, group-varint.

### 2. Codec Readers (done)
Format-specific readers for each codec version. Each reader has its own read-side in-memory data structures optimized for seeking and iteration — they do NOT share internal structures with the writers. What they share is the file format and encoding utilities.

**Done:**
- Segment info reader (lucene99) — reads `.si` files
- Field infos reader (lucene94) — reads `.fnm` files
- Segments_N reader — reads `segments_N` commit point, returns raw segment entries (name, id, codec name). Does NOT call codec readers — the caller dispatches to the right format readers based on the codec name. This creates the seam for future codec versioning.
- Compound file reader (lucene90) — `CompoundDirectory` implements `Directory` (read-only) by parsing `.cfe` entry table and slicing `.cfs` data file. Codec readers use it transparently via `dir.open_input()`.
- `codec_util` read functions — `check_header`, `check_index_header`, `check_footer` (production); `checksum_entire_file` (test-only)
- Stored fields reader (lucene90) — reads `.fdt`/`.fdx`/`.fdm` files, decompresses LZ4-with-preset-dict chunks, decodes all stored field value types (string, int, long, float, double, bytes). Uses `DirectReader` + `DirectMonotonicReader` for chunk index lookup.
- Block tree terms reader (lucene103) — reads `.tim`/`.tip`/`.tmd` files, provides per-field term metadata (term count, sum total term freq, doc count, min/max term).
- Norms reader (lucene90) — reads `.nvm`/`.nvd` files. Eager metadata, lazy data reads. Supports ALL, EMPTY, and SPARSE (via IndexedDISI) patterns with 0/1/2/4/8-byte norm values.
- Doc values metadata reader (lucene90) — reads `.dvm` metadata for all 5 doc values types (NUMERIC, BINARY, SORTED, SORTED_SET, SORTED_NUMERIC). Provides per-field document counts; value reads deferred.
- Term vectors metadata reader (lucene90) — reads `.tvm`/`.tvx`/`.tvd` files. Eager metadata, lazy chunk data. Exposes chunk count for golden summary validation.
- Points/BKD metadata reader (lucene90) — reads `.kdm`/`.kdi`/`.kdd` files. Per-field BKD stats (pointCount, docCount, numLeaves). Tree/leaf data deferred.
- Postings metadata reader (lucene103) — reads `.psm`/`.doc`/`.pos` files. Impact stats from metadata; posting list data deferred.

All codec metadata readers are complete. Golden summary validates per-field stats bidirectionally (Java write→Rust read, Rust write→Rust read) for: termCount, sumTotalTermFreq, sumDocFreq, termsDocCount, dvDocCount, normsDocCount, pointDocCount, pointCount, tvChunks.

FOR/PFOR decode functions go in `codecs::lucene103::for_util` alongside the existing encode functions — format-version-specific, not a general encoding utility.

### 3. Index Reader Hierarchy (done)
- `SegmentReader` — opens a single segment, wires up all codec readers conditionally (compound/non-compound transparent). Provides access to stored fields, norms, doc values, term vectors, points, terms, and postings readers.
- `DirectoryReader` — opens an index directory via `segments_N`, creates `SegmentReader` per segment. `LeafReaderContext` provides per-segment doc base for global doc ID mapping.
- `generate_summary` binary simplified to use `DirectoryReader`, validating the hierarchy end-to-end across all E2E tests.

### 4. Term Seeking & Doc Iteration (done)
First query path: given a field name and term, find all matching doc IDs.

- **FOR/PFOR decode** — `for_util` decode functions (reverse of encode): `decode`, `pfor_decode`, `for_delta_decode` with expand/prefix-sum. Round-trip tested at all BPV thresholds.
- **RandomAccessInput** — absolute-position read trait for trie navigation. Implemented for `ByteSliceIndexInput` and `FSIndexInput`.
- **TrieReader** — navigates the `.tip` FST-like trie index. Handles all 3 node types (leaf, single-child, multi-children) and all 3 child save strategies (BITS, ARRAY, REVERSE_ARRAY). Returns block file pointer + floor data for term block lookup.
- **SegmentTermsEnum** — parses `.tim` term blocks. Loads 5-section blocks (header, suffixes, suffix lengths, stats, metadata), scans suffixes for exact match, decodes `IntBlockTermState` with singleton RLE and delta encoding. Handles LZ4 and lowercase ASCII suffix compression. Floor block scanning for multi-block fields.
- **BlockDocIterator** — sequential doc ID iteration from `.doc` file. Handles singleton (pulsed), VInt tail (group-varint), and full 128-doc blocks (FOR-delta, bitset, consecutive). Skips impact/freq data.
- **SegmentReader::postings()** — end-to-end convenience: field lookup → trie seek → block scan → metadata decode → doc ID iteration. Works for both compound and non-compound segments.

**Deferred from this step** (see `docs/backlog/block_doc_iterator_gaps.md`):
- Frequency decoding — needed for scoring (BM25)
- `advance(target)` / skip-based seeking — needed for conjunctive (AND) queries
- Level1 skip handling — needed for terms with > 4096 docs (writer also limited)

## Phase 2 — Search

### 5. Search Infrastructure (done)
Query/Weight/Scorer/BulkScorer abstractions, BM25 similarity, collectors, MmapDirectory for zero-copy reads, lazy norms access, Terms/TermsEnum reader abstractions.

### 6. Core Query Types
#### TermQuery (done)
Single-term BM25 scoring with competitive skipping via impacts. Byte-identical results to Java Lucene.

#### BooleanQuery — All 1-2 term combinations (done)
All valid boolean query structures with up to 2 terms: pure MUST, pure SHOULD, MUST+MUST_NOT, SHOULD+MUST_NOT, and mixed MUST+SHOULD. Includes dynamic pruning for conjunction, window-based bulk scoring for disjunction, exclusion filtering, and TOP_SCORES competitive skipping for mixed queries. Cross-validated against Java Lucene across multiple corpus sizes.

**Query performance (2000 docs, 60M corpus, 2000 queries — all query types):**

| Metric | Bearing | Lucene | Ratio |
|---|---|---|---|
| Avg query time | 39 µs | 126 µs | **3.2x faster** |
| Peak RSS | 10 MB | 101 MB | **10x less memory** |

**Query performance (5000 docs, 192M corpus, 5000 queries — all query types):**

| Metric | Bearing | Lucene | Ratio |
|---|---|---|---|
| Avg query time | 47 µs | 92 µs | **1.9x faster** |
| Peak RSS | 23 MB | 103 MB | **4.5x less memory** |

#### BooleanQuery — 3+ term queries
- Multiple SHOULD in mixed queries (`+required word1 word2`)
- Multiple MUST_NOT (`+word1 -word2 -word3`)
- minShouldMatch > 0 with conjunction

#### Beyond Boolean
Phrase, range, wildcard, etc.

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
