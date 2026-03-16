# Scaling: Memory and Segment Count

See also: [Block Pools, Streaming Flush, and Segment Merging](BLOCK_POOLS_AND_SEGMENT_MERGING.md)

## Problem

With 12 threads indexing 2000 Gutenberg texts (796 MB input), Bearing uses far more memory than Java Lucene and produces orders of magnitude more segments:

| Metric | Java (12T) | Rust (12T) |
|---|---|---|
| Peak RSS | ~105 MB | ~2.6 GB (initial) |
| Index size | ~250 MB | ~1.5 GB |
| File count | ~90 | ~11,000 |

The file count disparity is a compounding problem: more segments means more compound files held in memory before commit, more overhead per segment, and a larger index on disk (duplicate dictionary entries across segments, per-segment codec overhead). Java keeps file count low through two mechanisms: a global flush policy that flushes fewer, larger segments, and `TieredMergePolicy` which consolidates small segments after flush. Bearing initially had neither.

Java Lucene achieves low memory usage through arena-style block pool allocators, accurate RAM tracking with a flush-largest-writer strategy, writing segment files to disk during flush, segment merging, and a streaming tokenizer that never materializes full document text. Bearing initially had none of these.

This document tracks every optimization attempted, what it achieved, and why the problem persisted.

---

## Step 1: Block Pool Data Structures

**What**: Port Java's `util/ByteBlockPool.java`, `util/IntBlockPool.java`, and `util/Counter.java`. These are arena-style allocators that allocate from reusable 32 KB blocks with O(1) memory tracking via a shared `Counter`. `ByteSlicePool` manages linked-list byte slices for per-term posting streams. `ByteSliceReader` reads them back sequentially.

**Why**: The existing `HashMap<String, PostingList>` used ~150+ bytes per term due to per-entry heap allocations (String keys, Vec bodies, HashMap bucket overhead). Java's block pools pack terms into contiguous memory at ~20-32 bytes per term. This was the foundation for replacing the HashMap.

**Why it didn't fix the problem**: Infrastructure only. The old HashMap was still in use — these data structures weren't wired in yet.

---

## Step 2: BytesRefHash

**What**: Port Java's `util/BytesRefHash.java`. Maps byte sequences to sequential integer IDs using an open-addressing hash table (`int[]` with linear probing). Term bytes are stored length-prefixed in a `ByteBlockPool` — no per-term heap allocation. ~4 bytes per hash slot vs ~72 bytes per `HashMap` bucket.

**Why**: `BytesRefHash` replaces `HashMap<String, ...>` for term lookup. It eliminates per-term `String` allocations by storing term bytes directly in the block pool. The sequential integer IDs enable parallel array indexing for posting state.

**Why it didn't fix the problem**: Still infrastructure — not yet integrated into the indexing chain.

---

## Step 3: FreqProxPostingsArray and TermsHashPerField

**What**: Port Java's `FreqProxTermsWriterPerField.java` and `ParallelPostingsArray.java`. `FreqProxPostingsArray` stores per-term posting state in parallel `Vec<i32>` arrays (last doc ID, term freq, last position, last offset) indexed by term ID. `TermsHashPerField` wires together `BytesRefHash` + `FreqProxPostingsArray` + `ByteSlicePool` to provide `add_term()` and `flush_term_postings()`.

**Why**: This is the complete replacement for the per-term `PostingList` struct. Instead of a HashMap entry with its own Vec for each term, all posting data lives in shared block pools with compact parallel arrays for per-term metadata.

**Why it didn't fix the problem**: Still not wired into `IndexingChain` — the old code path was still active.

---

## Step 4: Replace HashMap with TermsHashPerField

**What**: Remove the old `PostingList` struct and all HashMap-based posting accumulation. `IndexingChain` creates a `TermsHashPerField` per indexed field, backed by the block pool infrastructure. Memory tracking becomes accurate via `Counter`.

**Why**: This was the actual swap — the culmination of steps 1-3. Per-term overhead dropped from ~150+ bytes to ~20-32 bytes. Accurate memory tracking via `Counter` meant the flush policy could finally make informed decisions about when to flush.

**Why it didn't fix the problem entirely**: Memory per DWPT dropped significantly, but `ram_bytes_used()` was now accurate, which exposed that the flush policy wasn't using it effectively. The flush policy checked each DWPT independently against the RAM buffer limit, but with 12 threads each buffering up to the limit, total memory was 12x the per-DWPT limit. Java uses global RAM tracking to flush the largest writer when *total* active bytes exceed the limit.

---

## Step 5: Streaming Flush

**What**: Replace `decode_postings()` — which materialized ALL terms' postings into `Vec<(Vec<u8>, Vec<DecodedPosting>)>` before passing to the codec writer — with a streaming API: `prepare_flush()`, `flush_term_bytes(ord)`, `flush_term_postings(ord)`. The codec writer processes one term at a time.

**Why**: During flush, the old code decoded every term's posting list into owned Vecs, doubling memory usage momentarily. With large segments this spike was substantial. Streaming eliminates the spike by processing terms one at a time directly from the block pools.

**Why it didn't fix the problem entirely**: Reduced peak memory during flush, but the dominant cost was still the indexing buffers themselves across 12 concurrent threads, not the flush spike. The flush policy still wasn't coordinating globally.

---

## Step 6: Global RAM Tracking with Flush-Largest-Writer

**What**: Port Java's `DocumentsWriterFlushControl` global RAM tracking. `FlushControl` tracks total `active_bytes` across all DWPTs. When total active bytes exceed the RAM buffer limit, it marks the largest non-pending DWPT for flush (not just the one that triggered the check). Stall control blocks new documents when flush bytes exceed 2x the RAM limit.

**Why**: With 12 threads, each DWPT could independently grow to the per-DWPT share of the buffer before being flushed. Without global tracking, total memory was unbounded (12 × buffer limit). Java's approach treats the RAM buffer as a global pool and flushes the largest writer to keep total memory bounded.

**Why it didn't fix the problem entirely**: Global RAM tracking correctly bounded *indexing buffers*, but flushed segment output (`sub_files: Vec<SegmentFile>`) accumulated in `pending_segments` until `commit()`. With ~405 segments flushed during the Gutenberg benchmark, ~317 MB of codec output (compound file data, segment info) was held entirely in memory until commit wrote everything to disk. Java writes segment files to its Directory immediately during flush.

**Benchmark (2000 synthetic docs, 149 MB input, 12 threads)**:

| Metric | Java (12T) | Rust (12T) |
|---|---|---|
| Peak RSS | 112.6 MB | 650 MB |

---

## Step 7: Write Compound Files to Directory During Flush

**What**: Give `IndexWriter` a `Directory` and write `.cfs`, `.cfe`, and `.si` files to the directory immediately during flush instead of buffering them in `pending_segments`. `FlushedSegment` becomes metadata-only (just `SegmentCommitInfo` + file names). Add `IndexWriter::open(dir, config)` constructor. The `indexfiles` binary creates an `FSDirectory` upfront and passes it to the writer.

**Why**: After step 6, the indexing buffers were properly bounded, but ~317 MB of flushed codec output accumulated in memory until commit. Java's `IndexWriter` holds a `Directory` and writes compound files to disk immediately during flush — only the `segments_N` commit point is deferred.

**Why it didn't fix the problem entirely**: RSS dropped from ~650 MB to ~322 MB — roughly halved. The codec output accumulation was eliminated. But 322 MB is still 3x Java's 105 MB. Heaptrack profiling revealed the remaining cost:

| Peak | Source |
|---|---|
| 74.76 MB | `StandardAnalyzer::analyze_to` — growing a `Vec` buffer to hold each document's full text during analysis |
| 36.83 MB | `text_field()` / `make_document` — cloned document text living across 12 threads |
| 28.41 MB | `ByteBlockPool::next_buffer` — indexing chain posting data (expected) |
| 6.42 MB | `TermsHashPerField::add_term` — term hash entries (expected) |
| 5.60 MB | `IntBlockPool::next_buffer` — posting int data (expected) |

The indexing chain data structures (~40 MB) are comparable to Java. The dominant remaining cost (~111 MB) is document text: reading files into `String`, cloning into `Document`, and the analyzer buffer growing to fit the largest document per thread.

**Benchmark (2000 Gutenberg texts, 796 MB input, 12 threads)**:

| Metric | Java (12T) | Rust (12T) |
|---|---|---|
| Indexing time | 9,917 ms | 7,419 ms |
| Peak RSS | 104.7 MB | 322.5 MB |
| Index size | 251.1 MB | 317.4 MB |
| File count | 89 | 1,243 |
| Speedup | — | 1.3x |

**Benchmark (2000 synthetic docs, 149 MB input, 12 threads)**:

| Metric | Java (12T) | Rust (12T) |
|---|---|---|
| Indexing time | 656 ms | 1,994 ms |
| Peak RSS | 33.9 MB | 112.6 MB |
| Speedup | 3.0x | — |

---

## Step 8: Streaming Tokenizer (Not Yet Implemented)

**What**: Replace the current approach where `indexfiles` calls `fs::read_to_string(path)` to load the entire file into a `String`, then passes it to `text_field()` which clones it, then `StandardAnalyzer::analyze_to` processes the full string — with a streaming tokenizer that reads from a `BufReader<File>` and tokenizes character-by-character without materializing the full document text.

**Why**: Heaptrack shows 74.76 MB peak from the analyzer buffer and 36.83 MB from document text clones — together 111 MB, over half the 210 MB peak heap. With 12 threads processing Gutenberg files (up to 5.6 MB for Shakespeare's complete works), each thread holds a buffer sized to the largest file it has seen. Java avoids this entirely: its `IndexFiles` demo uses a `FileReader`/`BufferedReader` that streams through the tokenizer. The tokenizer reads character-by-character from the reader — the full file content is never in memory.

**What this requires**: A new `Analyzer`/`Tokenizer` API that accepts a `Read` or `BufRead` instead of `&str`. The `StandardTokenizer` would need to maintain its state machine while reading from a buffered stream. `IndexingChain` would need to accept a reader for text fields instead of a pre-materialized string. This is an analyzer architecture change, not an IndexWriter change.

**Expected impact**: Should eliminate the ~111 MB of document text overhead, bringing Rust's peak heap close to Java's ~105 MB for the Gutenberg benchmark. The remaining ~40 MB of indexing chain data (block pools, term hashes) is structurally comparable to Java and not easily reducible further.

---

## Step 9: Segment Merging (Blocked on Read Path)

**What**: Implement `TieredMergePolicy` and `SegmentMerger` to consolidate small segments into larger ones after flush, matching Java's behavior. After each flush, the merge policy evaluates whether any set of segments should be merged based on size tiers. The merger reads source segments, combines them, and writes a single merged output segment.

**Why**: Even with all the memory optimizations above, Bearing produces far more segments than Java. The Gutenberg benchmark shows 1,243 files (Rust) vs 89 files (Java) — a 14x gap. This directly inflates index size (317 MB vs 251 MB) because each segment carries its own dictionary, codec headers/footers, and metadata. More segments also means slower queries when the read path is implemented, since every search must visit every segment. Java's `TieredMergePolicy` runs after each flush and merges segments with similar sizes, keeping total segment count logarithmic in the number of flushes.

**What this requires**: Merging requires reading segments back, which means implementing the full codec read path — the inverse of every writer we already have:

- **Compound file reader** — open `.cfs`/`.cfe` and provide access to sub-files
- **Stored fields reader** — read `.fdt`, `.fdx`, `.fdm`
- **Norms reader** — read `.nvd`, `.nvm`
- **Doc values reader** — read `.dvd`, `.dvm`
- **Points reader** — read `.kdd`, `.kdi`, `.kdm`
- **Postings reader** — read `.tim`, `.tip`, `.doc`, `.pos`, `.tmd`, `.psm`

Plus merge-specific infrastructure:

- **`MergeState`** — reader instances, document ID mappings, field info for a merge
- **`DocMap`** — old doc ID → new doc ID in the merged segment
- **`SegmentMerger`** — orchestrates reading N source segments and writing 1 merged output
- **`TieredMergePolicy`** — decides which segments to merge based on size tiers

These readers are also required for the search/query path, so this work is blocked until read path stories are implemented. Once readers exist, merging can be wired in as a post-flush step in `IndexWriter`.

**Expected impact**: Should reduce file count to parity with Java (~90 files for the Gutenberg benchmark) and shrink index size by eliminating per-segment overhead duplication. Memory impact is indirect — fewer segments means less metadata in `pending_segments` — but the primary benefit is index size and future query performance.
