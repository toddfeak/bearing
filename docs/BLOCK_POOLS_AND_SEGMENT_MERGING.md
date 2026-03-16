# Block Pools, Streaming Flush, and Segment Merging

See also: [Scaling: Memory and Segment Count](SCALING_MEMORY_AND_SEGMENTS.md)

## Problem

With 12 threads indexing 76K Gutenberg texts (3.7 GB input), Bearing uses far more memory than Java Lucene and produces orders of magnitude more segments:

| Metric | Java (12T) | Rust (12T) |
|---|---|---|
| Peak RSS | 128 MB | 2.6 GB |
| File count | 119 | ~11,000 |
| Index size | 1.12 GB | 1.53 GB |

Two root causes:

1. **Memory-inefficient posting data structures**: `HashMap<String, PostingList>` used ~150+ bytes per term vs Java's ~20-32 bytes with block pool allocators. This caused both excessive memory usage and inaccurate RAM tracking (the flush policy couldn't make good decisions because `ram_bytes_used()` dramatically underreported actual heap usage).

2. **No segment merging**: Java consolidates small segments into larger ones via `TieredMergePolicy`. Bearing has no merge capability, so every flush produces a permanent segment. Flush policy tuning alone cannot fix this — it only controls when segments are created, not how many survive.

## What's Done

Five commits (unpushed, on `main` ahead of `origin/main`) replace the memory-inefficient posting data structures with Java's block pool architecture and add streaming flush:

### Commit 1: Block pool data structures
`9e71fb4` — Add `Counter`, `ByteBlockPool`, `IntBlockPool`, `ByteSlicePool`, `ByteSliceReader`, and `murmurhash3`.

Ports Java's `util/ByteBlockPool.java`, `util/IntBlockPool.java`, and `util/Counter.java`. These are arena-style allocators that allocate from reusable `byte[32768]` blocks with O(1) memory tracking via a shared `Counter`. `ByteSlicePool` manages linked-list byte slices for per-term posting streams. `ByteSliceReader` reads them back sequentially.

### Commit 2: BytesRefHash
`5091500` — Add `BytesRefHash`: open-addressing term hash table with pool-backed storage.

Ports Java's `util/BytesRefHash.java`. Maps byte sequences to sequential integer IDs using an open-addressing hash table (`int[]` with linear probing). Term bytes are stored length-prefixed in a `ByteBlockPool` — no per-term heap allocation. ~4 bytes per hash slot vs ~72 bytes per `HashMap` bucket.

### Commit 3: FreqProxPostingsArray and TermsHashPerField
`16b2433` — Add `FreqProxPostingsArray` and `TermsHashPerField` with pool-backed posting streams.

Ports Java's `index/FreqProxTermsWriterPerField.java` and `index/ParallelPostingsArray.java`. `FreqProxPostingsArray` stores per-term posting state in parallel `Vec<i32>` arrays (last doc ID, last doc code, term freq, last position, last offset) indexed by term ID. `TermsHashPerField` wires together `BytesRefHash` + `FreqProxPostingsArray` + `ByteSlicePool` to provide `add_term()` for indexing and `flush_term_postings()` for codec writes.

### Commit 4: Replace HashMap with TermsHashPerField
`b651fe8` — Replace `HashMap<String, PostingList>` with `TermsHashPerField` in `IndexingChain`.

Removes the old `PostingList` struct and all HashMap-based posting accumulation. `IndexingChain` now creates a `TermsHashPerField` per indexed field, backed by the block pool infrastructure. Memory tracking is accurate via `Counter`. Per-term overhead drops from ~150+ bytes to ~20-32 bytes.

### Commit 5: Streaming flush
`c245caa` — Stream postings one term at a time during flush instead of materializing all at once.

Replaces `decode_postings()` (which materialized ALL terms' postings into `Vec<(Vec<u8>, Vec<DecodedPosting>)>` before passing to the codec writer) with a streaming API: `prepare_flush()`, `flush_term_bytes(ord)`, `flush_term_postings(ord)`. The codec writer now processes one term at a time, reducing peak memory during flush.

### Current benchmark results (500 Gutenberg texts, 201 MB input, 12 threads)

| Metric | Java | Rust |
|---|---|---|
| Indexing time | 4491ms | 1689ms |
| Peak RSS | 106.8 MB | 255.8 MB |
| Index size | 67.0 MB | 71.4 MB |
| File count | 110 | 70 |
| Speedup | — | 2.6x |

RSS is still ~2.4x Java's. Segment count is reasonable at this scale but will diverge at larger corpora because Bearing has no merging.

## What's Next: Segment Merging

The remaining gap is segment merging. Java's `TieredMergePolicy` runs after each flush and consolidates small segments, keeping the total segment count bounded regardless of corpus size. This requires two major pieces we don't have:

### 1. Codec readers

Merging requires reading segments back. We have codec writers for all formats but no readers. Each reader is the inverse of the corresponding writer:

- **Postings reader** — Read `.tim`, `.tip`, `.doc`, `.pos` files (Lucene103 format)
- **Stored fields reader** — Read `.fdt`, `.fdx`, `.fdm` files (Lucene90 format)
- **Doc values reader** — Read `.dvd`, `.dvm` files (Lucene90 format)
- **Norms reader** — Read `.nvd`, `.nvm` files (Lucene90 format)
- **Points reader** — Read `.kdd`, `.kdi`, `.kdm` files (Lucene90 format)
- **Compound file reader** — Read `.cfs`, `.cfe` files (Lucene90 format)

These readers also enable search (which we'll eventually need), so this work has value beyond merging.

### 2. Merge infrastructure

- **`MergeState`** — Holds all reader instances, document ID mappings, and field info for a merge operation
- **`DocMap`** — Maps old document IDs to new document IDs in the merged segment
- **`SegmentMerger`** — Orchestrates the merge: opens readers on source segments, creates writers for the output segment, iterates all data through the mapping
- **`TieredMergePolicy`** — Decides which segments to merge based on size tiers (Java's version is ~1000 lines)
- **`IndexWriter` integration** — Call `maybe_merge()` after each flush, execute merges, update commit point

### Plan

The plan has explicit review/commit checkpoints marked with **[CHECKPOINT]**. Do not proceed past a checkpoint without review.

#### Phase 1: Compound file reader

The compound file (`.cfs`/`.cfe`) is the container format — all per-segment codec files are packed into it. Every other reader needs to read from inside the compound file, so this comes first.

**Java reference**: `lucene/core/src/java/org/apache/lucene/codecs/lucene90/Lucene90CompoundFormat.java`

- Implement `CompoundFileReader` that opens `.cfs`/`.cfe` and provides random access to individual sub-files by name
- Port applicable tests from Java

**[CHECKPOINT]**: Review and commit.

#### Phase 2: Stored fields reader

Stored fields are the simplest codec format to read — sequential documents, each with a list of field name/value pairs. Good starting point to validate the reader pattern.

**Java reference**: `lucene/core/src/java/org/apache/lucene/codecs/lucene90/Lucene90StoredFieldsReader.java` (reads `.fdt`, `.fdx`, `.fdm`)

- Implement `StoredFieldsReader` that reads stored field data for a given doc ID
- Write round-trip tests: write stored fields with existing writer, read them back, verify
- Test reading from inside a compound file

**[CHECKPOINT]**: Review and commit.

#### Phase 3: Norms and doc values readers

These are structurally similar (both are per-document numeric values). Port together.

**Java reference**:
- `lucene/core/src/java/org/apache/lucene/codecs/lucene90/Lucene90NormsProducer.java`
- `lucene/core/src/java/org/apache/lucene/codecs/lucene90/Lucene90DocValuesProducer.java`

- Implement `NormsReader` (`.nvd`, `.nvm`)
- Implement `DocValuesReader` (`.dvd`, `.dvm`)
- Round-trip tests for each

**[CHECKPOINT]**: Review and commit.

#### Phase 4: Points reader

BKD tree reader for numeric range data.

**Java reference**: `lucene/core/src/java/org/apache/lucene/codecs/lucene90/Lucene90PointsReader.java`

- Implement `PointsReader` (`.kdd`, `.kdi`, `.kdm`)
- Round-trip tests

**[CHECKPOINT]**: Review and commit.

#### Phase 5: Postings reader

The most complex reader — inverted index terms, doc IDs, frequencies, positions.

**Java reference**:
- `lucene/core/src/java/org/apache/lucene/codecs/lucene103/Lucene103PostingsReader.java`
- `lucene/core/src/java/org/apache/lucene/codecs/blocktree/SegmentTermsEnum.java`

- Implement `PostingsReader` (`.tim`, `.tip`, `.doc`, `.pos`, `.tmd`, `.psm`)
- Round-trip tests: write postings with existing writer, read back, verify term/doc/freq/position data

**[CHECKPOINT]**: Review and commit.

#### Phase 6: SegmentMerger and MergeState

With all readers available, implement the merge orchestrator.

**Java reference**:
- `lucene/core/src/java/org/apache/lucene/index/SegmentMerger.java`
- `lucene/core/src/java/org/apache/lucene/index/MergeState.java`
- `lucene/core/src/java/org/apache/lucene/index/DocIDMerger.java`

- Implement `MergeState` (reader instances + doc ID mapping)
- Implement `DocMap` (old doc ID → new doc ID)
- Implement `SegmentMerger` that reads N source segments and writes 1 merged segment
- Test: create 3 small segments, merge into 1, verify merged segment via readers
- Test: verify merged segment passes Java `VerifyIndex`

**[CHECKPOINT]**: Review and commit.

#### Phase 7: TieredMergePolicy

Implement the policy that decides which segments to merge.

**Java reference**: `lucene/core/src/java/org/apache/lucene/index/TieredMergePolicy.java`

- Implement `MergePolicy` trait
- Implement `TieredMergePolicy` (score-based segment selection, size tiers)
- Unit tests for merge selection logic

**[CHECKPOINT]**: Review and commit.

#### Phase 8: IndexWriter integration

Wire merging into the IndexWriter lifecycle.

- Add `maybe_merge()` call after each flush
- Execute merges synchronously (no concurrent merge scheduler yet)
- Update commit flow to include merged segments
- Integration tests: index documents, verify merging reduces segment count
- Run full benchmark suite and compare to Java

**[CHECKPOINT]**: Review and commit.

## How to Run Benchmarks

### Java vs Rust comparison (correctness + performance, 2000 synthetic docs)

Generates a synthetic corpus, indexes with both Java and Rust, runs `VerifyIndex` on all four indexes (Java 1T, Java 12T, Rust 1T, Rust 12T), and prints a summary table.

```bash
# Generate test corpus (one-time)
python3 testdata/gen_docs.py -n 2000

# Run comparison
./tests/compare_java_rust.sh -docs /tmp/perf-docs -release --threads 12
```

### Gutenberg benchmark (performance only, real-world text)

Indexes Project Gutenberg plain text files with both Java and Rust. No VerifyIndex (Gutenberg docs don't match the test field schema). Compares indexing time, peak RSS, index size, and segment count.

Requires Gutenberg corpus already extracted to `/tmp/gutenberg-docs/` (76K files, ~3.7 GB). See `/tmp/gutenberg-benchmark/benchmark.sh` for download instructions.

```bash
# 500 docs (quick check, ~200 MB input)
/tmp/gutenberg-benchmark/benchmark.sh -n 500 -release --skip-extract --threads 12

# Full corpus (76K docs, ~3.7 GB input)
/tmp/gutenberg-benchmark/benchmark.sh -release --skip-extract --threads 12
```

### Memory profiling with heaptrack

Profiles heap allocations to identify memory hotspots. Produces an interactive GUI showing allocation sizes, call stacks, and peak heap over time.

```bash
# Profile the Rust indexer
cargo build --release --bin indexfiles
heaptrack target/release/indexfiles -docs /tmp/perf-docs -index /tmp/heap-idx --threads 12

# View results
heaptrack_gui heaptrack.indexfiles.<pid>.zst
```

Key things to look for:
- Peak heap vs `ram_bytes_used()` — are they close? If heap is much larger, something isn't tracked.
- Top allocation sites — which data structures dominate?
- Allocation rate over time — are there spikes during flush?

### CPU profiling with flamegraph

Generates a flamegraph SVG showing where CPU time is spent.

```bash
cargo flamegraph --bin indexfiles -- -docs /tmp/perf-docs -index /tmp/flame-idx --threads 12
# Opens flamegraph.svg
```

Key things to look for:
- Time in `add_term` vs flush vs codec writes
- Hash table operations (rehash, probe chains)
- Memory allocation overhead (are we spending time in `alloc`/`dealloc`?)

## Java Source References

Block pool infrastructure (`util/`):
- `ByteBlockPool.java` — byte block allocation with counter tracking
- `IntBlockPool.java` — int block allocation with counter tracking
- `BytesRefHash.java` — compact term-to-ID hash table backed by ByteBlockPool
- `Counter.java` — shared allocation counter

Posting arrays (`index/`):
- `ParallelPostingsArray.java` — base parallel array
- `FreqProxTermsWriterPerField.java` — posting array with freq/pos/offset
- `TermsHashPerField.java` — wires BytesRefHash + PostingsBytesStartArray

Flush control (`index/`):
- `DocumentsWriterFlushControl.java` — global RAM tracking, flush-largest selection
- `FlushByRamOrCountsPolicy.java` — checks global active bytes
- `DocumentsWriterPerThread.java` — per-thread document writer

Merge (`index/`):
- `SegmentMerger.java` — merge orchestrator
- `MergeState.java` — merge metadata and reader instances
- `DocIDMerger.java` — document ID iteration across segments
- `TieredMergePolicy.java` — segment selection policy

All in `reference/lucene-10.3.2/lucene/core/src/java/org/apache/lucene/`.
