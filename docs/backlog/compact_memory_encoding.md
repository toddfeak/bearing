# Compact In-Memory Encoding

## Related
- [memory_and_segments.md](memory_and_segments.md) — broader memory and flushing problems
- [streaming_index_writes.md](streaming_index_writes.md) — lifecycle-based incremental writes to reduce buffered data

## Problem

During indexing, Bearing stores intermediate data as rich Rust types (enums, Strings, Vecs of tuples) and encodes to the compact on-disk format only at flush time. Lucene encodes data at insertion time, so its in-memory footprint is close to the on-disk size. This difference means Bearing uses 2-8x more memory per value depending on the component, which compounds with the lifecycle problem (holding data too long) to inflate `ram_bytes_used()` and trigger premature flushes.

Even after implementing incremental writes from `streaming_index_writes.md`, segment-level data (postings, norms, doc values, points) still sits in memory for the full segment lifetime. Encoding that data compactly at insertion time reduces its footprint without changing when it's written.

## Component Comparison

### Postings — Addressed

Replaced per-term `PostingList` structs (~193 bytes each) with a struct-of-arrays `PostingsArray` (~33 bytes per term). Position and offset deltas are now written immediately as vInts to per-term `prox_streams` during tokenization, eliminating the `Vec<i32>` position buffers entirely. For a 33MB document with ~50K unique terms, postings memory dropped from 106.4MB to 87.8MB (−17.5%).

### Stored Fields — Structured Enums vs Pre-Encoded Bytes

| | Lucene | Bearing |
|---|---|---|
| In-memory format | `ByteBuffersDataOutput` — field data encoded as vLong (field number + type code) followed by encoded value (vInt-length + UTF-8 for strings, zInt/zLong/zFloat/zDouble for numerics) | `Vec<(u32, StoredValue)>` — Rust enum holding owned `String`, `Vec<u8>`, or primitive |
| Overhead per string field | ~5 + string length bytes | 24 (String struct) + string length + capacity padding + 4 (field number) |
| Overhead per numeric field | ~2-9 bytes | 16-24 bytes (enum discriminant + value + field number tuple) |

Lucene's stored fields writer encodes field data into a byte buffer immediately during `startDocument`/`writeField`. The buffer is the pre-compression chunk payload — when the chunk triggers, it's LZ4-compressed and written directly.

Bearing holds `StoredValue` enums with owned heap data until flush, then iterates to encode. For documents with substantial stored text, the String allocations dominate.

### Term Vectors — Structured Hierarchy vs Byte Streams

| | Lucene | Bearing |
|---|---|---|
| In-memory format | ByteSlicePool byte streams — positions and offsets encoded as vInts into the same pooled byte infrastructure as postings | `TermVectorDoc` → `Vec<TermVectorField>` → `Vec<TermVectorTerm>` with owned `String` per term, `Vec<i32>` for positions, `OffsetBuffers` for offsets |
| Memory per term occurrence | ~5-15 bytes (encoded) | 24+ bytes (String) + 12+ bytes per position + offset Vecs |

This is the component where the gap is largest in practice, because term vectors store data per-term-per-document and there are thousands of unique terms per document. A document with 2K unique terms holds 2K `String` allocations just for the term text — text that already exists in `PerFieldData.term_ids`.

Note: the committed optimization stores TV accumulators directly on `PostingList` (eliminating the redundant HashMap), but `take_term_vector_data` still clones term strings into `TermVectorTerm` for the codec writer.

### Doc Values — Raw Tuples vs Packed Values

| | Lucene | Bearing |
|---|---|---|
| Numeric | `PackedLongValues.Builder` — delta-packed longs, ~2-4 bytes per value, plus `DocsWithFieldSet` sparse bitset | `Vec<(i32, i64)>` — 16 bytes per value (doc ID + raw i64) |
| Binary | `ByteBlockPool` — pooled byte storage | `Vec<(i32, Vec<u8>)>` — 4 + 24 + data length per value |
| Sorted | Deduplicated term dictionary + ordinal mapping | `Vec<(i32, BytesRef)>` — no deduplication during accumulation |
| SortedNumeric | Packed longs with position tracking | `Vec<(i32, Vec<i64>)>` — nested Vec per doc |
| SortedSet | Ordinal-mapped with deduplication | `Vec<(i32, Vec<BytesRef>)>` — no deduplication during accumulation |

The overhead multiplier varies by type. Numeric is ~4-8x (16 bytes vs 2-4). Binary depends on value size. Sorted types pay additionally for no deduplication — the same byte value may appear in multiple documents but is stored separately in each tuple.

### Norms — Raw i64 vs Packed

| | Lucene | Bearing |
|---|---|---|
| In-memory format | Packed numeric doc values (~1 byte per doc for typical norm range) | `Vec<i64>` (8 bytes per value) + `Vec<i32>` (4 bytes per doc ID) |
| Overhead per doc | ~1-2 bytes | 12 bytes |

Norms are single-byte values (after SmallFloat encoding) stored as i64. The 8-byte i64 representation wastes 7 bytes per norm. The parallel `norms_docs` Vec adds another 4 bytes per doc for the sparse doc ID tracking.

## Approach

The general pattern is: encode to a compact byte representation at insertion time rather than at flush time. This trades some code simplicity for significantly lower memory usage.

Priority should be guided by which components contribute most to memory pressure. Term vectors and stored fields are the largest per-document contributors and are also candidates for incremental writes (see `streaming_index_writes.md`). For those, the compact encoding and incremental write optimizations are complementary — encode compactly into a chunk buffer, flush the chunk when it hits the threshold.

For segment-level data (doc values, norms), compact encoding reduces the per-value footprint for data that must stay in memory for the full segment. This is lower priority but still valuable for large segments.

### Arena-Based Memory Architecture

Lucene's ByteBlockPool / ByteSlicePool is essentially an arena allocator — pre-allocate large 32KB blocks, bump-allocate variable-length slices from them, free everything at once when the owning context resets. This reduces allocation overhead, improves cache locality, and enables buffer recycling across segments in multi-threaded indexing. ByteSlicePool adds forwarding-address chains between slices so a single term's data can grow across multiple non-contiguous slices within the pool.

Bearing's per-term `Vec<u8>` approach is simpler and correct, but creates many small heap allocations (one per unique term per field). For multi-threaded indexing with SegmentWorker pools, an arena approach would reduce allocator pressure. Rust crates like `bumpalo` provide arena allocators that fit this pattern.

A critical design consideration: arenas must align with data lifecycles (see `streaming_index_writes.md`). Different index data types flush at different times, and an arena can only bulk-free when *all* data in it is ready to be released. Mixing lifecycles in a single arena prevents incremental freeing. This means multiple arenas per SegmentWorker, at minimum:

- **Stored fields chunk arena** — freed each time a stored fields chunk flushes to `.fdt` (~16KB or 128 docs)
- **Term vectors chunk arena** — freed each time a term vectors chunk flushes to `.tvd` (~4KB or 128 docs)
- **Segment arena** — freed when the segment flushes (holds postings, norms, doc values, points)

Each arena's lifetime matches its flush cycle. The incremental arenas churn frequently with low peak usage. The segment arena grows steadily but only holds data that genuinely requires the full segment lifetime. If additional incremental file types are added in the future, each gets its own arena matching its flush cycle.
