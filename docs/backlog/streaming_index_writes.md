# Streaming Index Writes

## Related
- [memory_and_segments.md](memory_and_segments.md) — broader memory and flushing problems
- [compact_memory_encoding.md](compact_memory_encoding.md) — compact in-memory encoding and arena architecture

## Problem

During indexing, Bearing treats nearly all index data as segment-level — accumulated in memory across all documents until the segment flushes. This inflates `ram_bytes_used()`, triggers premature flushes, and produces far more segments than necessary. The term vectors implementation made this visible: enabling TV on the "contents" field increased segments from 3 to 28 on a 2000-doc corpus (and is worse on Gutenberg data), because each document's TV data (term strings, positions, offsets) was held in structured `Vec<TermVectorDoc>` until flush.

Java Lucene avoids this by writing data incrementally during indexing. The `CompressingTermVectorsWriter` flushes chunks to `.tvd` as soon as 4KB of term suffix data or 128 documents have accumulated. Stored fields work similarly. The RAM buffer only holds data that genuinely requires cross-document knowledge.

Not all index data has the same lifecycle. We should handle each appropriately rather than treating everything as segment-level.

## Data Lifecycle Categories

### Per-Document

Raw accumulation buffers that are consumed and reset after each document completes. In Lucene, the TermsHash pools for term vectors reset every document — the raw per-term TV data (positions, offsets, frequencies) is gathered during tokenization and then drained into the chunk buffer when the document finishes. The memory is immediately available for the next document.

This lifecycle applies to intermediate working state, not final output. The data feeds into a per-chunk buffer (see below) rather than being written to disk directly.

### Per-Chunk

Encoded output that accumulates across multiple documents and flushes to disk when a threshold is reached (byte size or document count). Each chunk is self-contained on disk — its own header, columnar metadata, compressed term data. Memory is bounded by the chunk size, not the segment size.

In Lucene, the `CompressingTermVectorsWriter` flushes at ~4KB of term suffix data or 128 documents. The `CompressingStoredFieldsWriter` flushes at ~16KB or 128 documents. After a chunk flush, the buffer resets and starts fresh.

### Per-Segment

Data that requires knowledge of all documents in the segment before it can be written. This is the only lifecycle that legitimately needs to stay in memory for the full segment lifetime.

## Lifecycle Interaction

Per-document and per-chunk lifecycles cooperate. Using term vectors as an example:

1. During tokenization, raw TV data (positions, offsets, frequencies) accumulates in per-document buffers.
2. When the document completes, the raw data is encoded and appended to the per-chunk buffer. The per-document buffers reset.
3. When the per-chunk buffer hits its threshold, the chunk is written to `.tvd`. The per-chunk buffer resets.
4. At segment flush, any remaining partial chunk is force-written (marked "dirty"), and the `.tvx`/`.tvm` index files are finalized from the small vector of chunk start pointers accumulated during step 3.

This layering means memory pressure from TV data is bounded by one document's raw data plus one chunk's encoded data, rather than the entire segment's worth.

## Index File Classification

### Stored Fields (`.fdt`, `.fdx`, `.fdm`)

| File | Lifecycle | Notes |
|---|---|---|
| `.fdt` (data) | **Per-Chunk** | Flushed at ~16KB or 128 docs. Each chunk is self-contained with LZ4 compression. Java flushes incrementally. |
| `.fdx` (index) | **Per-Segment** | Chunk-to-file-pointer mapping. Written at segment flush from a small vector of chunk start pointers accumulated during incremental `.fdt` writes. |
| `.fdm` (meta) | **Per-Segment** | Header, chunk counts, pointers into `.fdx`. Written at segment flush. Tiny. |

### Term Vectors (`.tvd`, `.tvx`, `.tvm`)

| File | Lifecycle | Notes |
|---|---|---|
| `.tvd` (data) | **Per-Document** (raw) → **Per-Chunk** (encoded) | Raw TV data resets per document. Encoded chunks flush at ~4KB of term suffix data or 128 docs. Java flushes via `triggerFlush()` after each `finishDocument()`. |
| `.tvx` (index) | **Per-Segment** | Chunk-to-file-pointer mapping. Same pattern as stored fields index. |
| `.tvm` (meta) | **Per-Segment** | Header, chunk counts, dirty chunk stats. Written at segment flush. Tiny. |

### Postings (`.doc`, `.pos`, `.pay`, `.tip`, `.tim`)

| File | Lifecycle | Notes |
|---|---|---|
| All postings files | **Per-Segment** | Requires all documents to compute per-term doc frequency, total term frequency, and to build the block tree index. The byte-stream encoding in `PostingList` is already compact, so this is less of a memory concern than stored fields or term vectors. |

### Norms (`.nvd`, `.nvm`)

| File | Lifecycle | Notes |
|---|---|---|
| `.nvd` (data) | **Per-Segment** | Needs sparse doc bitset (IndexedDISI) across all docs, plus all norm values for encoding decisions. |
| `.nvm` (meta) | **Per-Segment** | Written at segment flush. |

### Doc Values (`.dvd`, `.dvm`)

| File | Lifecycle | Notes |
|---|---|---|
| `.dvd` (data) | **Per-Segment** | Sorted types need all values for ordinal mapping. Numeric types need all values for encoding strategy (GCD, delta, table). |
| `.dvm` (meta) | **Per-Segment** | Written at segment flush. |

### Points / BKD (`.kdm`, `.kdi`, `.kdd`)

| File | Lifecycle | Notes |
|---|---|---|
| All BKD files | **Per-Segment** | Needs all points to build the BKD tree (splitting, sorting, leaf construction). |

### Segment Info and Field Info (`.si`, `.fnm`)

| File | Lifecycle | Notes |
|---|---|---|
| Both | **Per-Segment** | Metadata written once at segment flush. Tiny. |

## Biggest Wins

The per-chunk files that we currently treat as per-segment are the clear targets, ranked by memory impact:

1. **Term vectors `.tvd`** — Currently the worst offender. Each document stores `String` + `Vec<i32>` + `OffsetBuffers` per term. With 9K tokens per document and ~2K unique terms, this is ~28KB per document of structured data. On Gutenberg-scale documents it would be far larger. Implementing per-document reset of raw TV buffers plus per-chunk flushing of encoded data (as Java does) caps memory at one document's raw data plus one chunk's encoded output.

2. **Stored fields `.fdt`** — Each document's stored field values sit in `Vec<StoredDoc>` until flush. For documents with stored text content, this grows proportionally to document size. Java flushes chunks incrementally at ~16KB.

Both of these are self-contained per-document data being held for no reason. The `.tvx`/`.fdx` index files they depend on only need a small `Vec<i64>` of chunk start pointers, which is negligible.

Moving these two to per-document / per-chunk lifecycle handling would mean the RAM buffer primarily holds postings, norms, doc values, and points — the data that genuinely needs the full segment. This should dramatically reduce segment counts and bring memory usage closer to Java's behavior.
