# Known Issues

Outstanding problems and optimization gaps in the indexing pipeline.

---

## ~~1. Flush Stall Control~~ — RESOLVED

`FlushControl` now includes `Condvar`-based stall control. Workers block before pulling new documents when total RAM exceeds 2x the buffer limit. Stall releases when RAM drops below the 80% flush target after flushes complete. Matches Java's `DocumentsWriterStallControl` pattern.

---

## 2. StandardTokenizer UAX#29 Compliance — PARTIALLY RESOLVED

**Status:** `UnicodeAnalyzer` available as an opt-in alternative via `IndexWriterConfig::analyzer_factory()`. Matches Java Lucene's token output for all categories except emoji sequences (0.09% token count gap on gutenberg-large-500). `StandardAnalyzer` remains the fast default (212 MB/s) but does not handle CJK, numeric grouping, or URLs correctly.

**Remaining gap:** Emoji tokenization (😀, 👍🏽, 🇺🇸) — `unicode-segmentation` does not classify emoji as word segments. ~280K tokens on gutenberg-large-500.

---

## ~~3. StandardAnalyzer Buffers All Tokens~~ — RESOLVED

Both `StandardAnalyzer` and `UnicodeAnalyzer` stream input in ~8 KB UTF-8 chunks via `utf8-zero`. Peak RSS: ~2 MB for tokenization regardless of document size.

---

## ~~4. Zero-Copy Tokenization with Sliding Window~~ — RESOLVED

Analyzer owns its reader via `set_reader()`. Tokens are zero-copy `&str` borrows from the analyzer's internal chunk buffer. Pluggable via `AnalyzerFactory` trait on `IndexWriterConfig`.

---

## 5. Dual Document/Field Models for Write vs Read

**Severity:** Architecture — two parallel representations for the same concept

The write path uses `document::Document` / `index::field::Field` (the builder DSL with `text()`, `keyword()`, `stored()`, etc.). The read path uses `StoredField { field_number, StoredValue }` from `CompressingStoredFieldsReader` and `FieldInfo`/`FieldInfos` for metadata.

In Lucene's Java model, `Document` and `Field` serve both read and write — a `Document` is what you index AND what you get back from `IndexReader.document()`. In Rust, these are completely separate types.

**Current state:** Both models work, no bugs. But having two separate document representations means code that reads an index and re-indexes (e.g., merging) would need to convert between them.

**Fix:** Consider whether `Document`/`Field` should be unified across read and write, or whether the separation is actually a feature (write-side fields are richer with builders and streaming readers, read-side fields are simpler). This is a design decision, not a bug.

---

## 6. Dual Directory Traits

**Severity:** Architecture — `store::Directory` uses `&mut self` for writes, preventing concurrent access without a `Mutex` wrapper (`SharedDirectory`)

The write path wraps `store::Directory` in `SharedDirectory` (a `Mutex`) for all file I/O. The original newindex design introduced a separate `Directory` trait with `&self` methods and `Send + Sync` bounds, but this was never adopted by the codec writers — they all use `SharedDirectory` directly.

**Current state:** The newindex `Directory` trait and its `DirectoryAdapter` have been deleted. Everything uses `store::Directory` through `SharedDirectory`. This works but means all file I/O is serialized through a single mutex.

**Fix:** Consider whether `store::Directory` should adopt `&self` semantics (with interior mutability in implementations) to enable concurrent file creation. This would eliminate the `SharedDirectory` wrapper. Lower priority — the mutex is not a bottleneck since file I/O is infrequent relative to in-memory indexing work.

---

## 7. Level1 Skip Data for Write Path

**Severity:** Correctness — posting lists with >4096 docs cannot be written

The postings writer (`PostingsWriter`) currently panics if a term has more than `LEVEL1_NUM_DOCS` (4096) documents. Level1 skip data is required for terms that span multiple level0 blocks (each block = 128 docs × 32 blocks = 4096 docs). Without it, the writer cannot produce valid postings for high-frequency terms in large indexes.

The read path (`PostingsReader`) already handles level1 skip data — the `level1_last_doc_id`, `level1_doc_end_fp`, and `level1_doc_count_upto` fields are in place, and the `next_doc`/`advance` methods have level1 branching logic. The gap is the write side.

**Fix:** In `PostingsWriter::write_term`, when `doc_count` reaches `LEVEL1_NUM_DOCS`, emit level1 skip metadata (aggregate doc count, file pointers, impact data) and reset the level0 block counter. Match Java's `Lucene103PostingsWriter.encodeTerm` level1 encoding.

---

## 8. Memory Fragmentation and Peak RSS

**Severity:** High — Rust peak RSS is 10x+ Java for the same workload

On gutenberg-large-500 (12 threads, 64 MB buffer): Java peak RSS is ~103 MB, Rust peak RSS is ~1,174 MB. Heap peak (measured by heaptrack) is only ~422 MB, and reported RAM in `FlushControl` never exceeds ~130 MB. The gap between logical heap usage and OS RSS is caused by memory fragmentation — glibc malloc does not return freed pages to the OS when many small/medium allocations are freed in a pattern that leaves holes.

**Root cause:** Each segment worker allocates hundreds of individual heap objects (32 KB byte pool blocks, 32 KB int pool blocks, growing postings arrays, hash tables) during indexing. When the worker flushes and drops, these are all freed individually. The allocator cannot coalesce them because they are interleaved with allocations from other threads. Over 227 segment flushes, this creates severe fragmentation.

**Profiling data** (heaptrack, gutenberg-large-500, 12 threads):
- 510M total allocation calls, 306M temporary
- Term vectors account for 157 MB of 166 MB peak heap (**94.6%**)
- Top TV allocation sites:
  - `TermVectorChunkWriter::finish_term` — 15.4M calls, 32.4 MB peak (Vec growth per term)
  - `TermVectorChunkWriter::add_prox` — 46.3M calls, ~30.7 MB peak (Vec growth per position/offset)
  - `TermVectorsConsumer::finish_document` — 15.4M calls, 10.75 MB peak (reserve/resize)
- Postings: 296.7M calls but 0 B peak (inline writes to pre-allocated pools)

**Proposed fix:** Use `bumpalo` as a bump allocator for per-segment (and potentially per-document) memory. A `Bump` arena allocates contiguously and frees everything at once when dropped or reset — no fragmentation, no syscalls per deallocation.

The `Bump` would live in `worker_thread_loop`, outliving the `SegmentWorker`. On flush, the worker drops (releasing all bump references), then `bump.reset()` reclaims the arena cheaply for the next segment. This avoids the alloc/free churn that fragments the heap.

**Primary target:** Term vectors — responsible for 94.6% of peak heap. The `TermVectorChunkWriter` uses standard `Vec<T>` collections that grow incrementally via `push()`. Replacing these with `bumpalo::collections::Vec` eliminates per-element reallocation.

**Design note:** Lucene solves this with `ByteBlockPool` / `IntBlockPool` — fixed-size block arenas that are recycled. Bumpalo is a simpler approach that achieves the same goal (contiguous allocation, bulk deallocation) without requiring custom pool management. The two approaches are not mutually exclusive — existing pools can remain for postings while bumpalo handles term vectors.

**Implementation cost:** `bumpalo::collections::Vec<'bump, T>` carries a lifetime tied to the arena. This requires a `'bump` parameter on `ByteBlockPool`, `TermsHash`, the TV consumer, and `SegmentWorker`. The lifetime is contained within the pipeline module — it does not propagate to `IndexCoordinator`, `IndexWriter`, or the public API. Works on stable Rust (no `allocator_api` needed).
