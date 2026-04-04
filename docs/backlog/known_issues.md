# Known Issues

Outstanding problems and optimization gaps in the indexing pipeline.

---

## 1. Flush Stall Control

**Severity:** High for large-document corpora

With large documents and many threads, total RAM overshoots the configured buffer significantly. The current `FlushControl` signals workers to flush cooperatively, but workers can't be interrupted mid-document. Between the signal and the flush, all threads continue pulling and processing new documents, each potentially holding multi-MB postings pools.

Java's `DocumentsWriterStallControl` solves this by blocking new document intake when total RAM exceeds 2x the buffer limit. Threads already processing documents continue to completion and flush, but no new documents are pulled until RAM drops below the threshold.

**Measured impact** (gutenberg-large-50, 12 threads, 16MB buffer):
- Java peak RSS: 103 MB
- Rust peak RSS: 653 MB (after streaming analyzer fix)
- Rust reported RAM peaks at ~127 MB before flush signals take effect
- OS RSS stays high because the allocator doesn't return freed pages

**Fix:** Add a `Condvar`-based stall in `worker_thread_loop`. Before calling `doc_rx.recv()`, check if total RAM exceeds `2 * ram_buffer_size`. If so, wait on the `Condvar` until flushes bring it below threshold.

The `FlushControl` already has per-worker RAM slots needed for this. The missing piece is:
1. A `Condvar` + `Mutex<bool>` for the stall state
2. Check after each `after_document` — if total > 2x, set stall flag
3. In `worker_thread_loop`, wait on the condvar before `recv` when stalled
4. After each flush completes (`reset_worker`), check if total dropped below threshold and notify the condvar

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
