# Known Issues

Outstanding problems and optimization gaps in the indexing pipeline.

---

## 1. Dual Document/Field Models for Write vs Read

**Severity:** Architecture — two parallel representations for the same concept

The write path uses `document::Document` / `index::field::Field` (the builder DSL with `text()`, `keyword()`, `stored()`, etc.). The read path uses `StoredField { field_number, StoredValue }` from `StoredFieldsReader` and `FieldInfo`/`FieldInfos` for metadata.

In Lucene's Java model, `Document` and `Field` serve both read and write — a `Document` is what you index AND what you get back from `IndexReader.document()`. In Rust, these are completely separate types.

**Current state:** Both models work, no bugs. But having two separate document representations means code that reads an index and re-indexes (e.g., merging) would need to convert between them.

**Fix:** Consider whether `Document`/`Field` should be unified across read and write, or whether the separation is actually a feature (write-side fields are richer with builders and streaming readers, read-side fields are simpler). This is a design decision, not a bug.

---

## 2. Dual Directory Traits

**Severity:** Architecture — `store::Directory` uses `&mut self` for writes, preventing concurrent access without a `Mutex` wrapper (`SharedDirectory`)

The write path wraps `store::Directory` in `SharedDirectory` (a `Mutex`) for all file I/O. The original newindex design introduced a separate `Directory` trait with `&self` methods and `Send + Sync` bounds, but this was never adopted by the codec writers — they all use `SharedDirectory` directly.

**Current state:** The newindex `Directory` trait and its `DirectoryAdapter` have been deleted. Everything uses `store::Directory` through `SharedDirectory`. This works but means all file I/O is serialized through a single mutex.

**Fix:** Consider whether `store::Directory` should adopt `&self` semantics (with interior mutability in implementations) to enable concurrent file creation. This would eliminate the `SharedDirectory` wrapper. Lower priority — the mutex is not a bottleneck since file I/O is infrequent relative to in-memory indexing work.

---

## 3. Level1 Skip Data for Write Path

**Severity:** Correctness — posting lists with >4096 docs cannot be written

The postings writer (`PostingsWriter`) currently panics if a term has more than `LEVEL1_NUM_DOCS` (4096) documents. Level1 skip data is required for terms that span multiple level0 blocks (each block = 128 docs × 32 blocks = 4096 docs). Without it, the writer cannot produce valid postings for high-frequency terms in large indexes.

The read path (`PostingsReader`) already handles level1 skip data — the `level1_last_doc_id`, `level1_doc_end_fp`, and `level1_doc_count_upto` fields are in place, and the `next_doc`/`advance` methods have level1 branching logic. The gap is the write side.

**Fix:** In `PostingsWriter::write_term`, when `doc_count` reaches `LEVEL1_NUM_DOCS`, emit level1 skip metadata (aggregate doc count, file pointers, impact data) and reset the level0 block counter. Match Java's `Lucene103PostingsWriter.encodeTerm` level1 encoding.

---

## 4. Peak RSS Higher Than Java

**Severity:** Medium — Rust peak RSS is higher than Java for the same workload

Rust peak RSS during indexing is larger than Java's. Two known contributing factors:

1. **Memory fragmentation** — partially addressed by switching to jemalloc, which returns freed pages to the OS more aggressively than glibc malloc.
2. **Higher default RAM buffer** — Rust defaults to 64 MB vs Java's 16 MB. Lowering the Rust default would reduce peak RSS but dramatically increases segment count because Rust does not yet merge segments during indexing.

**Next steps:** Implement segment merging during indexing (matching Java's `IndexWriter` merge policy). Once merging is in place, reduce the default RAM buffer to match Java's 16 MB. Then remeasure and profile for additional memory optimization opportunities.
