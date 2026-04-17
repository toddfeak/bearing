# Known Issues

Outstanding problems and optimization gaps in the indexing pipeline.

---

## 1. Level1 Skip Data for Write Path

**Severity:** Correctness — posting lists with >4096 docs cannot be written

The postings writer (`PostingsWriter`) currently panics if a term has more than `LEVEL1_NUM_DOCS` (4096) documents. Level1 skip data is required for terms that span multiple level0 blocks (each block = 128 docs × 32 blocks = 4096 docs). Without it, the writer cannot produce valid postings for high-frequency terms in large indexes.

The read path (`PostingsReader`) already handles level1 skip data — the `level1_last_doc_id`, `level1_doc_end_fp`, and `level1_doc_count_upto` fields are in place, and the `next_doc`/`advance` methods have level1 branching logic. The gap is the write side.

**Fix:** In `PostingsWriter::write_term`, when `doc_count` reaches `LEVEL1_NUM_DOCS`, emit level1 skip metadata (aggregate doc count, file pointers, impact data) and reset the level0 block counter. Match Java's `Lucene103PostingsWriter.encodeTerm` level1 encoding.

---

## 2. Peak RSS Higher Than Java

**Severity:** Medium — Rust peak RSS is higher than Java for the same workload

Rust peak RSS during indexing is larger than Java's. Two known contributing factors:

1. **Memory fragmentation** — partially addressed by switching to jemalloc, which returns freed pages to the OS more aggressively than glibc malloc.
2. **Higher default RAM buffer** — Rust defaults to 64 MB vs Java's 16 MB. Lowering the Rust default would reduce peak RSS but dramatically increases segment count because Rust does not yet merge segments during indexing.

**Next steps:** Implement segment merging during indexing (matching Java's `IndexWriter` merge policy). Once merging is in place, reduce the default RAM buffer to match Java's 16 MB. Then remeasure and profile for additional memory optimization opportunities.

---

## 3. Incomplete SmallFloat Port

**Severity:** Low — missing methods not yet needed

`src/util/small_float.rs` is a partial port of `o.a.l.util.SmallFloat`. The integer encoding methods (`longToInt4`, `int4ToLong`, `intToByte4`, `byte4ToInt`) are ported and working. Four float encoding methods are missing:

- `floatToByte(float, numMantissaBits, zeroExp)` — generic float-to-byte encoder using IEEE 754 bit manipulation
- `byteToFloat(byte, numMantissaBits, zeroExp)` — the reverse
- `floatToByte315(float)` — specialization with mantissa=3, zeroExp=15
- `byte315ToFloat(byte)` — the reverse

Additionally, `intToByte4` silently returns 0 on negative input instead of panicking (Java throws `IllegalArgumentException`).

**When this matters:** The missing float methods are used by the similarity/scoring layer (e.g., `BM25Similarity`) to encode and decode norm values as floats. They will be needed when porting similarity implementations for query-time scoring. The integer methods used by norms during indexing are already correct.

**Fix:** Complete the port following porting rules. Port the corresponding tests from `o.a.l.util.TestSmallFloat`.
