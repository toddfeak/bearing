# Known Issues

Outstanding problems and optimization gaps in the indexing pipeline.

---

## 1. Peak RSS Higher Than Java

**Severity:** Medium — Rust peak RSS is higher than Java for the same workload

Rust peak RSS during indexing is larger than Java's. Two known contributing factors:

1. **Memory fragmentation** — partially addressed by switching to jemalloc, which returns freed pages to the OS more aggressively than glibc malloc.
2. **Higher default RAM buffer** — Rust defaults to 64 MB vs Java's 16 MB. Lowering the Rust default would reduce peak RSS but dramatically increases segment count because Rust does not yet merge segments during indexing.

**Next steps:** Implement segment merging during indexing (matching Java's `IndexWriter` merge policy). Once merging is in place, reduce the default RAM buffer to match Java's 16 MB. Then remeasure and profile for additional memory optimization opportunities.

---

## 2. Incomplete SmallFloat Port

**Severity:** Low — missing methods not yet needed

`src/util/small_float.rs` is a partial port of `o.a.l.util.SmallFloat`. The integer encoding methods (`longToInt4`, `int4ToLong`, `intToByte4`, `byte4ToInt`) are ported and working. Four float encoding methods are missing:

- `floatToByte(float, numMantissaBits, zeroExp)` — generic float-to-byte encoder using IEEE 754 bit manipulation
- `byteToFloat(byte, numMantissaBits, zeroExp)` — the reverse
- `floatToByte315(float)` — specialization with mantissa=3, zeroExp=15
- `byte315ToFloat(byte)` — the reverse

Additionally, `intToByte4` silently returns 0 on negative input instead of panicking (Java throws `IllegalArgumentException`).

**When this matters:** The missing float methods are used by the similarity/scoring layer (e.g., `BM25Similarity`) to encode and decode norm values as floats. They will be needed when porting similarity implementations for query-time scoring. The integer methods used by norms during indexing are already correct.

**Fix:** Complete the port following porting rules. Port the corresponding tests from `o.a.l.util.TestSmallFloat`.

---

## 3. No Offset or Payload Support in Write Path

**Severity:** Feature gap — fields with offsets or payloads cannot be indexed

The postings writer (`PostingsWriter`) does not support writing offsets or payloads. There is no `pay_out` file handle, no offset/payload buffering, and no encoding logic for the `.pay` file. The read path (`PostingsReader`) already handles `has_offsets_or_payloads` in its skip data parsing and postings iteration.

This affects `IndexOptions::DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS` and any field that uses payloads (e.g., `TermVectorOptions` with payloads).

**Fix:** Port the offset/payload branches from Java's `Lucene103PostingsWriter` — `payOut` file creation, `offsetStartDeltaBuffer`/`offsetLengthBuffer`/`payloadLengthBuffer` buffering, payload byte accumulation, and the `.pay` encoding in `flushDocBlock`/`addPosition`/`finishTerm`. Port corresponding skip data fields for level0 and level1.
