# .tim Suffix Compression

## Problem

The BlockTree terms dictionary (.tim) is ~19% larger in Rust than Java because Rust does not implement suffix compression. On a 500-doc gen_docs corpus (single segment), this is 35,225 vs 29,613 bytes (+5,612).

The comment at `src/codecs/lucene103/blocktree_writer.rs:731` marks this as an MVP limitation:
```rust
// Write suffix data (uncompressed, no LZ4 for MVP)
```

## How Java Does It

When writing a block's suffix bytes, Java tries three strategies in order (`Lucene103BlockTreeTermsWriter.java:886-935`):

1. **LZ4** — if average suffix length > 6 bytes AND compression achieves >= 25% savings
2. **LOWERCASE_ASCII** — a simple byte-packing scheme for lowercase ASCII text
3. **NO_COMPRESSION** — fallback

Two conditions skip compression entirely:
- `suffixBytesLength <= 2 * numEntries` (too few bytes to bother)
- `prefixLength <= 2` (preserves fuzzy query performance)

The chosen algorithm is encoded in the suffix token's low bits:
```
token = (suffixBytesLength << 3) | isLeafBlock | compressionCode
```
Where `compressionCode`: 0 = none, 1 = LOWERCASE_ASCII, 2 = LZ4.

## What Rust Has Today

Rust writes the token correctly but always sets `compressionCode = 0`. The block splitting logic, suffix length optimization, stats encoding, and floor block encoding all match Java.

## What To Implement

1. Port `LowercaseAsciiCompression.compress()` from `org.apache.lucene.util.compress.LowercaseAsciiCompression`
2. Port LZ4 compression from `org.apache.lucene.util.compress.LZ4`
3. Add the selection logic in `blocktree_writer.rs` around line 731

LZ4 is the bigger lift. LOWERCASE_ASCII is simpler and may cover a useful fraction of the savings on its own.

Note: We already have a Rust LZ4 implementation in `src/util/compress/lz4.rs` (used by stored fields), but it uses a standard hash table (`HASH_LOG = 14`). Java's blocktree uses `LZ4.HighCompressionHashTable` for better compression ratios. Before porting the Java high-compression variant, evaluate whether a Rust LZ4 crate could provide this instead — it may be more correct, better tested, and easier to maintain. The key constraint is that the compressed output must be byte-compatible with Lucene's LZ4 decompressor (standard LZ4 block format, no framing).

## Cascading Effects

Fixing this will also change (and likely fix) these related files:
- `.tmd` — term dictionary metadata (file pointers into .tim)
- `.tip` — term index (block pointers into .tim)
- `.psm` — postings metadata (references .tim offsets)

## Reference Files

- Rust: `src/codecs/lucene103/blocktree_writer.rs` (lines 725-750)
- Java: `reference/lucene-10.3.2/lucene/core/src/java/org/apache/lucene/codecs/lucene103/blocktree/Lucene103BlockTreeTermsWriter.java` (lines 886-950)
- LZ4: `reference/lucene-10.3.2/lucene/core/src/java/org/apache/lucene/util/compress/LZ4.java`
- LOWERCASE_ASCII: `reference/lucene-10.3.2/lucene/core/src/java/org/apache/lucene/util/compress/LowercaseAsciiCompression.java`
- Format spec: `reference/formats/lucene103-formats.md` (lines 360-368)
