# TermsEnum Sequential Iteration

Implement `TermsEnum::next()` and `seek_ceil()` to enable sequential term iteration, then build tooling on top.

## Current State

`SegmentTermsEnum` supports only `seek_exact()`. It uses a flat, stateless approach: each call navigates the trie from scratch via `TrieReader::seek_to_block()`, parses the block with `scan_block()`, finds the term, and discards all state. There is no way to iterate terms in order.

The `TermsEnum` trait already declares `next()` and `seek_ceil()` with `todo!()` default implementations. The `Terms` trait, `FieldReader`, `BlockTreeTermsReader`, and `SegmentReader` are all in place.

### What Works

- `seek_exact(term)` — point lookup by exact term bytes
- `doc_freq()` / `total_term_freq()` — after a successful seek
- `term_state()` — returns `IntBlockTermState` for postings access
- `seek_exact_with_state()` — restore position from saved state
- Trie navigation (`TrieReader::seek_to_block`, `lookup_child`)
- Block parsing (suffix decompression, stats, metadata decoding)

### What's Missing

- `next()` — advance to the next term in lexicographic order
- `seek_ceil()` — seek to the first term >= target
- Frame stack architecture required by both

## Why This Is Needed

1. **Term inspection tooling** — listing all terms in a field for debugging and validation
2. **Golden summary enhancement** — per-field term counts validated against Java
3. **Future: multi-term queries** — `PrefixQuery`, `WildcardQuery`, `FuzzyQuery` all iterate terms
4. **Future: merge path** — segment merging iterates all terms from source segments

## Java Architecture (Lucene 103)

Reference files:
- `reference/lucene-10.3.2/lucene/core/src/java/org/apache/lucene/codecs/lucene103/blocktree/SegmentTermsEnum.java` (1070 lines)
- `reference/lucene-10.3.2/lucene/core/src/java/org/apache/lucene/codecs/lucene103/blocktree/SegmentTermsEnumFrame.java` (879 lines)

### Frame Stack

The block tree terms dictionary is a tree of blocks. Each block holds terms sharing a common prefix. When a block exceeds the max size, it splits into sub-blocks at the next byte position — creating depth.

Java's `SegmentTermsEnum` maintains a **frame stack** (`SegmentTermsEnumFrame[]`) where each frame represents one loaded block at a specific tree depth:

```
stack[0] = root block (prefix "")
stack[1] = child block (prefix "a")
stack[2] = grandchild block (prefix "ab")
...
currentFrame = stack[depth]  // the frame we're currently iterating in
```

A **frame** (`SegmentTermsEnumFrame`) holds:
- Block metadata: `fp`, `fpOrig`, `fpEnd`, `prefixLength`, `entCount`, `isLeafBlock`, `isFloor`, `isLastInFloor`
- Parsed block data: `suffixBytes[]`, `suffixLengthBytes[]`, `statBytes[]`, `bytes[]` (metadata)
- Reader positions: `suffixesReader`, `suffixLengthsReader`, `statsReader`, `bytesReader` (all `ByteArrayDataInput`)
- Iteration cursor: `nextEnt` (-1 = not loaded, 0..entCount during iteration)
- Floor data: `floorDataReader`, `nextFloorLabel`, `numFollowFloorBlocks`
- Term state: `state` (`BlockTermState`), `metaDataUpto`, `lastSubFP`

### How `next()` Works

`SegmentTermsEnum.next()` (Java lines 896-980):

1. **First call**: push root frame, load root block
2. **Static frame recovery**: if positioned via `seekExact(TermState)`, re-seek to catch up internal state
3. **Pop exhausted frames**: when `nextEnt == entCount`:
   - If not last in floor: `loadNextFloorBlock()` and continue
   - If last in floor and `ord == 0`: return null (exhausted)
   - Otherwise: pop to parent (`stack[ord-1]`), reload if needed via `scanToFloorFrame()` + `loadBlock()` + `scanToSubBlock()`
4. **Advance within frame**: call `currentFrame.next()`:
   - Returns `false` for term entries → return the term
   - Returns `true` for sub-block entries → push child frame, `loadBlock()`, repeat

### Key Frame Methods

| Method | Java Lines | Purpose |
|---|---|---|
| `loadBlock()` | 163-258 | Read block header, suffixes, suffix lengths, stats, metadata from `.tim`. Set `nextEnt = 0`. |
| `next()` | 310-317 | Dispatch to `nextLeaf()` or `nextNonLeaf()` |
| `nextLeaf()` | 319-331 | Read next suffix, copy to term buffer, increment `nextEnt` |
| `nextNonLeaf()` | 333-375 | Read next entry; if term → return false; if sub-block → set `lastSubFP`, return true |
| `loadNextFloorBlock()` | 129-137 | Set `fp = fpEnd`, reload via `loadBlock()` |
| `rewind()` | 260-307 | Reset frame to `fpOrig`, force reload |
| `scanToFloorFrame()` | 380-451 | Scan floor data to find correct sub-block for a target |
| `scanToSubBlock()` | 517-545 | Scan entries to find sub-block with given FP |
| `decodeMetaData()` | 453-501 | Lazily decode stats+metadata up to current term position |
| `scanToTerm()` | 548-871 | Scan block for a target term (used by `seekExact`/`seekCeil`) |

### Lazy Metadata Decoding

A critical optimization: `next()` only decodes the term bytes (suffix). Stats (docFreq, totalTermFreq) and postings metadata (file pointers) are decoded lazily via `decodeMetaData()` only when `docFreq()`, `totalTermFreq()`, or `postings()` is called. This makes term-only iteration fast.

`metaDataUpto` tracks how many terms have had their metadata decoded. `decodeMetaData()` catches up from `metaDataUpto` to `getTermBlockOrd()`. Without this, every `next()` would pay O(block_size) metadata decoding cost.

### Shared Term Buffer

Java's `SegmentTermsEnum` owns a single `BytesRefBuilder term` that frames write into. Frame methods like `nextLeaf()` set `ste.term.setLength(prefixLength + suffixLength)` and copy suffix bytes into it. The `term()` method returns a view of this buffer.

### `seek_exact()` with Frames

Java's `seekExact()` (lines 253-516) also uses the frame stack, not the flat approach we currently have. It:

1. Compares the target with the current term to find the common prefix
2. Reuses seek state for the shared prefix (avoids re-navigating from root)
3. Walks the trie index for the remaining bytes, pushing frames at each node with output
4. Calls `scanToFloorFrame()` + `loadBlock()` + `scanToTerm()` on the final frame

This means `seek_exact()` must also be refactored to use frames when we add `next()`, since `seek_exact` followed by `next()` must leave the frame stack in a consistent state.

## Approach

### Data Access Model

Frames use `IndexInput` for all data access — both sequential reads (block headers, VInts) and section reads (suffixes, stats, metadata). This matches Java's approach: `loadBlock()` reads each section from the `IndexInput` into frame-owned buffers, then uses `ByteArrayDataInput` readers for independent cursor access within those buffers.

In Rust, `SliceReader<'a>` (borrows `&[u8]`, provides `DataInput` methods) fills the role of Java's `ByteArrayDataInput`. Frames read sections into owned `Vec<u8>` buffers via `IndexInput`, then create `SliceReader` instances for zero-copy parsing within those buffers.

Performance depends on the `IndexInput` implementation: `MmapIndexInput` and `ByteSliceIndexInput` read from memory (fast), while `FSIndexInput` issues syscalls (slow). Command-line tools use `MmapDirectory` by default; tests use `MemoryDirectory`. No special handling is needed — the `IndexInput` abstraction works as-is.

### Rust-Specific Design Decisions

Port 1-1 from Java as the starting point. Notes on ownership patterns that differ:

**Frame ↔ SegmentTermsEnum back-reference**: Java's frame holds `ste` (parent reference) and freely mutates `ste.term`, `ste.termExists`, `ste.currentFrame`. Rust can't do back-references. Pass `&mut Vec<u8>` (term buffer) and `&mut bool` (term_exists) as parameters to frame methods — closest to Java's semantics.

**`currentFrame` as index, not reference**: Java uses `currentFrame` as a mutable reference into `stack[]`. Rust tracks `current_frame_ord: i32` and indexes into `self.stack[ord]`. When `ord == -1`, use `self.static_frame`.

**ByteArrayDataInput → SliceReader**: Java's frame has `suffixesReader`, `suffixLengthsReader`, `statsReader`, `bytesReader` as `ByteArrayDataInput` objects wrapping owned `byte[]`. In Rust, frames own `Vec<u8>` buffers for each section and create `SliceReader` instances on demand for zero-copy parsing within those buffers.

**Floor data**: Java's frame holds an `IndexInput` for floor data. In Rust, the frame stores floor data in an owned buffer and uses `SliceReader` for reads.

**Node storage**: Java stores `TrieReader.Node[] nodes` parallel to the frame stack. Our `Node` is `pub(crate)` and needs to be made constructible via `Node::new()` (currently private).

### Potential Rust Improvements

These are observations for future consideration, not part of the initial port:

- **Iterator trait**: After the Java-style `next()` works, we could optionally provide a Rust `Iterator` adapter for `for term in terms_enum { ... }` and standard iterator combinators.

- **Binary search in equal-length leaf blocks**: Java's `binarySearchTermLeaf()` (lines 662-748) does binary search when all suffixes in a leaf block have equal length. This is already in Java — port it.

## Implementation Steps

### Porting Rules (apply to all steps)

- Read the COMPLETE Java source method/class first
- Port line-by-line preserving control flow, variable names (snake_case), branch structure, loop structure
- Do NOT restructure conditionals, flatten loops, merge branches, reorder operations
- Show Java line ranges -> Rust line ranges side-by-side when done
- If a structural change seems necessary, STOP and ask

### 1. Update `Terms::iterator()` lifetime

- Change `Terms::iterator()` return type from `Box<dyn TermsEnum>` to `Box<dyn TermsEnum + '_>`
- Update `FieldReader`'s `impl Terms` to match
- Update all callers of `Terms::iterator()` to accommodate the lifetime

Test: existing tests pass.

**Review and commit checkpoint.**

### 2. `SegmentTermsEnumFrame` struct and `loadBlock()`

Add `SegmentTermsEnumFrame` (either in `segment_terms_enum.rs` or a new `segment_terms_enum_frame.rs`).

Port from Java `SegmentTermsEnumFrame`:
- Struct fields (lines 31-101) — frame owns `Vec<u8>` buffers for suffix, suffix length, stats, and metadata bytes
- `loadBlock()` (lines 163-258) — reads sections from `IndexInput` into owned buffers
- `rewind()` (lines 260-307)
- `setFloorData()` (lines 112-123) — stores floor data in owned buffer
- `getTermBlockOrd()` (lines 125-127)
- `loadNextFloorBlock()` (lines 129-137)

Expose `Node::new()` as `pub(crate)` in `trie_reader.rs`.

Test: verify `loadBlock()` correctly parses a block written by the blocktree writer.

**Review and commit checkpoint.**

### 3. Frame iteration methods: `next()`, `nextLeaf()`, `nextNonLeaf()`

Port from Java `SegmentTermsEnumFrame`:
- `next()` (lines 310-317)
- `nextLeaf()` (lines 319-331)
- `nextNonLeaf()` (lines 333-375)

These methods write into the shared term buffer and return whether the entry is a sub-block.

Test: unit tests that load a block and iterate entries, verifying term bytes and sub-block detection.

**Review and commit checkpoint.**

### 4. Lazy metadata decoding: `decodeMetaData()`

Port from Java `SegmentTermsEnumFrame`:
- `decodeMetaData()` (lines 453-501)

This replaces the current `decode_term_state()` which always decodes from block start. The new version tracks `metaDataUpto` and decodes incrementally.

Test: verify `docFreq()` and `totalTermFreq()` return correct values after positioning via `next()`.

**Review and commit checkpoint.**

### 5. `scanToFloorFrame()` and `scanToSubBlock()`

Port from Java `SegmentTermsEnumFrame`:
- `scanToFloorFrame()` (lines 380-451)
- `scanToSubBlock()` (lines 517-545)

These are needed by both `next()` (pop path) and `seekExact()`.

Test: verify iteration works with floor blocks (indexes with many terms sharing a prefix).

**Review and commit checkpoint.**

### 6. Restructure `SegmentTermsEnum` with frame stack and implement `next()`

Replace the flat `SegmentTermsEnum` struct with the frame-stack version. The new struct owns `IndexInput` handles for `.tim` and `.tip` data. Port:
- Constructor (Java lines 60-77) — takes `tim_data: &'a [u8]`, `tip_data: &'a [u8]`
- `getFrame()` / `getNode()` (Java lines 162-188)
- `pushFrame()` — both overloads (Java lines 191-239)
- `next()` (Java lines 896-980)
- `term()`, `docFreq()`, `totalTermFreq()`, `termState()` — updated to use `currentFrame.decodeMetaData()` (Java lines 982-1059)

Java's `initIndexInput()` (lazy clone of `.tim` handle) is not needed — we borrow the data upfront.

Tests:
- Iterate all terms in single-block index (3 terms)
- Iterate all terms in multi-block index (100+ terms)
- Iterate terms with floor blocks
- Verify lexicographic order
- Verify doc_freq/total_term_freq after next()
- Verify next() returns None after exhaustion
- Re-iteration after exhaustion

**Review and commit checkpoint.**

### 7. Refactor `seekExact()` to use frames

Port `prepareSeekExact()` (Java lines 253-516) to use the frame-based architecture. Remove the flat `seek_exact_in_block()` and `scan_block()` functions.

Port from Java `SegmentTermsEnumFrame`:
- `scanToTerm()` (lines 548-558)
- `scanToTermLeaf()` (lines 567-657)
- `binarySearchTermLeaf()` (lines 662-748)
- `scanToTermNonLeaf()` (lines 752-871)
- `fillTerm()` (lines 873-878)

All existing `seek_exact` tests must continue to pass. Additional tests:
- `seek_exact` followed by `next()` continues from correct position
- Interleaved `seek_exact` and `next()` calls
- `seek_exact_with_state` followed by `next()`

**Review and commit checkpoint.**

### 8. Implement `seekCeil()`

Port `seekCeil()` (Java lines 530-769). This uses the same frame infrastructure and calls `scanToTerm(target, false)`.

Tests:
- `seek_ceil` for exact match returns `Found`
- `seek_ceil` for prefix match returns `NotFound` with correct ceiling term
- `seek_ceil` past all terms returns `End`

**Review and commit checkpoint.**

### 9. Golden summary term count validation

Add total term count per field to `generate_summary` output. This exercises `next()` in a real E2E scenario.

- Add `termCountVerified` field to `FieldSummary` in `src/bin/generate_summary.rs` — iterate all terms via `next()` and count them, compare against `Terms::size()`
- Add matching field to Java `GenerateIndexSummary.java`
- Regenerate `testdata/golden-summary.json`
- Verify Rust and Java produce identical output

**Review and commit checkpoint.**

### 10. Read-path types in prelude

Add read-path types to `src/prelude.rs`:
- `DirectoryReader`
- `LeafReaderContext`
- `Terms` trait
- `TermsEnum` trait
- Any other types needed for the public read API

**Review and commit checkpoint.**

## Verification

After all steps:
- `cargo test` — all existing + new unit tests pass
- `cargo clippy --all-targets` — no warnings
- `cargo fmt` — formatted
- `./tests/e2e_all.sh` — golden summary matches with term count field

## Open Questions

- **File organization**: Should `SegmentTermsEnumFrame` live in its own file (`segment_terms_enum_frame.rs`) or inline in `segment_terms_enum.rs`? Java uses a separate file (879 lines). Either way works; separate file matches Java's structure.
- **`postings()` and `impacts()` on TermsEnum**: Java's `SegmentTermsEnum` has these methods (lines 1005-1028) that call `postingsReader.postings(fieldInfo, state, reuse, flags)`. Currently not on the Rust `TermsEnum` trait due to ownership issues noted in the trait definition. Deferring — not needed for term iteration.
