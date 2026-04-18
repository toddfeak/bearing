# Cursor-Based Read Path

Replace `Read` with `std::io::Cursor` as the foundation for `DataInput` on the read/query path. This enables optimistic multi-byte reads for variable-length integers and simplifies the I/O abstraction stack.

## Motivation

Profiling term iteration shows that `read_vint` is a significant per-term cost. The root problem:

**Byte-at-a-time VInt reads**: `read_vint` must read one byte, check the continuation bit, then decide whether to read another. With `Read` as the underlying interface, there is no way to optimistically read up to 5 bytes and then roll back the position if fewer were needed — `Read` is forward-only with no seek. So we're forced into a loop of 1-byte `read_exact` calls (up to 5 per VInt). `Cursor` implements `Seek`, so we can read 5 bytes, parse the VInt from a local buffer, then seek back to the actual end position — one I/O call instead of up to five.

This was measured during the terms iteration work. Attempts to optimize `read_vint` without changing the underlying interface (slice-based direct reads, `read` vs `read_exact`) yielded only ~5% improvement and weren't worth the added complexity.

## Key Insight

All production read-path `IndexInput` implementations are backed by contiguous byte slices:
- `MmapIndexInput` — memory-mapped file (`&[u8]`)
- `ByteSliceIndexInput` — owned `Vec<u8>` / borrowed `&[u8]`
- `MemoryDirectory` — in-memory `Vec<u8>`

`FSIndexInput` (file handle + syscall reads) is only used when `FSDirectory::open_with_file_handles` is called, which is only in `indexfiles.rs` for the write path. The write path's only read operation is `segment_infos.rs` reading a few hundred bytes of segment metadata — trivial to mmap.

## Implementation Steps

### 1. Switch `indexfiles.rs` from `open_with_file_handles` to `FSDirectory::open`

The indexing binary currently uses `FSDirectory::open_with_file_handles`, which creates `FSIndexInput` for reads. Switch to `FSDirectory::open` (mmap). The only read during indexing is segment info (a few hundred bytes) — no memory concern.

This removes the last production caller of `FSIndexInput`. It can remain for its own unit tests but is no longer on any hot path.

### 2. Use `Cursor` internally in `IndexInput` implementations

`MmapIndexInput` and `ByteSliceIndexInput` should wrap their byte data in a `Cursor<&[u8]>` internally and delegate their `DataInput` methods through it. This keeps `IndexInput` as the public interface while the underlying I/O goes through `Cursor`. Callers continue to use `IndexInput` methods, which now benefit from `Cursor` semantics internally.

### 3. Update `DataInput` to be based on `Cursor` instead of `Read`

`DataInput` currently extends `Read`. Change it so that implementations use `Cursor` for positioned access. The encoding layer (`read_vint`, `read_vlong`, etc. in `encoding/varint.rs`, `encoding/read_encoding.rs`) currently takes `&mut dyn Read`. Update to work with `Cursor`-based access instead.

### 4. Optimize VInt/VLong reads for `Cursor`

With `Cursor` as the interface, `read_vint` can optimistically read up to 5 bytes, parse the VInt from a local buffer, and seek back to the correct position. One read call and one seek instead of up to five individual byte reads.

### 5. Evaluate `SliceReader` removal

`SliceReader` is a hand-rolled cursor over `&[u8]` with `DataInput` methods. If `Cursor<&[u8]>` with the updated encoding layer provides the same functionality, `SliceReader` may become redundant. Evaluate whether it can be replaced entirely or if any specialized methods (`read_slice` for zero-copy access) need to be preserved.

### 6. Update frame iteration to use `Cursor`

`SegmentTermsEnumFrame` methods (`next_leaf`, `next_non_leaf`, `scan_to_term_*`, `decode_meta_data`) currently create `SliceReader` instances per call. Replace with cursor-based access to the frame's owned buffers.

## Verification

- `cargo test`, `cargo clippy --all-targets`, `cargo fmt`
- `./tests/e2e_all.sh` — all e2e tests pass
- Query performance: `./tests/compare_query_perf.sh -docs /tmp/gutenberg-large-500` — should show improvement over current baseline (~57 us/query, 4.2x vs Java)
- Term iteration: modified `listterms` (iteration-only mode, no BTreeSet/output) against a large index — should show improvement over current baseline (~1.0s for 33M terms)
