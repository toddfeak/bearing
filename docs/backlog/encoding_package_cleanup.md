# Encoding package cleanup

## 1. Module boundaries — for_util and packed.rs

Revisit whether `for_util.rs` (FOR/PFOR/ForDelta) and `packed.rs`
(DirectWriter, DirectMonotonicWriter, BlockPackedWriter) belong in
`src/encoding/` rather than their current locations under `codecs/` and
`util/`.

These are pure encoding algorithms that semantically belong with varint,
zigzag, and lz4. The blocker is that they currently depend on
`DataInput`/`DataOutput` instead of `io::Read`/`io::Write`. A partial
migration is fine — move what works cleanly with `Read`/`Write`, leave
what truly needs `IndexInput`/`IndexOutput`.

### for_util.rs

Currently in `codecs/lucene103/`. Encode uses `DataOutput`, decode uses
`DataInput`. Both map straightforwardly to `io::Write`/`io::Read`
(write_le_int → write_all with to_le_bytes, etc.).

### packed.rs

Currently in `util/`. `DirectWriter`, `DirectMonotonicWriter`, and
`BlockPackedWriter` store `&mut dyn IndexOutput` and use `file_pointer()`
for offset computation.

Verified feasible: `file_pointer()` is only used in
`DirectMonotonicWriter::finish` (2 calls). Fix: have
`DirectWriter::finish` return `usize` (bytes written), and
`DirectMonotonicWriter` tracks `data_offset` internally. All `DataOutput`
methods map to `io::Write::write_all` with `to_le_bytes()`.

Scope: ~40 call sites across 4 codec files (doc_values.rs,
stored_fields.rs, term_vectors.rs) plus packed.rs tests need
`DataOutputWriter` wrapping. Variable patterns vary, so needs manual
per-file updates.

Status: `numeric_utils.rs` already moved. `packed.rs` is the last
remaining encoding item in `util/`.

## 2. Allocation efficiency

Audit encode/decode functions for allocation efficiency. Where the output
size is known upfront, return `Box<[u8]>` instead of `Vec<u8>` to avoid
over-allocation and signal immutability.

Candidates:
- `lowercase_ascii::decompress_from_reader` — output size is `len` parameter
- `lz4::decompress` / `decompress_from_reader` / `decompress_with_prefix` — output size is `dest_len`
- Compress functions — output size unknown, `Vec` is appropriate
