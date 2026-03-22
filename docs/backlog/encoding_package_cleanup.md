# Encoding package cleanup

## 1. Module boundaries — packed.rs

`packed.rs` remains in `util/` with `DirectWriter`, `DirectMonotonicWriter`,
and `BlockPackedWriter` depending on `IndexOutput::file_pointer()`.

Verified feasible: `file_pointer()` is only used in
`DirectMonotonicWriter::finish` (2 calls). Fix: have
`DirectWriter::finish` return `usize` (bytes written), and
`DirectMonotonicWriter` tracks `data_offset` internally. All `DataOutput`
methods map to `io::Write::write_all` with `to_le_bytes()`.

Scope: ~40 call sites across 4 codec files (doc_values.rs,
stored_fields.rs, term_vectors.rs) plus packed.rs tests need
`DataOutputWriter` wrapping. Variable patterns vary, so needs manual
per-file updates.

## 2. Allocation efficiency

Audit encode/decode functions for allocation efficiency. Where the output
size is known upfront, return `Box<[u8]>` instead of `Vec<u8>` to avoid
over-allocation and signal immutability.

Candidates:
- `lowercase_ascii::decompress_from_reader` — output size is `len` parameter
- `lz4::decompress` / `decompress_from_reader` / `decompress_with_prefix` — output size is `dest_len`
- Compress functions — output size unknown, `Vec` is appropriate
