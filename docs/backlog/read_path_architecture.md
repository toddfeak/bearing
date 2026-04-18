# Read Path Architecture: Single Struct, Borrowed Cursor, Shared File Ownership

The current read path is layered around traits (`DataInput`, `IndexInput`, `RandomAccessInput`) with multiple implementations behind `Box<dyn IndexInput>` and `Arc<Mmap>` inside each input. This document describes a target architecture that collapses the read-path types to a single concrete struct backed by `Cursor<&[u8]>`, with file ownership lifted out of the input layer and into the segment level where the natural cache boundary lives.

This is a companion to [cursor_read_path.md](cursor_read_path.md). That doc describes the immediate trigger (VInt optimization). This doc describes the destination shape.

## Motivation

Three observations drove the design:

1. **All production read-path inputs are byte-slice-backed.** `MmapIndexInput`, `ByteSliceIndexInput`, and `SliceReader` are all hand-rolled cursors over `&[u8]` with manual `pos` tracking. `FSIndexInput` (file handle + syscalls) is no longer used on any production hot path. There is no implementation that requires the trait abstraction.

2. **The `Read` supertrait blocks the most common micro-optimization.** `DataInput: Read` forces encoding functions like `read_vint` to take `&mut dyn Read`, which has no way to peek at the underlying slice. Every multi-byte VInt requires up to five 1-byte vtable calls. `Cursor<&[u8]>` implements `BufRead`, whose `fill_buf()` returns a `&[u8]` view into the remaining bytes — enough to parse a VInt directly from the slice in a single call.

3. **Shared file ownership is what enables cross-query caching.** Mmap is virtual memory; holding mmaps open is cheap, but the lifecycle of "which files stay mapped" needs to be explicit. The natural place for that ownership is the segment level, with shared (Arc) semantics so a future cache layer can own files independently of any one reader.

## Target Architecture

### Layer 1: File ownership

```rust
enum FileBacking {
    Mmap(Mmap),
    Owned(Vec<u8>),     // for MemoryDirectory / tests
}

struct SegmentFiles {
    docs: FileBacking,
    pos: FileBacking,
    pay: FileBacking,
    terms_index: FileBacking,
    // ... one per codec component file
}
```

`SegmentFiles` is the unit of file ownership. One per segment. Held via `Arc<SegmentFiles>` so that segment readers, codec readers, and any future cache layer can share without lifetime contortions.

### Layer 2: Single read-path struct

```rust
struct IndexInput<'a> {
    name: String,
    cursor: Cursor<&'a [u8]>,
}
```

One concrete struct. No traits. All read methods (VInt, VLong, strings, fixed-width ints in either byte order, absolute-position reads) are inherent methods on this struct. Constructed cheaply from any `&[u8]` borrowed from a `FileBacking`.

### Layer 3: Codec readers

Codec readers (`PostingsReader`, `BlockTreeTermsReader`, etc.) hold `Arc<SegmentFiles>`. They store metadata (read at open time) and per-field offsets. They do not hold cursor state.

### Layer 4: Per-query iterators

Iterators (`BlockPostingsEnum`, term enumerators, etc.) hold `IndexInput<'_>` instances borrowed from the codec reader's `Arc<SegmentFiles>`. The lifetime says "this iterator cannot outlive its codec reader." Cursor state lives here, not in the codec reader, because cursor state is per-query.

## What This Gains

### Zero-copy hot path

`Cursor<&[u8]>::fill_buf()` exposes the remaining bytes as a borrowed slice. This means:

- `read_vint` parses from the borrowed slice in one call instead of looping byte-by-byte
- Bulk reads (term bytes, posting block bytes, payload blobs) become slice references instead of `copy_from_slice` into a caller buffer
- Dynamic dispatch through `Box<dyn IndexInput>` is gone; methods inline

The honest scope: copies that are intrinsic to query semantics remain (LZ4/zstd decompression, packed-int decode into i32 arrays, result aggregation). What disappears is incidental copying — the read-into-buffer-then-parse pattern that exists only because of the API shape.

### Process memory characterization

With this architecture, query-time RSS = mmap pages currently faulted in + decompressed blocks held by readers + per-query aggregation state. Nothing else. No shadow buffer for VInts, no Arc churn on slice operations, no copied byte pools.

### Cache lifecycle is explicit

`Arc<SegmentFiles>` is the ownership unit that maps to caching:

- **Per-iterator:** `IndexInput<'_>` dropped at iteration end; no impact on file lifetime
- **Per-segment, multi-query:** `SegmentReader` holds `Arc<SegmentFiles>` for its lifetime; multiple queries on the same segment share the same Arc
- **Cross-segment, cross-query:** `IndexReader` holds segment readers; app keeps `IndexReader` alive across queries
- **Future cache layer:** A `FileCache` can own `Arc<FileBacking>`s and have `SegmentFiles` clone from it. Eviction = drop the cache's strong references; files stay alive while any reader holds them

The Arc is not a workaround. It is the indirection that allows ownership to be split between cache and reader without lifetime contortions, and it supports `IndexReader.reopen()` semantics where mid-flight queries on old segments must keep working while new readers swap in.

## What Goes Away

- `DataInput` trait
- `IndexInput` trait
- `RandomAccessInput` trait
- `MmapIndexInput`, `ByteSliceIndexInput`, `FSIndexInput`, `SliceReader` (collapse into `IndexInput<'a>`)
- `ChecksumIndexInput` (collapses to a free function: `verify_checksum(bytes, expected)`)
- `Box<dyn IndexInput>` everywhere
- `ReadEncoding` blanket trait (methods become inherent on `IndexInput`)
- Manual `pos` tracking inside any read-path type

`Directory` remains a trait (production vs in-memory have genuinely different file-management behavior), but its `open_input` returns `FileBacking` (the owner) rather than a boxed trait object. Constructing an `IndexInput` is the caller's job.

## Departure From Lucene

This is a real departure from Lucene's Java structure, where `DataInput`/`IndexInput`/`RandomAccessInput` are abstract base classes with multiple subclasses. The Java hierarchy exists because Java does not have ergonomic borrowing or first-class lifetimes — abstract classes with virtual dispatch are how you express "any byte source" in that language.

Rust's `&[u8]` + lifetime model expresses the same concept directly: any borrowed byte slice, valid for some lifetime, freely composable. The trait hierarchy is solving a problem we do not have. Collapsing to a single concrete struct is more idiomatic Rust and produces faster code (no virtual dispatch, no Arc on every slice).

This trade is consistent with the project's porting rule: idiomatic Rust types and ownership are allowed; algorithms are not. The algorithms (VInt encoding, block-tree traversal, posting list iteration) remain Lucene-faithful. The I/O abstraction stack does not.

## Explicit Non-Goals

- **Lifetime-only ownership (no Arc).** Achievable as a follow-up if profiling shows Arc clone/drop cost matters for codec reader construction. Not the starting target. Arc is the right semantic for the cache lifecycles described above.
- **A file cache implementation.** Architecturally enabled by this design but not in scope. The `Arc<SegmentFiles>` ownership model is the prerequisite.
- **Removal of `Directory` as a trait.** Production and in-memory directories have distinct management semantics. The trait stays.
- **Changes to the write path.** `DataOutput`, `IndexOutput`, and write-side encoding remain unchanged. This document is read-path only.
