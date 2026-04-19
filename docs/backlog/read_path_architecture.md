# Read Path Architecture: Single Struct, Borrowed Cursor, Shared File Ownership

The current read path is layered around traits (`DataInput`, `IndexInput`, `RandomAccessInput`) with multiple implementations behind `Box<dyn IndexInput>` and `Arc<Mmap>` inside each input. This document describes a target architecture that collapses the read-path types to a single concrete struct backed by `Cursor<&[u8]>`, with file ownership moved out of the input layer and into the codec readers that actually know which files they need.

This is a companion to [cursor_read_path.md](cursor_read_path.md). That doc describes the immediate trigger (VInt optimization). This doc describes the destination shape.

## Motivation

Three observations drove the design:

1. **All production read-path inputs are byte-slice-backed.** `MmapIndexInput`, `ByteSliceIndexInput`, and `SliceReader` are all hand-rolled cursors over `&[u8]` with manual `pos` tracking. `FSIndexInput` (file handle + syscalls) is no longer used on any production hot path. There is no implementation that requires the trait abstraction.

2. **The `Read` supertrait blocks the most common micro-optimization.** `DataInput: Read` forces encoding functions like `read_vint` to take `&mut dyn Read`, which has no way to peek at the underlying slice. Every multi-byte VInt requires up to five 1-byte vtable calls. `Cursor<&[u8]>` implements `BufRead`, whose `fill_buf()` returns a `&[u8]` view into the remaining bytes — enough to parse a VInt directly from the slice in a single call.

3. **File ownership belongs with the codec that uses those files.** Mmap is virtual memory; holding mmaps open is cheap, but the lifecycle of "which files stay mapped" needs to be explicit. Each codec reader knows statically which component files it needs (postings, positions, payloads, terms index, etc.); holding those as typed `FileBacking` fields keeps lookups zero-cost and lets the type system enforce which files exist. Sharing across readers or a future cache layer is layered on top via `Arc` wrapping, not by introducing a separate owner type.

## Target Architecture

### Layer 1: File ownership

```rust
enum FileBacking {
    Mmap(Mmap),
    Owned(Vec<u8>),     // for MemoryDirectory / tests
}
```

`FileBacking` owns the bytes of one file. There is no segment-wide container; each codec reader holds exactly the `FileBacking`s it needs as typed fields (see Layer 3). Sharing across readers is handled by wrapping those fields in `Arc` when and where it's needed, not by introducing an intermediate owner type.

### Layer 2: Single read-path struct

```rust
struct IndexInput<'a> {
    name: String,
    cursor: Cursor<&'a [u8]>,
}
```

One concrete struct. No traits. All read methods (VInt, VLong, strings, fixed-width ints in either byte order, absolute-position reads) are inherent methods on this struct. Constructed cheaply from any `&[u8]` borrowed from a `FileBacking`.

### Layer 3: Codec readers

Each codec reader owns its component files directly as typed `FileBacking` fields:

```rust
struct PostingsReader {
    // metadata read eagerly at open time
    ...
    docs: FileBacking,
    pos: FileBacking,
    pay: FileBacking,
}

impl PostingsReader {
    fn open_docs(&self) -> IndexInput<'_> {
        IndexInput::new("docs", self.docs.as_bytes())
    }
}
```

Codec readers store lightweight metadata (headers, per-field offsets, stats) read at open time. Actual data is read on demand during queries by constructing an `IndexInput<'_>` over the relevant `FileBacking`'s bytes. Codec readers do not hold cursor state.

### Layer 4: Per-query iterators

Iterators (`BlockPostingsEnum`, term enumerators, etc.) hold `IndexInput<'_>` instances borrowed from a codec reader's `FileBacking` fields. The lifetime says "this iterator cannot outlive its codec reader." Cursor state lives here, not in the codec reader, because cursor state is per-query.

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

Ownership is layered from individual files up to the reader tree:

- **Per-iterator:** `IndexInput<'_>` dropped at iteration end; no impact on file lifetime
- **Per-codec, multi-query:** each codec reader owns its `FileBacking` fields; two queries on the same codec reader share them via `&reader` (immutable borrow), each getting its own `IndexInput<'_>` with independent cursor state
- **Cross-segment, cross-query:** `SegmentReader` holds `Arc<CodecReader>` for each of its codec readers; `IndexReader` holds its segment readers. `IndexReader.reopen()` semantics fall out naturally — in-flight queries keep their `Arc<CodecReader>` clones alive until they finish
- **Future cache layer:** a `FileCache` can own `Arc<FileBacking>`s; codec readers change their field type from `FileBacking` to `Arc<FileBacking>` and clone from the cache at construction. Eviction = drop the cache's strong references; files stay alive while any reader holds them. This is a localized field-type change, not a re-architecture

Arc is not used on the hot path; it sits at the segment-reader layer (around codec readers) and optionally at the field level (around `FileBacking`s) if/when a cache is introduced.

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

- **Lifetime-only ownership (no Arc).** Achievable as a follow-up if profiling shows Arc clone/drop cost matters for codec reader construction. Not the starting target. Arc is the right semantic for the reopen and cache lifecycles described above.
- **A file cache implementation.** Architecturally enabled by this design but not in scope. Wrapping codec-reader `FileBacking` fields in `Arc` at cache-introduction time is the prerequisite.
- **Removal of `Directory` as a trait.** Production and in-memory directories have distinct management semantics. The trait stays.
- **Changes to the write path.** `DataOutput`, `IndexOutput`, and write-side encoding remain unchanged. This document is read-path only.
