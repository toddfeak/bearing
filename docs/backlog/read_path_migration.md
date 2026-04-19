# Read Path Migration Strategy

How to transition from the current trait-based read path to the target architecture in [read_path_architecture.md](read_path_architecture.md) without breaking the codebase mid-migration. This document is about strategy and sequencing, not the destination shape (covered by the architecture doc) or the immediate VInt optimization trigger (covered by [cursor_read_path.md](cursor_read_path.md)).

## Approach: Parallel Build, Module Isolation, Per-Codec Migration

Build the new read-path types in a separate module alongside the old ones. Migrate codec readers one at a time. Delete the old types in a final cleanup commit. Every commit in between compiles and passes tests.

The change is large enough that a single big-bang rewrite is risky and unreviewable. Parallel build trades temporary code duplication for the ability to pause, verify, and roll back at any point.

## Module Isolation

Place the new types under `src/store2/`. The new `IndexInput<'a>` struct and the existing `IndexInput` trait collide on the name; module qualification keeps them disambiguated during the overlap period.

If module qualification gets confusing in practice, give the new struct a temporary distinct name (`FileView<'a>`, `ReadCursor<'a>`, etc.) and rename it to `IndexInput` in the final cleanup commit, after the trait is deleted.

## Visibility During Migration

Mark all types and methods in `src/store2/` as `pub` from the start. While building out the parallel module, new types are added before they are referenced from outside the module — without `pub`, they would generate continuous `dead_code` warnings (or require per-item `#[expect(dead_code)]` annotations on every new item).

Treat visibility as an audit step in the cleanup commit, not during build-up. Once the migration completes and all callers are wired through, tighten each type and method to the narrowest appropriate visibility (`pub`, `pub(crate)`, `pub(super)`, or private) based on actual external usage.

## What Builds in Parallel Without Touching Old Code

These are all new types in the new module — no conflict with the existing read path:

- `FileBacking` enum (`Mmap` or `Owned`)
- The new `IndexInput<'a>` struct with all inherent read methods
- Free function for checksum verification
- Unit tests for all of the above

No generic segment-wide container is introduced; each codec reader will hold its own typed `FileBacking` fields at migration time (see architecture doc, Layer 3).

This entire layer can ship and pass tests before any codec reader knows it exists.

## What Requires Touching Shared Code

Two shared touchpoints, both additive:

- **`Directory` trait gains a new method** (e.g., `open_file -> FileBacking`) alongside the existing `open_input -> Box<dyn IndexInput>`. Production directories implement both. Old method stays until the final cleanup commit.
- **`SegmentReader` constructs codec readers either way.** During migration, some codec readers are constructed via the old path, some via the new. Both paths coexist in `SegmentReader::open_from_directory`.

`CompoundDirectory` is the awkward case: it currently slices into a parent `Box<dyn IndexInput>`. It needs to support the new `open_file` method by slicing into the parent's bytes. Either implement it in parallel (both methods working independently) or gate compound migration until the underlying directory layer migrates first.

## Per-Codec Migration

Each codec reader migrates in its own commit. The pattern:

1. `SegmentReader` constructs that codec reader the new way (calls `open_file`, builds new `IndexInput`)
2. The codec reader's struct fields and method signatures switch to the new types
3. The codec reader's tests update to construct the new input type
4. e2e still passes because every other codec reader is unchanged

Suggested order (simplest first to validate the pattern, complex last):

1. ~~`points_reader`~~ — migrated (commit b9ed958)
2. ~~`stored_fields_reader`~~ — migrated (this commit); `term_vectors_reader` partially migrated (its `.tvm` meta path only, since it shares `FieldsIndexReader` with stored_fields); `DirectReader` / `DirectMonotonicReader` in `packed_readers.rs` also moved to `store2::IndexInput`
3. `norms_reader`
4. `doc_values_producer` and `term_vectors_reader` (full migration of the `.tvd`/`.tvx` paths)
5. `postings_reader`
6. `blocktree_reader` and `segment_terms_enum` (deepest call chains, most iterators)
7. `compound_reader` (Directory implementation, may need its own ordering consideration)

## Final Cleanup Commit

Once every codec reader is migrated:

- Delete old `DataInput`, `IndexInput`, `RandomAccessInput` traits
- Delete `MmapIndexInput`, `ByteSliceIndexInput`, `FSIndexInput`, `SliceReader`, `ChecksumIndexInput`
- Delete the old `Directory::open_input` method
- Delete the old `ReadEncoding` blanket trait if no longer used
- Move `verify_checksum` from `src/store2/checksum.rs` into `src/codecs/codec_util.rs` alongside `write_footer`; drop the local `FOOTER_MAGIC` / `FOOTER_LENGTH` copies in favor of the existing `codec_util` constants. The footer format is codec-specific wire format, not generic byte I/O, and belongs next to its writer.
- Move new module contents to `src/store/`, rename types if temporary names were used
- Update imports across the codebase

## Cost

Temporary code duplication during the migration window. Two implementations of "byte slice with position tracking" coexist. The codebase is larger and slightly more confusing while the migration is in flight.

## Where This Plan Could Fail

The plan assumes shared touchpoints are additive. If the new types require a change to `Directory` or `SegmentReader` that breaks old codec readers, parallel-build falls apart and we have to choose between halting or going big-bang. From inspection of the existing code, the only shared touchpoints look additive (new method on Directory, new branches in SegmentReader). The first codec migration is the cheapest way to validate this assumption — if it forces a breaking change to `Directory`, the strategy needs revisiting before continuing.

## Success Criteria

The migration delivers value if it produces measurable improvement against current baselines:

- **Query performance:** baseline ~57 us/query (4.2x vs Java) measured by `./tests/compare_query_perf.sh -docs /tmp/gutenberg-large-500`. Target is meaningful improvement.
- **Term iteration:** baseline ~1.0s for 33M terms measured via `listterms` (iteration-only mode, no `BTreeSet`/output) against a large index. Target is meaningful improvement.

The VInt optimization unlocked by `Cursor`/`BufRead` is the primary expected driver of improvement, with secondary contributions from removed dynamic dispatch and removed Arc churn on slice operations.
