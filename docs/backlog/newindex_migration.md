# New Indexing Pipeline Migration

## Goal

Replace the current Bearing indexing code (`src/index/`) with the new `src/newindex/` pipeline. The new pipeline is designed around Rust ownership and borrowing rather than mirroring Lucene's class hierarchy.

## Not a Lucene 1-1 Port

The `src/newindex/` code is explicitly and intentionally **not** a line-by-line port of Lucene. The 1-1 porting approach used in `src/index/` did not translate well to Rust's ownership model and produced code that was difficult to maintain and reason about.

The new pipeline uses Lucene as a reference in two ways:
- **On-disk format compatibility is required** — Rust-written indexes must be readable by Java Lucene. Codec output must be byte-compatible.
- **Lucene's logic informs design decisions** — we understand *what* Lucene does and *why*, then design a Rust-native solution that achieves the same result.

The architecture, data flow, ownership model, and trait hierarchy are original to this project. The porting rules in the top-level `CLAUDE.md` do not apply to `src/newindex/`.

## Constraints

- **Do not modify `src/index/`** — the current indexing path must continue to work throughout migration. All existing tests and e2e scripts must keep passing.
- **Reuse existing code where it fits** — codec writers, encoding utilities, data structures in `src/codecs/`, `src/util/`, `src/store/`, and `src/analysis/` are fair game. Use thin adapters to bridge interface differences between `src/index/` types and `src/newindex/` traits.
- **Work on a branch** — the new pipeline evolves on `index-rebuild` (or similar). Merge to main when milestones are stable. Back up and rethink if the model doesn't hold.
- **Parallel e2e tests** — new e2e scripts validate the new pipeline without replacing existing ones. Both run in CI. When migration is complete, the old scripts are retired.
- **Accept and track temporary debt** — thin adapters, duplicate test infrastructure, and parallel code paths are expected during migration. Clean up happens after the switch, not during. Debt is tracked in two ways:
  - **In code:** `// DEBT: description` comments mark temporary code that must be cleaned up after switchover. These are greppable (`grep -r "// DEBT:"`) for easy discovery.
  - **In docs:** `docs/backlog/newindex_debt.md` catalogs larger systemic debt (adapter layers, duplicate scripts, parallel code paths) with context on when each item can be resolved.

## Phases

### Phase 0: First Real Index

Build a minimal index through the new pipeline that Java Lucene can open and read. This is the proof that the architecture produces correct output end-to-end.

**Target index:** One segment, stored-only fields. The simplest thing Java's `VerifyIndex` can validate.

**Work:**
- Adapter for existing `Directory` (`src/store/`) to `newindex::Directory` trait
- `IdGenerator` implementation (wrap `src/util/string_helper::random_id`)
- `FieldInfoRegistry` implementation (field number assignment, metadata tracking)
- `StoredFieldsConsumer` — a `FieldConsumer` that adapts to the existing `Lucene90CompressingStoredFieldsWriter`
- `FieldInfosConsumer` — a `FieldConsumer` registered last, writes `.fnm` via existing codec
- Segment info (`.si`) writing in `SegmentWorker::flush()` via existing codec
- `SegmentInfos::commit()` — writes `segments_N` via existing codec
- `radix_fmt` implementation (or adapter to existing `index_file_names::radix36`)
- `WorkerFactory` implementation wiring everything together
- New binary or integration test that indexes a few documents with stored fields
- New e2e script: Java `VerifyIndex` reads the index successfully
- Single-threaded only; no mid-stream flush needed

**Validates:** The full lifecycle works end-to-end — document → channel → worker → consumer → codec → directory → Java-readable index.

### Phase 1: Multi-Segment and Multi-Thread

Still using only stored fields and field infos consumers. Prove that the coordinator, channel, and worker lifecycle handle multiple segments and concurrent threads correctly.

**Work:**
- Mid-stream flush: segment ID assignment for replacement workers (thread needs to mint new IDs from the factory)
- `max_buffered_docs` triggering mid-stream flush → worker consumed, replacement created, thread continues
- Multiple worker threads producing independent segments
- E2e validation: Java reads a multi-segment index with multiple stored-field-only segments

**Validates:** Worker disposal and replacement, channel distribution across threads, coordinator collecting segments from multiple workers.

### Phase 2: Compound Files

Still stored fields only. Add compound file packaging so both formats are proven before adding more complex consumers.

**Work:**
- Compound file packaging in the coordinator (after collecting flushed segments, before writing commit point)
- Adapter to existing compound file writer
- E2e validation: Java reads both compound and non-compound indexes

**Validates:** Post-flush file packaging, compound format correctness with the new pipeline.

### Phase 3: Text Fields (Postings + Norms)

Add `TextField` support. This exercises the core of the indexing pipeline: tokenization, term accumulation in pools, postings encoding, and norms computation.

**Work:**
- Populate `SegmentAccumulator` with `ByteBlockPool`, `IntBlockPool`
- `PostingsConsumer` — a `FieldConsumer` that accumulates terms/postings in pools, flushes via existing postings codec
- `NormsConsumer` — a `FieldConsumer` that computes norms in `finish_field`, flushes via existing norms codec
- Analyzer adapter bridging `newindex::Analyzer` to existing `src/analysis/` tokenizers
- Field invert state tracked in `SegmentAccumulator` (token count, unique terms, etc.)
- Wire the analyzer into the `SegmentWorker` token loop (replace the `b""` stub)
- E2e validation: Java reads postings, terms, and norms from a Rust-written index

**Validates:** Token flow through consumers, shared pool accumulation, norms→postings flush ordering.

### Phase 4: Remaining Field Types

Incrementally add consumers for the remaining field types. Each one is a `FieldConsumer` implementation adapting to the existing codec writer.

- `DocValuesConsumer` — all five types (NUMERIC, BINARY, SORTED, SORTED_SET, SORTED_NUMERIC)
- `PointsConsumer` — BKD tree writing
- `TermVectorsConsumer` — per-document term vector writing
- `StringField`, `KeywordField`, `IntField`, `FloatField`, `DoubleField`, `LongField`, `LatLonPoint`, `FeatureField`, range fields
- Expand `FieldType` and `FieldValue` to cover all field configurations

Each field type addition gets its own e2e validation pass via `VerifyIndex`.

### Phase 5: RAM-Based Flush Control

- RAM-based flush signaling via `SegmentAccumulator` memory tracking
- Stall control when total RAM exceeds limits
- E2e validation with RAM-driven flushing

### Phase 6: Feature Parity E2E

Full cross-validation against the existing indexing path.

- New e2e script that mirrors `e2e_all.sh` but uses the new pipeline binary
- Golden summary comparison: new pipeline vs existing pipeline vs Java
- Impact verification (`VerifyImpacts`)
- Performance comparison: new pipeline vs existing pipeline

### Phase 7: Switchover

Replace the old pipeline with the new one.

- New pipeline becomes the default in `indexfiles` binary
- Remove adapter layers — update existing codec/utility code to fit `newindex` interfaces directly
- Retire `src/index/` and old e2e scripts
- Update `CLAUDE.md` and documentation

## Adapter Strategy

During migration, thin adapters bridge `src/newindex/` traits to existing code:

| newindex trait | Existing code | Adapter |
|---|---|---|
| `newindex::Directory` | `store::Directory` | Wrapper delegating to existing methods |
| `newindex::Analyzer` | `analysis::Analyzer` | Wrapper adapting streaming API |
| `FieldConsumer` (stored) | `Lucene90CompressingStoredFieldsWriter` | Consumer holding a codec writer |
| `FieldConsumer` (postings) | `Lucene103PostingsWriter` + `BlockTreeTermsWriter` | Consumer driving the existing codec |
| `FieldConsumer` (norms) | `Lucene90NormsConsumer` | Consumer wrapping the codec |

These adapters exist temporarily. After switchover (Phase 5), the underlying code is updated to align with `newindex` interfaces and the adapters are removed.

## What This Does NOT Cover

- Deletes, updates, merging (require read path)
- KNN vectors, shape fields
- Index-time sorting
- Near-real-time (NRT) search
- Per-field codec overrides

These remain in the existing backlog and are orthogonal to the pipeline migration.
