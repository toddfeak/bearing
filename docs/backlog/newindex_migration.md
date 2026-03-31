# New Indexing Pipeline Migration

## Goal

Replace the current Bearing indexing code (`src/index/`) with the new `src/newindex/` pipeline. The new pipeline is designed around Rust ownership and borrowing rather than mirroring Lucene's class hierarchy.

## Not a Lucene 1-1 Port

The `src/newindex/` code is explicitly and intentionally **not** a line-by-line port of Lucene. The 1-1 porting approach used in `src/index/` did not translate well to Rust's ownership model and produced code that was difficult to maintain and reason about.

The new pipeline uses Lucene as a reference in two ways:
- **On-disk format compatibility is required** — Rust-written indexes must be readable by Java Lucene. Codec output must be byte-compatible.
- **Lucene's logic informs design decisions** — we understand *what* Lucene does and *why*, then design a Rust-native solution that achieves the same result.

The architecture, data flow, ownership model, and trait hierarchy are original to this project. The porting rules in the top-level `CLAUDE.md` do not apply to `src/newindex/`.

## Design Principles

- **Build for the next phase, not just the current one.** Each phase's implementation must support the requirements of the following phase without rework. Don't build throwaway scaffolding that will be replaced one or two phases later. If a design decision will block the next phase, solve it now.
- **The coordinator controls worker lifecycle.** The coordinator owns segment identity assignment and worker creation. Worker threads execute policy set by the coordinator — they do not run autonomously. This is required for future flush control (RAM-based triggering, stall control under memory pressure). The `WorkerSource` is the coordinator's delegate for thread-safe worker creation.
- **Keep threading simple.** Use the minimum coordination needed. Shared mutable state should be behind a `Mutex` on rare paths (e.g., worker creation on flush), not on the per-document hot path. No management threads, no complex channel topologies.

## Constraints

- **Do not modify `src/index/`** — the current indexing path must continue to work throughout migration. All existing tests and e2e scripts must keep passing.
- **Reuse existing code where it fits** — codec writers, encoding utilities, data structures in `src/codecs/`, `src/util/`, `src/store/`, and `src/analysis/` are fair game. Use thin adapters to bridge interface differences between `src/index/` types and `src/newindex/` traits.
- **Work on a branch** — the new pipeline evolves on `index-rebuild` (or similar). Merge to main when milestones are stable. Back up and rethink if the model doesn't hold.
- **Parallel e2e tests** — new e2e scripts validate the new pipeline without replacing existing ones. Both run in CI. When migration is complete, the old scripts are retired.
- **Accept and track temporary debt** — thin adapters, duplicate test infrastructure, and parallel code paths are expected during migration. Clean up happens after the switch, not during. Debt is tracked in two ways:
  - **In code:** `// DEBT: description` comments mark temporary code that must be cleaned up after switchover. These are greppable (`grep -r "// DEBT:"`) for easy discovery.
  - **In docs:** `docs/backlog/newindex_debt.md` catalogs larger systemic debt (adapter layers, duplicate scripts, parallel code paths) with context on when each item can be resolved.

## Phases

### Phase 0: First Real Index ✓

**Complete.** One segment, stored-only fields, validated by Java Lucene's `CheckIndex`.

**What was built:**
- `DirectoryAdapter` bridges `store::Directory` → `newindex::Directory`
- `RandomIdGenerator`, `FieldInfoRegistry`, `radix_fmt` / `segment_file_name`
- `newindex/codecs/` sub-package with DEBT copies of stored fields, field infos, segment info, and segments_N writers (no `src/index` imports)
- `StoredFieldsConsumer`, `FieldInfosConsumer` — both `FieldConsumer` implementations
- `SegmentContext` — segment identity passed to consumers at flush time (avoids duplicating directory/name/id across consumers)
- `.si` writing in `SegmentWorker::flush()`, `SegmentInfos::commit()` for `segments_N`
- `DefaultWorkerFactory`, `StandardAnalyzer` (stub with `todo!`)
- Simplified `IndexWriter::new(config, directory)` — hides Arc/SharedDirectory/factory wiring
- Integration tests (`tests/newindex_indexing.rs`), demo binary (`newindex_demo`), e2e script (`e2e_newindex.sh`) with Java `CheckIndex`

**Design changes from original plan:**
- `FieldConsumer::flush` takes `&SegmentContext` — segment identity flows at flush time rather than being stored per-consumer
- `WorkerFactory::create_worker` returns `(SegmentWorker, SegmentContext)`
- Codec writers copied into `newindex/codecs/` with local types instead of importing from `src/index`
- `IndexWriter::new` simplified to 2 parameters (config + directory), factory/id-gen are internal
- Coordinator lifecycle reworked: worker threads use a shared `WorkerSource` for initial and replacement worker creation. Thread loop extracted into a named function. Mid-stream flush creates a replacement worker via `WorkerSource::create_worker()` — no management threads or complex coordination needed.

### Phase 1: Multi-Segment and Multi-Thread ✓

**Complete.** Validated multi-segment and multi-thread indexing with stored fields only.

**What was built:**
- `max_buffered_docs` triggering mid-stream flush → worker consumed, replacement created via `WorkerSource`, thread continues
- Multiple worker threads producing independent segments
- Integration tests for multi-segment (single thread), multi-thread, and combined configurations
- Demo binary CLI flags: `--max-buffered-docs`, `--threads`, `--doc-count`
- E2e validation: Java `CheckIndex` on single-segment, multi-segment, multi-thread, and combined indexes
- Unit tests for `WorkerSource`, `worker_thread_loop`, and `package_compound_segment`

### Phase 2: Compound Files ✓

**Complete.** Compound file packaging validated with stored fields across single and multi-segment configurations.

**What was built:**
- `package_compound_segment()` in coordinator's `shutdown()` — adapts existing `lucene90::compound::write_to()`
- `use_compound_file` config plumbed through `IndexCoordinator`
- Demo binary `--compound` flag
- Integration tests for compound vs non-compound, compound with multi-segment
- E2e validation: Java `CheckIndex` on compound single-segment and compound multi-segment indexes

### Phase 3: Tokenization and Norms ✓

**Complete.** Tokenization and norms validated by Java CheckIndex across single-segment, multi-segment, and multi-thread configurations.

**What was built:**
- `StandardAnalyzer` adapter bridging pull-based `newindex::Analyzer` to push-based `analysis::StandardAnalyzer` via internal buffering
- `FieldType` extended with `tokenized`, `omit_norms` fields + `STORED`, `TEXT_STORED`, `TEXT` constants + `stored_field()`/`text_field()` convenience constructors
- Token loop wired in `SegmentWorker`: checks `field.field_type().tokenized`, extracts field value as reader
- `NormsConsumer` — counts tokens per field, computes SmallFloat-encoded norms, writes `.nvm`/`.nvd` via DEBT norms codec copy
- `.fnm` writer updated for indexed fields: dynamic bits encoding (STORE_TERMVECTOR, OMIT_NORMS, STORE_PAYLOADS), configurable index_options
- Norms codec DEBT copy at `newindex/codecs/norms.rs` with ALL/SPARSE/EMPTY/CONSTANT patterns
- Integration tests reading real docs from `testdata/docs/`, e2e text field scenarios with Java CheckIndex

**Design changes from original plan:**
- Norms data stored in `NormsConsumer` directly, not in `SegmentAccumulator` — no other consumer needs it
- `.fnm` `FieldInfo` does not track `stored` — it's not part of the Lucene94 `.fnm` format (stored-ness is implicit from stored field data)

### Phase 4: Postings ✓

**Complete.** Postings pipeline validated by Java CheckIndex across single-segment, multi-segment, multi-thread, and compound file configurations.

**What was built:**
- `PostingsConsumer` — a `FieldConsumer` that owns two `ByteBlockPool` instances (doc/freq and positions), accumulates terms/postings, flushes via block tree terms writer
- `PerFieldPostings` — per-field term deduplication via `BytesRefHash`, parallel arrays for doc/freq/position tracking, byte-sorted term iteration at flush
- DEBT codec copies at `newindex/codecs/postings_writer.rs` and `newindex/codecs/blocktree_writer.rs` producing `.tim`, `.tip`, `.tmd`, `.doc`, `.pos`, `.psm` files
- Norms-to-postings integration: `PostingsConsumer` reads norms from `SegmentAccumulator` at flush time for competitive impact encoding
- Integration tests with real testdata docs, e2e text field scenarios with Java CheckIndex

**Design changes from original plan:**
- Pools owned by `PostingsConsumer` directly, not stored in `SegmentAccumulator` — pools have per-consumer lifetimes and don't need to be shared. `SegmentAccumulator` stores only cross-consumer metadata (norms).
- No `IntBlockPool` — position deltas written directly to a dedicated `ByteBlockPool` during token processing, doc/freq deltas written at document boundaries. This splits encoding work between the hot path (positions) and document-boundary path (doc/freq).

### Phase 4b: Tooling and Field Refactor ✓

**Complete.** E2e tooling, FieldKind enum refactor, and FieldValue::Reader streaming support.

**What was built:**
- `newindex_demo` binary — DEBT copy of `indexfiles` with `-docs`/`-index` CLI, streaming `BufReader` for contents
- `VerifyNewindex` Java utility — content validation (stored fields, terms, norms, queries)
- `IndexNewindex` Java utility — matching Java baseline indexer with multi-threading
- `compare_newindex_perf.sh` — performance comparison script
- `e2e_newindex.sh` upgraded with real-doc scenarios and `VerifyNewindex` content verification
- `FieldKind` enum replacing `FieldType` + `FieldValue` + `FieldBuilder` — invalid states unrepresentable at compile time
- Streaming tokenization via `FieldKind::into_reader()` unification

### Phase 5a: StringField ✓

**Complete.** Non-tokenized indexed fields (StringField) with DOCS-only postings.

**What was built:**
- `FieldKind::Indexed` and `FieldKind::StoredIndexed` variants for exact-match indexed fields
- `IndexOptions` from `src/document.rs` used as first-class type in `FieldKind`, `PostingsConsumer`, and `PerFieldPostings`
- `PostingsConsumer` handles non-tokenized indexed fields directly in `start_field` — reads the field value and records it as a single term, returns `NoTokens`
- `PerFieldPostings` DOCS-only support via `has_freqs` flag — no freq/position writing
- `FieldKind::is_tokenized()` and `string_value()` methods — segment worker uses properties, not variant matching
- `newindex_demo` updated: `path` and `title` as `StoredIndexed` (StringField)
- Java tooling updated: `StringField` for path/title, title terms verification
- Integration tests for DOCS-only postings and mixed string+text fields

### Phase 5b: DocValuesConsumer

Add doc values support for all five types.

- `DocValuesConsumer` — a new `FieldConsumer` accumulating doc values, flushing via DEBT codec copy
- DEBT codec copy of `Lucene90DocValuesConsumer` at `newindex/codecs/doc_values.rs`
- Fix `.fnm` writer to emit `PerFieldDocValuesFormat.format`/`.suffix` attributes (known gap)
- New `FieldKind` variants with doc values data
- E2e validation via updated `VerifyNewindex`

### Phase 5c: KeywordField and Remaining Field Types

Add remaining consumers and field types. Each gets its own e2e validation pass.

- `KeywordField` — requires `DocValuesConsumer` (Phase 5b) for SORTED_SET doc values
- `PointsConsumer` — BKD tree writing for `IntField`, `FloatField`, `DoubleField`, `LongField`, `LatLonPoint`, range fields
- `TermVectorsConsumer` — per-document term vector writing
- `FeatureField` — special postings with DOCS_AND_FREQS index options

### Phase 6: RAM-Based Flush Control

- RAM-based flush signaling via `SegmentAccumulator` memory tracking
- Stall control when total RAM exceeds limits
- E2e validation with RAM-driven flushing

### Phase 7: Feature Parity E2E

Full cross-validation against the existing indexing path.

- Upgrade `VerifyNewindex` to match `VerifyIndex` check coverage
- Golden summary comparison: new pipeline vs existing pipeline vs Java
- Impact verification (`VerifyImpacts`)
- Performance comparison: new pipeline vs existing pipeline

### Phase 8: Switchover

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
| `FieldConsumer` (postings) | `Lucene103PostingsWriter` + `BlockTreeTermsWriter` | DEBT codec copies adapted to accept `PerFieldPostings` + `ByteBlockPool` |
| `FieldConsumer` (norms) | `Lucene90NormsConsumer` | Consumer wrapping the codec |

These adapters exist temporarily. After switchover (Phase 8), the underlying code is updated to align with `newindex` interfaces and the adapters are removed.

## What This Does NOT Cover

- Deletes, updates, merging (require read path)
- KNN vectors, shape fields
- Index-time sorting
- Near-real-time (NRT) search
- Per-field codec overrides

These remain in the existing backlog and are orthogonal to the pipeline migration.
