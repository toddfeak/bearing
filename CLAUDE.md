# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository

`github.com/toddfeak/bearing`

## Project

A port of Apache Lucene's indexing functionality from Java to Rust. The Java implementation of **Apache Lucene 10.3.2** is the canonical source. All Rust code should be derived from the Lucene 10.3.2 Java source in `reference/lucene-10.3.2/lucene/`. See `reference/CLAUDE.md` for details. Run `./reference/download-lucene.sh` to set up the reference source.

Do **not** reference the golucene Go port as a source of information for this project.

## Current State

- Target codec: **Lucene103**
- Multi-threaded `IndexWriter` with DWPT pool (no merging, no deletes)
- Eight field types: `KeywordField`, `LongField`, `TextField`, `StringField`, `IntField`, `FloatField`, `DoubleField`, `StoredField`
- 344 tests passing, Java Lucene VerifyIndex validates output

### Known Limitations

- Sparse doc values/norms return `Err` (all demo fields are always present)
- Multi-valued `SORTED_NUMERIC` / `SORTED_SET` return `Err`
- No `.pay` file (payloads)
- No merging, no deletes
- `StandardAnalyzer` has no stop words
- Impact metadata in skip data uses simplified approximation (norm=1)

## Build & Test

```bash
cargo build                    # debug build
cargo build --release          # release build (for benchmarks)
cargo test                     # run all 288 tests
cargo clippy                   # lint
```

Run a single test:
```bash
cargo test <test_name>
```

Run tests for a specific module:
```bash
cargo test --lib <module_name>
```

## CLI Reference

The `indexfiles` binary indexes files from a directory, modeled after Lucene's `IndexFiles` demo.

```bash
cargo run --bin indexfiles -- -docs <DOCS_PATH> [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `-docs PATH` | *(required)* | Source directory with files to index |
| `-index PATH` | `index` | Output directory for the index |
| `--max-buffered-docs N` | disabled | Flush after N documents per segment |
| `--ram-buffer-size MB` | `16.0` | RAM buffer size in MB |
| `--threads N` | `1` | Number of indexing threads |

## E2E Test

The end-to-end test builds the binary, indexes `testdata/docs`, verifies index files on disk, tests re-indexing, and validates with Java Lucene's VerifyIndex.

```bash
./tests/e2e_indexfiles.sh
```

### Java Test Utilities

`tests/java/` is a Gradle project containing Java utilities (`VerifyIndex`, `IndexAllFields`) that validate Rust-generated indexes using Java Lucene. The Lucene dependency is fetched from Maven Central automatically. These are invoked by the shell scripts above — no need to run Gradle directly.

## Performance Comparison

Compare indexing speed, memory usage, and correctness between Java Lucene and Rust:

```bash
./tests/compare_java_rust.sh -release --threads 12
./tests/compare_java_rust.sh -docs /tmp/perf-docs -release
```

| Flag | Default | Description |
|---|---|---|
| `-docs DIR` | `testdata/docs` | Documents directory |
| `-release` | debug | Build Rust in release mode |
| `--threads N` | `12` | Thread count for multi-threaded Rust run |

## Test Data

`testdata/docs/` contains 3 small text files (`animals.txt`, `history.txt`, `technology.txt`) used as the default corpus for quick tests.

Generate a larger synthetic corpus for benchmarking:

```bash
python3 testdata/gen_docs.py -n 2000    # generates to /tmp/perf-docs/
```

## Profiling with Flamegraph

Generate an interactive CPU flamegraph:

```bash
cargo install flamegraph                  # one-time setup
cargo flamegraph --bin indexfiles -- -docs /tmp/perf-docs -index /tmp/flame-idx
open flamegraph.svg                       # interactive SVG in browser
```

For better symbol resolution, add to `Cargo.toml`:
```toml
[profile.release]
debug = true
```

## Format Reference

`reference/formats/` contains byte-level encoding documentation for all Lucene file formats used by our target codec (Lucene103). Start with `reference/formats/MAINTAINING.md` for the codec version mapping table and verification process. See `reference/CLAUDE.md` for the full file listing.

## Source Reference

The key Java packages to port from are under `reference/lucene-10.3.2/lucene/core/src/java/org/apache/lucene/`:

| Java Package | Purpose |
|---|---|
| `index` | Core indexing: `IndexWriter`, `IndexReader`, `DocumentsWriter`, `SegmentInfo`, `Terms`, `PostingsEnum` |
| `document` | `Document`, `Field`, `FieldType`, `IndexOptions`, `DocValuesType` |
| `store` | `Directory`, `IndexInput`, `IndexOutput`, `Lock`, `IOContext` |
| `codecs` | `Codec` and all sub-format interfaces for encoding/decoding index data |
| `analysis` | `Analyzer`, `TokenStream`, `Tokenizer`, `TokenFilter` |
| `util` | Shared utilities: `BytesRef`, `Bits`, `FixedBitSet`, `InfoStream`, packed integers, FST, automaton |

## Conventions

- No external dependencies except `log` (logging facade) and `simple_logger` (concrete logger for the binary). Use the Rust standard library for everything else.
- Unimplemented methods should use `todo!("description")` or return `Err(...)` with a descriptive message.
- Prefer Rust idioms (traits, enums, `Result`/`Option`) over direct 1:1 Java translation. For example, use traits where Java uses interfaces/abstract classes, and enums where Java uses constant sets.
- Use `#[cfg(test)]` modules within source files for unit tests.
- Use modern Rust module style: `foo.rs` + `foo/` directory for modules with children, flat `foo.rs` for leaf modules. Do **not** use `mod.rs` files.
- **Byte order**: Use little-endian (LE) for data and big-endian (BE) for codec headers/footers. This is the #1 source of bugs — always double-check endianness against the Java source.
- **CRC32**: Must match `java.util.zip.CRC32` (polynomial `0xEDB88320`).
- **Codec module naming**: Codec format implementations must live under version-named modules matching the Java package they were ported from (e.g., `codecs::lucene90`, `codecs::lucene94`, `codecs::lucene99`, `codecs::lucene103`). Version-agnostic utilities like `codec_util` stay directly under `codecs`.

## Coding Style

- **Error handling**: Use `io::Result<T>` for all fallible operations. Create errors with `io::Error::other("descriptive message")` or `io::Error::new(ErrorKind, msg)`.
- **Zero-allocation hot paths**: Use callback-based APIs with borrowed references rather than returning owned collections. Example: `analyze_to()` takes `&mut dyn FnMut(TokenRef<'_>)` instead of returning `Vec<Token>`.
- **Buffer reuse**: Pass reusable `String` buffers through call chains (e.g., `lowercase_buf`) rather than allocating per-call.
- **Compact storage**: Use vInt-encoded `Vec<u8>` byte streams instead of struct vectors for hot-path data (e.g., `PostingList::byte_stream`).
- **No unsafe code**: The entire codebase is safe Rust.
- **Constants**: Name all protocol/format values with `SCREAMING_SNAKE_CASE`. Group semantically at the top of the module.
- **Imports**: Order as std first, external crates second, `crate::` internal imports third.
- **Traits**: Use `dyn Trait` with `Send + Sync` bounds for polymorphism. Use `Arc<dyn Trait>` for shared ownership across threads.

## Logging

- Use `log::debug!` for debug output when writing index data (codec writers, headers, footers).
- Set `RUST_LOG=debug` to enable (e.g., `RUST_LOG=debug cargo run -- -docs testdata/docs`).
- Log at a semantic level (what's being written and why), not every byte.
- Do **not** log in hot loops (per-token, per-document). Only at boundaries (codec headers/footers, flush decisions).
- The binary initializes `simple_logger` in `main()`; the library only uses the `log` facade.

## Testing

- All code changes must include corresponding tests. When porting code from Java Lucene, port the applicable tests from the Java Lucene 10.3.2 test suite as well.
- The Java Lucene 10.3.2 test source is at `reference/lucene-10.3.2/lucene/core/src/test/java/org/apache/lucene/`. Use it to find canonical test cases for any feature being ported.
- Test functions should reference the Java test class they were ported from in a comment (e.g. `// Ported from org.apache.lucene.codecs.TestCodecUtil`).
- Use an in-memory `Directory` implementation for unit tests.
