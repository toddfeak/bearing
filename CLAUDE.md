# CLAUDE.md

## Project

`github.com/toddfeak/bearing` — A port of Apache Lucene from Java to Rust.

The Java implementation of **Apache Lucene 10.3.2** is the canonical source. All Rust code should be derived from the Lucene 10.3.2 Java source in `reference/lucene-10.3.2/lucene/`. See `reference/CLAUDE.md` for details.

## Directory Layout

| Directory | Purpose |
|---|---|
| `src/` | Library and binary source code |
| `tests/` | Integration tests, E2E scripts, Java validation utilities (see `tests/CLAUDE.md`) |
| `testdata/` | Test corpus and data generation scripts |
| `reference/` | Lucene 10.3.2 Java source and format documentation (see `reference/CLAUDE.md`) |
| `docs/` | Roadmap and planning documents |

## Current State

- Target codec: **Lucene103**
- Multi-threaded `IndexWriter` with DWPT pool (no merging, no deletes)
- Eight field types: `KeywordField`, `LongField`, `TextField`, `StringField`, `IntField`, `FloatField`, `DoubleField`, `StoredField`

## Build & Test

```bash
cargo build                    # debug build
cargo test                     # run all tests
cargo clippy                   # lint
```

## Conventions

- Write idiomatic Rust. Use traits, enums, `Result`/`Option` — not 1:1 Java translation.
- No external dependencies except `log` (logging facade) and `simple_logger` (binary logger).
- Use `io::Result<T>` for all fallible operations. Create errors with `io::Error::other("message")`.
- No unsafe code.
- Unimplemented methods should use `todo!("description")` or return `Err(...)`.
- **Byte order**: Little-endian (LE) for data, big-endian (BE) for codec headers/footers. This is the #1 source of bugs — always verify against the Java source.
- **CRC32**: Must match `java.util.zip.CRC32` (polynomial `0xEDB88320`).
- **Codec module naming**: Codec implementations live under version-named modules matching the Java package (e.g., `codecs::lucene90`, `codecs::lucene103`). Version-agnostic utilities stay directly under `codecs`.
- **Logging**: Use `log::debug!` at semantic boundaries (codec headers/footers, flush decisions). Do not log in hot loops.
- **Rustdoc**: Keep `///` and `//!` documentation up to date when changing public API. All public items must be documented.

## Testing

- All code changes must include corresponding tests.
- When porting Java Lucene code, port the applicable tests from `reference/lucene-10.3.2/lucene/core/src/test/java/org/apache/lucene/`.
- Test functions should reference the Java test class they were ported from (e.g., `// Ported from org.apache.lucene.codecs.TestCodecUtil`).
- Use the in-memory `Directory` implementation for unit tests.
- Integration tests in `tests/*.rs` use only `pub` items from `bearing::*`.
- See `tests/CLAUDE.md` for E2E tests, Java utilities, performance comparison, and profiling.
