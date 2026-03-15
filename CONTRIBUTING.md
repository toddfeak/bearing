# Contributing to Bearing

## Project Status

Bearing is a personal project — a Rust port of Apache Lucene's indexing functionality. Pull requests are **not currently accepted**. You're welcome to fork and make it your own.

## Building & Testing

```bash
cargo build            # debug build
cargo test             # run all tests
cargo clippy           # lint (warnings should be zero)
cargo fmt              # format code
```

Full end-to-end test (requires Java 21+; Gradle handles the Lucene dependency):

```bash
./tests/e2e_indexfiles.sh
```

## Coding Conventions

See [`CLAUDE.md`](CLAUDE.md) for the authoritative style guide, including error handling, module layout, byte order rules, and naming conventions.

## Porting Methodology

Bearing is a port of **Apache Lucene 10.3.2**. Lucene is the source of truth for **behavior and storage formats** — the on-disk output must be compatible. However, the Rust code should be idiomatic Rust, not a transliteration of Java. Prefer traits, enums, and Rust's ownership model over mirroring Java's object hierarchy. Codec version naming (e.g., `lucene90`, `lucene103`) follows the Java convention.

To set up the reference source (requires Java 21+):

```bash
./reference/download-lucene.sh
```

When porting a feature, locate the corresponding Java source under `reference/lucene-10.3.2/lucene/` and port both the implementation and its tests.

## Dependency Policy

Bearing uses only two external crates: `log` (logging facade) and `env_logger` (concrete logger for the binary). Everything else uses the Rust standard library.

## If PRs Open in the Future

Contributions would need to:

- Pass `cargo test` (all tests)
- Pass `cargo clippy -- -D warnings` (no lint warnings)
- Be formatted with `cargo fmt`
- Include tests for new functionality
- Reference the Java Lucene source class when porting (e.g., `// Ported from org.apache.lucene.codecs.TestCodecUtil`)
