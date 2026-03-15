# Tests

## E2E Test

The end-to-end test builds the binary, indexes `testdata/docs`, verifies index files on disk, tests re-indexing, and validates with Java Lucene's VerifyIndex.

```bash
./tests/e2e_indexfiles.sh
```

## Java Test Utilities

`tests/java/` is a Gradle project containing Java utilities (`VerifyIndex`, `IndexAllFields`) that validate Rust-generated indexes using Java Lucene. The Lucene dependency is fetched from Maven Central automatically. These are invoked by the shell scripts — no need to run Gradle directly.

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

## Test Data

`testdata/docs/` contains 3 small text files used as the default corpus for quick tests.

Generate a larger synthetic corpus for benchmarking:

```bash
python3 testdata/gen_docs.py -n 2000    # generates to /tmp/perf-docs/
```

## Profiling

```bash
cargo flamegraph --bin indexfiles -- -docs /tmp/perf-docs -index /tmp/flame-idx
```
