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
./tests/compare_java_rust.sh                                  # default: MT, release, verify
./tests/compare_java_rust.sh --1t                             # also run single-threaded
./tests/compare_java_rust.sh --debug                          # debug build
./tests/compare_java_rust.sh -docs /tmp/perf-docs --no-verify # large corpus, skip verify
```

| Flag | Default | Description |
|---|---|---|
| `-docs DIR` | `testdata/docs` | Documents directory |
| `--debug` | release | Build Rust in debug mode |
| `--threads N` | `12` | Thread count for multi-threaded runs |
| `--1t` | off | Also run single-threaded (1T) for both Java and Rust |
| `--no-verify` | verify on | Skip VerifyIndex validation |
| `--compound` | off | Use compound file format (.cfs/.cfe) |

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
| `--compound` | off | Package segment files into .cfs/.cfe |

## Test Data

`testdata/docs/` contains 3 small text files used as the default corpus for quick tests.

Generate a larger synthetic corpus for benchmarking:

```bash
python3 testdata/gen_docs.py -n 2000    # generates to /tmp/perf-docs/
```

## Profiling

### CPU Flamegraph

```bash
cargo flamegraph --bin indexfiles -- -docs /tmp/perf-docs -index /tmp/flame-idx
```

### Heap Profiling (heaptrack)

Always run heaptrack on the compiled binary directly — not via `cargo run`:

```bash
cargo build --release
heaptrack --record-only -o /tmp/heaptrack_%p target/release/indexfiles -docs <DOCS_PATH> -index /tmp/heap-idx --threads 12
heaptrack_print /tmp/heaptrack_<PID>.zst
```
