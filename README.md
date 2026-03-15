# Bearing

A Rust port of Apache Lucene.

## About

This project exists at the intersection of learning and building. It's an exercise in learning Rust and exploring AI-assisted development with Claude Code, while producing something that might actually be useful. The code is AI-generated and a work in progress — it won't be perfect, and it moves at its own pace. This is a personal project: no pull requests, no issue tracker, no collaborators. If it interests you, fork it and make it your own.

## Why "Bearing"?

The name is a play on words. A bearing gives direction — fitting for a search library. Bearings also carry load — as this project aims to do for indexing workloads. And of course, bearings are made of steel, which isn't too far from Rust.

## Status

- Target: Apache Lucene 10.3.2, Lucene103 codec
- Working: write path with multi-threaded `IndexWriter`
- 8 field types: KeywordField, LongField, TextField, StringField, IntField, FloatField, DoubleField, StoredField
- 353 tests passing
- Java Lucene VerifyIndex validates output

## Performance

Benchmark indexing 2,000 synthetic documents (149 MB) on Linux:

| | Java (1 thread) | Java (12 threads) | Bearing (1 thread) | Bearing (12 threads) |
|---|---|---|---|---|
| **Indexing time** | 2,672 ms | 1,512 ms | 1,284 ms | 586 ms |
| **Peak RSS** | 275 MB | 252 MB | 54 MB | 58 MB |
| **Speedup vs Java** | — | — | 2.1x | 2.6x |
| **Memory savings** | — | — | 5.1x | 4.3x |

## Build

    cargo build
    cargo test
    cargo clippy

## Reference Source

Download the Apache Lucene 10.3.2 source (used as the canonical reference when porting). This is primarily for Claude Code:

    ./reference/download-lucene.sh

Requires internet access. Not required for building or testing.

## Test Data

`testdata/docs/` has 3 small files for quick tests. Generate a larger corpus for benchmarking:

    python3 testdata/gen_docs.py -n 2000    # generates to /tmp/perf-docs/

## E2E Test

Roundtrip test: Bearing writes an index, Java Lucene reads and validates it.

    ./tests/e2e_indexfiles.sh

Requires Java 21+. Gradle handles the Lucene dependency automatically.

## Performance Comparison

Compare indexing speed and correctness between Java Lucene and Rust:

    ./tests/compare_java_rust.sh -release --threads 12
    ./tests/compare_java_rust.sh -docs /tmp/perf-docs -release

| Flag | Default | Description |
|---|---|---|
| `-docs DIR` | `testdata/docs` | Documents directory |
| `-release` | debug | Build Rust in release mode |
| `--threads N` | `12` | Thread count for multi-threaded Rust run |

## Roadmap

See [PLAN.md](docs/PLAN.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

This project is derived from [Apache Lucene](https://lucene.apache.org/).
