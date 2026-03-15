# Testing Strategy

## Current Test Layers

### Unit Tests (`#[cfg(test)]` inline)

Inline unit tests within source files provide the majority of coverage. These test internal logic, codecs, data structures, and encoding/decoding at the module level.

### Integration Tests (`tests/*.rs`)

Rust integration tests in `tests/` exercise the public API as an external consumer would. They verify that `IndexWriter`, `Document`, field types, `Directory` implementations, and `Analyzer` work correctly together through the public interface only.

### End-to-End Tests (`tests/e2e_indexfiles.sh`)

Shell-based tests that build the `indexfiles` binary, index test data, verify index files on disk, and validate output using Java Lucene's `VerifyIndex`.

### Performance Comparison (`tests/compare_java_rust.sh`)

Benchmarks indexing speed, memory usage, and correctness between Java Lucene and Rust.

## Cross-Verification Strategy

### Bearing writes → Lucene reads (working)

The e2e test writes an index with the Rust `indexfiles` binary and validates it with Java Lucene's `VerifyIndex`. This is the primary correctness check and runs today.

### Lucene writes → Bearing reads (blocked)

Requires `IndexReader` implementation. When the read path lands, create Java-written golden indexes in `testdata/fixtures/` as regression anchors. The Java `IndexAllFields` utility in `tests/java/` can generate these fixtures.

### Bearing writes → Bearing reads (blocked)

Requires `IndexReader` implementation. Once available, add round-trip tests that write with `IndexWriter` and read back with `IndexReader`, verifying all field types survive the round trip.

## Future Work

### Golden Fixtures

When the read path exists:
1. Generate indexes with Java Lucene 10.3.2 using `tests/java/IndexAllFields`
2. Store in `testdata/fixtures/` with version metadata
3. Write Rust tests that read these fixtures and verify field values
4. Re-generate fixtures when upgrading target Lucene version

### Property-Based Testing

Consider property-based testing (e.g., `proptest` or `quickcheck`) for codec encoding/decoding once the read path enables round-trip verification. Good candidates:
- VInt/VLong encoding round-trips
- Stored field values round-trip
- Doc values encoding/decoding
