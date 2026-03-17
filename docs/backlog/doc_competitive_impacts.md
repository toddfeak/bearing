# .doc Competitive Impacts

## Problem

The .doc (postings) file is 1,880 bytes smaller in Rust than Java (164,278 vs 166,158 for a 500-doc gen_docs corpus). Java writes more data per block because it encodes richer competitive impact information.

## How Java Does It

Each 128-doc block includes skip data with **competitive impacts** — pairs of (frequency, norm) that allow a searcher to skip blocks that can't contribute to top-K results.

Java uses `CompetitiveImpactAccumulator` to collect multiple impacts per block. For terms with varied frequency distributions, this produces several (freq, norm) pairs per block, each encoded as VInts in the impact data section.

Reference: `Lucene103PostingsWriter.java` lines 537-552 (impact accumulation), 400-401 (impact encoding).

## What Rust Does Today

Rust writes a single minimal impact per block: just `(max_freq - 1) << 1` with no norm delta. This is correct enough for index validity (Java's reader accepts it), but produces less skip data than Java.

Location: `src/codecs/lucene103/postings_writer.rs` around line 610, `encode_impacts()`.

## What To Implement

Port `CompetitiveImpactAccumulator` from `org.apache.lucene.codecs.lucene103.Lucene103PostingsWriter` to collect per-block competitive impacts the same way Java does. This affects only the impact encoding within each block's skip data — the FOR/PFor doc ID and frequency encoding is already correct.

## Note

This difference makes Rust indexes *smaller* (less impact data), not larger. It's functionally correct but means a Java reader's block-skipping optimization is less effective on Rust-produced indexes. Fixing this would make Rust indexes slightly larger but byte-closer to Java and better for query performance.

## Testing

After implementing, update E2E testing and Java index verification (`VerifyIndex`) to confirm Rust is producing non-empty competitive impact data and that it matches the expected format. The current tests validate index correctness but don't check impact data specifically — a minimal single-impact encoding passes verification even though it's incomplete.

## Reference Files

- Rust: `src/codecs/lucene103/postings_writer.rs` (impact encoding ~line 610)
- Java: `reference/lucene-10.3.2/lucene/core/src/java/org/apache/lucene/codecs/lucene103/Lucene103PostingsWriter.java`
- Format spec: `reference/formats/lucene103-formats.md`
