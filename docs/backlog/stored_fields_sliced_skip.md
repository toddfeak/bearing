# Stored Fields: Skip sub-blocks in sliced chunks

## Problem

When a stored fields chunk is "sliced" (total data >= 2x chunk size), it contains multiple LZ4 sub-blocks. The current `StoredFieldsReader` decompresses all sub-blocks even when the target document only spans a subset.

## Lucene behavior

Java's `Lucene90CompressingStoredFieldsReader` can skip sub-blocks before the target document's byte offset, only decompressing from the first relevant sub-block onward.

## Impact

Low — sliced chunks only occur when a single chunk's data exceeds 163,840 bytes (2x 81,920). This requires either very large stored fields or many documents per chunk. Typical workloads won't hit this path.

## Fix

Track cumulative sub-block sizes and skip `fields_stream` past sub-blocks that end before the target document's byte offset. Only decompress from the first overlapping sub-block.
