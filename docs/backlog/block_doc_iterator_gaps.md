# BlockDocIterator gaps

Deferred features from the initial BlockDocIterator implementation.

## Must fix before scoring queries

- **Freq values discarded**: `read_vint_block` strips the freq LSB but doesn't
  store frequencies. `refill_full_block` skips PFOR freq data entirely.
  Need to decode and buffer freqs for TF/IDF scoring.

- **No `advance(target)` / skip-based seeking**: Only sequential `next_doc()`
  is supported. Conjunctive queries (AND) need `advance()` to skip ahead
  efficiently. Requires parsing level0 skip headers instead of skipping them.

## Must fix for large terms (> 4096 docs)

- **No level1 skip handling**: Assumes all blocks fit in one level1 group
  (< 4096 docs per term). Matches the writer's current `assert!` limitation.
  When the writer adds level1 support, the reader must handle the level1
  skip wrapper that groups 32 level0 blocks.
