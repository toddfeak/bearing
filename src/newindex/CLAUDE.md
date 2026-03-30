# src/newindex

## Purpose

This module is a ground-up rebuild of the indexing pipeline. It is **not** a line-by-line port of Lucene. The architecture is designed by the project owner to fit Rust's ownership and borrowing model.

## Rules

### No Lucene Line-by-Line Matching

The porting rules in the top-level CLAUDE.md **do not apply** to code in this module. The architecture here is original — informed by Lucene's design but not constrained to match its control flow, variable names, or structure.

Do not:
- Reference Java line numbers in comments
- Restructure code to match Lucene's class hierarchy
- Add "matches Java" or "port of" comments

### LOCKED Markers

Structs and traits marked with `// LOCKED` must **not** be modified without explicit permission from the user. This includes:
- Adding, removing, or renaming fields
- Changing method signatures
- Adding or removing trait bounds
- Changing visibility

If you believe a LOCKED item needs to change, **stop and ask**. Explain what you want to change and why.

### No Crosstalk with src/index

This module must not import from `src/index/` and `src/index/` must not import from this module. They are parallel implementations.

Shared dependencies are fine:
- `src/util/` (pools, hashing, encoding)
- `src/store/` (directory, I/O)
- `src/codecs/` (codec implementations)
- `src/document.rs` (field types, documents)
- `src/analysis/` (tokenization)
