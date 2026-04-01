# StandardTokenizer UAX#29 Compliance

## Problem

The Rust `StandardTokenizer` (`src/analysis/standard.rs`) uses a simple hand-written scanner that splits on non-alphanumeric characters (plus apostrophes as internal separators). Java's `StandardTokenizer` implements UAX#29 Unicode text segmentation rules, which handle:

- Numeric grouping separators: `1,200` → single token `1,200`
- Decimal points in numbers: `12.1` → single token `12.1`
- Email addresses, URLs
- Southeast Asian scripts, emoji sequences
- Many other Unicode word break rules

## Impact

On the gutenberg-small-500 corpus, this produces:
- **2,366 fewer unique terms** in Rust (28,831 vs 31,197)
- **12,521 higher sumTotalTermFreq** in Rust (1,834,520 vs 1,821,999)
- Different `.pos` file sizes (~14KB difference on 500 docs)

The defect affects both `src/index/` and `src/newindex/` indexing paths since both use the same `analysis::StandardAnalyzer`.

## Evidence

`dump_terms` comparison of Java vs Rust indexes shows Java keeps numeric tokens intact while Rust splits them:

```
Java: contents  1,200       df=1  ttf=1    (single token)
Rust: contents  1           df=500 ttf=...  (split into "1" and "200")

Java: contents  12.1        df=1  ttf=1
Rust: (split into "12" and "1")

Java: contents  0.186ex     df=1  ttf=1
Rust: (split into "0", "186ex" or similar)
```

## Fix Options

1. **Implement UAX#29 word break rules** — substantial specification work
2. **Use `unicode-segmentation` crate** — adds a dependency, gets correctness
3. **Port Java's JFlex-generated grammar** — complex but guaranteed compatibility
