# Reference: Apache Lucene 10.3.2 Source

This directory contains the canonical Apache Lucene 10.3.2 Java source used as the reference implementation for this Rust port.

## Setup

Run the download script to fetch the Lucene source:

```bash
./reference/download-lucene.sh
```

This downloads the source tarball from the Apache archive and extracts it to `reference/lucene-10.3.2/`.

## Rules

- **Source of truth**: All Rust code should be derived from the Java source in `reference/lucene-10.3.2/lucene/`.
- **Do not modify**: Do not modify any files under `reference/lucene-10.3.2/` without explicit direction from the user.
- **Re-download**: If the source becomes corrupted or needs a clean state, delete `reference/lucene-10.3.2/` and re-run the download script.

## Format Reference Docs

`reference/formats/` contains byte-level encoding documentation for all Lucene file formats used by our target codec (Lucene103). These are a convenience reference — the Java implementation code is always the source of truth.

| Document | Contents |
|---|---|
| `MAINTAINING.md` | **Lucene 10.3.2 codec version mapping**, trust hierarchy, verification checklist |
| `encoding-primitives.md` | DataOutput/DataInput methods, VInt/VLong, ZigZag, CodecUtil headers/footers |
| `lucene90-formats.md` | Stored fields, doc values, norms, BKD, compound, IndexedDISI, DirectMonotonic, DirectWriter |
| `lucene94-formats.md` | Field infos (.fnm) — enum byte mappings, field bits flags |
| `lucene99-formats.md` | Segment info (.si) — version encoding, YES/NO flags |
| `lucene103-formats.md` | Postings, terms dictionary, FOR/PFor encoding, skip data |

Start with `MAINTAINING.md` for the codec version mapping table — it shows which format version each component uses and which doc to consult.

## Key Paths

| Path | Purpose |
|---|---|
| `reference/lucene-10.3.2/lucene/core/src/java/org/apache/lucene/` | Main source code |
| `reference/lucene-10.3.2/lucene/core/src/test/java/org/apache/lucene/` | Test source code |
