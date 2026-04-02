# Newindex Known Issues

Outstanding problems and optimization gaps in the newindex pipeline.

---

## 1. Flush Stall Control

**Severity:** High for large-document corpora

With large documents and many threads, total RAM overshoots the configured buffer significantly. The current `FlushControl` signals workers to flush cooperatively, but workers can't be interrupted mid-document. Between the signal and the flush, all threads continue pulling and processing new documents, each potentially holding multi-MB postings pools.

Java's `DocumentsWriterStallControl` solves this by blocking new document intake when total RAM exceeds 2x the buffer limit. Threads already processing documents continue to completion and flush, but no new documents are pulled until RAM drops below the threshold.

**Measured impact** (gutenberg-large-50, 12 threads, 16MB buffer):
- Java peak RSS: 103 MB
- Rust peak RSS: 653 MB (after streaming analyzer fix)
- Rust reported RAM peaks at ~127 MB before flush signals take effect
- OS RSS stays high because the allocator doesn't return freed pages

**Fix:** Add a `Condvar`-based stall in `worker_thread_loop`. Before calling `doc_rx.recv()`, check if total RAM exceeds `2 * ram_buffer_size`. If so, wait on the `Condvar` until flushes bring it below threshold.

The `FlushControl` already has per-worker RAM slots needed for this. The missing piece is:
1. A `Condvar` + `Mutex<bool>` for the stall state
2. Check after each `after_document` — if total > 2x, set stall flag
3. In `worker_thread_loop`, wait on the condvar before `recv` when stalled
4. After each flush completes (`reset_worker`), check if total dropped below threshold and notify the condvar

---

## 2. StandardTokenizer UAX#29 Compliance

**Severity:** Correctness — produces different term counts than Java

The Rust `StandardTokenizer` (`src/analysis/standard.rs`) uses a simple hand-written scanner that splits on non-alphanumeric characters. Java's `StandardTokenizer` implements UAX#29 Unicode text segmentation rules, which handle:
- Numeric grouping separators: `1,200` → single token
- Decimal points in numbers: `12.1` → single token
- Email addresses, URLs
- Southeast Asian scripts, emoji sequences

**Measured impact** (gutenberg-small-500):
- 2,366 fewer unique terms in Rust (28,831 vs 31,197)
- 12,521 higher sumTotalTermFreq in Rust (1,834,520 vs 1,821,999)

**Evidence:**
```
Java: contents  1,200       df=1  ttf=1    (single token)
Rust: contents  1           df=500 ttf=...  (split into "1" and "200")

Java: contents  12.1        df=1  ttf=1
Rust: (split into "12" and "1")
```

**Recommended fix:** Use the `unicode-segmentation` crate (v1.13.2+, MIT/Apache-2.0, zero dependencies, 273M+ downloads) to replace the hand-written word boundary logic in `StandardTokenizer::tokenize_inner`.

- `split_word_bounds()` handles `MidNum`, `MidLetter`, `MidNumLet` classes correctly
- Operates on `&str` slices — fits our existing chunked `analyze_reader` pattern
- Not faster than our scanner for ASCII text (correctness, not speed)
- Does NOT classify token types (ALPHANUM vs NUM) — add a thin wrapper
- Replace `tokenize_inner` internals, keep the streaming chunk infrastructure

**Other options considered:**
- `logos` — can't express UAX#29 rules (no Unicode property escapes)
- `icu_segmenter` — 8+ deps, overkill unless CJK/SE-Asian is near-term
- Port Java's JFlex grammar — no Rust JFlex equivalent

---

## 3. Streaming StandardAnalyzer (FIXED)

**Status:** Fixed in current branch. Documented here for context.

The newindex `StandardAnalyzer` adapter previously buffered ALL tokens for a document in a `VecDeque` before returning any. For a 33MB document, this consumed ~880MB peak across 12 threads.

**Fix applied:** Rewrote `StandardAnalyzer` to read input in 8KB chunks, tokenize each chunk, and buffer only that chunk's tokens. Peak heap dropped from 1.24GB to 243MB (5x reduction) on the gutenberg-large-50 corpus.

---

## 4. ReadProvider for Tokenized+Stored Fields

**Severity:** API design — current approach forces unnecessary memory usage or awkward splits

The text field builder currently offers two paths for providing content:
- `.value(String)` — loads the entire string into memory, works for both tokenizing and storing
- `.reader(Read)` — streams for tokenization, but the reader is consumed once and can't be re-read for storing

This forces large documents to either be loaded entirely into memory (via String) or split into separate stored and tokenized fields. Neither is ideal.

**Proposed fix:** Introduce a `ReadProvider` trait:

```rust
trait ReadProvider: Send {
    fn open(&self) -> io::Result<Box<dyn Read + Send>>;
}
```

The provider is lightweight and owned by the field. Each consumer calls `open()` independently — the tokenizer streams through one reader, the stored fields writer streams through another. Neither holds the full content in memory (for file-backed providers).

Built-in conversions via `From`/`Into`:
- `String` → provider returning `Cursor::new(bytes)` each time
- `PathBuf` / `&Path` → provider returning `File::open()` each time
- `Vec<u8>` → provider returning `Cursor::new(bytes)` each time

The builder API becomes:
```rust
text("body").value("hello world")                // String → ReadProvider
text("body").stored().value("hello world")       // tokenized + stored
text("body").stored().value(path_buf)            // tokenized + stored, streamed from disk
stored("body").value("hello world")              // stored only
```

The `.stored()` chain remains orthogonal. The `ReadProvider` replaces both the current String and Reader paths with a single unified mechanism. Large file-backed fields never load into memory for either tokenization or storage.

---

## 5. Zero-Copy Tokenization with Sliding Window

**Severity:** Performance/allocation optimization

Items 3 and 4 above address specific problems independently. This item combines them with a sliding window buffer to eliminate per-token allocation entirely and simplify the ownership model across the tokenization path.

**Current problems (three intertwined):**
- The segment worker extracts the reader from the field via `take_invertable()` (mutates the field, consumes the value)
- The analyzer takes `&mut dyn Read` and `&mut String buf` as parameters to `next_token` — three-way borrow coordination between worker, reader, and analyzer
- Each token's text is copied into an owned `String` (either via the buf parameter or `BufferedToken` in the VecDeque)

**Proposed design — three changes that reinforce each other:**

1. **ReadProvider** (item 4): Fields hold a lightweight provider. Consumers call `provider.open()` to get independent readers. Fields stay immutable.

2. **Analyzer owns its reader**: Instead of the worker passing `&mut reader` to every `next_token` call, the worker calls `analyzer.set_reader(provider.open()?)` once per field. The analyzer owns the reader internally. `next_token()` becomes `fn next_token(&mut self) -> io::Result<Option<Token<'_>>>` — no reader or buf parameter.

3. **Sliding window buffer**: The analyzer owns a fixed 64KB buffer (2x Lucene's 32KB max term length), allocated as `Box<[u8; 65536]>` — not a `Vec`, which carries hidden capacity overhead (up to 2x) and grow semantics we don't need. Input is read into the buffer, lowercased in-place (ASCII), and tokenized. Tokens are zero-copy `&str` slices into the window. When the scan passes the midpoint, the unconsumed tail is shifted to the start via `copy_within` and the rest is refilled. No per-token String allocation. No VecDeque.

**Token lifetime:** The token borrows from `&self` (the window), valid until the next `next_token` call. The borrow checker enforces this naturally — the caller can't hold two tokens simultaneously. The segment worker's loop already processes one token fully (passing to all consumers) before requesting the next.

**Segment worker token loop becomes:**
```rust
analyzer.set_reader(field.provider().open()?);
while let Some(token) = analyzer.next_token()? {
    for &i in &interested {
        consumers[i].add_token(field_id, field, &token, &mut acc)?;
    }
}
```

No reader variable in the worker. No `take_invertable`. No buf parameter. Fields stay immutable.

**What this eliminates:**
- `VecDeque<BufferedToken>` — no buffering, no per-token String allocation
- `buf: &'b mut String` parameter on `next_token` — token text borrows from the window
- `field.field_type_mut().take_invertable()` — fields are never mutated
- Per-token `to_string()` in the analyzer bridge — gone
- Reader borrow coordination between worker and analyzer — analyzer owns its reader

**Buffer lifecycle:** The 64KB window is allocated once when the analyzer is created. `reset()` between fields zeros the cursors and drops the reader but keeps the buffer. The same buffer is reused across all fields and all documents in the segment — one allocation per worker for the entire segment lifetime. With 12 workers, that's 768KB total for the entire indexing pipeline.

**What remains:**
- One 64KB window allocation per analyzer (reused across all documents and fields via reset)
- Infrequent `memmove` when sliding the window (amortized over many tokens)
- Consumers that need owned copies (e.g., BytesRefHash) copy from the slice — but they already do this today
