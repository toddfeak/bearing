# Encoding Cursor Migration

Follow-up to the read-path migration. Now that codec readers own `Cursor<&[u8]>` over borrowed bytes via `store::IndexInput`, migrate selected `/encoding` decode functions to take a concrete `&mut Cursor<&[u8]>` where measurement shows it's faster than going through `&mut dyn Read`.

## Approach (Option D)

Mix of direct-Cursor and generic-Read within `/encoding`:

- **Cursor-based form** for decode functions that meaningfully benefit from direct slice access (bulk chunk grabs, skipping per-byte `read_exact` dispatch, avoiding intermediate allocations).
- **Read-based form** preserved where the decode doesn't meaningfully benefit, OR where a non-Cursor client (e.g., `ByteSliceReader` on the indexing path) needs to call it.
- **Both forms** only when a decode benefits from Cursor AND has live clients that can't be Cursor-ified.

Per-function decision rule:

1. If all clients are Cursor-capable → switch the decode to Cursor-only.
2. If clients are split → support both forms, duplicating minimal glue and sharing the inner logic where possible.
3. Consider pushing Read clients onto Cursor only if the Cursor form is *substantially* faster and the migration is cheap.

`IndexInput` continues to work with both forms — it impls `Read` for Read-based decodes, and exposes `cursor_mut()` for Cursor-based decodes. The three adapter methods currently on `IndexInput` (`read_group_vints`, `pfor_decode`, `for_delta_decode`) disappear once the encoding side is migrated.

## Measured Speedups (Read-based vs Cursor-based)

Measured with `examples/encoding_bench.rs` (release build). Both variants read from `std::io::Cursor<&[u8]>` over identical in-memory buffers. Times are ns/op, median of 7 runs after 3 warmup rounds.

| Function | Scenario | Read | Cursor | Speedup |
|---|---|---:|---:|---:|
| `varint::read_vint` | 1B (val=127) | 3.3 | 1.4 | **2.36x** |
| `varint::read_vint` | 2B (val=16383) | 9.8 | 1.6 | **6.02x** |
| `varint::read_vint` | 3B (val=2097151) | 8.3 | 2.0 | **4.09x** |
| `varint::read_vint` | 4B (val=268435455) | 9.5 | 2.5 | **3.76x** |
| `varint::read_vint` | 5B (val=i32::MAX) | 11.3 | 2.8 | **4.06x** |
| `varint::read_vlong` | 1B | 3.0 | 1.1 | **2.66x** |
| `varint::read_vlong` | 5B | 11.3 | 2.5 | **4.48x** |
| `varint::read_vlong` | 9B | 18.1 | 4.0 | **4.49x** |
| `varint::read_zint` | 1B (v=0) | 4.3 | 1.6 | **2.62x** |
| `varint::read_zint` | 3B | 10.5 | 2.8 | **3.78x** |
| `varint::read_zint` | 5B (v=i32::MAX) | 13.0 | 3.1 | **4.17x** |
| `varint::read_zlong` | 1B (v=0) | 3.2 | 1.4 | **2.33x** |
| `varint::read_zlong` | 5B | 14.5 | 3.3 | **4.42x** |
| `varint::read_zlong` | 10B (v=i64::MAX) | 20.1 | 4.8 | **4.17x** |
| `varint::read_signed_vlong` | 1B (v=0) | 3.0 | 1.1 | **2.66x** |
| `varint::read_signed_vlong` | 5B | 11.1 | 2.5 | **4.41x** |
| `varint::read_signed_vlong` | 10B (v=i64::MIN) | 22.4 | 4.5 | **4.94x** |
| `string::read_string` | 10B | 27.1 | 21.2 | **1.28x** |
| `string::read_string` | 200B | 48.8 | 26.8 | **1.82x** |
| `group_vint::read_group_vints` | all-1B | 22.6 | 22.1 | 1.02x |
| `group_vint::read_group_vints` | mixed | 21.6 | 19.1 | 1.13x |
| `group_vint::read_group_vints` | all-4B | 19.8 | 10.3 | **1.92x** |
| `pfor::decode` | bpv=1 | 57.7 | 55.7 | 1.03x |
| `pfor::decode` | bpv=8 | 153.7 | 49.1 | **3.13x** |
| `pfor::decode` | bpv=16 | 277.8 | 73.3 | **3.79x** |
| `pfor::decode` | bpv=20 | 440.6 | 189.8 | **2.32x** |
| `pfor::pfor_decode` | all-equal | 11.9 | 11.7 | 1.02x |
| `pfor::pfor_decode` | small-values | 73.5 | 48.6 | **1.51x** |
| `pfor::pfor_decode` | with-exceptions | 112.5 | 53.9 | **2.09x** |
| `pfor::for_delta_decode` | bpv=2 | 77.9 | 63.1 | **1.23x** |
| `pfor::for_delta_decode` | bpv=5 | 129.3 | 78.0 | **1.66x** |
| `pfor::for_delta_decode` | bpv=12 | 296.9 | 148.9 | **1.99x** |
| `lowercase_ascii::decompress_from_reader` | no exc (1KB) | 96.3 | 90.9 | 1.06x |
| `lowercase_ascii::decompress_from_reader` | with exc (1KB) | 153.5 | 104.3 | **1.47x** |
| `lz4::decompress_from_reader` | 4KB | 2246.9 | 2099.2 | 1.07x |

## Migration Candidates

### Strong (migrate)

- `varint::read_vint` / `read_vlong` / `read_zint` / `read_zlong` / `read_signed_vlong` — 2-6x across every byte-length path. The original driver of the read-path migration.
- `string::read_string` (and `read_set_of_strings`, `read_map_of_strings` which compose it) — 1.3-1.8x from avoided intermediate `Vec<u8>` + direct UTF-8 validation on the slice, plus varint composition.
- `pfor::decode` / `pfor_decode` / `for_delta_decode` — 1.5-3.8x from bulk-reading the packed byte block, except at trivially small `bpv` where the decode is negligible anyway.
- `lowercase_ascii::decompress_from_reader` — 1.47x with exceptions (the per-exception 2-byte `read_exact` pairs are the bottleneck under Read); flat without. The caller can't predict which shape applies, so migration captures the with-exc case for free.

### Modest (migrate if cheap)

- `group_vint::read_group_vints` — 1.0-1.9x depending on per-value byte widths. Worth doing when it's convenient since the API surface is small; don't expect a big win.

### Drop (leave as Read)

- `lz4::decompress_from_reader` — 1.07x. The bottleneck is the match-copy byte-loop over `output`, which Cursor access doesn't touch. Revisit only if someone tackles the match-copy (bulk `copy_within` for non-overlapping matches, `extend_from_within` patterns, etc.) — that's a separate performance question unrelated to the Read/Cursor distinction.

## Client Audit (Not Yet Done)

Before picking Cursor-only vs both-forms per function, enumerate callers of each:

- For each migration candidate, list every call site (codec readers, indexing path, tests).
- Mark each caller as Cursor-capable (holds `IndexInput` / `Cursor<&[u8]>`) or Read-only (e.g., `ByteSliceReader` on the indexing path).
- If the indexing path needs a given decode AND can't use Cursor, keep the Read form and add a Cursor form alongside.

`ByteSliceReader` is known to need `read_vint` on the indexing path (it has an inherent `read_vint` today that delegates to `varint::read_vint(self)`) — so varint will need both forms unless `ByteSliceReader` stops using `varint`. Full audit needed for the others.

## Benchmark Harness

The measurement code lives at `examples/encoding_bench.rs`. It's temporary scaffolding:

- Includes Cursor-based reference implementations of every candidate.
- Verifies each Cursor version against its Read counterpart for correctness before running measurements (aborts on mismatch).
- Runs `cargo run --release --example encoding_bench`.

Delete the harness once the encoding migration completes. The Cursor implementations there are drafts — the final production versions may differ in API shape (lifetimes, error handling) once client sites are wired up.

## Proposed Sequencing

1. Client audit per candidate function → decide Cursor-only vs both-forms per function.
2. Migrate varint first (largest speedup, most call sites, highest-frequency path — primary validation of the approach).
3. Re-measure end-to-end query/term-iteration performance against the baselines in `read_path_migration` (~57 us/query, ~1.0s for 33M terms) to confirm the micro-wins translate to real speedups.
4. Proceed with `string`, `pfor`, `lowercase_ascii`, `group_vint` once varint delivers measurable end-to-end improvement.
5. Delete `IndexInput`'s adapter methods (`read_group_vints`, `pfor_decode`, `for_delta_decode`) once their encoding counterparts are Cursor-native.
6. Delete `examples/encoding_bench.rs`.

## Preserved Cursor Implementations

Reference Cursor-based implementations used by the benchmark harness. These were verified against the existing `&mut dyn Read`-based functions for correctness on a representative input set; the final production versions may differ in API shape (lifetimes, error paths, naming) once integrated into codec readers.

### `varint`

```rust
fn cursor_read_vint(c: &mut Cursor<&[u8]>) -> io::Result<i32> {
    let start = c.position() as usize;
    let buf = c.get_ref();
    if start >= buf.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let remaining = &buf[start..];
    let mut result = 0i32;
    let mut shift = 0;
    for (i, &byte) in remaining.iter().take(5).enumerate() {
        let b = byte as i32;
        result |= (b & 0x7F) << shift;
        if b & 0x80 == 0 {
            c.set_position((start + i + 1) as u64);
            return Ok(result);
        }
        shift += 7;
    }
    Err(io::Error::other("vint too long or EOF"))
}

fn cursor_read_vlong(c: &mut Cursor<&[u8]>) -> io::Result<i64> {
    let start = c.position() as usize;
    let buf = c.get_ref();
    if start >= buf.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let remaining = &buf[start..];
    let mut result = 0i64;
    let mut shift = 0;
    for (i, &byte) in remaining.iter().take(10).enumerate() {
        let b = byte as i64;
        result |= (b & 0x7F) << shift;
        if b & 0x80 == 0 {
            c.set_position((start + i + 1) as u64);
            return Ok(result);
        }
        shift += 7;
    }
    Err(io::Error::other("vlong too long or EOF"))
}

fn cursor_read_zint(c: &mut Cursor<&[u8]>) -> io::Result<i32> {
    let v = cursor_read_vint(c)?;
    Ok(((v as u32) >> 1) as i32 ^ -(v & 1))
}

fn cursor_read_signed_vlong(c: &mut Cursor<&[u8]>) -> io::Result<i64> {
    // Same byte layout as vlong but allows full i64 range (up to 10 bytes).
    cursor_read_vlong(c)
}

fn cursor_read_zlong(c: &mut Cursor<&[u8]>) -> io::Result<i64> {
    let v = cursor_read_signed_vlong(c)?;
    Ok(((v as u64) >> 1) as i64 ^ -(v & 1))
}
```

### `string::read_string`

UTF-8 is validated directly on the cursor's slice, skipping the intermediate `Vec<u8>` + `read_exact` + `String::from_utf8(vec)` path. The returned `String` still owns its bytes.

```rust
fn cursor_read_string(c: &mut Cursor<&[u8]>) -> io::Result<String> {
    let len = cursor_read_vint(c)?;
    let len = usize::try_from(len).map_err(|_| io::Error::other("negative string length"))?;
    let pos = c.position() as usize;
    let buf = c.get_ref();
    if pos + len > buf.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let s = std::str::from_utf8(&buf[pos..pos + len])
        .map_err(|e| io::Error::other(e.to_string()))?
        .to_owned();
    c.set_position((pos + len) as u64);
    Ok(s)
}
```

### `group_vint::read_group_vints`

The flag byte fully determines the group's total byte width; the 4 values are unpacked directly from the cursor's slice.

```rust
fn cursor_read_group_vints(
    c: &mut Cursor<&[u8]>,
    values: &mut [i32],
    limit: usize,
) -> io::Result<()> {
    let mut read_pos = 0;
    let buf = c.get_ref();
    let mut pos = c.position() as usize;

    while limit - read_pos >= 4 {
        if pos >= buf.len() {
            c.set_position(pos as u64);
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let flag = buf[pos] as u32;
        pos += 1;

        let sizes = [
            (((flag >> 6) & 0x03) + 1) as usize,
            (((flag >> 4) & 0x03) + 1) as usize,
            (((flag >> 2) & 0x03) + 1) as usize,
            ((flag & 0x03) + 1) as usize,
        ];
        let total: usize = sizes.iter().sum();
        if pos + total > buf.len() {
            c.set_position(pos as u64);
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        for &size in &sizes {
            let mut tmp = [0u8; 4];
            tmp[..size].copy_from_slice(&buf[pos..pos + size]);
            values[read_pos] = i32::from_le_bytes(tmp);
            read_pos += 1;
            pos += size;
        }
    }

    c.set_position(pos as u64);
    while read_pos < limit {
        values[read_pos] = cursor_read_vint(c)?;
        read_pos += 1;
    }

    Ok(())
}
```

### `lowercase_ascii::decompress_from_reader`

Bulk-copies the packed body in one `copy_from_slice`, then indexes the exception pairs directly from the cursor slice (saving two vtable-dispatched `read_exact` calls per exception).

```rust
fn cursor_lowercase_ascii_decompress(
    c: &mut Cursor<&[u8]>,
    len: usize,
) -> io::Result<Vec<u8>> {
    let saved = len >> 2;
    let compressed_len = len - saved;

    let mut out = vec![0u8; len];
    let pos = c.position() as usize;
    let buf = c.get_ref();
    if pos + compressed_len > buf.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    out[..compressed_len].copy_from_slice(&buf[pos..pos + compressed_len]);
    c.set_position((pos + compressed_len) as u64);

    for i in 0..saved {
        out[compressed_len + i] = ((out[i] & 0xC0) >> 2)
            | ((out[saved + i] & 0xC0) >> 4)
            | ((out[(saved << 1) + i] & 0xC0) >> 6);
    }

    for b in &mut out[..len] {
        *b = ((*b & 0x1F) | 0x20 | ((*b & 0x20) << 1)).wrapping_sub(1);
    }

    let num_exceptions = cursor_read_vint(c)? as usize;
    let pos = c.position() as usize;
    let buf = c.get_ref();
    if pos + 2 * num_exceptions > buf.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let mut i = 0usize;
    for k in 0..num_exceptions {
        i += buf[pos + k * 2] as usize;
        out[i] = buf[pos + k * 2 + 1];
    }
    c.set_position((pos + 2 * num_exceptions) as u64);

    Ok(out)
}
```

### `pfor` family

All three pfor-family decoders bulk-read the packed block of `(bpv * 4) * 4` bytes in a single slice operation, unpack as LE i32s, then delegate to the existing bit-unpacking logic (`decode_ints`, `expand8`, `expand16`, `prefix_sum`).

```rust
fn cursor_for_decode(
    bpv: u32,
    c: &mut Cursor<&[u8]>,
    longs: &mut [i64; BLOCK_SIZE],
) -> io::Result<()> {
    if bpv == 0 {
        longs.fill(0);
        return Ok(());
    }
    let num_ints_per_shift = (bpv * 4) as usize;
    let bytes_needed = num_ints_per_shift * 4;

    let pos = c.position() as usize;
    let buf = c.get_ref();
    if pos + bytes_needed > buf.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let slice = &buf[pos..pos + bytes_needed];

    let mut ints = [0i32; BLOCK_SIZE];
    for i in 0..num_ints_per_shift {
        let off = i * 4;
        ints[i] = i32::from_le_bytes([
            slice[off],
            slice[off + 1],
            slice[off + 2],
            slice[off + 3],
        ]);
    }
    c.set_position((pos + bytes_needed) as u64);

    let primitive_size = if bpv <= 8 {
        8
    } else if bpv <= 16 {
        16
    } else {
        32
    };

    decode_ints(&mut ints, bpv, primitive_size);

    if bpv <= 8 {
        expand8(&mut ints);
    } else if bpv <= 16 {
        expand16(&mut ints);
    }

    for i in 0..BLOCK_SIZE {
        longs[i] = ints[i] as i64;
    }
    Ok(())
}

fn cursor_pfor_decode(c: &mut Cursor<&[u8]>, longs: &mut [i64; BLOCK_SIZE]) -> io::Result<()> {
    let pos = c.position() as usize;
    let buf = c.get_ref();
    if pos >= buf.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let token = buf[pos] as u32;
    c.set_position((pos + 1) as u64);
    let bpv = token & 0x1F;
    if bpv == 0 {
        let value = cursor_read_vint(c)? as i64;
        longs.fill(value);
    } else {
        cursor_for_decode(bpv, c, longs)?;
    }
    let num_exceptions = token >> 5;
    let pos = c.position() as usize;
    let buf = c.get_ref();
    if pos + (num_exceptions as usize) * 2 > buf.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    for i in 0..num_exceptions as usize {
        let position = buf[pos + i * 2] as usize;
        let patch = buf[pos + i * 2 + 1] as i64;
        longs[position] |= patch << bpv;
    }
    c.set_position((pos + (num_exceptions as usize) * 2) as u64);
    Ok(())
}

fn cursor_for_delta_decode(
    bpv: u32,
    c: &mut Cursor<&[u8]>,
    base: i32,
    ints: &mut [i32; BLOCK_SIZE],
) -> io::Result<()> {
    if bpv == 0 {
        ints.fill(base);
        return Ok(());
    }
    let num_ints_per_shift = (bpv * 4) as usize;
    let bytes_needed = num_ints_per_shift * 4;

    let pos = c.position() as usize;
    let buf = c.get_ref();
    if pos + bytes_needed > buf.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let slice = &buf[pos..pos + bytes_needed];

    ints.fill(0);
    for i in 0..num_ints_per_shift {
        let off = i * 4;
        ints[i] = i32::from_le_bytes([
            slice[off],
            slice[off + 1],
            slice[off + 2],
            slice[off + 3],
        ]);
    }
    c.set_position((pos + bytes_needed) as u64);

    let primitive_size = if bpv <= 3 {
        8
    } else if bpv <= 10 {
        16
    } else {
        32
    };

    decode_ints(ints, bpv, primitive_size);

    if bpv <= 3 {
        prefix_sum(&mut ints[..32], 0);
        expand8(ints);
        let l0 = base;
        let l1 = l0 + ints[31];
        let l2 = l1 + ints[63];
        let l3 = l2 + ints[95];
        for i in 0..32 {
            ints[i] += l0;
            ints[32 + i] += l1;
            ints[64 + i] += l2;
            ints[96 + i] += l3;
        }
    } else if bpv <= 10 {
        prefix_sum(&mut ints[..64], 0);
        expand16(ints);
        let l0 = base;
        let l1 = base + ints[63];
        for i in 0..64 {
            ints[i] += l0;
            ints[64 + i] += l1;
        }
    } else {
        prefix_sum(&mut ints[..BLOCK_SIZE], base);
    }

    Ok(())
}
```

The pfor decoders reuse the existing `decode_ints`, `expand8`, `expand16`, `prefix_sum`, and `MASKS{8,16,32}` helpers in `src/encoding/pfor.rs`. Those helpers operate on in-register integer arrays and are independent of how the packed bytes were read, so they don't change between the Read and Cursor paths. They're intentionally not duplicated here — see `src/encoding/pfor.rs` for the canonical versions.
