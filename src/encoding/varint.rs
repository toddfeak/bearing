// SPDX-License-Identifier: Apache-2.0

//! Variable-length integer encoding/decoding (VInt, VLong).
//!
//! Uses 7 bits per byte with the high bit as a continuation flag.
//! Small values encode in fewer bytes, making them space-efficient for
//! fields that are typically small (e.g., string lengths, doc deltas).

use crate::encoding::zigzag;
use std::io;

/// Writes a variable-length integer (1-5 bytes).
///
/// Uses 7 bits per byte. High bit = continuation.
pub fn write_vint(out: &mut impl io::Write, i: i32) -> io::Result<()> {
    let mut val = i as u32;
    while (val & !0x7F) != 0 {
        out.write_all(&[((val & 0x7F) | 0x80) as u8])?;
        val >>= 7;
    }
    out.write_all(&[val as u8])
}

/// Writes a variable-length long (1-9 bytes).
///
/// Uses 7 bits per byte. High bit = continuation.
pub fn write_vlong(out: &mut impl io::Write, i: i64) -> io::Result<()> {
    let mut val = i as u64;
    while (val & !0x7F) != 0 {
        out.write_all(&[((val & 0x7F) | 0x80) as u8])?;
        val >>= 7;
    }
    out.write_all(&[val as u8])
}

/// Writes a zigzag-encoded variable-length int.
pub fn write_zint(out: &mut impl io::Write, i: i32) -> io::Result<()> {
    write_vint(out, zigzag::encode_i32(i))
}

/// Writes a zigzag-encoded variable-length long.
pub fn write_zlong(out: &mut impl io::Write, i: i64) -> io::Result<()> {
    let encoded = zigzag::encode_i64(i);
    write_signed_vlong(out, encoded)
}

/// Writes a variable-length long that may be negative (used by [`write_zlong`]).
pub fn write_signed_vlong(out: &mut impl io::Write, mut i: i64) -> io::Result<()> {
    while (i & !0x7Fi64) != 0 {
        out.write_all(&[((i & 0x7F) | 0x80) as u8])?;
        i = ((i as u64) >> 7) as i64;
    }
    out.write_all(&[i as u8])
}

/// Reads a variable-length integer (1-5 bytes).
///
/// High bit = continuation.
pub fn read_vint(reader: &mut impl io::Read) -> io::Result<i32> {
    let mut buf = [0u8; 1];
    let mut result = 0i32;
    let mut shift = 0;
    loop {
        reader.read_exact(&mut buf)?;
        let b = buf[0] as i32;
        result |= (b & 0x7F) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_vint() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 0).unwrap();
        assert_eq!(buf, [0x00]);

        let mut buf = Vec::new();
        write_vint(&mut buf, 127).unwrap();
        assert_eq!(buf, [0x7F]);

        let mut buf = Vec::new();
        write_vint(&mut buf, 128).unwrap();
        assert_eq!(buf, [0x80, 0x01]);

        let mut buf = Vec::new();
        write_vint(&mut buf, 16383).unwrap();
        assert_eq!(buf, [0xFF, 0x7F]);

        let mut buf = Vec::new();
        write_vint(&mut buf, 16384).unwrap();
        assert_eq!(buf, [0x80, 0x80, 0x01]);
    }

    #[test]
    fn test_write_vlong() {
        let mut buf = Vec::new();
        write_vlong(&mut buf, 0).unwrap();
        assert_eq!(buf, [0x00]);

        let mut buf = Vec::new();
        write_vlong(&mut buf, 128).unwrap();
        assert_eq!(buf, [0x80, 0x01]);
    }

    // Ported from org.apache.lucene.store.BaseDataOutputTestCase

    #[test]
    fn test_write_zint() {
        let mut buf = Vec::new();
        write_zint(&mut buf, 0).unwrap();
        assert_eq!(buf, [0x00]);

        let mut buf = Vec::new();
        write_zint(&mut buf, -1).unwrap();
        assert_eq!(buf, [0x01]);

        let mut buf = Vec::new();
        write_zint(&mut buf, 1).unwrap();
        assert_eq!(buf, [0x02]);

        let mut buf = Vec::new();
        write_zint(&mut buf, i32::MIN).unwrap();
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);

        let mut buf = Vec::new();
        write_zint(&mut buf, i32::MAX).unwrap();
        assert_eq!(buf, [0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
    }

    #[test]
    fn test_write_zlong() {
        let mut buf = Vec::new();
        write_zlong(&mut buf, 0).unwrap();
        assert_eq!(buf, [0x00]);

        let mut buf = Vec::new();
        write_zlong(&mut buf, -1).unwrap();
        assert_eq!(buf, [0x01]);

        let mut buf = Vec::new();
        write_zlong(&mut buf, 1).unwrap();
        assert_eq!(buf, [0x02]);

        let mut buf = Vec::new();
        write_zlong(&mut buf, i64::MIN).unwrap();
        assert_eq!(buf.len(), 10);
        for &b in &buf[..9] {
            assert_eq!(b, 0xFF);
        }
        assert_eq!(buf[9], 0x01);

        let mut buf = Vec::new();
        write_zlong(&mut buf, i64::MAX).unwrap();
        assert_eq!(buf.len(), 10);
        assert_eq!(buf[0], 0xFE);
        for &b in &buf[1..9] {
            assert_eq!(b, 0xFF);
        }
        assert_eq!(buf[9], 0x01);
    }

    #[test]
    fn test_read_vint_roundtrip() {
        let test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 0x7FFF_FFFF, -1];
        for &val in &test_values {
            let mut buf = Vec::new();
            write_vint(&mut buf, val).unwrap();
            let mut cursor = &buf[..];
            let decoded = read_vint(&mut cursor).unwrap();
            assert_eq!(decoded, val, "round-trip failed for {val}");
            assert!(cursor.is_empty(), "not all bytes consumed for {val}");
        }
    }

    #[test]
    fn test_write_vint_sizes() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 1);

        buf.clear();
        write_vint(&mut buf, 127).unwrap();
        assert_eq!(buf.len(), 1);

        buf.clear();
        write_vint(&mut buf, 128).unwrap();
        assert_eq!(buf.len(), 2);
    }
}
