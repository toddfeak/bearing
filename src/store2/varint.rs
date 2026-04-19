// SPDX-License-Identifier: Apache-2.0

//! Slice-aware variable-length integer parsers over `Cursor<&[u8]>`.
//!
//! Each parser reads from the cursor's remaining bytes via
//! [`BufRead::fill_buf`] and advances the cursor position by exactly the
//! number of bytes consumed on success. On error the cursor is not advanced.

use std::io;
use std::io::BufRead;
use std::io::Cursor;

use crate::encoding::zigzag;

const VINT_MAX_BYTES: usize = 5;
const VLONG_MAX_BYTES: usize = 10;

/// Reads a variable-length 32-bit integer (1-5 bytes, 7 bits per byte).
pub fn read_vint(cursor: &mut Cursor<&[u8]>) -> io::Result<i32> {
    let buf = cursor.fill_buf()?;
    let mut result: i32 = 0;
    let mut shift = 0;
    for (i, &b) in buf.iter().take(VINT_MAX_BYTES).enumerate() {
        result |= ((b & 0x7F) as i32) << shift;
        if b & 0x80 == 0 {
            cursor.consume(i + 1);
            return Ok(result);
        }
        shift += 7;
    }
    Err(truncated_or_malformed(buf.len(), VINT_MAX_BYTES, "VInt"))
}

/// Reads a variable-length 64-bit integer (1-10 bytes, 7 bits per byte).
pub fn read_vlong(cursor: &mut Cursor<&[u8]>) -> io::Result<i64> {
    let buf = cursor.fill_buf()?;
    let mut result: i64 = 0;
    let mut shift = 0;
    for (i, &b) in buf.iter().take(VLONG_MAX_BYTES).enumerate() {
        result |= ((b & 0x7F) as i64) << shift;
        if b & 0x80 == 0 {
            cursor.consume(i + 1);
            return Ok(result);
        }
        shift += 7;
    }
    Err(truncated_or_malformed(buf.len(), VLONG_MAX_BYTES, "VLong"))
}

/// Reads a zigzag-encoded variable-length 32-bit integer.
pub fn read_zint(cursor: &mut Cursor<&[u8]>) -> io::Result<i32> {
    Ok(zigzag::decode_i32(read_vint(cursor)?))
}

/// Reads a zigzag-encoded variable-length 64-bit integer.
pub fn read_zlong(cursor: &mut Cursor<&[u8]>) -> io::Result<i64> {
    Ok(zigzag::decode_i64(read_vlong(cursor)?))
}

fn truncated_or_malformed(buf_len: usize, limit: usize, label: &str) -> io::Error {
    if buf_len < limit {
        io::Error::new(io::ErrorKind::UnexpectedEof, format!("truncated {label}"))
    } else {
        io::Error::other(format!("{label} exceeds {limit} bytes"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::varint::{write_vint, write_vlong, write_zint, write_zlong};

    fn encode<F>(f: F) -> Vec<u8>
    where
        F: FnOnce(&mut Vec<u8>) -> io::Result<()>,
    {
        let mut buf = Vec::new();
        f(&mut buf).unwrap();
        buf
    }

    // read_vint

    #[test]
    fn vint_roundtrip_values() {
        for &value in &[0i32, 1, 127, 128, 255, 16383, 16384, i32::MAX, -1] {
            let buf = encode(|b| write_vint(b, value));
            let mut cursor = Cursor::new(&buf[..]);
            assert_eq!(read_vint(&mut cursor).unwrap(), value, "value = {value}");
            assert_eq!(
                cursor.position() as usize,
                buf.len(),
                "cursor fully consumed for {value}"
            );
        }
    }

    #[test]
    fn vint_empty_input_errors() {
        let mut cursor = Cursor::new(&[][..]);
        assert_err!(read_vint(&mut cursor));
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn vint_truncated_continuation_errors() {
        let mut cursor = Cursor::new(&[0x80, 0x80][..]);
        assert_err!(read_vint(&mut cursor));
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn vint_exceeds_max_bytes_errors() {
        let mut cursor = Cursor::new(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80][..]);
        assert_err!(read_vint(&mut cursor));
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn vint_consecutive_reads_advance_cursor() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 42).unwrap();
        write_vint(&mut buf, 128).unwrap();
        write_vint(&mut buf, i32::MAX).unwrap();
        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(read_vint(&mut cursor).unwrap(), 42);
        assert_eq!(read_vint(&mut cursor).unwrap(), 128);
        assert_eq!(read_vint(&mut cursor).unwrap(), i32::MAX);
        assert_eq!(cursor.position() as usize, buf.len());
    }

    // read_vlong

    #[test]
    fn vlong_roundtrip_values() {
        for &value in &[0i64, 1, 127, 128, 255, 16383, 16384, i64::MAX] {
            let buf = encode(|b| write_vlong(b, value));
            let mut cursor = Cursor::new(&buf[..]);
            assert_eq!(read_vlong(&mut cursor).unwrap(), value, "value = {value}");
            assert_eq!(
                cursor.position() as usize,
                buf.len(),
                "cursor fully consumed for {value}"
            );
        }
    }

    #[test]
    fn vlong_empty_input_errors() {
        let mut cursor = Cursor::new(&[][..]);
        assert_err!(read_vlong(&mut cursor));
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn vlong_truncated_continuation_errors() {
        let mut cursor = Cursor::new(&[0x80, 0x80, 0x80][..]);
        assert_err!(read_vlong(&mut cursor));
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn vlong_exceeds_max_bytes_errors() {
        let mut cursor = Cursor::new(&[0x80; 11][..]);
        assert_err!(read_vlong(&mut cursor));
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn vlong_consecutive_reads_advance_cursor() {
        let mut buf = Vec::new();
        write_vlong(&mut buf, 42).unwrap();
        write_vlong(&mut buf, 128).unwrap();
        write_vlong(&mut buf, i64::MAX).unwrap();
        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(read_vlong(&mut cursor).unwrap(), 42);
        assert_eq!(read_vlong(&mut cursor).unwrap(), 128);
        assert_eq!(read_vlong(&mut cursor).unwrap(), i64::MAX);
        assert_eq!(cursor.position() as usize, buf.len());
    }

    // read_zint

    #[test]
    fn zint_roundtrip_values() {
        for &value in &[0i32, 1, -1, 127, -128, i32::MIN, i32::MAX, 42, -42] {
            let buf = encode(|b| write_zint(b, value));
            let mut cursor = Cursor::new(&buf[..]);
            assert_eq!(read_zint(&mut cursor).unwrap(), value, "value = {value}");
            assert_eq!(
                cursor.position() as usize,
                buf.len(),
                "cursor fully consumed for {value}"
            );
        }
    }

    #[test]
    fn zint_propagates_vint_error() {
        let mut cursor = Cursor::new(&[][..]);
        assert_err!(read_zint(&mut cursor));
        assert_eq!(cursor.position(), 0);
    }

    // read_zlong

    #[test]
    fn zlong_roundtrip_values() {
        for &value in &[0i64, 1, -1, 127, -128, i64::MIN, i64::MAX, 42, -42] {
            let buf = encode(|b| write_zlong(b, value));
            let mut cursor = Cursor::new(&buf[..]);
            assert_eq!(read_zlong(&mut cursor).unwrap(), value, "value = {value}");
            assert_eq!(
                cursor.position() as usize,
                buf.len(),
                "cursor fully consumed for {value}"
            );
        }
    }

    #[test]
    fn zlong_propagates_vlong_error() {
        let mut cursor = Cursor::new(&[][..]);
        assert_err!(read_zlong(&mut cursor));
        assert_eq!(cursor.position(), 0);
    }
}
