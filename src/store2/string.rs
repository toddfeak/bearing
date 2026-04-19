// SPDX-License-Identifier: Apache-2.0

//! Cursor-based decoders for VInt-prefixed strings and string collections.
//!
//! Each parser advances the cursor by the bytes it consumes. On error the
//! cursor is left at the point the failure was detected (consistent with
//! Lucene's `DataInput` semantics and the existing `crate::encoding::string`
//! reader); a corrupt file is expected to abort the surrounding read, not
//! be resumed from the failed cursor.

use std::collections::HashMap;
use std::io;
use std::io::BufRead;
use std::io::Cursor;
use std::str;

use crate::store2::varint;

/// Reads a VInt-prefixed UTF-8 string.
pub fn read_string(cursor: &mut Cursor<&[u8]>) -> io::Result<String> {
    let len = varint::read_vint(cursor)?;
    let len = usize::try_from(len).map_err(|_| io::Error::other("negative string length"))?;
    let buf = cursor.fill_buf()?;
    if buf.len() < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated string body",
        ));
    }
    let s = str::from_utf8(&buf[..len])
        .map_err(|e| io::Error::other(format!("invalid UTF-8: {e}")))?
        .to_owned();
    cursor.consume(len);
    Ok(s)
}

/// Reads a VInt count followed by that many UTF-8 strings.
pub fn read_set_of_strings(cursor: &mut Cursor<&[u8]>) -> io::Result<Vec<String>> {
    let count = read_count(cursor, "set")?;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(read_string(cursor)?);
    }
    Ok(out)
}

/// Reads a VInt count followed by that many key/value UTF-8 string pairs.
pub fn read_map_of_strings(cursor: &mut Cursor<&[u8]>) -> io::Result<HashMap<String, String>> {
    let count = read_count(cursor, "map")?;
    let mut out = HashMap::with_capacity(count);
    for _ in 0..count {
        let key = read_string(cursor)?;
        let value = read_string(cursor)?;
        out.insert(key, value);
    }
    Ok(out)
}

fn read_count(cursor: &mut Cursor<&[u8]>, label: &str) -> io::Result<usize> {
    let count = varint::read_vint(cursor)?;
    usize::try_from(count).map_err(|_| io::Error::other(format!("negative {label} count")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::string::{write_map_of_strings, write_set_of_strings, write_string};
    use crate::encoding::varint::write_vint;

    fn encode<F>(f: F) -> Vec<u8>
    where
        F: FnOnce(&mut Vec<u8>) -> io::Result<()>,
    {
        let mut buf = Vec::new();
        f(&mut buf).unwrap();
        buf
    }

    // read_string

    #[test]
    fn string_roundtrip_values() {
        for s in &["", "a", "hello", "hello world", "\u{00e9}\u{00e8}"] {
            let buf = encode(|b| write_string(b, s));
            let mut cursor = Cursor::new(&buf[..]);
            assert_eq!(read_string(&mut cursor).unwrap(), *s, "value = {s:?}");
            assert_eq!(
                cursor.position() as usize,
                buf.len(),
                "cursor fully consumed for {s:?}"
            );
        }
    }

    #[test]
    fn string_empty_input_errors() {
        let mut cursor = Cursor::new(&[][..]);
        assert_err!(read_string(&mut cursor));
    }

    #[test]
    fn string_negative_length_errors() {
        let buf = encode(|b| write_vint(b, -1));
        let mut cursor = Cursor::new(&buf[..]);
        assert_err!(read_string(&mut cursor));
    }

    #[test]
    fn string_truncated_body_errors() {
        // VInt(5) says length 5, but only 3 body bytes present
        let buf = [0x05u8, b'a', b'b', b'c'];
        let mut cursor = Cursor::new(&buf[..]);
        assert_err!(read_string(&mut cursor));
    }

    #[test]
    fn string_invalid_utf8_errors() {
        // VInt(2) + invalid UTF-8 bytes
        let buf = [0x02u8, 0xFF, 0xFE];
        let mut cursor = Cursor::new(&buf[..]);
        assert_err!(read_string(&mut cursor));
    }

    #[test]
    fn string_consecutive_reads_advance_cursor() {
        let mut buf = Vec::new();
        write_string(&mut buf, "alpha").unwrap();
        write_string(&mut buf, "beta").unwrap();
        write_string(&mut buf, "gamma").unwrap();
        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(read_string(&mut cursor).unwrap(), "alpha");
        assert_eq!(read_string(&mut cursor).unwrap(), "beta");
        assert_eq!(read_string(&mut cursor).unwrap(), "gamma");
        assert_eq!(cursor.position() as usize, buf.len());
    }

    // read_set_of_strings

    #[test]
    fn set_roundtrip_empty() {
        let buf = encode(|b| write_set_of_strings(b, &[]));
        let mut cursor = Cursor::new(&buf[..]);
        let decoded = read_set_of_strings(&mut cursor).unwrap();
        assert_is_empty!(&decoded);
        assert_eq!(cursor.position() as usize, buf.len());
    }

    #[test]
    fn set_roundtrip_single() {
        let set = vec!["only".to_string()];
        let buf = encode(|b| write_set_of_strings(b, &set));
        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(read_set_of_strings(&mut cursor).unwrap(), set);
        assert_eq!(cursor.position() as usize, buf.len());
    }

    #[test]
    fn set_roundtrip_multiple() {
        let set = vec!["one".to_string(), "two".to_string(), "three".to_string()];
        let buf = encode(|b| write_set_of_strings(b, &set));
        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(read_set_of_strings(&mut cursor).unwrap(), set);
        assert_eq!(cursor.position() as usize, buf.len());
    }

    #[test]
    fn set_empty_input_errors() {
        let mut cursor = Cursor::new(&[][..]);
        assert_err!(read_set_of_strings(&mut cursor));
    }

    #[test]
    fn set_negative_count_errors() {
        let buf = encode(|b| write_vint(b, -1));
        let mut cursor = Cursor::new(&buf[..]);
        assert_err!(read_set_of_strings(&mut cursor));
    }

    #[test]
    fn set_truncated_mid_element_errors() {
        // Count=3, first element "one" successfully read, second element truncated.
        let mut buf = Vec::new();
        write_vint(&mut buf, 3).unwrap();
        write_string(&mut buf, "one").unwrap();
        buf.push(0x05); // second element claims length 5
        buf.extend_from_slice(b"ab"); // only 2 body bytes
        let mut cursor = Cursor::new(&buf[..]);
        assert_err!(read_set_of_strings(&mut cursor));
    }

    // read_map_of_strings

    #[test]
    fn map_roundtrip_empty() {
        let buf = encode(|b| write_map_of_strings(b, &HashMap::new()));
        let mut cursor = Cursor::new(&buf[..]);
        let decoded = read_map_of_strings(&mut cursor).unwrap();
        assert_is_empty!(&decoded);
        assert_eq!(cursor.position() as usize, buf.len());
    }

    #[test]
    fn map_roundtrip_single() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), "val".to_string());
        let buf = encode(|b| write_map_of_strings(b, &map));
        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(read_map_of_strings(&mut cursor).unwrap(), map);
        assert_eq!(cursor.position() as usize, buf.len());
    }

    #[test]
    fn map_roundtrip_multiple() {
        let mut map = HashMap::new();
        map.insert("k1".to_string(), "v1".to_string());
        map.insert("k2".to_string(), "v2".to_string());
        map.insert("k3".to_string(), "v3".to_string());
        let buf = encode(|b| write_map_of_strings(b, &map));
        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(read_map_of_strings(&mut cursor).unwrap(), map);
        assert_eq!(cursor.position() as usize, buf.len());
    }

    #[test]
    fn map_empty_input_errors() {
        let mut cursor = Cursor::new(&[][..]);
        assert_err!(read_map_of_strings(&mut cursor));
    }

    #[test]
    fn map_negative_count_errors() {
        let buf = encode(|b| write_vint(b, -1));
        let mut cursor = Cursor::new(&buf[..]);
        assert_err!(read_map_of_strings(&mut cursor));
    }

    #[test]
    fn map_truncated_mid_value_errors() {
        // Count=2, first key+value ok, second key ok, second value truncated.
        let mut buf = Vec::new();
        write_vint(&mut buf, 2).unwrap();
        write_string(&mut buf, "k1").unwrap();
        write_string(&mut buf, "v1").unwrap();
        write_string(&mut buf, "k2").unwrap();
        buf.push(0x04); // second value claims length 4
        buf.push(b'x'); // only 1 body byte present
        let mut cursor = Cursor::new(&buf[..]);
        assert_err!(read_map_of_strings(&mut cursor));
    }
}
