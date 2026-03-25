// SPDX-License-Identifier: Apache-2.0

//! VInt-prefixed string encoding/decoding.
//!
//! Strings are encoded as a VInt byte length followed by the UTF-8 bytes.
//! Collections (sets, maps) are encoded as a VInt count followed by each element.

use crate::encoding::varint;
use std::collections::HashMap;
use std::io;

/// Writes a string as VInt-encoded byte length followed by UTF-8 bytes.
pub fn write_string(out: &mut impl io::Write, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    let len = i32::try_from(bytes.len())
        .map_err(|_| io::Error::other("string length exceeds i32::MAX"))?;
    varint::write_vint(out, len)?;
    out.write_all(bytes)
}

/// Reads a string: VInt-encoded byte length followed by UTF-8 bytes.
pub fn read_string(reader: &mut impl io::Read) -> io::Result<String> {
    let len = varint::read_vint(reader)?;
    let len = usize::try_from(len).map_err(|_| io::Error::other("negative string length"))?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::other(e.to_string()))
}

/// Writes a set of strings: VInt count followed by each string.
pub fn write_set_of_strings(out: &mut impl io::Write, set: &[String]) -> io::Result<()> {
    let count =
        i32::try_from(set.len()).map_err(|_| io::Error::other("set size exceeds i32::MAX"))?;
    varint::write_vint(out, count)?;
    for s in set {
        write_string(out, s)?;
    }
    Ok(())
}

/// Reads a set of strings: VInt count followed by each string.
pub fn read_set_of_strings(reader: &mut impl io::Read) -> io::Result<Vec<String>> {
    let count = varint::read_vint(reader)?;
    let count = usize::try_from(count).map_err(|_| io::Error::other("negative set count"))?;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        result.push(read_string(reader)?);
    }
    Ok(result)
}

/// Writes a map of strings: VInt count followed by key-value pairs.
pub fn write_map_of_strings(
    out: &mut impl io::Write,
    map: &HashMap<String, String>,
) -> io::Result<()> {
    let count =
        i32::try_from(map.len()).map_err(|_| io::Error::other("map size exceeds i32::MAX"))?;
    varint::write_vint(out, count)?;
    for (k, v) in map {
        write_string(out, k)?;
        write_string(out, v)?;
    }
    Ok(())
}

/// Reads a map of strings: VInt count followed by key-value pairs.
pub fn read_map_of_strings(reader: &mut impl io::Read) -> io::Result<HashMap<String, String>> {
    let count = varint::read_vint(reader)?;
    let count = usize::try_from(count).map_err(|_| io::Error::other("negative map count"))?;
    let mut result = HashMap::with_capacity(count);
    for _ in 0..count {
        let key = read_string(reader)?;
        let value = read_string(reader)?;
        result.insert(key, value);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_string() {
        let mut buf = Vec::new();
        write_string(&mut buf, "hello").unwrap();
        // VInt(5) = 0x05, then "hello" as UTF-8
        assert_eq!(buf, [0x05, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_read_string_roundtrip() {
        for s in &["", "hello", "a", "hello world", "\u{00e9}\u{00e8}"] {
            let mut buf = Vec::new();
            write_string(&mut buf, s).unwrap();
            let mut cursor = &buf[..];
            let decoded = read_string(&mut cursor).unwrap();
            assert_eq!(&decoded, s);
            assert_is_empty!(cursor);
        }
    }

    #[test]
    fn test_write_set_of_strings_empty() {
        let mut buf = Vec::new();
        write_set_of_strings(&mut buf, &[]).unwrap();
        assert_eq!(buf, [0x00]); // vint(0)
    }

    #[test]
    fn test_write_set_of_strings() {
        let mut buf = Vec::new();
        let set = vec!["hello".to_string(), "world".to_string()];
        write_set_of_strings(&mut buf, &set).unwrap();
        // vint(2) + string("hello") + string("world")
        let mut expected = vec![0x02]; // count = 2
        expected.extend_from_slice(&[0x05, b'h', b'e', b'l', b'l', b'o']); // "hello"
        expected.extend_from_slice(&[0x05, b'w', b'o', b'r', b'l', b'd']); // "world"
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_read_set_of_strings_roundtrip() {
        let set = vec!["hello".to_string(), "world".to_string()];
        let mut buf = Vec::new();
        write_set_of_strings(&mut buf, &set).unwrap();
        let mut cursor = &buf[..];
        let decoded = read_set_of_strings(&mut cursor).unwrap();
        assert_eq!(decoded, set);
        assert_is_empty!(cursor);
    }

    #[test]
    fn test_read_set_of_strings_empty() {
        let mut buf = Vec::new();
        write_set_of_strings(&mut buf, &[]).unwrap();
        let mut cursor = &buf[..];
        let decoded = read_set_of_strings(&mut cursor).unwrap();
        assert_is_empty!(&decoded);
    }

    #[test]
    fn test_write_map_of_strings_empty() {
        let mut buf = Vec::new();
        let map = HashMap::new();
        write_map_of_strings(&mut buf, &map).unwrap();
        assert_eq!(buf, [0x00]); // vint(0)
    }

    #[test]
    fn test_write_map_of_strings() {
        let mut buf = Vec::new();
        let mut map = HashMap::new();
        map.insert("key".to_string(), "val".to_string());
        write_map_of_strings(&mut buf, &map).unwrap();
        // vint(1) + string("key") + string("val")
        assert_eq!(buf[0], 0x01); // count = 1
        assert_eq!(&buf[1..], &[0x03, b'k', b'e', b'y', 0x03, b'v', b'a', b'l']);
    }

    #[test]
    fn test_read_map_of_strings_roundtrip() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), "val1".to_string());
        map.insert("key2".to_string(), "val2".to_string());
        let mut buf = Vec::new();
        write_map_of_strings(&mut buf, &map).unwrap();
        let mut cursor = &buf[..];
        let decoded = read_map_of_strings(&mut cursor).unwrap();
        assert_eq!(decoded, map);
        assert_is_empty!(cursor);
    }

    #[test]
    fn test_read_map_of_strings_empty() {
        let mut buf = Vec::new();
        write_map_of_strings(&mut buf, &HashMap::new()).unwrap();
        let mut cursor = &buf[..];
        let decoded = read_map_of_strings(&mut cursor).unwrap();
        assert_is_empty!(&decoded);
    }
}
