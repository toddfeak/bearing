// SPDX-License-Identifier: Apache-2.0

//! VInt-prefixed string encoding.
//!
//! Strings are encoded as a VInt byte length followed by the UTF-8 bytes.
//! Collections (sets, maps) are encoded as a VInt count followed by each element.

use crate::encoding::varint;
use std::collections::HashMap;
use std::io;

/// Writes a string as VInt-encoded byte length followed by UTF-8 bytes.
pub fn write_string(out: &mut impl io::Write, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    varint::write_vint(out, bytes.len() as i32)?;
    out.write_all(bytes)
}

/// Writes a set of strings: VInt count followed by each string.
pub fn write_set_of_strings(out: &mut impl io::Write, set: &[String]) -> io::Result<()> {
    varint::write_vint(out, set.len() as i32)?;
    for s in set {
        write_string(out, s)?;
    }
    Ok(())
}

/// Writes a map of strings: VInt count followed by key-value pairs.
pub fn write_map_of_strings(
    out: &mut impl io::Write,
    map: &HashMap<String, String>,
) -> io::Result<()> {
    varint::write_vint(out, map.len() as i32)?;
    for (k, v) in map {
        write_string(out, k)?;
        write_string(out, v)?;
    }
    Ok(())
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
}
