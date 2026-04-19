// SPDX-License-Identifier: Apache-2.0

//! Codec header validation against [`IndexInput`].
//!
//! Mirrors `codecs::codec_util::{check_header, check_index_header}` but reads
//! through the new [`IndexInput`] struct instead of `&mut dyn DataInput`.
//!
//! # Intended final home
//!
//! These helpers live under `store2` only during the read-path migration.
//! The codec header format is codec-specific wire format and its final home
//! is `src/codecs/codec_util.rs`, alongside `write_header` /
//! `write_index_header`. See `docs/backlog/read_path_migration.md`
//! ("Final Cleanup Commit").

use std::io;

use crate::store2::IndexInput;

/// Magic number written at the start of every codec header (big-endian).
pub const CODEC_MAGIC: i32 = 0x3FD76C17_u32 as i32;

/// Length of the segment ID in bytes.
pub const ID_LENGTH: usize = 16;

/// Reads and validates a codec header, returning the version.
///
/// Checks, in order:
///   1. Magic matches [`CODEC_MAGIC`].
///   2. Codec name matches `codec`.
///   3. Version is in `[min_version, max_version]`.
pub fn check_header(
    input: &mut IndexInput<'_>,
    codec: &str,
    min_version: i32,
    max_version: i32,
) -> io::Result<i32> {
    let actual_magic = input.read_be_int()?;
    if actual_magic != CODEC_MAGIC {
        return Err(io::Error::other(format!(
            "codec header mismatch: expected 0x{CODEC_MAGIC:08X}, got 0x{actual_magic:08X}"
        )));
    }

    let actual_codec = input.read_string()?;
    if actual_codec != codec {
        return Err(io::Error::other(format!(
            "codec mismatch: expected {codec:?}, got {actual_codec:?}"
        )));
    }

    let version = input.read_be_int()?;
    if version < min_version || version > max_version {
        return Err(io::Error::other(format!(
            "version {version} out of range [{min_version}, {max_version}] for codec {codec:?}"
        )));
    }

    Ok(version)
}

/// Reads and validates an index header (header + segment ID + suffix), returning the version.
pub fn check_index_header(
    input: &mut IndexInput<'_>,
    codec: &str,
    min_version: i32,
    max_version: i32,
    expected_id: &[u8; ID_LENGTH],
    expected_suffix: &str,
) -> io::Result<i32> {
    let version = check_header(input, codec, min_version, max_version)?;

    let mut actual_id = [0u8; ID_LENGTH];
    input.read_bytes(&mut actual_id)?;
    if actual_id != *expected_id {
        return Err(io::Error::other(format!(
            "segment ID mismatch: expected {expected_id:02x?}, got {actual_id:02x?}"
        )));
    }

    let suffix_len = input.read_byte()? as usize;
    let mut suffix_bytes = vec![0u8; suffix_len];
    input.read_bytes(&mut suffix_bytes)?;
    let actual_suffix =
        String::from_utf8(suffix_bytes).map_err(|e| io::Error::other(e.to_string()))?;
    if actual_suffix != expected_suffix {
        return Err(io::Error::other(format!(
            "suffix mismatch: expected {expected_suffix:?}, got {actual_suffix:?}"
        )));
    }

    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;

    /// Builds a valid codec header: magic + vint length + name + version.
    fn build_header(codec: &str, version: i32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&CODEC_MAGIC.to_be_bytes());
        // VInt of length 1: codec name length is < 128 in our tests
        let name_bytes = codec.as_bytes();
        assert!(name_bytes.len() < 128);
        buf.push(name_bytes.len() as u8);
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&version.to_be_bytes());
        buf
    }

    /// Builds a valid index header: header + 16-byte ID + 1-byte suffix length + suffix.
    fn build_index_header(
        codec: &str,
        version: i32,
        id: &[u8; ID_LENGTH],
        suffix: &str,
    ) -> Vec<u8> {
        let mut buf = build_header(codec, version);
        buf.extend_from_slice(id);
        let suffix_bytes = suffix.as_bytes();
        assert!(suffix_bytes.len() <= 255);
        buf.push(suffix_bytes.len() as u8);
        buf.extend_from_slice(suffix_bytes);
        buf
    }

    #[test]
    fn check_header_returns_version_on_match() {
        let bytes = build_header("MyCodec", 7);
        let mut input = IndexInput::new("test", &bytes);
        assert_ok_eq_x!(check_header(&mut input, "MyCodec", 5, 10), 7);
    }

    #[test]
    fn check_header_min_max_boundaries() {
        let bytes = build_header("MyCodec", 5);
        let mut input = IndexInput::new("test", &bytes);
        assert_ok!(check_header(&mut input, "MyCodec", 5, 10));

        let bytes = build_header("MyCodec", 10);
        let mut input = IndexInput::new("test", &bytes);
        assert_ok!(check_header(&mut input, "MyCodec", 5, 10));
    }

    #[test]
    fn check_header_bad_magic() {
        let mut bytes = build_header("MyCodec", 7);
        bytes[0] ^= 0xFF;
        let mut input = IndexInput::new("test", &bytes);
        assert_err!(check_header(&mut input, "MyCodec", 5, 10));
    }

    #[test]
    fn check_header_wrong_codec_name() {
        let bytes = build_header("MyCodec", 7);
        let mut input = IndexInput::new("test", &bytes);
        assert_err!(check_header(&mut input, "OtherCodec", 5, 10));
    }

    #[test]
    fn check_header_version_below_min() {
        let bytes = build_header("MyCodec", 4);
        let mut input = IndexInput::new("test", &bytes);
        assert_err!(check_header(&mut input, "MyCodec", 5, 10));
    }

    #[test]
    fn check_header_version_above_max() {
        let bytes = build_header("MyCodec", 11);
        let mut input = IndexInput::new("test", &bytes);
        assert_err!(check_header(&mut input, "MyCodec", 5, 10));
    }

    #[test]
    fn check_index_header_returns_version_on_match() {
        let id = [0xABu8; ID_LENGTH];
        let bytes = build_index_header("MyCodec", 7, &id, "suffix1");
        let mut input = IndexInput::new("test", &bytes);
        let version = check_index_header(&mut input, "MyCodec", 5, 10, &id, "suffix1").unwrap();
        assert_eq!(version, 7);
    }

    #[test]
    fn check_index_header_id_mismatch() {
        let id = [0xABu8; ID_LENGTH];
        let other = [0xCDu8; ID_LENGTH];
        let bytes = build_index_header("MyCodec", 7, &id, "suffix1");
        let mut input = IndexInput::new("test", &bytes);
        assert_err!(check_index_header(
            &mut input, "MyCodec", 5, 10, &other, "suffix1"
        ));
    }

    #[test]
    fn check_index_header_suffix_mismatch() {
        let id = [0xABu8; ID_LENGTH];
        let bytes = build_index_header("MyCodec", 7, &id, "suffix1");
        let mut input = IndexInput::new("test", &bytes);
        assert_err!(check_index_header(
            &mut input, "MyCodec", 5, 10, &id, "suffix2"
        ));
    }

    #[test]
    fn check_index_header_empty_suffix() {
        let id = [0u8; ID_LENGTH];
        let bytes = build_index_header("MyCodec", 7, &id, "");
        let mut input = IndexInput::new("test", &bytes);
        assert_ok!(check_index_header(&mut input, "MyCodec", 5, 10, &id, ""));
    }
}
