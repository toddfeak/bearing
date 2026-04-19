// SPDX-License-Identifier: Apache-2.0

//! Footer-based CRC32 integrity check for codec-framed files.
//!
//! A codec-framed file ends with a 16-byte footer:
//!   - 4 bytes: footer magic (big-endian [`FOOTER_MAGIC`])
//!   - 4 bytes: algorithm ID (big-endian `0` = zlib CRC32)
//!   - 8 bytes: stored CRC32 (big-endian) over every preceding byte
//!
//! [`verify_checksum`] validates all three fields in one pass.
//!
//! # Intended final home
//!
//! This module lives under `store2` only during the read-path migration.
//! The footer format is codec-specific wire format and its final home is
//! `src/codecs/codec_util.rs`, alongside `write_footer`. See
//! `docs/backlog/read_path_migration.md` ("Final Cleanup Commit").

use std::io;

use crate::store::checksum::CRC32;

/// Footer length in bytes: 4 (magic) + 4 (algorithm ID) + 8 (stored CRC).
pub const FOOTER_LENGTH: usize = 16;

/// Footer magic (big-endian `i32`). Bitwise NOT of the codec header magic
/// (`0x3FD76C17`).
pub const FOOTER_MAGIC: i32 = !(0x3FD76C17_u32 as i32);

/// Verifies the trailing codec footer of `bytes`.
///
/// Checks, in order:
///   1. `bytes.len() >= FOOTER_LENGTH`.
///   2. Footer magic matches [`FOOTER_MAGIC`].
///   3. Algorithm ID is `0` (zlib CRC32).
///   4. The stored CRC equals a CRC32 computed over `bytes[..bytes.len() - 8]`.
///
/// Returns `Ok(())` on success. Returns the first failing check as `Err`.
pub fn verify_checksum(bytes: &[u8]) -> io::Result<()> {
    if bytes.len() < FOOTER_LENGTH {
        return Err(io::Error::other(format!(
            "file too short for footer: {} < {FOOTER_LENGTH}",
            bytes.len()
        )));
    }

    let footer_start = bytes.len() - FOOTER_LENGTH;
    let magic = read_be_i32(&bytes[footer_start..footer_start + 4]);
    if magic != FOOTER_MAGIC {
        return Err(io::Error::other(format!(
            "footer magic mismatch: expected 0x{:08X}, got 0x{:08X}",
            FOOTER_MAGIC as u32, magic as u32,
        )));
    }

    let algorithm_id = read_be_i32(&bytes[footer_start + 4..footer_start + 8]);
    if algorithm_id != 0 {
        return Err(io::Error::other(format!(
            "unsupported checksum algorithm: {algorithm_id}"
        )));
    }

    let stored_crc = read_be_i64(&bytes[footer_start + 8..footer_start + 16]) as u64;

    let mut crc = CRC32::new();
    crc.update(&bytes[..bytes.len() - 8]);
    let computed = crc.value();

    if computed != stored_crc {
        return Err(io::Error::other(format!(
            "checksum mismatch: stored=0x{stored_crc:016X}, computed=0x{computed:016X}",
        )));
    }

    Ok(())
}

fn read_be_i32(bytes: &[u8]) -> i32 {
    let arr: [u8; 4] = bytes.try_into().expect("slice of length 4");
    i32::from_be_bytes(arr)
}

fn read_be_i64(bytes: &[u8]) -> i64 {
    let arr: [u8; 8] = bytes.try_into().expect("slice of length 8");
    i64::from_be_bytes(arr)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a valid codec-framed buffer: `data` followed by a correct footer.
    fn with_valid_footer(data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(data.len() + FOOTER_LENGTH);
        buf.extend_from_slice(data);
        buf.extend_from_slice(&FOOTER_MAGIC.to_be_bytes());
        buf.extend_from_slice(&0i32.to_be_bytes());
        let mut crc = CRC32::new();
        crc.update(&buf);
        buf.extend_from_slice(&(crc.value() as i64).to_be_bytes());
        buf
    }

    #[test]
    fn valid_footer_over_nonempty_data() {
        let bytes = with_valid_footer(b"hello world");
        verify_checksum(&bytes).unwrap();
    }

    #[test]
    fn valid_footer_over_empty_data() {
        let bytes = with_valid_footer(b"");
        assert_len_eq_x!(&bytes, FOOTER_LENGTH);
        verify_checksum(&bytes).unwrap();
    }

    #[test]
    fn too_short_errors() {
        assert_err!(verify_checksum(&[]));
        assert_err!(verify_checksum(&[0u8; 15]));
    }

    #[test]
    fn bad_magic_errors() {
        let mut bytes = with_valid_footer(b"hello");
        let footer_start = bytes.len() - FOOTER_LENGTH;
        bytes[footer_start] ^= 0xFF;
        assert_err!(verify_checksum(&bytes));
    }

    #[test]
    fn bad_algorithm_errors() {
        let mut bytes = with_valid_footer(b"hello");
        let footer_start = bytes.len() - FOOTER_LENGTH;
        bytes[footer_start + 4] = 0xFF;
        assert_err!(verify_checksum(&bytes));
    }

    #[test]
    fn wrong_stored_crc_errors() {
        let mut bytes = with_valid_footer(b"hello");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert_err!(verify_checksum(&bytes));
    }

    #[test]
    fn corrupted_data_errors() {
        let mut bytes = with_valid_footer(b"hello");
        bytes[0] ^= 0xFF;
        assert_err!(verify_checksum(&bytes));
    }
}
