// SPDX-License-Identifier: Apache-2.0
//! Utilities for writing codec headers and footers with CRC32 integrity checks.

use std::io;

use log::debug;

use crate::store::{DataOutput, IndexOutput};

#[cfg(test)]
use crate::store::checksum_input::ChecksumIndexInput;
#[cfg(test)]
use crate::store::{DataInput, IndexInput};

/// Magic number written at the start of every codec header (big-endian).
pub const CODEC_MAGIC: i32 = 0x3fd76c17_u32 as i32;

/// Magic number written at the start of every codec footer (big-endian).
/// This is the bitwise NOT of CODEC_MAGIC.
pub const FOOTER_MAGIC: i32 = !CODEC_MAGIC;

/// Length of the footer in bytes: 4 (magic) + 4 (algorithm) + 8 (checksum) = 16.
pub const FOOTER_LENGTH: usize = 16;

/// Length of the segment ID in bytes.
pub const ID_LENGTH: usize = 16;

/// Writes a codec header.
///
/// Format (all big-endian):
///   - 4 bytes: CODEC_MAGIC (BE int)
///   - N bytes: codec name (VInt length + UTF-8 string)
///   - 4 bytes: version (BE int)
///
/// Returns the number of bytes written (= 9 + codec.len()).
pub fn write_header(out: &mut dyn DataOutput, codec: &str, version: i32) -> io::Result<usize> {
    validate_codec_name(codec)?;
    out.write_be_int(CODEC_MAGIC)?;
    out.write_string(codec)?;
    out.write_be_int(version)?;
    Ok(header_length(codec))
}

/// Writes an index header (header + segment ID + suffix).
///
/// Format:
///   - header (write_header)
///   - 16 bytes: segment ID
///   - 1 byte: suffix length
///   - N bytes: suffix bytes
///
/// Returns the number of bytes written.
pub fn write_index_header(
    out: &mut dyn DataOutput,
    codec: &str,
    version: i32,
    id: &[u8; ID_LENGTH],
    suffix: &str,
) -> io::Result<usize> {
    write_header(out, codec, version)?;
    out.write_bytes(id)?;
    let suffix_bytes = suffix.as_bytes();
    if suffix_bytes.len() > 255 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("suffix too long: {}", suffix_bytes.len()),
        ));
    }
    out.write_byte(suffix_bytes.len() as u8)?;
    out.write_bytes(suffix_bytes)?;
    debug!(
        "write_index_header: codec={codec:?}, version={version}, suffix={suffix:?}, id={id:02x?}"
    );
    Ok(index_header_length(codec, suffix))
}

/// Writes the codec footer: magic (BE), algorithm ID 0 (BE), CRC32 (BE long).
/// The CRC32 covers all bytes written to the output before the footer, plus
/// the first 8 bytes of the footer itself (magic + algorithm).
pub fn write_footer(out: &mut dyn IndexOutput) -> io::Result<()> {
    out.write_be_int(FOOTER_MAGIC)?;
    out.write_be_int(0)?; // algorithm ID = 0 (zlib crc32)
    let checksum = out.checksum();
    debug!("write_footer: checksum=0x{checksum:08x}");
    out.write_be_long(checksum as i64)?;
    Ok(())
}

/// Returns the byte length of a codec header for the given codec name.
/// 4 (magic) + 1+ (vint string length) + codec.len() + 4 (version)
/// For ASCII codec names < 128 chars, the VInt is 1 byte.
pub fn header_length(codec: &str) -> usize {
    // 4 (magic BE int) + vint_size(codec.len()) + codec.len() + 4 (version BE int)
    4 + vint_size(codec.len() as u32) + codec.len() + 4
}

/// Returns the byte length of an index header.
pub fn index_header_length(codec: &str, suffix: &str) -> usize {
    header_length(codec) + ID_LENGTH + 1 + suffix.len()
}

/// Returns the number of bytes needed to encode a value as a VInt.
fn vint_size(mut val: u32) -> usize {
    let mut size = 1;
    while val > 0x7F {
        val >>= 7;
        size += 1;
    }
    size
}

/// Reads and validates a codec header, returning the version.
///
/// Checks that:
///   - The magic number matches [`CODEC_MAGIC`]
///   - The codec name matches `codec`
///   - The version is in `[min_version, max_version]`
#[cfg(test)]
pub fn check_header(
    input: &mut dyn DataInput,
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
#[cfg(test)]
pub fn check_index_header(
    input: &mut dyn DataInput,
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

/// Validates a codec footer: checks magic, algorithm ID, and CRC32.
///
/// The input must be a [`ChecksumIndexInput`] positioned just before the footer.
/// The footer is 16 bytes: magic (BE int) + algorithm ID (BE int) + CRC32 (BE long).
#[cfg(test)]
pub fn check_footer(input: &mut ChecksumIndexInput) -> io::Result<()> {
    let remaining = input.length() - input.file_pointer();
    if remaining != FOOTER_LENGTH as u64 {
        return Err(io::Error::other(format!(
            "expected {FOOTER_LENGTH} footer bytes remaining, got {remaining}"
        )));
    }

    let magic = input.read_be_int()?;
    if magic != FOOTER_MAGIC {
        return Err(io::Error::other(format!(
            "footer magic mismatch: expected 0x{:08X}, got 0x{magic:08X}",
            FOOTER_MAGIC as u32
        )));
    }

    let algorithm_id = input.read_be_int()?;
    if algorithm_id != 0 {
        return Err(io::Error::other(format!(
            "unsupported checksum algorithm: {algorithm_id}"
        )));
    }

    let checksum_before_crc = input.checksum();
    let stored_checksum = input.read_be_long()? as u64;
    if stored_checksum != checksum_before_crc {
        return Err(io::Error::other(format!(
            "checksum mismatch: stored 0x{stored_checksum:08X}, computed 0x{checksum_before_crc:08X}"
        )));
    }

    Ok(())
}

/// Computes the CRC32 checksum of an entire file.
///
/// Seeks to the start, reads all bytes through a [`ChecksumIndexInput`],
/// and returns the checksum.
#[cfg(test)]
pub fn checksum_entire_file(input: Box<dyn IndexInput>) -> io::Result<u64> {
    let len = input.length();
    let mut checksum_input = ChecksumIndexInput::new(input);
    checksum_input.seek(0)?;
    checksum_input.skip_bytes(len)?;
    Ok(checksum_input.checksum())
}

/// Validates that a codec name is simple ASCII and not too long.
fn validate_codec_name(codec: &str) -> io::Result<()> {
    if codec.len() >= 128 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("codec name too long: {}", codec.len()),
        ));
    }
    if !codec.bytes().all(|b| b.is_ascii_graphic() || b == b' ') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "codec name must be simple ASCII",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::byte_slice_input::ByteSliceIndexInput;
    use crate::store::memory::MemoryIndexOutput;

    // Ported from org.apache.lucene.codecs.TestCodecUtil

    #[test]
    fn test_header_length() {
        assert_eq!(header_length("FooBar"), 9 + 6);
    }

    #[test]
    fn test_write_header() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let len = write_header(&mut out, "FooBar", 5).unwrap();
        let bytes = out.bytes();

        assert_eq!(len, 15);
        assert_len_eq_x!(&bytes, 15);

        // Magic (BE): 0x3fd76c17
        assert_eq!(bytes[0], 0x3f);
        assert_eq!(bytes[1], 0xd7);
        assert_eq!(bytes[2], 0x6c);
        assert_eq!(bytes[3], 0x17);

        // String: VInt(6) = 0x06, then "FooBar"
        assert_eq!(bytes[4], 6);
        assert_eq!(&bytes[5..11], b"FooBar");

        // Version (BE): 5
        assert_eq!(bytes[11], 0);
        assert_eq!(bytes[12], 0);
        assert_eq!(bytes[13], 0);
        assert_eq!(bytes[14], 5);
    }

    #[test]
    fn test_write_index_header() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let id = [1u8; 16];
        let len = write_index_header(&mut out, "FooBar", 5, &id, "xyz").unwrap();
        let bytes = out.bytes();

        // header(15) + 16(id) + 1(suffix len) + 3(suffix) = 35
        assert_eq!(len, 35);
        assert_len_eq_x!(&bytes, 35);

        // ID starts at byte 15
        assert_eq!(&bytes[15..31], &[1u8; 16]);

        // Suffix length at byte 31
        assert_eq!(bytes[31], 3);

        // Suffix at bytes 32..35
        assert_eq!(&bytes[32..35], b"xyz");
    }

    #[test]
    fn test_write_footer() {
        let mut out = MemoryIndexOutput::new("test".to_string());

        // Write some data first
        out.write_bytes(b"hello").unwrap();

        write_footer(&mut out).unwrap();
        let bytes = out.bytes();

        // Total: 5 (data) + 16 (footer) = 21
        assert_len_eq_x!(&bytes, 21);

        // Footer magic (BE): ~0x3fd76c17 = 0xC02893E8
        let footer_start = 5;
        assert_eq!(bytes[footer_start], 0xc0);
        assert_eq!(bytes[footer_start + 1], 0x28);
        assert_eq!(bytes[footer_start + 2], 0x93);
        assert_eq!(bytes[footer_start + 3], 0xe8);

        // Algorithm ID (BE): 0
        assert_eq!(&bytes[footer_start + 4..footer_start + 8], &[0, 0, 0, 0]);

        // CRC32 is a BE long (8 bytes) — upper 32 bits should be 0
        assert_eq!(&bytes[footer_start + 8..footer_start + 12], &[0, 0, 0, 0]);
    }

    #[test]
    fn test_footer_magic_is_not_of_codec_magic() {
        assert_eq!(FOOTER_MAGIC, !CODEC_MAGIC);
        // CODEC_MAGIC = 0x3fd76c17
        // ~0x3fd76c17 = 0xC02893E8 as i32 (two's complement)
        assert_eq!(CODEC_MAGIC, 0x3fd76c17_u32 as i32);
    }

    #[test]
    fn test_validate_codec_name_empty() {
        // Empty codec name is valid in Java
        let mut out = MemoryIndexOutput::new("test".to_string());
        assert_ok!(write_header(&mut out, "", 0));
    }

    #[test]
    fn test_validate_codec_name_too_long() {
        let long_name: String = "a".repeat(128);
        let mut out = MemoryIndexOutput::new("test".to_string());
        assert_err!(write_header(&mut out, &long_name, 0));
    }

    #[test]
    fn test_validate_codec_name_non_ascii() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        assert_err!(write_header(&mut out, "bad\x01name", 0));
    }

    #[test]
    fn test_index_header_suffix_too_long() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let id = [0u8; 16];
        let long_suffix: String = "x".repeat(256);
        assert_err!(write_index_header(&mut out, "Test", 1, &id, &long_suffix));
    }

    #[test]
    fn test_vint_size_multi_byte() {
        // header_length uses vint_size internally. A codec name of length 128+
        // would need 2 vint bytes, but validate_codec_name rejects >= 128.
        // So test via header_length with a 127-char name (vint = 1 byte).
        let name = "a".repeat(127);
        assert_eq!(header_length(&name), 4 + 1 + 127 + 4);
    }

    // --- Read-side tests ---
    // Ported from org.apache.lucene.codecs.TestCodecUtil

    #[test]
    fn test_check_header_roundtrip() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_header(&mut out, "FooBar", 5).unwrap();
        let bytes = out.bytes().to_vec();

        let mut input = ByteSliceIndexInput::new("test".into(), bytes);
        let version = check_header(&mut input, "FooBar", 1, 10).unwrap();
        assert_eq!(version, 5);
    }

    #[test]
    fn test_check_header_wrong_magic() {
        let bytes = vec![0x00, 0x00, 0x00, 0x00]; // wrong magic
        let mut input = ByteSliceIndexInput::new("test".into(), bytes);
        assert!(check_header(&mut input, "Test", 1, 1).is_err());
    }

    #[test]
    fn test_check_header_wrong_codec() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_header(&mut out, "FooBar", 5).unwrap();
        let bytes = out.bytes().to_vec();

        let mut input = ByteSliceIndexInput::new("test".into(), bytes);
        assert!(check_header(&mut input, "WrongName", 1, 10).is_err());
    }

    #[test]
    fn test_check_header_version_too_low() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_header(&mut out, "Test", 3).unwrap();
        let bytes = out.bytes().to_vec();

        let mut input = ByteSliceIndexInput::new("test".into(), bytes);
        assert!(check_header(&mut input, "Test", 5, 10).is_err());
    }

    #[test]
    fn test_check_header_version_too_high() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_header(&mut out, "Test", 15).unwrap();
        let bytes = out.bytes().to_vec();

        let mut input = ByteSliceIndexInput::new("test".into(), bytes);
        assert!(check_header(&mut input, "Test", 1, 10).is_err());
    }

    #[test]
    fn test_check_index_header_roundtrip() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let id = [0xABu8; ID_LENGTH];
        write_index_header(&mut out, "FooBar", 5, &id, "xyz").unwrap();
        let bytes = out.bytes().to_vec();

        let mut input = ByteSliceIndexInput::new("test".into(), bytes);
        let version = check_index_header(&mut input, "FooBar", 1, 10, &id, "xyz").unwrap();
        assert_eq!(version, 5);
    }

    #[test]
    fn test_check_index_header_wrong_id() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let id = [0xABu8; ID_LENGTH];
        write_index_header(&mut out, "Test", 1, &id, "s").unwrap();
        let bytes = out.bytes().to_vec();

        let wrong_id = [0xCDu8; ID_LENGTH];
        let mut input = ByteSliceIndexInput::new("test".into(), bytes);
        assert!(check_index_header(&mut input, "Test", 1, 1, &wrong_id, "s").is_err());
    }

    #[test]
    fn test_check_index_header_wrong_suffix() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        let id = [1u8; ID_LENGTH];
        write_index_header(&mut out, "Test", 1, &id, "abc").unwrap();
        let bytes = out.bytes().to_vec();

        let mut input = ByteSliceIndexInput::new("test".into(), bytes);
        assert!(check_index_header(&mut input, "Test", 1, 1, &id, "xyz").is_err());
    }

    #[test]
    fn test_check_footer_roundtrip() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_header(&mut out, "Test", 1).unwrap();
        out.write_bytes(b"payload data").unwrap();
        write_footer(&mut out).unwrap();
        let bytes = out.bytes().to_vec();

        let inner = ByteSliceIndexInput::new("test".into(), bytes.clone());
        let mut input = ChecksumIndexInput::new(Box::new(inner));
        // Skip past header + payload to footer
        let footer_pos = bytes.len() as u64 - FOOTER_LENGTH as u64;
        input.seek(footer_pos).unwrap();
        check_footer(&mut input).unwrap();
    }

    #[test]
    fn test_check_footer_corrupted_crc() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        write_header(&mut out, "Test", 1).unwrap();
        out.write_bytes(b"payload data").unwrap();
        write_footer(&mut out).unwrap();
        let mut bytes = out.bytes().to_vec();

        // Corrupt a payload byte
        bytes[header_length("Test")] ^= 0xFF;

        let inner = ByteSliceIndexInput::new("test".into(), bytes.clone());
        let mut input = ChecksumIndexInput::new(Box::new(inner));
        let footer_pos = bytes.len() as u64 - FOOTER_LENGTH as u64;
        input.seek(footer_pos).unwrap();
        assert!(check_footer(&mut input).is_err());
    }

    #[test]
    fn test_checksum_entire_file() {
        let mut out = MemoryIndexOutput::new("test".to_string());
        out.write_bytes(b"hello world").unwrap();
        let expected = out.checksum();
        let bytes = out.bytes().to_vec();

        let input = ByteSliceIndexInput::new("test".into(), bytes);
        let actual = checksum_entire_file(Box::new(input)).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_footer_covers_preceding_bytes() {
        // The CRC in the footer covers all bytes up to and including
        // the first 8 bytes of the footer (magic + algorithm ID)
        let mut out = MemoryIndexOutput::new("test".to_string());
        out.write_string("test data").unwrap();

        let checksum_before_crc = {
            // Simulate: checksum after writing magic + algo but before CRC
            // This is what write_footer captures with out.checksum()
            // after writing the first 8 footer bytes
            let mut out2 = MemoryIndexOutput::new("test2".to_string());
            out2.write_string("test data").unwrap();
            out2.write_be_int(FOOTER_MAGIC).unwrap();
            out2.write_be_int(0).unwrap();
            out2.checksum()
        };

        write_footer(&mut out).unwrap();
        let bytes = out.bytes();

        // Extract the CRC from the footer (last 8 bytes, BE long)
        let footer_crc_offset = bytes.len() - 8;
        let written_crc = u64::from_be_bytes(
            bytes[footer_crc_offset..footer_crc_offset + 8]
                .try_into()
                .unwrap(),
        );

        assert_eq!(written_crc, checksum_before_crc);
    }
}
