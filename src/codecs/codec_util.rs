// SPDX-License-Identifier: Apache-2.0
//! Utilities for writing codec headers and footers with CRC32 integrity checks.

use std::io;

use log::debug;

use crate::store::{DataOutput, IndexOutput};

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
        assert_eq!(bytes.len(), 15);

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
        assert_eq!(bytes.len(), 35);

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
        assert_eq!(bytes.len(), 21);

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
        assert!(write_header(&mut out, "", 0).is_ok());
    }

    #[test]
    fn test_validate_codec_name_too_long() {
        let long_name: String = "a".repeat(128);
        let mut out = MemoryIndexOutput::new("test".to_string());
        assert!(write_header(&mut out, &long_name, 0).is_err());
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
