// SPDX-License-Identifier: Apache-2.0

// Ported from org.apache.lucene.util.compress.LZ4
// This implements the LZ4 block compression format used by Lucene's stored fields.

use std::io;

const MIN_MATCH: usize = 4;
const LAST_LITERALS: usize = 5;
const HASH_LOG: usize = 14;
const HASH_TABLE_SIZE: usize = 1 << HASH_LOG;
const MAX_DISTANCE: usize = 1 << 16; // 65536

/// LZ4 compress a block of data.
/// Returns the compressed bytes.
pub fn compress(input: &[u8]) -> Vec<u8> {
    if input.is_empty() {
        return Vec::new();
    }
    if input.len() < MIN_MATCH + LAST_LITERALS {
        return encode_literals_only(input);
    }

    let mut hash_table = [0u32; HASH_TABLE_SIZE];
    compress_inner(input, 0, &mut hash_table)
}

/// Shared compression loop used by both `compress` and `compress_with_dictionary`.
/// `buffer` is the full input (possibly prefixed by dictionary bytes).
/// `data_start` is the byte offset where actual data begins (0 for plain compress,
/// dict_len for dictionary compress).
/// `hash_table` must be pre-sized to HASH_TABLE_SIZE and pre-populated if using a dictionary.
fn compress_inner(buffer: &[u8], data_start: usize, hash_table: &mut [u32]) -> Vec<u8> {
    let data_len = buffer.len() - data_start;
    let mut output = Vec::with_capacity(data_len);

    let mut anchor = data_start;
    let mut ip = data_start;
    let limit = buffer.len() - LAST_LITERALS;
    let match_limit = limit - MIN_MATCH;

    while ip < match_limit {
        let h = hash4(buffer, ip);
        let ref_pos = hash_table[h] as usize;
        hash_table[h] = ip as u32;

        if ref_pos < ip
            && ip - ref_pos < MAX_DISTANCE
            && buffer[ref_pos] == buffer[ip]
            && buffer[ref_pos + 1] == buffer[ip + 1]
            && buffer[ref_pos + 2] == buffer[ip + 2]
            && buffer[ref_pos + 3] == buffer[ip + 3]
        {
            let match_distance = ip - ref_pos;

            // Extend match forward — cap at `limit` to preserve LAST_LITERALS bytes
            let mut match_len = MIN_MATCH;
            while ip + match_len < limit && buffer[ref_pos + match_len] == buffer[ip + match_len] {
                match_len += 1;
            }

            let literal_len = ip - anchor;
            encode_sequence(
                &mut output,
                buffer,
                anchor,
                literal_len,
                match_distance,
                match_len,
            );

            anchor = ip + match_len;
            ip = anchor;
        } else {
            ip += 1;
        }
    }

    // Encode remaining literals
    let remaining = buffer.len() - anchor;
    encode_last_literals(&mut output, buffer, anchor, remaining);

    output
}

/// Hash 4 bytes at position `pos`.
fn hash4(data: &[u8], pos: usize) -> usize {
    let v = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    ((v.wrapping_mul(2654435761)) >> (32 - HASH_LOG)) as usize
}

/// Encode a sequence: literal_len literals + match(distance, match_len).
fn encode_sequence(
    output: &mut Vec<u8>,
    input: &[u8],
    literal_start: usize,
    literal_len: usize,
    match_distance: usize,
    match_len: usize,
) {
    // Token byte: [literal_len:4][match_len-4:4]
    let lit_code = literal_len.min(15);
    let match_code = (match_len - MIN_MATCH).min(15);
    output.push(((lit_code << 4) | match_code) as u8);

    // Extended literal length
    if literal_len >= 15 {
        let mut remaining = literal_len - 15;
        while remaining >= 255 {
            output.push(255);
            remaining -= 255;
        }
        output.push(remaining as u8);
    }

    // Literal bytes
    output.extend_from_slice(&input[literal_start..literal_start + literal_len]);

    // Match distance (LE u16)
    output.push(match_distance as u8);
    output.push((match_distance >> 8) as u8);

    // Extended match length
    if match_len - MIN_MATCH >= 15 {
        let mut remaining = match_len - MIN_MATCH - 15;
        while remaining >= 255 {
            output.push(255);
            remaining -= 255;
        }
        output.push(remaining as u8);
    }
}

/// Encode remaining literals at the end of the block (no match follows).
fn encode_last_literals(output: &mut Vec<u8>, input: &[u8], start: usize, len: usize) {
    let lit_code = len.min(15);
    output.push((lit_code << 4) as u8);

    if len >= 15 {
        let mut remaining = len - 15;
        while remaining >= 255 {
            output.push(255);
            remaining -= 255;
        }
        output.push(remaining as u8);
    }

    output.extend_from_slice(&input[start..start + len]);
}

/// Encode input as all-literals (no matches possible).
fn encode_literals_only(input: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len() + 10);
    encode_last_literals(&mut output, input, 0, input.len());
    output
}

/// LZ4 compress with a preset dictionary.
/// `buffer[0..dict_len]` is the dictionary, `buffer[dict_len..]` is the data to compress.
/// Returns compressed bytes for the data portion only.
/// Ported from org.apache.lucene.util.compress.LZ4.compressWithDictionary
pub fn compress_with_dictionary(buffer: &[u8], dict_len: usize) -> Vec<u8> {
    let data_len = buffer.len() - dict_len;

    if data_len == 0 {
        return Vec::new();
    }
    if data_len < MIN_MATCH + LAST_LITERALS {
        return encode_literals_only(&buffer[dict_len..]);
    }

    let mut hash_table = [0u32; HASH_TABLE_SIZE];

    // Pre-populate hash table with dictionary positions.
    // Like Java's initDictionary, we hash every position in the dict
    // (the 4-byte read may extend into data, which is fine since the buffer is contiguous).
    for i in 0..dict_len {
        if i + 4 <= buffer.len() {
            let h = hash4(buffer, i);
            hash_table[h] = i as u32;
        }
    }

    compress_inner(buffer, dict_len, &mut hash_table)
}

/// LZ4 decompress. `dest_len` is the expected uncompressed size.
pub fn decompress(compressed: &[u8], dest_len: usize) -> io::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(dest_len);
    let mut ip = 0;

    while ip < compressed.len() && output.len() < dest_len {
        let token = compressed[ip] as usize;
        ip += 1;

        // Literal length
        let mut literal_len = token >> 4;
        if literal_len == 15 {
            loop {
                if ip >= compressed.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated literal length",
                    ));
                }
                let b = compressed[ip] as usize;
                ip += 1;
                literal_len += b;
                if b < 255 {
                    break;
                }
            }
        }

        // Copy literals
        if ip + literal_len > compressed.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "literal bytes out of bounds",
            ));
        }
        output.extend_from_slice(&compressed[ip..ip + literal_len]);
        ip += literal_len;

        if output.len() >= dest_len {
            break;
        }

        // Match distance (LE u16)
        if ip + 2 > compressed.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated match distance",
            ));
        }
        let distance = compressed[ip] as usize | ((compressed[ip + 1] as usize) << 8);
        ip += 2;

        if distance == 0 || distance > output.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid match distance",
            ));
        }

        // Match length
        let mut match_len = (token & 0x0F) + MIN_MATCH;
        if (token & 0x0F) == 15 {
            loop {
                if ip >= compressed.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated match length",
                    ));
                }
                let b = compressed[ip] as usize;
                ip += 1;
                match_len += b;
                if b < 255 {
                    break;
                }
            }
        }

        // Copy match (may overlap)
        let match_start = output.len() - distance;
        for i in 0..match_len {
            let b = output[match_start + i];
            output.push(b);
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_simple() {
        let input = b"hello world hello world hello world!";
        let compressed = compress(input);
        let decompressed = decompress(&compressed, input.len()).unwrap();
        assert_eq!(&decompressed, input);
    }

    #[test]
    fn test_compress_decompress_short() {
        let input = b"hi";
        let compressed = compress(input);
        let decompressed = decompress(&compressed, input.len()).unwrap();
        assert_eq!(&decompressed, input);
    }

    #[test]
    fn test_compress_decompress_empty() {
        let input = b"";
        let compressed = compress(input);
        assert!(compressed.is_empty());
    }

    #[test]
    fn test_compress_decompress_repetitive() {
        let input: Vec<u8> = "abcdefgh".repeat(100).into_bytes();
        let compressed = compress(&input);
        // Repetitive data should compress well
        assert!(compressed.len() < input.len());
        let decompressed = decompress(&compressed, input.len()).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn test_compress_decompress_longer_text() {
        let input = b"The quick brown fox jumps over the lazy dog. \
                       The quick brown fox jumps over the lazy dog again. \
                       And once more the quick brown fox jumps over the lazy dog.";
        let compressed = compress(input);
        let decompressed = decompress(&compressed, input.len()).unwrap();
        assert_eq!(&decompressed[..], &input[..]);
    }

    #[test]
    fn test_compress_all_same_byte() {
        let input = vec![0xAA; 1000];
        let compressed = compress(&input);
        assert!(compressed.len() < input.len());
        let decompressed = decompress(&compressed, input.len()).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn test_compress_with_dictionary_no_dict() {
        // With dict_len=0, should behave like regular compress
        let input = b"hello world hello world hello world!";
        let buffer = input.to_vec();
        let compressed = compress_with_dictionary(&buffer, 0);
        let decompressed = decompress(&compressed, input.len()).unwrap();
        assert_eq!(&decompressed, input);
    }

    #[test]
    fn test_compress_with_dictionary_small() {
        // Dict + data
        let dict = b"the quick brown fox ";
        let data = b"the quick brown fox jumps over";
        let mut buffer = Vec::new();
        buffer.extend_from_slice(dict);
        buffer.extend_from_slice(data);

        let compressed = compress_with_dictionary(&buffer, dict.len());
        // Decompress: need dict bytes in output buffer first, then decompress data
        let output = Vec::from(&dict[..]);
        let decompressed = decompress_with_prefix(&compressed, data.len(), &output);
        assert_eq!(&decompressed, data);
    }

    #[test]
    fn test_compress_with_dictionary_short_data() {
        // Very short data that's all literals
        let dict = b"abcd";
        let data = b"xyz";
        let mut buffer = Vec::new();
        buffer.extend_from_slice(dict);
        buffer.extend_from_slice(data);

        let compressed = compress_with_dictionary(&buffer, dict.len());
        let decompressed = decompress(&compressed, data.len()).unwrap();
        assert_eq!(&decompressed, data);
    }

    /// Regression test: when dict+data is small (e.g. 27 bytes) and a match near the end of
    /// the data references the dictionary, the compressor must NOT consume all remaining bytes
    /// in the match — at least LAST_LITERALS (5) bytes must remain for the final-literals token.
    /// Otherwise, a spurious 0x00 byte is emitted, causing stream misalignment in the
    /// LZ4WithPresetDict decompressor.
    /// Ported from the bug exposed by Java's LZ4WithPresetDictDecompressor failing with
    /// "arraycopy: source index -999 out of bounds for byte[32]".
    #[test]
    fn test_compress_with_dictionary_last_literals_preserved() {
        // Simulate the stored fields scenario: 9-byte dict + 18-byte block where
        // the block data ends with bytes that match the dictionary prefix.
        // Dict = [0x00, 0x3C, '/', 'h', 'o', 'm', 'e', '/', 'r']
        // Block data contains "...XX" + [0x00, 0x3C, '/', 'h', 'o', 'm', 'e', '/']
        // which matches dict[0..8], potentially consuming all remaining bytes.
        let dict: &[u8] = &[0x00, 0x3C, b'/', b'h', b'o', b'm', b'e', b'/', b'r'];
        let data: &[u8] = &[
            b'n', b'o', b'l', b'o', b'g', b'y', b'.', b't', b'x', b't', 0x00, 0x3C, b'/', b'h',
            b'o', b'm', b'e', b'/',
        ];
        let mut buffer = Vec::new();
        buffer.extend_from_slice(dict);
        buffer.extend_from_slice(data);

        let compressed = compress_with_dictionary(&buffer, dict.len());

        // Verify the compressed output ends with a valid last-literals token
        // (i.e., no spurious 0x00 byte from encode_last_literals with len=0).
        // The last token must encode at least LAST_LITERALS (5) literal bytes.
        assert!(!compressed.is_empty());

        // Decompress with dictionary prefix and verify round-trip
        let decompressed = decompress_with_prefix(&compressed, data.len(), dict);
        assert_eq!(
            &decompressed,
            data,
            "round-trip failed: compressed {} bytes",
            compressed.len()
        );

        // Verify the compressed data is exactly consumed by decompression
        // (no spurious trailing bytes that would cause stream misalignment)
        let mut consumed = 0;
        let mut produced = 0;
        while consumed < compressed.len() && produced < data.len() {
            let token = compressed[consumed] as usize;
            consumed += 1;
            let mut literal_len = token >> 4;
            if literal_len == 15 {
                loop {
                    let b = compressed[consumed] as usize;
                    consumed += 1;
                    literal_len += b;
                    if b < 255 {
                        break;
                    }
                }
            }
            consumed += literal_len;
            produced += literal_len;
            if produced >= data.len() {
                break;
            }
            consumed += 2; // match distance
            let mut match_len = (token & 0x0F) + MIN_MATCH;
            if (token & 0x0F) == 15 {
                loop {
                    let b = compressed[consumed] as usize;
                    consumed += 1;
                    match_len += b;
                    if b < 255 {
                        break;
                    }
                }
            }
            produced += match_len;
        }
        assert_eq!(
            consumed,
            compressed.len(),
            "compressed data has {} unconsumed trailing bytes (stream misalignment bug)",
            compressed.len() - consumed
        );
    }

    /// Decompress with a pre-existing prefix (dictionary) in the output buffer.
    /// The compressed data may reference positions in the prefix.
    fn decompress_with_prefix(compressed: &[u8], dest_len: usize, prefix: &[u8]) -> Vec<u8> {
        let mut output = Vec::from(prefix);
        let target_len = prefix.len() + dest_len;
        let mut ip = 0;

        while ip < compressed.len() && output.len() < target_len {
            let token = compressed[ip] as usize;
            ip += 1;

            // Literal length
            let mut literal_len = token >> 4;
            if literal_len == 15 {
                loop {
                    let b = compressed[ip] as usize;
                    ip += 1;
                    literal_len += b;
                    if b < 255 {
                        break;
                    }
                }
            }

            output.extend_from_slice(&compressed[ip..ip + literal_len]);
            ip += literal_len;

            if output.len() >= target_len {
                break;
            }

            // Match distance
            let distance = compressed[ip] as usize | ((compressed[ip + 1] as usize) << 8);
            ip += 2;

            // Match length
            let mut match_len = (token & 0x0F) + MIN_MATCH;
            if (token & 0x0F) == 15 {
                loop {
                    let b = compressed[ip] as usize;
                    ip += 1;
                    match_len += b;
                    if b < 255 {
                        break;
                    }
                }
            }

            // Copy match (may overlap, may reference prefix/dictionary)
            let match_start = output.len() - distance;
            for i in 0..match_len {
                let b = output[match_start + i];
                output.push(b);
            }
        }

        // Return only the data portion (after the prefix)
        output[prefix.len()..].to_vec()
    }
}
