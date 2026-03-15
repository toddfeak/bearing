// SPDX-License-Identifier: Apache-2.0

// Ported from org.apache.lucene.util.BitUtil

/// ZigZag-encodes a 32-bit integer.
/// Maps signed integers to unsigned: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, ...
pub fn zig_zag_encode_i32(i: i32) -> i32 {
    (i >> 31) ^ (i << 1)
}

/// ZigZag-decodes a 32-bit integer.
pub fn zig_zag_decode_i32(i: i32) -> i32 {
    ((i as u32) >> 1) as i32 ^ -(i & 1)
}

/// ZigZag-encodes a 64-bit integer.
pub fn zig_zag_encode_i64(i: i64) -> i64 {
    (i >> 63) ^ (i << 1)
}

/// ZigZag-decodes a 64-bit integer.
pub fn zig_zag_decode_i64(i: i64) -> i64 {
    ((i as u64) >> 1) as i64 ^ -(i & 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ported from org.apache.lucene.util.TestBitUtil

    #[test]
    fn test_zig_zag_i32() {
        assert_eq!(zig_zag_encode_i32(0), 0);
        assert_eq!(zig_zag_encode_i32(-1), 1);
        assert_eq!(zig_zag_encode_i32(1), 2);
        assert_eq!(zig_zag_encode_i32(-2), 3);
        assert_eq!(zig_zag_encode_i32(2), 4);
        assert_eq!(zig_zag_encode_i32(i32::MIN), -1); // 0xFFFFFFFF
        assert_eq!(zig_zag_encode_i32(i32::MAX), -2); // 0xFFFFFFFE
    }

    #[test]
    fn test_zig_zag_roundtrip_i32() {
        for &v in &[0, 1, -1, 127, -128, i32::MIN, i32::MAX, 42, -42] {
            assert_eq!(zig_zag_decode_i32(zig_zag_encode_i32(v)), v);
        }
    }

    #[test]
    fn test_zig_zag_i64() {
        assert_eq!(zig_zag_encode_i64(0), 0);
        assert_eq!(zig_zag_encode_i64(-1), 1);
        assert_eq!(zig_zag_encode_i64(1), 2);
        assert_eq!(zig_zag_encode_i64(-2), 3);
        assert_eq!(zig_zag_encode_i64(i64::MIN), -1);
        assert_eq!(zig_zag_encode_i64(i64::MAX), -2);
    }

    #[test]
    fn test_zig_zag_roundtrip_i64() {
        for &v in &[0i64, 1, -1, 127, -128, i64::MIN, i64::MAX, 42, -42] {
            assert_eq!(zig_zag_decode_i64(zig_zag_encode_i64(v)), v);
        }
    }
}
