// SPDX-License-Identifier: Apache-2.0

// Ported from org.apache.lucene.util.NumericUtils

/// Converts a long value to sortable bytes (8 bytes, big-endian).
/// Flips the sign bit so that negative values sort before positive values.
pub fn long_to_sortable_bytes(value: i64) -> [u8; 8] {
    let flipped = value ^ i64::MIN; // XOR with 0x8000000000000000
    flipped.to_be_bytes()
}

/// Converts sortable bytes back to a long value.
pub fn sortable_bytes_to_long(bytes: &[u8; 8]) -> i64 {
    let v = i64::from_be_bytes(*bytes);
    v ^ i64::MIN
}

/// Converts an int value to sortable bytes (4 bytes, big-endian).
/// Flips the sign bit so that negative values sort before positive values.
pub fn int_to_sortable_bytes(value: i32) -> [u8; 4] {
    let flipped = value ^ i32::MIN; // XOR with 0x80000000
    flipped.to_be_bytes()
}

/// Converts sortable bytes back to an int value.
pub fn sortable_bytes_to_int(bytes: &[u8; 4]) -> i32 {
    let v = i32::from_be_bytes(*bytes);
    v ^ i32::MIN
}

/// Converts a float to a sortable int using IEEE 754 bit manipulation.
/// Ported from org.apache.lucene.util.NumericUtils.floatToSortableInt
pub fn float_to_sortable_int(value: f32) -> i32 {
    sortable_float_bits(f32::to_bits(value) as i32)
}

/// Converts a sortable int back to a float.
/// Ported from org.apache.lucene.util.NumericUtils.sortableIntToFloat
pub fn sortable_int_to_float(encoded: i32) -> f32 {
    f32::from_bits(sortable_float_bits(encoded) as u32)
}

/// Converts a double to a sortable long using IEEE 754 bit manipulation.
/// Ported from org.apache.lucene.util.NumericUtils.doubleToSortableLong
pub fn double_to_sortable_long(value: f64) -> i64 {
    sortable_double_bits(f64::to_bits(value) as i64)
}

/// Converts a sortable long back to a double.
/// Ported from org.apache.lucene.util.NumericUtils.sortableLongToDouble
pub fn sortable_long_to_double(encoded: i64) -> f64 {
    f64::from_bits(sortable_double_bits(encoded) as u64)
}

/// Converts a float to sortable bytes (4 bytes, big-endian).
pub fn float_to_sortable_bytes(value: f32) -> [u8; 4] {
    int_to_sortable_bytes(float_to_sortable_int(value))
}

/// Converts a double to sortable bytes (8 bytes, big-endian).
pub fn double_to_sortable_bytes(value: f64) -> [u8; 8] {
    long_to_sortable_bytes(double_to_sortable_long(value))
}

/// Converts IEEE 754 float bits to sortable order (or back).
/// Ported from org.apache.lucene.util.NumericUtils.sortableFloatBits
fn sortable_float_bits(bits: i32) -> i32 {
    bits ^ ((bits >> 31) & 0x7FFFFFFF)
}

/// Converts IEEE 754 double bits to sortable order (or back).
/// Ported from org.apache.lucene.util.NumericUtils.sortableDoubleBits
fn sortable_double_bits(bits: i64) -> i64 {
    bits ^ ((bits >> 63) & 0x7FFFFFFFFFFFFFFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ported from org.apache.lucene.util.TestNumericUtils

    #[test]
    fn test_long_to_sortable_bytes_zero() {
        let bytes = long_to_sortable_bytes(0);
        assert_eq!(bytes, [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_long_to_sortable_bytes_positive() {
        let bytes = long_to_sortable_bytes(1);
        assert_eq!(bytes, [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
    }

    #[test]
    fn test_long_to_sortable_bytes_negative() {
        let bytes = long_to_sortable_bytes(-1);
        assert_eq!(bytes, [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_long_to_sortable_bytes_min() {
        let bytes = long_to_sortable_bytes(i64::MIN);
        assert_eq!(bytes, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_long_to_sortable_bytes_max() {
        let bytes = long_to_sortable_bytes(i64::MAX);
        assert_eq!(bytes, [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_long_sortable_ordering() {
        // Negative values should sort before positive
        let neg = long_to_sortable_bytes(-100);
        let zero = long_to_sortable_bytes(0);
        let pos = long_to_sortable_bytes(100);
        assert!(neg < zero);
        assert!(zero < pos);
    }

    #[test]
    fn test_long_roundtrip() {
        for &v in &[0i64, 1, -1, i64::MIN, i64::MAX, 42, -42, 1000000] {
            let bytes = long_to_sortable_bytes(v);
            assert_eq!(sortable_bytes_to_long(&bytes), v);
        }
    }

    #[test]
    fn test_int_to_sortable_bytes_zero() {
        let bytes = int_to_sortable_bytes(0);
        assert_eq!(bytes, [0x80, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_int_roundtrip() {
        for &v in &[0i32, 1, -1, i32::MIN, i32::MAX, 42, -42] {
            let bytes = int_to_sortable_bytes(v);
            assert_eq!(sortable_bytes_to_int(&bytes), v);
        }
    }

    // Ported from org.apache.lucene.util.TestNumericUtils

    #[test]
    fn test_float_to_sortable_int_roundtrip() {
        for &v in &[
            0.0f32,
            -0.0,
            1.0,
            -1.0,
            42.5,
            -42.5,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ] {
            let encoded = float_to_sortable_int(v);
            let decoded = sortable_int_to_float(encoded);
            assert_eq!(v.to_bits(), decoded.to_bits(), "roundtrip failed for {v}");
        }
    }

    #[test]
    fn test_float_to_sortable_int_nan() {
        let encoded = float_to_sortable_int(f32::NAN);
        let decoded = sortable_int_to_float(encoded);
        assert!(decoded.is_nan());
    }

    #[test]
    fn test_float_sortable_ordering() {
        let neg = float_to_sortable_int(-1.0);
        let zero = float_to_sortable_int(0.0);
        let pos = float_to_sortable_int(1.0);
        let inf = float_to_sortable_int(f32::INFINITY);
        let nan = float_to_sortable_int(f32::NAN);

        assert!(neg < zero);
        assert!(zero < pos);
        assert!(pos < inf);
        assert!(inf < nan); // NaN sorts after infinity
    }

    #[test]
    fn test_double_to_sortable_long_roundtrip() {
        for &v in &[
            0.0f64,
            -0.0,
            1.0,
            -1.0,
            42.5,
            -42.5,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            let encoded = double_to_sortable_long(v);
            let decoded = sortable_long_to_double(encoded);
            assert_eq!(v.to_bits(), decoded.to_bits(), "roundtrip failed for {v}");
        }
    }

    #[test]
    fn test_double_to_sortable_long_nan() {
        let encoded = double_to_sortable_long(f64::NAN);
        let decoded = sortable_long_to_double(encoded);
        assert!(decoded.is_nan());
    }

    #[test]
    fn test_double_sortable_ordering() {
        let neg = double_to_sortable_long(-1.0);
        let zero = double_to_sortable_long(0.0);
        let pos = double_to_sortable_long(1.0);
        let inf = double_to_sortable_long(f64::INFINITY);
        let nan = double_to_sortable_long(f64::NAN);

        assert!(neg < zero);
        assert!(zero < pos);
        assert!(pos < inf);
        assert!(inf < nan);
    }

    #[test]
    fn test_float_to_sortable_bytes_roundtrip() {
        for &v in &[0.0f32, 1.0, -1.0, 42.5, f32::INFINITY] {
            let bytes = float_to_sortable_bytes(v);
            let int_val = sortable_bytes_to_int(&bytes);
            let decoded = sortable_int_to_float(int_val);
            assert_eq!(
                v.to_bits(),
                decoded.to_bits(),
                "byte roundtrip failed for {v}"
            );
        }
    }

    #[test]
    fn test_double_to_sortable_bytes_roundtrip() {
        for &v in &[0.0f64, 1.0, -1.0, 42.5, f64::INFINITY] {
            let bytes = double_to_sortable_bytes(v);
            let long_val = sortable_bytes_to_long(&bytes);
            let decoded = sortable_long_to_double(long_val);
            assert_eq!(
                v.to_bits(),
                decoded.to_bits(),
                "byte roundtrip failed for {v}"
            );
        }
    }
}
