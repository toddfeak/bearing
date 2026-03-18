// SPDX-License-Identifier: Apache-2.0
//! Numeric encoding utilities for sortable byte representations of integers and floats.

/// Converts a long value to sortable bytes (8 bytes, big-endian).
/// Flips the sign bit so that negative values sort before positive values.
pub fn long_to_sortable_bytes(value: i64) -> [u8; 8] {
    let flipped = value ^ i64::MIN; // XOR with 0x8000000000000000
    flipped.to_be_bytes()
}

/// Converts an int value to sortable bytes (4 bytes, big-endian).
/// Flips the sign bit so that negative values sort before positive values.
pub fn int_to_sortable_bytes(value: i32) -> [u8; 4] {
    let flipped = value ^ i32::MIN; // XOR with 0x80000000
    flipped.to_be_bytes()
}

/// Converts a float to a sortable int using IEEE 754 bit manipulation.
pub fn float_to_sortable_int(value: f32) -> i32 {
    sortable_float_bits(f32::to_bits(value) as i32)
}

/// Converts a double to a sortable long using IEEE 754 bit manipulation.
pub fn double_to_sortable_long(value: f64) -> i64 {
    sortable_double_bits(f64::to_bits(value) as i64)
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
fn sortable_float_bits(bits: i32) -> i32 {
    bits ^ ((bits >> 31) & 0x7FFFFFFF)
}

/// Converts IEEE 754 double bits to sortable order (or back).
fn sortable_double_bits(bits: i64) -> i64 {
    bits ^ ((bits >> 63) & 0x7FFFFFFFFFFFFFFF)
}

// --- Geo encoding utilities ---
// Ported from org.apache.lucene.geo.GeoEncodingUtils

const LAT_SCALE: f64 = (1u64 << 32) as f64 / 180.0;
const LAT_DECODE: f64 = 1.0 / LAT_SCALE;
const LON_SCALE: f64 = (1u64 << 32) as f64 / 360.0;
const LON_DECODE: f64 = 1.0 / LON_SCALE;

/// Quantizes latitude (-90..90) to a 32-bit integer (rounding toward -90).
///
/// Exact +90.0 is handled as a special case to avoid overflow.
pub fn encode_latitude(lat: f64) -> i32 {
    assert!(
        (-90.0..=90.0).contains(&lat),
        "latitude {lat} out of range [-90, 90]"
    );
    let lat = if lat == 90.0 {
        f64::from_bits(90.0f64.to_bits() - 1)
    } else {
        lat
    };
    (lat / LAT_DECODE).floor() as i32
}

/// Quantizes longitude (-180..180) to a 32-bit integer (rounding toward -180).
///
/// Exact +180.0 is handled as a special case to avoid overflow.
pub fn encode_longitude(lon: f64) -> i32 {
    assert!(
        (-180.0..=180.0).contains(&lon),
        "longitude {lon} out of range [-180, 180]"
    );
    let lon = if lon == 180.0 {
        f64::from_bits(180.0f64.to_bits() - 1)
    } else {
        lon
    };
    (lon / LON_DECODE).floor() as i32
}

/// Decodes a quantized latitude back to a double.
#[cfg(test)]
pub fn decode_latitude(encoded: i32) -> f64 {
    encoded as f64 * LAT_DECODE
}

/// Decodes a quantized longitude back to a double.
#[cfg(test)]
pub fn decode_longitude(encoded: i32) -> f64 {
    encoded as f64 * LON_DECODE
}

// --- Range encoding utilities ---
// Ported from org.apache.lucene.document.{IntRange,LongRange,FloatRange,DoubleRange}

/// Encodes an integer range as sortable bytes: `[min1..minN, max1..maxN]`.
///
/// Each dimension's min and max are encoded as 4-byte sortable values.
/// Panics if `mins` and `maxs` differ in length, exceed 4 dimensions,
/// or any `min[i] > max[i]`.
pub fn encode_int_range(mins: &[i32], maxs: &[i32]) -> Vec<u8> {
    validate_range_dims(mins.len(), maxs.len());
    for i in 0..mins.len() {
        assert!(
            mins[i] <= maxs[i],
            "min[{i}] ({}) > max[{i}] ({})",
            mins[i],
            maxs[i]
        );
    }
    let dims = mins.len();
    let mut bytes = vec![0u8; dims * 2 * 4];
    for (i, &min) in mins.iter().enumerate() {
        bytes[i * 4..(i + 1) * 4].copy_from_slice(&int_to_sortable_bytes(min));
    }
    for (i, &max) in maxs.iter().enumerate() {
        let offset = (dims + i) * 4;
        bytes[offset..offset + 4].copy_from_slice(&int_to_sortable_bytes(max));
    }
    bytes
}

/// Encodes a long range as sortable bytes: `[min1..minN, max1..maxN]`.
///
/// Each dimension's min and max are encoded as 8-byte sortable values.
pub fn encode_long_range(mins: &[i64], maxs: &[i64]) -> Vec<u8> {
    validate_range_dims(mins.len(), maxs.len());
    for i in 0..mins.len() {
        assert!(
            mins[i] <= maxs[i],
            "min[{i}] ({}) > max[{i}] ({})",
            mins[i],
            maxs[i]
        );
    }
    let dims = mins.len();
    let mut bytes = vec![0u8; dims * 2 * 8];
    for (i, &min) in mins.iter().enumerate() {
        bytes[i * 8..(i + 1) * 8].copy_from_slice(&long_to_sortable_bytes(min));
    }
    for (i, &max) in maxs.iter().enumerate() {
        let offset = (dims + i) * 8;
        bytes[offset..offset + 8].copy_from_slice(&long_to_sortable_bytes(max));
    }
    bytes
}

/// Encodes a float range as sortable bytes: `[min1..minN, max1..maxN]`.
///
/// Each dimension's min and max are converted to sortable ints, then encoded
/// as 4-byte sortable values.
pub fn encode_float_range(mins: &[f32], maxs: &[f32]) -> Vec<u8> {
    validate_range_dims(mins.len(), maxs.len());
    for i in 0..mins.len() {
        assert!(
            mins[i] <= maxs[i],
            "min[{i}] ({}) > max[{i}] ({})",
            mins[i],
            maxs[i]
        );
    }
    let dims = mins.len();
    let mut bytes = vec![0u8; dims * 2 * 4];
    for (i, &min) in mins.iter().enumerate() {
        bytes[i * 4..(i + 1) * 4].copy_from_slice(&float_to_sortable_bytes(min));
    }
    for (i, &max) in maxs.iter().enumerate() {
        let offset = (dims + i) * 4;
        bytes[offset..offset + 4].copy_from_slice(&float_to_sortable_bytes(max));
    }
    bytes
}

/// Encodes a double range as sortable bytes: `[min1..minN, max1..maxN]`.
///
/// Each dimension's min and max are converted to sortable longs, then encoded
/// as 8-byte sortable values.
pub fn encode_double_range(mins: &[f64], maxs: &[f64]) -> Vec<u8> {
    validate_range_dims(mins.len(), maxs.len());
    for i in 0..mins.len() {
        assert!(
            mins[i] <= maxs[i],
            "min[{i}] ({}) > max[{i}] ({})",
            mins[i],
            maxs[i]
        );
    }
    let dims = mins.len();
    let mut bytes = vec![0u8; dims * 2 * 8];
    for (i, &min) in mins.iter().enumerate() {
        bytes[i * 8..(i + 1) * 8].copy_from_slice(&double_to_sortable_bytes(min));
    }
    for (i, &max) in maxs.iter().enumerate() {
        let offset = (dims + i) * 8;
        bytes[offset..offset + 8].copy_from_slice(&double_to_sortable_bytes(max));
    }
    bytes
}

fn validate_range_dims(min_len: usize, max_len: usize) {
    assert_eq!(min_len, max_len, "mins and maxs must have equal length");
    assert!(
        (1..=4).contains(&min_len),
        "dimensions must be 1-4, got {min_len}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Converts sortable bytes back to a long value.
    fn sortable_bytes_to_long(bytes: &[u8; 8]) -> i64 {
        let v = i64::from_be_bytes(*bytes);
        v ^ i64::MIN
    }

    /// Converts sortable bytes back to an int value.
    fn sortable_bytes_to_int(bytes: &[u8; 4]) -> i32 {
        let v = i32::from_be_bytes(*bytes);
        v ^ i32::MIN
    }

    /// Converts a sortable int back to a float.
    fn sortable_int_to_float(encoded: i32) -> f32 {
        f32::from_bits(sortable_float_bits(encoded) as u32)
    }

    /// Converts a sortable long back to a double.
    fn sortable_long_to_double(encoded: i64) -> f64 {
        f64::from_bits(sortable_double_bits(encoded) as u64)
    }

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
        assert_lt!(neg, zero);
        assert_lt!(zero, pos);
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

        assert_lt!(neg, zero);
        assert_lt!(zero, pos);
        assert_lt!(pos, inf);
        assert_lt!(inf, nan); // NaN sorts after infinity
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

        assert_lt!(neg, zero);
        assert_lt!(zero, pos);
        assert_lt!(pos, inf);
        assert_lt!(inf, nan);
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

    // --- Geo encoding tests ---
    // Ported from org.apache.lucene.geo.TestGeoEncodingUtils

    #[test]
    fn test_encode_latitude_roundtrip() {
        for &lat in &[0.0, 45.0, -45.0, -90.0, 89.999] {
            let encoded = encode_latitude(lat);
            let decoded = decode_latitude(encoded);
            // Quantization error < LAT_DECODE (≈4.19e-8)
            assert_in_delta!(decoded, lat, 4.2e-8);
        }
    }

    #[test]
    fn test_encode_longitude_roundtrip() {
        for &lon in &[0.0, 90.0, -90.0, -180.0, 179.999] {
            let encoded = encode_longitude(lon);
            let decoded = decode_longitude(encoded);
            // Quantization error < LON_DECODE (≈8.38e-8)
            assert_in_delta!(decoded, lon, 8.4e-8);
        }
    }

    #[test]
    fn test_encode_latitude_ordering() {
        let south = encode_latitude(-45.0);
        let equator = encode_latitude(0.0);
        let north = encode_latitude(45.0);
        assert_lt!(south, equator);
        assert_lt!(equator, north);
    }

    #[test]
    fn test_encode_longitude_ordering() {
        let west = encode_longitude(-90.0);
        let prime = encode_longitude(0.0);
        let east = encode_longitude(90.0);
        assert_lt!(west, prime);
        assert_lt!(prime, east);
    }

    #[test]
    fn test_encode_latitude_boundary_90() {
        // +90.0 uses nextDown to avoid overflow
        let encoded = encode_latitude(90.0);
        let decoded = decode_latitude(encoded);
        assert_lt!(decoded, 90.0);
        assert_gt!(decoded, 89.999);
    }

    #[test]
    fn test_encode_longitude_boundary_180() {
        let encoded = encode_longitude(180.0);
        let decoded = decode_longitude(encoded);
        assert_lt!(decoded, 180.0);
        assert_gt!(decoded, 179.999);
    }

    #[test]
    fn test_encode_latitude_min() {
        let encoded = encode_latitude(-90.0);
        let decoded = decode_latitude(encoded);
        assert_in_delta!(decoded, -90.0, 4.2e-8);
    }

    #[test]
    fn test_encode_longitude_min() {
        let encoded = encode_longitude(-180.0);
        let decoded = decode_longitude(encoded);
        assert_in_delta!(decoded, -180.0, 8.4e-8);
    }

    #[test]
    #[should_panic(expected = "latitude")]
    fn test_encode_latitude_out_of_range() {
        encode_latitude(91.0);
    }

    #[test]
    #[should_panic(expected = "longitude")]
    fn test_encode_longitude_out_of_range() {
        encode_longitude(181.0);
    }

    // Cross-validate with known Java outputs:
    // Java: GeoEncodingUtils.encodeLatitude(40.7128) = 971445247
    // Java: GeoEncodingUtils.encodeLongitude(-74.006) = -882925972
    #[test]
    fn test_encode_latitude_cross_validate() {
        let encoded = encode_latitude(40.7128);
        assert_eq!(encoded, 971_445_247);
    }

    #[test]
    fn test_encode_longitude_cross_validate() {
        let encoded = encode_longitude(-74.006);
        assert_eq!(encoded, -882_925_972);
    }

    // --- Range encoding tests ---

    #[test]
    fn test_encode_int_range_single_dim() {
        let bytes = encode_int_range(&[10], &[20]);
        assert_len_eq_x!(&bytes, 8); // 1 dim * 2 * 4 bytes
        assert_eq!(&bytes[0..4], &int_to_sortable_bytes(10));
        assert_eq!(&bytes[4..8], &int_to_sortable_bytes(20));
    }

    #[test]
    fn test_encode_int_range_multi_dim() {
        let bytes = encode_int_range(&[1, 2], &[10, 20]);
        assert_len_eq_x!(&bytes, 16); // 2 dims * 2 * 4 bytes
        // mins first, then maxs
        assert_eq!(&bytes[0..4], &int_to_sortable_bytes(1));
        assert_eq!(&bytes[4..8], &int_to_sortable_bytes(2));
        assert_eq!(&bytes[8..12], &int_to_sortable_bytes(10));
        assert_eq!(&bytes[12..16], &int_to_sortable_bytes(20));
    }

    #[test]
    fn test_encode_long_range_single_dim() {
        let bytes = encode_long_range(&[100], &[200]);
        assert_len_eq_x!(&bytes, 16); // 1 dim * 2 * 8 bytes
        assert_eq!(&bytes[0..8], &long_to_sortable_bytes(100));
        assert_eq!(&bytes[8..16], &long_to_sortable_bytes(200));
    }

    #[test]
    fn test_encode_float_range_single_dim() {
        let bytes = encode_float_range(&[1.0], &[2.0]);
        assert_len_eq_x!(&bytes, 8);
        assert_eq!(&bytes[0..4], &float_to_sortable_bytes(1.0));
        assert_eq!(&bytes[4..8], &float_to_sortable_bytes(2.0));
    }

    #[test]
    fn test_encode_double_range_single_dim() {
        let bytes = encode_double_range(&[1.0], &[2.0]);
        assert_len_eq_x!(&bytes, 16);
        assert_eq!(&bytes[0..8], &double_to_sortable_bytes(1.0));
        assert_eq!(&bytes[8..16], &double_to_sortable_bytes(2.0));
    }

    #[test]
    #[should_panic(expected = "equal length")]
    fn test_encode_int_range_mismatched_dims() {
        encode_int_range(&[1, 2], &[10]);
    }

    #[test]
    #[should_panic(expected = "dimensions must be 1-4")]
    fn test_encode_int_range_zero_dims() {
        encode_int_range(&[], &[]);
    }

    #[test]
    #[should_panic(expected = "dimensions must be 1-4")]
    fn test_encode_int_range_five_dims() {
        encode_int_range(&[1, 2, 3, 4, 5], &[10, 20, 30, 40, 50]);
    }

    #[test]
    #[should_panic(expected = "min[0]")]
    fn test_encode_int_range_min_gt_max() {
        encode_int_range(&[20], &[10]);
    }

    #[test]
    fn test_encode_int_range_equal_min_max() {
        let bytes = encode_int_range(&[5], &[5]);
        assert_eq!(&bytes[0..4], &bytes[4..8]);
    }

    #[test]
    fn test_encode_long_range_multi_dim() {
        let bytes = encode_long_range(&[1, 2, 3], &[10, 20, 30]);
        assert_len_eq_x!(&bytes, 48); // 3 dims * 2 * 8 bytes
    }

    #[test]
    fn test_encode_float_range_multi_dim() {
        let bytes = encode_float_range(&[0.5, 1.5], &[2.5, 3.5]);
        assert_len_eq_x!(&bytes, 16); // 2 dims * 2 * 4 bytes
    }

    #[test]
    fn test_encode_double_range_four_dims() {
        let bytes = encode_double_range(&[1.0, 2.0, 3.0, 4.0], &[5.0, 6.0, 7.0, 8.0]);
        assert_len_eq_x!(&bytes, 64); // 4 dims * 2 * 8 bytes
    }
}
