// SPDX-License-Identifier: Apache-2.0

// DEBT: duplicates index::index_file_names — reconcile after switchover

/// Formats a number as a base-36 string (lowercase).
pub(crate) fn radix_fmt(n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = Vec::new();
    let mut val = n;
    while val > 0 {
        buf.push(DIGITS[(val % 36) as usize]);
        val /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).expect("base-36 digits are ASCII")
}

/// Builds a segment file name from segment name, suffix, and extension.
pub(crate) fn segment_file_name(segment_name: &str, segment_suffix: &str, ext: &str) -> String {
    if segment_suffix.is_empty() {
        format!("{segment_name}.{ext}")
    } else {
        format!("{segment_name}_{segment_suffix}.{ext}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn radix_fmt_zero() {
        assert_eq!(radix_fmt(0), "0");
    }

    #[test]
    fn radix_fmt_single_digits() {
        assert_eq!(radix_fmt(1), "1");
        assert_eq!(radix_fmt(9), "9");
    }

    #[test]
    fn radix_fmt_letters() {
        assert_eq!(radix_fmt(10), "a");
        assert_eq!(radix_fmt(35), "z");
    }

    #[test]
    fn radix_fmt_multi_char() {
        assert_eq!(radix_fmt(36), "10");
        assert_eq!(radix_fmt(46), "1a");
        assert_eq!(radix_fmt(100), "2s");
    }
}
