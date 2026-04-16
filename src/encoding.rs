// SPDX-License-Identifier: Apache-2.0

//! Data encoding and decoding algorithms.
//!
//! This module centralizes all encoding/decoding logic used by codecs, index
//! writers, and index readers. Algorithms include variable-length integers,
//! zigzag encoding, packed integers, and compression.
//!
//! All encoding functions operate on [`std::io::Write`] / [`std::io::Read`],
//! keeping them independent of any project-specific I/O traits.

pub mod geo;
pub mod group_vint;
pub mod lowercase_ascii;
pub mod lz4;
pub mod packed;
pub mod pfor;
pub mod range;
pub mod read_encoding;
pub mod sortable_bytes;
pub mod string;
pub mod varint;
pub mod zigzag;
