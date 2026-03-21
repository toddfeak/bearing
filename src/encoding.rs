// SPDX-License-Identifier: Apache-2.0

//! Data encoding and decoding algorithms.
//!
//! This module centralizes all encoding/decoding logic used by codecs, index
//! writers, and index readers. Algorithms include variable-length integers,
//! zigzag encoding, packed integers, and compression.
//!
//! All encoding functions operate on [`std::io::Write`] / [`std::io::Read`],
//! keeping them independent of any project-specific I/O traits.

pub mod varint;
pub mod zigzag;
