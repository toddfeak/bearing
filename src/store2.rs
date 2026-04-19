// SPDX-License-Identifier: Apache-2.0

//! Read path: byte-backed [`IndexInput`] over borrowed bytes, with
//! [`FileBacking`] owning the bytes at the segment level.

pub mod codec_footers;
pub mod codec_headers;
pub mod file_backing;
pub mod index_input;
pub mod string;
pub mod varint;

pub use file_backing::FileBacking;
pub use index_input::IndexInput;
