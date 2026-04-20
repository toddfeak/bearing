// SPDX-License-Identifier: Apache-2.0

//! Read path: byte-backed [`IndexInput`] over borrowed bytes, with
//! [`FileBacking`] owning the bytes at the segment level.

pub(crate) mod codec_footers;
pub(crate) mod codec_headers;
pub(crate) mod file_backing;
pub(crate) mod index_input;
pub(super) mod string;
pub(super) mod varint;

pub(crate) use file_backing::FileBacking;
pub(crate) use index_input::IndexInput;
