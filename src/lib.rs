// SPDX-License-Identifier: Apache-2.0

//! A Rust port of the Apache Lucene write path.
//!
//! Bearing implements the indexing (write) side of Apache Lucene 10.3.2. It can
//! build Lucene-compatible indexes that are readable by Java Lucene. Search
//! (the read path) is not yet implemented.
//!
//! # Quick start
//!
//! ```no_run
//! use bearing::document::{Document, text_field, keyword_field};
//! use bearing::index::IndexWriter;
//! use bearing::store::FSDirectory;
//!
//! let writer = bearing::index::IndexWriter::new();
//!
//! let mut doc = Document::new();
//! doc.add(text_field("body", "the quick brown fox"));
//! doc.add(keyword_field("category", "animals"));
//! writer.add_document(doc).unwrap();
//!
//! let result = writer.commit().unwrap();
//! let mut dir = FSDirectory::open(std::path::Path::new("/tmp/my-index")).unwrap();
//! result.write_to_directory(&mut dir).unwrap();
//! ```
//!
//! # Modules
//!
//! - [`analysis`] — Text analysis pipeline (tokenizers, filters, analyzers).
//! - [`document`] — Document model and field factory functions.
//! - [`encoding`] — Data encoding/decoding algorithms (varint, zigzag, packed ints, compression).
//! - [`index`] — Index writer, configuration, and segment metadata types.
//! - [`store`] — Storage abstraction (in-memory and filesystem directories).
//! - [`util`] — Utility types such as [`util::BytesRef`].

#[cfg(test)]
#[macro_use]
extern crate assertables;

pub mod analysis;
pub mod codecs;
pub mod document;
pub mod encoding;
pub mod index;
pub mod store;
#[cfg(test)]
pub mod test_util;
pub mod util;
