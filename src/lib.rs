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
//! use std::sync::Arc;
//! use bearing::index::config::IndexWriterConfig;
//! use bearing::newindex::document::DocumentBuilder;
//! use bearing::newindex::field::{text, keyword};
//! use bearing::index::writer::IndexWriter;
//! use bearing::store::{FSDirectory, SharedDirectory};
//!
//! let fs_dir = FSDirectory::open(std::path::Path::new("/tmp/my-index")).unwrap();
//! let directory = Arc::new(SharedDirectory::new(Box::new(fs_dir)));
//! let writer = IndexWriter::new(IndexWriterConfig::default(), directory);
//!
//! let doc = DocumentBuilder::new()
//!     .add_field(text("body").value("the quick brown fox"))
//!     .add_field(keyword("category").value("animals"))
//!     .build();
//! writer.add_document(doc).unwrap();
//!
//! writer.commit().unwrap();
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
pub mod newindex;
pub mod search;
pub mod store;
#[cfg(test)]
pub mod test_util;
pub mod util;
