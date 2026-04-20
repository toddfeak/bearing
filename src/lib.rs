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
//! use bearing::prelude::{
//!     DocumentBuilder, FSDirectory, IndexWriter, IndexWriterConfig,
//!     keyword, text,
//! };
//!
//! let directory = FSDirectory::open(std::path::Path::new("/tmp/my-index")).unwrap();
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
//! - [`prelude`] — Convenience re-exports for common types.
//! - [`analysis`] — Text analysis pipeline (tokenizers, filters, analyzers).
//! - [`document`] — Document model and field factory functions.
//! - [`encoding`] — Data encoding/decoding algorithms (varint, zigzag, packed ints, compression).
//! - [`index`] — Index writer, configuration, and segment metadata types.
//! - [`store`] — Storage abstraction (in-memory and filesystem directories).
//! - [`util`] — Utility types (byte block pool, bytes ref hash).

#[cfg(test)]
#[macro_use]
extern crate assertables;

pub mod analysis;
pub mod codecs;
pub mod document;
pub mod encoding;
pub use index::field;
pub mod index;
pub mod prelude;
pub mod search;
pub mod store;
#[cfg(test)]
pub mod test_util;
pub mod util;
