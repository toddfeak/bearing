// SPDX-License-Identifier: Apache-2.0

//! Codec writers adapted for the newindex pipeline.
//!
// DEBT: these are copies of codec writing logic from src/codecs/ and
// src/index/, adapted to use newindex-local types. After switchover,
// reconcile with the originals into a single set of writers.

pub(crate) mod blocktree_writer;
pub(crate) mod doc_values;
pub(crate) mod field_infos;
pub(crate) mod norms;
pub(crate) mod points;
pub(crate) mod postings_writer;
pub(crate) mod segment_info;
pub(crate) mod segment_infos;
pub(crate) mod stored_fields;
