// SPDX-License-Identifier: Apache-2.0

//! Parallel read-path implementation based on a single concrete `IndexInput<'a>`
//! struct over `Cursor<&'a [u8]>` with file ownership lifted to the segment level.
//!
//! This module is being built in parallel with [`crate::store`] during the
//! read-path migration described in `docs/backlog/read_path_migration.md`. It
//! will eventually replace `crate::store` once every codec reader has been
//! migrated.

pub mod file_backing;
pub mod string;
pub mod varint;

pub use file_backing::FileBacking;
