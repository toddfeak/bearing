// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::io;

use crate::document::IndexOptions;
use crate::index::indexing_chain::PerFieldData;
use crate::index::{FieldInfos, SegmentInfo};
use crate::store::SegmentFile;

use super::blocktree_writer::BlockTreeTermsWriter;

// --- Postings format constants ---

pub const BLOCK_SIZE: usize = 128;
pub const BLOCK_MASK: usize = BLOCK_SIZE - 1;
pub const LEVEL1_FACTOR: usize = 32;
pub const LEVEL1_NUM_DOCS: usize = LEVEL1_FACTOR * BLOCK_SIZE;
pub const LEVEL1_MASK: usize = LEVEL1_NUM_DOCS - 1;

pub const VERSION_START: i32 = 0;
pub const VERSION_CURRENT: i32 = VERSION_START;

// Postings file extensions
pub const META_EXTENSION: &str = "psm";
pub const DOC_EXTENSION: &str = "doc";
pub const POS_EXTENSION: &str = "pos";
pub const PAY_EXTENSION: &str = "pay";

// Postings codec names
pub const TERMS_CODEC: &str = "Lucene103PostingsWriterTerms";
pub const META_CODEC: &str = "Lucene103PostingsWriterMeta";
pub const DOC_CODEC: &str = "Lucene103PostingsWriterDoc";
pub const POS_CODEC: &str = "Lucene103PostingsWriterPos";
pub const PAY_CODEC: &str = "Lucene103PostingsWriterPay";

// BlockTree terms dict constants
pub const TERMS_EXTENSION: &str = "tim";
pub const TERMS_INDEX_EXTENSION: &str = "tip";
pub const TERMS_META_EXTENSION: &str = "tmd";

pub const TERMS_CODEC_NAME: &str = "BlockTreeTermsDict";
pub const TERMS_INDEX_CODEC_NAME: &str = "BlockTreeTermsIndex";
pub const TERMS_META_CODEC_NAME: &str = "BlockTreeTermsMeta";

pub const BLOCKTREE_VERSION_START: i32 = 0;
pub const BLOCKTREE_VERSION_CURRENT: i32 = BLOCKTREE_VERSION_START;

pub const DEFAULT_MIN_BLOCK_SIZE: usize = 25;
pub const DEFAULT_MAX_BLOCK_SIZE: usize = 48;

/// Per-term metadata stored in .tim blocks.
#[derive(Clone, Copy, Debug)]
pub struct IntBlockTermState {
    // From BlockTermState base class
    pub doc_freq: i32,
    pub total_term_freq: i64,
    // Lucene103-specific fields
    pub doc_start_fp: i64,
    pub pos_start_fp: i64,
    pub pay_start_fp: i64,
    pub last_pos_block_offset: i64,
    pub singleton_doc_id: i32,
}

impl IntBlockTermState {
    pub fn new() -> Self {
        Self {
            doc_freq: 0,
            total_term_freq: 0,
            doc_start_fp: 0,
            pos_start_fp: 0,
            pay_start_fp: 0,
            last_pos_block_offset: -1,
            singleton_doc_id: -1,
        }
    }
}

impl Default for IntBlockTermState {
    fn default() -> Self {
        Self::new()
    }
}

/// Write all postings files for the given fields.
/// Returns a Vec of [`SegmentFile`]s for all output files.
pub fn write(
    segment_info: &SegmentInfo,
    segment_suffix: &str,
    field_infos: &FieldInfos,
    per_field: &HashMap<String, PerFieldData>,
) -> io::Result<Vec<SegmentFile>> {
    let mut btw = BlockTreeTermsWriter::new(
        &segment_info.name,
        segment_suffix,
        &segment_info.id,
        field_infos,
    )?;

    // Process fields in field number order
    let mut indexed_fields: Vec<_> = field_infos
        .iter()
        .filter(|fi| fi.index_options() != IndexOptions::None)
        .collect();
    indexed_fields.sort_by_key(|fi| fi.number());

    for fi in indexed_fields {
        if let Some(pfd) = per_field.get(fi.name())
            && !pfd.postings.is_empty()
        {
            btw.write_field(fi, &pfd.postings)?;
        }
    }

    btw.finish()
}
