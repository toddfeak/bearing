// SPDX-License-Identifier: Apache-2.0

//! Indexing API and segment metadata.
//!
//! [`IndexWriter`] is the primary entry point for building an index.
//! [`IndexWriterConfig`] controls flush behavior. The remaining types
//! ([`FieldInfo`], [`FieldInfos`], [`SegmentInfo`], [`SegmentCommitInfo`])
//! represent segment-level metadata.

pub(crate) mod flush_control;
pub(crate) mod flush_policy;
pub(crate) mod index_file_names;
pub(crate) mod index_writer;
pub(crate) mod index_writer_config;
pub(crate) mod segment_worker;
pub(crate) mod segment_worker_pool;

pub use index_writer::{CommitResult, IndexWriter};
pub use index_writer_config::IndexWriterConfig;
pub(crate) mod indexing_chain;
pub(crate) mod segment_infos;

use std::collections::{HashMap, HashSet};

use mem_dbg::MemSize;

use crate::document::{DocValuesType, IndexOptions};
use crate::util::string_helper;

/// Configuration for point (BKD tree) dimensions.
#[derive(Clone, Copy, Debug, Default, MemSize)]
#[mem_size_flat]
pub struct PointDimensionConfig {
    pub dimension_count: u32,
    pub index_dimension_count: u32,
    pub num_bytes: u32,
}

impl PointDimensionConfig {
    pub fn has_values(&self) -> bool {
        self.dimension_count > 0
    }
}

/// Information about a single field in the index.
#[derive(Clone, Debug, MemSize)]
pub struct FieldInfo {
    name: String,
    number: u32,
    store_term_vector: bool,
    omit_norms: bool,
    store_payloads: bool,
    index_options: IndexOptions,
    doc_values_type: DocValuesType,
    dv_gen: i64,
    attributes: HashMap<String, String>,
    point_config: PointDimensionConfig,
    vector_dimension: u32,
    soft_deletes_field: bool,
    is_parent_field: bool,
}

impl FieldInfo {
    pub fn new(
        name: String,
        number: u32,
        store_term_vector: bool,
        omit_norms: bool,
        index_options: IndexOptions,
        doc_values_type: DocValuesType,
        point_config: PointDimensionConfig,
    ) -> Self {
        Self {
            name,
            number,
            store_term_vector,
            omit_norms,
            store_payloads: false,
            index_options,
            doc_values_type,
            dv_gen: -1,
            attributes: HashMap::new(),
            point_config,
            vector_dimension: 0,
            soft_deletes_field: false,
            is_parent_field: false,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn number(&self) -> u32 {
        self.number
    }

    pub fn store_term_vector(&self) -> bool {
        self.store_term_vector
    }

    pub fn omit_norms(&self) -> bool {
        self.omit_norms
    }

    pub fn store_payloads(&self) -> bool {
        self.store_payloads
    }

    pub fn index_options(&self) -> IndexOptions {
        self.index_options
    }

    pub fn doc_values_type(&self) -> DocValuesType {
        self.doc_values_type
    }

    pub fn dv_gen(&self) -> i64 {
        self.dv_gen
    }

    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    pub fn point_config(&self) -> PointDimensionConfig {
        self.point_config
    }

    pub fn vector_dimension(&self) -> u32 {
        self.vector_dimension
    }

    pub fn soft_deletes_field(&self) -> bool {
        self.soft_deletes_field
    }

    pub fn is_parent_field(&self) -> bool {
        self.is_parent_field
    }

    pub fn is_indexed(&self) -> bool {
        self.index_options != IndexOptions::None
    }

    pub fn has_norms(&self) -> bool {
        self.is_indexed() && !self.omit_norms
    }

    pub fn has_payloads(&self) -> bool {
        self.store_payloads
    }

    pub fn has_vectors(&self) -> bool {
        self.store_term_vector
    }

    pub fn has_doc_values(&self) -> bool {
        self.doc_values_type != DocValuesType::None
    }

    pub fn has_point_values(&self) -> bool {
        self.point_config.has_values()
    }

    pub fn has_vector_values(&self) -> bool {
        self.vector_dimension > 0
    }

    /// Returns the attribute value for the given key, if any.
    pub fn get_attribute(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(|s| s.as_str())
    }

    /// Sets an attribute key-value pair.
    pub fn put_attribute(&mut self, key: String, value: String) {
        self.attributes.insert(key, value);
    }
}

/// A collection of FieldInfo objects indexed by name and number.
#[derive(Clone, Debug)]
pub struct FieldInfos {
    fields: Vec<FieldInfo>,
    by_name: HashMap<String, usize>,
    by_number: HashMap<u32, usize>,
    has_freq: bool,
    has_postings: bool,
    has_prox: bool,
    has_payloads: bool,
    has_offsets: bool,
    has_vectors: bool,
    has_norms: bool,
    has_doc_values: bool,
    has_point_values: bool,
    has_vector_values: bool,
}

impl FieldInfos {
    pub fn new(fields: Vec<FieldInfo>) -> Self {
        let mut by_name = HashMap::new();
        let mut by_number = HashMap::new();
        let mut has_freq = false;
        let mut has_postings = false;
        let mut has_prox = false;
        let mut has_payloads = false;
        let mut has_offsets = false;
        let mut has_vectors = false;
        let mut has_norms = false;
        let mut has_doc_values = false;
        let mut has_point_values = false;
        let mut has_vector_values = false;

        for (idx, fi) in fields.iter().enumerate() {
            by_name.insert(fi.name().to_string(), idx);
            by_number.insert(fi.number(), idx);

            if fi.index_options() != IndexOptions::None {
                has_postings = true;
            }
            if fi.index_options() >= IndexOptions::DocsAndFreqs {
                has_freq = true;
            }
            if fi.index_options() >= IndexOptions::DocsAndFreqsAndPositions {
                has_prox = true;
            }
            if fi.index_options() >= IndexOptions::DocsAndFreqsAndPositionsAndOffsets {
                has_offsets = true;
            }
            if fi.store_payloads() {
                has_payloads = true;
            }
            if fi.store_term_vector() {
                has_vectors = true;
            }
            if fi.has_norms() {
                has_norms = true;
            }
            if fi.doc_values_type() != DocValuesType::None {
                has_doc_values = true;
            }
            if fi.point_config().has_values() {
                has_point_values = true;
            }
            if fi.vector_dimension() > 0 {
                has_vector_values = true;
            }
        }

        Self {
            fields,
            by_name,
            by_number,
            has_freq,
            has_postings,
            has_prox,
            has_payloads,
            has_offsets,
            has_vectors,
            has_norms,
            has_doc_values,
            has_point_values,
            has_vector_values,
        }
    }

    pub fn field_info_by_name(&self, name: &str) -> Option<&FieldInfo> {
        self.by_name.get(name).map(|&idx| &self.fields[idx])
    }

    pub fn field_info_by_number(&self, number: u32) -> Option<&FieldInfo> {
        self.by_number.get(&number).map(|&idx| &self.fields[idx])
    }

    pub fn iter(&self) -> impl Iterator<Item = &FieldInfo> {
        self.fields.iter()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn has_freq(&self) -> bool {
        self.has_freq
    }

    pub fn has_postings(&self) -> bool {
        self.has_postings
    }

    pub fn has_prox(&self) -> bool {
        self.has_prox
    }

    pub fn has_payloads(&self) -> bool {
        self.has_payloads
    }

    pub fn has_offsets(&self) -> bool {
        self.has_offsets
    }

    pub fn has_vectors(&self) -> bool {
        self.has_vectors
    }

    pub fn has_norms(&self) -> bool {
        self.has_norms
    }

    pub fn has_doc_values(&self) -> bool {
        self.has_doc_values
    }

    pub fn has_point_values(&self) -> bool {
        self.has_point_values
    }

    pub fn has_vector_values(&self) -> bool {
        self.has_vector_values
    }
}

/// Information about a segment in the index.
#[derive(Clone, Debug)]
pub struct SegmentInfo {
    pub name: String,
    pub max_doc: i32,
    pub is_compound_file: bool,
    pub id: [u8; string_helper::ID_LENGTH],
    pub diagnostics: HashMap<String, String>,
    pub attributes: HashMap<String, String>,
    pub files: HashSet<String>,
    pub has_blocks: bool,
}

impl SegmentInfo {
    pub fn new(
        name: String,
        max_doc: i32,
        is_compound_file: bool,
        id: [u8; string_helper::ID_LENGTH],
        diagnostics: HashMap<String, String>,
        attributes: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            max_doc,
            is_compound_file,
            id,
            diagnostics,
            attributes,
            files: HashSet::new(),
            has_blocks: false,
        }
    }
}

/// Information about a segment at a particular commit point.
#[derive(Clone, Debug)]
pub struct SegmentCommitInfo {
    pub info: SegmentInfo,
    pub field_infos: FieldInfos,
    pub del_count: i32,
    pub soft_del_count: i32,
    pub del_gen: i64,
    pub field_infos_gen: i64,
    pub doc_values_gen: i64,
    pub id: Option<[u8; string_helper::ID_LENGTH]>,
}

impl SegmentCommitInfo {
    pub fn new(
        info: SegmentInfo,
        field_infos: FieldInfos,
        id: Option<[u8; string_helper::ID_LENGTH]>,
    ) -> Self {
        Self {
            info,
            field_infos,
            del_count: 0,
            soft_del_count: 0,
            del_gen: -1,
            field_infos_gen: -1,
            doc_values_gen: -1,
            id,
        }
    }

    pub fn has_deletions(&self) -> bool {
        self.del_gen != -1
    }

    pub fn has_field_updates(&self) -> bool {
        self.field_infos_gen != -1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util;

    fn make_test_field_info(name: &str, number: u32, index_opts: IndexOptions) -> FieldInfo {
        test_util::make_field_info(name, number, false, index_opts, DocValuesType::None)
    }

    #[test]
    fn test_field_info_basics() {
        let fi = make_test_field_info("contents", 0, IndexOptions::DocsAndFreqsAndPositions);
        assert!(fi.is_indexed());
        assert!(fi.has_norms());
        assert!(!fi.has_payloads());
        assert!(!fi.has_doc_values());
        assert!(!fi.has_point_values());
    }

    #[test]
    fn test_field_info_keyword() {
        let fi = FieldInfo::new(
            "path".to_string(),
            0,
            false,
            true, // omitNorms
            IndexOptions::Docs,
            DocValuesType::SortedSet,
            PointDimensionConfig::default(),
        );
        assert!(fi.is_indexed());
        assert!(!fi.has_norms()); // omitNorms=true
        assert!(fi.has_doc_values());
    }

    #[test]
    fn test_field_infos_aggregation() {
        let fi_path = FieldInfo::new(
            "path".to_string(),
            0,
            false,
            true,
            IndexOptions::Docs,
            DocValuesType::SortedSet,
            PointDimensionConfig::default(),
        );
        let fi_modified = FieldInfo::new(
            "modified".to_string(),
            1,
            false,
            false,
            IndexOptions::None,
            DocValuesType::SortedNumeric,
            PointDimensionConfig {
                dimension_count: 1,
                index_dimension_count: 1,
                num_bytes: 8,
            },
        );
        let fi_contents = FieldInfo::new(
            "contents".to_string(),
            2,
            false,
            false,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::None,
            PointDimensionConfig::default(),
        );

        let fis = FieldInfos::new(vec![fi_path, fi_modified, fi_contents]);
        assert_len_eq_x!(&fis, 3);
        assert!(fis.has_postings());
        assert!(fis.has_freq());
        assert!(fis.has_prox());
        assert!(!fis.has_offsets());
        assert!(!fis.has_payloads());
        assert!(!fis.has_vectors());
        assert!(fis.has_norms());
        assert!(fis.has_doc_values());
        assert!(fis.has_point_values());
    }

    #[test]
    fn test_field_infos_lookup() {
        let fi = make_test_field_info("test", 42, IndexOptions::Docs);
        let fis = FieldInfos::new(vec![fi]);

        assert_some!(fis.field_info_by_name("test"));
        assert_eq!(fis.field_info_by_name("test").unwrap().number(), 42);
        assert_some!(fis.field_info_by_number(42));
        assert_none!(fis.field_info_by_name("missing"));
    }

    #[test]
    fn test_segment_info() {
        let id = [0u8; 16];
        let si = SegmentInfo::new(
            "_0".to_string(),
            3,
            true,
            id,
            HashMap::new(),
            HashMap::new(),
        );
        assert_eq!(si.name, "_0");
        assert_eq!(si.max_doc, 3);
        assert!(si.is_compound_file);
    }

    #[test]
    fn test_segment_commit_info() {
        let id = [0u8; 16];
        let si = SegmentInfo::new(
            "_0".to_string(),
            3,
            true,
            id,
            HashMap::new(),
            HashMap::new(),
        );
        let fis = FieldInfos::new(vec![]);
        let sci = SegmentCommitInfo::new(si, fis, Some([1u8; 16]));
        assert!(!sci.has_deletions());
        assert!(!sci.has_field_updates());
    }
}
