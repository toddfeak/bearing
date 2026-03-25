// SPDX-License-Identifier: Apache-2.0

//! Search types for query execution, scoring, and result collection.

pub mod collector;
pub mod doc_id_set_iterator;
pub mod scorable;
pub mod similarity;
pub mod top_docs;

pub use collector::{
    Collector, CollectorManager, DocAndFloatFeatureBuffer, DocIdStream, LeafCollector,
    RangeDocIdStream, ScoreMode,
};
pub use doc_id_set_iterator::{DocIdSetIterator, NO_MORE_DOCS};
pub use scorable::Scorable;
pub use similarity::{
    BM25Similarity, BulkSimScorer, CollectionStatistics, SimScorer, Similarity, TermStatistics,
};
pub use top_docs::{Relation, ScoreDoc, TopDocs, TotalHits};
