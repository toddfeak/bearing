// SPDX-License-Identifier: Apache-2.0

//! Search types for query execution, scoring, and result collection.

pub mod boolean_query;
pub mod boolean_weight;
pub mod collector;
pub mod conjunction;
pub mod doc_id_set_iterator;
pub mod index_searcher;
pub mod long_heap;
pub mod query;
pub mod scorable;
pub mod scorer;
pub mod similarity;
pub mod term_query;
pub mod term_states;
pub mod top_docs;
pub mod top_score_doc_collector;

pub use boolean_query::{BooleanClause, BooleanQuery, Occur};
pub use collector::{
    Collector, CollectorManager, DocAndFloatFeatureBuffer, DocIdStream, LeafCollector,
    RangeDocIdStream, ScoreContext, ScoreMode, SimpleScorable,
};
pub use doc_id_set_iterator::{DocIdSetIterator, NO_MORE_DOCS};
pub use index_searcher::IndexSearcher;
pub use long_heap::LongHeap;
pub use query::{BatchScoreBulkScorer, BulkScorer, Query, ScorerSupplier, Weight};
pub use scorable::Scorable;
pub use scorer::{DocScoreEncoder, ImpactsSource, MaxScoreAccumulator, Scorer};
pub use similarity::{
    BM25Similarity, BulkSimScorer, CollectionStatistics, SimScorer, Similarity, TermStatistics,
};
pub use term_query::TermQuery;
pub use term_states::TermStates;
pub use top_docs::{Relation, ScoreDoc, TopDocs, TotalHits};
pub use top_score_doc_collector::{TopScoreDocCollector, TopScoreDocCollectorManager};
