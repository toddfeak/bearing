// SPDX-License-Identifier: Apache-2.0

//! Query execution types: `Weight`, `ScorerSupplier`, and `BulkScorer`.

use std::io;

use super::collector::LeafCollector;
use super::doc_id_set_iterator::DocIdSetIterator;
use super::scorer::Scorer;
use crate::index::directory_reader::LeafReaderContext;

// ---------------------------------------------------------------------------
// BulkScorer
// ---------------------------------------------------------------------------

/// Scores a range of documents at once, returned by `Weight::bulk_scorer`.
///
/// Only queries that have a more optimized means of scoring across a range of documents
/// need to override this. Otherwise, a default implementation is wrapped around the `Scorer`
/// returned by `Weight::scorer`.
pub trait BulkScorer {
    /// Collects matching documents in a range and returns an estimation of the next matching
    /// document which is on or after `max`.
    ///
    /// `min` is the minimum document to be considered for matching. All documents strictly
    /// before this value must be ignored. `max` is the upper bound (exclusive).
    fn score(&mut self, collector: &mut dyn LeafCollector, min: i32, max: i32) -> io::Result<i32>;

    /// Same as `DocIdSetIterator::cost` for bulk scorers.
    fn cost(&self) -> i64;
}

// ---------------------------------------------------------------------------
// ScorerSupplier
// ---------------------------------------------------------------------------

/// A supplier of `Scorer`. This allows getting an estimate of the cost before building the
/// `Scorer`.
pub trait ScorerSupplier {
    /// Get the `Scorer`. This may not return `None` and must be called at most once.
    ///
    /// `lead_cost` can be interpreted as an upper bound of the number of times that
    /// `DocIdSetIterator::next_doc`, `DocIdSetIterator::advance` will be called. Under doubt,
    /// pass `i64::MAX`.
    fn get(&mut self, lead_cost: i64) -> io::Result<Box<dyn Scorer>>;

    /// Optional method: Get a scorer that is optimized for bulk-scoring. The default
    /// implementation iterates matches from the `Scorer`.
    fn bulk_scorer(&mut self) -> io::Result<Box<dyn BulkScorer>> {
        let scorer = self.get(i64::MAX)?;
        Ok(Box::new(DefaultBulkScorer::new(scorer)))
    }

    /// Get an estimate of the `Scorer` that would be returned by `get`. This may be a costly
    /// operation, so it should only be called if necessary.
    fn cost(&self) -> i64;

    /// Inform this `ScorerSupplier` that its returned scorers produce scores that get passed
    /// to the collector, as opposed to partial scores that then need to get combined.
    fn set_top_level_scoring_clause(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Weight
// ---------------------------------------------------------------------------

/// Expert: Calculate query weights and build query scorers.
///
/// The purpose of `Weight` is to ensure searching does not modify a `Query`, so that a
/// `Query` instance can be reused.
///
/// `IndexSearcher` dependent state of the query should reside in the `Weight`.
/// `LeafReader` dependent state should reside in the `Scorer`.
pub trait Weight {
    /// Get a `ScorerSupplier`, which allows knowing the cost of the `Scorer` before building
    /// it. Returns `None` if no documents match.
    fn scorer_supplier(
        &self,
        context: &LeafReaderContext,
    ) -> io::Result<Option<Box<dyn ScorerSupplier>>>;

    /// Returns a `Scorer` which can iterate in order over all matching documents and assign
    /// them a score. Returns `None` if no documents match.
    ///
    /// The default implementation delegates to `scorer_supplier`.
    fn scorer(&self, context: &LeafReaderContext) -> io::Result<Option<Box<dyn Scorer>>> {
        match self.scorer_supplier(context)? {
            None => Ok(None),
            Some(mut supplier) => Ok(Some(supplier.get(i64::MAX)?)),
        }
    }

    /// Helper method that delegates to `scorer_supplier`. Returns a `BulkScorer` for the
    /// given leaf, or `None` if no documents match.
    ///
    /// The default implementation calls `set_top_level_scoring_clause` then `bulk_scorer`.
    fn bulk_scorer(&self, context: &LeafReaderContext) -> io::Result<Option<Box<dyn BulkScorer>>> {
        match self.scorer_supplier(context)? {
            None => Ok(None),
            Some(mut supplier) => {
                supplier.set_top_level_scoring_clause()?;
                Ok(Some(supplier.bulk_scorer()?))
            }
        }
    }

    /// Counts the number of live documents that match. Returns -1 if the count could not be
    /// computed in sub-linear time.
    fn count(&self, _context: &LeafReaderContext) -> io::Result<i32> {
        Ok(-1)
    }
}

// ---------------------------------------------------------------------------
// DefaultBulkScorer
// ---------------------------------------------------------------------------

/// Just wraps a `Scorer` and performs top scoring using it.
///
/// This is the default `BulkScorer` implementation when a `Weight` doesn't provide a
/// specialized one.
pub struct DefaultBulkScorer {
    scorer: Box<dyn Scorer>,
}

impl DefaultBulkScorer {
    /// Sole constructor.
    pub fn new(scorer: Box<dyn Scorer>) -> Self {
        Self { scorer }
    }

    /// Score documents in `[min, max)` using the simple iterator path (no TwoPhaseIterator,
    /// no competitive iterator).
    fn score_iterator(
        collector: &mut dyn LeafCollector,
        iterator: &mut dyn DocIdSetIterator,
        max: i32,
    ) -> io::Result<()> {
        let mut doc = iterator.doc_id();
        while doc < max {
            collector.collect(doc)?;
            doc = iterator.next_doc()?;
        }
        Ok(())
    }

    /// Score documents in `[min, max)` using a competitive iterator that can skip
    /// non-competitive docs.
    fn score_competitive_iterator(
        collector: &mut dyn LeafCollector,
        iterator: &mut dyn DocIdSetIterator,
        competitive_iterator: &mut dyn DocIdSetIterator,
        max: i32,
    ) -> io::Result<()> {
        let mut doc = iterator.doc_id();
        while doc < max {
            debug_assert!(competitive_iterator.doc_id() <= doc);
            if competitive_iterator.doc_id() < doc {
                let competitive_next = competitive_iterator.advance(doc)?;
                if competitive_next != doc {
                    doc = iterator.advance(competitive_next)?;
                    continue;
                }
            }
            collector.collect(doc)?;
            doc = iterator.next_doc()?;
        }
        Ok(())
    }
}

impl BulkScorer for DefaultBulkScorer {
    fn score(&mut self, collector: &mut dyn LeafCollector, min: i32, max: i32) -> io::Result<i32> {
        collector.set_scorer(self.scorer.as_mut())?;
        let competitive_iterator = collector.competitive_iterator();

        let iterator = self.scorer.iterator();

        if iterator.doc_id() < min {
            if iterator.doc_id() == min - 1 {
                iterator.next_doc()?;
            } else {
                iterator.advance(min)?;
            }
        }

        match competitive_iterator {
            None => {
                Self::score_iterator(collector, iterator, max)?;
            }
            Some(mut ci) => {
                let ci_doc = ci.doc_id();
                let effective_min = if ci_doc > min { ci_doc.min(max) } else { min };
                if iterator.doc_id() < effective_min {
                    iterator.advance(effective_min)?;
                }
                Self::score_competitive_iterator(collector, iterator, ci.as_mut(), max)?;
            }
        }

        Ok(iterator.doc_id())
    }

    fn cost(&self) -> i64 {
        // TODO: Java returns `iterator.cost()` here, but Rust's `Scorer::iterator()` requires
        // `&mut self` which conflicts with `BulkScorer::cost(&self)`. To fix properly, either
        // cache the cost at construction time, or change the Scorer trait to expose cost
        // without &mut self. Returns 0 as a placeholder — callers currently get cost from
        // ScorerSupplier before constructing the BulkScorer.
        0
    }
}

// ---------------------------------------------------------------------------
// DefaultScorerSupplier
// ---------------------------------------------------------------------------

/// A wrapper for a pre-built `Scorer` that implements `ScorerSupplier`.
pub struct DefaultScorerSupplier {
    scorer: Option<Box<dyn Scorer>>,
    cost: i64,
}

impl DefaultScorerSupplier {
    /// Creates a new `DefaultScorerSupplier` wrapping the given scorer.
    pub fn new(scorer: Box<dyn Scorer>, cost: i64) -> Self {
        Self {
            scorer: Some(scorer),
            cost,
        }
    }
}

impl ScorerSupplier for DefaultScorerSupplier {
    fn get(&mut self, _lead_cost: i64) -> io::Result<Box<dyn Scorer>> {
        self.scorer
            .take()
            .ok_or_else(|| io::Error::other("ScorerSupplier.get() called more than once"))
    }

    fn cost(&self) -> i64 {
        self.cost
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::doc_id_set_iterator::NO_MORE_DOCS;
    use crate::search::scorable::Scorable;

    /// DocIdSetIterator over a fixed list of doc IDs.
    struct MockScorerIterator {
        docs: Vec<i32>,
        index: usize,
    }

    impl DocIdSetIterator for MockScorerIterator {
        fn doc_id(&self) -> i32 {
            if self.index == 0 {
                -1
            } else if self.index > self.docs.len() {
                NO_MORE_DOCS
            } else {
                self.docs[self.index - 1]
            }
        }
        fn next_doc(&mut self) -> io::Result<i32> {
            if self.index >= self.docs.len() {
                self.index = self.docs.len() + 1;
                return Ok(NO_MORE_DOCS);
            }
            self.index += 1;
            Ok(self.docs[self.index - 1])
        }
        fn advance(&mut self, target: i32) -> io::Result<i32> {
            loop {
                let doc = self.next_doc()?;
                if doc >= target {
                    return Ok(doc);
                }
            }
        }
        fn cost(&self) -> i64 {
            self.docs.len() as i64
        }
    }

    /// We need Scorer to delegate to an iterator. Since Scorer::iterator() returns
    /// &mut dyn DocIdSetIterator, MockScorer needs to own an iterator.
    struct FullMockScorer {
        iter: MockScorerIterator,
        scores: Vec<f32>,
    }

    impl FullMockScorer {
        fn new(docs: Vec<i32>, scores: Vec<f32>) -> Self {
            let iter = MockScorerIterator {
                docs: docs.clone(),
                index: 0,
            };
            Self { iter, scores }
        }
    }

    impl Scorable for FullMockScorer {
        fn score(&mut self) -> io::Result<f32> {
            let doc = self.iter.doc_id();
            if doc < 0 || doc == NO_MORE_DOCS {
                return Ok(0.0);
            }
            // Find the index of this doc
            for (i, &d) in self.iter.docs.iter().enumerate() {
                if d == doc {
                    return Ok(self.scores[i]);
                }
            }
            Ok(0.0)
        }
    }

    impl Scorer for FullMockScorer {
        fn doc_id(&self) -> i32 {
            self.iter.doc_id()
        }
        fn iterator(&mut self) -> &mut dyn DocIdSetIterator {
            &mut self.iter
        }
        fn get_max_score(&mut self, _up_to: i32) -> io::Result<f32> {
            Ok(f32::MAX)
        }
    }

    // -- DefaultScorerSupplier tests --

    #[test]
    fn test_default_scorer_supplier_get() {
        let scorer = FullMockScorer::new(vec![0, 1, 2], vec![1.0, 2.0, 3.0]);
        let mut supplier = DefaultScorerSupplier::new(Box::new(scorer), 3);
        assert_eq!(supplier.cost(), 3);
        let s = supplier.get(100);
        assert!(s.is_ok());
    }

    #[test]
    fn test_default_scorer_supplier_get_twice_fails() {
        let scorer = FullMockScorer::new(vec![0], vec![1.0]);
        let mut supplier = DefaultScorerSupplier::new(Box::new(scorer), 1);
        supplier.get(100).unwrap();
        let result = supplier.get(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_default_scorer_supplier_bulk_scorer() {
        let scorer = FullMockScorer::new(vec![0, 1, 2], vec![1.0, 2.0, 3.0]);
        let mut supplier = DefaultScorerSupplier::new(Box::new(scorer), 3);
        let bs = supplier.bulk_scorer();
        assert!(bs.is_ok());
    }

    // -- DefaultBulkScorer tests --

    /// Simple leaf collector that records doc IDs.
    struct DocCollector {
        docs: Vec<i32>,
    }

    impl DocCollector {
        fn new() -> Self {
            Self { docs: Vec::new() }
        }
    }

    impl LeafCollector for DocCollector {
        fn set_scorer(&mut self, _scorer: &mut dyn Scorable) -> io::Result<()> {
            Ok(())
        }
        fn collect(&mut self, doc: i32) -> io::Result<()> {
            self.docs.push(doc);
            Ok(())
        }
    }

    #[test]
    fn test_default_bulk_scorer_scores_all_docs() {
        let scorer = FullMockScorer::new(vec![0, 5, 10], vec![1.0, 2.0, 3.0]);
        let mut bulk = DefaultBulkScorer::new(Box::new(scorer));
        let mut collector = DocCollector::new();

        bulk.score(&mut collector, 0, NO_MORE_DOCS).unwrap();

        assert_eq!(collector.docs, vec![0, 5, 10]);
    }

    #[test]
    fn test_default_bulk_scorer_respects_range() {
        let scorer = FullMockScorer::new(vec![0, 5, 10, 15], vec![1.0, 2.0, 3.0, 4.0]);
        let mut bulk = DefaultBulkScorer::new(Box::new(scorer));
        let mut collector = DocCollector::new();

        // Score docs in [3, 12) — should get doc 5 and 10
        bulk.score(&mut collector, 3, 12).unwrap();

        assert_eq!(collector.docs, vec![5, 10]);
    }

    #[test]
    fn test_default_bulk_scorer_empty_range() {
        let scorer = FullMockScorer::new(vec![10, 20], vec![1.0, 2.0]);
        let mut bulk = DefaultBulkScorer::new(Box::new(scorer));
        let mut collector = DocCollector::new();

        bulk.score(&mut collector, 0, 5).unwrap();

        assert!(collector.docs.is_empty());
    }

    // NOTE: Weight::scorer and Weight::bulk_scorer default method tests require a real
    // LeafReaderContext (which needs a SegmentReader with actual index data). These will be
    // tested as integration tests when TermQuery (Tier 5) wires everything together.
    // The default method logic is straightforward delegation and is covered by the
    // DefaultScorerSupplier and DefaultBulkScorer tests above.
}
