// SPDX-License-Identifier: Apache-2.0

//! `BooleanWeight` and `BooleanScorerSupplier` — weight and scorer creation for `BooleanQuery`.

use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;
use std::io;

use super::block_max_conjunction::BlockMaxConjunctionBulkScorer;
use super::boolean_query::{BooleanClause, Occur};
use super::boolean_scorer::BooleanScorer;
use super::collector::ScoreMode;
use super::conjunction::ConjunctionScorer;
use super::index_searcher::IndexSearcher;
use super::query::{BulkScorer, DefaultBulkScorer, ScorerSupplier, Weight};
use super::req_excl_bulk_scorer::ReqExclBulkScorer;
use super::scorer::Scorer;
use super::scorer_util;
use crate::index::directory_reader::LeafReaderContext;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// A `BooleanClause` paired with its `Weight`.
///
/// The reference implementation stores a `BooleanClause` reference here. In Rust, the clause
/// is borrowed during construction and only the `Occur` is retained — the `Query` inside the
/// clause has already been consumed by `create_weight`.
struct WeightedBooleanClause {
    occur: Occur,
    weight: Box<dyn Weight>,
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Expert: the Weight for BooleanQuery, used to score and explain boolean queries.
pub(crate) struct BooleanWeight {
    weighted_clauses: Vec<WeightedBooleanClause>,
    score_mode: ScoreMode,
    min_should_match: i32,
}

impl fmt::Debug for BooleanWeight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BooleanWeight")
            .field("num_clauses", &self.weighted_clauses.len())
            .field("score_mode", &self.score_mode)
            .field("min_should_match", &self.min_should_match)
            .finish()
    }
}

impl BooleanWeight {
    /// Creates a new `BooleanWeight` for the given clauses.
    pub(crate) fn new(
        clauses: &[BooleanClause],
        searcher: &IndexSearcher,
        score_mode: ScoreMode,
        min_should_match: i32,
        boost: f32,
    ) -> io::Result<Self> {
        let mut weighted_clauses = Vec::with_capacity(clauses.len());
        for c in clauses {
            let w = searcher.create_weight(
                c.query(),
                if c.occur() == Occur::Must || c.occur() == Occur::Should {
                    score_mode
                } else {
                    ScoreMode::CompleteNoScores
                },
                boost,
            )?;
            weighted_clauses.push(WeightedBooleanClause {
                occur: c.occur(),
                weight: w,
            });
        }
        Ok(Self {
            weighted_clauses,
            score_mode,
            min_should_match,
        })
    }
}

impl Weight for BooleanWeight {
    fn scorer_supplier(
        &self,
        context: &LeafReaderContext,
    ) -> io::Result<Option<Box<dyn ScorerSupplier>>> {
        let mut min_should_match = self.min_should_match;

        let mut scorers: HashMap<Occur, Vec<Box<dyn ScorerSupplier>>> = HashMap::new();
        scorers.insert(Occur::Must, Vec::new());
        scorers.insert(Occur::Filter, Vec::new());
        scorers.insert(Occur::Should, Vec::new());
        scorers.insert(Occur::MustNot, Vec::new());

        for wc in &self.weighted_clauses {
            let sub_scorer = wc.weight.scorer_supplier(context)?;
            match sub_scorer {
                None => {
                    if wc.occur == Occur::Must || wc.occur == Occur::Filter {
                        return Ok(None);
                    }
                }
                Some(supplier) => {
                    scorers.get_mut(&wc.occur).unwrap().push(supplier);
                }
            }
        }

        // scorer simplifications:

        if scorers[&Occur::Should].len() == min_should_match as usize {
            // any optional clauses are in fact required
            let should = scorers.remove(&Occur::Should).unwrap();
            scorers.get_mut(&Occur::Must).unwrap().extend(should);
            scorers.insert(Occur::Should, Vec::new());
            min_should_match = 0;
        }

        if scorers[&Occur::Filter].is_empty()
            && scorers[&Occur::Must].is_empty()
            && scorers[&Occur::Should].is_empty()
        {
            // no required and optional clauses.
            return Ok(None);
        } else if (scorers[&Occur::Should].len() as i32) < min_should_match {
            // either >1 req scorer, or there are 0 req scorers and at least 1
            // optional scorer. Therefore if there are not enough optional scorers
            // no documents will be matched by the query
            return Ok(None);
        }

        if !self.score_mode.needs_scores()
            && min_should_match == 0
            && scorers[&Occur::Must].len() + scorers[&Occur::Filter].len() > 0
        {
            // Purely optional clauses are useless without scoring.
            scorers.get_mut(&Occur::Should).unwrap().clear();
        }

        Ok(Some(Box::new(BooleanScorerSupplier::new(
            scorers,
            self.score_mode,
            min_should_match,
            context.reader.max_doc(),
        )?)))
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Supplier of scorers for `BooleanQuery`.
struct BooleanScorerSupplier {
    subs: HashMap<Occur, Vec<Box<dyn ScorerSupplier>>>,
    score_mode: ScoreMode,
    min_should_match: i32,
    max_doc: i32,
    cost: Cell<i64>,
    top_level_scoring_clause: bool,
}

impl fmt::Debug for BooleanScorerSupplier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BooleanScorerSupplier")
            .field("score_mode", &self.score_mode)
            .field("min_should_match", &self.min_should_match)
            .finish()
    }
}

impl BooleanScorerSupplier {
    fn new(
        subs: HashMap<Occur, Vec<Box<dyn ScorerSupplier>>>,
        score_mode: ScoreMode,
        min_should_match: i32,
        max_doc: i32,
    ) -> io::Result<Self> {
        if min_should_match < 0 {
            return Err(io::Error::other(format!(
                "minShouldMatch must be positive, but got: {min_should_match}"
            )));
        }
        if min_should_match != 0 && min_should_match as usize >= subs[&Occur::Should].len() {
            return Err(io::Error::other(
                "minShouldMatch must be strictly less than the number of SHOULD clauses",
            ));
        }
        if !score_mode.needs_scores()
            && min_should_match == 0
            && !subs[&Occur::Should].is_empty()
            && subs[&Occur::Must].len() + subs[&Occur::Filter].len() > 0
        {
            return Err(io::Error::other(
                "Cannot pass purely optional clauses if scores are not needed",
            ));
        }
        if subs[&Occur::Should].len() + subs[&Occur::Must].len() + subs[&Occur::Filter].len() == 0 {
            return Err(io::Error::other(
                "There should be at least one positive clause",
            ));
        }

        Ok(Self {
            subs,
            score_mode,
            min_should_match,
            max_doc,
            cost: Cell::new(-1),
            top_level_scoring_clause: false,
        })
    }

    fn compute_cost(&self) -> i64 {
        let min_required_cost = self.subs[&Occur::Must]
            .iter()
            .chain(self.subs[&Occur::Filter].iter())
            .map(|s| s.cost())
            .min();

        if let Some(min_cost) = min_required_cost
            && self.min_should_match == 0
        {
            return min_cost;
        }

        // No required clauses or minShouldMatch > 0: need should cost.
        let should_cost = self.compute_should_cost();
        min_required_cost.unwrap_or(i64::MAX).min(should_cost)
    }

    fn compute_should_cost(&self) -> i64 {
        let optional_scorers = &self.subs[&Occur::Should];
        let costs: Vec<i64> = optional_scorers.iter().map(|s| s.cost()).collect();
        scorer_util::cost_with_min_should_match(
            &costs,
            optional_scorers.len(),
            self.min_should_match,
        )
    }

    fn get_internal(&mut self, lead_cost: i64) -> io::Result<Box<dyn Scorer>> {
        // three cases: conjunction, disjunction, or mix
        let lead_cost = lead_cost.min(self.cost());
        let top_level_scoring_clause = self.top_level_scoring_clause;

        // pure conjunction
        if self.subs[&Occur::Should].is_empty() {
            let filter_suppliers = self.subs.remove(&Occur::Filter).unwrap_or_default();
            let must_suppliers = self.subs.remove(&Occur::Must).unwrap_or_default();
            let must_not_suppliers = self.subs.remove(&Occur::MustNot).unwrap_or_default();
            let req_scorer = Self::req(
                filter_suppliers,
                must_suppliers,
                lead_cost,
                top_level_scoring_clause,
                self.score_mode,
            )?;
            return Self::excl(req_scorer, must_not_suppliers, lead_cost);
        }

        // pure disjunction
        if self.subs[&Occur::Filter].is_empty() && self.subs[&Occur::Must].is_empty() {
            todo!("pure disjunction not yet implemented")
        }

        // conjunction-disjunction mix
        todo!("conjunction-disjunction mix not yet implemented")
    }

    fn boolean_scorer(&mut self) -> io::Result<Option<Box<dyn BulkScorer>>> {
        let num_optional_clauses = self.subs[&Occur::Should].len();
        let num_must_clauses = self.subs[&Occur::Must].len();
        let num_required_clauses = num_must_clauses + self.subs[&Occur::Filter].len();

        let positive_scorer;
        if num_required_clauses == 0 {
            let cost_threshold: i64 = if self.min_should_match <= 1 {
                // when all clauses are optional, use BooleanScorer aggressively
                -1
            } else {
                // when a minimum number of clauses should match, BooleanScorer is
                // going to score all windows that have at least minNrShouldMatch
                // matches in the window
                (self.max_doc / 3) as i64
            };

            if self.cost() < cost_threshold {
                return Ok(None);
            }

            match self.optional_bulk_scorer()? {
                Some(s) => positive_scorer = s,
                None => return Ok(None),
            }
        } else if num_must_clauses == 0 && num_optional_clauses > 1 && self.min_should_match >= 1 {
            // filteredOptionalBulkScorer: not yet implemented.
            return Ok(None);
        } else if num_required_clauses > 0
            && num_optional_clauses == 0
            && self.min_should_match == 0
        {
            match self.required_bulk_scorer()? {
                Some(s) => positive_scorer = s,
                None => return Ok(None),
            }
        } else {
            return Ok(None);
        }

        let positive_scorer_cost = positive_scorer.cost();

        let mut prohibited: Vec<Box<dyn Scorer>> = Vec::new();
        for ss in self.subs.get_mut(&Occur::MustNot).unwrap() {
            prohibited.push(ss.get(positive_scorer_cost)?);
        }

        if prohibited.is_empty() {
            Ok(Some(positive_scorer))
        } else if prohibited.len() == 1 {
            let prohibited_scorer = prohibited.remove(0);
            Ok(Some(Box::new(ReqExclBulkScorer::new(
                positive_scorer,
                prohibited_scorer,
            ))))
        } else {
            todo!("multiple MUST_NOT not yet implemented")
        }
    }

    /// Returns a `BulkScorer` for the optional (SHOULD) clauses only, or `None` if not
    /// applicable.
    fn optional_bulk_scorer(&mut self) -> io::Result<Option<Box<dyn BulkScorer>>> {
        let should = self.subs.get_mut(&Occur::Should).unwrap();
        if should.is_empty() {
            return Ok(None);
        }
        // Single SHOULD clause with msm <= 1: delegate directly
        if should.len() == 1 && self.min_should_match <= 1 {
            return Ok(Some(should[0].bulk_scorer()?));
        }

        // TODO: TOP_SCORES + msm <= 1 should use MaxScoreBulkScorer.
        // Fall through to BooleanScorer for now.

        let should_cost = self.compute_should_cost();
        let should = self.subs.get_mut(&Occur::Should).unwrap();
        let mut optional: Vec<Box<dyn Scorer>> = Vec::new();
        for ss in should {
            optional.push(ss.get(should_cost)?);
        }

        Ok(Some(Box::new(BooleanScorer::new(
            optional,
            self.min_should_match.max(1),
            self.score_mode.needs_scores(),
        )?)))
    }

    fn required_bulk_scorer(&mut self) -> io::Result<Option<Box<dyn BulkScorer>>> {
        if self.subs[&Occur::Must].len() + self.subs[&Occur::Filter].len() == 0 {
            return Ok(None);
        }
        if self.subs[&Occur::Must].len() + self.subs[&Occur::Filter].len() == 1 {
            if !self.subs[&Occur::Must].is_empty() {
                return Ok(Some(
                    self.subs.get_mut(&Occur::Must).unwrap()[0].bulk_scorer()?,
                ));
            } else {
                let scorer = self.subs.get_mut(&Occur::Filter).unwrap()[0].bulk_scorer()?;
                // TODO: disableScoring if scoreMode.needsScores()
                return Ok(Some(scorer));
            }
        }

        let must_lead_cost = self.subs[&Occur::Must]
            .iter()
            .map(|s| s.cost())
            .min()
            .unwrap_or(i64::MAX);
        let filter_lead_cost = self.subs[&Occur::Filter]
            .iter()
            .map(|s| s.cost())
            .min()
            .unwrap_or(i64::MAX);
        let lead_cost = must_lead_cost.min(filter_lead_cost);

        let mut required_no_scoring: Vec<Box<dyn Scorer>> = Vec::new();
        for ss in self.subs.get_mut(&Occur::Filter).unwrap() {
            required_no_scoring.push(ss.get(lead_cost)?);
        }
        let mut required_scoring: Vec<Box<dyn Scorer>> = Vec::new();
        let required_scoring_supplier_size = self.subs[&Occur::Must].len();
        for ss in self.subs.get_mut(&Occur::Must).unwrap() {
            if required_scoring_supplier_size == 1 {
                ss.set_top_level_scoring_clause()?;
            }
            required_scoring.push(ss.get(lead_cost)?);
        }

        // Java lines 387-397: TOP_SCORES with multiple scoring scorers and no two-phase
        // iterators → use BlockMaxConjunctionBulkScorer for dynamic pruning.
        // Note: We don't have twoPhaseIterator in Rust yet, so the two-phase check is
        // trivially true (our TermQuery scorers never have two-phase iterators).
        if self.score_mode == ScoreMode::TopScores && required_scoring.len() > 1 {
            // Java wraps filter scorers as ConstantScoreScorer(0f) here.
            // We don't have ConstantScoreScorer yet — assert no filters for now.
            if !required_no_scoring.is_empty() {
                todo!(
                    "ConstantScoreScorer wrapping for FILTER clauses in BlockMaxConjunctionBulkScorer"
                );
            }
            return Ok(Some(Box::new(BlockMaxConjunctionBulkScorer::new(
                self.max_doc,
                required_scoring,
            ))));
        }

        // Java lines 399-411: non-TOP_SCORES paths (ConjunctionBulkScorer,
        // DenseConjunctionBulkScorer) — not yet implemented, fall through to
        // DefaultBulkScorer wrapping a ConjunctionScorer.

        let conjunction_scorer: Box<dyn Scorer>;
        if required_no_scoring.len() + required_scoring.len() == 1 {
            if required_scoring.len() == 1 {
                conjunction_scorer = required_scoring.remove(0);
            } else {
                conjunction_scorer = required_no_scoring.remove(0);
                // TODO: if scoreMode.needsScores(), wrap in FilterScorer returning 0
            }
        } else {
            // Rust: ConjunctionScorer::new takes (non-scoring required, scoring) as disjoint vecs
            conjunction_scorer = Box::new(ConjunctionScorer::new(
                required_no_scoring,
                required_scoring,
            ));
            // TOP_SCORES with no scoring — not yet implemented.
        }
        Ok(Some(Box::new(DefaultBulkScorer::new(conjunction_scorer))))
    }

    fn req(
        mut required_no_scoring: Vec<Box<dyn ScorerSupplier>>,
        mut required_scoring: Vec<Box<dyn ScorerSupplier>>,
        lead_cost: i64,
        top_level_scoring_clause: bool,
        score_mode: ScoreMode,
    ) -> io::Result<Box<dyn Scorer>> {
        if required_no_scoring.len() + required_scoring.len() == 1 {
            let req = if required_no_scoring.is_empty() {
                required_scoring[0].get(lead_cost)?
            } else {
                required_no_scoring[0].get(lead_cost)?
            };

            if !score_mode.needs_scores() {
                return Ok(req);
            }

            if required_scoring.is_empty() {
                // Scores are needed but we only have a filter clause.
                // TODO: wrap in FilterScorer returning score=0
                return Ok(req);
            }

            return Ok(req);
        }

        let mut required_scorers: Vec<Box<dyn Scorer>> = Vec::new();
        let mut scoring_scorers: Vec<Box<dyn Scorer>> = Vec::new();
        for s in &mut required_no_scoring {
            required_scorers.push(s.get(lead_cost)?);
        }
        for s in &mut required_scoring {
            scoring_scorers.push(s.get(lead_cost)?);
        }

        // — not yet implemented, fall through.
        let _ = top_level_scoring_clause;

        // Rust: ConjunctionScorer::new takes (non-scoring required, scoring) as disjoint vecs
        Ok(Box::new(ConjunctionScorer::new(
            required_scorers,
            scoring_scorers,
        )))
    }

    fn excl(
        main: Box<dyn Scorer>,
        mut prohibited: Vec<Box<dyn ScorerSupplier>>,
        lead_cost: i64,
    ) -> io::Result<Box<dyn Scorer>> {
        if prohibited.is_empty() {
            Ok(main)
        } else {
            let _ = lead_cost;
            let _ = prohibited.drain(..);
            todo!("MUST_NOT exclusion not yet implemented")
        }
    }
}

impl ScorerSupplier for BooleanScorerSupplier {
    fn get(&mut self, lead_cost: i64) -> io::Result<Box<dyn Scorer>> {
        let scorer = self.get_internal(lead_cost)?;
        // with no scoring clauses — not yet implemented.
        Ok(scorer)
    }

    fn bulk_scorer(&mut self) -> io::Result<Box<dyn BulkScorer>> {
        let bulk_scorer = self.boolean_scorer()?;
        if let Some(bs) = bulk_scorer {
            // bulk scoring is applicable, use it
            Ok(bs)
        } else {
            // use a Scorer-based impl (BS2)
            let scorer = self.get(i64::MAX)?;
            Ok(Box::new(DefaultBulkScorer::new(scorer)))
        }
    }

    fn cost(&self) -> i64 {
        if self.cost.get() == -1 {
            self.cost.set(self.compute_cost());
        }
        self.cost.get()
    }

    fn set_top_level_scoring_clause(&mut self) -> io::Result<()> {
        self.top_level_scoring_clause = true;
        if self.subs[&Occur::Should].len() + self.subs[&Occur::Must].len() == 1 {
            // If there is a single scoring clause, propagate the call.
            for ss in self.subs.get_mut(&Occur::Should).unwrap() {
                ss.set_top_level_scoring_clause()?;
            }
            for ss in self.subs.get_mut(&Occur::Must).unwrap() {
                ss.set_top_level_scoring_clause()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{self, Document};
    use crate::index::directory_reader::DirectoryReader;
    use crate::index::{IndexWriter, IndexWriterConfig};
    use crate::search::term_query::TermQuery;
    use crate::store::{Directory, MemoryDirectory};

    fn build_test_index() -> (Box<dyn Directory>, DirectoryReader) {
        let config = IndexWriterConfig::new().set_use_compound_file(false);
        let writer = IndexWriter::with_config(config);

        let mut doc = Document::new();
        doc.add(document::text_field("content", "hello world"));
        writer.add_document(doc).unwrap();

        let mut doc = Document::new();
        doc.add(document::text_field("content", "hello there"));
        writer.add_document(doc).unwrap();

        let mut doc = Document::new();
        doc.add(document::text_field("content", "world peace"));
        writer.add_document(doc).unwrap();

        let result = writer.commit().unwrap();
        let seg_files = result.into_segment_files().unwrap();

        let mut mem_dir = MemoryDirectory::new();
        for sf in &seg_files {
            mem_dir.write_file(&sf.name, &sf.data).unwrap();
        }
        let dir = Box::new(mem_dir) as Box<dyn Directory>;
        let reader = DirectoryReader::open(dir.as_ref()).unwrap();
        (dir, reader)
    }

    #[test]
    fn test_boolean_weight_required_clause_no_match() {
        let (_dir, reader) = build_test_index();
        let searcher = IndexSearcher::new(&reader);

        let clauses = vec![
            BooleanClause::new(Box::new(TermQuery::new("content", b"hello")), Occur::Must),
            BooleanClause::new(
                Box::new(TermQuery::new("content", b"nonexistent")),
                Occur::Must,
            ),
        ];
        let weight = BooleanWeight::new(&clauses, &searcher, ScoreMode::Complete, 0, 1.0).unwrap();

        // "nonexistent" has no postings, so scorer_supplier should return None
        for leaf in reader.leaves() {
            let supplier = weight.scorer_supplier(leaf).unwrap();
            assert!(
                supplier.is_none(),
                "expected None when a MUST clause has no matches"
            );
        }
    }

    #[test]
    fn test_boolean_weight_two_must_clauses() {
        let (_dir, reader) = build_test_index();
        let searcher = IndexSearcher::new(&reader);

        let clauses = vec![
            BooleanClause::new(Box::new(TermQuery::new("content", b"hello")), Occur::Must),
            BooleanClause::new(Box::new(TermQuery::new("content", b"world")), Occur::Must),
        ];
        let weight = BooleanWeight::new(&clauses, &searcher, ScoreMode::Complete, 0, 1.0).unwrap();

        for leaf in reader.leaves() {
            let supplier = weight.scorer_supplier(leaf).unwrap();
            assert!(
                supplier.is_some(),
                "expected Some when both MUST clauses have matches"
            );
        }
    }
}
