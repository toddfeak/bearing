// SPDX-License-Identifier: Apache-2.0

//! `TermQuery` matches documents containing a specific term, producing BM25 scores.

use std::io;

use crate::codecs::lucene103::postings_reader::BlockPostingsEnum;
use crate::index::directory_reader::LeafReaderContext;
use crate::search::collector::{DocAndFloatFeatureBuffer, ScoreMode};
use crate::search::doc_id_set_iterator::{DocIdSetIterator, NO_MORE_DOCS};
use crate::search::query::{
    BatchScoreBulkScorer, BulkScorer, DefaultBulkScorer, Query, ScorerSupplier, Weight,
};
use crate::search::scorable::Scorable;
use crate::search::scorer::Scorer;
use crate::search::similarity::{CollectionStatistics, SimScorer, Similarity, TermStatistics};
use crate::util::BytesRef;

// ---------------------------------------------------------------------------
// TermQuery
// ---------------------------------------------------------------------------

/// A query that matches documents containing a term.
///
/// This may be combined with other queries via a `BooleanQuery`.
pub struct TermQuery {
    field: String,
    term: Vec<u8>,
}

impl TermQuery {
    /// Constructs a query for the given field and term bytes.
    pub fn new(field: &str, term: &[u8]) -> Self {
        Self {
            field: field.to_string(),
            term: term.to_vec(),
        }
    }

    /// Returns the field name.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Returns the term bytes.
    pub fn term(&self) -> &[u8] {
        &self.term
    }
}

impl Query for TermQuery {
    fn create_weight(
        &self,
        similarity: &dyn Similarity,
        score_mode: ScoreMode,
        boost: f32,
    ) -> io::Result<Box<dyn Weight>> {
        Ok(Box::new(TermWeight {
            field: self.field.clone(),
            term: self.term.clone(),
            similarity: similarity.box_clone(),
            score_mode,
            boost,
        }))
    }
}

// ---------------------------------------------------------------------------
// TermWeight
// ---------------------------------------------------------------------------

/// Expert: Calculate query weights and build scorers for a `TermQuery`.
///
/// Holds the field, term, similarity, score mode, and boost. The `SimScorer` is created
/// per-leaf in `scorer_supplier` using per-segment statistics. When `IndexSearcher` is
/// available (Tier 7), statistics will be aggregated across segments first.
pub struct TermWeight {
    field: String,
    term: Vec<u8>,
    similarity: Box<dyn Similarity>,
    score_mode: ScoreMode,
    boost: f32,
}

impl Weight for TermWeight {
    fn scorer_supplier(
        &self,
        context: &LeafReaderContext,
    ) -> io::Result<Option<Box<dyn ScorerSupplier>>> {
        let reader = &context.reader;

        // Look up the field info
        let field_info = match reader.field_infos().field_info_by_name(&self.field) {
            Some(fi) => fi,
            None => return Ok(None),
        };

        // Get terms reader to access field-level statistics
        let terms_reader = match reader.terms_reader() {
            Some(tr) => tr,
            None => return Ok(None),
        };

        let field_reader = match terms_reader.field_reader(field_info.number()) {
            Some(fr) => fr,
            None => return Ok(None),
        };

        // Navigate the trie to find the term and get IntBlockTermState
        let trie = field_reader.new_trie_reader()?;
        let trie_result = match trie.seek_to_block(&self.term)? {
            Some(r) => r,
            None => return Ok(None),
        };

        let index_input = field_reader.index_input()?;
        let term_state = crate::codecs::lucene103::segment_terms_enum::seek_exact(
            terms_reader.terms_in(),
            &trie_result,
            &self.term,
            field_info.index_options(),
            &*index_input,
        )?;

        let Some(state) = term_state else {
            return Ok(None);
        };

        // Build SimScorer from collection and term statistics
        let doc_freq = state.doc_freq as i64;
        let total_term_freq = if state.total_term_freq > 0 {
            state.total_term_freq
        } else {
            doc_freq
        };

        let sim_scorer = if self.score_mode.needs_scores() {
            let collection_stats = CollectionStatistics::new(
                self.field.clone(),
                reader.max_doc() as i64,
                field_reader.doc_count as i64,
                field_reader.sum_total_term_freq,
                field_reader.sum_doc_freq,
            );
            let term_stats =
                TermStatistics::new(BytesRef::new(self.term.clone()), doc_freq, total_term_freq);
            self.similarity
                .scorer(self.boost, &collection_stats, &[term_stats])
        } else {
            // Use fake stats for non-scoring mode
            let fake_coll = CollectionStatistics::new(self.field.clone(), 1, 1, 1, 1);
            let fake_term = TermStatistics::new(BytesRef::new(self.term.clone()), 1, 1);
            self.similarity.scorer(self.boost, &fake_coll, &[fake_term])
        };

        // Build postings enum — use impacts mode for TopScores
        let postings_reader = match reader.postings_reader() {
            Some(pr) => pr,
            None => return Ok(None),
        };

        let index_has_freq = field_info.index_options().has_freqs();
        let needs_freq = self.score_mode.needs_scores();
        let postings_enum = if self.score_mode == ScoreMode::TopScores {
            postings_reader.impacts(&state, index_has_freq, needs_freq)?
        } else {
            postings_reader.postings(&state, index_has_freq, needs_freq)?
        };

        // Load norms for this field. SegmentReader::get_norm uses RefCell for interior
        // mutability, allowing us to read norms from an immutable reference.
        let norms: Option<Box<[i64]>> = if field_info.has_norms() {
            let max_doc = reader.max_doc();
            let field_num = field_info.number();
            let mut norm_values = vec![1i64; max_doc as usize];
            for doc_id in 0..max_doc {
                norm_values[doc_id as usize] = reader.get_norm(field_num, doc_id)?;
            }
            Some(norm_values.into_boxed_slice())
        } else {
            None
        };

        Ok(Some(Box::new(TermScorerSupplier {
            postings_enum: Some(postings_enum),
            sim_scorer: Some(sim_scorer),
            norms,
            doc_freq: state.doc_freq,
            score_mode: self.score_mode,
            top_level_scoring_clause: false,
        })))
    }
}

// ---------------------------------------------------------------------------
// TermScorerSupplier
// ---------------------------------------------------------------------------

/// Supplies a `TermScorer` for a single leaf, holding pre-built components.
struct TermScorerSupplier {
    postings_enum: Option<BlockPostingsEnum>,
    sim_scorer: Option<Box<dyn SimScorer>>,
    norms: Option<Box<[i64]>>,
    doc_freq: i32,
    score_mode: ScoreMode,
    top_level_scoring_clause: bool,
}

impl ScorerSupplier for TermScorerSupplier {
    fn get(&mut self, _lead_cost: i64) -> io::Result<Box<dyn Scorer>> {
        let postings_enum = self
            .postings_enum
            .take()
            .ok_or_else(|| io::Error::other("ScorerSupplier.get() called more than once"))?;
        let sim_scorer = self
            .sim_scorer
            .take()
            .ok_or_else(|| io::Error::other("ScorerSupplier.get() called more than once"))?;
        let norms = self.norms.take();

        let scorer = TermScorer::new(
            postings_enum,
            sim_scorer,
            norms,
            self.score_mode,
            self.top_level_scoring_clause,
        );
        Ok(Box::new(scorer))
    }

    fn bulk_scorer(&mut self) -> io::Result<Box<dyn BulkScorer>> {
        if !self.score_mode.needs_scores() {
            let scorer = self.get(i64::MAX)?;
            return Ok(Box::new(DefaultBulkScorer::new(scorer)));
        }
        let scorer = self.get(i64::MAX)?;
        Ok(Box::new(BatchScoreBulkScorer::new(scorer)))
    }

    fn cost(&self) -> i64 {
        self.doc_freq as i64
    }

    fn set_top_level_scoring_clause(&mut self) -> io::Result<()> {
        self.top_level_scoring_clause = true;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TermScorer
// ---------------------------------------------------------------------------

/// Expert: A `Scorer` for documents matching a `Term`.
///
/// Iterates over matching documents and computes BM25 scores. When
/// `top_level_scoring_clause` is true, includes ImpactsDISI logic inline to skip
/// non-competitive blocks using `MaxScoreCache`.
pub struct TermScorer {
    postings_enum: BlockPostingsEnum,
    sim_scorer: Box<dyn SimScorer>,
    max_score_cache: crate::search::scorer::MaxScoreCache,
    norms: Option<Box<[i64]>>,
    norm_values: Vec<i64>,
    // ImpactsDISI state (inlined — see Java ImpactsDISI)
    min_competitive_score: f32,
    up_to: i32,
    max_score: f32,
    top_level_scoring_clause: bool,
}

impl TermScorer {
    /// Constructs a new `TermScorer`.
    fn new(
        postings_enum: BlockPostingsEnum,
        sim_scorer: Box<dyn SimScorer>,
        norms: Option<Box<[i64]>>,
        _score_mode: ScoreMode,
        top_level_scoring_clause: bool,
    ) -> Self {
        let max_score_cache = crate::search::scorer::MaxScoreCache::new(sim_scorer.as_ref());
        Self {
            postings_enum,
            sim_scorer,
            max_score_cache,
            norms,
            norm_values: Vec::new(),
            min_competitive_score: 0.0,
            up_to: NO_MORE_DOCS,
            max_score: f32::MAX,
            top_level_scoring_clause,
        }
    }

    /// Returns term frequency in the current document.
    pub fn freq(&mut self) -> io::Result<i32> {
        self.postings_enum.freq()
    }

    /// Look up the norm value for a document ID.
    fn norm_value(&self, doc_id: i32) -> i64 {
        match &self.norms {
            Some(norms) if (doc_id as usize) < norms.len() => norms[doc_id as usize],
            _ => 1,
        }
    }

    // -- ImpactsDISI logic inlined (from Java ImpactsDISI) --

    /// Compute the target to advance to, skipping non-competitive blocks.
    /// Matches Java's `ImpactsDISI.advanceTarget(int)`.
    fn advance_target(&mut self, target: i32) -> io::Result<i32> {
        if target <= self.up_to {
            // Still in the current block, considered competitive
            return Ok(target);
        }

        self.up_to = self
            .max_score_cache
            .advance_shallow(&mut self.postings_enum, target)?;
        self.max_score = self
            .max_score_cache
            .get_max_score_for_level_zero(&mut self.postings_enum, self.sim_scorer.as_ref())?;

        let mut target = target;
        loop {
            debug_assert!(self.up_to >= target);

            if self.max_score >= self.min_competitive_score {
                return Ok(target);
            }

            if self.up_to == NO_MORE_DOCS {
                return Ok(NO_MORE_DOCS);
            }

            let skip_up_to = self.max_score_cache.get_skip_up_to(
                &mut self.postings_enum,
                self.sim_scorer.as_ref(),
                self.min_competitive_score,
            )?;
            if skip_up_to == -1 {
                // no further skipping
                target = self.up_to + 1;
            } else if skip_up_to == NO_MORE_DOCS {
                return Ok(NO_MORE_DOCS);
            } else {
                target = skip_up_to + 1;
            }
            self.up_to = self
                .max_score_cache
                .advance_shallow(&mut self.postings_enum, target)?;
            self.max_score = self
                .max_score_cache
                .get_max_score_for_level_zero(&mut self.postings_enum, self.sim_scorer.as_ref())?;
        }
    }

    /// If the current doc is not competitive, advance to a competitive one.
    /// Matches Java's `ImpactsDISI.ensureCompetitive()`.
    fn ensure_competitive(&mut self) -> io::Result<()> {
        if !self.top_level_scoring_clause {
            return Ok(());
        }
        let doc = self.postings_enum.doc_id();
        let advance_target = self.advance_target(doc)?;
        if advance_target != doc {
            self.postings_enum.advance(advance_target)?;
        }
        Ok(())
    }
}

impl Scorable for TermScorer {
    fn score(&mut self) -> io::Result<f32> {
        let freq = self.postings_enum.freq()? as f32;
        let doc_id = self.postings_enum.doc_id();
        let norm = self.norm_value(doc_id);
        Ok(self.sim_scorer.score(freq, norm))
    }

    fn smoothing_score(&mut self, doc_id: i32) -> io::Result<f32> {
        let norm = self.norm_value(doc_id);
        Ok(self.sim_scorer.score(0.0, norm))
    }

    fn set_min_competitive_score(&mut self, min_score: f32) -> io::Result<()> {
        if self.top_level_scoring_clause {
            // Matches Java ImpactsDISI.setMinCompetitiveScore
            debug_assert!(min_score >= self.min_competitive_score);
            if min_score > self.min_competitive_score {
                self.min_competitive_score = min_score;
                // Force up_to and max_score to be recomputed
                self.up_to = -1;
            }
        }
        Ok(())
    }
}

impl Scorer for TermScorer {
    fn doc_id(&self) -> i32 {
        self.postings_enum.doc_id()
    }

    fn iterator(&mut self) -> &mut dyn DocIdSetIterator {
        // NOTE: When top_level_scoring_clause is true, Java returns ImpactsDISI here.
        // We inline the ImpactsDISI logic in next_docs_and_scores/ensureCompetitive
        // instead, so we always return the raw postings_enum. The competitive skipping
        // happens in ensureCompetitive() which is called from next_docs_and_scores().
        &mut self.postings_enum
    }

    fn advance_shallow(&mut self, target: i32) -> io::Result<i32> {
        self.max_score_cache
            .advance_shallow(&mut self.postings_enum, target)
    }

    fn get_max_score(&mut self, up_to: i32) -> io::Result<f32> {
        self.max_score_cache
            .get_max_score(&mut self.postings_enum, self.sim_scorer.as_ref(), up_to)
    }

    fn next_docs_and_scores(
        &mut self,
        up_to: i32,
        buffer: &mut DocAndFloatFeatureBuffer,
    ) -> io::Result<()> {
        // Matches Java TermScorer.nextDocsAndScores:
        // 1. ensureCompetitive (ImpactsDISI)
        // 2. fill docs + freqs
        // 3. fill norms
        // 4. bulk score

        // Note: Java loops here to retry if liveDocs filtering empties the buffer.
        // We skip liveDocs (Bits not ported), so a single pass suffices.
        self.ensure_competitive()?;

        // Fill buffer with docs and freqs
        let batch_size = 64;
        buffer.grow_no_copy(batch_size);
        let mut size = 0;
        let mut doc = self.postings_enum.doc_id();
        while doc < up_to && size < batch_size {
            buffer.docs[size] = doc;
            buffer.features[size] = self.postings_enum.freq()? as f32;
            size += 1;
            doc = self.postings_enum.next_doc()?;
        }
        buffer.size = size;

        // Fill norm values for the batch
        if self.norm_values.len() < size {
            self.norm_values.resize(size, 1);
            if self.norms.is_none() {
                self.norm_values.fill(1);
            }
        }
        if let Some(ref norms) = self.norms {
            for i in 0..size {
                let doc_id = buffer.docs[i] as usize;
                self.norm_values[i] = if doc_id < norms.len() {
                    norms[doc_id]
                } else {
                    1
                };
            }
        }

        // Bulk score: compute scores from freqs (in features) + norms.
        // Rust can't alias features as both input and output, so score in-place per element.
        // Java uses BulkSimScorer which allows the same array; Rust's borrowing rules prevent it.
        for i in 0..size {
            buffer.features[i] = self
                .sim_scorer
                .score(buffer.features[i], self.norm_values[i]);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{self, Document};
    use crate::index::directory_reader::DirectoryReader;
    use crate::index::{IndexWriter, IndexWriterConfig};
    use crate::search::doc_id_set_iterator::NO_MORE_DOCS;
    use crate::search::similarity::BM25Similarity;
    use crate::store::{Directory, MemoryDirectory};

    fn build_test_index() -> (Box<dyn crate::store::Directory>, DirectoryReader) {
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
        let dir = Box::new(mem_dir) as Box<dyn crate::store::Directory>;
        let reader = DirectoryReader::open(dir.as_ref()).unwrap();
        (dir, reader)
    }

    // -- TermQuery construction tests --

    #[test]
    fn test_term_query_new() {
        let q = TermQuery::new("content", b"hello");
        assert_eq!(q.field(), "content");
        assert_eq!(q.term(), b"hello");
    }

    #[test]
    fn test_term_query_create_weight() {
        let q = TermQuery::new("content", b"hello");
        let sim = BM25Similarity::default();
        let weight = q.create_weight(&sim, ScoreMode::Complete, 1.0);
        assert!(weight.is_ok());
    }

    // -- TermScorer basic scoring tests --

    #[test]
    fn test_term_scorer_iterates_matching_docs() {
        let (_dir, reader) = build_test_index();
        let q = TermQuery::new("content", b"hello");
        let sim = BM25Similarity::default();
        let weight = q.create_weight(&sim, ScoreMode::Complete, 1.0).unwrap();

        let leaf = &reader.leaves()[0];
        let supplier = weight.scorer_supplier(leaf).unwrap();
        assert!(supplier.is_some(), "scorer_supplier should find the term");

        let mut scorer = supplier.unwrap().get(i64::MAX).unwrap();
        let iter = scorer.iterator();

        // "hello" is in docs 0 and 1
        let doc0 = iter.next_doc().unwrap();
        assert_eq!(doc0, 0);
        let doc1 = iter.next_doc().unwrap();
        assert_eq!(doc1, 1);
        let end = iter.next_doc().unwrap();
        assert_eq!(end, NO_MORE_DOCS);
    }

    #[test]
    fn test_term_scorer_scores_are_positive() {
        let (_dir, reader) = build_test_index();
        let q = TermQuery::new("content", b"hello");
        let sim = BM25Similarity::default();
        let weight = q.create_weight(&sim, ScoreMode::Complete, 1.0).unwrap();

        let leaf = &reader.leaves()[0];
        let mut scorer = weight
            .scorer_supplier(leaf)
            .unwrap()
            .unwrap()
            .get(i64::MAX)
            .unwrap();

        scorer.iterator().next_doc().unwrap();
        let score0 = scorer.score().unwrap();
        assert!(score0 > 0.0, "score should be positive, got {score0}");

        scorer.iterator().next_doc().unwrap();
        let score1 = scorer.score().unwrap();
        assert!(score1 > 0.0, "score should be positive, got {score1}");
    }

    #[test]
    fn test_term_scorer_rare_term_scores_higher() {
        let (_dir, reader) = build_test_index();
        let sim = BM25Similarity::default();

        // "world" is in 2 of 3 docs
        let q_common = TermQuery::new("content", b"world");
        let weight_common = q_common
            .create_weight(&sim, ScoreMode::Complete, 1.0)
            .unwrap();
        let leaf = &reader.leaves()[0];
        let mut scorer_common = weight_common
            .scorer_supplier(leaf)
            .unwrap()
            .unwrap()
            .get(i64::MAX)
            .unwrap();
        scorer_common.iterator().next_doc().unwrap();
        let score_common = scorer_common.score().unwrap();

        // "peace" is in 1 of 3 docs — rarer term should have higher IDF
        let q_rare = TermQuery::new("content", b"peace");
        let weight_rare = q_rare
            .create_weight(&sim, ScoreMode::Complete, 1.0)
            .unwrap();
        let mut scorer_rare = weight_rare
            .scorer_supplier(leaf)
            .unwrap()
            .unwrap()
            .get(i64::MAX)
            .unwrap();
        scorer_rare.iterator().next_doc().unwrap();
        let score_rare = scorer_rare.score().unwrap();

        assert!(
            score_rare > score_common,
            "rare term should score higher: peace={score_rare} vs world={score_common}"
        );
    }

    #[test]
    fn test_term_scorer_nonexistent_term() {
        let (_dir, reader) = build_test_index();
        let q = TermQuery::new("content", b"nonexistent");
        let sim = BM25Similarity::default();
        let weight = q.create_weight(&sim, ScoreMode::Complete, 1.0).unwrap();

        let leaf = &reader.leaves()[0];
        let supplier = weight.scorer_supplier(leaf).unwrap();
        assert!(supplier.is_none(), "nonexistent term should return None");
    }

    #[test]
    fn test_term_scorer_nonexistent_field() {
        let (_dir, reader) = build_test_index();
        let q = TermQuery::new("no_such_field", b"hello");
        let sim = BM25Similarity::default();
        let weight = q.create_weight(&sim, ScoreMode::Complete, 1.0).unwrap();

        let leaf = &reader.leaves()[0];
        let supplier = weight.scorer_supplier(leaf).unwrap();
        assert!(supplier.is_none(), "nonexistent field should return None");
    }

    #[test]
    fn test_term_scorer_boost_scales_score() {
        let (_dir, reader) = build_test_index();
        let sim = BM25Similarity::default();
        let leaf = &reader.leaves()[0];

        let q1 = TermQuery::new("content", b"hello");
        let weight1 = q1.create_weight(&sim, ScoreMode::Complete, 1.0).unwrap();
        let mut scorer1 = weight1
            .scorer_supplier(leaf)
            .unwrap()
            .unwrap()
            .get(i64::MAX)
            .unwrap();
        scorer1.iterator().next_doc().unwrap();
        let score1 = scorer1.score().unwrap();

        let q2 = TermQuery::new("content", b"hello");
        let weight2 = q2.create_weight(&sim, ScoreMode::Complete, 2.0).unwrap();
        let mut scorer2 = weight2
            .scorer_supplier(leaf)
            .unwrap()
            .unwrap()
            .get(i64::MAX)
            .unwrap();
        scorer2.iterator().next_doc().unwrap();
        let score2 = scorer2.score().unwrap();

        assert!(
            (score2 - score1 * 2.0).abs() < 0.01,
            "2x boost should ~double the score: {score1} vs {score2}"
        );
    }

    // -- BatchScoreBulkScorer tests --

    #[test]
    fn test_batch_score_bulk_scorer_collects_docs() {
        let (_dir, reader) = build_test_index();
        let q = TermQuery::new("content", b"hello");
        let sim = BM25Similarity::default();
        let weight = q.create_weight(&sim, ScoreMode::Complete, 1.0).unwrap();

        let leaf = &reader.leaves()[0];
        let scorer = weight
            .scorer_supplier(leaf)
            .unwrap()
            .unwrap()
            .get(i64::MAX)
            .unwrap();

        let mut bulk = BatchScoreBulkScorer::new(scorer);

        struct CollectedDoc {
            docs: Vec<i32>,
        }
        impl crate::search::collector::LeafCollector for CollectedDoc {
            fn set_scorer(
                &mut self,
                _score_context: std::rc::Rc<crate::search::collector::ScoreContext>,
            ) -> io::Result<()> {
                Ok(())
            }
            fn collect(&mut self, doc: i32) -> io::Result<()> {
                self.docs.push(doc);
                Ok(())
            }
        }

        let mut collector = CollectedDoc { docs: Vec::new() };
        bulk.score(&mut collector, 0, NO_MORE_DOCS).unwrap();

        // "hello" is in docs 0 and 1
        assert_eq!(collector.docs, vec![0, 1]);
    }

    #[test]
    fn test_batch_score_bulk_scorer_respects_range() {
        let (_dir, reader) = build_test_index();
        let q = TermQuery::new("content", b"world");
        let sim = BM25Similarity::default();
        let weight = q.create_weight(&sim, ScoreMode::Complete, 1.0).unwrap();

        let leaf = &reader.leaves()[0];
        let scorer = weight
            .scorer_supplier(leaf)
            .unwrap()
            .unwrap()
            .get(i64::MAX)
            .unwrap();

        let mut bulk = BatchScoreBulkScorer::new(scorer);

        struct CollectedDoc {
            docs: Vec<i32>,
        }
        impl crate::search::collector::LeafCollector for CollectedDoc {
            fn set_scorer(
                &mut self,
                _score_context: std::rc::Rc<crate::search::collector::ScoreContext>,
            ) -> io::Result<()> {
                Ok(())
            }
            fn collect(&mut self, doc: i32) -> io::Result<()> {
                self.docs.push(doc);
                Ok(())
            }
        }

        let mut collector = CollectedDoc { docs: Vec::new() };
        // "world" is in docs 0 and 2. Score only [1, 3) should get doc 2.
        bulk.score(&mut collector, 1, 3).unwrap();

        assert_eq!(collector.docs, vec![2]);
    }
}
