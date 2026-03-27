// SPDX-License-Identifier: Apache-2.0

//! Integration tests for BooleanQuery, cross-validated against Java Lucene 10.3.2.
//!
//! Uses the golden-docs corpus (15 documents from `testdata/golden-docs/`), indexed with
//! field name "contents" using `text_field` (DOCS_AND_FREQS_AND_POSITIONS, tokenized,
//! no term vectors). Files are indexed in sorted filename order for deterministic doc IDs.
//!
//! Expected results were obtained by indexing the same documents in the same order with
//! Java Lucene 10.3.2 (StandardAnalyzer, BM25Similarity default k1=1.2 b=0.75) and
//! running the same BooleanQuery with two MUST TermQuery clauses.

use assertables::*;
use bearing::document::{self, Document};
use bearing::index::directory_reader::DirectoryReader;
use bearing::index::{IndexWriter, IndexWriterConfig};
use bearing::search::*;
use bearing::store::{Directory, MemoryDirectory};

/// Indexes golden-docs into an in-memory directory, returning the directory and reader.
/// Files are indexed in sorted filename order to match the Java indexing order.
fn build_golden_docs_index() -> (Box<dyn Directory>, DirectoryReader) {
    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    let docs_dir = std::path::Path::new("testdata/golden-docs");
    let mut paths: Vec<_> = std::fs::read_dir(docs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "txt"))
        .map(|e| e.path())
        .collect();
    paths.sort();

    for path in &paths {
        let contents = std::fs::read_to_string(path).unwrap();
        let mut doc = Document::new();
        doc.add(document::text_field("contents", &contents));
        writer.add_document(doc).unwrap();
    }

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

/// Helper to build a two-MUST boolean query.
fn must_must_query(field: &str, term1: &[u8], term2: &[u8]) -> BooleanQuery {
    let mut builder = BooleanQuery::builder();
    builder.add_query(Box::new(TermQuery::new(field, term1)), Occur::Must);
    builder.add_query(Box::new(TermQuery::new(field, term2)), Occur::Must);
    builder.build()
}

// -------------------------------------------------------------------------
// Query 1: +algorithms +data → 4 hits
// Java results:
//   doc=11 score=0.9744643
//   doc=0  score=0.8020670
//   doc=14 score=0.8000477
//   doc=3  score=0.6710463
// -------------------------------------------------------------------------

#[test]
fn test_boolean_must_algorithms_data() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = must_must_query("contents", b"algorithms", b"data");

    let top_docs = searcher.search(&query, 10).unwrap();

    assert_eq!(top_docs.total_hits.value, 4);
    assert_eq!(top_docs.score_docs.len(), 4);

    assert_eq!(top_docs.score_docs[0].doc, 11);
    assert_in_delta!(top_docs.score_docs[0].score, 0.9744643_f32, 1e-5);

    assert_eq!(top_docs.score_docs[1].doc, 0);
    assert_in_delta!(top_docs.score_docs[1].score, 0.802067_f32, 1e-5);

    assert_eq!(top_docs.score_docs[2].doc, 14);
    assert_in_delta!(top_docs.score_docs[2].score, 0.8000477_f32, 1e-5);

    assert_eq!(top_docs.score_docs[3].doc, 3);
    assert_in_delta!(top_docs.score_docs[3].score, 0.6710463_f32, 1e-5);
}

// -------------------------------------------------------------------------
// Query 2: +distributed +systems → 6 hits
// Java results:
//   doc=13 score=0.6466637
//   doc=2  score=0.5788049
//   doc=7  score=0.5369343
//   doc=8  score=0.5369343
//   doc=6  score=0.5074845
//   doc=0  score=0.4957356
// -------------------------------------------------------------------------

#[test]
fn test_boolean_must_distributed_systems() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = must_must_query("contents", b"distributed", b"systems");

    let top_docs = searcher.search(&query, 10).unwrap();

    assert_eq!(top_docs.total_hits.value, 6);
    assert_eq!(top_docs.score_docs.len(), 6);

    assert_eq!(top_docs.score_docs[0].doc, 13);
    assert_in_delta!(top_docs.score_docs[0].score, 0.6466637_f32, 1e-5);

    assert_eq!(top_docs.score_docs[1].doc, 2);
    assert_in_delta!(top_docs.score_docs[1].score, 0.5788049_f32, 1e-5);

    assert_eq!(top_docs.score_docs[2].doc, 7);
    assert_in_delta!(top_docs.score_docs[2].score, 0.5369343_f32, 1e-5);

    assert_eq!(top_docs.score_docs[3].doc, 8);
    assert_in_delta!(top_docs.score_docs[3].score, 0.5369343_f32, 1e-5);

    assert_eq!(top_docs.score_docs[4].doc, 6);
    assert_in_delta!(top_docs.score_docs[4].score, 0.5074845_f32, 1e-5);

    assert_eq!(top_docs.score_docs[5].doc, 0);
    assert_in_delta!(top_docs.score_docs[5].score, 0.4957356_f32, 1e-5);
}

// -------------------------------------------------------------------------
// Query 3: +memory +performance → 1 hit
// Java results:
//   doc=0 score=1.1177642
// -------------------------------------------------------------------------

#[test]
fn test_boolean_must_memory_performance() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = must_must_query("contents", b"memory", b"performance");

    let top_docs = searcher.search(&query, 10).unwrap();

    assert_eq!(top_docs.total_hits.value, 1);
    assert_eq!(top_docs.score_docs.len(), 1);

    assert_eq!(top_docs.score_docs[0].doc, 0);
    assert_in_delta!(top_docs.score_docs[0].score, 1.1177642_f32, 1e-5);
}

// -------------------------------------------------------------------------
// Query 4: +quantum +computing → 1 hit
// Java results:
//   doc=9 score=1.7729266
// -------------------------------------------------------------------------

#[test]
fn test_boolean_must_quantum_computing() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = must_must_query("contents", b"quantum", b"computing");

    let top_docs = searcher.search(&query, 10).unwrap();

    assert_eq!(top_docs.total_hits.value, 1);
    assert_eq!(top_docs.score_docs.len(), 1);

    assert_eq!(top_docs.score_docs[0].doc, 9);
    assert_in_delta!(top_docs.score_docs[0].score, 1.7729266_f32, 1e-5);
}

// -------------------------------------------------------------------------
// Query 5: +nonexistent1 +nonexistent2 → 0 hits
// -------------------------------------------------------------------------

#[test]
fn test_boolean_must_nonexistent_terms() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = must_must_query("contents", b"nonexistent1", b"nonexistent2");

    let top_docs = searcher.search(&query, 10).unwrap();

    assert_eq!(top_docs.total_hits.value, 0);
    assert_is_empty!(top_docs.score_docs);
}

// -------------------------------------------------------------------------
// Conjunction scoring pruning: verify that BooleanQuery conjunctions interact
// correctly with the totalHits threshold (TOTAL_HITS_THRESHOLD = 1000).
//
// Java's BooleanScorerSupplier uses BlockMaxConjunctionBulkScorer for
// TOP_SCORES mode conjunctions with 2+ scoring clauses. This scorer prunes
// non-competitive docs via setMinCompetitiveScore, which means fewer docs
// reach the collector and totalHits is a lower bound (< actual match count).
//
// For TermQuery, BatchScoreBulkScorer already does this pruning correctly.
// This test verifies that BooleanQuery conjunctions prune the same way.
// -------------------------------------------------------------------------

/// Builds an in-memory index with `doc_count` documents, each containing
/// the given terms in a "content" text field.
fn build_large_index(doc_count: usize, terms: &[&str]) -> (Box<dyn Directory>, DirectoryReader) {
    let config = IndexWriterConfig::new().set_use_compound_file(false);
    let writer = IndexWriter::with_config(config);

    for i in 0..doc_count {
        let mut doc = Document::new();
        // Each doc gets the common terms plus filler words to vary document length,
        // which produces varying BM25 scores across docs.
        let filler_count = (i % 20) + 1;
        let filler: String = (0..filler_count)
            .map(|j| format!("word{}", j))
            .collect::<Vec<_>>()
            .join(" ");
        let text = format!("{} {}", terms.join(" "), filler);
        doc.add(document::text_field("content", &text));
        writer.add_document(doc).unwrap();
    }

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
fn test_boolean_conjunction_pruning_matches_term_query() {
    let doc_count = 1500;
    let (_dir, reader) = build_large_index(doc_count, &["alpha", "beta"]);
    let searcher = IndexSearcher::new(&reader);

    // TermQuery with default threshold (1000): should prune via BatchScoreBulkScorer,
    // giving totalHits < doc_count.
    let term_query = TermQuery::new("content", b"alpha");
    let term_result = searcher.search(&term_query, 10).unwrap();
    assert_lt!(
        term_result.total_hits.value,
        doc_count as i64,
        "TermQuery should prune non-competitive docs when totalHits > threshold"
    );

    // BooleanQuery +alpha +beta with default threshold: should also prune via
    // BlockMaxConjunctionBulkScorer, giving totalHits < doc_count.
    // Currently FAILS because Rust uses DefaultBulkScorer which counts every doc.
    let bool_query = must_must_query("content", b"alpha", b"beta");
    let bool_result = searcher.search(&bool_query, 10).unwrap();
    assert_lt!(
        bool_result.total_hits.value,
        doc_count as i64,
        "BooleanQuery conjunction should prune non-competitive docs like TermQuery does"
    );
}
