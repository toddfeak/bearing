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
// SHOULD queries — pure disjunction
// Cross-validated against Java Lucene 10.3.2. Java index built with sorted
// filenames, StandardAnalyzer(EMPTY_SET), text_field("contents").
// Java queries via QueryParser default operator (OR = SHOULD).
// -------------------------------------------------------------------------

/// Helper to build a two-SHOULD boolean query.
fn should_should_query(field: &str, term1: &[u8], term2: &[u8]) -> BooleanQuery {
    let mut builder = BooleanQuery::builder();
    builder.add_query(Box::new(TermQuery::new(field, term1)), Occur::Should);
    builder.add_query(Box::new(TermQuery::new(field, term2)), Occur::Should);
    builder.build()
}

// -------------------------------------------------------------------------
// SHOULD Query 1: algorithms data → 10 hits
// Java results:
//   doc=11  score=0.9744643
//   doc=0   score=0.8020670
//   doc=14  score=0.8000477
//   doc=3   score=0.6710463
//   doc=5   score=0.2621497
//   doc=1   score=0.2506270
//   doc=10  score=0.2121821
//   doc=12  score=0.1924969
//   doc=6   score=0.1707188
//   doc=9   score=0.1577401
// -------------------------------------------------------------------------

#[test]
fn test_boolean_should_algorithms_data() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = should_should_query("contents", b"algorithms", b"data");

    let top_docs = searcher.search(&query, 15).unwrap();

    assert_eq!(top_docs.total_hits.value, 10);
    assert_eq!(top_docs.score_docs.len(), 10);

    let expected = [
        (11, 0.9744643_f32),
        (0, 0.802067),
        (14, 0.8000477),
        (3, 0.6710463),
        (5, 0.2621497),
        (1, 0.250627),
        (10, 0.2121821),
        (12, 0.1924969),
        (6, 0.1707188),
        (9, 0.1577401),
    ];
    for (i, &(doc, score)) in expected.iter().enumerate() {
        assert_eq!(top_docs.score_docs[i].doc, doc);
        assert_in_delta!(top_docs.score_docs[i].score, score, 1e-5);
    }
}

// -------------------------------------------------------------------------
// SHOULD Query 2: distributed systems → 12 hits
// Java results:
//   doc=13  score=0.6466637
//   doc=2   score=0.5788049
//   doc=7   score=0.5369343
//   doc=8   score=0.5369343
//   doc=6   score=0.5074845
//   doc=0   score=0.4957356
//   doc=12  score=0.1548606
//   doc=3   score=0.1403393
//   doc=4   score=0.1385187
//   doc=9   score=0.1345176
//   doc=10  score=0.1243533
//   doc=11  score=0.0942376
// -------------------------------------------------------------------------

#[test]
fn test_boolean_should_distributed_systems() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = should_should_query("contents", b"distributed", b"systems");

    let top_docs = searcher.search(&query, 15).unwrap();

    assert_eq!(top_docs.total_hits.value, 12);
    assert_eq!(top_docs.score_docs.len(), 12);

    let expected = [
        (13, 0.6466637_f32),
        (2, 0.5788049),
        (7, 0.5369343),
        (8, 0.5369343),
        (6, 0.5074845),
        (0, 0.4957356),
        (12, 0.1548606),
        (3, 0.1403393),
        (4, 0.1385187),
        (9, 0.1345176),
        (10, 0.1243533),
        (11, 0.0942376),
    ];
    for (i, &(doc, score)) in expected.iter().enumerate() {
        assert_eq!(top_docs.score_docs[i].doc, doc);
        assert_in_delta!(top_docs.score_docs[i].score, score, 1e-5);
    }
}

// -------------------------------------------------------------------------
// SHOULD Query 3: memory performance → 7 hits
// Java results:
//   doc=0   score=1.1177642
//   doc=8   score=0.7110608
//   doc=1   score=0.6353776
//   doc=9   score=0.5691590
//   doc=14  score=0.5055991
//   doc=2   score=0.4995965
//   doc=12  score=0.4880089
// -------------------------------------------------------------------------

#[test]
fn test_boolean_should_memory_performance() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = should_should_query("contents", b"memory", b"performance");

    let top_docs = searcher.search(&query, 15).unwrap();

    assert_eq!(top_docs.total_hits.value, 7);
    assert_eq!(top_docs.score_docs.len(), 7);

    let expected = [
        (0, 1.1177642_f32),
        (8, 0.7110608),
        (1, 0.6353776),
        (9, 0.569159),
        (14, 0.5055991),
        (2, 0.4995965),
        (12, 0.4880089),
    ];
    for (i, &(doc, score)) in expected.iter().enumerate() {
        assert_eq!(top_docs.score_docs[i].doc, doc);
        assert_in_delta!(top_docs.score_docs[i].score, score, 1e-5);
    }
}

// -------------------------------------------------------------------------
// MUST_NOT queries — exclusion
// Cross-validated against Java Lucene 10.3.2. Same index setup as SHOULD tests.
// Java queries via QueryParser: `+word1 -word2` and `word1 word2 -word3`.
// -------------------------------------------------------------------------

/// Helper to build a MUST + single MUST_NOT boolean query.
fn must_must_not_query(field: &str, must_term: &[u8], not_term: &[u8]) -> BooleanQuery {
    let mut builder = BooleanQuery::builder();
    builder.add_query(Box::new(TermQuery::new(field, must_term)), Occur::Must);
    builder.add_query(Box::new(TermQuery::new(field, not_term)), Occur::MustNot);
    builder.build()
}

// -------------------------------------------------------------------------
// MUST_NOT Query 1: +distributed -security → 6 hits
// Java results:
//   doc=13  score=0.4596379
//   doc=2   score=0.4214391
//   doc=7   score=0.4214391
//   doc=8   score=0.4214391
//   doc=0   score=0.3891023
//   doc=6   score=0.3650910
// -------------------------------------------------------------------------

#[test]
fn test_boolean_must_not_distributed_security() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = must_must_not_query("contents", b"distributed", b"security");

    let top_docs = searcher.search(&query, 15).unwrap();

    assert_eq!(top_docs.total_hits.value, 6);
    assert_eq!(top_docs.score_docs.len(), 6);

    let expected = [
        (13, 0.4596379_f32),
        (2, 0.4214391),
        (7, 0.4214391),
        (8, 0.4214391),
        (0, 0.3891023),
        (6, 0.365091),
    ];
    for (i, &(doc, score)) in expected.iter().enumerate() {
        assert_eq!(top_docs.score_docs[i].doc, doc);
        assert_in_delta!(top_docs.score_docs[i].score, score, 1e-5);
    }
}

// -------------------------------------------------------------------------
// MUST_NOT Query 2: +memory -quantum → 2 hits
// Java results:
//   doc=8   score=0.7110608
//   doc=0   score=0.6565015
// -------------------------------------------------------------------------

#[test]
fn test_boolean_must_not_memory_quantum() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = must_must_not_query("contents", b"memory", b"quantum");

    let top_docs = searcher.search(&query, 15).unwrap();

    assert_eq!(top_docs.total_hits.value, 2);
    assert_eq!(top_docs.score_docs.len(), 2);

    let expected = [(8, 0.7110608_f32), (0, 0.6565015)];
    for (i, &(doc, score)) in expected.iter().enumerate() {
        assert_eq!(top_docs.score_docs[i].doc, doc);
        assert_in_delta!(top_docs.score_docs[i].score, score, 1e-5);
    }
}

// -------------------------------------------------------------------------
// MUST_NOT Query 3: distributed -security → 6 hits (SHOULD + single MUST_NOT)
// Java results:
//   doc=13  score=0.4596379
//   doc=2   score=0.4214391
//   doc=7   score=0.4214391
//   doc=8   score=0.4214391
//   doc=0   score=0.3891023
//   doc=6   score=0.3650910
// -------------------------------------------------------------------------

#[test]
fn test_boolean_should_must_not_distributed_security() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);

    let mut builder = BooleanQuery::builder();
    builder.add_query(
        Box::new(TermQuery::new("contents", b"distributed")),
        Occur::Should,
    );
    builder.add_query(
        Box::new(TermQuery::new("contents", b"security")),
        Occur::MustNot,
    );
    let query = builder.build();

    let top_docs = searcher.search(&query, 15).unwrap();

    assert_eq!(top_docs.total_hits.value, 6);
    assert_eq!(top_docs.score_docs.len(), 6);

    let expected = [
        (13, 0.4596379_f32),
        (2, 0.4214391),
        (7, 0.4214391),
        (8, 0.4214391),
        (0, 0.3891023),
        (6, 0.365091),
    ];
    for (i, &(doc, score)) in expected.iter().enumerate() {
        assert_eq!(top_docs.score_docs[i].doc, doc);
        assert_in_delta!(top_docs.score_docs[i].score, score, 1e-5);
    }
}

// -------------------------------------------------------------------------
// MUST_NOT Query 4: memory -quantum → 2 hits (SHOULD + single MUST_NOT)
// Java results:
//   doc=8   score=0.7110608
//   doc=0   score=0.6565015
// -------------------------------------------------------------------------

#[test]
fn test_boolean_should_must_not_memory_quantum() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);

    let mut builder = BooleanQuery::builder();
    builder.add_query(
        Box::new(TermQuery::new("contents", b"memory")),
        Occur::Should,
    );
    builder.add_query(
        Box::new(TermQuery::new("contents", b"quantum")),
        Occur::MustNot,
    );
    let query = builder.build();

    let top_docs = searcher.search(&query, 15).unwrap();

    assert_eq!(top_docs.total_hits.value, 2);
    assert_eq!(top_docs.score_docs.len(), 2);

    let expected = [(8, 0.7110608_f32), (0, 0.6565015)];
    for (i, &(doc, score)) in expected.iter().enumerate() {
        assert_eq!(top_docs.score_docs[i].doc, doc);
        assert_in_delta!(top_docs.score_docs[i].score, score, 1e-5);
    }
}

// -------------------------------------------------------------------------
// Mixed MUST + SHOULD queries
// Cross-validated against Java Lucene 10.3.2. Same index setup.
// Java queries via QueryParser: `+word1 word2` (MUST + SHOULD).
// -------------------------------------------------------------------------

/// Helper to build a MUST + SHOULD boolean query.
fn must_should_query(field: &str, must_term: &[u8], should_term: &[u8]) -> BooleanQuery {
    let mut builder = BooleanQuery::builder();
    builder.add_query(Box::new(TermQuery::new(field, must_term)), Occur::Must);
    builder.add_query(Box::new(TermQuery::new(field, should_term)), Occur::Should);
    builder.build()
}

// -------------------------------------------------------------------------
// Mixed Query 1: +algorithms data → 4 hits (only docs with "algorithms")
// Scores: docs that also contain "data" get boosted
// Java results:
//   doc=11  score=0.9744643
//   doc=0   score=0.8020670
//   doc=14  score=0.8000477
//   doc=3   score=0.6710463
// -------------------------------------------------------------------------

#[test]
fn test_boolean_mixed_algorithms_data() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = must_should_query("contents", b"algorithms", b"data");

    let top_docs = searcher.search(&query, 15).unwrap();

    assert_eq!(top_docs.total_hits.value, 4);
    assert_eq!(top_docs.score_docs.len(), 4);

    let expected = [
        (11, 0.9744643_f32),
        (0, 0.802067),
        (14, 0.8000477),
        (3, 0.6710463),
    ];
    for (i, &(doc, score)) in expected.iter().enumerate() {
        assert_eq!(top_docs.score_docs[i].doc, doc);
        assert_in_delta!(top_docs.score_docs[i].score, score, 1e-5);
    }
}

// -------------------------------------------------------------------------
// Mixed Query 2: +distributed systems → 6 hits
// Java results:
//   doc=13  score=0.6466637
//   doc=2   score=0.5788049
//   doc=7   score=0.5369343
//   doc=8   score=0.5369343
//   doc=6   score=0.5074845
//   doc=0   score=0.4957356
// -------------------------------------------------------------------------

#[test]
fn test_boolean_mixed_distributed_systems() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = must_should_query("contents", b"distributed", b"systems");

    let top_docs = searcher.search(&query, 15).unwrap();

    assert_eq!(top_docs.total_hits.value, 6);
    assert_eq!(top_docs.score_docs.len(), 6);

    let expected = [
        (13, 0.6466637_f32),
        (2, 0.5788049),
        (7, 0.5369343),
        (8, 0.5369343),
        (6, 0.5074845),
        (0, 0.4957356),
    ];
    for (i, &(doc, score)) in expected.iter().enumerate() {
        assert_eq!(top_docs.score_docs[i].doc, doc);
        assert_in_delta!(top_docs.score_docs[i].score, score, 1e-5);
    }
}

// -------------------------------------------------------------------------
// Mixed Query 3: +memory performance → 3 hits
// Java results:
//   doc=0   score=1.1177642
//   doc=8   score=0.7110608
//   doc=9   score=0.5691590
// -------------------------------------------------------------------------

#[test]
fn test_boolean_mixed_memory_performance() {
    let (_dir, reader) = build_golden_docs_index();
    let searcher = IndexSearcher::new(&reader);
    let query = must_should_query("contents", b"memory", b"performance");

    let top_docs = searcher.search(&query, 15).unwrap();

    assert_eq!(top_docs.total_hits.value, 3);
    assert_eq!(top_docs.score_docs.len(), 3);

    let expected = [(0, 1.1177642_f32), (8, 0.7110608), (9, 0.569159)];
    for (i, &(doc, score)) in expected.iter().enumerate() {
        assert_eq!(top_docs.score_docs[i].doc, doc);
        assert_in_delta!(top_docs.score_docs[i].score, score, 1e-5);
    }
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
