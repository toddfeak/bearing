// Query a Bearing-readable index with a list of query strings and report per-query timing.
//
// Each line in the queries file is a query in Lucene standard syntax:
//   - bare word:         algorithms         (TermQuery)
//   - boolean MUST:      +algorithms +data  (BooleanQuery with MUST clauses)
//   - boolean SHOULD:    algorithms data    (BooleanQuery with SHOULD clauses)
//   - boolean MUST_NOT:  +algorithms -data  (BooleanQuery with MUST and MUST_NOT clauses)
//
// Usage:
//   cargo run --release --bin queryindex -- -index <DIR> -queries <FILE> [-output <FILE>]

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::Write;
use std::panic;
use std::path::Path;
use std::process;
use std::time::Instant;

use bearing::index::directory_reader::DirectoryReader;
use bearing::search::index_searcher::IndexSearcher;
use bearing::search::query::Query;
use bearing::search::term_query::TermQuery;
use bearing::search::top_score_doc_collector::TopScoreDocCollectorManager;
use bearing::search::{BooleanQuery, Occur};
use bearing::store::FSDirectory;

/// Parses a query string in Lucene standard syntax into a Query.
///
/// Supported formats:
///   - `word`              → TermQuery on the given field
///   - `+word1 +word2`     → BooleanQuery with MUST clauses
///   - `+word1 -word2`     → BooleanQuery with MUST and MUST_NOT clauses
///   - `word1 word2`       → BooleanQuery with SHOULD clauses
///   - `word1 word2 -word3` → BooleanQuery with SHOULD and MUST_NOT clauses
fn parse_query(query_str: &str, field: &str) -> Box<dyn Query> {
    let tokens: Vec<&str> = query_str.split_whitespace().collect();

    let has_plus = tokens.iter().any(|t| t.starts_with('+'));
    let has_minus = tokens.iter().any(|t| t.starts_with('-'));

    if has_plus || has_minus {
        let mut builder = BooleanQuery::builder();
        for token in &tokens {
            if let Some(term) = token.strip_prefix('+') {
                builder.add_query(
                    Box::new(TermQuery::new(field, term.as_bytes())),
                    Occur::Must,
                );
            } else if let Some(term) = token.strip_prefix('-') {
                builder.add_query(
                    Box::new(TermQuery::new(field, term.as_bytes())),
                    Occur::MustNot,
                );
            } else {
                // Bare word in a mixed query → SHOULD (matches QueryParser behavior)
                builder.add_query(
                    Box::new(TermQuery::new(field, token.as_bytes())),
                    Occur::Should,
                );
            }
        }
        Box::new(builder.build())
    } else if tokens.len() > 1 {
        let mut builder = BooleanQuery::builder();
        for token in &tokens {
            builder.add_query(
                Box::new(TermQuery::new(field, token.as_bytes())),
                Occur::Should,
            );
        }
        Box::new(builder.build())
    } else {
        Box::new(TermQuery::new(field, tokens[0].as_bytes()))
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut index_path = String::new();
    let mut queries_path = String::new();
    let mut output_path = String::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-index" => {
                i += 1;
                index_path = args[i].clone();
            }
            "-queries" => {
                i += 1;
                queries_path = args[i].clone();
            }
            "-output" => {
                i += 1;
                output_path = args[i].clone();
            }
            other => {
                eprintln!("Unknown argument: {other}");
                process::exit(1);
            }
        }
        i += 1;
    }

    if index_path.is_empty() || queries_path.is_empty() {
        eprintln!(
            "Usage: queryindex -index <INDEX_DIR> -queries <QUERIES_FILE> [-output <RESULTS_FILE>]"
        );
        process::exit(1);
    }

    let queries_content = fs::read_to_string(&queries_path).expect("Failed to read queries file");
    let queries: Vec<&str> = queries_content
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();

    let dir = FSDirectory::open(Path::new(&index_path)).expect("Failed to open index directory");
    let reader = DirectoryReader::open(&dir).expect("Failed to open DirectoryReader");
    let searcher = IndexSearcher::new(&reader);

    // Collect results in memory — no I/O during timed section
    let mut results: Vec<String> = Vec::with_capacity(queries.len());
    let mut errors = 0;

    let start = Instant::now();

    for query_str in &queries {
        let query = parse_query(query_str, "contents");
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let manager = TopScoreDocCollectorManager::new(10, None, i32::MAX);
            searcher.search_with_collector_manager(query.as_ref(), &manager)
        }));
        match result {
            Ok(Ok(top_docs)) => {
                let mut line = String::new();
                write!(
                    line,
                    "{:<30} hits={:<6}",
                    query_str, top_docs.total_hits.value
                )
                .unwrap();
                for sd in &top_docs.score_docs {
                    write!(line, "  doc={:<5} score={:.4}", sd.doc, sd.score).unwrap();
                }
                results.push(line);
            }
            Ok(Err(e)) => {
                results.push(format!("{:<30} ERROR: {}", query_str, e));
                errors += 1;
            }
            Err(_) => {
                results.push(format!("{:<30} PANIC", query_str));
                errors += 1;
            }
        }
    }

    let elapsed = start.elapsed();

    // Write results to file or stdout
    if !output_path.is_empty() {
        let mut f = fs::File::create(&output_path).expect("Failed to create output file");
        for line in &results {
            writeln!(f, "{line}").unwrap();
        }
    } else {
        for line in &results {
            println!("{line}");
        }
    }

    // Timing always goes to stdout
    println!(
        "Queried {} queries in {elapsed:.2?} ({errors} errors)",
        queries.len()
    );
    println!(
        "Average: {:.2} µs/query",
        elapsed.as_micros() as f64 / queries.len() as f64
    );
}
