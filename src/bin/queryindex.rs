// Query a Bearing-readable index with a list of words and report per-query timing.
//
// Usage:
//   cargo run --release --bin queryindex -- -index <DIR> -words <FILE> [-output <FILE>]

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::Write;
// use std::panic;
use std::path::Path;
use std::process;
use std::time::Instant;

use bearing::index::directory_reader::DirectoryReader;
// use bearing::search::index_searcher::IndexSearcher;
// use bearing::search::term_query::TermQuery;
use bearing::store::FSDirectory;

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut index_path = String::new();
    let mut words_path = String::new();
    let mut output_path = String::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-index" => {
                i += 1;
                index_path = args[i].clone();
            }
            "-words" => {
                i += 1;
                words_path = args[i].clone();
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

    if index_path.is_empty() || words_path.is_empty() {
        eprintln!(
            "Usage: queryindex -index <INDEX_DIR> -words <WORDS_FILE> [-output <RESULTS_FILE>]"
        );
        process::exit(1);
    }

    let words_content = fs::read_to_string(&words_path).expect("Failed to read words file");
    let words: Vec<&str> = words_content
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();

    let dir = FSDirectory::open(Path::new(&index_path)).expect("Failed to open index directory");
    let _reader = DirectoryReader::open(&dir).expect("Failed to open DirectoryReader");
    // let searcher = IndexSearcher::new(reader);

    // Collect results in memory — no I/O during timed section
    let mut results: Vec<String> = Vec::with_capacity(words.len());
    let errors = 0;

    let start = Instant::now();

    for word in &words {
        // TODO: restore once search framework is rebuilt
        // let query = TermQuery::new("contents", word.as_bytes());
        // let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        //     searcher.search_top_docs(&query, 10)
        // }));
        // match result {
        //     Ok(Ok(top_docs)) => {
        //         let mut line = String::new();
        //         write!(line, "{:<20} hits={:<6}", word, top_docs.total_hits.value).unwrap();
        //         for sd in &top_docs.score_docs {
        //             write!(line, "  doc={:<5} score={:.4}", sd.doc, sd.score).unwrap();
        //         }
        //         results.push(line);
        //     }
        //     Ok(Err(e)) => {
        //         results.push(format!("{:<20} ERROR: {}", word, e));
        //         errors += 1;
        //     }
        //     Err(_) => {
        //         results.push(format!("{:<20} PANIC", word));
        //         errors += 1;
        //     }
        // }
        let mut line = String::new();
        write!(line, "{:<20} TODO", word).unwrap();
        results.push(line);
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
        "Queried {} words in {elapsed:.2?} ({errors} errors)",
        words.len()
    );
    println!(
        "Average: {:.2} µs/query",
        elapsed.as_micros() as f64 / words.len() as f64
    );
}
