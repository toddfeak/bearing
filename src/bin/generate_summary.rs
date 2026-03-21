// SPDX-License-Identifier: Apache-2.0

//! Generates a JSON summary of an index's structure and statistics.
//!
//! Produces the same format as the Java `GenerateIndexSummary` tool for
//! golden-file comparison. `dvDocCount` outputs `0` as a placeholder
//! until a doc values metadata reader is implemented.
//!
//! Usage: `generate_summary -index <path>`

use std::env;
use std::path::Path;
use std::process;

use serde::Serialize;

use bearing::codecs::lucene90::compound_reader::CompoundDirectory;
use bearing::codecs::lucene94::field_infos_format;
use bearing::codecs::lucene99::segment_info_format;
use bearing::codecs::lucene103::blocktree_reader::BlockTreeTermsReader;
use bearing::document::{DocValuesType, IndexOptions};
use bearing::index::{FieldInfo, segment_infos};
use bearing::store::{Directory, FSDirectory};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Summary {
    total_docs: i32,
    max_doc: i32,
    segments: Vec<SegmentSummary>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SegmentSummary {
    index: usize,
    max_doc: i32,
    num_docs: i32,
    fields: Vec<FieldSummary>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FieldSummary {
    name: String,
    number: u32,
    index_options: String,
    has_norms: bool,
    store_term_vector: bool,
    has_payloads: bool,
    doc_values_type: String,
    point_dimension_count: u32,
    point_index_dimension_count: u32,
    point_num_bytes: u32,
    term_count: i64,
    dv_doc_count: i64,
}

fn main() {
    let index_path = parse_args();

    let dir = FSDirectory::open(Path::new(&index_path)).unwrap_or_else(|e| {
        eprintln!("Failed to open index directory '{index_path}': {e}");
        process::exit(1);
    });

    let files = dir.list_all().unwrap_or_else(|e| {
        eprintln!("Failed to list files: {e}");
        process::exit(1);
    });
    let segments_file = files
        .iter()
        .find(|f| f.starts_with("segments_"))
        .unwrap_or_else(|| {
            eprintln!("No segments_N file found in '{index_path}'");
            process::exit(1);
        });

    let infos = segment_infos::read(&dir, segments_file).unwrap_or_else(|e| {
        eprintln!("Failed to read segments: {e}");
        process::exit(1);
    });

    let mut summary = Summary {
        total_docs: 0,
        max_doc: 0,
        segments: Vec::new(),
    };

    for (i, seg) in infos.segments.iter().enumerate() {
        let si = segment_info_format::read(&dir, &seg.name, &seg.id).unwrap_or_else(|e| {
            eprintln!("Failed to read segment info for '{}': {e}", seg.name);
            process::exit(1);
        });

        summary.total_docs += si.max_doc;
        summary.max_doc += si.max_doc;

        // Read field infos and terms metadata — use compound directory if needed
        let (field_infos, terms_reader) = if si.is_compound_file {
            let compound_dir =
                CompoundDirectory::open(&dir, &seg.name, &seg.id).unwrap_or_else(|e| {
                    eprintln!("Failed to open compound dir for '{}': {e}", seg.name);
                    process::exit(1);
                });
            let fi = field_infos_format::read(&compound_dir, &si, "").unwrap_or_else(|e| {
                eprintln!("Failed to read field infos for '{}': {e}", seg.name);
                process::exit(1);
            });
            let suffix = postings_suffix(&fi);
            let tr = suffix.and_then(|s| {
                BlockTreeTermsReader::open(&compound_dir, &seg.name, &s, &seg.id, &fi).ok()
            });
            (fi, tr)
        } else {
            let fi = field_infos_format::read(&dir, &si, "").unwrap_or_else(|e| {
                eprintln!("Failed to read field infos for '{}': {e}", seg.name);
                process::exit(1);
            });
            let suffix = postings_suffix(&fi);
            let tr = suffix
                .and_then(|s| BlockTreeTermsReader::open(&dir, &seg.name, &s, &seg.id, &fi).ok());
            (fi, tr)
        };

        let mut fields: Vec<&FieldInfo> = field_infos.iter().collect();
        fields.sort_by_key(|fi| fi.number());

        let field_summaries = fields
            .iter()
            .map(|fi| {
                let term_count = terms_reader
                    .as_ref()
                    .and_then(|r| r.field_reader(fi.number()))
                    .map_or(0, |fr| fr.num_terms);

                FieldSummary {
                    name: fi.name().to_string(),
                    number: fi.number(),
                    index_options: index_options_str(fi.index_options()).to_string(),
                    has_norms: fi.has_norms(),
                    store_term_vector: fi.store_term_vector(),
                    has_payloads: fi.has_payloads(),
                    doc_values_type: doc_values_type_str(fi.doc_values_type()).to_string(),
                    point_dimension_count: fi.point_config().dimension_count,
                    point_index_dimension_count: fi.point_config().index_dimension_count,
                    point_num_bytes: fi.point_config().num_bytes,
                    term_count,
                    dv_doc_count: 0,
                }
            })
            .collect();

        summary.segments.push(SegmentSummary {
            index: i,
            max_doc: si.max_doc,
            num_docs: si.max_doc,
            fields: field_summaries,
        });
    }

    let json = serde_json::to_string_pretty(&summary).unwrap_or_else(|e| {
        eprintln!("Failed to serialize JSON: {e}");
        process::exit(1);
    });
    println!("{json}");
}

fn index_options_str(opt: IndexOptions) -> &'static str {
    match opt {
        IndexOptions::None => "NONE",
        IndexOptions::Docs => "DOCS",
        IndexOptions::DocsAndFreqs => "DOCS_AND_FREQS",
        IndexOptions::DocsAndFreqsAndPositions => "DOCS_AND_FREQS_AND_POSITIONS",
        IndexOptions::DocsAndFreqsAndPositionsAndOffsets => {
            "DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS"
        }
    }
}

fn doc_values_type_str(dvt: DocValuesType) -> &'static str {
    match dvt {
        DocValuesType::None => "NONE",
        DocValuesType::Numeric => "NUMERIC",
        DocValuesType::Binary => "BINARY",
        DocValuesType::Sorted => "SORTED",
        DocValuesType::SortedNumeric => "SORTED_NUMERIC",
        DocValuesType::SortedSet => "SORTED_SET",
    }
}

/// Derives the per-field postings segment suffix (e.g., "Lucene103_0") from field attributes.
fn postings_suffix(field_infos: &bearing::index::FieldInfos) -> Option<String> {
    field_infos.iter().find_map(|fi| {
        let format = fi.get_attribute("PerFieldPostingsFormat.format")?;
        let suffix = fi.get_attribute("PerFieldPostingsFormat.suffix")?;
        Some(format!("{format}_{suffix}"))
    })
}

fn parse_args() -> String {
    let args: Vec<String> = env::args().collect();
    let mut index_path = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-index" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Missing value for -index");
                    process::exit(1);
                }
                index_path = Some(args[i].clone());
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage();
                process::exit(1);
            }
        }
        i += 1;
    }

    match index_path {
        Some(p) => p,
        None => {
            print_usage();
            process::exit(1);
        }
    }
}

fn print_usage() {
    eprintln!("Usage: generate_summary -index <index_path>");
    eprintln!("\nGenerates a JSON summary of a Lucene index's structure.");
}
