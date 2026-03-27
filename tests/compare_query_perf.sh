#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Compare query performance between Java Lucene and Rust Bearing.
#
# For a given document corpus:
#   1. Build Java and Rust indexes (single-threaded)
#   2. Generate queries (mix of term and boolean queries)
#   3. Query both indexes with the same queries
#   4. Verify results match exactly (including totalHits)
#   5. Report timing comparison
#
# Usage: compare_query_perf.sh -docs DIR [--threads N]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

GRADLE="$SCRIPT_DIR/java/gradlew --project-dir=$SCRIPT_DIR/java"
DOCS_DIR=""
INDEX_THREADS=12

while [[ $# -gt 0 ]]; do
    case "$1" in
        -docs)
            DOCS_DIR="$2"
            shift 2
            ;;
        --threads)
            INDEX_THREADS="$2"
            shift 2
            ;;
        *)
            echo "Usage: compare_query_perf.sh -docs DIR [--threads N]"
            exit 1
            ;;
    esac
done

if [[ -z "$DOCS_DIR" ]]; then
    echo "Usage: compare_query_perf.sh -docs DIR [--threads N]"
    exit 1
fi

DOCS_DIR="$(cd "$DOCS_DIR" && pwd)"
DOC_COUNT=$(find -L "$DOCS_DIR" -type f | wc -l)
DOC_SIZE=$(du -shL "$DOCS_DIR" | cut -f1)

# Measure peak RSS of a command. Sets _PEAK_RSS_KB after the call.
_PEAK_RSS_KB=0
run_measured() {
    local time_output
    time_output=$(mktemp)
    /usr/bin/time -v "$@" 2>"$time_output" 1>/dev/null
    _PEAK_RSS_KB=$(grep "Maximum resident set size" "$time_output" | awk '{print $NF}')
    rm -f "$time_output"
}

# Use a single Java-built index so doc IDs are identical across both query engines
INDEX_DIR="$(mktemp -d)"
QUERIES_FILE="$(mktemp)"
JAVA_RESULTS="$(mktemp)"
RUST_RESULTS="$(mktemp)"
trap 'rm -rf "$INDEX_DIR" "$QUERIES_FILE" "$JAVA_RESULTS" "$RUST_RESULTS"' EXIT

echo "========================================"
echo "  Query Performance: Java vs Rust"
echo "========================================"
echo ""
echo "Docs:   $DOCS_DIR ($DOC_COUNT files, $DOC_SIZE)"
echo ""

# --- Build ---
echo "Building Java test utilities..."
$GRADLE compileJava --quiet 2>&1

echo "Building Rust queryindex (release)..."
cargo build --release --bin indexfiles --bin queryindex \
    --manifest-path "$PROJECT_DIR/Cargo.toml" 2>&1 | tail -1
INDEXFILES="$PROJECT_DIR/target/release/indexfiles"
QUERYINDEX="$PROJECT_DIR/target/release/queryindex"
echo ""

# --- Index (Java-built so doc IDs are identical for both query engines) ---
echo "Indexing with Java ($INDEX_THREADS threads)..."
$GRADLE indexAllFields --quiet \
    "-PdocsDir=$DOCS_DIR" "-PindexDir=$INDEX_DIR" "-Pthreads=$INDEX_THREADS" 2>&1 | tail -1
INDEX_SIZE=$(du -sh "$INDEX_DIR" | cut -f1)
INDEX_FILES=$(find "$INDEX_DIR" -type f | wc -l)
echo "Index size: $INDEX_SIZE ($INDEX_FILES files)"
echo ""

# --- Generate queries ---
echo "Generating queries from corpus..."
python3 -c "
import os, random, sys

docs_dir = '$DOCS_DIR'
n = int('$DOC_COUNT')

# Collect vocabulary from corpus.
# ASCII-only: Bearing's StandardTokenizer doesn't yet match Java's Unicode text
# segmentation, so non-ASCII text can tokenize differently between the two engines.
# Known gaps: CJK ideographs (split per-character by Java), hyphenated words,
# email addresses, and other cases where StandardTokenizer applies rules beyond
# simple whitespace splitting.
words = set()
files = sorted(os.listdir(docs_dir))
sample = files[:min(50, len(files))]
for f in sample:
    path = os.path.join(docs_dir, f)
    try:
        text = open(path, errors='replace').read()
        for w in text.split():
            w = w.strip('.,;:!?\"()[]{}').lower()
            if w.isascii() and w.isalpha() and 3 <= len(w) <= 15:
                words.add(w)
    except:
        pass
words = sorted(words)
if len(words) < 2:
    print(f'Error: need at least 2 unique words, found {len(words)}', file=sys.stderr)
    sys.exit(1)
if len(words) < n:
    print(f'Warning: only {len(words)} unique words found, need {n}', file=sys.stderr)
    n = len(words)

rng = random.Random(42)

# --- Query generators (add new types here) ---
def gen_term_query(words, rng):
    \"\"\"Single term query: word\"\"\"
    return rng.choice(words)

def gen_boolean_must_query(words, rng):
    \"\"\"Double MUST boolean query: +word1 +word2\"\"\"
    w1, w2 = rng.sample(words, 2)
    return f'+{w1} +{w2}'

def gen_boolean_should_query(words, rng):
    \"\"\"Double SHOULD boolean query: word1 word2\"\"\"
    w1, w2 = rng.sample(words, 2)
    return f'{w1} {w2}'

def gen_boolean_must_not_query(words, rng):
    \"\"\"MUST with single MUST_NOT: +word1 -word2\"\"\"
    w1, w2 = rng.sample(words, 2)
    return f'+{w1} -{w2}'

def gen_boolean_should_must_not_query(words, rng):
    \"\"\"SHOULD with single MUST_NOT: word1 word2 -word3\"\"\"
    w1, w2, w3 = rng.sample(words, 3)
    return f'{w1} {w2} -{w3}'

generators = [
    (gen_term_query,                    0.35),
    (gen_boolean_must_query,            0.15),
    (gen_boolean_should_query,          0.15),
    (gen_boolean_must_not_query,        0.15),
    (gen_boolean_should_must_not_query, 0.20),
]

# Build cumulative weights for weighted selection
cum_weights = []
total = 0
for _, weight in generators:
    total += weight
    cum_weights.append(total)

queries = []
for _ in range(n):
    r = rng.random() * total
    for i, cw in enumerate(cum_weights):
        if r <= cw:
            queries.append(generators[i][0](words, rng))
            break

with open('$QUERIES_FILE', 'w') as f:
    for q in queries:
        f.write(q + '\n')

term_count = sum(1 for q in queries if ' ' not in q)
must_count = sum(1 for q in queries if q.startswith('+') and '-' not in q)
should_count = sum(1 for q in queries if ' ' in q and not q.startswith('+') and '-' not in q)
excl_count = sum(1 for q in queries if '-' in q)
print(f'  {len(queries)} queries generated ({term_count} term, {must_count} MUST, {should_count} SHOULD, {excl_count} MUST_NOT)')
"
echo ""

# --- Query ---
echo "========================================"
echo "  Querying Java index"
echo "========================================"
JAVA_TIMING=$($GRADLE queryIndex --quiet \
    "-PindexDir=$INDEX_DIR" "-PqueriesFile=$QUERIES_FILE" "-PoutputFile=$JAVA_RESULTS" 2>&1)
echo "$JAVA_TIMING"
JAVA_AVG=$(echo "$JAVA_TIMING" | grep "Average:" | sed 's/.*Average: \([0-9.]*\).*/\1/')
run_measured $GRADLE queryIndex --quiet \
    "-PindexDir=$INDEX_DIR" "-PqueriesFile=$QUERIES_FILE" "-PoutputFile=/dev/null"
JAVA_RSS_MB=$(echo "scale=1; $_PEAK_RSS_KB / 1024" | bc)
echo "Peak RSS: ${JAVA_RSS_MB} MB"
echo ""

echo "========================================"
echo "  Querying same index with Rust"
echo "========================================"
RUST_TIMING=$("$QUERYINDEX" -index "$INDEX_DIR" -queries "$QUERIES_FILE" -output "$RUST_RESULTS" 2>&1)
echo "$RUST_TIMING"
RUST_AVG=$(echo "$RUST_TIMING" | grep "Average:" | sed 's/.*Average: \([0-9.]*\).*/\1/')
run_measured "$QUERYINDEX" -index "$INDEX_DIR" -queries "$QUERIES_FILE" -output /dev/null
RUST_RSS_MB=$(echo "scale=1; $_PEAK_RSS_KB / 1024" | bc)
echo "Peak RSS: ${RUST_RSS_MB} MB"
echo ""

# --- Compare results ---
echo "========================================"
echo "  Verifying results match"
echo "========================================"

DIFF_COUNT=$(diff "$JAVA_RESULTS" "$RUST_RESULTS" | grep "^[<>]" | wc -l || true)

if [[ "$DIFF_COUNT" -eq 0 ]]; then
    echo "PASSED: All query results match exactly (including totalHits and scores)"
else
    echo "FAILED: $DIFF_COUNT result lines differ"
    echo "First differences:"
    diff "$JAVA_RESULTS" "$RUST_RESULTS" | head -20
    exit 1
fi
echo ""

# --- Summary ---
QUERY_COUNT=$(wc -l < "$QUERIES_FILE")
echo "========================================"
echo "  SUMMARY ($DOC_COUNT docs, $DOC_SIZE input, $QUERY_COUNT queries)"
echo "========================================"
echo ""
printf "%-20s %15s %15s\n" "" "Java" "Rust"
printf "%-20s %15s %15s\n" "--------------------" "---------------" "---------------"
printf "%-20s %15s\n" "Index size" "$INDEX_SIZE ($INDEX_FILES files)"
printf "%-20s %12s µs %12s µs\n" "Avg query time" "$JAVA_AVG" "$RUST_AVG"
printf "%-20s %12s MB %12s MB\n" "Query peak RSS" "$JAVA_RSS_MB" "$RUST_RSS_MB"
if [[ -n "$JAVA_AVG" && -n "$RUST_AVG" ]]; then
    RATIO=$(echo "scale=1; $JAVA_AVG / $RUST_AVG" | bc 2>/dev/null || echo "?")
    printf "%-20s %15s %15s\n" "Rust speedup" "" "${RATIO}x"
fi
echo ""
