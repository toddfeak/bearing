#!/usr/bin/env bash
set -euo pipefail

# Usage: compare_java_rust.sh [-docs DOCS_DIR] [-release] [--threads N]
#
# Options:
#   -docs DIR      Use DIR as the documents directory (default: testdata/docs)
#   -release       Build and run Rust binary in release mode (default: debug)
#   --threads N    Number of threads for multi-threaded runs (default: 12)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

LUCENE_BASE="$PROJECT_DIR/reference/lucene-10.3.2/lucene"
LUCENE_CORE="$LUCENE_BASE/core/build/libs/lucene-core-10.3.2-SNAPSHOT.jar"

DOCS_DIR="$PROJECT_DIR/testdata/docs"
VERIFY_JAVA="$SCRIPT_DIR/VerifyIndex.java"
INDEX_ALL_JAVA="$SCRIPT_DIR/IndexAllFields.java"
BUILD_MODE="debug"
CARGO_FLAGS=""
MT_THREADS=12

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -docs)
            DOCS_DIR="$2"
            shift 2
            ;;
        -release)
            BUILD_MODE="release"
            CARGO_FLAGS="--release"
            shift
            ;;
        --threads)
            MT_THREADS="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: compare_java_rust.sh [-docs DOCS_DIR] [-release] [--threads N]"
            exit 1
            ;;
    esac
done

DOC_COUNT=$(find "$DOCS_DIR" -type f | wc -l)
DOC_SIZE=$(du -sh "$DOCS_DIR" | cut -f1)

JAVA_1T_INDEX="$(mktemp -d)"
JAVA_MT_INDEX="$(mktemp -d)"
RUST_1T_INDEX="$(mktemp -d)"
RUST_MT_INDEX="$(mktemp -d)"
VERIFY_CLASSES="$(mktemp -d)"
trap 'rm -rf "$JAVA_1T_INDEX" "$JAVA_MT_INDEX" "$RUST_1T_INDEX" "$RUST_MT_INDEX" "$VERIFY_CLASSES"' EXIT

echo "========================================"
echo "  Java vs Rust Lucene Index Comparison"
echo "========================================"
echo ""
echo "Docs directory: $DOCS_DIR ($DOC_COUNT files, $DOC_SIZE)"
echo "Rust build:     $BUILD_MODE"
echo "Threads:        1, $MT_THREADS"
echo ""

# --- Helper: extract peak RSS (KB) from /usr/bin/time -v output ---
# Runs command, captures wall time and peak RSS.
# Usage: run_with_metrics LABEL command [args...]
# Sets: _TIME_MS, _PEAK_RSS_KB
run_with_metrics() {
    local label="$1"
    shift
    local time_output
    time_output=$(mktemp)
    local start end
    start=$(date +%s%N)
    /usr/bin/time -v "$@" 2>"$time_output" 1>/dev/null
    end=$(date +%s%N)
    _TIME_MS=$(( (end - start) / 1000000 ))
    _PEAK_RSS_KB=$(grep "Maximum resident set size" "$time_output" | awk '{print $NF}')
    rm -f "$time_output"
}

# --- Helper: print index file listing and compute total size ---
# Usage: print_index_files DIR
# Sets: _INDEX_TOTAL
print_index_files() {
    local dir="$1"
    _INDEX_TOTAL=0
    for f in $(ls -1 "$dir" | sort); do
        if [ -f "$dir/$f" ]; then
            local size
            size=$(stat --format='%s' "$dir/$f")
            _INDEX_TOTAL=$((_INDEX_TOTAL + size))
            printf "  %-20s %10s bytes\n" "$f" "$size"
        fi
    done
    printf "  %-20s %10s bytes\n" "TOTAL" "$_INDEX_TOTAL"
}

# --- Compile Java classes ---
echo "Compiling IndexAllFields.java and VerifyIndex.java..."
javac -cp "$LUCENE_CORE" "$INDEX_ALL_JAVA" "$VERIFY_JAVA" -d "$VERIFY_CLASSES" 2>&1

# --- Build Rust binary ---
echo "Building Rust indexfiles ($BUILD_MODE)..."
cargo build --bin indexfiles $CARGO_FLAGS --manifest-path "$PROJECT_DIR/Cargo.toml" 2>&1 | tail -1
INDEXFILES="$PROJECT_DIR/target/$BUILD_MODE/indexfiles"
echo ""

JAVA_CP="$LUCENE_CORE:$VERIFY_CLASSES"

# --- 1. Java IndexAllFields (1 thread) ---
echo "========================================"
echo "  1. JAVA IndexAllFields (1 thread)"
echo "========================================"
run_with_metrics "Java-1T" java -cp "$JAVA_CP" IndexAllFields "$DOCS_DIR" "$JAVA_1T_INDEX"
JAVA_1T_MS=$_TIME_MS
JAVA_1T_RSS_KB=$_PEAK_RSS_KB
JAVA_1T_RSS_MB=$(echo "scale=1; $JAVA_1T_RSS_KB / 1024" | bc)
echo "Wall clock time: ${JAVA_1T_MS}ms"
echo "Peak RSS:        ${JAVA_1T_RSS_MB} MB"
echo ""
echo "Java (1 thread) index files:"
print_index_files "$JAVA_1T_INDEX"
JAVA_1T_TOTAL=$_INDEX_TOTAL
echo ""

# --- 2. Java IndexAllFields (N threads) ---
echo "========================================"
echo "  2. JAVA IndexAllFields ($MT_THREADS threads)"
echo "========================================"
run_with_metrics "Java-MT" java -cp "$JAVA_CP" IndexAllFields "$DOCS_DIR" "$JAVA_MT_INDEX" --threads "$MT_THREADS"
JAVA_MT_MS=$_TIME_MS
JAVA_MT_RSS_KB=$_PEAK_RSS_KB
JAVA_MT_RSS_MB=$(echo "scale=1; $JAVA_MT_RSS_KB / 1024" | bc)
echo "Wall clock time: ${JAVA_MT_MS}ms"
echo "Peak RSS:        ${JAVA_MT_RSS_MB} MB"
echo ""
echo "Java ($MT_THREADS threads) index files:"
print_index_files "$JAVA_MT_INDEX"
JAVA_MT_TOTAL=$_INDEX_TOTAL
echo ""

# --- 3. Rust indexfiles (1 thread) ---
echo "========================================"
echo "  3. RUST indexfiles ($BUILD_MODE, 1 thread)"
echo "========================================"
run_with_metrics "Rust-1T" "$INDEXFILES" -docs "$DOCS_DIR" -index "$RUST_1T_INDEX" --threads 1
RUST_1T_MS=$_TIME_MS
RUST_1T_RSS_KB=$_PEAK_RSS_KB
RUST_1T_RSS_MB=$(echo "scale=1; $RUST_1T_RSS_KB / 1024" | bc)
echo "Wall clock time: ${RUST_1T_MS}ms"
echo "Peak RSS:        ${RUST_1T_RSS_MB} MB"
echo ""
echo "Rust (1 thread) index files:"
print_index_files "$RUST_1T_INDEX"
RUST_1T_TOTAL=$_INDEX_TOTAL
echo ""

# --- 4. Rust indexfiles (N threads) ---
echo "========================================"
echo "  4. RUST indexfiles ($BUILD_MODE, $MT_THREADS threads)"
echo "========================================"
run_with_metrics "Rust-MT" "$INDEXFILES" -docs "$DOCS_DIR" -index "$RUST_MT_INDEX" --threads "$MT_THREADS"
RUST_MT_MS=$_TIME_MS
RUST_MT_RSS_KB=$_PEAK_RSS_KB
RUST_MT_RSS_MB=$(echo "scale=1; $RUST_MT_RSS_KB / 1024" | bc)
echo "Wall clock time: ${RUST_MT_MS}ms"
echo "Peak RSS:        ${RUST_MT_RSS_MB} MB"
echo ""
echo "Rust ($MT_THREADS threads) index files:"
print_index_files "$RUST_MT_INDEX"
RUST_MT_TOTAL=$_INDEX_TOTAL
echo ""

# --- VerifyIndex on all indexes ---
for label_dir in "Java (1T):$JAVA_1T_INDEX" "Java (${MT_THREADS}T):$JAVA_MT_INDEX" \
                 "Rust (1T):$RUST_1T_INDEX" "Rust (${MT_THREADS}T):$RUST_MT_INDEX"; do
    label="${label_dir%%:*}"
    dir="${label_dir#*:}"
    echo "========================================"
    echo "  VerifyIndex on $label index"
    echo "========================================"
    java -cp "$JAVA_CP" VerifyIndex "$dir" "$DOC_COUNT" 2>&1
    echo ""
done

# --- Summary ---
echo "========================================"
echo "  SUMMARY  ($DOC_COUNT docs, $DOC_SIZE input)"
echo "========================================"
echo ""
printf "%-25s %15s %15s %15s %15s\n" "" "Java (1T)" "Java (${MT_THREADS}T)" "Rust (1T)" "Rust (${MT_THREADS}T)"
printf "%-25s %15s %15s %15s %15s\n" "-------------------------" "---------------" "---------------" "---------------" "---------------"
printf "%-25s %15s %15s %15s %15s\n" "Indexing time" "${JAVA_1T_MS}ms" "${JAVA_MT_MS}ms" "${RUST_1T_MS}ms" "${RUST_MT_MS}ms"
printf "%-25s %15s %15s %15s %15s\n" "Peak RSS" "${JAVA_1T_RSS_MB} MB" "${JAVA_MT_RSS_MB} MB" "${RUST_1T_RSS_MB} MB" "${RUST_MT_RSS_MB} MB"
printf "%-25s %15s %15s %15s %15s\n" "Total index size" "${JAVA_1T_TOTAL}b" "${JAVA_MT_TOTAL}b" "${RUST_1T_TOTAL}b" "${RUST_MT_TOTAL}b"
JAVA_1T_FC=$(ls -1 "$JAVA_1T_INDEX" | wc -l)
JAVA_MT_FC=$(ls -1 "$JAVA_MT_INDEX" | wc -l)
RUST_1T_FC=$(ls -1 "$RUST_1T_INDEX" | wc -l)
RUST_MT_FC=$(ls -1 "$RUST_MT_INDEX" | wc -l)
printf "%-25s %15s %15s %15s %15s\n" "File count" "$JAVA_1T_FC" "$JAVA_MT_FC" "$RUST_1T_FC" "$RUST_MT_FC"

# Speedup: Rust vs Java at same thread count
if [ "$RUST_1T_MS" -gt 0 ] && [ "$JAVA_1T_MS" -gt 0 ]; then
    RATIO_1T=$(echo "scale=1; $JAVA_1T_MS / $RUST_1T_MS" | bc)
    printf "%-25s %15s %15s %15s %15s\n" "Rust vs Java speedup" "" "" "${RATIO_1T}x" ""
fi
if [ "$RUST_MT_MS" -gt 0 ] && [ "$JAVA_MT_MS" -gt 0 ]; then
    RATIO_MT=$(echo "scale=1; $JAVA_MT_MS / $RUST_MT_MS" | bc)
    printf "%-25s %15s %15s %15s %15s\n" "Rust vs Java speedup" "" "" "" "${RATIO_MT}x"
fi
echo ""
