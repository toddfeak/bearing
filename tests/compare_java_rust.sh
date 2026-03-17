#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Usage: compare_java_rust.sh [-docs DIR] [--debug] [--threads N] [--1t] [--no-verify] [--compound]
#
# Options:
#   -docs DIR      Documents directory (default: testdata/docs)
#   --debug        Build Rust in debug mode (default: release)
#   --threads N    Thread count for multi-threaded runs (default: 12)
#   --1t           Also run single-threaded (1T) for both Java and Rust
#   --no-verify    Skip VerifyIndex validation
#   --compound     Use compound file format (.cfs/.cfe)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

GRADLE="$SCRIPT_DIR/java/gradlew --project-dir=$SCRIPT_DIR/java"

DOCS_DIR="$PROJECT_DIR/testdata/docs"
BUILD_MODE="release"
CARGO_FLAGS="--release"
MT_THREADS=12
RUN_1T=""
VERIFY="yes"
COMPOUND=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -docs)
            DOCS_DIR="$2"
            shift 2
            ;;
        --debug)
            BUILD_MODE="debug"
            CARGO_FLAGS=""
            shift
            ;;
        --threads)
            MT_THREADS="$2"
            shift 2
            ;;
        --1t)
            RUN_1T="yes"
            shift
            ;;
        --no-verify)
            VERIFY=""
            shift
            ;;
        --compound)
            COMPOUND="yes"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: compare_java_rust.sh [-docs DIR] [--debug] [--threads N] [--1t] [--no-verify] [--compound]"
            exit 1
            ;;
    esac
done

DOCS_DIR="$(cd "$DOCS_DIR" && pwd)"
DOC_COUNT=$(find "$DOCS_DIR" -type f -o -type l | wc -l)
DOC_SIZE=$(du -shL "$DOCS_DIR" | cut -f1)

JAVA_MT_INDEX="$(mktemp -d)"
RUST_MT_INDEX="$(mktemp -d)"
CLEANUP="$JAVA_MT_INDEX $RUST_MT_INDEX"

if [[ -n "$RUN_1T" ]]; then
    JAVA_1T_INDEX="$(mktemp -d)"
    RUST_1T_INDEX="$(mktemp -d)"
    CLEANUP="$CLEANUP $JAVA_1T_INDEX $RUST_1T_INDEX"
fi
trap 'rm -rf $CLEANUP' EXIT

echo "========================================"
echo "  Java vs Rust Lucene Index Comparison"
echo "========================================"
echo ""
echo "Docs directory: $DOCS_DIR ($DOC_COUNT files, $DOC_SIZE)"
echo "Rust build:     $BUILD_MODE"
if [[ -n "$RUN_1T" ]]; then
    echo "Threads:        1, $MT_THREADS"
else
    echo "Threads:        $MT_THREADS"
fi
echo "Compound:       $(if [[ -n "$COMPOUND" ]]; then echo yes; else echo no; fi)"
echo "Verify:         $(if [[ -n "$VERIFY" ]]; then echo yes; else echo no; fi)"
echo ""

# --- Helpers ---

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

index_stats() {
    local dir="$1"
    _INDEX_TOTAL=0
    _INDEX_FILE_COUNT=0
    for f in "$dir"/*; do
        if [ -f "$f" ]; then
            local size
            size=$(stat --format='%s' "$f")
            _INDEX_TOTAL=$((_INDEX_TOTAL + size))
            _INDEX_FILE_COUNT=$((_INDEX_FILE_COUNT + 1))
        fi
    done
}

format_time() {
    local ms="$1"
    if [ "$ms" -ge 60000 ]; then
        local min=$(( ms / 60000 ))
        local sec=$(( (ms % 60000) / 1000 ))
        echo "${min}m${sec}s"
    elif [ "$ms" -ge 10000 ]; then
        echo "$(echo "scale=1; $ms / 1000" | bc)s"
    else
        echo "${ms}ms"
    fi
}

format_size() {
    local bytes="$1"
    if [ "$bytes" -ge 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1073741824" | bc) GB"
    elif [ "$bytes" -ge 1048576 ]; then
        echo "$(echo "scale=1; $bytes / 1048576" | bc) MB"
    else
        echo "$(echo "scale=1; $bytes / 1024" | bc) KB"
    fi
}

run_step=0

run_index() {
    local label="$1"
    local cmd="$2"
    local index_dir="$3"
    shift 3

    run_step=$((run_step + 1))
    echo "========================================"
    echo "  $run_step. $label"
    echo "========================================"
    run_with_metrics "$label" $cmd "$@"
    local ms=$_TIME_MS
    local rss_kb=$_PEAK_RSS_KB
    local rss_mb
    rss_mb=$(echo "scale=1; $rss_kb / 1024" | bc)
    index_stats "$index_dir"
    local total=$_INDEX_TOTAL
    local fc=$_INDEX_FILE_COUNT
    echo "Time:       $(format_time $ms)"
    echo "Peak RSS:   ${rss_mb} MB"
    echo "Index size: $(format_size $total) ($fc files)"
    echo ""

    # Export results via naming convention: caller reads _TIME_MS etc.
    _TIME_MS=$ms
    _PEAK_RSS_KB=$rss_kb
    _INDEX_TOTAL=$total
    _INDEX_FILE_COUNT=$fc
}

# --- Build ---
RUST_COMPOUND_FLAG=""
JAVA_COMPOUND_FLAG=""
if [[ -n "$COMPOUND" ]]; then
    RUST_COMPOUND_FLAG="--compound"
    JAVA_COMPOUND_FLAG="-Pcompound=true"
fi

echo "Building Java test utilities..."
$GRADLE compileJava --quiet 2>&1

echo "Building Rust indexfiles ($BUILD_MODE)..."
cargo build --bin indexfiles $CARGO_FLAGS --manifest-path "$PROJECT_DIR/Cargo.toml" 2>&1 | tail -1
INDEXFILES="$PROJECT_DIR/target/$BUILD_MODE/indexfiles"
echo ""

# --- 1T runs (optional) ---
if [[ -n "$RUN_1T" ]]; then
    run_index "JAVA IndexAllFields (1 thread)" \
        "$GRADLE indexAllFields --quiet -PdocsDir=$DOCS_DIR -PindexDir=$JAVA_1T_INDEX $JAVA_COMPOUND_FLAG" \
        "$JAVA_1T_INDEX"
    JAVA_1T_MS=$_TIME_MS; JAVA_1T_RSS_KB=$_PEAK_RSS_KB
    JAVA_1T_TOTAL=$_INDEX_TOTAL; JAVA_1T_FC=$_INDEX_FILE_COUNT

    run_index "RUST indexfiles ($BUILD_MODE, 1 thread)" \
        "$INDEXFILES -docs $DOCS_DIR -index $RUST_1T_INDEX --threads 1 $RUST_COMPOUND_FLAG" \
        "$RUST_1T_INDEX"
    RUST_1T_MS=$_TIME_MS; RUST_1T_RSS_KB=$_PEAK_RSS_KB
    RUST_1T_TOTAL=$_INDEX_TOTAL; RUST_1T_FC=$_INDEX_FILE_COUNT
fi

# --- MT runs (always) ---
run_index "JAVA IndexAllFields ($MT_THREADS threads)" \
    "$GRADLE indexAllFields --quiet -PdocsDir=$DOCS_DIR -PindexDir=$JAVA_MT_INDEX -Pthreads=$MT_THREADS $JAVA_COMPOUND_FLAG" \
    "$JAVA_MT_INDEX"
JAVA_MT_MS=$_TIME_MS; JAVA_MT_RSS_KB=$_PEAK_RSS_KB
JAVA_MT_TOTAL=$_INDEX_TOTAL; JAVA_MT_FC=$_INDEX_FILE_COUNT

run_index "RUST indexfiles ($BUILD_MODE, $MT_THREADS threads)" \
    "$INDEXFILES -docs $DOCS_DIR -index $RUST_MT_INDEX --threads $MT_THREADS $RUST_COMPOUND_FLAG" \
    "$RUST_MT_INDEX"
RUST_MT_MS=$_TIME_MS; RUST_MT_RSS_KB=$_PEAK_RSS_KB
RUST_MT_TOTAL=$_INDEX_TOTAL; RUST_MT_FC=$_INDEX_FILE_COUNT

# --- VerifyIndex ---
if [[ -n "$VERIFY" ]]; then
    VERIFY_TARGETS=()
    if [[ -n "$RUN_1T" ]]; then
        VERIFY_TARGETS+=("Java (1T):$JAVA_1T_INDEX" "Rust (1T):$RUST_1T_INDEX")
    fi
    VERIFY_TARGETS+=("Java (${MT_THREADS}T):$JAVA_MT_INDEX" "Rust (${MT_THREADS}T):$RUST_MT_INDEX")
    for label_dir in "${VERIFY_TARGETS[@]}"; do
        label="${label_dir%%:*}"
        dir="${label_dir#*:}"
        echo "========================================"
        echo "  VerifyIndex on $label index"
        echo "========================================"
        $GRADLE verifyIndex --quiet -PindexDir="$dir" -PdocCount="$DOC_COUNT" 2>&1
        echo ""
    done
fi

# --- Summary ---
echo "========================================"
echo "  SUMMARY  ($DOC_COUNT docs, $DOC_SIZE input)"
echo "========================================"
echo ""

if [[ -n "$RUN_1T" ]]; then
    # 4-column summary
    JAVA_1T_RSS_MB=$(echo "scale=1; $JAVA_1T_RSS_KB / 1024" | bc)
    RUST_1T_RSS_MB=$(echo "scale=1; $RUST_1T_RSS_KB / 1024" | bc)
    JAVA_MT_RSS_MB=$(echo "scale=1; $JAVA_MT_RSS_KB / 1024" | bc)
    RUST_MT_RSS_MB=$(echo "scale=1; $RUST_MT_RSS_KB / 1024" | bc)

    printf "%-25s %15s %15s %15s %15s\n" "" "Java (1T)" "Java (${MT_THREADS}T)" "Rust (1T)" "Rust (${MT_THREADS}T)"
    printf "%-25s %15s %15s %15s %15s\n" "-------------------------" "---------------" "---------------" "---------------" "---------------"
    printf "%-25s %15s %15s %15s %15s\n" "Indexing time" "$(format_time $JAVA_1T_MS)" "$(format_time $JAVA_MT_MS)" "$(format_time $RUST_1T_MS)" "$(format_time $RUST_MT_MS)"
    printf "%-25s %15s %15s %15s %15s\n" "Peak RSS" "${JAVA_1T_RSS_MB} MB" "${JAVA_MT_RSS_MB} MB" "${RUST_1T_RSS_MB} MB" "${RUST_MT_RSS_MB} MB"
    printf "%-25s %15s %15s %15s %15s\n" "Total index size" "$(format_size $JAVA_1T_TOTAL)" "$(format_size $JAVA_MT_TOTAL)" "$(format_size $RUST_1T_TOTAL)" "$(format_size $RUST_MT_TOTAL)"
    printf "%-25s %15s %15s %15s %15s\n" "File count" "$JAVA_1T_FC" "$JAVA_MT_FC" "$RUST_1T_FC" "$RUST_MT_FC"

    JAVA_1T_DPS=$(echo "scale=0; $DOC_COUNT * 1000 / $JAVA_1T_MS" | bc)
    JAVA_MT_DPS=$(echo "scale=0; $DOC_COUNT * 1000 / $JAVA_MT_MS" | bc)
    RUST_1T_DPS=$(echo "scale=0; $DOC_COUNT * 1000 / $RUST_1T_MS" | bc)
    RUST_MT_DPS=$(echo "scale=0; $DOC_COUNT * 1000 / $RUST_MT_MS" | bc)
    printf "%-25s %15s %15s %15s %15s\n" "Docs/sec" "$JAVA_1T_DPS" "$JAVA_MT_DPS" "$RUST_1T_DPS" "$RUST_MT_DPS"

    if [ "$RUST_1T_MS" -gt 0 ] && [ "$JAVA_1T_MS" -gt 0 ]; then
        RATIO_1T=$(echo "scale=1; $JAVA_1T_MS / $RUST_1T_MS" | bc)
        printf "%-25s %15s %15s %15s %15s\n" "Rust vs Java speedup" "" "" "${RATIO_1T}x" ""
    fi
    if [ "$RUST_MT_MS" -gt 0 ] && [ "$JAVA_MT_MS" -gt 0 ]; then
        RATIO_MT=$(echo "scale=1; $JAVA_MT_MS / $RUST_MT_MS" | bc)
        printf "%-25s %15s %15s %15s %15s\n" "Rust vs Java speedup" "" "" "" "${RATIO_MT}x"
    fi
else
    # 2-column summary (MT only)
    JAVA_MT_RSS_MB=$(echo "scale=1; $JAVA_MT_RSS_KB / 1024" | bc)
    RUST_MT_RSS_MB=$(echo "scale=1; $RUST_MT_RSS_KB / 1024" | bc)

    printf "%-25s %15s %15s\n" "" "Java (${MT_THREADS}T)" "Rust (${MT_THREADS}T)"
    printf "%-25s %15s %15s\n" "-------------------------" "---------------" "---------------"
    printf "%-25s %15s %15s\n" "Indexing time" "$(format_time $JAVA_MT_MS)" "$(format_time $RUST_MT_MS)"
    printf "%-25s %15s %15s\n" "Peak RSS" "${JAVA_MT_RSS_MB} MB" "${RUST_MT_RSS_MB} MB"
    printf "%-25s %15s %15s\n" "Total index size" "$(format_size $JAVA_MT_TOTAL)" "$(format_size $RUST_MT_TOTAL)"
    printf "%-25s %15s %15s\n" "File count" "$JAVA_MT_FC" "$RUST_MT_FC"

    JAVA_MT_DPS=$(echo "scale=0; $DOC_COUNT * 1000 / $JAVA_MT_MS" | bc)
    RUST_MT_DPS=$(echo "scale=0; $DOC_COUNT * 1000 / $RUST_MT_MS" | bc)
    printf "%-25s %15s %15s\n" "Docs/sec" "$JAVA_MT_DPS" "$RUST_MT_DPS"

    if [ "$RUST_MT_MS" -gt 0 ] && [ "$JAVA_MT_MS" -gt 0 ]; then
        RATIO=$(echo "scale=1; $JAVA_MT_MS / $RUST_MT_MS" | bc)
        printf "%-25s %15s %15s\n" "Rust vs Java speedup" "" "${RATIO}x"
    fi
fi

echo ""
