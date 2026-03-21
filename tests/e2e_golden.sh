#!/bin/bash
# Golden index summary test: Rust writes → Java reads → compare against golden summary.
#
# This verifies that a Rust-written index has the same structure and statistics
# as a Java-written index for the same corpus.
#
# Usage: ./tests/e2e_golden.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
GOLDEN_DOCS="$PROJECT_DIR/testdata/golden-docs"
GOLDEN_SUMMARY="$PROJECT_DIR/testdata/golden-summary.json"
JAVA_DIR="$PROJECT_DIR/tests/java"

# Create temp directory for the Rust-written index
INDEX_DIR=$(mktemp -d)
trap "rm -rf $INDEX_DIR" EXIT

echo "Building Rust indexfiles binary..."
cargo build --bin indexfiles --manifest-path "$PROJECT_DIR/Cargo.toml" 2>&1 | tail -1

echo ""
echo "========================================"
echo "  Golden Summary Test"
echo "========================================"

# Index golden-docs with Rust (non-compound, single-threaded)
echo "Indexing golden-docs with Rust..."
"$PROJECT_DIR/target/debug/indexfiles" \
    -docs "$GOLDEN_DOCS" \
    -index "$INDEX_DIR" \
    > /dev/null 2>&1

# Generate summary from Rust-written index using Java
echo "Generating summary from Rust-written index..."
ACTUAL_SUMMARY=$(mktemp)
"$JAVA_DIR/gradlew" -p "$JAVA_DIR" -q generateIndexSummary \
    -PindexDir="$INDEX_DIR" \
    2>/dev/null > "$ACTUAL_SUMMARY"

# Compare against golden
echo "Comparing against golden summary..."
if diff -u "$GOLDEN_SUMMARY" "$ACTUAL_SUMMARY" > /dev/null 2>&1; then
    echo "PASSED: Rust-written index matches golden summary"
    rm -f "$ACTUAL_SUMMARY"
    exit 0
else
    echo "FAILED: Rust-written index differs from golden summary"
    echo ""
    echo "Diff (expected vs actual):"
    diff -u "$GOLDEN_SUMMARY" "$ACTUAL_SUMMARY" || true
    rm -f "$ACTUAL_SUMMARY"
    exit 1
fi
