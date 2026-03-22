#!/bin/bash
# Golden summary test: verifies Rust's generate_summary produces identical output
# to the checked-in golden summary when reading indexes from both Java and Rust.
#
# Sub-test 1: Java writes index → Rust reads → compare against golden
# Sub-test 2: Rust writes index → Rust reads → compare against golden
#
# Usage: ./tests/e2e_golden.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
GOLDEN_DOCS="$PROJECT_DIR/testdata/golden-docs"
GOLDEN_SUMMARY="$PROJECT_DIR/testdata/golden-summary.json"
JAVA_DIR="$PROJECT_DIR/tests/java"

FAILED=0

echo "Building Rust binaries..."
cargo build --bin indexfiles --bin generate_summary --manifest-path "$PROJECT_DIR/Cargo.toml" 2>&1 | tail -1

echo ""
echo "========================================"
echo "  Golden Summary Test"
echo "========================================"

# --- Sub-test 1: Java write → Rust read ---

echo ""
echo "--- Java write → Rust read ---"

JAVA_INDEX_DIR=$(mktemp -d)
trap "rm -rf $JAVA_INDEX_DIR" EXIT

echo "Indexing golden-docs with Java..."
GOLDEN_DOCS_ABS=$(cd "$GOLDEN_DOCS" && pwd)
"$JAVA_DIR/gradlew" -p "$JAVA_DIR" -q indexAllFields \
    -PdocsDir="$GOLDEN_DOCS_ABS" \
    -PindexDir="$JAVA_INDEX_DIR" \
    > /dev/null 2>&1

echo "Generating summary with Rust..."
ACTUAL=$(mktemp)
"$PROJECT_DIR/target/debug/generate_summary" -index "$JAVA_INDEX_DIR" > "$ACTUAL"

if diff -u "$GOLDEN_SUMMARY" "$ACTUAL" > /dev/null 2>&1; then
    echo "PASSED: Rust reader matches golden summary (Java-written index)"
else
    echo "FAILED: Rust reader differs from golden summary (Java-written index)"
    echo ""
    diff -u "$GOLDEN_SUMMARY" "$ACTUAL" || true
    FAILED=1
fi
rm -f "$ACTUAL"

# --- Sub-test 2: Rust write → Rust read ---

echo ""
echo "--- Rust write → Rust read ---"

RUST_INDEX_DIR=$(mktemp -d)
trap "rm -rf $JAVA_INDEX_DIR $RUST_INDEX_DIR" EXIT

echo "Indexing golden-docs with Rust..."
"$PROJECT_DIR/target/debug/indexfiles" \
    -docs "$GOLDEN_DOCS" \
    -index "$RUST_INDEX_DIR" \
    > /dev/null 2>&1

echo "Generating summary with Rust..."
ACTUAL=$(mktemp)
"$PROJECT_DIR/target/debug/generate_summary" -index "$RUST_INDEX_DIR" > "$ACTUAL"

if diff -u "$GOLDEN_SUMMARY" "$ACTUAL" > /dev/null 2>&1; then
    echo "PASSED: Rust reader matches golden summary (Rust-written index)"
else
    echo "FAILED: Rust reader differs from golden summary (Rust-written index)"
    echo ""
    diff -u "$GOLDEN_SUMMARY" "$ACTUAL" || true
    FAILED=1
fi
rm -f "$ACTUAL"

# --- Result ---

echo ""
if [ $FAILED -eq 0 ]; then
    echo "PASSED"
    exit 0
else
    echo "FAILED"
    exit 1
fi
