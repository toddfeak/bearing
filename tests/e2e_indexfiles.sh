#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCS_DIR="$PROJECT_DIR/testdata/docs"

# Build the binary
cargo build --bin indexfiles --manifest-path "$PROJECT_DIR/Cargo.toml"

INDEXFILES="$PROJECT_DIR/target/debug/indexfiles"

# Create temporary directories for index output
INDEX_DIR="$(mktemp -d)"
JAVA_INDEX_DIR="$(mktemp -d)"
trap 'rm -rf "$INDEX_DIR" "$JAVA_INDEX_DIR"' EXIT

GRADLE="$SCRIPT_DIR/java/gradlew --project-dir=$SCRIPT_DIR/java --quiet"

echo "=== Test: indexfiles with -docs and -index ==="
"$INDEXFILES" -docs "$DOCS_DIR" -index "$INDEX_DIR"
echo "PASSED"

echo ""
echo "=== Test: verify index files exist on disk ==="
EXPECTED_FILES="segments_1 _0.si _0.cfs _0.cfe"
for f in $EXPECTED_FILES; do
    if [ ! -f "$INDEX_DIR/$f" ]; then
        echo "FAILED: expected file '$f' not found in $INDEX_DIR"
        ls -la "$INDEX_DIR"
        exit 1
    fi
    echo "  found: $f ($(stat --format='%s' "$INDEX_DIR/$f") bytes)"
done
echo "PASSED"

echo ""
echo "=== Test: re-index over existing index (should replace) ==="
"$INDEXFILES" -docs "$DOCS_DIR" -index "$INDEX_DIR"
# Verify the same expected files still exist after re-indexing
for f in $EXPECTED_FILES; do
    if [ ! -f "$INDEX_DIR/$f" ]; then
        echo "FAILED: expected file '$f' not found after re-index in $INDEX_DIR"
        ls -la "$INDEX_DIR"
        exit 1
    fi
done
echo "PASSED"

echo ""
echo "=== Test: Java Lucene verification (all-fields Rust index) ==="
$GRADLE verifyIndex -PindexDir="$INDEX_DIR" -PdocCount=3 2>&1
echo "PASSED"

echo ""
echo "=== Test: Java IndexAllFields + verification ==="
$GRADLE indexAllFields -PdocsDir="$DOCS_DIR" -PindexDir="$JAVA_INDEX_DIR" 2>&1
$GRADLE verifyIndex -PindexDir="$JAVA_INDEX_DIR" -PdocCount=3 2>&1
echo "PASSED"

echo ""
echo "All tests passed."
