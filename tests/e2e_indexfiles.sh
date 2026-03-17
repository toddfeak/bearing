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

echo "=== Test: indexfiles default (non-compound) ==="
"$INDEXFILES" -docs "$DOCS_DIR" -index "$INDEX_DIR"
echo "PASSED"

echo ""
echo "=== Test: verify non-compound index files exist on disk ==="
EXPECTED_FILES="segments_1 _0.si _0.fnm _0_Lucene103_0.doc _0_Lucene103_0.pos _0_Lucene103_0.tim _0_Lucene103_0.tip _0.fdt _0.fdm _0.fdx _0.kdm _0.kdi _0.kdd _0.nvm _0.nvd"
for f in $EXPECTED_FILES; do
    if [ ! -f "$INDEX_DIR/$f" ]; then
        echo "FAILED: expected file '$f' not found in $INDEX_DIR"
        ls -la "$INDEX_DIR"
        exit 1
    fi
    echo "  found: $f ($(stat --format='%s' "$INDEX_DIR/$f") bytes)"
done
# Verify compound files do NOT exist
for f in _0.cfs _0.cfe; do
    if [ -f "$INDEX_DIR/$f" ]; then
        echo "FAILED: compound file '$f' should not exist in non-compound mode"
        ls -la "$INDEX_DIR"
        exit 1
    fi
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
echo "=== Test: indexfiles --compound ==="
COMPOUND_INDEX_DIR="$(mktemp -d)"
trap 'rm -rf "$INDEX_DIR" "$JAVA_INDEX_DIR" "$COMPOUND_INDEX_DIR"' EXIT
"$INDEXFILES" -docs "$DOCS_DIR" -index "$COMPOUND_INDEX_DIR" --compound
COMPOUND_EXPECTED="_0.cfs _0.cfe"
for f in $COMPOUND_EXPECTED; do
    if [ ! -f "$COMPOUND_INDEX_DIR/$f" ]; then
        echo "FAILED: expected compound file '$f' not found in $COMPOUND_INDEX_DIR"
        ls -la "$COMPOUND_INDEX_DIR"
        exit 1
    fi
    echo "  found: $f ($(stat --format='%s' "$COMPOUND_INDEX_DIR/$f") bytes)"
done
echo "PASSED"

echo ""
echo "=== Test: Java Lucene verification (all-fields Rust index) ==="
$GRADLE verifyIndex -PindexDir="$INDEX_DIR" -PdocCount=4 2>&1
echo "PASSED"

echo ""
echo "=== Test: Java IndexAllFields + verification ==="
$GRADLE indexAllFields -PdocsDir="$DOCS_DIR" -PindexDir="$JAVA_INDEX_DIR" 2>&1
$GRADLE verifyIndex -PindexDir="$JAVA_INDEX_DIR" -PdocCount=4 2>&1
echo "PASSED"

echo ""
echo "All tests passed."
