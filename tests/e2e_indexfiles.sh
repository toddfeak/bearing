#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCS_DIR="$PROJECT_DIR/testdata/docs"

# Build the binary
cargo build --bin indexfiles --manifest-path "$PROJECT_DIR/Cargo.toml"

INDEXFILES="$PROJECT_DIR/target/debug/indexfiles"

# Create temporary directories for index output
INDEX_DIR="$(mktemp -d)"
VERIFY_CLASSES="$(mktemp -d)"
JAVA_INDEX_DIR="$(mktemp -d)"
trap 'rm -rf "$INDEX_DIR" "$VERIFY_CLASSES" "$JAVA_INDEX_DIR"' EXIT

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
LUCENE_CORE="$PROJECT_DIR/reference/lucene-10.3.2/lucene/core/build/libs/lucene-core-10.3.2-SNAPSHOT.jar"
VERIFY_JAVA="$SCRIPT_DIR/VerifyIndex.java"

if [ ! -f "$LUCENE_CORE" ]; then
    echo "SKIPPED: lucene-core JAR not found at $LUCENE_CORE"
else
    javac -cp "$LUCENE_CORE" "$VERIFY_JAVA" -d "$VERIFY_CLASSES" 2>&1
    java -cp "$LUCENE_CORE:$VERIFY_CLASSES" VerifyIndex "$INDEX_DIR" 3 2>&1
    echo "PASSED"
fi

echo ""
echo "=== Test: Java IndexAllFields + verification ==="
INDEX_ALL_JAVA="$SCRIPT_DIR/IndexAllFields.java"

if [ ! -f "$LUCENE_CORE" ]; then
    echo "SKIPPED: lucene-core JAR not found at $LUCENE_CORE"
else
    javac -cp "$LUCENE_CORE" "$INDEX_ALL_JAVA" -d "$VERIFY_CLASSES" 2>&1
    java -cp "$LUCENE_CORE:$VERIFY_CLASSES" IndexAllFields "$DOCS_DIR" "$JAVA_INDEX_DIR" 2>&1
    java -cp "$LUCENE_CORE:$VERIFY_CLASSES" VerifyIndex "$JAVA_INDEX_DIR" 3 2>&1
    echo "PASSED"
fi

echo ""
echo "All tests passed."
