#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Verifies competitive impact data in Rust-generated indexes.
#
# 1. Generates a 500-doc corpus (enough for 128-doc blocks on common terms)
# 2. Indexes with Rust indexfiles
# 3. Runs Java VerifyImpacts — checks for proper norm/impact data
# 4. Indexes same corpus with Java IndexAllFields
# 5. Runs Java VerifyImpacts on Java index (baseline — should always pass)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Generate a 500-doc corpus
DOCS_DIR="$(mktemp -d)"
RUST_INDEX_DIR="$(mktemp -d)"
JAVA_INDEX_DIR="$(mktemp -d)"
trap 'rm -rf "$DOCS_DIR" "$RUST_INDEX_DIR" "$JAVA_INDEX_DIR"' EXIT

echo "=== Generating 500-doc corpus ==="
python3 "$PROJECT_DIR/testdata/gen_docs.py" -n 500 2>&1
DOCS_DIR="/tmp/perf-docs"

# Build the Rust binary
cargo build --bin indexfiles --manifest-path "$PROJECT_DIR/Cargo.toml"
INDEXFILES="$PROJECT_DIR/target/debug/indexfiles"

GRADLE="$SCRIPT_DIR/java/gradlew --project-dir=$SCRIPT_DIR/java --quiet"

echo ""
echo "=== Indexing 500 docs with Rust ==="
"$INDEXFILES" -docs "$DOCS_DIR" -index "$RUST_INDEX_DIR"

echo ""
echo "=== Verifying impacts on Rust index ==="
if $GRADLE verifyImpacts -PindexDir="$RUST_INDEX_DIR" 2>&1; then
    echo "Rust index: VerifyImpacts PASSED"
    RUST_RESULT="PASS"
else
    echo "Rust index: VerifyImpacts FAILED (expected — defect not yet fixed)"
    RUST_RESULT="FAIL"
fi

echo ""
echo "=== Indexing 500 docs with Java ==="
$GRADLE indexAllFields -PdocsDir="$DOCS_DIR" -PindexDir="$JAVA_INDEX_DIR" 2>&1

echo ""
echo "=== Verifying impacts on Java index (baseline) ==="
$GRADLE verifyImpacts -PindexDir="$JAVA_INDEX_DIR" 2>&1
echo "Java index: VerifyImpacts PASSED (baseline)"

echo ""
echo "=== Results ==="
echo "  Rust index: $RUST_RESULT"
echo "  Java index: PASS (baseline)"

if [ "$RUST_RESULT" = "FAIL" ]; then
    echo ""
    echo "Rust index lacks proper competitive impacts (expected before fix)."
    exit 1
fi
