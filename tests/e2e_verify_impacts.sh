#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Verifies competitive impact data in Rust-generated indexes.
#
# 1. Indexes testdata/impact-docs (150 docs, enough for 128-doc blocks)
# 2. Runs Java VerifyImpacts — checks for proper norm/impact data
# 3. Indexes same docs with Java IndexAllFields
# 4. Runs Java VerifyImpacts on Java index (baseline — should always pass)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DOCS_DIR="$PROJECT_DIR/testdata/impact-docs"
RUST_INDEX_DIR="$(mktemp -d)"
JAVA_INDEX_DIR="$(mktemp -d)"
trap 'rm -rf "$RUST_INDEX_DIR" "$JAVA_INDEX_DIR"' EXIT

# Build the Rust binary
cargo build --bin indexfiles --manifest-path "$PROJECT_DIR/Cargo.toml"
INDEXFILES="$PROJECT_DIR/target/debug/indexfiles"

GRADLE="$SCRIPT_DIR/java/gradlew --project-dir=$SCRIPT_DIR/java --quiet"

echo ""
echo "=== Indexing 150 docs with Rust ==="
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
echo "=== Indexing 150 docs with Java ==="
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
