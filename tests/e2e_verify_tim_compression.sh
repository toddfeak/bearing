#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Verifies .tim suffix compression in Rust-generated indexes.
#
# 1. Builds the Rust indexfiles binary
# 2. Indexes testdata/docs (or -docs DIR) with Rust
# 3. Runs Java VerifyTimCompression — checks for LOWERCASE_ASCII and LZ4 blocks
# 4. Indexes same docs with Java IndexAllFields
# 5. Runs Java VerifyTimCompression on Java index (baseline — should always pass)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse args
DOCS_DIR="$PROJECT_DIR/testdata/docs"
while [[ $# -gt 0 ]]; do
    case "$1" in
        -docs) DOCS_DIR="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

RUST_INDEX_DIR="$(mktemp -d)"
JAVA_INDEX_DIR="$(mktemp -d)"
trap 'rm -rf "$RUST_INDEX_DIR" "$JAVA_INDEX_DIR"' EXIT

# Build the Rust binary
cargo build --bin indexfiles --manifest-path "$PROJECT_DIR/Cargo.toml"
INDEXFILES="$PROJECT_DIR/target/debug/indexfiles"

GRADLE="$SCRIPT_DIR/java/gradlew --project-dir=$SCRIPT_DIR/java --quiet"

echo ""
echo "=== Indexing docs with Rust ==="
"$INDEXFILES" -docs "$DOCS_DIR" -index "$RUST_INDEX_DIR"

echo ""
echo "=== Verifying .tim compression on Rust index ==="
if $GRADLE verifyTimCompression -PindexDir="$RUST_INDEX_DIR" 2>&1; then
    echo "Rust index: VerifyTimCompression PASSED"
    RUST_RESULT="PASS"
else
    echo "Rust index: VerifyTimCompression FAILED"
    RUST_RESULT="FAIL"
fi

echo ""
echo "=== Indexing docs with Java ==="
$GRADLE indexAllFields -PdocsDir="$DOCS_DIR" -PindexDir="$JAVA_INDEX_DIR" 2>&1

echo ""
echo "=== Verifying .tim compression on Java index (baseline) ==="
$GRADLE verifyTimCompression -PindexDir="$JAVA_INDEX_DIR" 2>&1
echo "Java index: VerifyTimCompression PASSED (baseline)"

echo ""
echo "=== Results ==="
echo "  Rust index: $RUST_RESULT"
echo "  Java index: PASS (baseline)"

if [ "$RUST_RESULT" = "FAIL" ]; then
    echo ""
    echo "Rust index lacks .tim suffix compression (expected before fix)."
    exit 1
fi
