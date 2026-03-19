#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Cross-validates Rust and Java term vector output.
#
# 1. Indexes 10 docs with term vectors from both Rust and Java
# 2. Verifies .tvd/.tvx/.tvm file sizes match between the two
# 3. Runs VerifyIndex on the Rust index to confirm Java can read term vectors

set -euo pipefail
cd "$(dirname "$0")/.."

JAVA_DIR="tests/java"
JAVA_INDEX=$(mktemp -d)
RUST_INDEX=$(mktemp -d)
trap 'rm -rf "$JAVA_INDEX" "$RUST_INDEX"' EXIT

echo "=== Rust: index with term vectors ==="
RUST_TV_INDEX_DIR="$RUST_INDEX" cargo test --release --test e2e_term_vectors 2>&1 | tail -3
echo "  Rust index at: $RUST_INDEX"

echo ""
echo "=== Java: index with term vectors ==="
(cd "$JAVA_DIR" && ./gradlew -q indexTermVectors -PindexDir="$JAVA_INDEX")
echo "  Java index at: $JAVA_INDEX"

echo ""
echo "=== Compare .tvd/.tvx/.tvm files ==="

JAVA_TVD=$(find "$JAVA_INDEX" -name "*.tvd" | head -1)
JAVA_TVX=$(find "$JAVA_INDEX" -name "*.tvx" | head -1)
JAVA_TVM=$(find "$JAVA_INDEX" -name "*.tvm" | head -1)
RUST_TVD=$(find "$RUST_INDEX" -name "*.tvd" | head -1)
RUST_TVX=$(find "$RUST_INDEX" -name "*.tvx" | head -1)
RUST_TVM=$(find "$RUST_INDEX" -name "*.tvm" | head -1)

FAIL=0

for ext in TVD TVX TVM; do
    java_var="JAVA_$ext"
    rust_var="RUST_$ext"
    java_file="${!java_var}"
    rust_file="${!rust_var}"

    if [ -z "$java_file" ] || [ -z "$rust_file" ]; then
        echo "FAILED: missing .$ext files (Java=${java_file:-none}, Rust=${rust_file:-none})"
        FAIL=1
        continue
    fi

    java_size=$(wc -c < "$java_file")
    rust_size=$(wc -c < "$rust_file")
    echo "  Java .$ext: $java_size bytes, Rust .$ext: $rust_size bytes"

    if [ "$java_size" -ne "$rust_size" ]; then
        echo "FAILED: .$ext size mismatch (Java=$java_size, Rust=$rust_size)"
        FAIL=1
    fi
done

if [ "$FAIL" -ne 0 ]; then
    exit 1
fi

echo ""
echo "=== Verify Rust index with Java VerifyIndex ==="
(cd "$JAVA_DIR" && ./gradlew -q verifyIndex -PindexDir="$RUST_INDEX" -PdocCount=10)

echo "PASSED"
