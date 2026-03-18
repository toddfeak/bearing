#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Cross-validates Rust and Java doc values output for NUMERIC, BINARY, SORTED,
# SORTED_SET, and SORTED_NUMERIC field types.
#
# 1. Indexes 10 docs with all 5 DV types from both Rust and Java
# 2. Verifies .dvm/.dvd file sizes match between the two

set -euo pipefail
cd "$(dirname "$0")/.."

JAVA_DIR="tests/java"
JAVA_INDEX=$(mktemp -d)
RUST_INDEX=$(mktemp -d)
trap 'rm -rf "$JAVA_INDEX" "$RUST_INDEX"' EXIT

echo "=== Rust: index with all doc values types ==="
RUST_DV_INDEX_DIR="$RUST_INDEX" cargo test --release --test e2e_doc_values 2>&1 | tail -3
echo "  Rust index at: $RUST_INDEX"

echo ""
echo "=== Java: index with all doc values types ==="
(cd "$JAVA_DIR" && ./gradlew -q indexDocValues -PindexDir="$JAVA_INDEX")
echo "  Java index at: $JAVA_INDEX"

echo ""
echo "=== Compare .dvm/.dvd files ==="

JAVA_DVM=$(find "$JAVA_INDEX" -name "*.dvm" | head -1)
JAVA_DVD=$(find "$JAVA_INDEX" -name "*.dvd" | head -1)
RUST_DVM=$(find "$RUST_INDEX" -name "*.dvm" | head -1)
RUST_DVD=$(find "$RUST_INDEX" -name "*.dvd" | head -1)

if [ -z "$JAVA_DVM" ] || [ -z "$RUST_DVM" ]; then
    echo "FAILED: missing .dvm files (Java=${JAVA_DVM:-none}, Rust=${RUST_DVM:-none})"
    exit 1
fi
if [ -z "$JAVA_DVD" ] || [ -z "$RUST_DVD" ]; then
    echo "FAILED: missing .dvd files (Java=${JAVA_DVD:-none}, Rust=${RUST_DVD:-none})"
    exit 1
fi

JAVA_DVM_SIZE=$(wc -c < "$JAVA_DVM")
RUST_DVM_SIZE=$(wc -c < "$RUST_DVM")
JAVA_DVD_SIZE=$(wc -c < "$JAVA_DVD")
RUST_DVD_SIZE=$(wc -c < "$RUST_DVD")

echo "  Java .dvm: $JAVA_DVM_SIZE bytes, .dvd: $JAVA_DVD_SIZE bytes"
echo "  Rust .dvm: $RUST_DVM_SIZE bytes, .dvd: $RUST_DVD_SIZE bytes"

FAIL=0
if [ "$JAVA_DVM_SIZE" -ne "$RUST_DVM_SIZE" ]; then
    echo "FAILED: .dvm size mismatch (Java=$JAVA_DVM_SIZE, Rust=$RUST_DVM_SIZE)"
    FAIL=1
fi
if [ "$JAVA_DVD_SIZE" -ne "$RUST_DVD_SIZE" ]; then
    echo "FAILED: .dvd size mismatch (Java=$JAVA_DVD_SIZE, Rust=$RUST_DVD_SIZE)"
    FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
    exit 1
fi

echo "PASSED"
