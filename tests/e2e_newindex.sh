#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# E2E test for the newindex pipeline.
#
# Writes a stored-fields-only index via the newindex_demo binary,
# then validates it with Java VerifyIndex.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
GRADLE="$SCRIPT_DIR/java/gradlew --project-dir=$SCRIPT_DIR/java --quiet"

INDEX_DIR="$(mktemp -d)"
trap 'rm -rf "$INDEX_DIR"' EXIT

DOC_COUNT=10

# --- Build ---
echo "Building newindex_demo binary..."
cargo build --bin newindex_demo --manifest-path "$PROJECT_DIR/Cargo.toml" 2>&1
DEMO="$PROJECT_DIR/target/debug/newindex_demo"

# --- Index ---
echo ""
echo "Writing index to $INDEX_DIR..."
"$DEMO" "$INDEX_DIR"

# --- Verify files exist ---
echo ""
echo "Checking index files..."
EXPECTED_FILES="segments_1 _0.si _0.fnm _0.fdt _0.fdm _0.fdx"
for f in $EXPECTED_FILES; do
    if [ ! -f "$INDEX_DIR/$f" ]; then
        echo "FAILED: expected file '$f' not found"
        exit 1
    fi
    echo "  found: $f ($(stat --format='%s' "$INDEX_DIR/$f") bytes)"
done

# --- Java CheckIndex ---
echo ""
echo "Running Java CheckIndex..."
if $GRADLE -q runJava -PmainClass=CheckIndex -Pargs="$INDEX_DIR" 2>&1; then
    echo ""
    echo "SUCCESS: Java CheckIndex passed"
else
    echo ""
    echo "FAILED: Java CheckIndex rejected the index"
    exit 1
fi
