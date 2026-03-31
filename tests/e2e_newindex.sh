#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# E2E test for the newindex pipeline.
#
# Indexes real documents via the newindex_demo binary under various configurations,
# then validates each with Java VerifyNewindex (content verification) and CheckIndex.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
GRADLE="$SCRIPT_DIR/java/gradlew --project-dir=$SCRIPT_DIR/java --quiet"
DOCS_DIR="$PROJECT_DIR/testdata/docs"
DOC_COUNT=$(find "$DOCS_DIR" -type f | wc -l)

PASSED=0
FAILED=0

# --- Build ---
echo "Building newindex_demo binary..."
cargo build --bin newindex_demo --manifest-path "$PROJECT_DIR/Cargo.toml" 2>&1
DEMO="$PROJECT_DIR/target/debug/newindex_demo"

run_scenario() {
    local name="$1"
    shift
    local args=("$@")

    echo ""
    echo "--- $name ---"

    local idx_dir
    idx_dir="$(mktemp -d)"

    "$DEMO" -docs "$DOCS_DIR" -index "$idx_dir" "${args[@]}"

    # Verify segments file exists
    if [ ! -f "$idx_dir/segments_1" ]; then
        echo "FAILED: segments_1 not found"
        FAILED=$((FAILED + 1))
        rm -rf "$idx_dir"
        return
    fi

    # Java VerifyNewindex validates content (stored fields, terms, norms)
    if $GRADLE -q verifyNewindex -PindexDir="$idx_dir" -PdocCount="$DOC_COUNT" 2>&1; then
        echo "PASSED: $name"
        PASSED=$((PASSED + 1))
    else
        echo "FAILED: $name — Java VerifyNewindex rejected the index"
        FAILED=$((FAILED + 1))
    fi

    rm -rf "$idx_dir"
}

# --- Scenarios ---
run_scenario "Single segment"
run_scenario "Multi-segment" --max-buffered-docs 2
run_scenario "Multi-thread" --threads 2
run_scenario "Multi-thread + multi-segment" --threads 2 --max-buffered-docs 2
run_scenario "Compound file" --compound
run_scenario "Compound + multi-segment" --max-buffered-docs 2 --compound

# --- Summary ---
echo ""
echo "========================================"
echo "  Newindex E2E: $PASSED passed, $FAILED failed"
echo "========================================"

exit $FAILED
