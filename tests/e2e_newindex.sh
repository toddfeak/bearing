#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# E2E test for the newindex pipeline.
#
# Writes stored-fields-only indexes via the newindex_demo binary
# under various configurations, then validates each with Java CheckIndex.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
GRADLE="$SCRIPT_DIR/java/gradlew --project-dir=$SCRIPT_DIR/java --quiet"

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

    "$DEMO" "$idx_dir" "${args[@]}"

    # Verify segments file exists
    if [ ! -f "$idx_dir/segments_1" ]; then
        echo "FAILED: segments_1 not found"
        FAILED=$((FAILED + 1))
        rm -rf "$idx_dir"
        return
    fi

    # Java CheckIndex validates segment structure
    if $GRADLE -q runJava -PmainClass=CheckIndex -Pargs="$idx_dir" 2>&1; then
        echo "PASSED: $name"
        PASSED=$((PASSED + 1))
    else
        echo "FAILED: $name — Java CheckIndex rejected the index"
        FAILED=$((FAILED + 1))
    fi

    rm -rf "$idx_dir"
}

# --- Scenarios ---
run_scenario "Single segment (default)" --doc-count 10
run_scenario "Multi-segment (max-buffered-docs)" --doc-count 10 --max-buffered-docs 3
run_scenario "Multi-thread" --doc-count 10 --threads 2
run_scenario "Multi-thread + multi-segment" --doc-count 10 --threads 2 --max-buffered-docs 3
run_scenario "Compound file" --doc-count 10 --compound
run_scenario "Compound + multi-segment" --doc-count 10 --max-buffered-docs 3 --compound
run_scenario "Text fields (single segment)" --doc-count 10 --text-fields
run_scenario "Text fields (multi-segment)" --doc-count 10 --text-fields --max-buffered-docs 3
run_scenario "Text fields (multi-thread)" --doc-count 10 --text-fields --threads 2

# --- Summary ---
echo ""
echo "========================================"
echo "  Newindex E2E: $PASSED passed, $FAILED failed"
echo "========================================"

exit $FAILED
