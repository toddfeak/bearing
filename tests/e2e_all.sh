#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Runs all e2e tests. Exits non-zero if any test fails.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TESTS=(
    "$SCRIPT_DIR/e2e_indexfiles.sh"
    "$SCRIPT_DIR/e2e_verify_impacts.sh"
    "$SCRIPT_DIR/e2e_verify_tim_compression.sh"
    "$SCRIPT_DIR/e2e_doc_values.sh"
)

PASSED=0
FAILED=0
FAILURES=()

for test in "${TESTS[@]}"; do
    name="$(basename "$test")"
    echo ""
    echo "========================================"
    echo "  $name"
    echo "========================================"
    if "$test" "$@"; then
        PASSED=$((PASSED + 1))
    else
        FAILED=$((FAILED + 1))
        FAILURES+=("$name")
    fi
done

echo ""
echo "========================================"
echo "  E2E Summary: $PASSED passed, $FAILED failed"
if [ $FAILED -gt 0 ]; then
    echo "  Failed:"
    for f in "${FAILURES[@]}"; do
        echo "    - $f"
    done
fi
echo "========================================"

exit $FAILED
