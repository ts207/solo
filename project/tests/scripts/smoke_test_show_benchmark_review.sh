#!/bin/bash
set -e

# Smoke test for show_benchmark_review.py
echo "Running smoke test for show_benchmark_review.py..."

# Find the latest smoke run in /tmp
TARGET_DIR=$(ls -td /tmp/benchmark_smoke_research_family_v1_* 2>/dev/null | head -n 1)

if [ -z "$TARGET_DIR" ]; then
    echo "Error: No smoke target record found. Run maintenance cycle dry-run first."
    exit 1
fi

PATH_TO_REVIEW="$TARGET_DIR/benchmark_review.json"

if [ ! -f "$PATH_TO_REVIEW" ]; then
    echo "Error: Review file not found at $PATH_TO_REVIEW"
    exit 1
fi

PYTHONPATH=. python3 project/scripts/show_benchmark_review.py --path "$PATH_TO_REVIEW" > /tmp/smoke_test_output.txt

if grep -q "BENCHMARK REVIEW: research_family_v1" /tmp/smoke_test_output.txt; then
    echo "Smoke test passed: Matrix ID found."
else
    echo "Smoke test failed: Matrix ID not found."
    cat /tmp/smoke_test_output.txt
    exit 1
fi

if grep -q "CERTIFICATION: " /tmp/smoke_test_output.txt; then
    echo "Smoke test passed: Certification status found."
else
    echo "Smoke test failed: Certification status not found."
    cat /tmp/smoke_test_output.txt
    exit 1
fi

echo "show_benchmark_review.py smoke test COMPLETED successfully."
