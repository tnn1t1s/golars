#!/bin/bash

# Run comparison between golars and Polars benchmarks
# Usage: ./run_comparison.sh [query] [size]
# Example: ./run_comparison.sh q1 medium

set -e

QUERY=${1:-"all"}
SIZE=${2:-"medium"}
POLARS_DIR="../../../polars/py-polars"
RESULTS_DIR="../results"

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "Running benchmark comparison for Query: $QUERY, Size: $SIZE"
echo "================================================"

# Run Polars benchmarks
echo "Running Polars benchmarks..."
if [ "$QUERY" = "all" ]; then
    cd "$POLARS_DIR"
    pytest tests/benchmark/test_group_by.py -v --benchmark-only --benchmark-json="$RESULTS_DIR/polars_groupby_${SIZE}.json"
else
    cd "$POLARS_DIR"
    pytest tests/benchmark/test_group_by.py::test_groupby_h2oai_${QUERY} -v --benchmark-only --benchmark-json="$RESULTS_DIR/polars_${QUERY}_${SIZE}.json"
fi
cd - > /dev/null

echo ""
echo "Running golars benchmarks..."
# Run golars benchmarks
if [ "$QUERY" = "all" ]; then
    go test -bench=".*_${SIZE^}" ../groupby -benchmem -count=5 > "$RESULTS_DIR/golars_groupby_${SIZE}.txt"
else
    go test -bench="BenchmarkGroupBy${QUERY^^}_${SIZE^}" ../groupby -benchmem -count=5 > "$RESULTS_DIR/golars_${QUERY}_${SIZE}.txt"
fi

echo ""
echo "Results saved to:"
echo "  - Polars: $RESULTS_DIR/polars_${QUERY}_${SIZE}.json"
echo "  - golars: $RESULTS_DIR/golars_${QUERY}_${SIZE}.txt"
echo ""
echo "To analyze results, run:"
echo "  python analyze.py --polars $RESULTS_DIR/polars_${QUERY}_${SIZE}.json --golars $RESULTS_DIR/golars_${QUERY}_${SIZE}.txt"