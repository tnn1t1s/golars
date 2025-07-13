#!/bin/bash
# Minimal benchmark comparison - runs only Q1-Q6 which are fully implemented

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Default parameters
SIZE=${1:-small}
QUERIES="Q1 Q2 Q3 Q4 Q5 Q6"

echo -e "${BLUE}Running minimal H2O.ai benchmarks (Q1-Q6) for size: $SIZE${NC}"

# Ensure results directory exists
mkdir -p results

# Generate test data if it doesn't exist
DATA_FILE="../data/h2oai_${SIZE}.parquet"
if [ ! -f "$DATA_FILE" ]; then
    echo -e "${BLUE}Generating test data...${NC}"
    (cd .. && go run cmd/generate/main.go -size $SIZE -format parquet)
fi

# Run golars benchmarks
echo -e "${GREEN}Running golars benchmarks...${NC}"
(cd ../groupby && go test -bench="BenchmarkGroupBy(Q[1-6])_${SIZE^}" -benchtime=3x -run=^$ | tee ../../compare/results/golars_minimal_${SIZE}.txt)

# Run Polars benchmarks
echo -e "${GREEN}Running Polars benchmarks...${NC}"
python3 run_polars_minimal.py $SIZE | tee results/polars_minimal_${SIZE}.txt

# Analyze results
echo -e "${GREEN}Analyzing results...${NC}"
python3 analyze_minimal_results.py $SIZE

echo -e "${BLUE}Comparison complete! Results saved in results/ directory${NC}"