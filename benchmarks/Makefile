# Makefile for golars benchmarks

.PHONY: all generate benchmark-groupby benchmark-all compare clean help

# Default data size
SIZE ?= medium

# Directories
DATA_DIR = data
RESULTS_DIR = results

help:
	@echo "golars Benchmark Commands:"
	@echo "  make generate          - Generate test data"
	@echo "  make benchmark-groupby - Run group-by benchmarks"
	@echo "  make benchmark-all     - Run all benchmarks"
	@echo "  make compare           - Compare with Polars"
	@echo "  make clean             - Clean generated files"
	@echo ""
	@echo "Options:"
	@echo "  SIZE=small|medium|large - Set data size (default: medium)"
	@echo ""
	@echo "Examples:"
	@echo "  make generate SIZE=large"
	@echo "  make benchmark-groupby SIZE=small"

# Create necessary directories
$(RESULTS_DIR):
	mkdir -p $(RESULTS_DIR)

$(DATA_DIR):
	mkdir -p $(DATA_DIR)

# Generate test data
generate: $(DATA_DIR)
	@echo "Generating $(SIZE) H2O.ai dataset..."
	go run cmd/generate/main.go -size $(SIZE) -format parquet

# Run group-by benchmarks
benchmark-groupby: $(RESULTS_DIR)
	@echo "Running group-by benchmarks with $(SIZE) data..."
	go test -bench=".*_$(shell echo $(SIZE) | sed 's/\b\(.\)/\u\1/')" ./groupby -benchmem -count=5 | tee $(RESULTS_DIR)/golars_groupby_$(SIZE).txt

# Run all benchmarks
benchmark-all: benchmark-groupby
	@echo "All benchmarks completed."

# Compare with Polars
compare: $(RESULTS_DIR)
	@echo "Running comparison with Polars..."
	cd compare && ./run_comparison.sh all $(SIZE)

# Clean generated files
clean:
	rm -rf $(RESULTS_DIR)
	rm -f $(DATA_DIR)/*.csv $(DATA_DIR)/*.parquet

# Run a quick test
test:
	go test ./... -short