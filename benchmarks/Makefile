# Makefile for golars benchmarks

.PHONY: all generate benchmark-all benchmark-groupby benchmark-filter benchmark-join benchmark-sort benchmark-agg benchmark-io compare clean help

# Default data size
SIZE ?= medium

# Benchmark settings
COUNT ?= 3
TIMEOUT ?= 30m

# Directories
DATA_DIR = data
RESULTS_DIR = results

help:
	@echo "golars Benchmark Commands:"
	@echo ""
	@echo "  make generate          - Generate test data"
	@echo "  make benchmark-all     - Run all benchmark suites"
	@echo "  make benchmark-groupby - Run group-by benchmarks"
	@echo "  make benchmark-filter  - Run filter benchmarks"
	@echo "  make benchmark-join    - Run join benchmarks"
	@echo "  make benchmark-sort    - Run sort benchmarks"
	@echo "  make benchmark-agg     - Run aggregation benchmarks"
	@echo "  make benchmark-io      - Run I/O benchmarks"
	@echo "  make compare           - Compare with Polars"
	@echo "  make report            - Generate benchmark report"
	@echo "  make clean             - Clean generated files"
	@echo ""
	@echo "Options:"
	@echo "  SIZE=small|medium|large - Set data size (default: medium)"
	@echo "  COUNT=N                  - Number of benchmark runs (default: 3)"
	@echo ""
	@echo "Examples:"
	@echo "  make generate SIZE=large"
	@echo "  make benchmark-all SIZE=small COUNT=5"

# Create necessary directories
$(RESULTS_DIR):
	mkdir -p $(RESULTS_DIR)

$(DATA_DIR):
	mkdir -p $(DATA_DIR)

# Generate test data
generate: $(DATA_DIR)
	@echo "Generating $(SIZE) H2O.ai dataset..."
	go run cmd/generate/main.go -size $(SIZE) -format parquet

# Individual benchmark suites
benchmark-groupby: $(RESULTS_DIR)
	@echo "Running group-by benchmarks..."
	go test -bench=. ./groupby -benchmem -count=$(COUNT) -timeout=$(TIMEOUT) | tee $(RESULTS_DIR)/groupby_$(SIZE).txt

benchmark-filter: $(RESULTS_DIR)
	@echo "Running filter benchmarks..."
	go test -bench=. ./filter -benchmem -count=$(COUNT) -timeout=$(TIMEOUT) | tee $(RESULTS_DIR)/filter_$(SIZE).txt

benchmark-join: $(RESULTS_DIR)
	@echo "Running join benchmarks..."
	go test -bench=. ./join -benchmem -count=$(COUNT) -timeout=$(TIMEOUT) | tee $(RESULTS_DIR)/join_$(SIZE).txt

benchmark-sort: $(RESULTS_DIR)
	@echo "Running sort benchmarks..."
	go test -bench=. ./sort -benchmem -count=$(COUNT) -timeout=$(TIMEOUT) | tee $(RESULTS_DIR)/sort_$(SIZE).txt

benchmark-agg: $(RESULTS_DIR)
	@echo "Running aggregation benchmarks..."
	go test -bench=. ./agg -benchmem -count=$(COUNT) -timeout=$(TIMEOUT) | tee $(RESULTS_DIR)/agg_$(SIZE).txt

benchmark-io: $(RESULTS_DIR)
	@echo "Running I/O benchmarks..."
	go test -bench=. ./io -benchmem -count=$(COUNT) -timeout=$(TIMEOUT) | tee $(RESULTS_DIR)/io_$(SIZE).txt

# Run all benchmarks
benchmark-all: $(RESULTS_DIR)
	@echo "Running all golars benchmarks with $(SIZE) data..."
	@echo "================================================"
	@$(MAKE) benchmark-agg SIZE=$(SIZE) COUNT=$(COUNT)
	@$(MAKE) benchmark-filter SIZE=$(SIZE) COUNT=$(COUNT)
	@$(MAKE) benchmark-sort SIZE=$(SIZE) COUNT=$(COUNT)
	@$(MAKE) benchmark-groupby SIZE=$(SIZE) COUNT=$(COUNT)
	@$(MAKE) benchmark-join SIZE=$(SIZE) COUNT=$(COUNT)
	@$(MAKE) benchmark-io SIZE=$(SIZE) COUNT=$(COUNT)
	@echo "================================================"
	@echo "All benchmarks completed. Results in $(RESULTS_DIR)/"

# Compare with Polars
compare: $(RESULTS_DIR)
	@echo "Running comparison with Polars..."
	cd compare && python run_polars_benchmarks.py --size $(SIZE)
	python compare/analyze.py --size $(SIZE)

# Generate report from existing results
report: $(RESULTS_DIR)
	@echo "Generating benchmark report..."
	python compare/analyze.py --size $(SIZE) --report $(RESULTS_DIR)/report_$(SIZE).md

# Clean generated files
clean:
	rm -rf $(RESULTS_DIR)
	rm -f $(DATA_DIR)/*.csv $(DATA_DIR)/*.parquet

# Run a quick test to verify benchmarks compile
test:
	go test ./... -short -run=^$$ -bench=. -benchtime=1x
