# Golars development commands

# Run all tests
test:
    go test ./...

# Run tests with verbose output
test-v:
    go test -v ./...

# Run tests with race detection
test-race:
    go test -race ./...

# Run tests with coverage
test-cover:
    go test -coverprofile=coverage.out ./...
    go tool cover -html=coverage.out -o coverage.html
    @echo "Coverage report: coverage.html"

# Build the project
build:
    go build ./...

# Run go vet
vet:
    go vet ./...

# Run go fmt
fmt:
    go fmt ./...

# Run all checks (fmt, vet, test)
check: fmt vet test

# Generate benchmark data
bench-generate size="medium":
    cd benchmarks && make generate SIZE={{size}}

# Run group-by benchmarks
bench-groupby size="medium":
    cd benchmarks && make benchmark-groupby SIZE={{size}}

# Run all benchmarks
bench-all size="medium":
    cd benchmarks && make benchmark-all SIZE={{size}}

# Clean benchmark data
bench-clean:
    cd benchmarks && make clean

# Show help
help:
    @echo "Golars Development Commands"
    @echo "==========================="
    @echo ""
    @echo "Testing:"
    @echo "  just test        - Run all tests"
    @echo "  just test-v      - Run tests with verbose output"
    @echo "  just test-race   - Run tests with race detection"
    @echo "  just test-cover  - Run tests with coverage report"
    @echo ""
    @echo "Building:"
    @echo "  just build       - Build the project"
    @echo "  just check       - Run fmt, vet, and tests"
    @echo ""
    @echo "Linting:"
    @echo "  just fmt         - Run go fmt"
    @echo "  just vet         - Run go vet"
    @echo ""
    @echo "Benchmarks:"
    @echo "  just bench-generate  - Generate benchmark data (SIZE=small|medium|large)"
    @echo "  just bench-groupby   - Run group-by benchmarks"
    @echo "  just bench-all       - Run all benchmarks"
    @echo "  just bench-clean     - Clean benchmark data"
