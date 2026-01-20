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

# Run benchmarks (small dataset)
bench-small:
    cd benchmarks && make bench-small

# Run benchmarks (medium dataset)
bench-medium:
    cd benchmarks && make bench-medium

# Run benchmarks (large dataset)
bench-large:
    cd benchmarks && make bench-large

# Generate benchmark data
bench-data size="small":
    cd benchmarks && make data-{{size}}

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
    @echo "  just bench-small  - Run benchmarks (small dataset)"
    @echo "  just bench-medium - Run benchmarks (medium dataset)"
    @echo "  just bench-large  - Run benchmarks (large dataset)"
    @echo "  just bench-data   - Generate benchmark data"
    @echo "  just bench-clean  - Clean benchmark data"
