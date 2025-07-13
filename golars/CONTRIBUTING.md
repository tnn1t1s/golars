# Contributing to Golars

Thank you for your interest in contributing to Golars! This guide will help you get started.

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct: be respectful, inclusive, and constructive in all interactions.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/golars.git
   cd golars
   ```
3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/davidpalaitis/golars.git
   ```
4. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Setup

### Prerequisites
- Go 1.21 or higher
- Apache Arrow Go library
- Make (optional, for convenience commands)

### Building
```bash
go build ./...
```

### Testing
```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run specific package tests
go test ./frame/...

# Run benchmarks
go test -bench=. ./...
```

## Making Changes

### Code Style

1. **Follow Go conventions**:
   - Use `gofmt` to format your code
   - Follow [Effective Go](https://go.dev/doc/effective_go) guidelines
   - Use meaningful variable and function names

2. **Golars-specific conventions**:
   - Immutable operations: methods should return new objects
   - Thread-safe: use appropriate locking (see existing patterns)
   - Null handling: be explicit about null behavior
   - Type safety: leverage generics where appropriate

3. **Documentation**:
   - Add godoc comments for all exported types and functions
   - Include examples in comments where helpful
   - Update relevant documentation files

### Adding Features

1. **Check existing issues** to avoid duplicate work
2. **Discuss major changes** by opening an issue first
3. **Follow the architecture**:
   - ChunkedArray for storage
   - Series for type-erased columns
   - DataFrame for table operations
   - Expressions for computations
4. **Write tests** for your feature:
   - Unit tests for core functionality
   - Integration tests if needed
   - Benchmarks for performance-critical code
5. **Update documentation**:
   - API docs in code
   - Update IMPLEMENTATION_SUMMARY.md if adding major features
   - Add examples to cmd/example/ if appropriate

### Testing Guidelines

1. **Test coverage**: Aim for >80% coverage
2. **Test cases**: Include:
   - Normal cases
   - Edge cases (empty data, single element)
   - Null handling
   - Error cases
3. **Benchmarks**: Add for performance-critical paths
4. **Examples**: Working examples in cmd/example/

### Performance Considerations

1. **Minimize allocations**: Reuse buffers where possible
2. **Use Arrow builders**: For efficient array construction
3. **Vectorize operations**: Process data in batches
4. **Profile your code**:
   ```bash
   go test -cpuprofile=cpu.prof -bench=YourBenchmark
   go tool pprof cpu.prof
   ```

## Submitting Changes

1. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add new feature X"
   ```
   
   Use conventional commits:
   - `feat:` new feature
   - `fix:` bug fix
   - `docs:` documentation changes
   - `test:` test additions/changes
   - `perf:` performance improvements
   - `refactor:` code refactoring

2. **Keep your fork updated**:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

3. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

4. **Create a Pull Request**:
   - Use a clear, descriptive title
   - Reference any related issues
   - Describe what changed and why
   - Include test results if relevant

## Pull Request Process

1. **CI checks**: Ensure all tests pass
2. **Code review**: Address reviewer feedback
3. **Squash commits** if requested
4. **Merge**: Maintainers will merge when ready

## Areas for Contribution

### High Priority
- Window functions (rolling operations, rank functions)
- Parquet I/O support
- String manipulation functions
- DateTime operations

### Medium Priority
- Additional query optimizers
- Performance improvements
- More aggregation functions
- Database connectors

### Documentation
- Improve API documentation
- Add more examples
- Write tutorials
- Performance guides

### Testing
- Increase test coverage
- Add fuzzing tests
- Benchmark new features
- Cross-platform testing

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions or ideas
- Check existing issues and discussions first

Thank you for contributing to Golars!