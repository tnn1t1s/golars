# Contributing to Golars

Thank you for your interest in contributing to Golars! This document provides guidelines for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/golars.git`
3. Create a feature branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Run tests: `go test ./...`
6. Commit your changes: `git commit -m "Add your feature"`
7. Push to your fork: `git push origin feature/your-feature`
8. Open a Pull Request

## Development Setup

### Requirements

- Go 1.21 or higher
- CGO enabled (required for Apache Arrow)

### Building

```bash
go build ./...
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests for a specific package
go test ./frame/...

# Run benchmarks
cd benchmarks && make bench-small
```

## Code Style

- Follow standard Go conventions and idioms
- Run `go fmt` before committing
- Run `go vet` to check for common issues
- Write tests for new functionality
- Add documentation comments for exported types and functions

## Pull Request Guidelines

1. **Keep PRs focused**: One feature or fix per PR
2. **Write clear commit messages**: Describe what changed and why
3. **Include tests**: Add or update tests as needed
4. **Update documentation**: Update README or docs if behavior changes
5. **Ensure CI passes**: All tests must pass before merging

## Reporting Issues

When reporting issues, please include:

- Go version (`go version`)
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant code snippets or error messages

## Code of Conduct

Be respectful and constructive. We welcome contributors of all backgrounds and experience levels.

## License

By contributing to Golars, you agree that your contributions will be licensed under the MIT License.
