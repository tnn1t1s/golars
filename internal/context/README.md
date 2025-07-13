# Golars Context Documentation

This directory contains comprehensive documentation about the Golars project implementation for context preservation.

## Directory Structure

- **architecture/** - Core design decisions and patterns
- **implementation/** - Details about each implemented component
- **api/** - API documentation and usage patterns
- **testing/** - Test coverage and benchmark results
- **examples/** - Code examples and usage patterns
- **next-steps/** - Roadmap and implementation guides for remaining features

## Quick Start for Next Agent

1. Read `architecture/overview.md` first to understand the design
2. Check `implementation/status.md` to see what's completed
3. Review `api/public-api.md` for the user-facing interface
4. See `next-steps/roadmap.md` for what needs to be done

## Key Files

- All core types are in the respective packages (datatypes/, series/, frame/, expr/, compute/)
- Main export file is `golars.go` at the root
- Examples are in `cmd/example/`
- Tests follow Go convention (`*_test.go` files)

## Current State

- ✅ Core data structures (ChunkedArray, Series, DataFrame)
- ✅ Type system with Arrow integration
- ✅ Expression DSL
- ✅ Filtering operations
- ✅ Basic compute kernels
- ❌ GroupBy operations
- ❌ Join operations
- ❌ Sorting
- ❌ I/O (CSV, Parquet)
- ❌ Lazy evaluation