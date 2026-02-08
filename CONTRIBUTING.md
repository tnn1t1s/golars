# Contributing to Golars

This guide is written for **autonomous agents and agent swarms**. If you are a
human operator configuring an agent run, the same rules apply.

---

## The Challenge

Every function and method body in this repository has been replaced with
`panic("not implemented")`. Your job: **make the test suite pass.**

You get:
- Full type signatures and interfaces (the spec)
- All 84 test files, unmodified (the acceptance criteria)
- Design notes for every package (`design/`)
- Test utilities (`testutil/`) for assertions, fixtures, and helpers
- Working reference packages: `expr/`, `internal/datatypes/`, `internal/parallel/`

You implement: ~1,237 stubbed function bodies across ~50 source files.

---

## Branch Naming Convention

All submissions MUST follow this naming pattern:

```
agent/<agent-name>/<run-id>
```

| Segment | Rules | Examples |
|---------|-------|---------|
| `agent/` | Literal prefix, always required | |
| `<agent-name>` | Lowercase alphanumeric + hyphens, identifies the system | `claude-swarm`, `gpt4-solo`, `devin-v2` |
| `<run-id>` | Unique run identifier, lowercase alphanumeric + hyphens | `run-001`, `2024-06-15-a`, `attempt-3` |

Examples:
```
agent/claude-swarm/run-001
agent/gpt4-solo/2024-06-15-a
agent/devin-v2/attempt-3
agent/custom-rag/baseline
```

Branches that do not match `agent/<name>/<id>` will be rejected by CI.

---

## Rules

### Do Not Modify

These files and directories are **immutable**. CI will reject any changes:

- All `*_test.go` files (84 files)
- `testutil/` (test helpers)
- `expr/` (expression AST, already working)
- `internal/datatypes/` (type definitions, already working)
- `internal/parallel/` (parallel utilities, already working)
- `benchmarks/` (benchmark tests and data)
- `go.mod`, `go.sum`, `go.work`, `go.work.sum`
- `design/` (design documentation)
- `.github/` (CI workflows)

### You May Only Edit

- Source files (`.go`, non-test) in these packages:
  - `internal/chunked/`
  - `series/`
  - `internal/group/`
  - `internal/window/` (except `function.go`, `spec.go`, `api.go`, `doc.go`)
  - `internal/datetime/`
  - `internal/strings/`
  - `frame/`
  - `io/`, `io/csv/`, `io/json/`, `io/parquet/`
  - `lazy/`
  - Root: `dataframe_auto.go`, `series_auto.go`

### You May Not

- Add new dependencies to `go.mod`
- Create new packages or directories
- Modify test files in any way
- Delete any file

---

## Getting Started

```bash
# Verify build (should pass)
go build ./...

# Run all tests (should all fail with "not implemented")
go test ./...

# Run a single package
go test ./series/ -v

# Run with race detector
go test -race ./...
```

### Recommended Order

Follow the dependency graph bottom-up:

1. `internal/chunked/` (storage layer, no internal deps)
2. `series/` (depends on chunked)
3. `internal/group/` (depends on series)
4. `internal/window/` (depends on series)
5. `internal/datetime/` (depends on series)
6. `internal/strings/` (depends on series)
7. `frame/` (depends on all above)
8. `io/` (depends on frame, series)
9. `lazy/` (depends on frame)
10. Root auto-constructors (depends on series, frame)

Read `design/README.md` for architecture hints per package.

---

## Scoring

Each run is evaluated by CI on these dimensions:

| Dimension | What It Measures |
|-----------|-----------------|
| **Build** | Does `go build ./...` succeed? |
| **Correctness** | Fraction of test cases passing |
| **Coverage** | Which packages have fully passing test suites |
| **No Tampering** | Were any protected files modified? |
| **Race Safety** | Do tests pass under `-race`? |

The benchmark report workflow generates a scorecard as a PR comment and
a JSON artifact for programmatic analysis.

---

## Submission

1. Fork the repository
2. Create a branch following the naming convention: `agent/<name>/<run-id>`
3. Implement the stubs
4. Push and open a Pull Request against `main`
5. CI will validate integrity and generate your scorecard

---

## License

By contributing, you agree that your contributions will be licensed under the
MIT License.
