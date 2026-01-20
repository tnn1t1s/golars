# Lazy Evaluation Implementation Plan

This document defines a refactor-first plan to build a new arena-based lazy
evaluation system for Golars. It complements
`docs/design/ARENA_BASED_LAZY_EVALUATION.md` and prioritizes clean, modern
architecture over backward compatibility.

## Goals

- Replace the current eager-first execution model with a lazy-first pipeline.
- Use an arena-based expression AST to enable fast analysis and optimization.
- Separate logical planning from physical execution.
- Provide a clean, Go-idiomatic public API (no legacy shims required).

## Non-Goals

- Preserve the current `expr` API or lazy implementation from commit `8f200ee`.
- Reuse previous string-based optimizers.
- Maintain API compatibility with legacy eager operations.

## Guiding Principles

- Prefer correctness and plan transparency over premature micro-optimizations.
- Minimize cross-layer coupling (expr vs plan vs execution).
- Keep optimizers composable and testable in isolation.
- Validate each phase with explicit acceptance gates.

## Implementation Phases

### Phase 0: Scope and Acceptance Gates (1 week)

Deliverables:
- Minimal query subset: `Scan -> Filter -> Select -> GroupBy -> Agg -> Collect`
- Schema inference rules and error behaviors
- Baseline benchmarks for the subset

Acceptance:
- Query subset executes end-to-end with deterministic results
- Schema inference covers basic numeric, string, and bool types

### Phase 1: Expression Arena (Weeks 1-2)

Deliverables:
- `NodeID` and `ExprArena` with interned strings
- `NodeKind` and typed node payloads
- Visitor traversal and rewrite utilities
- Expression analysis helpers (column collection, literal folding hooks)

Acceptance:
- Arena supports add/get/transform in O(1)
- Visitor can traverse and rewrite trees without allocations
- Unit tests for arena, interner, and traversal

### Phase 2: Logical Planning (Weeks 3-4)

Deliverables:
- `LogicalPlan` interface and plan nodes
- Schema derivation per node
- Evaluation context for filter/agg/selection

Acceptance:
- Plan builds and validates for the subset queries
- Schema derivation is deterministic and cached

### Phase 3: Optimization Pipeline (Weeks 5-6)

Deliverables:
- Optimizer interface + pipeline
- Predicate pushdown and projection pushdown
- Constant folding and simple expression rewrites

Acceptance:
- Optimizer produces correct, simplified plans
- Metrics confirm reduced plan node count where applicable

### Phase 4: Physical Execution (Weeks 7-8)

Deliverables:
- Physical expression compiler
- Physical plan nodes for the subset
- Execution runtime and memory strategy

Acceptance:
- Subset queries run without fallback to eager operations
- Memory profile is stable across repeated runs

### Phase 5: API Integration (Weeks 9-10)

Deliverables:
- `LazyFrame` API and builder utilities
- `Collect` and `Explain` outputs
- Minimal eager wrapper (optional)

Acceptance:
- API coverage for the subset queries
- `Explain` output matches logical/physical plan structure

### Phase 6: Advanced Features (Weeks 11-12)

Deliverables:
- Window functions, type coercion, wildcard expansion
- Additional optimizers (CSE, null propagation)
- Benchmark-focused tuning

Acceptance:
- Feature parity with current eager equivalents
- Performance meets or exceeds eager for target queries

## Architecture Decisions (Initial)

- Use arena-allocated nodes to avoid pointer-heavy ASTs.
- Store string data via interner to reduce duplication.
- Represent nodes with typed payloads and explicit children lists.
- Keep logical plans immutable; use structural sharing on rewrites.

## Risk Areas

- Type system integration and schema inference complexity.
- Execution engine scope creep if not gated by subset queries.
- Optimizer correctness when merging predicates across joins.

## Success Criteria (12-week horizon)

- Lazy queries for the subset run end-to-end with stable schemas.
- Optimizer yields measurable plan improvements (node count and elapsed time).
- Benchmarks show <= 2x vs Polars on medium groupby queries, with a clear
  roadmap for further gains.
