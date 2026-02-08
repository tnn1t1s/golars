# frame -- DataFrame

## Purpose

The central data structure. A DataFrame is a collection of named, equal-length
Series with a schema. Most user-facing operations live here.

## Key Design Decisions

**Struct layout.** `DataFrame` holds `[]series.Series`, a `*datatypes.Schema`,
a `height int`, and a `sync.RWMutex`. All methods that read take RLock; methods
that produce new DataFrames do not mutate the original (immutable-by-convention).

**Expression evaluation.** `eval.go` contains `evaluateExpr(expr.Expr)` which
is the core expression evaluator. It type-switches on expression types:
- `ColumnExpr` -- returns the named column
- `LiteralExpr` -- creates a constant-filled Series
- `BinaryExpr` -- evaluates left and right, applies the operator element-wise
- `UnaryExpr` -- evaluates the operand, applies the operator
- `CastExpr` -- evaluates then casts
- `AliasExpr` -- evaluates then renames
- `window.Expr` -- delegates to window evaluation

This evaluator is used by `WithColumn`, `Select`, `Filter`, and other methods.

**Filter.** Two implementations:
- `filter.go` -- Go-native: evaluates the expression to a boolean Series, then
  gathers matching row indices
- `filter_arrow.go` -- Arrow-accelerated: uses Arrow compute kernels for
  comparison and boolean operations, then applies an Arrow take

**Join.** `join.go` defines `JoinType` constants and the dispatch logic.
`join_arrow.go` implements hash-join using Arrow arrays for key matching.
The pattern: build a hash map from the smaller table's join keys, probe with
the larger table, collect matching index pairs, then gather columns from both
tables. Suffix handling appends "_right" to duplicate column names.

**GroupBy.** `groupby.go` creates a `group.GroupBy` (from `internal/group`)
and wraps the result. The `GroupByWrapper` type provides a fluent API
(`.Agg()`, `.Sum()`, `.Count()`, etc.) that delegates to the group engine.

**Sort.** `sort.go` computes sort indices using `series.ArgSort` on the
specified columns (with multi-column tiebreaking), then gathers all columns
by those indices via `series.Take`.

**Stats.** `stats.go` provides Describe, Corr, Cov (basic statistics).
`stats_advanced.go` adds Quantile, Skew, Kurtosis, RollingMean,
RollingStd, EWM (exponentially weighted moving) functions.

**Reshape operations:**
- `pivot.go` -- pivot (long to wide)
- `melt.go` -- melt/unpivot (wide to long)
- `reshape.go` -- stack, unstack
- `explode.go` -- explode list columns into rows
- `concat.go` -- vertical/horizontal concatenation of DataFrames

**Advanced joins:**
- `iejoin.go` -- inequality join (join on < / > / <= / >= conditions)
- `merge_asof.go` -- as-of join (match nearest key, used in time series)
- `rolling_join.go` -- rolling window join

**Missing data:** `missing.go` handles FillNull, DropNull, ForwardFill,
BackwardFill, and Interpolate for missing values.

**Cumulative:** `cumulative.go` provides CumSum, CumProd, CumMin, CumMax.

**Compare:** `compare.go` provides element-wise DataFrame comparison.
