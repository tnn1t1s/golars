// Package join_where contains exact translations of Polars benchmark tests.
//
// Source: https://github.com/pola-rs/polars/blob/main/py-polars/tests/benchmark/test_join_where.py
//
// SKIPPED: golars does not have JoinWhere (inequality join) functionality.
// All benchmarks in this file are skipped to clearly show the feature gap.
package join_where

import (
	"testing"
)

// =============================================================================
// Polars test_join_where.py - Exact Translations
// =============================================================================

// BenchmarkStrictInequalities matches test_strict_inequalities
// Polars:
//
//	east.lazy()
//	.join_where(
//	    west.lazy(),
//	    [pl.col("dur") < pl.col("time"), pl.col("rev") > pl.col("cost")],
//	)
//	.collect()
//
// SKIPPED: golars does not have JoinWhere for inequality conditions
func BenchmarkStrictInequalities(b *testing.B) {
	b.Skip("FEATURE GAP: golars does not have JoinWhere for inequality joins")
}

// BenchmarkNonStrictInequalities matches test_non_strict_inequalities
// Polars:
//
//	east.lazy()
//	.join_where(
//	    west.lazy(),
//	    [pl.col("dur") <= pl.col("time"), pl.col("rev") >= pl.col("cost")],
//	)
//	.collect()
//
// SKIPPED: golars does not have JoinWhere for inequality conditions
func BenchmarkNonStrictInequalities(b *testing.B) {
	b.Skip("FEATURE GAP: golars does not have JoinWhere for inequality joins")
}

// BenchmarkSingleInequality matches test_single_inequality
// Polars:
//
//	east.lazy()
//	.with_columns((pl.col("dur") * 30).alias("scaled_dur"))
//	.join_where(
//	    west.lazy(),
//	    pl.col("scaled_dur") < pl.col("time"),
//	)
//	.collect()
//
// SKIPPED: golars does not have JoinWhere for inequality conditions
func BenchmarkSingleInequality(b *testing.B) {
	b.Skip("FEATURE GAP: golars does not have JoinWhere for inequality joins")
}
