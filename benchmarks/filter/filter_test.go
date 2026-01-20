// Package filter contains exact translations of Polars benchmark tests.
//
// Source: https://github.com/pola-rs/polars/blob/main/py-polars/tests/benchmark/test_filter.py
//
// Data configuration matches Polars conftest.py:
//
//	groupby_data = generate_group_by_data(10_000, 100, null_ratio=0.05)
package filter

import (
	"testing"

	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
)

// testData matches Polars' groupby_data fixture from conftest.py
// Default: 10,000 rows, 100 groups, 5% null ratio
var testData *frame.DataFrame

func init() {
	var err error
	testData, err = data.GenerateH2OAIData(data.H2OAISmall)
	if err != nil {
		panic(err)
	}
}

// =============================================================================
// Polars test_filter.py - Exact Translations
// =============================================================================

// BenchmarkFilter1 matches test_filter1
// Polars:
//
//	groupby_data.lazy()
//	.filter(pl.col("id1").eq_missing(pl.lit("id046")))
//	.select(
//	    pl.col("id6").cast(pl.Int64).sum(),
//	    pl.col("v3").sum(),
//	)
//	.collect()
//
// Note: golars uses Eq() instead of eq_missing() which handles nulls differently
// Note: golars Filter returns a DataFrame, then we aggregate columns directly
func BenchmarkFilter1(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Filter
		filtered, err := testData.Filter(expr.Col("id1").Eq(expr.Lit("id046")))
		if err != nil {
			b.Fatal(err)
		}

		// Select and aggregate: id6.cast(Int64).sum() and v3.sum()
		id6, err := filtered.Column("id6")
		if err != nil {
			b.Fatal(err)
		}
		v3, err := filtered.Column("v3")
		if err != nil {
			b.Fatal(err)
		}

		// Polars does cast(Int64).sum() on id6; golars id6 is already Int32, Sum returns float64
		id6Sum := id6.Sum()
		v3Sum := v3.Sum()

		_, _ = id6Sum, v3Sum
	}
}

// BenchmarkFilter2 matches test_filter2
// Polars:
//
//	groupby_data.lazy()
//	.filter(~(pl.col("id1").eq_missing(pl.lit("id046"))))
//	.select(
//	    pl.col("id6").cast(pl.Int64).sum(),
//	    pl.col("v3").sum(),
//	)
//	.collect()
//
// Note: Polars uses ~ (not) on eq_missing; golars uses Ne() for not-equal
func BenchmarkFilter2(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Filter with negation (not equal)
		filtered, err := testData.Filter(expr.Col("id1").Ne(expr.Lit("id046")))
		if err != nil {
			b.Fatal(err)
		}

		// Select and aggregate: id6.cast(Int64).sum() and v3.sum()
		id6, err := filtered.Column("id6")
		if err != nil {
			b.Fatal(err)
		}
		v3, err := filtered.Column("v3")
		if err != nil {
			b.Fatal(err)
		}

		id6Sum := id6.Sum()
		v3Sum := v3.Sum()

		_, _ = id6Sum, v3Sum
	}
}
