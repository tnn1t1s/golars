// Package filter contains filter operation benchmarks.
//
// Polars Comparable: BenchmarkFilter1, BenchmarkFilter2
// These match: https://github.com/pola-rs/polars/blob/main/py-polars/tests/benchmark/test_filter.py
//
// Golars Additional: BenchmarkFilterNumeric, BenchmarkFilterCompound, BenchmarkFilterOr
// These are additional benchmarks not in the Polars test suite.
package filter

import (
	"testing"

	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
)

var testData struct {
	small  *frame.DataFrame
	medium *frame.DataFrame
}

func init() {
	small, err := data.GenerateH2OAIData(data.H2OAISmall)
	if err != nil {
		panic(err)
	}
	testData.small = small

	medium, err := data.GenerateH2OAIData(data.H2OAIMediumSafe)
	if err != nil {
		panic(err)
	}
	testData.medium = medium
}

// =============================================================================
// Polars-Comparable Benchmarks (match test_filter.py)
// =============================================================================

// BenchmarkFilter1 - Matches Polars test_filter1
// Polars: df.lazy().filter(pl.col("id1").eq_missing(pl.lit("id046"))).select(...).collect()
// Note: Polars also does aggregation after filter; golars tests filter only
func BenchmarkFilter1_Small(b *testing.B) {
	benchmarkFilter1(b, testData.small)
}

func BenchmarkFilter1_Medium(b *testing.B) {
	benchmarkFilter1(b, testData.medium)
}

func benchmarkFilter1(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Filter(expr.Col("id1").Eq(expr.Lit("id046")))
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkFilter2 - Matches Polars test_filter2
// Polars: df.lazy().filter(~(pl.col("id1").eq_missing(pl.lit("id046")))).select(...).collect()
func BenchmarkFilter2_Small(b *testing.B) {
	benchmarkFilter2(b, testData.small)
}

func BenchmarkFilter2_Medium(b *testing.B) {
	benchmarkFilter2(b, testData.medium)
}

func benchmarkFilter2(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Filter(expr.Col("id1").Ne(expr.Lit("id046")))
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// =============================================================================
// Additional Benchmarks (not in Polars test suite)
// =============================================================================

// BenchmarkFilterNumeric - Filter on numeric column
func BenchmarkFilterNumeric_Small(b *testing.B) {
	benchmarkFilterNumeric(b, testData.small)
}

func BenchmarkFilterNumeric_Medium(b *testing.B) {
	benchmarkFilterNumeric(b, testData.medium)
}

func benchmarkFilterNumeric(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Filter(expr.Col("v1").Gt(3))
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkFilterCompound - Compound filter with AND
func BenchmarkFilterCompound_Small(b *testing.B) {
	benchmarkFilterCompound(b, testData.small)
}

func BenchmarkFilterCompound_Medium(b *testing.B) {
	benchmarkFilterCompound(b, testData.medium)
}

func benchmarkFilterCompound(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Filter(
			expr.Col("v1").Gt(3).And(expr.Col("v2").Lt(10)),
		)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkFilterOr - Filter with OR condition
func BenchmarkFilterOr_Small(b *testing.B) {
	benchmarkFilterOr(b, testData.small)
}

func BenchmarkFilterOr_Medium(b *testing.B) {
	benchmarkFilterOr(b, testData.medium)
}

func benchmarkFilterOr(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Filter(
			expr.Col("v1").Gt(3).Or(expr.Col("v2").Lt(5)),
		)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}
