package filter

import (
	"testing"

	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
)

// Global variable to store test data
var testData struct {
	small  *frame.DataFrame
	medium *frame.DataFrame
	large  *frame.DataFrame
}

// init loads the test data once
func init() {
	// Load small dataset
	small, err := data.GenerateH2OAIData(data.H2OAISmall)
	if err != nil {
		panic(err)
	}
	testData.small = small

	// Load medium dataset
	medium, err := data.GenerateH2OAIData(data.H2OAIMedium)
	if err != nil {
		panic(err)
	}
	testData.medium = medium
}

// BenchmarkFilterSimple - Simple filter on single column
// Polars: df.filter(pl.col("v1") > 5)
func BenchmarkFilterSimple_Small(b *testing.B) {
	benchmarkFilterSimple(b, testData.small)
}

func BenchmarkFilterSimple_Medium(b *testing.B) {
	benchmarkFilterSimple(b, testData.medium)
}

func benchmarkFilterSimple(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Filter(expr.Col("v1").Gt(5))
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkFilterCompound - Compound filter with AND
// Polars: df.filter((pl.col("v1") > 5) & (pl.col("v2") < 10))
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
			expr.Col("v1").Gt(5).And(
				expr.Col("v2").Lt(10),
			),
		)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkFilterString - Filter on string column
// Polars: df.filter(pl.col("id1") == "id010")
func BenchmarkFilterString_Small(b *testing.B) {
	benchmarkFilterString(b, testData.small)
}

func BenchmarkFilterString_Medium(b *testing.B) {
	benchmarkFilterString(b, testData.medium)
}

func benchmarkFilterString(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Filter(expr.Col("id1").Eq(expr.Lit("id010")))
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkFilterOr - Filter with OR condition
// Polars: df.filter((pl.col("v1") > 4) | (pl.col("v2") < 5))
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
			expr.Col("v1").Gt(4).Or(expr.Col("v2").Lt(5)),
		)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}
