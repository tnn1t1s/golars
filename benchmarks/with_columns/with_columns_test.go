// Package with_columns contains exact translations of Polars benchmark tests.
//
// Source: https://github.com/pola-rs/polars/blob/main/py-polars/tests/benchmark/test_with_columns.py
package with_columns

import (
	"fmt"
	"testing"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

// =============================================================================
// Polars test_with_columns.py - Exact Translations
// =============================================================================

// BenchmarkWithColumnsQuadratic matches test_with_columns_quadratic_19503
// Polars:
//
//	num_columns = 10_000
//	data1 = {f"col_{i}": [0] for i in range(num_columns)}
//	df1 = pl.DataFrame(data1)
//	data2 = {f"feature_{i}": [0] for i in range(num_columns)}
//	df2 = pl.DataFrame(data2)
//	df1.with_columns(rhs)  # where rhs = df2
//
// This tests that with_columns does not have O(n^2) complexity
func BenchmarkWithColumnsQuadratic(b *testing.B) {
	numColumns := 10_000

	// Create df1 with col_0, col_1, ... col_9999
	cols1 := make([]series.Series, numColumns)
	for i := 0; i < numColumns; i++ {
		cols1[i] = series.NewInt32Series(fmt.Sprintf("col_%d", i), []int32{0})
	}
	df1, err := frame.NewDataFrame(cols1...)
	if err != nil {
		b.Fatal(err)
	}

	// Create df2 with feature_0, feature_1, ... feature_9999
	cols2 := make([]series.Series, numColumns)
	for i := 0; i < numColumns; i++ {
		cols2[i] = series.NewInt32Series(fmt.Sprintf("feature_%d", i), []int32{0})
	}
	df2, err := frame.NewDataFrame(cols2...)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df1.WithColumnsFrame(df2)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkWithColumnsSmall benchmarks with_columns on smaller dataset
// This is a scaled-down version for faster CI runs
func BenchmarkWithColumnsSmall(b *testing.B) {
	numColumns := 100

	cols1 := make([]series.Series, numColumns)
	for i := 0; i < numColumns; i++ {
		cols1[i] = series.NewInt32Series(fmt.Sprintf("col_%d", i), []int32{0})
	}
	df1, err := frame.NewDataFrame(cols1...)
	if err != nil {
		b.Fatal(err)
	}
	cols2 := make([]series.Series, numColumns)
	for i := 0; i < numColumns; i++ {
		cols2[i] = series.NewInt32Series(fmt.Sprintf("feature_%d", i), []int32{0})
	}
	df2, err := frame.NewDataFrame(cols2...)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df1.WithColumnsFrame(df2)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}
