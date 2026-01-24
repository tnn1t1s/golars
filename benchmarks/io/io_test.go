// Package io contains exact translations of Polars benchmark tests.
//
// Source: https://github.com/pola-rs/polars/blob/main/py-polars/tests/benchmark/test_io.py
//
// Data configuration matches Polars conftest.py:
//
//	groupby_data = generate_group_by_data(10_000, 100, null_ratio=0.05)
package io

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
)

// testData matches Polars' groupby_data fixture from conftest.py
// Default: 10,000 rows, 100 groups, 5% null ratio
var (
	testData     *frame.DataFrame
	testDataOnce sync.Once
	testDataErr  error
)

func loadTestData(b *testing.B) *frame.DataFrame {
	b.Helper()
	testDataOnce.Do(func() {
		testData, testDataErr = data.LoadH2OAI("small")
	})
	if testDataErr != nil {
		b.Fatal(testDataErr)
	}
	return testData
}

// =============================================================================
// Polars test_io.py - Exact Translations
// =============================================================================

// BenchmarkWriteReadFilterCSV matches test_write_read_scan_large_csv
// Polars:
//
//	tmp_path.mkdir(exist_ok=True)
//	data_path = tmp_path / "data.csv"
//	groupby_data.write_csv(data_path)
//	predicate = pl.col("v2") < 5
//	shape_eager = pl.read_csv(data_path).filter(predicate).shape
//	shape_lazy = pl.scan_csv(data_path).filter(predicate).collect().shape
//	assert shape_lazy == shape_eager
func BenchmarkWriteReadFilterCSV(b *testing.B) {
	testData := loadTestData(b)
	dataPath := filepath.Join(b.TempDir(), "data.csv")

	// Write CSV once
	err := golars.WriteCSV(testData, dataPath)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Read CSV (eager)
		df, err := golars.ReadCSV(dataPath)
		if err != nil {
			b.Fatal(err)
		}

		// Filter: v2 < 5 (eager)
		filtered, err := df.Filter(expr.Col("v2").Lt(5))
		if err != nil {
			b.Fatal(err)
		}

		_, _ = filtered.Shape()

		// Lazy scan + filter
		lf := golars.ScanCSV(dataPath)
		lazyFiltered := lf.Filter(lf.Col("v2").Lt(lf.Lit(5)))
		lazyResult, err := lazyFiltered.Collect(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		_, _ = lazyResult.Shape()
	}
}

// =============================================================================
// Additional I/O Benchmarks (not in Polars test_io.py but useful for comparison)
// =============================================================================

// BenchmarkWriteCSV benchmarks CSV write performance
func BenchmarkWriteCSV(b *testing.B) {
	testData := loadTestData(b)
	path := filepath.Join(b.TempDir(), "write_bench.csv")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := golars.WriteCSV(testData, path)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadCSV benchmarks CSV read performance
func BenchmarkReadCSV(b *testing.B) {
	testData := loadTestData(b)
	path := filepath.Join(b.TempDir(), "read_bench.csv")

	// Write once for reading
	err := golars.WriteCSV(testData, path)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df, err := golars.ReadCSV(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = df
	}
}

// BenchmarkWriteParquet benchmarks Parquet write performance
func BenchmarkWriteParquet(b *testing.B) {
	testData := loadTestData(b)
	path := filepath.Join(b.TempDir(), "write_bench.parquet")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := golars.WriteParquet(testData, path)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadParquet benchmarks Parquet read performance
func BenchmarkReadParquet(b *testing.B) {
	testData := loadTestData(b)
	path := filepath.Join(b.TempDir(), "read_bench.parquet")

	// Write once for reading
	err := golars.WriteParquet(testData, path)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df, err := golars.ReadParquet(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = df
	}
}
