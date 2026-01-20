// Package io contains exact translations of Polars benchmark tests.
//
// Source: https://github.com/pola-rs/polars/blob/main/py-polars/tests/benchmark/test_io.py
//
// Data configuration matches Polars conftest.py:
//
//	groupby_data = generate_group_by_data(10_000, 100, null_ratio=0.05)
package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
)

// testData matches Polars' groupby_data fixture from conftest.py
// Default: 10,000 rows, 100 groups, 5% null ratio
var testData *frame.DataFrame
var tempDir string

func init() {
	var err error
	testData, err = data.GenerateH2OAIData(data.H2OAISmall)
	if err != nil {
		panic(err)
	}

	// Create temp directory for I/O tests
	tempDir, err = os.MkdirTemp("", "golars_bench_io_*")
	if err != nil {
		panic(err)
	}
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
//
// Note: golars does not have scan_csv (lazy evaluation), so we only test eager read
func BenchmarkWriteReadFilterCSV(b *testing.B) {
	dataPath := filepath.Join(tempDir, "data.csv")

	// Write CSV once
	err := golars.WriteCSV(testData, dataPath)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(dataPath)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Read CSV (eager)
		df, err := golars.ReadCSV(dataPath)
		if err != nil {
			b.Fatal(err)
		}

		// Filter: v2 < 5
		filtered, err := df.Filter(expr.Col("v2").Lt(5))
		if err != nil {
			b.Fatal(err)
		}

		_, _ = filtered.Shape()
	}
}

// =============================================================================
// Additional I/O Benchmarks (not in Polars test_io.py but useful for comparison)
// =============================================================================

// BenchmarkWriteCSV benchmarks CSV write performance
func BenchmarkWriteCSV(b *testing.B) {
	path := filepath.Join(tempDir, "write_bench.csv")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := golars.WriteCSV(testData, path)
		if err != nil {
			b.Fatal(err)
		}
		os.Remove(path)
	}
}

// BenchmarkReadCSV benchmarks CSV read performance
func BenchmarkReadCSV(b *testing.B) {
	path := filepath.Join(tempDir, "read_bench.csv")

	// Write once for reading
	err := golars.WriteCSV(testData, path)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(path)

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
	path := filepath.Join(tempDir, "write_bench.parquet")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := golars.WriteParquet(testData, path)
		if err != nil {
			b.Fatal(err)
		}
		os.Remove(path)
	}
}

// BenchmarkReadParquet benchmarks Parquet read performance
func BenchmarkReadParquet(b *testing.B) {
	path := filepath.Join(tempDir, "read_bench.parquet")

	// Write once for reading
	err := golars.WriteParquet(testData, path)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(path)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df, err := golars.ReadParquet(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = df
	}
}

// TestMain cleans up temp directory
func TestMain(m *testing.M) {
	code := m.Run()
	if tempDir != "" {
		os.RemoveAll(tempDir)
	}
	os.Exit(code)
}
