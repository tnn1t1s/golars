// Package size benchmarks IO performance across different data sizes.
//
// Size ladder: small (10K), medium (100K), large (1M), huge (10M) rows
// Uses H2O.ai style 9-column schema for consistency with groupby benchmarks.
package size

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/benchmarks/io/data"
)

var (
	baseDir     string
	cleanupDir  bool
	externalDir string
)

func init() {
	externalDir = data.ExternalDir()
	if externalDir != "" {
		baseDir = externalDir
		cleanupDir = false
		return
	}

	var err error
	baseDir, err = os.MkdirTemp("", "golars_io_size_*")
	if err != nil {
		panic(err)
	}
	cleanupDir = true
}

func TestMain(m *testing.M) {
	code := m.Run()
	if cleanupDir && baseDir != "" {
		os.RemoveAll(baseDir)
	}
	os.Exit(code)
}

// =============================================================================
// Parquet Read - Size Ladder
// =============================================================================

func BenchmarkParquetRead_Small(b *testing.B) {
	benchmarkParquetRead(b, data.Small)
}

func BenchmarkParquetRead_Medium(b *testing.B) {
	benchmarkParquetRead(b, data.Medium)
}

func BenchmarkParquetRead_Large(b *testing.B) {
	benchmarkParquetRead(b, data.Large)
}

func BenchmarkParquetRead_Huge(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping huge benchmark in short mode")
	}
	benchmarkParquetRead(b, data.Huge)
}

func benchmarkParquetRead(b *testing.B, size data.Size) {
	path := parquetReadPath(size)
	if externalDir != "" {
		if !data.FileExists(path) {
			df, err := data.GenerateH2OStyle(size, 42)
			if err != nil {
				b.Fatal(err)
			}
			if err := data.EnsureDir(path); err != nil {
				b.Fatal(err)
			}
			if err := golars.WriteParquet(df, path); err != nil {
				b.Fatal(err)
			}
		}
	} else {
		df, err := data.GenerateH2OStyle(size, 42)
		if err != nil {
			b.Fatal(err)
		}
		if err := golars.WriteParquet(df, path); err != nil {
			b.Fatal(err)
		}
		defer os.Remove(path)
	}

	b.ResetTimer()
	b.ReportMetric(float64(size.Rows), "rows")

	for i := 0; i < b.N; i++ {
		result, err := golars.ReadParquet(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// =============================================================================
// Parquet Write - Size Ladder
// =============================================================================

func BenchmarkParquetWrite_Small(b *testing.B) {
	benchmarkParquetWrite(b, data.Small)
}

func BenchmarkParquetWrite_Medium(b *testing.B) {
	benchmarkParquetWrite(b, data.Medium)
}

func BenchmarkParquetWrite_Large(b *testing.B) {
	benchmarkParquetWrite(b, data.Large)
}

func BenchmarkParquetWrite_Huge(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping huge benchmark in short mode")
	}
	benchmarkParquetWrite(b, data.Huge)
}

func benchmarkParquetWrite(b *testing.B, size data.Size) {
	df, err := data.GenerateH2OStyle(size, 42)
	if err != nil {
		b.Fatal(err)
	}

	path := filepath.Join(baseDir, size.Name+"_write.parquet")
	if err := data.EnsureDir(path); err != nil {
		b.Fatal(err)
	}
	defer os.Remove(path)

	b.ResetTimer()
	b.ReportMetric(float64(size.Rows), "rows")

	for i := 0; i < b.N; i++ {
		if err := golars.WriteParquet(df, path); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// CSV Read - Size Ladder
// =============================================================================

func BenchmarkCSVRead_Small(b *testing.B) {
	benchmarkCSVRead(b, data.Small)
}

func BenchmarkCSVRead_Medium(b *testing.B) {
	benchmarkCSVRead(b, data.Medium)
}

func BenchmarkCSVRead_Large(b *testing.B) {
	benchmarkCSVRead(b, data.Large)
}

func benchmarkCSVRead(b *testing.B, size data.Size) {
	path := csvReadPath(size)
	if externalDir != "" {
		if !data.FileExists(path) {
			df, err := data.GenerateH2OStyle(size, 42)
			if err != nil {
				b.Fatal(err)
			}
			if err := data.EnsureDir(path); err != nil {
				b.Fatal(err)
			}
			if err := golars.WriteCSV(df, path); err != nil {
				b.Fatal(err)
			}
		}
	} else {
		df, err := data.GenerateH2OStyle(size, 42)
		if err != nil {
			b.Fatal(err)
		}
		if err := golars.WriteCSV(df, path); err != nil {
			b.Fatal(err)
		}
		defer os.Remove(path)
	}

	b.ResetTimer()
	b.ReportMetric(float64(size.Rows), "rows")

	for i := 0; i < b.N; i++ {
		result, err := golars.ReadCSV(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// =============================================================================
// CSV Write - Size Ladder
// =============================================================================

func BenchmarkCSVWrite_Small(b *testing.B) {
	benchmarkCSVWrite(b, data.Small)
}

func BenchmarkCSVWrite_Medium(b *testing.B) {
	benchmarkCSVWrite(b, data.Medium)
}

func BenchmarkCSVWrite_Large(b *testing.B) {
	benchmarkCSVWrite(b, data.Large)
}

func benchmarkCSVWrite(b *testing.B, size data.Size) {
	df, err := data.GenerateH2OStyle(size, 42)
	if err != nil {
		b.Fatal(err)
	}

	path := filepath.Join(baseDir, size.Name+"_write.csv")
	if err := data.EnsureDir(path); err != nil {
		b.Fatal(err)
	}
	defer os.Remove(path)

	b.ResetTimer()
	b.ReportMetric(float64(size.Rows), "rows")

	for i := 0; i < b.N; i++ {
		if err := golars.WriteCSV(df, path); err != nil {
			b.Fatal(err)
		}
	}
}

func parquetReadPath(size data.Size) string {
	if externalDir != "" {
		return data.ExternalPath(baseDir, "size", size.Name, "parquet")
	}
	return filepath.Join(baseDir, size.Name+"_read.parquet")
}

func csvReadPath(size data.Size) string {
	if externalDir != "" {
		return data.ExternalPath(baseDir, "size", size.Name, "csv")
	}
	return filepath.Join(baseDir, size.Name+"_read.csv")
}
