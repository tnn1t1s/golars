// Package width benchmarks IO performance across different column widths.
//
// Width ladder:
//   - narrow: 3 columns (1 str, 1 int, 1 float)
//   - medium: 9 columns (H2O.ai style)
//   - wide: 50 columns (10 str, 20 int, 20 float)
//   - very_wide: 200 columns (50 str, 100 int, 50 float)
//
// Uses fixed row count (100K) to isolate width effects.
package width

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

// Fixed size for width benchmarks
var benchSize = data.Size{Name: "width_test", Rows: 100_000, Groups: 1_000}

func init() {
	externalDir = data.ExternalDir()
	if externalDir != "" {
		baseDir = externalDir
		cleanupDir = false
		return
	}

	var err error
	baseDir, err = os.MkdirTemp("", "golars_io_width_*")
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
// Parquet Read - Width Ladder
// =============================================================================

func BenchmarkParquetRead_Narrow(b *testing.B) {
	benchmarkParquetRead(b, data.Narrow)
}

func BenchmarkParquetRead_Medium(b *testing.B) {
	benchmarkParquetRead(b, data.Medium9)
}

func BenchmarkParquetRead_Wide(b *testing.B) {
	benchmarkParquetRead(b, data.Wide)
}

func BenchmarkParquetRead_VeryWide(b *testing.B) {
	benchmarkParquetRead(b, data.VeryWide)
}

func benchmarkParquetRead(b *testing.B, width data.Width) {
	path := parquetReadPath(width)
	if externalDir != "" {
		if !data.FileExists(path) {
			df, err := data.GenerateData(benchSize, width, 42)
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
		df, err := data.GenerateData(benchSize, width, 42)
		if err != nil {
			b.Fatal(err)
		}
		if err := golars.WriteParquet(df, path); err != nil {
			b.Fatal(err)
		}
		defer os.Remove(path)
	}

	fi, _ := os.Stat(path)

	b.ResetTimer()
	b.ReportMetric(float64(width.TotalColumns()), "cols")
	b.ReportMetric(float64(fi.Size()), "bytes")

	for i := 0; i < b.N; i++ {
		result, err := golars.ReadParquet(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// =============================================================================
// Parquet Write - Width Ladder
// =============================================================================

func BenchmarkParquetWrite_Narrow(b *testing.B) {
	benchmarkParquetWrite(b, data.Narrow)
}

func BenchmarkParquetWrite_Medium(b *testing.B) {
	benchmarkParquetWrite(b, data.Medium9)
}

func BenchmarkParquetWrite_Wide(b *testing.B) {
	benchmarkParquetWrite(b, data.Wide)
}

func BenchmarkParquetWrite_VeryWide(b *testing.B) {
	benchmarkParquetWrite(b, data.VeryWide)
}

func benchmarkParquetWrite(b *testing.B, width data.Width) {
	df, err := data.GenerateData(benchSize, width, 42)
	if err != nil {
		b.Fatal(err)
	}

	path := filepath.Join(baseDir, width.Name+"_write.parquet")
	if err := data.EnsureDir(path); err != nil {
		b.Fatal(err)
	}
	defer os.Remove(path)

	b.ResetTimer()
	b.ReportMetric(float64(width.TotalColumns()), "cols")

	for i := 0; i < b.N; i++ {
		if err := golars.WriteParquet(df, path); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// CSV Read - Width Ladder
// =============================================================================

func BenchmarkCSVRead_Narrow(b *testing.B) {
	benchmarkCSVRead(b, data.Narrow)
}

func BenchmarkCSVRead_Medium(b *testing.B) {
	benchmarkCSVRead(b, data.Medium9)
}

func BenchmarkCSVRead_Wide(b *testing.B) {
	benchmarkCSVRead(b, data.Wide)
}

func BenchmarkCSVRead_VeryWide(b *testing.B) {
	benchmarkCSVRead(b, data.VeryWide)
}

func benchmarkCSVRead(b *testing.B, width data.Width) {
	path := csvReadPath(width)
	if externalDir != "" {
		if !data.FileExists(path) {
			df, err := data.GenerateData(benchSize, width, 42)
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
		df, err := data.GenerateData(benchSize, width, 42)
		if err != nil {
			b.Fatal(err)
		}
		if err := golars.WriteCSV(df, path); err != nil {
			b.Fatal(err)
		}
		defer os.Remove(path)
	}

	fi, _ := os.Stat(path)

	b.ResetTimer()
	b.ReportMetric(float64(width.TotalColumns()), "cols")
	b.ReportMetric(float64(fi.Size()), "bytes")

	for i := 0; i < b.N; i++ {
		result, err := golars.ReadCSV(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// =============================================================================
// CSV Write - Width Ladder
// =============================================================================

func BenchmarkCSVWrite_Narrow(b *testing.B) {
	benchmarkCSVWrite(b, data.Narrow)
}

func BenchmarkCSVWrite_Medium(b *testing.B) {
	benchmarkCSVWrite(b, data.Medium9)
}

func BenchmarkCSVWrite_Wide(b *testing.B) {
	benchmarkCSVWrite(b, data.Wide)
}

func BenchmarkCSVWrite_VeryWide(b *testing.B) {
	benchmarkCSVWrite(b, data.VeryWide)
}

func benchmarkCSVWrite(b *testing.B, width data.Width) {
	df, err := data.GenerateData(benchSize, width, 42)
	if err != nil {
		b.Fatal(err)
	}

	path := filepath.Join(baseDir, width.Name+"_write.csv")
	if err := data.EnsureDir(path); err != nil {
		b.Fatal(err)
	}
	defer os.Remove(path)

	b.ResetTimer()
	b.ReportMetric(float64(width.TotalColumns()), "cols")

	for i := 0; i < b.N; i++ {
		if err := golars.WriteCSV(df, path); err != nil {
			b.Fatal(err)
		}
	}
}

func parquetReadPath(width data.Width) string {
	if externalDir != "" {
		return data.ExternalPath(baseDir, "width", width.Name, "parquet")
	}
	return filepath.Join(baseDir, width.Name+"_read.parquet")
}

func csvReadPath(width data.Width) string {
	if externalDir != "" {
		return data.ExternalPath(baseDir, "width", width.Name, "csv")
	}
	return filepath.Join(baseDir, width.Name+"_read.csv")
}
