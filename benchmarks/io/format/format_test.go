// Package format benchmarks Parquet vs CSV performance.
//
// Direct comparison of read/write speeds between formats
// using identical data to isolate format overhead.
package format

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/benchmarks/io/data"
	"github.com/tnn1t1s/golars/frame"
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
	baseDir, err = os.MkdirTemp("", "golars_io_format_*")
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
// Format Comparison - Small Data (10K rows, 9 cols)
// =============================================================================

func BenchmarkFormatComparison_Small(b *testing.B) {
	df, err := data.GenerateH2OStyle(data.Small, 42)
	if err != nil {
		b.Fatal(err)
	}
	runFormatComparison(b, df, "small")
}

// =============================================================================
// Format Comparison - Medium Data (100K rows, 9 cols)
// =============================================================================

func BenchmarkFormatComparison_Medium(b *testing.B) {
	df, err := data.GenerateH2OStyle(data.Medium, 42)
	if err != nil {
		b.Fatal(err)
	}
	runFormatComparison(b, df, "medium")
}

// =============================================================================
// Format Comparison - Wide Data (100K rows, 50 cols)
// =============================================================================

func BenchmarkFormatComparison_Wide(b *testing.B) {
	size := data.Size{Name: "wide_test", Rows: 100_000, Groups: 1_000}
	df, err := data.GenerateData(size, data.Wide, 42)
	if err != nil {
		b.Fatal(err)
	}
	runFormatComparison(b, df, "wide")
}

func runFormatComparison(b *testing.B, df *frame.DataFrame, name string) {
	parquetPath := formatParquetPath(name)
	csvPath := formatCSVPath(name)

	// Write files once
	if externalDir != "" {
		if !data.FileExists(parquetPath) || !data.FileExists(csvPath) {
			if err := data.EnsureDir(parquetPath); err != nil {
				b.Fatal(err)
			}
			if err := golars.WriteParquet(df, parquetPath); err != nil {
				b.Fatal(err)
			}
			if err := golars.WriteCSV(df, csvPath); err != nil {
				b.Fatal(err)
			}
		}
	} else {
		if err := golars.WriteParquet(df, parquetPath); err != nil {
			b.Fatal(err)
		}
		if err := golars.WriteCSV(df, csvPath); err != nil {
			b.Fatal(err)
		}
		defer os.Remove(parquetPath)
		defer os.Remove(csvPath)
	}

	parquetSize, _ := os.Stat(parquetPath)
	csvSize, _ := os.Stat(csvPath)

	b.Run("ParquetWrite", func(b *testing.B) {
		b.ReportMetric(float64(parquetSize.Size()), "file_bytes")
		for i := 0; i < b.N; i++ {
			golars.WriteParquet(df, parquetPath)
		}
	})

	b.Run("CSVWrite", func(b *testing.B) {
		b.ReportMetric(float64(csvSize.Size()), "file_bytes")
		for i := 0; i < b.N; i++ {
			golars.WriteCSV(df, csvPath)
		}
	})

	b.Run("ParquetRead", func(b *testing.B) {
		b.ReportMetric(float64(parquetSize.Size()), "file_bytes")
		for i := 0; i < b.N; i++ {
			result, _ := golars.ReadParquet(parquetPath)
			_ = result
		}
	})

	b.Run("CSVRead", func(b *testing.B) {
		b.ReportMetric(float64(csvSize.Size()), "file_bytes")
		for i := 0; i < b.N; i++ {
			result, _ := golars.ReadCSV(csvPath)
			_ = result
		}
	})
}

// =============================================================================
// Compression Comparison (Parquet only)
// =============================================================================

func BenchmarkParquetCompression(b *testing.B) {
	df, err := data.GenerateH2OStyle(data.Medium, 42)
	if err != nil {
		b.Fatal(err)
	}

	compressions := []struct {
		name string
		comp golars.CompressionType
	}{
		{"None", golars.CompressionNone},
		{"Snappy", golars.CompressionSnappy},
		{"Gzip", golars.CompressionGzip},
		{"Zstd", golars.CompressionZstd},
	}

	for _, c := range compressions {
		path := filepath.Join(baseDir, "compress_"+c.name+".parquet")
		if err := data.EnsureDir(path); err != nil {
			b.Fatal(err)
		}

		b.Run("Write_"+c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				golars.WriteParquet(df, path, golars.WithCompression(c.comp))
			}
			fi, _ := os.Stat(path)
			b.ReportMetric(float64(fi.Size()), "file_bytes")
		})

		// Write once for read benchmark
		golars.WriteParquet(df, path, golars.WithCompression(c.comp))

		b.Run("Read_"+c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				result, _ := golars.ReadParquet(path)
				_ = result
			}
		})

		os.Remove(path)
	}
}

func formatParquetPath(name string) string {
	if externalDir != "" {
		return data.ExternalPath(baseDir, "format", name, "parquet")
	}
	return filepath.Join(baseDir, name+".parquet")
}

func formatCSVPath(name string) string {
	if externalDir != "" {
		return data.ExternalPath(baseDir, "format", name, "csv")
	}
	return filepath.Join(baseDir, name+".csv")
}
