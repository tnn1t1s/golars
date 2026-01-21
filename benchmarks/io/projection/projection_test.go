// Package projection benchmarks column projection during reads.
//
// Tests reading subsets of columns from wide tables:
//   - Read 1 column from 50-col table
//   - Read 5 columns from 50-col table
//   - Read 25 columns from 50-col table
//   - Read all 50 columns (baseline)
//
// This tests whether the reader can skip unused columns efficiently.
package projection

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/benchmarks/io/data"
	ioparquet "github.com/tnn1t1s/golars/io/parquet"
)

var (
	baseDir     string
	cleanupDir  bool
	externalDir string
	parquetPath string
	csvPath     string
)

// Use wide data for projection benchmarks
var benchSize = data.Size{Name: "projection_test", Rows: 100_000, Groups: 1_000}
var benchWidth = data.Wide // 50 columns

func init() {
	externalDir = data.ExternalDir()
	if externalDir != "" {
		baseDir = externalDir
		cleanupDir = false
	} else {
		var err error
		baseDir, err = os.MkdirTemp("", "golars_io_projection_*")
		if err != nil {
			panic(err)
		}
		cleanupDir = true
	}

	parquetPath = projectionParquetPath()
	csvPath = projectionCSVPath()

	if externalDir != "" {
		if !data.FileExists(parquetPath) || !data.FileExists(csvPath) {
			df, err := data.GenerateData(benchSize, benchWidth, 42)
			if err != nil {
				panic(err)
			}
			if err := data.EnsureDir(parquetPath); err != nil {
				panic(err)
			}
			if err := golars.WriteParquet(df, parquetPath); err != nil {
				panic(err)
			}
			if err := golars.WriteCSV(df, csvPath); err != nil {
				panic(err)
			}
		}
	} else {
		df, err := data.GenerateData(benchSize, benchWidth, 42)
		if err != nil {
			panic(err)
		}
		if err := golars.WriteParquet(df, parquetPath); err != nil {
			panic(err)
		}
		if err := golars.WriteCSV(df, csvPath); err != nil {
			panic(err)
		}
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if cleanupDir && baseDir != "" {
		os.RemoveAll(baseDir)
	}
	os.Exit(code)
}

// =============================================================================
// Parquet Column Projection
// =============================================================================

func BenchmarkParquetProjection_1of50(b *testing.B) {
	benchmarkParquetProjection(b, []string{"str_0"})
}

func projectionParquetPath() string {
	if externalDir != "" {
		return data.ExternalPath(baseDir, "projection", "projection_50cols", "parquet")
	}
	return filepath.Join(baseDir, "projection_test.parquet")
}

func projectionCSVPath() string {
	if externalDir != "" {
		return data.ExternalPath(baseDir, "projection", "projection_50cols", "csv")
	}
	return filepath.Join(baseDir, "projection_test.csv")
}

func BenchmarkParquetProjection_5of50(b *testing.B) {
	benchmarkParquetProjection(b, []string{"str_0", "int_0", "int_5", "flt_0", "flt_10"})
}

func BenchmarkParquetProjection_10of50(b *testing.B) {
	cols := []string{
		"str_0", "str_5",
		"int_0", "int_5", "int_10", "int_15",
		"flt_0", "flt_5", "flt_10", "flt_15",
	}
	benchmarkParquetProjection(b, cols)
}

func BenchmarkParquetProjection_25of50(b *testing.B) {
	cols := make([]string, 0, 25)
	for i := 0; i < 5; i++ {
		cols = append(cols, fmt.Sprintf("str_%d", i))
	}
	for i := 0; i < 10; i++ {
		cols = append(cols, fmt.Sprintf("int_%d", i))
	}
	for i := 0; i < 10; i++ {
		cols = append(cols, fmt.Sprintf("flt_%d", i))
	}
	benchmarkParquetProjection(b, cols[:25])
}

func BenchmarkParquetProjection_AllCols(b *testing.B) {
	// Read all columns (baseline)
	b.ResetTimer()
	b.ReportMetric(float64(benchWidth.TotalColumns()), "cols")

	for i := 0; i < b.N; i++ {
		result, err := golars.ReadParquet(parquetPath)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

func benchmarkParquetProjection(b *testing.B, columns []string) {
	opts := ioparquet.ReaderOptions{
		Columns: columns,
	}

	b.ResetTimer()
	b.ReportMetric(float64(len(columns)), "cols")

	for i := 0; i < b.N; i++ {
		result, err := ioparquet.ReadParquetWithOptions(parquetPath, opts)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// =============================================================================
// Parquet vs Full Read Comparison
// =============================================================================

// BenchmarkParquetFullVsProjected compares full read vs projected read
func BenchmarkParquetFullVsProjected(b *testing.B) {
	b.Run("Full50Cols", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result, err := golars.ReadParquet(parquetPath)
			if err != nil {
				b.Fatal(err)
			}
			_ = result
		}
	})

	b.Run("Projected5Cols", func(b *testing.B) {
		opts := ioparquet.ReaderOptions{
			Columns: []string{"str_0", "int_0", "int_5", "flt_0", "flt_10"},
		}
		for i := 0; i < b.N; i++ {
			result, err := ioparquet.ReadParquetWithOptions(parquetPath, opts)
			if err != nil {
				b.Fatal(err)
			}
			_ = result
		}
	})
}
