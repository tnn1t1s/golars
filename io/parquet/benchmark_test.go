package parquet

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/io/csv"
	"github.com/tnn1t1s/golars/series"
)

func createBenchmarkDataFrame(size int) *frame.DataFrame {
	ids := make([]int64, size)
	names := make([]string, size)
	scores := make([]float64, size)
	active := make([]bool, size)
	categories := make([]string, size)
	
	for i := 0; i < size; i++ {
		ids[i] = int64(i)
		names[i] = "User" + string(rune('A'+i%26))
		scores[i] = float64(i%100) + 0.5
		active[i] = i%2 == 0
		categories[i] = []string{"A", "B", "C", "D", "E"}[i%5]
	}
	
	df, _ := frame.NewDataFrame(
		series.NewInt64Series("id", ids),
		series.NewStringSeries("name", names),
		series.NewFloat64Series("score", scores),
		series.NewBooleanSeries("active", active),
		series.NewStringSeries("category", categories),
	)
	
	return df
}

func BenchmarkParquetWrite(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	
	for _, size := range sizes {
		df := createBenchmarkDataFrame(size)
		
		b.Run("Size"+string(rune(size)), func(b *testing.B) {
			tmpDir := b.TempDir()
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				filename := filepath.Join(tmpDir, "bench.parquet")
				writer := NewWriter(DefaultWriterOptions())
				err := writer.WriteFile(df, filename)
				if err != nil {
					b.Fatal(err)
				}
				os.Remove(filename)
			}
		})
	}
}

func BenchmarkParquetRead(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	
	for _, size := range sizes {
		df := createBenchmarkDataFrame(size)
		tmpDir := b.TempDir()
		filename := filepath.Join(tmpDir, "bench.parquet")
		
		// Write once
		writer := NewWriter(DefaultWriterOptions())
		err := writer.WriteFile(df, filename)
		if err != nil {
			b.Fatal(err)
		}
		
		b.Run("Size"+string(rune(size)), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := NewReader(DefaultReaderOptions())
				_, err := reader.ReadFile(filename)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkParquetVsCSV(b *testing.B) {
	df := createBenchmarkDataFrame(10000)
	tmpDir := b.TempDir()
	
	parquetFile := filepath.Join(tmpDir, "data.parquet")
	csvFile := filepath.Join(tmpDir, "data.csv")
	
	// Write both formats once
	err := WriteParquet(df, parquetFile)
	if err != nil {
		b.Fatal(err)
	}
	
	csvWriter := csv.NewWriter(nil, csv.DefaultWriteOptions())
	f, _ := os.Create(csvFile)
	csvWriter = csv.NewWriter(f, csv.DefaultWriteOptions())
	err = csvWriter.Write(df)
	f.Close()
	if err != nil {
		b.Fatal(err)
	}
	
	// Compare file sizes
	parquetInfo, _ := os.Stat(parquetFile)
	csvInfo, _ := os.Stat(csvFile)
	b.Logf("Parquet size: %d bytes, CSV size: %d bytes (%.2f%% of CSV)", 
		parquetInfo.Size(), csvInfo.Size(), 
		float64(parquetInfo.Size())/float64(csvInfo.Size())*100)
	
	b.Run("ParquetWrite", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := WriteParquet(df, parquetFile)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	
	b.Run("CSVWrite", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, _ := os.Create(csvFile)
			w := csv.NewWriter(f, csv.DefaultWriteOptions())
			err := w.Write(df)
			f.Close()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	
	b.Run("ParquetRead", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := ReadParquet(parquetFile)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	
	b.Run("CSVRead", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, _ := os.Open(csvFile)
			r := csv.NewReader(f, csv.DefaultReadOptions())
			_, err := r.Read()
			f.Close()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCompressionTypes(b *testing.B) {
	df := createBenchmarkDataFrame(10000)
	tmpDir := b.TempDir()
	
	compressionTypes := []struct {
		name        string
		compression CompressionType
	}{
		{"None", CompressionNone},
		{"Snappy", CompressionSnappy},
		{"Gzip", CompressionGzip},
	}
	
	for _, tc := range compressionTypes {
		b.Run(tc.name, func(b *testing.B) {
			filename := filepath.Join(tmpDir, "bench_"+tc.name+".parquet")
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				writer := NewWriter(WriterOptions{
					Compression: tc.compression,
				})
				err := writer.WriteFile(df, filename)
				if err != nil {
					b.Fatal(err)
				}
			}
			
			// Report file size
			info, _ := os.Stat(filename)
			b.ReportMetric(float64(info.Size()), "bytes")
		})
	}
}

func BenchmarkColumnSelection(b *testing.B) {
	// Create DataFrame with many columns
	size := 10000
	ids := make([]int64, size)
	names := make([]string, size)
	scores := make([]float64, size)
	active := make([]bool, size)
	categories := make([]string, size)
	
	for i := 0; i < size; i++ {
		ids[i] = int64(i)
		names[i] = "User" + string(rune('A'+i%26))
		scores[i] = float64(i%100) + 0.5
		active[i] = i%2 == 0
		categories[i] = []string{"A", "B", "C", "D", "E"}[i%5]
	}
	
	manyCols := []series.Series{
		series.NewInt64Series("id", ids),
		series.NewStringSeries("name", names),
		series.NewFloat64Series("score", scores),
		series.NewBooleanSeries("active", active),
		series.NewStringSeries("category", categories),
	}
	
	// Add more columns
	for i := 0; i < 10; i++ {
		values := make([]float64, size)
		for j := range values {
			values[j] = float64(j * i)
		}
		manyCols = append(manyCols, series.NewFloat64Series("extra"+string(rune('0'+i)), values))
	}
	
	df, _ := frame.NewDataFrame(manyCols...)
	
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "many_columns.parquet")
	
	// Write once
	err := WriteParquet(df, filename)
	if err != nil {
		b.Fatal(err)
	}
	
	b.Run("AllColumns", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := NewReader(DefaultReaderOptions())
			_, err := reader.ReadFile(filename)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	
	b.Run("SelectThreeColumns", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := NewReader(ReaderOptions{
				Columns: []string{"id", "name", "score"},
			})
			_, err := reader.ReadFile(filename)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRowGroupSize(b *testing.B) {
	df := createBenchmarkDataFrame(100000)
	tmpDir := b.TempDir()
	
	rowGroupSizes := []int64{
		10 * 1024 * 1024,   // 10MB
		50 * 1024 * 1024,   // 50MB
		128 * 1024 * 1024,  // 128MB (default)
	}
	
	for _, rgSize := range rowGroupSizes {
		b.Run("RG"+string(rune(rgSize/(1024*1024)))+"MB", func(b *testing.B) {
			filename := filepath.Join(tmpDir, "bench_rg.parquet")
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				writer := NewWriter(WriterOptions{
					Compression:  CompressionSnappy,
					RowGroupSize: rgSize,
				})
				err := writer.WriteFile(df, filename)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}