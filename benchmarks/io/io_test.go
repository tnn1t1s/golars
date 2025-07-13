package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/frame"
)

// Global variable to store test data
var testData struct {
	small  *frame.DataFrame
	medium *frame.DataFrame
	tempDir string
}

// init loads the test data once
func init() {
	// Create temp directory for I/O tests
	tempDir, err := os.MkdirTemp("", "golars_bench_io_*")
	if err != nil {
		panic(err)
	}
	testData.tempDir = tempDir

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

// BenchmarkWriteCSV - Write DataFrame to CSV
func BenchmarkWriteCSV_Small(b *testing.B) {
	benchmarkWriteCSV(b, testData.small, "small.csv")
}

func BenchmarkWriteCSV_Medium(b *testing.B) {
	benchmarkWriteCSV(b, testData.medium, "medium.csv")
}

func benchmarkWriteCSV(b *testing.B, df *frame.DataFrame, filename string) {
	path := filepath.Join(testData.tempDir, filename)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := golars.WriteCSV(df, path)
		if err != nil {
			b.Fatal(err)
		}
		
		// Clean up after each iteration
		os.Remove(path)
	}
}

// BenchmarkReadCSV - Read DataFrame from CSV
func BenchmarkReadCSV_Small(b *testing.B) {
	benchmarkReadCSV(b, testData.small, "small_read.csv")
}

func BenchmarkReadCSV_Medium(b *testing.B) {
	benchmarkReadCSV(b, testData.medium, "medium_read.csv")
}

func benchmarkReadCSV(b *testing.B, df *frame.DataFrame, filename string) {
	path := filepath.Join(testData.tempDir, filename)
	
	// Write the file once for reading
	err := golars.WriteCSV(df, path)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(path)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := golars.ReadCSV(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkWriteParquet - Write DataFrame to Parquet
func BenchmarkWriteParquet_Small(b *testing.B) {
	benchmarkWriteParquet(b, testData.small, "small.parquet")
}

func BenchmarkWriteParquet_Medium(b *testing.B) {
	benchmarkWriteParquet(b, testData.medium, "medium.parquet")
}

func benchmarkWriteParquet(b *testing.B, df *frame.DataFrame, filename string) {
	path := filepath.Join(testData.tempDir, filename)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := golars.WriteParquet(df, path)
		if err != nil {
			b.Fatal(err)
		}
		
		// Clean up after each iteration
		os.Remove(path)
	}
}

// BenchmarkReadParquet - Read DataFrame from Parquet
func BenchmarkReadParquet_Small(b *testing.B) {
	benchmarkReadParquet(b, testData.small, "small_read.parquet")
}

func BenchmarkReadParquet_Medium(b *testing.B) {
	benchmarkReadParquet(b, testData.medium, "medium_read.parquet")
}

func benchmarkReadParquet(b *testing.B, df *frame.DataFrame, filename string) {
	path := filepath.Join(testData.tempDir, filename)
	
	// Write the file once for reading
	err := golars.WriteParquet(df, path)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(path)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := golars.ReadParquet(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// Cleanup temp directory
func TestMain(m *testing.M) {
	code := m.Run()
	// Clean up temp directory
	if testData.tempDir != "" {
		os.RemoveAll(testData.tempDir)
	}
	os.Exit(code)
}