package sort

import (
	"testing"

	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/frame"
)

var testData struct {
	small  *frame.DataFrame
	medium *frame.DataFrame
}

func init() {
	small, err := data.GenerateH2OAIData(data.H2OAISmall)
	if err != nil {
		panic(err)
	}
	testData.small = small

	medium, err := data.GenerateH2OAIData(data.H2OAIMediumSafe)
	if err != nil {
		panic(err)
	}
	testData.medium = medium
}

// BenchmarkSortSingleInt - Sort by single integer column
func BenchmarkSortSingleInt_Small(b *testing.B) {
	benchmarkSortSingleInt(b, testData.small)
}

func BenchmarkSortSingleInt_Medium(b *testing.B) {
	benchmarkSortSingleInt(b, testData.medium)
}

func benchmarkSortSingleInt(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Sort("v1")
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkSortSingleString - Sort by single string column
func BenchmarkSortSingleString_Small(b *testing.B) {
	benchmarkSortSingleString(b, testData.small)
}

func BenchmarkSortSingleString_Medium(b *testing.B) {
	benchmarkSortSingleString(b, testData.medium)
}

func benchmarkSortSingleString(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Sort("id1")
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkSortMultiColumn - Sort by multiple columns
func BenchmarkSortMultiColumn_Small(b *testing.B) {
	benchmarkSortMultiColumn(b, testData.small)
}

func BenchmarkSortMultiColumn_Medium(b *testing.B) {
	benchmarkSortMultiColumn(b, testData.medium)
}

func benchmarkSortMultiColumn(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Sort("id1", "id2", "v1")
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkSortDescending - Sort descending
func BenchmarkSortDescending_Small(b *testing.B) {
	benchmarkSortDescending(b, testData.small)
}

func BenchmarkSortDescending_Medium(b *testing.B) {
	benchmarkSortDescending(b, testData.medium)
}

func benchmarkSortDescending(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.SortDesc("v1")
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkSortFloat - Sort by float column
func BenchmarkSortFloat_Small(b *testing.B) {
	benchmarkSortFloat(b, testData.small)
}

func BenchmarkSortFloat_Medium(b *testing.B) {
	benchmarkSortFloat(b, testData.medium)
}

func benchmarkSortFloat(b *testing.B, df *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := df.Sort("v3")
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}
