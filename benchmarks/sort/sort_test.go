package sort

import (
	"sync"
	"testing"

	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/frame"
)

var testData struct {
	small      *frame.DataFrame
	mediumSafe *frame.DataFrame
}

var (
	loadOnce sync.Once
	loadErr  error
)

func loadSortData(b *testing.B) {
	b.Helper()
	loadOnce.Do(func() {
		small, err := data.LoadH2OAI("small")
		if err != nil {
			loadErr = err
			return
		}
		mediumSafe, err := data.LoadH2OAI("medium-safe")
		if err != nil {
			loadErr = err
			return
		}
		testData.small = small
		testData.mediumSafe = mediumSafe
	})
	if loadErr != nil {
		b.Fatal(loadErr)
	}
}

// BenchmarkSortSingleInt - Sort by single integer column
func BenchmarkSortSingleInt_Small(b *testing.B) {
	loadSortData(b)
	benchmarkSortSingleInt(b, testData.small)
}

func BenchmarkSortSingleInt_MediumSafe(b *testing.B) {
	loadSortData(b)
	benchmarkSortSingleInt(b, testData.mediumSafe)
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
	loadSortData(b)
	benchmarkSortSingleString(b, testData.small)
}

func BenchmarkSortSingleString_MediumSafe(b *testing.B) {
	loadSortData(b)
	benchmarkSortSingleString(b, testData.mediumSafe)
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
	loadSortData(b)
	benchmarkSortMultiColumn(b, testData.small)
}

func BenchmarkSortMultiColumn_MediumSafe(b *testing.B) {
	loadSortData(b)
	benchmarkSortMultiColumn(b, testData.mediumSafe)
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
	loadSortData(b)
	benchmarkSortDescending(b, testData.small)
}

func BenchmarkSortDescending_MediumSafe(b *testing.B) {
	loadSortData(b)
	benchmarkSortDescending(b, testData.mediumSafe)
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
	loadSortData(b)
	benchmarkSortFloat(b, testData.small)
}

func BenchmarkSortFloat_MediumSafe(b *testing.B) {
	loadSortData(b)
	benchmarkSortFloat(b, testData.mediumSafe)
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
