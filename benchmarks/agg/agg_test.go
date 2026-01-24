package agg

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

func loadAggData(b *testing.B) {
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

// BenchmarkSum - Sum aggregation on numeric column
func BenchmarkSum_Small(b *testing.B) {
	loadAggData(b)
	benchmarkSum(b, testData.small)
}

func BenchmarkSum_MediumSafe(b *testing.B) {
	loadAggData(b)
	benchmarkSum(b, testData.mediumSafe)
}

func benchmarkSum(b *testing.B, df *frame.DataFrame) {
	col, err := df.Column("v1")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := col.Sum()
		_ = result
	}
}

// BenchmarkMean - Mean aggregation on numeric column
func BenchmarkMean_Small(b *testing.B) {
	loadAggData(b)
	benchmarkMean(b, testData.small)
}

func BenchmarkMean_MediumSafe(b *testing.B) {
	loadAggData(b)
	benchmarkMean(b, testData.mediumSafe)
}

func benchmarkMean(b *testing.B, df *frame.DataFrame) {
	col, err := df.Column("v3")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := col.Mean()
		_ = result
	}
}

// BenchmarkMin - Min aggregation
func BenchmarkMin_Small(b *testing.B) {
	loadAggData(b)
	benchmarkMin(b, testData.small)
}

func BenchmarkMin_MediumSafe(b *testing.B) {
	loadAggData(b)
	benchmarkMin(b, testData.mediumSafe)
}

func benchmarkMin(b *testing.B, df *frame.DataFrame) {
	col, err := df.Column("v1")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := col.Min()
		_ = result
	}
}

// BenchmarkMax - Max aggregation
func BenchmarkMax_Small(b *testing.B) {
	loadAggData(b)
	benchmarkMax(b, testData.small)
}

func BenchmarkMax_MediumSafe(b *testing.B) {
	loadAggData(b)
	benchmarkMax(b, testData.mediumSafe)
}

func benchmarkMax(b *testing.B, df *frame.DataFrame) {
	col, err := df.Column("v1")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := col.Max()
		_ = result
	}
}

// BenchmarkStd - Standard deviation
func BenchmarkStd_Small(b *testing.B) {
	loadAggData(b)
	benchmarkStd(b, testData.small)
}

func BenchmarkStd_MediumSafe(b *testing.B) {
	loadAggData(b)
	benchmarkStd(b, testData.mediumSafe)
}

func benchmarkStd(b *testing.B, df *frame.DataFrame) {
	col, err := df.Column("v3")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := col.Std()
		_ = result
	}
}

// BenchmarkMedian - Median aggregation
func BenchmarkMedian_Small(b *testing.B) {
	loadAggData(b)
	benchmarkMedian(b, testData.small)
}

func BenchmarkMedian_MediumSafe(b *testing.B) {
	loadAggData(b)
	benchmarkMedian(b, testData.mediumSafe)
}

func benchmarkMedian(b *testing.B, df *frame.DataFrame) {
	col, err := df.Column("v3")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := col.Median()
		_ = result
	}
}

// BenchmarkCount - Count aggregation
func BenchmarkCount_Small(b *testing.B) {
	loadAggData(b)
	benchmarkCount(b, testData.small)
}

func BenchmarkCount_MediumSafe(b *testing.B) {
	loadAggData(b)
	benchmarkCount(b, testData.mediumSafe)
}

func benchmarkCount(b *testing.B, df *frame.DataFrame) {
	col, err := df.Column("v1")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := col.Len()
		_ = result
	}
}

// BenchmarkVar - Variance
func BenchmarkVar_Small(b *testing.B) {
	loadAggData(b)
	benchmarkVar(b, testData.small)
}

func BenchmarkVar_MediumSafe(b *testing.B) {
	loadAggData(b)
	benchmarkVar(b, testData.mediumSafe)
}

func benchmarkVar(b *testing.B, df *frame.DataFrame) {
	col, err := df.Column("v3")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := col.Var()
		_ = result
	}
}
