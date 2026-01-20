package agg

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

// BenchmarkSum - Sum aggregation on numeric column
func BenchmarkSum_Small(b *testing.B) {
	benchmarkSum(b, testData.small)
}

func BenchmarkSum_Medium(b *testing.B) {
	benchmarkSum(b, testData.medium)
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
	benchmarkMean(b, testData.small)
}

func BenchmarkMean_Medium(b *testing.B) {
	benchmarkMean(b, testData.medium)
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
	benchmarkMin(b, testData.small)
}

func BenchmarkMin_Medium(b *testing.B) {
	benchmarkMin(b, testData.medium)
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
	benchmarkMax(b, testData.small)
}

func BenchmarkMax_Medium(b *testing.B) {
	benchmarkMax(b, testData.medium)
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
	benchmarkStd(b, testData.small)
}

func BenchmarkStd_Medium(b *testing.B) {
	benchmarkStd(b, testData.medium)
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
	benchmarkMedian(b, testData.small)
}

func BenchmarkMedian_Medium(b *testing.B) {
	benchmarkMedian(b, testData.medium)
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
	benchmarkCount(b, testData.small)
}

func BenchmarkCount_Medium(b *testing.B) {
	benchmarkCount(b, testData.medium)
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
	benchmarkVar(b, testData.small)
}

func BenchmarkVar_Medium(b *testing.B) {
	benchmarkVar(b, testData.medium)
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
