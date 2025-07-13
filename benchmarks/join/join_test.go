package join

import (
	"testing"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/frame"
)

// Global variable to store test data
var testData struct {
	small  *frame.DataFrame
	medium *frame.DataFrame
	// Additional dataframes for join operations
	smallRight  *frame.DataFrame
	mediumRight *frame.DataFrame
}

// init loads the test data once
func init() {
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

	// Create right-side dataframes for joins
	// Using a subset of the data with some modifications
	testData.smallRight = createRightDataFrame(small, 5000)
	testData.mediumRight = createRightDataFrame(medium, 500000)
}

func createRightDataFrame(df *frame.DataFrame, nRows int) *frame.DataFrame {
	// Take a subset and modify slightly to create join data
	subset := df.Head(nRows)
	
	// For simplicity, just return the subset
	// In a real benchmark, we'd want to modify the data
	return subset
}

// BenchmarkInnerJoin - Inner join on single column
func BenchmarkInnerJoin_Small(b *testing.B) {
	benchmarkInnerJoin(b, testData.small, testData.smallRight)
}

func BenchmarkInnerJoin_Medium(b *testing.B) {
	benchmarkInnerJoin(b, testData.medium, testData.mediumRight)
}

func benchmarkInnerJoin(b *testing.B, left, right *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := left.Join(right, "id1", golars.InnerJoin)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkLeftJoin - Left join on single column
func BenchmarkLeftJoin_Small(b *testing.B) {
	benchmarkLeftJoin(b, testData.small, testData.smallRight)
}

func BenchmarkLeftJoin_Medium(b *testing.B) {
	benchmarkLeftJoin(b, testData.medium, testData.mediumRight)
}

func benchmarkLeftJoin(b *testing.B, left, right *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := left.Join(right, "id1", golars.LeftJoin)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkMultiKeyJoin - Join on multiple columns
func BenchmarkMultiKeyJoin_Small(b *testing.B) {
	benchmarkMultiKeyJoin(b, testData.small, testData.smallRight)
}

func BenchmarkMultiKeyJoin_Medium(b *testing.B) {
	benchmarkMultiKeyJoin(b, testData.medium, testData.mediumRight)
}

func benchmarkMultiKeyJoin(b *testing.B, left, right *frame.DataFrame) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := left.JoinOn(right, []string{"id1", "id2"}, []string{"id1", "id2"}, golars.InnerJoin)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}