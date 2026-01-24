package join

import (
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	arrowcompute "github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/benchmarks/data"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

// Global variable to store test data
var testData struct {
	small      *frame.DataFrame
	mediumSafe *frame.DataFrame
	// Additional dataframes for join operations
	smallRight           *frame.DataFrame
	mediumSafeRight      *frame.DataFrame
	smallRightMulti      *frame.DataFrame
	mediumSafeRightMulti *frame.DataFrame
}

var (
	loadOnce sync.Once
	loadErr  error
)

func loadJoinData(b *testing.B) {
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
		testData.smallRight = createUniqueRightDataFrame(small, []string{"id1"})
		testData.mediumSafeRight = createUniqueRightDataFrame(mediumSafe, []string{"id1"})
		testData.smallRightMulti = createUniqueRightDataFrame(small, []string{"id1", "id2"})
		testData.mediumSafeRightMulti = createUniqueRightDataFrame(mediumSafe, []string{"id1", "id2"})
	})
	if loadErr != nil {
		b.Fatal(loadErr)
	}
}

func createUniqueRightDataFrame(df *frame.DataFrame, keys []string) *frame.DataFrame {
	grouped, err := df.GroupBy(keys...)
	if err != nil {
		return df.Head(0)
	}
	counted, err := grouped.Count()
	if err != nil {
		return df.Head(0)
	}
	return counted
}

// BenchmarkInnerJoin - Inner join on single column
func BenchmarkInnerJoin_Small(b *testing.B) {
	loadJoinData(b)
	benchmarkInnerJoin(b, testData.small, testData.smallRight)
}

func BenchmarkInnerJoin_MediumSafe(b *testing.B) {
	loadJoinData(b)
	benchmarkInnerJoin(b, testData.mediumSafe, testData.mediumSafeRight)
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
	loadJoinData(b)
	benchmarkLeftJoin(b, testData.small, testData.smallRight)
}

func BenchmarkLeftJoin_MediumSafe(b *testing.B) {
	loadJoinData(b)
	benchmarkLeftJoin(b, testData.mediumSafe, testData.mediumSafeRight)
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
	loadJoinData(b)
	benchmarkMultiKeyJoin(b, testData.small, testData.smallRightMulti)
}

func BenchmarkMultiKeyJoin_MediumSafe(b *testing.B) {
	loadJoinData(b)
	benchmarkMultiKeyJoin(b, testData.mediumSafe, testData.mediumSafeRightMulti)
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

// BenchmarkMultiKeyJoinIndices isolates index creation cost for multi-key joins.
func BenchmarkMultiKeyJoinIndices_MediumSafe(b *testing.B) {
	loadJoinData(b)
	left := testData.mediumSafe
	right := testData.mediumSafeRightMulti

	leftA, _ := left.Column("id1")
	leftB, _ := left.Column("id2")
	rightA, _ := right.Column("id1")
	rightB, _ := right.Column("id2")

	leftChunkedA, ok := series.ArrowChunked(leftA)
	if !ok {
		b.Fatal("left id1 not Arrow-backed")
	}
	leftChunkedB, ok := series.ArrowChunked(leftB)
	if !ok {
		leftChunkedA.Release()
		b.Fatal("left id2 not Arrow-backed")
	}
	rightChunkedA, ok := series.ArrowChunked(rightA)
	if !ok {
		leftChunkedA.Release()
		leftChunkedB.Release()
		b.Fatal("right id1 not Arrow-backed")
	}
	rightChunkedB, ok := series.ArrowChunked(rightB)
	if !ok {
		leftChunkedA.Release()
		leftChunkedB.Release()
		rightChunkedA.Release()
		b.Fatal("right id2 not Arrow-backed")
	}
	defer leftChunkedA.Release()
	defer leftChunkedB.Release()
	defer rightChunkedA.Release()
	defer rightChunkedB.Release()

	leftChunks := []*arrow.Chunked{leftChunkedA, leftChunkedB}
	rightChunks := []*arrow.Chunked{rightChunkedA, rightChunkedB}

	opts := arrowcompute.DefaultHashJoinOptions()
	opts.JoinType = arrowcompute.InnerJoin

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		leftArr, rightArr, err := arrowcompute.HashJoinChunkedIndicesMulti(leftChunks, rightChunks, opts)
		if err != nil {
			b.Fatal(err)
		}
		_ = leftArr.Len()
		_ = rightArr.Len()
		leftArr.Release()
		rightArr.Release()
	}
}
