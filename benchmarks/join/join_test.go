package join

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowcompute "github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		leftComposite, err := buildCompositeJoinChunked([]series.Series{leftA, leftB})
		if err != nil {
			b.Fatal(err)
		}
		rightComposite, err := buildCompositeJoinChunked([]series.Series{rightA, rightB})
		if err != nil {
			b.Fatal(err)
		}
		opts := arrowcompute.DefaultHashJoinOptions()
		opts.JoinType = arrowcompute.InnerJoin
		leftArr, rightArr, err := arrowcompute.HashJoinChunkedIndices(leftComposite, rightComposite, opts)
		leftComposite.Release()
		rightComposite.Release()
		if err != nil {
			b.Fatal(err)
		}
		leftIdx, err := arrowArrayToInts(leftArr)
		if err != nil {
			b.Fatal(err)
		}
		rightIdx, err := arrowArrayToInts(rightArr)
		if err != nil {
			b.Fatal(err)
		}
		leftArr.Release()
		rightArr.Release()
		_ = leftIdx
		_ = rightIdx
	}
}

func buildCompositeJoinChunked(cols []series.Series) (*arrow.Chunked, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("no join columns provided")
	}
	mem := arrowcompute.DefaultHashJoinOptions().Allocator
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	keyArrs := make([]arrow.Array, len(cols))
	for i, col := range cols {
		chunked, ok := series.ArrowChunked(col)
		if !ok {
			return nil, fmt.Errorf("column %s is not Arrow-backed", col.Name())
		}
		arr, err := array.Concatenate(chunked.Chunks(), mem)
		chunked.Release()
		if err != nil {
			return nil, err
		}
		keyArrs[i] = arr
	}
	defer func() {
		for _, arr := range keyArrs {
			arr.Release()
		}
	}()

	colsSpec := make([]joinKeyColumn, len(keyArrs))
	for i, arr := range keyArrs {
		col, err := newJoinKeyColumn(arr)
		if err != nil {
			return nil, err
		}
		if arr.Len() != keyArrs[0].Len() {
			return nil, fmt.Errorf("join key columns length mismatch")
		}
		colsSpec[i] = col
	}

	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.Reserve(keyArrs[0].Len())
	for i := 0; i < keyArrs[0].Len(); i++ {
		hasNull := false
		for _, col := range colsSpec {
			if col.isNull(i) {
				hasNull = true
				break
			}
		}
		if hasNull {
			builder.AppendNull()
			continue
		}
		var sb strings.Builder
		for _, col := range colsSpec {
			col.appendEncoded(&sb, i)
		}
		builder.Append(sb.String())
	}

	arr := builder.NewArray()
	chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	arr.Release()
	return chunked, nil
}

type joinKeyColumn struct {
	kind string
	i64  *array.Int64
	i32  *array.Int32
	str  *array.String
}

func newJoinKeyColumn(arr arrow.Array) (joinKeyColumn, error) {
	switch typed := arr.(type) {
	case *array.Int64:
		return joinKeyColumn{kind: "i64", i64: typed}, nil
	case *array.Int32:
		return joinKeyColumn{kind: "i32", i32: typed}, nil
	case *array.String:
		return joinKeyColumn{kind: "str", str: typed}, nil
	default:
		return joinKeyColumn{}, fmt.Errorf("unsupported join key type %s", arr.DataType().String())
	}
}

func (c joinKeyColumn) isNull(i int) bool {
	switch c.kind {
	case "i64":
		return c.i64.IsNull(i)
	case "i32":
		return c.i32.IsNull(i)
	case "str":
		return c.str.IsNull(i)
	default:
		return true
	}
}

func (c joinKeyColumn) appendEncoded(b *strings.Builder, i int) {
	switch c.kind {
	case "i64":
		b.WriteString("i64:")
		b.WriteString(strconv.FormatInt(c.i64.Value(i), 10))
		b.WriteByte(';')
	case "i32":
		b.WriteString("i32:")
		b.WriteString(strconv.FormatInt(int64(c.i32.Value(i)), 10))
		b.WriteByte(';')
	case "str":
		val := c.str.Value(i)
		b.WriteString("str:")
		b.WriteString(strconv.Itoa(len(val)))
		b.WriteByte(':')
		b.WriteString(val)
		b.WriteByte(';')
	}
}

func arrowArrayToInts(arr arrow.Array) ([]int, error) {
	switch a := arr.(type) {
	case *array.Int64:
		values := a.Int64Values()
		out := make([]int, len(values))
		for i, v := range values {
			out[i] = int(v)
		}
		return out, nil
	case *array.Int32:
		values := a.Int32Values()
		out := make([]int, len(values))
		for i, v := range values {
			out[i] = int(v)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported index array type %s", arr.DataType().String())
	}
}
