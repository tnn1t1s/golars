package frame

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowcompute "github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func arrowJoinIndices(leftCols, rightCols []series.Series, joinType JoinType) ([]int, []int, error) {
	if len(leftCols) == 0 || len(leftCols) != len(rightCols) {
		return nil, nil, fmt.Errorf("arrow join requires matching key columns")
	}
	if len(leftCols) == 1 {
		return arrowJoinIndicesSingle(leftCols[0], rightCols[0], joinType)
	}
	return arrowJoinIndicesMulti(leftCols, rightCols, joinType)
}

func arrowJoinIndicesSingle(leftCol, rightCol series.Series, joinType JoinType) ([]int, []int, error) {
	if !leftCol.DataType().Equals(rightCol.DataType()) {
		return nil, nil, fmt.Errorf("arrow join requires matching key types")
	}
	if !isArrowJoinTypeSupported(leftCol) {
		return nil, nil, fmt.Errorf("arrow join unsupported key type %s", leftCol.DataType().String())
	}

	opts := arrowcompute.DefaultHashJoinOptions()
	switch joinType {
	case InnerJoin:
		opts.JoinType = arrowcompute.InnerJoin
	case LeftJoin:
		opts.JoinType = arrowcompute.LeftJoin
	default:
		return nil, nil, fmt.Errorf("arrow join type %s not supported", joinType)
	}

	leftChunked, ok := series.ArrowChunked(leftCol)
	if !ok {
		return nil, nil, fmt.Errorf("arrow join requires Arrow-backed series")
	}
	defer leftChunked.Release()
	rightChunked, ok := series.ArrowChunked(rightCol)
	if !ok {
		return nil, nil, fmt.Errorf("arrow join requires Arrow-backed series")
	}
	defer rightChunked.Release()

	leftArr, rightArr, err := arrowcompute.HashJoinChunkedIndices(leftChunked, rightChunked, opts)
	if err != nil {
		return nil, nil, err
	}
	defer leftArr.Release()
	defer rightArr.Release()

	leftIdx, err := arrowIndexArrayToInts(leftArr, false)
	if err != nil {
		return nil, nil, err
	}
	rightIdx, err := arrowIndexArrayToInts(rightArr, joinType == LeftJoin)
	if err != nil {
		return nil, nil, err
	}

	return leftIdx, rightIdx, nil
}

func arrowJoinIndicesMulti(leftCols, rightCols []series.Series, joinType JoinType) ([]int, []int, error) {
	if len(leftCols) != len(rightCols) {
		return nil, nil, fmt.Errorf("arrow join requires matching key columns")
	}

	opts := arrowcompute.DefaultHashJoinOptions()
	switch joinType {
	case InnerJoin:
		opts.JoinType = arrowcompute.InnerJoin
	case LeftJoin:
		opts.JoinType = arrowcompute.LeftJoin
	default:
		return nil, nil, fmt.Errorf("arrow join type %s not supported", joinType)
	}

	leftComposite, err := buildCompositeJoinChunked(leftCols)
	if err != nil {
		return nil, nil, err
	}
	defer leftComposite.Release()
	rightComposite, err := buildCompositeJoinChunked(rightCols)
	if err != nil {
		return nil, nil, err
	}
	defer rightComposite.Release()

	leftArr, rightArr, err := arrowcompute.HashJoinChunkedIndices(leftComposite, rightComposite, opts)
	if err != nil {
		return nil, nil, err
	}
	defer leftArr.Release()
	defer rightArr.Release()

	leftIdx, err := arrowIndexArrayToInts(leftArr, false)
	if err != nil {
		return nil, nil, err
	}
	rightIdx, err := arrowIndexArrayToInts(rightArr, joinType == LeftJoin)
	if err != nil {
		return nil, nil, err
	}

	return leftIdx, rightIdx, nil
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
		return joinKeyColumn{}, fmt.Errorf("arrow join unsupported key type %s", arr.DataType().String())
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

func buildCompositeJoinChunked(cols []series.Series) (*arrow.Chunked, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("arrow join requires key columns")
	}
	mem := memory.NewGoAllocator()
	keyArrs := make([]arrow.Array, len(cols))
	for i, col := range cols {
		if !isArrowJoinTypeSupported(col) {
			return nil, fmt.Errorf("arrow join unsupported key type %s", col.DataType().String())
		}
		chunked, ok := series.ArrowChunked(col)
		if !ok {
			return nil, fmt.Errorf("arrow join requires Arrow-backed series")
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
			return nil, fmt.Errorf("arrow join key columns length mismatch")
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

func arrowIndexArrayToInts(arr arrow.Array, allowNulls bool) ([]int, error) {
	switch a := arr.(type) {
	case *array.Int64:
		values := a.Int64Values()
		result := make([]int, len(values))
		if a.NullN() == 0 {
			for i, v := range values {
				result[i] = int(v)
			}
			return result, nil
		}
		for i, v := range values {
			if a.IsNull(i) {
				if !allowNulls {
					return nil, fmt.Errorf("unexpected null in join indices")
				}
				result[i] = -1
				continue
			}
			result[i] = int(v)
		}
		return result, nil
	case *array.Int32:
		values := a.Int32Values()
		result := make([]int, len(values))
		if a.NullN() == 0 {
			for i, v := range values {
				result[i] = int(v)
			}
			return result, nil
		}
		for i, v := range values {
			if a.IsNull(i) {
				if !allowNulls {
					return nil, fmt.Errorf("unexpected null in join indices")
				}
				result[i] = -1
				continue
			}
			result[i] = int(v)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported join index type %s", arr.DataType().String())
	}
}

func isArrowJoinTypeSupported(col series.Series) bool {
	switch col.DataType().(type) {
	case datatypes.Int64, datatypes.Int32, datatypes.String:
		return true
	default:
		return false
	}
}
