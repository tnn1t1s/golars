package frame

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowcompute "github.com/apache/arrow-go/v18/arrow/compute"
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

	leftChunked := make([]*arrow.Chunked, len(leftCols))
	rightChunked := make([]*arrow.Chunked, len(rightCols))
	for i := range leftCols {
		if !isArrowJoinTypeSupported(leftCols[i]) {
			return nil, nil, fmt.Errorf("arrow join unsupported key type %s", leftCols[i].DataType().String())
		}
		if !isArrowJoinTypeSupported(rightCols[i]) {
			return nil, nil, fmt.Errorf("arrow join unsupported key type %s", rightCols[i].DataType().String())
		}
		lchunked, ok := series.ArrowChunked(leftCols[i])
		if !ok {
			return nil, nil, fmt.Errorf("arrow join requires Arrow-backed series")
		}
		rchunked, ok := series.ArrowChunked(rightCols[i])
		if !ok {
			lchunked.Release()
			return nil, nil, fmt.Errorf("arrow join requires Arrow-backed series")
		}
		leftChunked[i] = lchunked
		rightChunked[i] = rchunked
	}
	defer func() {
		for _, chunked := range leftChunked {
			if chunked != nil {
				chunked.Release()
			}
		}
		for _, chunked := range rightChunked {
			if chunked != nil {
				chunked.Release()
			}
		}
	}()

	leftArr, rightArr, err := arrowcompute.HashJoinChunkedIndicesMulti(leftChunked, rightChunked, opts)
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
