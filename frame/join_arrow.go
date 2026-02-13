package frame

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/tnn1t1s/golars/series"
)

func arrowJoinIndices(leftCols, rightCols []series.Series, joinType JoinType) ([]int, []int, error) {
	if len(leftCols) != len(rightCols) {
		return nil, nil, fmt.Errorf("mismatched number of join columns: %d vs %d", len(leftCols), len(rightCols))
	}

	if len(leftCols) == 1 {
		return arrowJoinIndicesSingle(leftCols[0], rightCols[0], joinType)
	}
	return arrowJoinIndicesMulti(leftCols, rightCols, joinType)
}

func arrowJoinIndicesSingle(leftCol, rightCol series.Series, joinType JoinType) ([]int, []int, error) {
	// Build hash map from right side for lookup
	rightMap := make(map[string][]int)
	for i := 0; i < rightCol.Len(); i++ {
		if rightCol.IsNull(i) {
			continue
		}
		key := rightCol.GetAsString(i)
		rightMap[key] = append(rightMap[key], i)
	}

	var leftIndices, rightIndices []int

	switch joinType {
	case InnerJoin:
		for i := 0; i < leftCol.Len(); i++ {
			if leftCol.IsNull(i) {
				continue
			}
			key := leftCol.GetAsString(i)
			if matches, ok := rightMap[key]; ok {
				for _, ri := range matches {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, ri)
				}
			}
		}
	case LeftJoin:
		for i := 0; i < leftCol.Len(); i++ {
			if leftCol.IsNull(i) {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, -1)
				continue
			}
			key := leftCol.GetAsString(i)
			if matches, ok := rightMap[key]; ok {
				for _, ri := range matches {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, ri)
				}
			} else {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, -1)
			}
		}
	case OuterJoin:
		rightMatched := make(map[int]bool)
		for i := 0; i < leftCol.Len(); i++ {
			if leftCol.IsNull(i) {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, -1)
				continue
			}
			key := leftCol.GetAsString(i)
			if matches, ok := rightMap[key]; ok {
				for _, ri := range matches {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, ri)
					rightMatched[ri] = true
				}
			} else {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, -1)
			}
		}
		for i := 0; i < rightCol.Len(); i++ {
			if !rightMatched[i] {
				leftIndices = append(leftIndices, -1)
				rightIndices = append(rightIndices, i)
			}
		}
	default:
		return nil, nil, fmt.Errorf("unsupported join type for arrow join: %v", joinType)
	}

	return leftIndices, rightIndices, nil
}

func arrowJoinIndicesMulti(leftCols, rightCols []series.Series, joinType JoinType) ([]int, []int, error) {
	// Build composite keys
	makeKey := func(cols []series.Series, row int) string {
		parts := make([]string, len(cols))
		for i, col := range cols {
			if col.IsNull(row) {
				parts[i] = "\x00null\x00"
			} else {
				parts[i] = col.GetAsString(row)
			}
		}
		return strings.Join(parts, "\x00")
	}

	rightMap := make(map[string][]int)
	rightLen := rightCols[0].Len()
	for i := 0; i < rightLen; i++ {
		hasNull := false
		for _, col := range rightCols {
			if col.IsNull(i) {
				hasNull = true
				break
			}
		}
		if hasNull {
			continue
		}
		key := makeKey(rightCols, i)
		rightMap[key] = append(rightMap[key], i)
	}

	leftLen := leftCols[0].Len()
	var leftIndices, rightIndices []int

	switch joinType {
	case InnerJoin:
		for i := 0; i < leftLen; i++ {
			hasNull := false
			for _, col := range leftCols {
				if col.IsNull(i) {
					hasNull = true
					break
				}
			}
			if hasNull {
				continue
			}
			key := makeKey(leftCols, i)
			if matches, ok := rightMap[key]; ok {
				for _, ri := range matches {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, ri)
				}
			}
		}
	case LeftJoin:
		for i := 0; i < leftLen; i++ {
			key := makeKey(leftCols, i)
			if matches, ok := rightMap[key]; ok {
				for _, ri := range matches {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, ri)
				}
			} else {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, -1)
			}
		}
	case OuterJoin:
		rightMatched := make(map[int]bool)
		for i := 0; i < leftLen; i++ {
			key := makeKey(leftCols, i)
			if matches, ok := rightMap[key]; ok {
				for _, ri := range matches {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, ri)
					rightMatched[ri] = true
				}
			} else {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, -1)
			}
		}
		for i := 0; i < rightLen; i++ {
			if !rightMatched[i] {
				leftIndices = append(leftIndices, -1)
				rightIndices = append(rightIndices, i)
			}
		}
	default:
		return nil, nil, fmt.Errorf("unsupported join type for arrow join: %v", joinType)
	}

	return leftIndices, rightIndices, nil
}

func arrowIndexArrayToInts(arr arrow.Array, allowNulls bool) ([]int, error) {
	result := make([]int, arr.Len())

	switch a := arr.(type) {
	case *array.Int32:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				if allowNulls {
					result[i] = -1
				} else {
					return nil, fmt.Errorf("unexpected null at index %d", i)
				}
			} else {
				result[i] = int(a.Value(i))
			}
		}
	case *array.Int64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				if allowNulls {
					result[i] = -1
				} else {
					return nil, fmt.Errorf("unexpected null at index %d", i)
				}
			} else {
				result[i] = int(a.Value(i))
			}
		}
	case *array.Uint32:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				if allowNulls {
					result[i] = -1
				} else {
					return nil, fmt.Errorf("unexpected null at index %d", i)
				}
			} else {
				result[i] = int(a.Value(i))
			}
		}
	case *array.Uint64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				if allowNulls {
					result[i] = -1
				} else {
					return nil, fmt.Errorf("unexpected null at index %d", i)
				}
			} else {
				result[i] = int(a.Value(i))
			}
		}
	default:
		return nil, fmt.Errorf("unsupported array type for index conversion: %T", arr)
	}

	return result, nil
}

func isArrowJoinTypeSupported(col series.Series) bool {
	_, ok := series.ArrowChunked(col)
	return ok
}
