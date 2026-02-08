package group

import (
	"fmt"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func (gb *GroupBy) tryAggTyped(aggregations map[string]expr.Expr) (*AggResult, bool, error) {
	// Only use typed path when we have rowGroupIDs (single-key fast path)
	if gb.rowGroupIDs == nil {
		return nil, false, nil
	}

	groupCount := len(gb.groupOrder)

	// Check all aggregations are simple agg expressions on columns
	for _, aggExpr := range aggregations {
		agg, ok := aggExpr.(*expr.AggExpr)
		if !ok {
			return nil, false, nil
		}
		if _, ok := agg.Input().(*expr.ColumnExpr); !ok {
			return nil, false, nil
		}
	}

	// Build key columns
	var columns []series.Series
	for colIdx, colName := range gb.groupCols {
		col, err := gb.df.Column(colName)
		if err != nil {
			return nil, false, err
		}
		keyValues := make([]interface{}, groupCount)
		for i, hash := range gb.groupOrder {
			keys := gb.groupKeys[hash]
			keyValues[i] = keys[colIdx]
		}
		columns = append(columns, createSeriesFromInterface(colName, keyValues, col.DataType()))
	}

	// Process each aggregation
	for colName, aggExpr := range aggregations {
		agg := aggExpr.(*expr.AggExpr)
		colRef := agg.Input().(*expr.ColumnExpr)
		col, err := gb.df.Column(colRef.Name())
		if err != nil {
			return nil, false, err
		}

		values, dtype, ok, err := computeAggTyped(col, gb.rowGroupIDs, groupCount, agg.AggType())
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		columns = append(columns, createSeriesFromInterface(colName, values, dtype))
	}

	return &AggResult{Columns: columns}, true, nil
}

func computeAggTyped(col series.Series, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch col.DataType().(type) {
	case datatypes.Int64:
		vals, validity, ok := series.Int64ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggInt64(vals, validity, groupIDs, groupCount, agg)
	case datatypes.Int32:
		vals, validity, ok := series.Int32ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggInt32(vals, validity, groupIDs, groupCount, agg)
	case datatypes.UInt64:
		vals, validity, ok := series.Uint64ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggUint64(vals, validity, groupIDs, groupCount, agg)
	case datatypes.UInt32:
		vals, validity, ok := series.Uint32ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggUint32(vals, validity, groupIDs, groupCount, agg)
	case datatypes.Float64:
		vals, validity, ok := series.Float64ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggFloat64(vals, validity, groupIDs, groupCount, agg)
	case datatypes.Float32:
		vals, validity, ok := series.Float32ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggFloat32(vals, validity, groupIDs, groupCount, agg)
	case datatypes.Int16:
		vals, validity, ok := series.Int16ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggInt16(vals, validity, groupIDs, groupCount, agg)
	case datatypes.Int8:
		vals, validity, ok := series.Int8ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggInt8(vals, validity, groupIDs, groupCount, agg)
	case datatypes.UInt16:
		vals, validity, ok := series.Uint16ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggUint16(vals, validity, groupIDs, groupCount, agg)
	case datatypes.UInt8:
		vals, validity, ok := series.Uint8ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggUint8(vals, validity, groupIDs, groupCount, agg)
	default:
		return nil, nil, false, nil
	}
}

type numeric interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

func typedAgg[T numeric](values []T, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp, dtype datatypes.DataType) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		return typedSum(values, validity, groupIDs, groupCount, dtype)
	case expr.AggMean:
		return typedMean(values, validity, groupIDs, groupCount)
	case expr.AggMin:
		return typedMin(values, validity, groupIDs, groupCount, dtype)
	case expr.AggMax:
		return typedMax(values, validity, groupIDs, groupCount, dtype)
	case expr.AggCount:
		return typedCount(values, validity, groupIDs, groupCount)
	default:
		return nil, nil, false, nil
	}
}

func typedSum[T numeric](values []T, validity []bool, groupIDs []uint32, groupCount int, dtype datatypes.DataType) ([]interface{}, datatypes.DataType, bool, error) {
	sums := make([]T, groupCount)
	for i, v := range values {
		if validity[i] {
			gid := groupIDs[i]
			sums[gid] += v
		}
	}
	result := make([]interface{}, groupCount)
	for i, s := range sums {
		result[i] = s
	}
	return result, dtype, true, nil
}

func typedMean[T numeric](values []T, validity []bool, groupIDs []uint32, groupCount int) ([]interface{}, datatypes.DataType, bool, error) {
	sums := make([]float64, groupCount)
	counts := make([]int, groupCount)
	for i, v := range values {
		if validity[i] {
			gid := groupIDs[i]
			sums[gid] += float64(v)
			counts[gid]++
		}
	}
	result := make([]interface{}, groupCount)
	for i := range sums {
		if counts[i] > 0 {
			result[i] = sums[i] / float64(counts[i])
		}
	}
	return result, datatypes.Float64{}, true, nil
}

func typedMin[T numeric](values []T, validity []bool, groupIDs []uint32, groupCount int, dtype datatypes.DataType) ([]interface{}, datatypes.DataType, bool, error) {
	mins := make([]T, groupCount)
	hasValue := make([]bool, groupCount)
	for i, v := range values {
		if validity[i] {
			gid := groupIDs[i]
			if !hasValue[gid] || v < mins[gid] {
				mins[gid] = v
				hasValue[gid] = true
			}
		}
	}
	result := make([]interface{}, groupCount)
	for i := range mins {
		if hasValue[i] {
			result[i] = mins[i]
		}
	}
	return result, dtype, true, nil
}

func typedMax[T numeric](values []T, validity []bool, groupIDs []uint32, groupCount int, dtype datatypes.DataType) ([]interface{}, datatypes.DataType, bool, error) {
	maxs := make([]T, groupCount)
	hasValue := make([]bool, groupCount)
	for i, v := range values {
		if validity[i] {
			gid := groupIDs[i]
			if !hasValue[gid] || v > maxs[gid] {
				maxs[gid] = v
				hasValue[gid] = true
			}
		}
	}
	result := make([]interface{}, groupCount)
	for i := range maxs {
		if hasValue[i] {
			result[i] = maxs[i]
		}
	}
	return result, dtype, true, nil
}

func typedCount[T numeric](values []T, validity []bool, groupIDs []uint32, groupCount int) ([]interface{}, datatypes.DataType, bool, error) {
	counts := make([]int64, groupCount)
	for i := range values {
		if validity[i] {
			gid := groupIDs[i]
			counts[gid]++
		}
	}
	result := make([]interface{}, groupCount)
	for i, c := range counts {
		result[i] = c
	}
	return result, datatypes.Int64{}, true, nil
}

func aggInt64(values []int64, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	return typedAgg(values, validity, groupIDs, groupCount, agg, datatypes.Int64{})
}

func aggInt32(values []int32, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	return typedAgg(values, validity, groupIDs, groupCount, agg, datatypes.Int32{})
}

func aggUint64(values []uint64, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	return typedAgg(values, validity, groupIDs, groupCount, agg, datatypes.UInt64{})
}

func aggUint32(values []uint32, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	return typedAgg(values, validity, groupIDs, groupCount, agg, datatypes.UInt32{})
}

func aggFloat64(values []float64, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	return typedAgg(values, validity, groupIDs, groupCount, agg, datatypes.Float64{})
}

func aggFloat32(values []float32, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	return typedAgg(values, validity, groupIDs, groupCount, agg, datatypes.Float32{})
}

func aggInt16(values []int16, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	return typedAgg(values, validity, groupIDs, groupCount, agg, datatypes.Int16{})
}

func aggInt8(values []int8, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	return typedAgg(values, validity, groupIDs, groupCount, agg, datatypes.Int8{})
}

func aggUint16(values []uint16, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	return typedAgg(values, validity, groupIDs, groupCount, agg, datatypes.UInt16{})
}

func aggUint8(values []uint8, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	return typedAgg(values, validity, groupIDs, groupCount, agg, datatypes.UInt8{})
}

// ensure fmt is used
var _ = fmt.Sprintf
