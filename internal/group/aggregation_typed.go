package group

import (
	"fmt"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func (gb *GroupBy) tryAggTyped(aggregations map[string]expr.Expr) (*AggResult, bool, error) {
	if len(gb.groupCols) != 1 || gb.rowGroupIDs == nil {
		return nil, false, nil
	}

	aggExprs := make(map[string]*expr.AggExpr, len(aggregations))
	for name, e := range aggregations {
		agg, ok := e.(*expr.AggExpr)
		if !ok {
			return nil, false, nil
		}
		if _, ok := agg.Input().(*expr.ColumnExpr); !ok {
			return nil, false, nil
		}
		switch agg.AggType() {
		case expr.AggSum, expr.AggMean, expr.AggMin, expr.AggMax, expr.AggCount:
		default:
			return nil, false, nil
		}
		aggExprs[name] = agg
	}

	groupCount := len(gb.groupOrder)
	result := &AggregationResult{
		GroupKeys: gb.groupKeys,
		Results:   make(map[string][]interface{}, len(aggregations)),
		DataTypes: make(map[string]datatypes.DataType, len(aggregations)),
	}

	for name, agg := range aggExprs {
		colExpr := agg.Input().(*expr.ColumnExpr)
		col, err := gb.df.Column(colExpr.Name())
		if err != nil {
			return nil, false, fmt.Errorf("column %s not found", colExpr.Name())
		}

		values, dtype, ok, err := computeAggTyped(col, gb.rowGroupIDs, groupCount, agg.AggType())
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		result.Results[name] = values
		result.DataTypes[name] = dtype
	}

	out, err := gb.buildResultDataFrame(result)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func computeAggTyped(col series.Series, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	if agg == expr.AggCount {
		counts := make([]int64, groupCount)
		for _, gid := range groupIDs {
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i, v := range counts {
			out[i] = v
		}
		return out, datatypes.Int64{}, true, nil
	}

	dtype := col.DataType()
	switch {
	case dtype.Equals(datatypes.Int64{}):
		values, validity, ok := series.Int64ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggInt64(values, validity, groupIDs, groupCount, agg)
	case dtype.Equals(datatypes.Int32{}):
		values, validity, ok := series.Int32ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggInt32(values, validity, groupIDs, groupCount, agg)
	case dtype.Equals(datatypes.UInt64{}):
		values, validity, ok := series.Uint64ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggUint64(values, validity, groupIDs, groupCount, agg)
	case dtype.Equals(datatypes.UInt32{}):
		values, validity, ok := series.Uint32ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggUint32(values, validity, groupIDs, groupCount, agg)
	case dtype.Equals(datatypes.Float64{}):
		values, validity, ok := series.Float64ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggFloat64(values, validity, groupIDs, groupCount, agg)
	case dtype.Equals(datatypes.Float32{}):
		values, validity, ok := series.Float32ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggFloat32(values, validity, groupIDs, groupCount, agg)
	case dtype.Equals(datatypes.Int16{}):
		values, validity, ok := series.Int16ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggInt16(values, validity, groupIDs, groupCount, agg)
	case dtype.Equals(datatypes.Int8{}):
		values, validity, ok := series.Int8ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggInt8(values, validity, groupIDs, groupCount, agg)
	case dtype.Equals(datatypes.UInt16{}):
		values, validity, ok := series.Uint16ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggUint16(values, validity, groupIDs, groupCount, agg)
	case dtype.Equals(datatypes.UInt8{}):
		values, validity, ok := series.Uint8ValuesWithValidity(col)
		if !ok {
			return nil, nil, false, nil
		}
		return aggUint8(values, validity, groupIDs, groupCount, agg)
	default:
		return nil, nil, false, nil
	}
}

func aggInt64(values []int64, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		sums := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			sums[groupIDs[i]] += v
		}
		out := make([]interface{}, groupCount)
		for i, v := range sums {
			out[i] = v
		}
		return out, datatypes.Int64{}, true, nil
	case expr.AggMean:
		sums := make([]float64, groupCount)
		counts := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			sums[gid] += float64(v)
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i := range sums {
			if counts[i] == 0 {
				out[i] = nil
				continue
			}
			out[i] = sums[i] / float64(counts[i])
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMin:
		mins := make([]int64, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v < mins[gid] {
				mins[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range mins {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Int64{}, true, nil
	case expr.AggMax:
		maxs := make([]int64, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v > maxs[gid] {
				maxs[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range maxs {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Int64{}, true, nil
	default:
		return nil, nil, false, nil
	}
}

func aggInt32(values []int32, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		sums := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			sums[groupIDs[i]] += int64(v)
		}
		out := make([]interface{}, groupCount)
		for i, v := range sums {
			out[i] = int32(v)
		}
		return out, datatypes.Int32{}, true, nil
	case expr.AggMean:
		sums := make([]float64, groupCount)
		counts := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			sums[gid] += float64(v)
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i := range sums {
			if counts[i] == 0 {
				out[i] = nil
				continue
			}
			out[i] = sums[i] / float64(counts[i])
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMin:
		mins := make([]int32, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v < mins[gid] {
				mins[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range mins {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Int32{}, true, nil
	case expr.AggMax:
		maxs := make([]int32, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v > maxs[gid] {
				maxs[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range maxs {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Int32{}, true, nil
	default:
		return nil, nil, false, nil
	}
}

func aggUint64(values []uint64, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		sums := make([]uint64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			sums[groupIDs[i]] += v
		}
		out := make([]interface{}, groupCount)
		for i, v := range sums {
			out[i] = v
		}
		return out, datatypes.UInt64{}, true, nil
	case expr.AggMean:
		sums := make([]float64, groupCount)
		counts := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			sums[gid] += float64(v)
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i := range sums {
			if counts[i] == 0 {
				out[i] = nil
				continue
			}
			out[i] = sums[i] / float64(counts[i])
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMin:
		mins := make([]uint64, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v < mins[gid] {
				mins[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range mins {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.UInt64{}, true, nil
	case expr.AggMax:
		maxs := make([]uint64, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v > maxs[gid] {
				maxs[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range maxs {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.UInt64{}, true, nil
	default:
		return nil, nil, false, nil
	}
}

func aggUint32(values []uint32, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		sums := make([]uint64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			sums[groupIDs[i]] += uint64(v)
		}
		out := make([]interface{}, groupCount)
		for i, v := range sums {
			out[i] = uint32(v)
		}
		return out, datatypes.UInt32{}, true, nil
	case expr.AggMean:
		sums := make([]float64, groupCount)
		counts := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			sums[gid] += float64(v)
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i := range sums {
			if counts[i] == 0 {
				out[i] = nil
				continue
			}
			out[i] = sums[i] / float64(counts[i])
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMin:
		mins := make([]uint32, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v < mins[gid] {
				mins[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range mins {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.UInt32{}, true, nil
	case expr.AggMax:
		maxs := make([]uint32, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v > maxs[gid] {
				maxs[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range maxs {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.UInt32{}, true, nil
	default:
		return nil, nil, false, nil
	}
}

func aggFloat64(values []float64, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		sums := make([]float64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			sums[groupIDs[i]] += v
		}
		out := make([]interface{}, groupCount)
		for i, v := range sums {
			out[i] = v
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMean:
		sums := make([]float64, groupCount)
		counts := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			sums[gid] += v
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i := range sums {
			if counts[i] == 0 {
				out[i] = nil
				continue
			}
			out[i] = sums[i] / float64(counts[i])
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMin:
		mins := make([]float64, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v < mins[gid] {
				mins[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range mins {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMax:
		maxs := make([]float64, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v > maxs[gid] {
				maxs[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range maxs {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Float64{}, true, nil
	default:
		return nil, nil, false, nil
	}
}

func aggFloat32(values []float32, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		sums := make([]float64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			sums[groupIDs[i]] += float64(v)
		}
		out := make([]interface{}, groupCount)
		for i, v := range sums {
			out[i] = float32(v)
		}
		return out, datatypes.Float32{}, true, nil
	case expr.AggMean:
		sums := make([]float64, groupCount)
		counts := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			sums[gid] += float64(v)
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i := range sums {
			if counts[i] == 0 {
				out[i] = nil
				continue
			}
			out[i] = sums[i] / float64(counts[i])
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMin:
		mins := make([]float32, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v < mins[gid] {
				mins[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range mins {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Float32{}, true, nil
	case expr.AggMax:
		maxs := make([]float32, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v > maxs[gid] {
				maxs[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range maxs {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Float32{}, true, nil
	default:
		return nil, nil, false, nil
	}
}

func aggInt16(values []int16, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		sums := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			sums[groupIDs[i]] += int64(v)
		}
		out := make([]interface{}, groupCount)
		for i, v := range sums {
			out[i] = int16(v)
		}
		return out, datatypes.Int16{}, true, nil
	case expr.AggMean:
		sums := make([]float64, groupCount)
		counts := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			sums[gid] += float64(v)
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i := range sums {
			if counts[i] == 0 {
				out[i] = nil
				continue
			}
			out[i] = sums[i] / float64(counts[i])
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMin:
		mins := make([]int16, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v < mins[gid] {
				mins[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range mins {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Int16{}, true, nil
	case expr.AggMax:
		maxs := make([]int16, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v > maxs[gid] {
				maxs[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range maxs {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Int16{}, true, nil
	default:
		return nil, nil, false, nil
	}
}

func aggInt8(values []int8, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		sums := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			sums[groupIDs[i]] += int64(v)
		}
		out := make([]interface{}, groupCount)
		for i, v := range sums {
			out[i] = int8(v)
		}
		return out, datatypes.Int8{}, true, nil
	case expr.AggMean:
		sums := make([]float64, groupCount)
		counts := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			sums[gid] += float64(v)
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i := range sums {
			if counts[i] == 0 {
				out[i] = nil
				continue
			}
			out[i] = sums[i] / float64(counts[i])
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMin:
		mins := make([]int8, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v < mins[gid] {
				mins[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range mins {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Int8{}, true, nil
	case expr.AggMax:
		maxs := make([]int8, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v > maxs[gid] {
				maxs[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range maxs {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.Int8{}, true, nil
	default:
		return nil, nil, false, nil
	}
}

func aggUint16(values []uint16, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		sums := make([]uint64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			sums[groupIDs[i]] += uint64(v)
		}
		out := make([]interface{}, groupCount)
		for i, v := range sums {
			out[i] = uint16(v)
		}
		return out, datatypes.UInt16{}, true, nil
	case expr.AggMean:
		sums := make([]float64, groupCount)
		counts := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			sums[gid] += float64(v)
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i := range sums {
			if counts[i] == 0 {
				out[i] = nil
				continue
			}
			out[i] = sums[i] / float64(counts[i])
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMin:
		mins := make([]uint16, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v < mins[gid] {
				mins[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range mins {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.UInt16{}, true, nil
	case expr.AggMax:
		maxs := make([]uint16, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v > maxs[gid] {
				maxs[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range maxs {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.UInt16{}, true, nil
	default:
		return nil, nil, false, nil
	}
}

func aggUint8(values []uint8, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	switch agg {
	case expr.AggSum:
		sums := make([]uint64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			sums[groupIDs[i]] += uint64(v)
		}
		out := make([]interface{}, groupCount)
		for i, v := range sums {
			out[i] = uint8(v)
		}
		return out, datatypes.UInt8{}, true, nil
	case expr.AggMean:
		sums := make([]float64, groupCount)
		counts := make([]int64, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			sums[gid] += float64(v)
			counts[gid]++
		}
		out := make([]interface{}, groupCount)
		for i := range sums {
			if counts[i] == 0 {
				out[i] = nil
				continue
			}
			out[i] = sums[i] / float64(counts[i])
		}
		return out, datatypes.Float64{}, true, nil
	case expr.AggMin:
		mins := make([]uint8, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v < mins[gid] {
				mins[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range mins {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.UInt8{}, true, nil
	case expr.AggMax:
		maxs := make([]uint8, groupCount)
		has := make([]bool, groupCount)
		for i, v := range values {
			if !validity[i] {
				continue
			}
			gid := groupIDs[i]
			if !has[gid] || v > maxs[gid] {
				maxs[gid] = v
				has[gid] = true
			}
		}
		out := make([]interface{}, groupCount)
		for i, v := range maxs {
			if !has[i] {
				out[i] = nil
				continue
			}
			out[i] = v
		}
		return out, datatypes.UInt8{}, true, nil
	default:
		return nil, nil, false, nil
	}
}
