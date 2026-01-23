package group

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	arrowcompute "github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func (gb *GroupBy) tryAggArrow(aggregations map[string]expr.Expr) (*AggResult, bool, error) {
	if len(gb.groupCols) == 0 {
		return nil, false, nil
	}

	groupCols := make([]series.Series, len(gb.groupCols))
	keyChunkedArrs := make([]*arrow.Chunked, len(gb.groupCols))
	for i, name := range gb.groupCols {
		groupCol, err := gb.df.Column(name)
		if err != nil {
			return nil, true, err
		}
		if !isArrowGroupByKeySupported(groupCol) {
			return nil, false, nil
		}
		chunked, ok := series.ArrowChunked(groupCol)
		if !ok {
			return nil, false, nil
		}
		groupCols[i] = groupCol
		keyChunkedArrs[i] = chunked
	}
	for _, chunked := range keyChunkedArrs {
		defer chunked.Release()
	}

	opts := arrowcompute.DefaultHashGroupByOptions()
	groupIDs, keyArrs, err := hashGroupByChunkedIndicesMulti(keyChunkedArrs)
	if err != nil {
		return nil, true, err
	}
	for _, arr := range keyArrs {
		defer arr.Release()
	}
	if len(keyArrs) == 0 {
		return nil, true, fmt.Errorf("arrow groupby returned no key arrays")
	}
	groupCount := keyArrs[0].Len()

	resultCols := make([]series.Series, 0, len(aggregations)+len(keyArrs))
	for i, keyArr := range keyArrs {
		keySeries, err := series.SeriesFromArrowArray(groupCols[i].Name(), keyArr)
		if err != nil {
			return nil, true, err
		}
		resultCols = append(resultCols, keySeries)
	}

	for name, aggExpr := range aggregations {
		agg, ok := aggExpr.(*expr.AggExpr)
		if !ok {
			return nil, false, nil
		}
		colExpr, ok := agg.Input().(*expr.ColumnExpr)
		if !ok {
			return nil, false, nil
		}
		op, ok := arrowAggOp(agg.AggType())
		if !ok {
			return nil, false, nil
		}

		var aggArr series.Series
		if op == arrowcompute.AggCount {
			aggArrow, err := arrowcompute.GroupByAggregateIndices(nil, groupIDs, groupCount, op, opts)
			if err != nil {
				return nil, true, err
			}
			defer aggArrow.Release()
			aggArr, err = series.SeriesFromArrowArray(name, aggArrow)
			if err != nil {
				return nil, true, err
			}
		} else {
			valueCol, err := gb.df.Column(colExpr.Name())
			if err != nil {
				return nil, true, err
			}
			if !isArrowGroupByValueSupported(valueCol, agg.AggType()) {
				return nil, false, nil
			}
			valueChunked, ok := series.ArrowChunked(valueCol)
			if !ok {
				return nil, false, nil
			}
			defer valueChunked.Release()

			aggArrow, err := arrowcompute.GroupByAggregateChunkedIndices(valueChunked, groupIDs, groupCount, op, opts)
			if err != nil {
				return nil, true, err
			}
			defer aggArrow.Release()
			aggArr, err = series.SeriesFromArrowArray(name, aggArrow)
			if err != nil {
				return nil, true, err
			}
		}
		resultCols = append(resultCols, aggArr)
	}

	return &AggResult{Columns: resultCols}, true, nil
}

func (gb *GroupBy) tryCountArrow() (*AggResult, bool, error) {
	if len(gb.groupCols) == 0 {
		return nil, false, nil
	}

	groupCols := make([]series.Series, len(gb.groupCols))
	keyChunkedArrs := make([]*arrow.Chunked, len(gb.groupCols))
	for i, name := range gb.groupCols {
		groupCol, err := gb.df.Column(name)
		if err != nil {
			return nil, true, err
		}
		if !isArrowGroupByKeySupported(groupCol) {
			return nil, false, nil
		}
		chunked, ok := series.ArrowChunked(groupCol)
		if !ok {
			return nil, false, nil
		}
		groupCols[i] = groupCol
		keyChunkedArrs[i] = chunked
	}
	for _, chunked := range keyChunkedArrs {
		defer chunked.Release()
	}

	opts := arrowcompute.DefaultHashGroupByOptions()
	groupIDs, keyArrs, err := hashGroupByChunkedIndicesMulti(keyChunkedArrs)
	if err != nil {
		return nil, true, err
	}
	for _, arr := range keyArrs {
		defer arr.Release()
	}
	if len(keyArrs) == 0 {
		return nil, true, fmt.Errorf("arrow groupby returned no key arrays")
	}

	resultCols := make([]series.Series, 0, len(keyArrs)+1)
	for i, keyArr := range keyArrs {
		keySeries, err := series.SeriesFromArrowArray(groupCols[i].Name(), keyArr)
		if err != nil {
			return nil, true, err
		}
		resultCols = append(resultCols, keySeries)
	}
	countArr, err := arrowcompute.GroupByAggregateIndices(nil, groupIDs, keyArrs[0].Len(), arrowcompute.AggCount, opts)
	if err != nil {
		return nil, true, err
	}
	defer countArr.Release()
	countSeries, err := series.SeriesFromArrowArray("count", countArr)
	if err != nil {
		return nil, true, err
	}
	resultCols = append(resultCols, countSeries)

	return &AggResult{Columns: resultCols}, true, nil
}

func arrowAggOp(op expr.AggOp) (arrowcompute.GroupByAggOp, bool) {
	switch op {
	case expr.AggSum:
		return arrowcompute.AggSum, true
	case expr.AggMean:
		return arrowcompute.AggMean, true
	case expr.AggMin:
		return arrowcompute.AggMin, true
	case expr.AggMax:
		return arrowcompute.AggMax, true
	case expr.AggCount:
		return arrowcompute.AggCount, true
	default:
		return 0, false
	}
}

func isArrowGroupByKeySupported(col series.Series) bool {
	switch col.DataType().(type) {
	case datatypes.Int64, datatypes.Int32, datatypes.String:
		return true
	default:
		return false
	}
}

func isArrowGroupByValueSupported(col series.Series, op expr.AggOp) bool {
	switch op {
	case expr.AggSum, expr.AggMean, expr.AggMin, expr.AggMax:
	default:
		return false
	}

	switch col.DataType().(type) {
	case datatypes.Int64, datatypes.Int32, datatypes.Float64, datatypes.Float32:
		return true
	default:
		return false
	}
}
