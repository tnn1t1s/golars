package group

import (
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func (gb *GroupBy) tryAggArrow(aggregations map[string]expr.Expr) (*AggResult, bool, error) {
	// Arrow compute groupby is not yet implemented; fall through.
	return nil, false, nil
}

func (gb *GroupBy) tryCountArrow() (*AggResult, bool, error) {
	// Fall through to generic path
	return nil, false, nil
}

func arrowAggOp(op expr.AggOp) (int, bool) {
	switch op {
	case expr.AggSum:
		return 0, true
	case expr.AggMean:
		return 1, true
	case expr.AggMin:
		return 2, true
	case expr.AggMax:
		return 3, true
	case expr.AggCount:
		return 4, true
	default:
		return -1, false
	}
}

func isArrowGroupByKeySupported(col series.Series) bool {
	_, ok := series.ArrowChunked(col)
	if !ok {
		return false
	}
	switch col.DataType().(type) {
	case datatypes.Int8, datatypes.Int16, datatypes.Int32, datatypes.Int64,
		datatypes.UInt8, datatypes.UInt16, datatypes.UInt32, datatypes.UInt64,
		datatypes.Float32, datatypes.Float64, datatypes.String:
		return true
	default:
		return false
	}
}

func isArrowGroupByValueSupported(col series.Series, op expr.AggOp) bool {
	_, ok := series.ArrowChunked(col)
	if !ok {
		return false
	}
	switch col.DataType().(type) {
	case datatypes.Int8, datatypes.Int16, datatypes.Int32, datatypes.Int64,
		datatypes.UInt8, datatypes.UInt16, datatypes.UInt32, datatypes.UInt64,
		datatypes.Float32, datatypes.Float64:
		return true
	default:
		return false
	}
}
