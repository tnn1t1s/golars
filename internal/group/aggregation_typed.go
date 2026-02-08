package group

import (
	_ "fmt"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func (gb *GroupBy) tryAggTyped(aggregations map[string]expr.Expr) (*AggResult, bool, error) {
	panic("not implemented")

}

func computeAggTyped(col series.Series, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}

func aggInt64(values []int64, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}

func aggInt32(values []int32, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}

func aggUint64(values []uint64, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}

func aggUint32(values []uint32, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}

func aggFloat64(values []float64, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}

func aggFloat32(values []float32, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}

func aggInt16(values []int16, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}

func aggInt8(values []int8, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}

func aggUint16(values []uint16, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}

func aggUint8(values []uint8, validity []bool, groupIDs []uint32, groupCount int, agg expr.AggOp) ([]interface{}, datatypes.DataType, bool, error) {
	panic("not implemented")

}
