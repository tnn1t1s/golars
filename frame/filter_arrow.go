package frame

import (
	_ "context"
	_ "fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func (df *DataFrame) tryArrowComputeFilter(filterExpr expr.Expr) (*DataFrame, bool, error) {
	panic("not implemented")

}

func (df *DataFrame) arrowComputeMask(e expr.Expr) (compute.Datum, bool, error) {
	panic("not implemented")

}

func normalizeLiteralCompare(e *expr.BinaryExpr) (*expr.ColumnExpr, *expr.LiteralExpr, expr.BinaryOp, bool) {
	panic("not implemented")

}

func invertCompareOp(op expr.BinaryOp) expr.BinaryOp {
	panic("not implemented")

}

func arrowCompareFunction(op expr.BinaryOp) (string, bool) {
	panic("not implemented")

}

func arrowBoolLiteralMask(length int, value bool) (compute.Datum, error) {
	panic("not implemented")

}

func boolNotDatum(input compute.Datum) (compute.Datum, error) {
	panic("not implemented")

}

func boolBinaryDatum(left, right compute.Datum, op expr.BinaryOp) (compute.Datum, error) {
	panic("not implemented")

}

func datumToBoolArray(d compute.Datum, mem memory.Allocator) (*array.Boolean, func(), error) {
	panic("not implemented")

}

func arrowDatumFromSeries(s series.Series) (*compute.ChunkedDatum, bool) {
	panic("not implemented")

}

func arrowScalarDatum(value interface{}, dtype datatypes.DataType) (*compute.ScalarDatum, bool, error) {
	panic("not implemented")

}

func coerceScalarValue(value interface{}, dtype datatypes.DataType) (interface{}, bool) {
	panic("not implemented")

}

func filterSeriesArrowCompute(col series.Series, mask compute.Datum) (series.Series, error) {
	panic("not implemented")

}
