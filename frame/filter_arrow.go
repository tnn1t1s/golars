package frame

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

func (df *DataFrame) tryArrowComputeFilter(filterExpr expr.Expr) (*DataFrame, bool, error) {
	mask, ok, err := df.arrowComputeMask(filterExpr)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	defer mask.Release()

	filteredCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		filtered, err := filterSeriesArrowCompute(col, mask)
		if err != nil {
			return nil, true, err
		}
		filteredCols[i] = filtered
	}

	out, err := NewDataFrame(filteredCols...)
	return out, true, err
}

func (df *DataFrame) arrowComputeMask(e expr.Expr) (compute.Datum, bool, error) {
	switch ex := e.(type) {
	case *expr.UnaryExpr:
		switch ex.Op() {
		case expr.OpNot:
			inner, ok, err := df.arrowComputeMask(ex.Expr())
			if !ok || err != nil {
				return nil, ok, err
			}
			defer inner.Release()
			mask, err := boolNotDatum(inner)
			return mask, err == nil, err
		case expr.OpIsNull, expr.OpIsNotNull:
			colExpr, ok := ex.Expr().(*expr.ColumnExpr)
			if !ok {
				return nil, false, nil
			}
			col, err := df.Column(colExpr.Name())
			if err != nil {
				return nil, true, err
			}
			colDatum, ok := arrowDatumFromSeries(col)
			if !ok {
				return nil, false, nil
			}
			defer colDatum.Release()
			mask, err := compute.CallFunction(context.Background(), "is_null", nil, colDatum)
			if err != nil {
				return nil, true, err
			}
			if ex.Op() == expr.OpIsNull {
				return mask, true, nil
			}
			defer mask.Release()
			inverted, err := boolNotDatum(mask)
			return inverted, err == nil, err
		default:
			return nil, false, nil
		}
	case *expr.BinaryExpr:
		switch ex.Op() {
		case expr.OpAnd, expr.OpOr:
			left, ok, err := df.arrowComputeMask(ex.Left())
			if !ok || err != nil {
				return nil, ok, err
			}
			defer left.Release()
			right, ok, err := df.arrowComputeMask(ex.Right())
			if !ok || err != nil {
				return nil, ok, err
			}
			defer right.Release()
			mask, err := boolBinaryDatum(left, right, ex.Op())
			return mask, err == nil, err
		case expr.OpEqual, expr.OpEqualMissing, expr.OpNotEqual, expr.OpLess, expr.OpLessEqual, expr.OpGreater, expr.OpGreaterEqual:
			colExpr, litExpr, op, ok := normalizeLiteralCompare(ex)
			if !ok {
				return nil, false, nil
			}
			col, err := df.Column(colExpr.Name())
			if err != nil {
				return nil, true, err
			}
			if litExpr.Value() == nil {
				if op != expr.OpEqualMissing {
					return nil, false, nil
				}
				colDatum, ok := arrowDatumFromSeries(col)
				if !ok {
					return nil, false, nil
				}
				defer colDatum.Release()
				mask, err := compute.CallFunction(context.Background(), "is_null", nil, colDatum)
				return mask, err == nil, err
			}

			fn, ok := arrowCompareFunction(op)
			if !ok {
				return nil, false, nil
			}
			colDatum, ok := arrowDatumFromSeries(col)
			if !ok {
				return nil, false, nil
			}
			defer colDatum.Release()
			scalarDatum, ok, err := arrowScalarDatum(litExpr.Value(), col.DataType())
			if !ok || err != nil {
				return nil, false, err
			}
			defer scalarDatum.Release()
			mask, err := compute.CallFunction(context.Background(), fn, nil, colDatum, scalarDatum)
			return mask, err == nil, err
		default:
			return nil, false, nil
		}
	case *expr.ColumnExpr:
		col, err := df.Column(ex.Name())
		if err != nil {
			return nil, true, err
		}
		if _, ok := col.DataType().(datatypes.Boolean); !ok {
			return nil, true, fmt.Errorf("filter column must be boolean")
		}
		colDatum, ok := arrowDatumFromSeries(col)
		if !ok {
			return nil, false, nil
		}
		return colDatum, true, nil
	case *expr.LiteralExpr:
		val, ok := ex.Value().(bool)
		if !ok {
			return nil, false, nil
		}
		mask, err := arrowBoolLiteralMask(df.height, val)
		return mask, err == nil, err
	default:
		return nil, false, nil
	}
}

func normalizeLiteralCompare(e *expr.BinaryExpr) (*expr.ColumnExpr, *expr.LiteralExpr, expr.BinaryOp, bool) {
	leftCol, leftIsCol := e.Left().(*expr.ColumnExpr)
	rightCol, rightIsCol := e.Right().(*expr.ColumnExpr)
	leftLit, leftIsLit := e.Left().(*expr.LiteralExpr)
	rightLit, rightIsLit := e.Right().(*expr.LiteralExpr)

	switch {
	case leftIsCol && rightIsLit:
		return leftCol, rightLit, e.Op(), true
	case leftIsLit && rightIsCol:
		return rightCol, leftLit, invertCompareOp(e.Op()), true
	default:
		return nil, nil, e.Op(), false
	}
}

func invertCompareOp(op expr.BinaryOp) expr.BinaryOp {
	switch op {
	case expr.OpLess:
		return expr.OpGreater
	case expr.OpLessEqual:
		return expr.OpGreaterEqual
	case expr.OpGreater:
		return expr.OpLess
	case expr.OpGreaterEqual:
		return expr.OpLessEqual
	default:
		return op
	}
}

func arrowCompareFunction(op expr.BinaryOp) (string, bool) {
	switch op {
	case expr.OpEqual, expr.OpEqualMissing:
		return "equal", true
	case expr.OpNotEqual:
		return "not_equal", true
	case expr.OpLess:
		return "less", true
	case expr.OpLessEqual:
		return "less_equal", true
	case expr.OpGreater:
		return "greater", true
	case expr.OpGreaterEqual:
		return "greater_equal", true
	default:
		return "", false
	}
}

func arrowBoolLiteralMask(length int, value bool) (compute.Datum, error) {
	mem := memory.NewGoAllocator()
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	builder.Reserve(length)
	for i := 0; i < length; i++ {
		builder.Append(value)
	}
	arr := builder.NewArray()
	datum := compute.NewDatum(arr)
	arr.Release()
	return datum, nil
}

func boolNotDatum(input compute.Datum) (compute.Datum, error) {
	mem := memory.NewGoAllocator()
	arr, release, err := datumToBoolArray(input, mem)
	if err != nil {
		return nil, err
	}
	defer release()

	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	builder.Reserve(arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}
		builder.Append(!arr.Value(i))
	}
	out := builder.NewArray()
	datum := compute.NewDatum(out)
	out.Release()
	return datum, nil
}

func boolBinaryDatum(left, right compute.Datum, op expr.BinaryOp) (compute.Datum, error) {
	mem := memory.NewGoAllocator()
	leftArr, leftRelease, err := datumToBoolArray(left, mem)
	if err != nil {
		return nil, err
	}
	defer leftRelease()
	rightArr, rightRelease, err := datumToBoolArray(right, mem)
	if err != nil {
		return nil, err
	}
	defer rightRelease()

	if leftArr.Len() != rightArr.Len() {
		return nil, fmt.Errorf("boolean masks length mismatch")
	}

	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	builder.Reserve(leftArr.Len())
	for i := 0; i < leftArr.Len(); i++ {
		if leftArr.IsNull(i) || rightArr.IsNull(i) {
			builder.AppendNull()
			continue
		}
		switch op {
		case expr.OpAnd:
			builder.Append(leftArr.Value(i) && rightArr.Value(i))
		case expr.OpOr:
			builder.Append(leftArr.Value(i) || rightArr.Value(i))
		default:
			return nil, fmt.Errorf("unsupported boolean op %v", op)
		}
	}
	out := builder.NewArray()
	datum := compute.NewDatum(out)
	out.Release()
	return datum, nil
}

func datumToBoolArray(d compute.Datum, mem memory.Allocator) (*array.Boolean, func(), error) {
	switch datum := d.(type) {
	case *compute.ArrayDatum:
		arr := datum.MakeArray()
		boolArr, ok := arr.(*array.Boolean)
		if !ok {
			arr.Release()
			return nil, nil, fmt.Errorf("expected boolean array, got %s", arr.DataType().String())
		}
		return boolArr, arr.Release, nil
	case *compute.ChunkedDatum:
		concat, err := array.Concatenate(datum.Value.Chunks(), mem)
		if err != nil {
			return nil, nil, err
		}
		boolArr, ok := concat.(*array.Boolean)
		if !ok {
			concat.Release()
			return nil, nil, fmt.Errorf("expected boolean array, got %s", concat.DataType().String())
		}
		return boolArr, concat.Release, nil
	default:
		return nil, nil, fmt.Errorf("unsupported mask datum %T", d)
	}
}

func arrowDatumFromSeries(s series.Series) (*compute.ChunkedDatum, bool) {
	chunkedArr, ok := series.ArrowChunked(s)
	if !ok {
		return nil, false
	}
	return &compute.ChunkedDatum{Value: chunkedArr}, true
}

func arrowScalarDatum(value interface{}, dtype datatypes.DataType) (*compute.ScalarDatum, bool, error) {
	coerced, ok := coerceScalarValue(value, dtype)
	if !ok {
		return nil, false, nil
	}
	arrowType := datatypes.GetPolarsType(dtype).ArrowType()
	if arrowType == nil {
		return nil, false, fmt.Errorf("unsupported Arrow scalar type for %T", dtype)
	}
	scalarVal, err := scalar.MakeScalarParam(coerced, arrowType)
	if err != nil {
		return nil, false, err
	}
	return &compute.ScalarDatum{Value: scalarVal}, true, nil
}

func coerceScalarValue(value interface{}, dtype datatypes.DataType) (interface{}, bool) {
	switch dtype.(type) {
	case datatypes.Int32:
		switch v := value.(type) {
		case int:
			return int32(v), true
		case int32:
			return v, true
		case int64:
			return int32(v), true
		case float64:
			return int32(v), true
		}
	case datatypes.Int64:
		switch v := value.(type) {
		case int:
			return int64(v), true
		case int32:
			return int64(v), true
		case int64:
			return v, true
		case float64:
			return int64(v), true
		}
	case datatypes.Float64:
		switch v := value.(type) {
		case int:
			return float64(v), true
		case int32:
			return float64(v), true
		case int64:
			return float64(v), true
		case float32:
			return float64(v), true
		case float64:
			return v, true
		}
	case datatypes.Float32:
		switch v := value.(type) {
		case int:
			return float32(v), true
		case int32:
			return float32(v), true
		case int64:
			return float32(v), true
		case float32:
			return v, true
		case float64:
			return float32(v), true
		}
	case datatypes.String:
		if v, ok := value.(string); ok {
			return v, true
		}
	case datatypes.Boolean:
		if v, ok := value.(bool); ok {
			return v, true
		}
	}
	return value, true
}

func filterSeriesArrowCompute(col series.Series, mask compute.Datum) (series.Series, error) {
	colDatum, ok := arrowDatumFromSeries(col)
	if !ok {
		return nil, fmt.Errorf("unsupported series type for Arrow compute filter")
	}
	defer colDatum.Release()

	out, err := compute.Filter(context.Background(), colDatum, mask, *compute.DefaultFilterOptions())
	if err != nil {
		return nil, err
	}
	defer out.Release()

	switch datum := out.(type) {
	case *compute.ArrayDatum:
		arr := datum.MakeArray()
		defer arr.Release()
		return series.SeriesFromArrowArray(col.Name(), arr)
	case *compute.ChunkedDatum:
		return series.SeriesFromArrowChunked(col.Name(), datum.Value)
	default:
		return nil, fmt.Errorf("unsupported Arrow compute output type %T", out)
	}
}
