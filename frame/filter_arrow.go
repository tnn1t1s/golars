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
	if !ok || err != nil {
		return nil, false, err
	}

	ctx := context.Background()
	mem := memory.DefaultAllocator

	boolArr, release, err := datumToBoolArray(mask, mem)
	if err != nil {
		return nil, false, err
	}
	defer release()

	// Filter each column
	var resultCols []series.Series
	for _, col := range df.columns {
		filtered, err := filterSeriesArrowCompute(col, &compute.ArrayDatum{Value: boolArr.Data()})
		if err != nil {
			return nil, false, err
		}
		resultCols = append(resultCols, filtered)
	}

	_ = ctx
	result, err := NewDataFrame(resultCols...)
	return result, true, err
}

func (df *DataFrame) arrowComputeMask(e expr.Expr) (compute.Datum, bool, error) {
	ctx := context.Background()

	switch te := e.(type) {
	case *expr.BinaryExpr:
		switch te.Op() {
		case expr.OpAnd:
			leftMask, ok, err := df.arrowComputeMask(te.Left())
			if !ok || err != nil {
				return nil, false, err
			}
			rightMask, ok, err := df.arrowComputeMask(te.Right())
			if !ok || err != nil {
				return nil, false, err
			}
			result, err := boolBinaryDatum(leftMask, rightMask, expr.OpAnd)
			return result, err == nil, err

		case expr.OpOr:
			leftMask, ok, err := df.arrowComputeMask(te.Left())
			if !ok || err != nil {
				return nil, false, err
			}
			rightMask, ok, err := df.arrowComputeMask(te.Right())
			if !ok || err != nil {
				return nil, false, err
			}
			result, err := boolBinaryDatum(leftMask, rightMask, expr.OpOr)
			return result, err == nil, err

		case expr.OpEqual, expr.OpNotEqual, expr.OpLess, expr.OpLessEqual, expr.OpGreater, expr.OpGreaterEqual:
			colExpr, litExpr, op, ok := normalizeLiteralCompare(te)
			if !ok {
				return nil, false, nil
			}

			col, err := df.Column(colExpr.Name())
			if err != nil {
				return nil, false, nil
			}

			arrowDatum, ok := arrowDatumFromSeries(col)
			if !ok {
				return nil, false, nil
			}

			scalarDatum, ok, err := arrowScalarDatum(litExpr.Value(), col.DataType())
			if !ok || err != nil {
				return nil, false, nil
			}

			fnName, ok := arrowCompareFunction(op)
			if !ok {
				return nil, false, nil
			}

			result, err := compute.CallFunction(ctx, fnName, nil, arrowDatum, scalarDatum)
			if err != nil {
				return nil, false, err
			}
			return result, true, nil
		}

	case *expr.UnaryExpr:
		if te.Op() == expr.OpNot {
			innerMask, ok, err := df.arrowComputeMask(te.Expr())
			if !ok || err != nil {
				return nil, false, err
			}
			result, err := boolNotDatum(innerMask)
			return result, err == nil, err
		}
		if te.Op() == expr.OpIsNull {
			if colExpr, ok := te.Expr().(*expr.ColumnExpr); ok {
				col, err := df.Column(colExpr.Name())
				if err != nil {
					return nil, false, nil
				}
				arrowDatum, ok := arrowDatumFromSeries(col)
				if !ok {
					return nil, false, nil
				}
				result, err := compute.CallFunction(ctx, "is_null", nil, arrowDatum)
				if err != nil {
					return nil, false, err
				}
				return result, true, nil
			}
		}
		if te.Op() == expr.OpIsNotNull {
			if colExpr, ok := te.Expr().(*expr.ColumnExpr); ok {
				col, err := df.Column(colExpr.Name())
				if err != nil {
					return nil, false, nil
				}
				arrowDatum, ok := arrowDatumFromSeries(col)
				if !ok {
					return nil, false, nil
				}
				result, err := compute.CallFunction(ctx, "is_valid", nil, arrowDatum)
				if err != nil {
					return nil, false, err
				}
				return result, true, nil
			}
		}

	case *expr.LiteralExpr:
		if bv, ok := te.Value().(bool); ok {
			result, err := arrowBoolLiteralMask(df.height, bv)
			return result, err == nil, err
		}
	}

	return nil, false, nil
}

func normalizeLiteralCompare(e *expr.BinaryExpr) (*expr.ColumnExpr, *expr.LiteralExpr, expr.BinaryOp, bool) {
	// Try left=col, right=lit
	if colExpr, ok := e.Left().(*expr.ColumnExpr); ok {
		if litExpr, ok := e.Right().(*expr.LiteralExpr); ok {
			return colExpr, litExpr, e.Op(), true
		}
	}
	// Try left=lit, right=col (need to invert operator)
	if litExpr, ok := e.Left().(*expr.LiteralExpr); ok {
		if colExpr, ok := e.Right().(*expr.ColumnExpr); ok {
			return colExpr, litExpr, invertCompareOp(e.Op()), true
		}
	}
	return nil, nil, 0, false
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
		return op // Equal and NotEqual are symmetric
	}
}

func arrowCompareFunction(op expr.BinaryOp) (string, bool) {
	switch op {
	case expr.OpEqual:
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
	mem := memory.DefaultAllocator
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()

	for i := 0; i < length; i++ {
		builder.Append(value)
	}
	arr := builder.NewBooleanArray()
	return &compute.ArrayDatum{Value: arr.Data()}, nil
}

func boolNotDatum(input compute.Datum) (compute.Datum, error) {
	ctx := context.Background()
	result, err := compute.CallFunction(ctx, "invert", nil, input)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func boolBinaryDatum(left, right compute.Datum, op expr.BinaryOp) (compute.Datum, error) {
	ctx := context.Background()
	var fnName string
	switch op {
	case expr.OpAnd:
		fnName = "and"
	case expr.OpOr:
		fnName = "or"
	default:
		return nil, fmt.Errorf("unsupported boolean binary operation: %v", op)
	}
	result, err := compute.CallFunction(ctx, fnName, nil, left, right)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func datumToBoolArray(d compute.Datum, mem memory.Allocator) (*array.Boolean, func(), error) {
	switch dt := d.(type) {
	case *compute.ArrayDatum:
		arr := array.MakeFromData(dt.Value)
		boolArr, ok := arr.(*array.Boolean)
		if !ok {
			arr.Release()
			return nil, func() {}, fmt.Errorf("expected boolean array, got %T", arr)
		}
		return boolArr, func() { boolArr.Release() }, nil
	case *compute.ChunkedDatum:
		// Concatenate chunks into a single array
		chunks := dt.Value.Chunks()
		if len(chunks) == 1 {
			boolArr, ok := chunks[0].(*array.Boolean)
			if !ok {
				return nil, func() {}, fmt.Errorf("expected boolean chunk, got %T", chunks[0])
			}
			boolArr.Retain()
			return boolArr, func() { boolArr.Release() }, nil
		}
		// Flatten chunks
		totalLen := 0
		for _, chunk := range chunks {
			totalLen += chunk.Len()
		}
		builder := array.NewBooleanBuilder(mem)
		for _, chunk := range chunks {
			boolChunk, ok := chunk.(*array.Boolean)
			if !ok {
				builder.Release()
				return nil, func() {}, fmt.Errorf("expected boolean chunk, got %T", chunk)
			}
			for i := 0; i < boolChunk.Len(); i++ {
				if boolChunk.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(boolChunk.Value(i))
				}
			}
		}
		arr := builder.NewBooleanArray()
		return arr, func() { arr.Release() }, nil
	default:
		return nil, func() {}, fmt.Errorf("unsupported datum type for boolean conversion: %T", d)
	}
}

func arrowDatumFromSeries(s series.Series) (*compute.ChunkedDatum, bool) {
	chunked, ok := series.ArrowChunked(s)
	if !ok {
		return nil, false
	}
	return &compute.ChunkedDatum{Value: chunked}, true
}

func arrowScalarDatum(value interface{}, dtype datatypes.DataType) (*compute.ScalarDatum, bool, error) {
	coerced, ok := coerceScalarValue(value, dtype)
	if !ok {
		return nil, false, nil
	}

	var sc scalar.Scalar
	switch v := coerced.(type) {
	case int8:
		sc = scalar.NewInt8Scalar(v)
	case int16:
		sc = scalar.NewInt16Scalar(v)
	case int32:
		sc = scalar.NewInt32Scalar(v)
	case int64:
		sc = scalar.NewInt64Scalar(v)
	case uint8:
		sc = scalar.NewUint8Scalar(v)
	case uint16:
		sc = scalar.NewUint16Scalar(v)
	case uint32:
		sc = scalar.NewUint32Scalar(v)
	case uint64:
		sc = scalar.NewUint64Scalar(v)
	case float32:
		sc = scalar.NewFloat32Scalar(v)
	case float64:
		sc = scalar.NewFloat64Scalar(v)
	case string:
		sc = scalar.NewStringScalar(v)
	case bool:
		sc = scalar.NewBooleanScalar(v)
	default:
		return nil, false, nil
	}

	return &compute.ScalarDatum{Value: sc}, true, nil
}

func coerceScalarValue(value interface{}, dtype datatypes.DataType) (interface{}, bool) {
	switch dtype.(type) {
	case datatypes.Int8:
		switch v := value.(type) {
		case int64:
			return int8(v), true
		case int:
			return int8(v), true
		case float64:
			return int8(v), true
		}
	case datatypes.Int16:
		switch v := value.(type) {
		case int64:
			return int16(v), true
		case int:
			return int16(v), true
		case float64:
			return int16(v), true
		}
	case datatypes.Int32:
		switch v := value.(type) {
		case int64:
			return int32(v), true
		case int:
			return int32(v), true
		case float64:
			return int32(v), true
		}
	case datatypes.Int64:
		switch v := value.(type) {
		case int64:
			return v, true
		case int:
			return int64(v), true
		case float64:
			return int64(v), true
		}
	case datatypes.UInt8:
		switch v := value.(type) {
		case int64:
			return uint8(v), true
		case uint64:
			return uint8(v), true
		}
	case datatypes.UInt16:
		switch v := value.(type) {
		case int64:
			return uint16(v), true
		case uint64:
			return uint16(v), true
		}
	case datatypes.UInt32:
		switch v := value.(type) {
		case int64:
			return uint32(v), true
		case uint64:
			return uint32(v), true
		}
	case datatypes.UInt64:
		switch v := value.(type) {
		case int64:
			return uint64(v), true
		case uint64:
			return v, true
		}
	case datatypes.Float32:
		switch v := value.(type) {
		case float64:
			return float32(v), true
		case float32:
			return v, true
		case int64:
			return float32(v), true
		}
	case datatypes.Float64:
		switch v := value.(type) {
		case float64:
			return v, true
		case float32:
			return float64(v), true
		case int64:
			return float64(v), true
		}
	case datatypes.String:
		switch v := value.(type) {
		case string:
			return v, true
		}
	case datatypes.Boolean:
		switch v := value.(type) {
		case bool:
			return v, true
		}
	}

	// If no coercion needed, return as-is if it matches common types
	switch value.(type) {
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, string, bool:
		return value, true
	}

	return nil, false
}

func filterSeriesArrowCompute(col series.Series, mask compute.Datum) (series.Series, error) {
	chunked, ok := series.ArrowChunked(col)
	if !ok {
		return nil, fmt.Errorf("cannot get arrow chunked array for column %q", col.Name())
	}

	ctx := context.Background()
	datum := &compute.ChunkedDatum{Value: chunked}

	result, err := compute.Filter(ctx, datum, mask, *compute.DefaultFilterOptions())
	if err != nil {
		return nil, fmt.Errorf("arrow compute filter for column %q: %w", col.Name(), err)
	}

	switch rd := result.(type) {
	case *compute.ChunkedDatum:
		return series.SeriesFromArrowChunked(col.Name(), rd.Value)
	case *compute.ArrayDatum:
		arr := array.MakeFromData(rd.Value)
		defer arr.Release()
		return series.SeriesFromArrowArray(col.Name(), arr)
	default:
		return nil, fmt.Errorf("unexpected result datum type from filter: %T", result)
	}
}
