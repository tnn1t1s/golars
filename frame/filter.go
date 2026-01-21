package frame

import (
	"fmt"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/internal/parallel"
	"github.com/tnn1t1s/golars/series"
)

// Filter returns a new DataFrame containing only rows where the expression evaluates to true
func (df *DataFrame) Filter(filterExpr expr.Expr) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	// Evaluate the expression to get a boolean mask
	mask, err := df.evaluateBooleanExpr(filterExpr)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate filter expression: %w", err)
	}

	// Count true values to determine output size
	outputSize := 0
	for i := 0; i < len(mask); i++ {
		if mask[i] {
			outputSize++
		}
	}

	// If no rows match, return empty DataFrame with same schema
	if outputSize == 0 {
		emptyCols := make([]series.Series, len(df.columns))
		for i, col := range df.columns {
			emptyCols[i] = col.Head(0)
		}
		return NewDataFrame(emptyCols...)
	}

	// Create filtered columns
	filteredCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		filtered, err := filterSeries(col, mask)
		if err != nil {
			return nil, err
		}
		filteredCols[i] = filtered
	}

	return NewDataFrame(filteredCols...)
}

// evaluateBooleanExpr evaluates an expression and returns a boolean mask
func (df *DataFrame) evaluateBooleanExpr(e expr.Expr) ([]bool, error) {
	switch ex := e.(type) {
	case *expr.BinaryExpr:
		return df.evaluateBinaryExpr(ex)
	case *expr.UnaryExpr:
		return df.evaluateUnaryExpr(ex)
	case *expr.ColumnExpr:
		// A column expression should be boolean type
		col, err := df.Column(ex.Name())
		if err != nil {
			return nil, err
		}
		return df.seriesToBoolMask(col)
	case *expr.LiteralExpr:
		// A literal boolean value
		if val, ok := ex.Value().(bool); ok {
			mask := make([]bool, df.height)
			if err := parallel.For(len(mask), func(start, end int) error {
				for i := start; i < end; i++ {
					mask[i] = val
				}
				return nil
			}); err != nil {
				return nil, err
			}
			return mask, nil
		}
		return nil, fmt.Errorf("literal expression must be boolean for filtering")
	default:
		return nil, fmt.Errorf("unsupported expression type for filtering: %T", e)
	}
}

// evaluateBinaryExpr evaluates a binary expression
func (df *DataFrame) evaluateBinaryExpr(e *expr.BinaryExpr) ([]bool, error) {
	switch e.Op() {
	case expr.OpEqual, expr.OpNotEqual, expr.OpLess, expr.OpLessEqual, expr.OpGreater, expr.OpGreaterEqual:
		return df.evaluateComparisonExpr(e)
	case expr.OpAnd, expr.OpOr:
		return df.evaluateLogicalExpr(e)
	default:
		return nil, fmt.Errorf("unsupported binary operation for filtering: %v", e.Op())
	}
}

// evaluateComparisonExpr evaluates comparison expressions
func (df *DataFrame) evaluateComparisonExpr(e *expr.BinaryExpr) ([]bool, error) {
	// Get left side values
	leftValues, err := df.evaluateExprValues(e.Left())
	if err != nil {
		return nil, err
	}

	// Get right side values
	rightValues, err := df.evaluateExprValues(e.Right())
	if err != nil {
		return nil, err
	}

	// Perform comparison
	mask := make([]bool, df.height)
	if err := parallel.For(df.height, func(start, end int) error {
		for i := start; i < end; i++ {
			// Handle nulls - any comparison with null is false
			if leftValues[i] == nil || rightValues[i] == nil {
				mask[i] = false
				continue
			}

			mask[i] = compareValues(leftValues[i], rightValues[i], e.Op())
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return mask, nil
}

// evaluateLogicalExpr evaluates logical expressions (AND, OR)
func (df *DataFrame) evaluateLogicalExpr(e *expr.BinaryExpr) ([]bool, error) {
	// Evaluate left side
	leftMask, err := df.evaluateBooleanExpr(e.Left())
	if err != nil {
		return nil, err
	}

	// Evaluate right side
	rightMask, err := df.evaluateBooleanExpr(e.Right())
	if err != nil {
		return nil, err
	}

	// Combine masks
	mask := make([]bool, df.height)
	if err := parallel.For(df.height, func(start, end int) error {
		for i := start; i < end; i++ {
			switch e.Op() {
			case expr.OpAnd:
				mask[i] = leftMask[i] && rightMask[i]
			case expr.OpOr:
				mask[i] = leftMask[i] || rightMask[i]
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return mask, nil
}

// evaluateUnaryExpr evaluates unary expressions
func (df *DataFrame) evaluateUnaryExpr(e *expr.UnaryExpr) ([]bool, error) {
	switch e.Op() {
	case expr.OpNot:
		innerMask, err := df.evaluateBooleanExpr(e.Expr())
		if err != nil {
			return nil, err
		}
		mask := make([]bool, len(innerMask))
		if err := parallel.For(len(innerMask), func(start, end int) error {
			for i := start; i < end; i++ {
				mask[i] = !innerMask[i]
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return mask, nil

	case expr.OpIsNull, expr.OpIsNotNull:
		values, err := df.evaluateExprValues(e.Expr())
		if err != nil {
			return nil, err
		}
		mask := make([]bool, len(values))
		if err := parallel.For(len(values), func(start, end int) error {
			for i := start; i < end; i++ {
				if e.Op() == expr.OpIsNull {
					mask[i] = values[i] == nil
				} else {
					mask[i] = values[i] != nil
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return mask, nil

	default:
		return nil, fmt.Errorf("unsupported unary operation for filtering: %v", e.Op())
	}
}

// evaluateExprValues evaluates an expression and returns the values
func (df *DataFrame) evaluateExprValues(e expr.Expr) ([]interface{}, error) {
	switch ex := e.(type) {
	case *expr.ColumnExpr:
		col, err := df.Column(ex.Name())
		if err != nil {
			return nil, err
		}
		values := make([]interface{}, df.height)
		if err := parallel.For(df.height, func(start, end int) error {
			for i := start; i < end; i++ {
				values[i] = col.Get(i)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return values, nil

	case *expr.LiteralExpr:
		// Broadcast literal value
		values := make([]interface{}, df.height)
		if err := parallel.For(len(values), func(start, end int) error {
			for i := start; i < end; i++ {
				values[i] = ex.Value()
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return values, nil
	case *expr.CastExpr:
		values, err := df.evaluateExprValues(ex.Expr())
		if err != nil {
			return nil, err
		}
		casted := make([]interface{}, len(values))
		if err := parallel.For(len(values), func(start, end int) error {
			for i := start; i < end; i++ {
				value := values[i]
				if value == nil {
					casted[i] = nil
					continue
				}
				converted, err := castValue(value, ex.TargetType())
				if err != nil {
					return err
				}
				casted[i] = converted
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return casted, nil
	case *expr.AliasExpr:
		return df.evaluateExprValues(ex.Expr())

	default:
		return nil, fmt.Errorf("unsupported expression type for value evaluation: %T", e)
	}
}

// seriesToBoolMask converts a boolean series to a boolean mask
func (df *DataFrame) seriesToBoolMask(s series.Series) ([]bool, error) {
	mask := make([]bool, s.Len())
	if err := parallel.For(s.Len(), func(start, end int) error {
		for i := start; i < end; i++ {
			val := s.Get(i)
			if val == nil {
				mask[i] = false
				continue
			}
			b, ok := val.(bool)
			if !ok {
				return fmt.Errorf("series must contain boolean values for filtering")
			}
			mask[i] = b
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return mask, nil
}

// filterSeries filters a series based on a boolean mask
func filterSeries(s series.Series, mask []bool) (series.Series, error) {
	if s.Len() != len(mask) {
		return nil, fmt.Errorf("mask length %d does not match series length %d", len(mask), s.Len())
	}

	// Count output size
	outputSize := 0
	for _, keep := range mask {
		if keep {
			outputSize++
		}
	}

	// Collect indices to keep
	indices := make([]int, 0, outputSize)
	for i, keep := range mask {
		if keep {
			indices = append(indices, i)
		}
	}

	// Create new series with filtered values
	return gatherSeries(s, indices)
}

// gatherSeries creates a new series by gathering values at specific indices
func gatherSeries(s series.Series, indices []int) (series.Series, error) {
	// This is a simplified implementation
	// A real implementation would be more efficient with type-specific paths

	values := make([]interface{}, len(indices))
	validity := make([]bool, len(indices))

	if err := parallel.For(len(indices), func(start, end int) error {
		for i := start; i < end; i++ {
			idx := indices[i]
			if idx < 0 || idx >= s.Len() {
				return fmt.Errorf("index %d out of bounds", idx)
			}
			values[i] = s.Get(idx)
			validity[i] = s.IsValid(idx)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Create new series based on type
	// This is simplified - real implementation would handle all types
	switch s.DataType().(type) {
	case datatypes.Int32:
		int32Values := make([]int32, len(values))
		for i, v := range values {
			if v != nil {
				int32Values[i] = v.(int32)
			}
		}
		return series.NewSeriesWithValidity(s.Name(), int32Values, validity, s.DataType()), nil

	case datatypes.Int64:
		int64Values := make([]int64, len(values))
		for i, v := range values {
			if v != nil {
				int64Values[i] = v.(int64)
			}
		}
		return series.NewSeriesWithValidity(s.Name(), int64Values, validity, s.DataType()), nil

	case datatypes.Float64:
		float64Values := make([]float64, len(values))
		for i, v := range values {
			if v != nil {
				float64Values[i] = v.(float64)
			}
		}
		return series.NewSeriesWithValidity(s.Name(), float64Values, validity, s.DataType()), nil

	case datatypes.String:
		stringValues := make([]string, len(values))
		for i, v := range values {
			if v != nil {
				stringValues[i] = v.(string)
			}
		}
		return series.NewSeriesWithValidity(s.Name(), stringValues, validity, s.DataType()), nil

	case datatypes.Boolean:
		boolValues := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				boolValues[i] = v.(bool)
			}
		}
		return series.NewSeriesWithValidity(s.Name(), boolValues, validity, s.DataType()), nil

	default:
		return nil, fmt.Errorf("unsupported data type for gather: %v", s.DataType())
	}
}

// compareValues compares two values based on the operation
func compareValues(left, right interface{}, op expr.BinaryOp) bool {
	// This is a simplified comparison
	// A real implementation would handle type-specific comparisons more efficiently

	switch op {
	case expr.OpEqual:
		return compareEqual(left, right)
	case expr.OpNotEqual:
		return !compareEqual(left, right)
	case expr.OpLess:
		return compareLess(left, right)
	case expr.OpLessEqual:
		return compareLess(left, right) || compareEqual(left, right)
	case expr.OpGreater:
		return !compareLess(left, right) && !compareEqual(left, right)
	case expr.OpGreaterEqual:
		return !compareLess(left, right)
	default:
		return false
	}
}

// compareEqual checks if two values are equal
func compareEqual(left, right interface{}) bool {
	// Handle different numeric types
	switch l := left.(type) {
	case int32:
		switch r := right.(type) {
		case int32:
			return l == r
		case int64:
			return int64(l) == r
		case float64:
			return float64(l) == r
		}
	case int64:
		switch r := right.(type) {
		case int32:
			return l == int64(r)
		case int64:
			return l == r
		case float64:
			return float64(l) == r
		}
	case float64:
		switch r := right.(type) {
		case int32:
			return l == float64(r)
		case int64:
			return l == float64(r)
		case float64:
			return l == r
		}
	case string:
		if r, ok := right.(string); ok {
			return l == r
		}
	case bool:
		if r, ok := right.(bool); ok {
			return l == r
		}
	}
	return false
}

// compareLess checks if left < right
func compareLess(left, right interface{}) bool {
	// Handle different numeric types
	switch l := left.(type) {
	case int32:
		switch r := right.(type) {
		case int32:
			return l < r
		case int64:
			return int64(l) < r
		case float64:
			return float64(l) < r
		}
	case int64:
		switch r := right.(type) {
		case int32:
			return l < int64(r)
		case int64:
			return l < r
		case float64:
			return float64(l) < r
		}
	case float64:
		switch r := right.(type) {
		case int32:
			return l < float64(r)
		case int64:
			return l < float64(r)
		case float64:
			return l < r
		}
	case string:
		if r, ok := right.(string); ok {
			return l < r
		}
	}
	return false
}

func castValue(value interface{}, target datatypes.DataType) (interface{}, error) {
	switch target.(type) {
	case datatypes.Int64:
		v, ok := castToInt64(value)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to int64", value)
		}
		return v, nil
	case datatypes.Int32:
		v, ok := castToInt32(value)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to int32", value)
		}
		return v, nil
	case datatypes.Int16:
		v, ok := castToInt16(value)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to int16", value)
		}
		return v, nil
	case datatypes.Int8:
		v, ok := castToInt8(value)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to int8", value)
		}
		return v, nil
	case datatypes.UInt64:
		v, ok := castToUInt64(value)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to uint64", value)
		}
		return v, nil
	case datatypes.UInt32:
		v, ok := castToUInt32(value)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to uint32", value)
		}
		return v, nil
	case datatypes.UInt16:
		v, ok := castToUInt16(value)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to uint16", value)
		}
		return v, nil
	case datatypes.UInt8:
		v, ok := castToUInt8(value)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to uint8", value)
		}
		return v, nil
	case datatypes.Float64:
		v, ok := castToFloat64(value)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to float64", value)
		}
		return v, nil
	case datatypes.Float32:
		v, ok := castToFloat32(value)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to float32", value)
		}
		return v, nil
	case datatypes.Boolean:
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to bool", value)
		}
		return v, nil
	case datatypes.String:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("cannot cast %T to string", value)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported cast type %s", target.String())
	}
}

func castToInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		return int64(val), true
	case float32:
		return int64(val), true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

func castToInt32(v interface{}) (int32, bool) {
	switch val := v.(type) {
	case int:
		return int32(val), true
	case int8:
		return int32(val), true
	case int16:
		return int32(val), true
	case int32:
		return val, true
	case int64:
		return int32(val), true
	case uint8:
		return int32(val), true
	case uint16:
		return int32(val), true
	case uint32:
		return int32(val), true
	case uint64:
		return int32(val), true
	case float32:
		return int32(val), true
	case float64:
		return int32(val), true
	default:
		return 0, false
	}
}

func castToInt16(v interface{}) (int16, bool) {
	switch val := v.(type) {
	case int:
		return int16(val), true
	case int8:
		return int16(val), true
	case int16:
		return val, true
	case int32:
		return int16(val), true
	case int64:
		return int16(val), true
	case uint8:
		return int16(val), true
	case uint16:
		return int16(val), true
	case uint32:
		return int16(val), true
	case uint64:
		return int16(val), true
	case float32:
		return int16(val), true
	case float64:
		return int16(val), true
	default:
		return 0, false
	}
}

func castToInt8(v interface{}) (int8, bool) {
	switch val := v.(type) {
	case int:
		return int8(val), true
	case int8:
		return val, true
	case int16:
		return int8(val), true
	case int32:
		return int8(val), true
	case int64:
		return int8(val), true
	case uint8:
		return int8(val), true
	case uint16:
		return int8(val), true
	case uint32:
		return int8(val), true
	case uint64:
		return int8(val), true
	case float32:
		return int8(val), true
	case float64:
		return int8(val), true
	default:
		return 0, false
	}
}

func castToUInt64(v interface{}) (uint64, bool) {
	switch val := v.(type) {
	case int:
		return uint64(val), true
	case int8:
		return uint64(val), true
	case int16:
		return uint64(val), true
	case int32:
		return uint64(val), true
	case int64:
		return uint64(val), true
	case uint8:
		return uint64(val), true
	case uint16:
		return uint64(val), true
	case uint32:
		return uint64(val), true
	case uint64:
		return val, true
	case float32:
		return uint64(val), true
	case float64:
		return uint64(val), true
	default:
		return 0, false
	}
}

func castToUInt32(v interface{}) (uint32, bool) {
	switch val := v.(type) {
	case int:
		return uint32(val), true
	case int8:
		return uint32(val), true
	case int16:
		return uint32(val), true
	case int32:
		return uint32(val), true
	case int64:
		return uint32(val), true
	case uint8:
		return uint32(val), true
	case uint16:
		return uint32(val), true
	case uint32:
		return val, true
	case uint64:
		return uint32(val), true
	case float32:
		return uint32(val), true
	case float64:
		return uint32(val), true
	default:
		return 0, false
	}
}

func castToUInt16(v interface{}) (uint16, bool) {
	switch val := v.(type) {
	case int:
		return uint16(val), true
	case int8:
		return uint16(val), true
	case int16:
		return uint16(val), true
	case int32:
		return uint16(val), true
	case int64:
		return uint16(val), true
	case uint8:
		return uint16(val), true
	case uint16:
		return val, true
	case uint32:
		return uint16(val), true
	case uint64:
		return uint16(val), true
	case float32:
		return uint16(val), true
	case float64:
		return uint16(val), true
	default:
		return 0, false
	}
}

func castToUInt8(v interface{}) (uint8, bool) {
	switch val := v.(type) {
	case int:
		return uint8(val), true
	case int8:
		return uint8(val), true
	case int16:
		return uint8(val), true
	case int32:
		return uint8(val), true
	case int64:
		return uint8(val), true
	case uint8:
		return val, true
	case uint16:
		return uint8(val), true
	case uint32:
		return uint8(val), true
	case uint64:
		return uint8(val), true
	case float32:
		return uint8(val), true
	case float64:
		return uint8(val), true
	default:
		return 0, false
	}
}

func castToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

func castToFloat32(v interface{}) (float32, bool) {
	switch val := v.(type) {
	case int:
		return float32(val), true
	case int8:
		return float32(val), true
	case int16:
		return float32(val), true
	case int32:
		return float32(val), true
	case int64:
		return float32(val), true
	case uint8:
		return float32(val), true
	case uint16:
		return float32(val), true
	case uint32:
		return float32(val), true
	case uint64:
		return float32(val), true
	case float32:
		return val, true
	case float64:
		return float32(val), true
	default:
		return 0, false
	}
}
