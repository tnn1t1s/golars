package frame

import (
	"fmt"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/series"
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
			for i := range mask {
				mask[i] = val
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
	for i := 0; i < df.height; i++ {
		// Handle nulls - any comparison with null is false
		if leftValues[i] == nil || rightValues[i] == nil {
			mask[i] = false
			continue
		}

		mask[i] = compareValues(leftValues[i], rightValues[i], e.Op())
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
	for i := 0; i < df.height; i++ {
		switch e.Op() {
		case expr.OpAnd:
			mask[i] = leftMask[i] && rightMask[i]
		case expr.OpOr:
			mask[i] = leftMask[i] || rightMask[i]
		}
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
		for i := range innerMask {
			mask[i] = !innerMask[i]
		}
		return mask, nil

	case expr.OpIsNull, expr.OpIsNotNull:
		values, err := df.evaluateExprValues(e.Expr())
		if err != nil {
			return nil, err
		}
		mask := make([]bool, len(values))
		for i := range values {
			if e.Op() == expr.OpIsNull {
				mask[i] = values[i] == nil
			} else {
				mask[i] = values[i] != nil
			}
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
		for i := 0; i < df.height; i++ {
			values[i] = col.Get(i)
		}
		return values, nil

	case *expr.LiteralExpr:
		// Broadcast literal value
		values := make([]interface{}, df.height)
		for i := range values {
			values[i] = ex.Value()
		}
		return values, nil

	default:
		return nil, fmt.Errorf("unsupported expression type for value evaluation: %T", e)
	}
}

// seriesToBoolMask converts a boolean series to a boolean mask
func (df *DataFrame) seriesToBoolMask(s series.Series) ([]bool, error) {
	mask := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		val := s.Get(i)
		if val == nil {
			mask[i] = false
		} else if b, ok := val.(bool); ok {
			mask[i] = b
		} else {
			return nil, fmt.Errorf("series must contain boolean values for filtering")
		}
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
	
	for i, idx := range indices {
		if idx < 0 || idx >= s.Len() {
			return nil, fmt.Errorf("index %d out of bounds", idx)
		}
		values[i] = s.Get(idx)
		validity[i] = s.IsValid(idx)
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