package frame

import (
	"fmt"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/series"
)

// Filter returns a new DataFrame containing only rows where the expression evaluates to true.
func (df *DataFrame) Filter(filterExpr expr.Expr) (*DataFrame, error) {
	// Try Arrow compute path first
	if result, ok, err := df.tryArrowComputeFilter(filterExpr); ok {
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// Evaluate expression to get boolean mask
	mask, err := df.evaluateExpr(filterExpr)
	if err != nil {
		return nil, err
	}

	// Verify mask is boolean
	isBool := false
	for i := 0; i < mask.Len(); i++ {
		if mask.IsNull(i) {
			continue
		}
		if _, ok := mask.Get(i).(bool); ok {
			isBool = true
		}
		break
	}
	if mask.Len() > 0 && !isBool {
		return nil, fmt.Errorf("filter expression must evaluate to a boolean series")
	}

	// Collect indices where mask is true
	n := mask.Len()
	indices := make([]int, 0, n)
	for i := 0; i < n; i++ {
		if mask.IsNull(i) {
			continue
		}
		val, ok := mask.Get(i).(bool)
		if ok && val {
			indices = append(indices, i)
		}
	}

	if len(indices) == 0 {
		// Return empty DataFrame with same schema
		cols := make([]series.Series, len(df.columns))
		for i, col := range df.columns {
			cols[i] = col.Head(0)
		}
		return NewDataFrame(cols...)
	}

	// Take rows at selected indices
	cols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		taken, ok := series.TakeFast(col, indices)
		if ok {
			cols[i] = taken
		} else {
			cols[i] = col.Take(indices)
		}
	}
	if len(cols) == 0 {
		return NewDataFrame()
	}
	return NewDataFrame(cols...)
}

// FilterByMask returns a new DataFrame containing only rows where the boolean mask is true.
func (df *DataFrame) FilterByMask(mask []bool) (*DataFrame, error) {
	if len(mask) != df.height {
		return nil, fmt.Errorf("mask length %d does not match DataFrame height %d", len(mask), df.height)
	}

	indices := make([]int, 0, df.height)
	for i, v := range mask {
		if v {
			indices = append(indices, i)
		}
	}

	cols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		taken, ok := series.TakeFast(col, indices)
		if ok {
			cols[i] = taken
		} else {
			cols[i] = col.Take(indices)
		}
	}
	return NewDataFrame(cols...)
}
