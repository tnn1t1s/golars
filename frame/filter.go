package frame

import (
	"fmt"

	"github.com/tnn1t1s/golars/expr"
)

// Filter returns a new DataFrame containing only rows where the expression evaluates to true.
func (df *DataFrame) Filter(filterExpr expr.Expr) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	filtered, ok, err := df.tryArrowComputeFilter(filterExpr)
	if err != nil {
		return nil, err
	}
	if ok {
		return filtered, nil
	}
	return nil, fmt.Errorf("arrow compute does not support filter expression %T", filterExpr)
}
