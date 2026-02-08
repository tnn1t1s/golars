package frame

import (
	_ "fmt"

	"github.com/tnn1t1s/golars/expr"
)

// Filter returns a new DataFrame containing only rows where the expression evaluates to true.
func (df *DataFrame) Filter(filterExpr expr.Expr) (*DataFrame, error) {
	panic("not implemented")

}
