package frame

import (
	_ "fmt"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/group"
)

// GroupByWrapper wraps the group.GroupBy to provide DataFrame-returning methods
type GroupByWrapper struct {
	gb *group.GroupBy
}

// GroupBy creates a grouped DataFrame for aggregation operations
func (df *DataFrame) GroupBy(columns ...string) (*GroupByWrapper, error) {
	panic("not implemented")

	// Validate columns exist

	// Validate at least one column specified

}

// Sum performs sum aggregation on specified columns
func (gbw *GroupByWrapper) Sum(columns ...string) (*DataFrame, error) {
	panic("not implemented")

}

// Mean performs mean aggregation on specified columns
func (gbw *GroupByWrapper) Mean(columns ...string) (*DataFrame, error) {
	panic("not implemented")

}

// Count returns the count of rows in each group
func (gbw *GroupByWrapper) Count() (*DataFrame, error) {
	panic("not implemented")

}

// Min performs min aggregation on specified columns
func (gbw *GroupByWrapper) Min(columns ...string) (*DataFrame, error) {
	panic("not implemented")

}

// Max performs max aggregation on specified columns
func (gbw *GroupByWrapper) Max(columns ...string) (*DataFrame, error) {
	panic("not implemented")

}

// Agg performs multiple aggregations on the grouped data
func (gbw *GroupByWrapper) Agg(aggregations map[string]expr.Expr) (*DataFrame, error) {
	panic("not implemented")

}
