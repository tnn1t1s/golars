package frame

import (
	"fmt"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/group"
)

// GroupByWrapper wraps the group.GroupBy to provide DataFrame-returning methods
type GroupByWrapper struct {
	gb *group.GroupBy
}

// GroupBy creates a grouped DataFrame for aggregation operations
func (df *DataFrame) GroupBy(columns ...string) (*GroupByWrapper, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("at least one column must be specified for GroupBy")
	}

	// Validate columns exist
	for _, name := range columns {
		if !df.HasColumn(name) {
			return nil, fmt.Errorf("column %q not found", name)
		}
	}

	gb, err := group.NewGroupBy(df, columns)
	if err != nil {
		return nil, err
	}

	return &GroupByWrapper{gb: gb}, nil
}

// Sum performs sum aggregation on specified columns
func (gbw *GroupByWrapper) Sum(columns ...string) (*DataFrame, error) {
	result, err := gbw.gb.Sum(columns...)
	if err != nil {
		return nil, err
	}
	return NewDataFrame(result.Columns...)
}

// Mean performs mean aggregation on specified columns
func (gbw *GroupByWrapper) Mean(columns ...string) (*DataFrame, error) {
	result, err := gbw.gb.Mean(columns...)
	if err != nil {
		return nil, err
	}
	return NewDataFrame(result.Columns...)
}

// Count returns the count of rows in each group
func (gbw *GroupByWrapper) Count() (*DataFrame, error) {
	result, err := gbw.gb.Count()
	if err != nil {
		return nil, err
	}
	return NewDataFrame(result.Columns...)
}

// Min performs min aggregation on specified columns
func (gbw *GroupByWrapper) Min(columns ...string) (*DataFrame, error) {
	result, err := gbw.gb.Min(columns...)
	if err != nil {
		return nil, err
	}
	return NewDataFrame(result.Columns...)
}

// Max performs max aggregation on specified columns
func (gbw *GroupByWrapper) Max(columns ...string) (*DataFrame, error) {
	result, err := gbw.gb.Max(columns...)
	if err != nil {
		return nil, err
	}
	return NewDataFrame(result.Columns...)
}

// Agg performs multiple aggregations on the grouped data
func (gbw *GroupByWrapper) Agg(aggregations map[string]expr.Expr) (*DataFrame, error) {
	result, err := gbw.gb.Agg(aggregations)
	if err != nil {
		return nil, err
	}
	return NewDataFrame(result.Columns...)
}
