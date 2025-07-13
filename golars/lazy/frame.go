package lazy

import (
	"fmt"
	"sync"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/frame"
)

// LazyFrame represents a lazy computation on a DataFrame
type LazyFrame struct {
	plan       LogicalPlan
	optimizers []Optimizer
	mu         sync.RWMutex
}

// NewLazyFrame creates a new LazyFrame from a logical plan
func NewLazyFrame(plan LogicalPlan) *LazyFrame {
	return &LazyFrame{
		plan: plan,
		optimizers: []Optimizer{
			NewPredicatePushdown(),
			NewProjectionPushdown(),
			NewCommonSubexpressionElimination(),
			NewFilterCombining(),
			NewJoinReordering(),
			// TODO: Implement these optimizers
			// NewColumnPruning(),
		},
	}
}

// NewLazyFrameFromDataFrame creates a LazyFrame from an existing DataFrame
func NewLazyFrameFromDataFrame(df *frame.DataFrame) *LazyFrame {
	return NewLazyFrame(NewScanNode(NewDataFrameSource(df)))
}

// Select applies a projection to select specific columns or expressions
func (lf *LazyFrame) Select(exprs ...expr.Expr) *LazyFrame {
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return &LazyFrame{
		plan:       NewProjectNode(lf.plan, exprs),
		optimizers: lf.optimizers,
	}
}

// SelectColumns is a convenience method to select columns by name
func (lf *LazyFrame) SelectColumns(columns ...string) *LazyFrame {
	exprs := make([]expr.Expr, len(columns))
	for i, col := range columns {
		exprs[i] = expr.Col(col)
	}
	return lf.Select(exprs...)
}

// Filter applies a predicate to filter rows
func (lf *LazyFrame) Filter(predicate expr.Expr) *LazyFrame {
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return &LazyFrame{
		plan:       NewFilterNode(lf.plan, predicate),
		optimizers: lf.optimizers,
	}
}

// GroupBy creates a lazy group by operation
func (lf *LazyFrame) GroupBy(columns ...string) *LazyGroupBy {
	keyExprs := make([]expr.Expr, len(columns))
	for i, col := range columns {
		keyExprs[i] = expr.Col(col)
	}

	return &LazyGroupBy{
		lf:   lf,
		keys: keyExprs,
	}
}

// Join performs a lazy join with another LazyFrame
func (lf *LazyFrame) Join(other *LazyFrame, on string, how frame.JoinType) *LazyFrame {
	return lf.JoinOn(other, []string{on}, []string{on}, how)
}

// JoinOn performs a lazy join with specific columns
func (lf *LazyFrame) JoinOn(other *LazyFrame, leftOn []string, rightOn []string, how frame.JoinType) *LazyFrame {
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	if len(leftOn) != len(rightOn) {
		panic("leftOn and rightOn must have the same length")
	}

	// For now, use the first column pair
	// TODO: Support multi-column joins in the logical plan
	return &LazyFrame{
		plan:       NewJoinNode(lf.plan, other.plan, leftOn, how),
		optimizers: lf.optimizers,
	}
}

// Sort sorts the LazyFrame by one or more columns
func (lf *LazyFrame) Sort(by string, reverse bool) *LazyFrame {
	return lf.SortBy([]string{by}, []bool{reverse})
}

// SortBy sorts by multiple columns with individual sort orders
func (lf *LazyFrame) SortBy(columns []string, reverse []bool) *LazyFrame {
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	if len(columns) != len(reverse) {
		panic("columns and reverse must have the same length")
	}

	exprs := make([]expr.Expr, len(columns))
	for i, col := range columns {
		exprs[i] = expr.Col(col)
	}

	return &LazyFrame{
		plan:       NewSortNode(lf.plan, exprs, reverse),
		optimizers: lf.optimizers,
	}
}

// Limit limits the number of rows
func (lf *LazyFrame) Limit(n int) *LazyFrame {
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return &LazyFrame{
		plan:       NewLimitNode(lf.plan, n),
		optimizers: lf.optimizers,
	}
}

// Head is an alias for Limit
func (lf *LazyFrame) Head(n int) *LazyFrame {
	return lf.Limit(n)
}

// Schema returns the expected output schema
func (lf *LazyFrame) Schema() (*datatypes.Schema, error) {
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return lf.plan.Schema()
}

// Explain returns a string representation of the logical plan
func (lf *LazyFrame) Explain() string {
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return lf.plan.String()
}

// ExplainOptimized returns the optimized plan as a string
func (lf *LazyFrame) ExplainOptimized() (string, error) {
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	optimized := lf.plan
	for _, optimizer := range lf.optimizers {
		var err error
		optimized, err = optimizer.Optimize(optimized)
		if err != nil {
			return "", fmt.Errorf("optimization failed: %w", err)
		}
	}

	return optimized.String(), nil
}

// Collect executes the lazy operations and returns a DataFrame
func (lf *LazyFrame) Collect() (*frame.DataFrame, error) {
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	// Optimize the plan
	optimized := lf.plan
	for _, optimizer := range lf.optimizers {
		var err error
		optimized, err = optimizer.Optimize(optimized)
		if err != nil {
			return nil, fmt.Errorf("optimization failed: %w", err)
		}
	}

	// Execute the plan
	executor := NewExecutor()
	return executor.Execute(optimized)
}

// Clone creates a copy of the LazyFrame
func (lf *LazyFrame) Clone() *LazyFrame {
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return &LazyFrame{
		plan:       lf.plan,
		optimizers: lf.optimizers,
	}
}

// WithOptimizers sets custom optimizers
func (lf *LazyFrame) WithOptimizers(optimizers ...Optimizer) *LazyFrame {
	lf.mu.Lock()
	defer lf.mu.Unlock()

	return &LazyFrame{
		plan:       lf.plan,
		optimizers: optimizers,
	}
}

// LazyGroupBy represents a lazy group by operation
type LazyGroupBy struct {
	lf   *LazyFrame
	keys []expr.Expr
}

// Agg applies aggregations to the grouped data
func (lgb *LazyGroupBy) Agg(aggs map[string]expr.Expr) *LazyFrame {
	lgb.lf.mu.RLock()
	defer lgb.lf.mu.RUnlock()

	// Convert map to slices, preserving the names
	aggExprs := make([]expr.Expr, 0, len(aggs))
	aggNames := make([]string, 0, len(aggs))
	for name, agg := range aggs {
		aggExprs = append(aggExprs, agg)
		aggNames = append(aggNames, name)
	}

	return &LazyFrame{
		plan:       NewGroupByNodeWithNames(lgb.lf.plan, lgb.keys, aggExprs, aggNames),
		optimizers: lgb.lf.optimizers,
	}
}

// Sum aggregates by summing the specified columns
func (lgb *LazyGroupBy) Sum(columns ...string) *LazyFrame {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		sumExpr := expr.Col(col).Sum()
		// Use the same naming convention as the eager GroupBy
		aggs[col+"_sum"] = sumExpr
	}
	return lgb.Agg(aggs)
}

// Mean aggregates by computing the mean of the specified columns
func (lgb *LazyGroupBy) Mean(columns ...string) *LazyFrame {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		aggs[col+"_mean"] = expr.Col(col).Mean()
	}
	return lgb.Agg(aggs)
}

// Count returns the count of rows in each group
func (lgb *LazyGroupBy) Count() *LazyFrame {
	aggs := map[string]expr.Expr{
		"count": expr.Col("*").Count(),
	}
	return lgb.Agg(aggs)
}

// Min aggregates by finding the minimum of the specified columns
func (lgb *LazyGroupBy) Min(columns ...string) *LazyFrame {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		aggs[col+"_min"] = expr.Col(col).Min()
	}
	return lgb.Agg(aggs)
}

// Max aggregates by finding the maximum of the specified columns
func (lgb *LazyGroupBy) Max(columns ...string) *LazyFrame {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		aggs[col+"_max"] = expr.Col(col).Max()
	}
	return lgb.Agg(aggs)
}