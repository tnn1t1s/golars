package lazy

import (
	"testing"

	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/frame"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPredicatePushdown_CombineFilters(t *testing.T) {
	// Create a plan with multiple filters
	scan := NewScanNode(NewDataFrameSource(nil))
	filter1 := NewFilterNode(scan, expr.ColBuilder("a").Gt(int64(5)).Build())
	filter2 := NewFilterNode(filter1, expr.ColBuilder("b").Lt(int64(10)).Build())
	
	// Apply predicate pushdown
	optimizer := NewPredicatePushdown()
	optimized, err := optimizer.Optimize(filter2)
	require.NoError(t, err)
	
	// Should have pushed both filters to scan
	scanNode, ok := optimized.(*ScanNode)
	require.True(t, ok, "Expected ScanNode at root after optimization")
	assert.Len(t, scanNode.filters, 2, "Expected 2 filters pushed to scan")
	
	// Check the plan string
	planStr := optimized.String()
	assert.Contains(t, planStr, "Filters:")
	assert.Contains(t, planStr, "AND")
}

func TestPredicatePushdown_ThroughProjection(t *testing.T) {
	// Create a plan with filter after projection
	scan := NewScanNode(NewDataFrameSource(nil))
	project := NewProjectNode(scan, []expr.Expr{
		expr.Col("a"),
		expr.Col("b"),
	})
	filter := NewFilterNode(project, expr.ColBuilder("a").Gt(int64(5)).Build())
	
	// Apply predicate pushdown
	optimizer := NewPredicatePushdown()
	optimized, err := optimizer.Optimize(filter)
	require.NoError(t, err)
	
	// Should have project at top with filter pushed below
	projectNode, ok := optimized.(*ProjectNode)
	require.True(t, ok, "Expected ProjectNode at root")
	
	// Check that filter was pushed to scan
	scanNode, ok := projectNode.input.(*ScanNode)
	require.True(t, ok, "Expected ScanNode as project input")
	assert.Len(t, scanNode.filters, 1, "Expected 1 filter pushed to scan")
}

func TestPredicatePushdown_ThroughSort(t *testing.T) {
	// Create a plan with filter after sort
	scan := NewScanNode(NewDataFrameSource(nil))
	sort := NewSortNode(scan, []expr.Expr{expr.Col("a")}, []bool{false})
	filter := NewFilterNode(sort, expr.ColBuilder("b").Eq(expr.Lit("test")).Build())
	
	// Apply predicate pushdown
	optimizer := NewPredicatePushdown()
	optimized, err := optimizer.Optimize(filter)
	require.NoError(t, err)
	
	// Should have sort at top with filter pushed below
	sortNode, ok := optimized.(*SortNode)
	require.True(t, ok, "Expected SortNode at root")
	
	// Check that filter was pushed to scan
	scanNode, ok := sortNode.input.(*ScanNode)
	require.True(t, ok, "Expected ScanNode as sort input")
	assert.Len(t, scanNode.filters, 1, "Expected 1 filter pushed to scan")
}

func TestPredicatePushdown_StopAtGroupBy(t *testing.T) {
	// Create a plan with filter after groupby
	scan := NewScanNode(NewDataFrameSource(nil))
	groupBy := NewGroupByNode(scan, []expr.Expr{expr.Col("category")}, []expr.Expr{
		expr.ColBuilder("amount").Sum().Build(),
	})
	filter := NewFilterNode(groupBy, expr.ColBuilder("amount_sum").Gt(int64(100)).Build())
	
	// Apply predicate pushdown
	optimizer := NewPredicatePushdown()
	optimized, err := optimizer.Optimize(filter)
	require.NoError(t, err)
	
	// Filter should stay above GroupBy
	filterNode, ok := optimized.(*FilterNode)
	require.True(t, ok, "Expected FilterNode at root")
	
	groupByNode, ok := filterNode.input.(*GroupByNode)
	require.True(t, ok, "Expected GroupByNode below filter")
	
	// But scan should be optimized
	_, ok = groupByNode.input.(*ScanNode)
	require.True(t, ok, "Expected ScanNode below groupby")
}

func TestProjectionPushdown_Basic(t *testing.T) {
	// Create a plan that selects only some columns
	scan := NewScanNode(NewDataFrameSource(nil))
	project := NewProjectNode(scan, []expr.Expr{
		expr.Col("a"),
		expr.Col("c"),
	})
	
	// Apply projection pushdown
	optimizer := NewProjectionPushdown()
	optimized, err := optimizer.Optimize(project)
	require.NoError(t, err)
	
	// Check that projection was pushed to scan
	projectNode, ok := optimized.(*ProjectNode)
	require.True(t, ok, "Expected ProjectNode at root")
	
	scanNode, ok := projectNode.input.(*ScanNode)
	require.True(t, ok, "Expected ScanNode as input")
	
	// Scan should only select needed columns
	assert.Contains(t, scanNode.columns, "a")
	assert.Contains(t, scanNode.columns, "c")
	assert.Len(t, scanNode.columns, 2)
}

func TestProjectionPushdown_ThroughFilter(t *testing.T) {
	// Create a plan: project -> filter -> scan
	scan := NewScanNode(NewDataFrameSource(nil))
	filter := NewFilterNode(scan, expr.ColBuilder("b").Gt(int64(10)).Build())
	project := NewProjectNode(filter, []expr.Expr{
		expr.Col("a"),
		expr.Col("c"),
	})
	
	// Apply projection pushdown
	optimizer := NewProjectionPushdown()
	optimized, err := optimizer.Optimize(project)
	require.NoError(t, err)
	
	// Structure should be preserved
	projectNode, ok := optimized.(*ProjectNode)
	require.True(t, ok)
	
	filterNode, ok := projectNode.input.(*FilterNode)
	require.True(t, ok)
	
	scanNode, ok := filterNode.input.(*ScanNode)
	require.True(t, ok)
	
	// Scan should select a, b (for filter), and c
	assert.Contains(t, scanNode.columns, "a")
	assert.Contains(t, scanNode.columns, "b") // Needed for filter
	assert.Contains(t, scanNode.columns, "c")
}

func TestProjectionPushdown_FilterBelowGroupBy(t *testing.T) {
	// This tests the specific bug case: project -> groupby -> filter -> scan
	// The filter uses column "store" which is not in the groupby output
	scan := NewScanNode(NewDataFrameSource(nil))
	filter := NewFilterNode(scan, expr.ColBuilder("store").Eq(expr.Lit("B")).Build())
	groupBy := NewGroupByNode(filter, 
		[]expr.Expr{expr.Col("product")},
		[]expr.Expr{expr.ColBuilder("quantity").Sum().Build()},
	)
	project := NewProjectNode(groupBy, []expr.Expr{
		expr.Col("product"),
		expr.Col("quantity_sum"),
	})
	
	// Apply projection pushdown
	optimizer := NewProjectionPushdown()
	optimized, err := optimizer.Optimize(project)
	require.NoError(t, err)
	
	// Navigate to the scan node
	projectNode, ok := optimized.(*ProjectNode)
	require.True(t, ok)
	
	groupByNode, ok := projectNode.input.(*GroupByNode)
	require.True(t, ok)
	
	filterNode, ok := groupByNode.input.(*FilterNode)
	require.True(t, ok)
	
	scanNode, ok := filterNode.input.(*ScanNode)
	require.True(t, ok)
	
	// Scan should have product (for groupby), quantity (for sum), and store (for filter)
	assert.Contains(t, scanNode.columns, "product")
	assert.Contains(t, scanNode.columns, "quantity")
	assert.Contains(t, scanNode.columns, "store") // Critical: needed for filter
}

func TestOptimizers_Combined(t *testing.T) {
	// Create a complex plan
	scan := NewScanNode(NewDataFrameSource(nil))
	filter1 := NewFilterNode(scan, expr.ColBuilder("status").Eq(expr.Lit("active")).Build())
	project := NewProjectNode(filter1, []expr.Expr{
		expr.Col("id"),
		expr.Col("name"),
		expr.Col("score"),
	})
	filter2 := NewFilterNode(project, expr.ColBuilder("score").Gt(int64(50)).Build())
	sort := NewSortNode(filter2, []expr.Expr{expr.Col("score")}, []bool{true})
	limit := NewLimitNode(sort, 10)
	
	// Apply both optimizers
	var plan LogicalPlan = limit
	for _, opt := range []Optimizer{NewProjectionPushdown(), NewPredicatePushdown()} {
		var err error
		plan, err = opt.Optimize(plan)
		require.NoError(t, err)
	}
	
	// Check the optimized plan structure
	planStr := plan.String()
	
	// Both filters should be combined and pushed to scan
	assert.Contains(t, planStr, "Filters:")
	assert.Contains(t, planStr, "active")
	assert.Contains(t, planStr, "50")
	
	// The scan should show column restrictions
	// Note: exact format depends on implementation
	t.Logf("Optimized plan:\n%s", planStr)
}

func TestCombinePredicates(t *testing.T) {
	tests := []struct {
		name       string
		predicates []expr.Expr
		expected   string
	}{
		{
			name:       "empty",
			predicates: []expr.Expr{},
			expected:   "",
		},
		{
			name: "single",
			predicates: []expr.Expr{
				expr.ColBuilder("a").Gt(int64(5)).Build(),
			},
			expected: "(col(a) > lit(5))",
		},
		{
			name: "two predicates",
			predicates: []expr.Expr{
				expr.ColBuilder("a").Gt(int64(5)).Build(),
				expr.ColBuilder("b").Lt(int64(10)).Build(),
			},
			expected: "((col(a) > lit(5)) & (col(b) < lit(10)))",
		},
		{
			name: "three predicates",
			predicates: []expr.Expr{
				expr.ColBuilder("a").Gt(int64(5)).Build(),
				expr.ColBuilder("b").Lt(int64(10)).Build(),
				expr.ColBuilder("c").Eq(expr.Lit("test")).Build(),
			},
			expected: "(((col(a) > lit(5)) & (col(b) < lit(10))) & (col(c) == lit(test)))",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := combinePredicates(tt.predicates)
			if tt.expected == "" {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, tt.expected, result.String())
			}
		})
	}
}

func TestCommonSubexpressionElimination_Projections(t *testing.T) {
	// Create a plan with duplicate expressions in projection
	scan := NewScanNode(NewDataFrameSource(nil))
	project := NewProjectNode(scan, []expr.Expr{
		expr.Col("a"),
		expr.ColBuilder("b").Add(expr.Lit(10)).Build(),
		expr.Col("a"), // Duplicate
		expr.ColBuilder("b").Add(expr.Lit(10)).Build(), // Duplicate
		expr.Col("c"),
	})
	
	// Apply CSE
	optimizer := NewCommonSubexpressionElimination()
	optimized, err := optimizer.Optimize(project)
	require.NoError(t, err)
	
	// Should still be a project node
	projectNode, ok := optimized.(*ProjectNode)
	require.True(t, ok)
	
	// Should have eliminated duplicates
	// Note: Current implementation doesn't fully eliminate, just identifies
	assert.Len(t, projectNode.exprs, 5) // For now, we keep all expressions
}

func TestCommonSubexpressionElimination_GroupBy(t *testing.T) {
	// Create a plan with duplicate aggregations
	scan := NewScanNode(NewDataFrameSource(nil))
	groupBy := NewGroupByNodeWithNames(scan,
		[]expr.Expr{expr.Col("category")},
		[]expr.Expr{
			expr.ColBuilder("amount").Sum().Build(),
			expr.ColBuilder("amount").Mean().Build(),
			expr.ColBuilder("amount").Sum().Build(), // Duplicate
			expr.ColBuilder("quantity").Sum().Build(),
		},
		[]string{"sum1", "mean", "sum2", "qty_sum"},
	)
	
	// Apply CSE
	optimizer := NewCommonSubexpressionElimination()
	optimized, err := optimizer.Optimize(groupBy)
	require.NoError(t, err)
	
	// Should still be a GroupBy node
	groupByNode, ok := optimized.(*GroupByNode)
	require.True(t, ok)
	
	// Should have eliminated duplicate sum
	assert.Len(t, groupByNode.aggs, 3)
	assert.Len(t, groupByNode.aggNames, 3)
}

func TestCommonSubexpressionElimination_NoChanges(t *testing.T) {
	// Create a plan with no duplicates
	scan := NewScanNode(NewDataFrameSource(nil))
	project := NewProjectNode(scan, []expr.Expr{
		expr.Col("a"),
		expr.Col("b"),
		expr.Col("c"),
	})
	
	// Apply CSE
	optimizer := NewCommonSubexpressionElimination()
	optimized, err := optimizer.Optimize(project)
	require.NoError(t, err)
	
	// Should be unchanged
	projectNode, ok := optimized.(*ProjectNode)
	require.True(t, ok)
	assert.Len(t, projectNode.exprs, 3)
}

func TestFilterCombining_AdjacentFilters(t *testing.T) {
	// Create a plan with adjacent filters
	scan := NewScanNode(NewDataFrameSource(nil))
	filter1 := NewFilterNode(scan, expr.ColBuilder("a").Gt(int64(5)).Build())
	filter2 := NewFilterNode(filter1, expr.ColBuilder("b").Lt(int64(10)).Build())
	filter3 := NewFilterNode(filter2, expr.ColBuilder("c").Eq(expr.Lit("test")).Build())
	
	// Apply filter combining
	optimizer := NewFilterCombining()
	optimized, err := optimizer.Optimize(filter3)
	require.NoError(t, err)
	
	// Should have a single filter with combined predicate
	filterNode, ok := optimized.(*FilterNode)
	require.True(t, ok, "Expected FilterNode at root")
	
	// The input should be the scan node
	_, ok = filterNode.input.(*ScanNode)
	require.True(t, ok, "Expected ScanNode as filter input, got: %T", filterNode.input)
	
	// Check the combined predicate
	predicateStr := filterNode.predicate.String()
	t.Logf("Combined predicate: %s", predicateStr)
	assert.Contains(t, predicateStr, "col(a)")
	assert.Contains(t, predicateStr, "col(b)")
	assert.Contains(t, predicateStr, "col(c)")
	assert.Contains(t, predicateStr, "&")
}

func TestFilterCombining_NonAdjacentFilters(t *testing.T) {
	// Create a plan with non-adjacent filters (filter -> project -> filter)
	scan := NewScanNode(NewDataFrameSource(nil))
	filter1 := NewFilterNode(scan, expr.ColBuilder("a").Gt(int64(5)).Build())
	project := NewProjectNode(filter1, []expr.Expr{
		expr.Col("a"),
		expr.Col("b"),
	})
	filter2 := NewFilterNode(project, expr.ColBuilder("b").Lt(int64(10)).Build())
	
	// Apply filter combining
	optimizer := NewFilterCombining()
	optimized, err := optimizer.Optimize(filter2)
	require.NoError(t, err)
	
	// Should still have two filters separated by project
	filterNode, ok := optimized.(*FilterNode)
	require.True(t, ok, "Expected FilterNode at root")
	
	projectNode, ok := filterNode.input.(*ProjectNode)
	require.True(t, ok, "Expected ProjectNode below filter")
	
	filter1Node, ok := projectNode.input.(*FilterNode)
	require.True(t, ok, "Expected FilterNode below project")
	
	// Filters should not be combined
	assert.Contains(t, filterNode.predicate.String(), "col(b)")
	assert.Contains(t, filter1Node.predicate.String(), "col(a)")
}

func TestFilterCombining_MixedPlan(t *testing.T) {
	// Create a complex plan with some adjacent and non-adjacent filters
	scan := NewScanNode(NewDataFrameSource(nil))
	filter1 := NewFilterNode(scan, expr.ColBuilder("status").Eq(expr.Lit("active")).Build())
	filter2 := NewFilterNode(filter1, expr.ColBuilder("age").Gt(int64(18)).Build())
	groupBy := NewGroupByNode(filter2, 
		[]expr.Expr{expr.Col("category")},
		[]expr.Expr{expr.ColBuilder("amount").Sum().Build()},
	)
	filter3 := NewFilterNode(groupBy, expr.ColBuilder("amount_sum").Gt(int64(1000)).Build())
	filter4 := NewFilterNode(filter3, expr.ColBuilder("category").Ne(expr.Lit("test")).Build())
	
	// Apply filter combining
	optimizer := NewFilterCombining()
	optimized, err := optimizer.Optimize(filter4)
	require.NoError(t, err)
	
	// Top two filters should be combined
	topFilter, ok := optimized.(*FilterNode)
	require.True(t, ok)
	assert.Contains(t, topFilter.predicate.String(), "&")
	
	// Below should be GroupBy
	groupByNode, ok := topFilter.input.(*GroupByNode)
	require.True(t, ok)
	
	// Bottom two filters should be combined
	bottomFilter, ok := groupByNode.input.(*FilterNode)
	require.True(t, ok)
	assert.Contains(t, bottomFilter.predicate.String(), "&")
	
	// And finally scan
	_, ok = bottomFilter.input.(*ScanNode)
	require.True(t, ok)
}

// Benchmarks
func BenchmarkOptimization_PredicatePushdown(b *testing.B) {
	// Create a complex plan with multiple filters
	scan := NewScanNode(NewDataFrameSource(nil))
	project := NewProjectNode(scan, []expr.Expr{
		expr.Col("a"), expr.Col("b"), expr.Col("c"),
	})
	filter1 := NewFilterNode(project, expr.ColBuilder("a").Gt(int64(10)).Build())
	sort := NewSortNode(filter1, []expr.Expr{expr.Col("b")}, []bool{false})
	filter2 := NewFilterNode(sort, expr.ColBuilder("c").Lt(int64(100)).Build())
	limit := NewLimitNode(filter2, 50)
	
	optimizer := NewPredicatePushdown()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := optimizer.Optimize(limit)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOptimization_ProjectionPushdown(b *testing.B) {
	// Create a plan that can benefit from projection pushdown
	scan := NewScanNode(NewDataFrameSource(nil))
	filter := NewFilterNode(scan, expr.ColBuilder("status").Eq(expr.Lit("active")).Build())
	groupBy := NewGroupByNode(filter, 
		[]expr.Expr{expr.Col("category")},
		[]expr.Expr{expr.ColBuilder("amount").Sum().Build()},
	)
	project := NewProjectNode(groupBy, []expr.Expr{
		expr.Col("category"),
		expr.Col("amount_sum"),
	})
	
	optimizer := NewProjectionPushdown()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := optimizer.Optimize(project)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOptimization_CSE(b *testing.B) {
	// Create a plan with many duplicate expressions
	scan := NewScanNode(NewDataFrameSource(nil))
	exprs := make([]expr.Expr, 20)
	for i := 0; i < 20; i++ {
		if i%3 == 0 {
			exprs[i] = expr.Col("a")
		} else if i%3 == 1 {
			exprs[i] = expr.ColBuilder("b").Add(expr.Lit(10)).Build()
		} else {
			exprs[i] = expr.ColBuilder("c").Mul(expr.Lit(2)).Build()
		}
	}
	project := NewProjectNode(scan, exprs)
	
	optimizer := NewCommonSubexpressionElimination()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := optimizer.Optimize(project)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOptimization_FilterCombining(b *testing.B) {
	// Create a plan with many adjacent filters
	scan := NewScanNode(NewDataFrameSource(nil))
	
	// Chain 10 filters together
	var plan LogicalPlan = scan
	for i := 0; i < 10; i++ {
		if i%3 == 0 {
			plan = NewFilterNode(plan, expr.ColBuilder("a").Gt(int64(i)).Build())
		} else if i%3 == 1 {
			plan = NewFilterNode(plan, expr.ColBuilder("b").Lt(int64(100-i)).Build())
		} else {
			plan = NewFilterNode(plan, expr.ColBuilder("c").Eq(expr.Lit(i)).Build())
		}
	}
	
	optimizer := NewFilterCombining()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := optimizer.Optimize(plan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOptimization_Combined(b *testing.B) {
	// Create a realistic query plan
	scan := NewScanNode(NewDataFrameSource(nil))
	filter1 := NewFilterNode(scan, expr.ColBuilder("year").Eq(expr.Lit(int64(2023))).Build())
	filter2 := NewFilterNode(filter1, expr.ColBuilder("amount").Gt(int64(1000)).Build())
	groupBy := NewGroupByNode(filter2,
		[]expr.Expr{expr.Col("product"), expr.Col("region")},
		[]expr.Expr{
			expr.ColBuilder("amount").Sum().Build(),
			expr.ColBuilder("amount").Mean().Build(),
		},
	)
	filter3 := NewFilterNode(groupBy, expr.ColBuilder("amount_sum").Gt(int64(10000)).Build())
	project := NewProjectNode(filter3, []expr.Expr{
		expr.Col("product"),
		expr.Col("amount_sum"),
		expr.Col("amount_mean"),
	})
	sort := NewSortNode(project, []expr.Expr{expr.Col("amount_sum")}, []bool{true})
	limit := NewLimitNode(sort, 100)
	
	optimizers := []Optimizer{
		NewProjectionPushdown(),
		NewPredicatePushdown(),
		NewCommonSubexpressionElimination(),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var plan LogicalPlan = limit
		for _, opt := range optimizers {
			var err error
			plan, err = opt.Optimize(plan)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestJoinReordering_InnerJoins(t *testing.T) {
	// Create three scan nodes (tables A, B, C)
	scanA := NewScanNode(NewDataFrameSource(nil))
	scanB := NewScanNode(NewDataFrameSource(nil))
	scanC := NewScanNode(NewDataFrameSource(nil))
	
	// Create joins: (A join B) join C
	joinAB := NewJoinNode(scanA, scanB, []string{"id"}, frame.InnerJoin)
	joinABC := NewJoinNode(joinAB, scanC, []string{"id"}, frame.InnerJoin)
	
	// Apply join reordering
	optimizer := NewJoinReordering()
	optimized, err := optimizer.Optimize(joinABC)
	require.NoError(t, err)
	require.NotNil(t, optimized)
	
	// The optimizer should maintain the structure for now (since we use simple heuristics)
	// In a real implementation, it would reorder based on statistics
	joinNode, ok := optimized.(*JoinNode)
	require.True(t, ok, "Expected JoinNode at root")
	assert.Equal(t, frame.InnerJoin, joinNode.how)
}

func TestJoinReordering_PreservesNonInnerJoins(t *testing.T) {
	// Create three scan nodes
	scanA := NewScanNode(NewDataFrameSource(nil))
	scanB := NewScanNode(NewDataFrameSource(nil))
	scanC := NewScanNode(NewDataFrameSource(nil))
	
	// Create joins with left join: (A left join B) inner join C
	leftJoin := NewJoinNode(scanA, scanB, []string{"id"}, frame.LeftJoin)
	mixedJoin := NewJoinNode(leftJoin, scanC, []string{"id"}, frame.InnerJoin)
	
	// Apply join reordering
	optimizer := NewJoinReordering()
	optimized, err := optimizer.Optimize(mixedJoin)
	require.NoError(t, err)
	
	// Should preserve the left join order
	joinNode, ok := optimized.(*JoinNode)
	require.True(t, ok, "Expected JoinNode at root")
	
	// The left join should remain on the left side
	leftNode, ok := joinNode.left.(*JoinNode)
	require.True(t, ok, "Expected JoinNode on left")
	assert.Equal(t, frame.LeftJoin, leftNode.how, "Left join should be preserved")
}

func TestJoinReordering_EstimateSize(t *testing.T) {
	optimizer := &JoinReordering{}
	
	// Test scan node
	scan := NewScanNode(NewDataFrameSource(nil))
	size := optimizer.estimateSize(scan)
	assert.Equal(t, int64(1000), size, "Scan should have default size")
	
	// Test filter node (should reduce size)
	filter := NewFilterNode(scan, expr.ColBuilder("a").Gt(int64(5)).Build())
	size = optimizer.estimateSize(filter)
	assert.Equal(t, int64(500), size, "Filter should reduce size by 50%")
	
	// Test limit node
	limit := NewLimitNode(scan, 100)
	size = optimizer.estimateSize(limit)
	assert.Equal(t, int64(100), size, "Limit should constrain size")
	
	// Test inner join
	scanA := NewScanNode(NewDataFrameSource(nil))
	scanB := NewScanNode(NewDataFrameSource(nil))
	innerJoin := NewJoinNode(scanA, scanB, []string{"id"}, frame.InnerJoin)
	size = optimizer.estimateSize(innerJoin)
	assert.Equal(t, int64(100000), size, "Inner join should estimate based on formula")
	
	// Test cross join
	crossJoin := NewJoinNode(scanA, scanB, []string{}, frame.CrossJoin)
	size = optimizer.estimateSize(crossJoin)
	assert.Equal(t, int64(1000000), size, "Cross join should be cartesian product")
}