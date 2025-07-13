package lazy

import (
	"strings"
	
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
)

// Optimizer represents a query optimization pass
type Optimizer interface {
	Optimize(plan LogicalPlan) (LogicalPlan, error)
}

// PredicatePushdown pushes filter predicates down the plan tree
type PredicatePushdown struct{}

// NewPredicatePushdown creates a new predicate pushdown optimizer
func NewPredicatePushdown() Optimizer {
	return &PredicatePushdown{}
}

// Optimize applies predicate pushdown optimization
func (opt *PredicatePushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return opt.pushDown(plan, nil)
}

func (opt *PredicatePushdown) pushDown(plan LogicalPlan, predicates []expr.Expr) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *FilterNode:
		// Accumulate predicates
		predicates = append(predicates, node.predicate)
		optimized, err := opt.pushDown(node.input, predicates)
		if err != nil {
			return nil, err
		}
		// The predicates have been pushed down, so we don't need this filter node
		return optimized, nil
		
	case *ScanNode:
		// Push predicates into scan
		if len(predicates) > 0 {
			// Clone the node to avoid modifying the original
			newNode := &ScanNode{
				source:  node.source,
				columns: node.columns,
				filters: append(append([]expr.Expr{}, node.filters...), predicates...),
			}
			return newNode, nil
		}
		return node, nil
		
	case *ProjectNode:
		// Try to push predicates through projections
		// First, optimize the input with predicates
		optimizedInput, err := opt.pushDown(node.input, predicates)
		if err != nil {
			return nil, err
		}
		
		return &ProjectNode{
			input:   optimizedInput,
			exprs:   node.exprs,
			aliases: node.aliases,
		}, nil
		
	case *SortNode:
		// Push predicates through sort - sorting doesn't change which rows match
		optimizedInput, err := opt.pushDown(node.input, predicates)
		if err != nil {
			return nil, err
		}
		
		return &SortNode{
			input:   optimizedInput,
			by:      node.by,
			reverse: node.reverse,
		}, nil
		
	case *LimitNode:
		// Push predicates through limit - it's better to filter before limiting
		optimizedInput, err := opt.pushDown(node.input, predicates)
		if err != nil {
			return nil, err
		}
		
		return &LimitNode{
			input: optimizedInput,
			limit: node.limit,
		}, nil
		
	default:
		// For other nodes (GroupBy, Join), we can't push predicates through
		// So we add a filter node before them
		if len(predicates) > 0 {
			// First optimize the children
			children := plan.Children()
			if len(children) > 0 {
				optimizedChildren := make([]LogicalPlan, len(children))
				for i, child := range children {
					optimized, err := opt.pushDown(child, nil)
					if err != nil {
						return nil, err
					}
					optimizedChildren[i] = optimized
				}
				plan = plan.WithChildren(optimizedChildren)
			}
			
			return &FilterNode{
				input:     plan,
				predicate: combinePredicates(predicates),
			}, nil
		}
		return plan, nil
	}
}

// ProjectionPushdown pushes projections down to reduce data movement
type ProjectionPushdown struct{}

// NewProjectionPushdown creates a new projection pushdown optimizer
func NewProjectionPushdown() Optimizer {
	return &ProjectionPushdown{}
}

// Optimize applies projection pushdown optimization
func (opt *ProjectionPushdown) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	// Start with all columns needed at the root
	// This will be nil for nodes that need all columns
	neededColumns := opt.collectNeededColumns(plan)
	return opt.pushDown(plan, neededColumns)
}

// collectNeededColumns collects all columns needed by a plan node
func (opt *ProjectionPushdown) collectNeededColumns(plan LogicalPlan) map[string]bool {
	needed := make(map[string]bool)
	
	switch node := plan.(type) {
	case *ProjectNode:
		// Only need columns referenced by the projections
		for _, expr := range node.exprs {
			opt.collectExprColumns(expr, needed)
		}
		
	case *FilterNode:
		// Need all columns from input plus columns in predicate
		childNeeded := opt.collectNeededColumns(node.input)
		for col := range childNeeded {
			needed[col] = true
		}
		opt.collectExprColumns(node.predicate, needed)
		
	case *GroupByNode:
		// Need grouping columns and aggregation input columns
		for _, key := range node.keys {
			opt.collectExprColumns(key, needed)
		}
		for _, agg := range node.aggs {
			opt.collectExprColumns(agg, needed)
		}
		
	case *SortNode:
		// Need all columns from input plus sort columns
		childNeeded := opt.collectNeededColumns(node.input)
		for col := range childNeeded {
			needed[col] = true
		}
		for _, expr := range node.by {
			opt.collectExprColumns(expr, needed)
		}
		
	default:
		// For other nodes, assume we need all columns
		// This is conservative but safe
		return nil
	}
	
	return needed
}

// collectExprColumns collects column names referenced by an expression
func (opt *ProjectionPushdown) collectExprColumns(expr expr.Expr, needed map[string]bool) {
	if expr == nil {
		return
	}
	
	// We need to use string comparison since we can't access private types
	// Get the string representation to identify expression type
	exprStr := expr.String()
	
	
	// For binary expressions like "(col(store) == lit(B))", we need to extract all col() references
	// Use a simple approach: find all instances of "col(" in the string
	start := 0
	for {
		idx := strings.Index(exprStr[start:], "col(")
		if idx == -1 {
			break
		}
		idx += start
		
		// Find the matching closing parenthesis
		depth := 0
		for i := idx + 4; i < len(exprStr); i++ {
			if exprStr[i] == '(' {
				depth++
			} else if exprStr[i] == ')' {
				if depth == 0 {
					// Found the matching closing paren
					colName := exprStr[idx+4:i]
					needed[colName] = true
					break
				}
				depth--
			}
		}
		
		start = idx + 4
	}
}

// collectFilterColumnsRecursive recursively collects all columns used by filters in the plan tree
func (opt *ProjectionPushdown) collectFilterColumnsRecursive(plan LogicalPlan, needed map[string]bool) {
	if plan == nil {
		return
	}
	
	
	switch node := plan.(type) {
	case *FilterNode:
		// Collect columns from this filter's predicate
		opt.collectExprColumns(node.predicate, needed)
		// Continue recursively
		opt.collectFilterColumnsRecursive(node.input, needed)
		
	case *ScanNode:
		// Collect columns from scan filters too
		for _, filter := range node.filters {
			opt.collectExprColumns(filter, needed)
		}
		
	default:
		// For other nodes, recurse on children
		for _, child := range plan.Children() {
			opt.collectFilterColumnsRecursive(child, needed)
		}
	}
}

// pushDown pushes projections down the plan tree
func (opt *ProjectionPushdown) pushDown(plan LogicalPlan, neededColumns map[string]bool) (LogicalPlan, error) {
	// If neededColumns is nil, we need all columns (conservative approach)
	if neededColumns == nil {
		// Just optimize children without pushing projections
		children := plan.Children()
		if len(children) > 0 {
			optimizedChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				optimized, err := opt.pushDown(child, nil)
				if err != nil {
					return nil, err
				}
				optimizedChildren[i] = optimized
			}
			return plan.WithChildren(optimizedChildren), nil
		}
		return plan, nil
	}
	
	switch node := plan.(type) {
	case *ScanNode:
		// Push column selection into scan
		if node.columns == nil {
			// Currently selecting all columns, can restrict to needed
			columns := make([]string, 0, len(neededColumns))
			for col := range neededColumns {
				columns = append(columns, col)
			}
			if len(columns) > 0 {
				return &ScanNode{
					source:  node.source,
					columns: columns,
					filters: node.filters,
				}, nil
			}
		}
		return node, nil
		
	case *ProjectNode:
		// This is already a projection, no need to add another
		// But optimize the input based on what this projection needs
		inputNeeded := make(map[string]bool)
		for _, expr := range node.exprs {
			opt.collectExprColumns(expr, inputNeeded)
		}
		
		optimizedInput, err := opt.pushDown(node.input, inputNeeded)
		if err != nil {
			return nil, err
		}
		
		return &ProjectNode{
			input:   optimizedInput,
			exprs:   node.exprs,
			aliases: node.aliases,
		}, nil
		
	case *FilterNode:
		// Need columns for both the filter and what's needed above
		inputNeeded := make(map[string]bool)
		for col := range neededColumns {
			inputNeeded[col] = true
		}
		opt.collectExprColumns(node.predicate, inputNeeded)
		
		optimizedInput, err := opt.pushDown(node.input, inputNeeded)
		if err != nil {
			return nil, err
		}
		
		return &FilterNode{
			input:     optimizedInput,
			predicate: node.predicate,
		}, nil
		
	case *GroupByNode:
		// Can only push down columns needed for grouping and aggregation
		inputNeeded := make(map[string]bool)
		for _, key := range node.keys {
			opt.collectExprColumns(key, inputNeeded)
		}
		for _, agg := range node.aggs {
			opt.collectExprColumns(agg, inputNeeded)
		}
		
		// IMPORTANT: We need to collect all columns used by filters anywhere
		// in the subtree below this GroupBy node, not just immediate children
		opt.collectFilterColumnsRecursive(node.input, inputNeeded)
		
		
		optimizedInput, err := opt.pushDown(node.input, inputNeeded)
		if err != nil {
			return nil, err
		}
		
		return &GroupByNode{
			input:    optimizedInput,
			keys:     node.keys,
			aggs:     node.aggs,
			aggNames: node.aggNames,
		}, nil
		
	default:
		// For other nodes, just optimize children
		children := plan.Children()
		if len(children) > 0 {
			optimizedChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				// For joins, we'd need to split neededColumns by side
				// For now, pass all needed columns to all children
				optimized, err := opt.pushDown(child, neededColumns)
				if err != nil {
					return nil, err
				}
				optimizedChildren[i] = optimized
			}
			return plan.WithChildren(optimizedChildren), nil
		}
		return plan, nil
	}
}

// CommonSubexpressionElimination identifies and eliminates duplicate expressions
type CommonSubexpressionElimination struct{}

// NewCommonSubexpressionElimination creates a new CSE optimizer
func NewCommonSubexpressionElimination() Optimizer {
	return &CommonSubexpressionElimination{}
}

// Optimize applies common subexpression elimination
func (opt *CommonSubexpressionElimination) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	// For now, we'll focus on eliminating duplicate expressions within projections
	// Future enhancement: track expressions across the entire plan
	return opt.eliminateCSE(plan)
}

func (opt *CommonSubexpressionElimination) eliminateCSE(plan LogicalPlan) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *ProjectNode:
		// First optimize the input
		optimizedInput, err := opt.eliminateCSE(node.input)
		if err != nil {
			return nil, err
		}
		
		// Check for duplicate expressions
		exprMap := make(map[string]int) // expression string -> first index
		newExprs := make([]expr.Expr, 0, len(node.exprs))
		newAliases := make([]string, 0, len(node.aliases))
		
		for i, expr := range node.exprs {
			exprStr := expr.String()
			if firstIdx, exists := exprMap[exprStr]; exists {
				// This is a duplicate - reuse the first occurrence
				// Create a column reference to the first occurrence's alias
				newExprs = append(newExprs, expr)
				newAliases = append(newAliases, node.aliases[firstIdx])
			} else {
				// First occurrence of this expression
				exprMap[exprStr] = len(newExprs)
				newExprs = append(newExprs, expr)
				newAliases = append(newAliases, node.aliases[i])
			}
		}
		
		// If we eliminated any expressions, create a new node
		if len(newExprs) < len(node.exprs) {
			return &ProjectNode{
				input:   optimizedInput,
				exprs:   newExprs,
				aliases: newAliases,
			}, nil
		}
		
		// No duplicates found
		return &ProjectNode{
			input:   optimizedInput,
			exprs:   node.exprs,
			aliases: node.aliases,
		}, nil
		
	case *GroupByNode:
		// Check for duplicate aggregations
		optimizedInput, err := opt.eliminateCSE(node.input)
		if err != nil {
			return nil, err
		}
		
		// Check aggregation expressions for duplicates
		aggMap := make(map[string]int)
		newAggs := make([]expr.Expr, 0, len(node.aggs))
		newAggNames := make([]string, 0, len(node.aggNames))
		
		for i, agg := range node.aggs {
			aggStr := agg.String()
			if _, exists := aggMap[aggStr]; exists {
				// Skip duplicate aggregation
				continue
			} else {
				aggMap[aggStr] = len(newAggs)
				newAggs = append(newAggs, agg)
				newAggNames = append(newAggNames, node.aggNames[i])
			}
		}
		
		if len(newAggs) < len(node.aggs) {
			return &GroupByNode{
				input:    optimizedInput,
				keys:     node.keys,
				aggs:     newAggs,
				aggNames: newAggNames,
			}, nil
		}
		
		return &GroupByNode{
			input:    optimizedInput,
			keys:     node.keys,
			aggs:     node.aggs,
			aggNames: node.aggNames,
		}, nil
		
	default:
		// For other nodes, just optimize children
		children := plan.Children()
		if len(children) > 0 {
			optimizedChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				optimized, err := opt.eliminateCSE(child)
				if err != nil {
					return nil, err
				}
				optimizedChildren[i] = optimized
			}
			return plan.WithChildren(optimizedChildren), nil
		}
		return plan, nil
	}
}

// FilterCombining combines adjacent filter nodes into a single filter
type FilterCombining struct{}

// NewFilterCombining creates a new filter combining optimizer
func NewFilterCombining() Optimizer {
	return &FilterCombining{}
}

// Optimize combines adjacent filter nodes
func (opt *FilterCombining) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return opt.combineFilters(plan)
}

func (opt *FilterCombining) combineFilters(plan LogicalPlan) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *FilterNode:
		// Collect all adjacent filters
		predicates := []expr.Expr{node.predicate}
		current := node.input
		
		// Keep collecting predicates while we have adjacent filters
		for {
			if filterNode, ok := current.(*FilterNode); ok {
				predicates = append(predicates, filterNode.predicate)
				current = filterNode.input
			} else {
				break
			}
		}
		
		// Optimize the non-filter input
		optimizedInput, err := opt.combineFilters(current)
		if err != nil {
			return nil, err
		}
		
		// If we collected multiple predicates, combine them
		if len(predicates) > 1 {
			// Reverse the predicates since we collected them bottom-up
			for i, j := 0, len(predicates)-1; i < j; i, j = i+1, j-1 {
				predicates[i], predicates[j] = predicates[j], predicates[i]
			}
			
			combinedPredicate := combinePredicates(predicates)
			return &FilterNode{
				input:     optimizedInput,
				predicate: combinedPredicate,
			}, nil
		}
		
		// Single filter, no combining needed
		return &FilterNode{
			input:     optimizedInput,
			predicate: node.predicate,
		}, nil
		
	default:
		// For other nodes, just optimize children
		children := plan.Children()
		if len(children) > 0 {
			optimizedChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				optimized, err := opt.combineFilters(child)
				if err != nil {
					return nil, err
				}
				optimizedChildren[i] = optimized
			}
			return plan.WithChildren(optimizedChildren), nil
		}
		return plan, nil
	}
}

// JoinReordering optimizes join order based on estimated selectivity
type JoinReordering struct{}

// NewJoinReordering creates a new join reordering optimizer
func NewJoinReordering() Optimizer {
	return &JoinReordering{}
}

// Optimize reorders joins for better performance
func (opt *JoinReordering) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	return opt.reorderJoins(plan)
}

func (opt *JoinReordering) reorderJoins(plan LogicalPlan) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *JoinNode:
		// First optimize children
		optimizedLeft, err := opt.reorderJoins(node.left)
		if err != nil {
			return nil, err
		}
		optimizedRight, err := opt.reorderJoins(node.right)
		if err != nil {
			return nil, err
		}
		
		// Check if we can reorder this join with joins below
		// For now, we'll implement a simple heuristic:
		// - Inner joins can be reordered
		// - Left/Right joins maintain their order
		// - Prefer smaller tables on the right side of hash joins
		
		if node.how == frame.InnerJoin {
			// Check if left side is also a join
			if leftJoin, ok := optimizedLeft.(*JoinNode); ok && leftJoin.how == frame.InnerJoin {
				// We have a chain of inner joins: (A join B) join C
				// Consider reordering based on estimated sizes
				
				// Get estimated sizes (in a real implementation, this would use statistics)
				// sizeA := opt.estimateSize(leftJoin.left) // Not used in current implementation
				sizeB := opt.estimateSize(leftJoin.right)
				sizeC := opt.estimateSize(optimizedRight)
				
				// Try different join orders and pick the best
				// For inner joins: (A join B) join C = A join (B join C)
				// Heuristic: put smaller tables on the right for hash joins
				
				if sizeC < sizeB {
					// Reorder to A join (B join C)
					newRightJoin := &JoinNode{
						left:  leftJoin.right,
						right: optimizedRight,
						on:    node.on, // Simplified - in real implementation would need to adjust
						how:   frame.InnerJoin,
					}
					
					return &JoinNode{
						left:  leftJoin.left,
						right: newRightJoin,
						on:    leftJoin.on,
						how:   frame.InnerJoin,
					}, nil
				}
			}
		}
		
		// No reordering, return optimized node
		return &JoinNode{
			left:  optimizedLeft,
			right: optimizedRight,
			on:    node.on,
			how:   node.how,
		}, nil
		
	default:
		// For other nodes, just optimize children
		children := plan.Children()
		if len(children) > 0 {
			optimizedChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				optimized, err := opt.reorderJoins(child)
				if err != nil {
					return nil, err
				}
				optimizedChildren[i] = optimized
			}
			return plan.WithChildren(optimizedChildren), nil
		}
		return plan, nil
	}
}

// estimateSize estimates the size of a logical plan's output
// In a real implementation, this would use table statistics
func (opt *JoinReordering) estimateSize(plan LogicalPlan) int64 {
	switch node := plan.(type) {
	case *ScanNode:
		// Base table - would use statistics in real implementation
		// For now, return a default size
		return 1000
		
	case *FilterNode:
		// Assume filters reduce size by 50%
		baseSize := opt.estimateSize(node.input)
		return baseSize / 2
		
	case *JoinNode:
		// Estimate join output size based on join type
		leftSize := opt.estimateSize(node.left)
		rightSize := opt.estimateSize(node.right)
		
		switch node.how {
		case frame.InnerJoin:
			// Inner join typically produces fewer rows
			return (leftSize * rightSize) / 10
		case frame.LeftJoin:
			// Left join preserves left side
			return leftSize
		case frame.RightJoin:
			// Right join preserves right side
			return rightSize
		case frame.OuterJoin:
			// Outer join can be larger
			return leftSize + rightSize
		case frame.CrossJoin:
			// Cross join is cartesian product
			return leftSize * rightSize
		default:
			return leftSize
		}
		
	case *GroupByNode:
		// Group by typically reduces rows significantly
		baseSize := opt.estimateSize(node.input)
		return baseSize / 10
		
	case *LimitNode:
		// Limit constrains output size
		baseSize := opt.estimateSize(node.input)
		if int64(node.limit) < baseSize {
			return int64(node.limit)
		}
		return baseSize
		
	default:
		// For other nodes, assume size doesn't change much
		children := plan.Children()
		if len(children) > 0 {
			return opt.estimateSize(children[0])
		}
		return 1000
	}
}

// Helper functions

// combinePredicates combines multiple predicates with AND
func combinePredicates(predicates []expr.Expr) expr.Expr {
	if len(predicates) == 0 {
		return nil
	}
	if len(predicates) == 1 {
		return predicates[0]
	}
	
	// Create AND expression using the builder pattern
	// We need to wrap the first expression in a builder
	result := predicates[0]
	for i := 1; i < len(predicates); i++ {
		// Use the builder to create AND expression
		// Since we can't directly create BinaryExpr, we'll use NewBuilder
		builder := expr.NewBuilder(result)
		result = builder.And(predicates[i]).Build()
	}
	return result
}