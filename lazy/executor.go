package lazy

import (
	"fmt"

	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/io"
	"github.com/davidpalaitis/golars/series"
)

// Executor executes logical plans to produce DataFrames
type Executor struct {
	// TODO: Add configuration like concurrency level
}

// NewExecutor creates a new executor
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute executes a logical plan and returns a DataFrame
func (e *Executor) Execute(plan LogicalPlan) (*frame.DataFrame, error) {
	switch node := plan.(type) {
	case *ScanNode:
		return e.executeScan(node)
		
	case *FilterNode:
		input, err := e.Execute(node.input)
		if err != nil {
			return nil, err
		}
		return e.executeFilter(input, node.predicate)
		
	case *ProjectNode:
		input, err := e.Execute(node.input)
		if err != nil {
			return nil, err
		}
		return e.executeProject(input, node.exprs, node.aliases)
		
	case *GroupByNode:
		input, err := e.Execute(node.input)
		if err != nil {
			return nil, err
		}
		return e.executeGroupByWithNames(input, node)
		
	case *JoinNode:
		left, err := e.Execute(node.left)
		if err != nil {
			return nil, err
		}
		right, err := e.Execute(node.right)
		if err != nil {
			return nil, err
		}
		return e.executeJoin(left, right, node.on, node.how)
		
	case *SortNode:
		input, err := e.Execute(node.input)
		if err != nil {
			return nil, err
		}
		return e.executeSort(input, node.by, node.reverse)
		
	case *LimitNode:
		input, err := e.Execute(node.input)
		if err != nil {
			return nil, err
		}
		return e.executeLimit(input, node.limit)
		
	default:
		return nil, fmt.Errorf("unknown plan node type: %T", node)
	}
}

func (e *Executor) executeScan(node *ScanNode) (*frame.DataFrame, error) {
	switch src := node.source.(type) {
	case *DataFrameSource:
		// Get the DataFrame
		df := src.df
		
		// Apply column selection if specified
		if node.columns != nil {
			// Select only the requested columns
			return df.Select(node.columns...)
		}
		
		// Apply filters if any
		for _, filter := range node.filters {
			var err error
			df, err = e.executeFilter(df, filter)
			if err != nil {
				return nil, err
			}
		}
		
		return df, nil
		
	case *CSVSource:
		// Read CSV file
		// For now, we'll use the io package
		df, err := io.ReadCSV(src.path)
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV: %w", err)
		}
		
		// Apply column selection if specified
		if node.columns != nil {
			// Select only the requested columns
			df, err = df.Select(node.columns...)
			if err != nil {
				return nil, err
			}
		}
		
		// Apply filters if any
		for _, filter := range node.filters {
			df, err = e.executeFilter(df, filter)
			if err != nil {
				return nil, err
			}
		}
		
		return df, nil
		
	case *ParquetSource:
		// Read Parquet file with column selection
		var opts []io.ParquetReadOption
		if node.columns != nil {
			opts = append(opts, io.WithParquetColumns(node.columns))
		}
		
		df, err := io.ReadParquet(src.path, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to read Parquet: %w", err)
		}
		
		// Apply filters if any (Parquet already handles column selection)
		for _, filter := range node.filters {
			df, err = e.executeFilter(df, filter)
			if err != nil {
				return nil, err
			}
		}
		
		return df, nil
		
	default:
		return nil, fmt.Errorf("unknown data source type: %T", src)
	}
}

func (e *Executor) executeFilter(df *frame.DataFrame, predicate expr.Expr) (*frame.DataFrame, error) {
	// Use the DataFrame's Filter method which handles expression evaluation
	return df.Filter(predicate)
}

func (e *Executor) executeProject(df *frame.DataFrame, exprs []expr.Expr, aliases []string) (*frame.DataFrame, error) {
	// For now, we'll handle column expressions specially
	// In a full implementation, we'd evaluate arbitrary expressions
	
	columnNames := make([]string, 0, len(exprs))
	for i := range exprs {
		// For now, we'll use the alias directly
		columnNames = append(columnNames, aliases[i])
	}
	
	// Select the columns
	if len(columnNames) > 0 {
		return df.Select(columnNames...)
	}
	
	return df, nil
}

func (e *Executor) executeGroupBy(df *frame.DataFrame, keys []expr.Expr, aggs []expr.Expr) (*frame.DataFrame, error) {
	// Extract key column names
	keyNames := make([]string, len(keys))
	for i, key := range keys {
		// For now, assume keys are column expressions
		keyNames[i] = getExprName(key)
	}
	
	// Create group by
	gb, err := df.GroupBy(keyNames...)
	if err != nil {
		return nil, err
	}
	
	// Build aggregation map
	// For now, we'll use the expression string as the name
	// This is a limitation of the current design
	aggMap := make(map[string]expr.Expr)
	for _, agg := range aggs {
		// Generate a name based on the expression
		// This should match what the LazyFrame.Sum() etc methods use
		aggName := getExprName(agg)
		aggMap[aggName] = agg
	}
	
	// Execute aggregations
	// The GroupByWrapper.Agg returns a DataFrame directly
	return gb.Agg(aggMap)
}

// executeGroupByWithNames is used when we have explicit names for aggregations
func (e *Executor) executeGroupByWithNames(df *frame.DataFrame, node *GroupByNode) (*frame.DataFrame, error) {
	// Extract key column names
	keyNames := make([]string, len(node.keys))
	for i, key := range node.keys {
		keyNames[i] = getExprName(key)
	}
	
	// Create group by
	gb, err := df.GroupBy(keyNames...)
	if err != nil {
		return nil, err
	}
	
	// Build aggregation map with explicit names
	aggMap := make(map[string]expr.Expr)
	for i, agg := range node.aggs {
		aggMap[node.aggNames[i]] = agg
	}
	
	// Execute aggregations
	return gb.Agg(aggMap)
}

func (e *Executor) executeJoin(left, right *frame.DataFrame, on []string, how frame.JoinType) (*frame.DataFrame, error) {
	// For now, support single column joins
	if len(on) == 1 {
		return left.Join(right, on[0], how)
	}
	
	// Multi-column join
	return left.JoinOn(right, on, on, how)
}

func (e *Executor) executeSort(df *frame.DataFrame, by []expr.Expr, reverse []bool) (*frame.DataFrame, error) {
	// Extract column names from expressions
	columns := make([]string, len(by))
	for i, expr := range by {
		columns[i] = getExprName(expr)
	}
	
	// Convert reverse to SortOrder
	orders := make([]series.SortOrder, len(reverse))
	for i, rev := range reverse {
		if rev {
			orders[i] = series.Descending
		} else {
			orders[i] = series.Ascending
		}
	}
	
	// Create sort options
	opts := frame.SortOptions{
		Columns: columns,
		Orders:  orders,
	}
	
	return df.SortBy(opts)
}

func (e *Executor) executeLimit(df *frame.DataFrame, limit int) (*frame.DataFrame, error) {
	if limit >= df.Height() {
		return df, nil
	}
	
	// Take the first 'limit' rows
	indices := make([]int, limit)
	for i := 0; i < limit; i++ {
		indices[i] = i
	}
	
	return df.Take(indices)
}