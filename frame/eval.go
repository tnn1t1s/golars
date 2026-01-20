package frame

import (
	"fmt"
	"strings"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/window"
	"github.com/tnn1t1s/golars/series"
)

// evaluateExpr evaluates an expression and returns a series
func (df *DataFrame) evaluateExpr(e expr.Expr) (series.Series, error) {
	switch ex := e.(type) {
	case *expr.ColumnExpr:
		// Return the column
		return df.Column(ex.Name())

	case *expr.LiteralExpr:
		// Create a series filled with the literal value
		return df.createLiteralSeries(ex.Value())

	case *window.Expr:
		// Handle window expressions
		return df.evaluateWindowExpr(ex)

	case *expr.AggExpr:
		// Handle aggregate expressions (future enhancement)
		return nil, fmt.Errorf("aggregate expressions not yet supported in WithColumn")

	case *expr.BinaryExpr:
		// Handle binary expressions
		return df.evaluateBinaryOpExpr(ex)

	case *expr.UnaryExpr:
		// Handle unary expressions
		return df.evaluateUnaryOpExpr(ex)

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", e)
	}
}

// evaluateWindowExpr evaluates a window expression
func (df *DataFrame) evaluateWindowExpr(we *window.Expr) (series.Series, error) {
	// Validate the window expression
	if err := we.Validate(); err != nil {
		return nil, err
	}

	spec := we.GetSpec()
	function := we.GetFunction()

	// Create partitions based on the window specification
	partitions, err := df.createPartitions(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to create partitions: %w", err)
	}

	// If ORDER BY is specified, sort within each partition
	if spec.HasOrderBy() {
		for _, partition := range partitions {
			// Apply ordering to the partition
			if wp, ok := partition.(*window.WindowPartition); ok {
				if err := wp.ApplyOrder(spec.GetOrderBy()); err != nil {
					return nil, fmt.Errorf("failed to apply order: %w", err)
				}
			}
		}
	}

	// Apply the window function to each partition and collect results
	partitionResults := make([]partitionResult, 0, len(partitions))
	for _, partition := range partitions {
		result, err := function.Compute(partition)
		if err != nil {
			return nil, fmt.Errorf("failed to compute window function: %w", err)
		}
		partitionResults = append(partitionResults, partitionResult{
			indices: partition.Indices(),
			series:  result,
		})
	}

	// Combine results from all partitions
	if len(partitionResults) == 0 {
		return nil, fmt.Errorf("no results from window function")
	}

	// Single partition case
	if len(partitionResults) == 1 {
		return partitionResults[0].series, nil
	}

	// Multiple partitions: merge results maintaining original row order
	return df.mergePartitionResults(partitionResults, function.Name())
}

// partitionResult holds the result of a window function on a partition
type partitionResult struct {
	indices []int
	series  series.Series
}

// mergePartitionResults combines results from multiple partitions in original row order
func (df *DataFrame) mergePartitionResults(results []partitionResult, name string) (series.Series, error) {
	// Determine the output data type from the first result
	if len(results) == 0 {
		return nil, fmt.Errorf("no partition results to merge")
	}
	dataType := results[0].series.DataType()

	// Create a result slice with the same length as the DataFrame
	// We'll fill it with the values from each partition
	switch dataType.String() {
	case "i32":
		return df.mergeInt32Results(results, name), nil
	case "i64":
		return df.mergeInt64Results(results, name), nil
	case "f64":
		return df.mergeFloat64Results(results, name), nil
	case "str":
		return df.mergeStringResults(results, name), nil
	default:
		return nil, fmt.Errorf("unsupported data type for merging: %s", dataType)
	}
}

// mergeInt32Results merges int32 results from partitions
func (df *DataFrame) mergeInt32Results(results []partitionResult, name string) series.Series {
	merged := make([]int32, df.height)

	for _, pr := range results {
		for i, idx := range pr.indices {
			if idx < len(merged) {
				merged[idx] = pr.series.Get(i).(int32)
			}
		}
	}

	return series.NewInt32Series(name, merged)
}

// mergeInt64Results merges int64 results from partitions
func (df *DataFrame) mergeInt64Results(results []partitionResult, name string) series.Series {
	merged := make([]int64, df.height)

	for _, pr := range results {
		for i, idx := range pr.indices {
			if idx < len(merged) {
				merged[idx] = pr.series.Get(i).(int64)
			}
		}
	}

	return series.NewInt64Series(name, merged)
}

// mergeFloat64Results merges float64 results from partitions
func (df *DataFrame) mergeFloat64Results(results []partitionResult, name string) series.Series {
	merged := make([]float64, df.height)

	for _, pr := range results {
		for i, idx := range pr.indices {
			if idx < len(merged) {
				merged[idx] = pr.series.Get(i).(float64)
			}
		}
	}

	return series.NewFloat64Series(name, merged)
}

// mergeStringResults merges string results from partitions
func (df *DataFrame) mergeStringResults(results []partitionResult, name string) series.Series {
	merged := make([]string, df.height)

	for _, pr := range results {
		for i, idx := range pr.indices {
			if idx < len(merged) {
				merged[idx] = pr.series.Get(i).(string)
			}
		}
	}

	return series.NewStringSeries(name, merged)
}

// createPartitions creates partitions based on the window specification
func (df *DataFrame) createPartitions(spec *window.Spec) ([]window.Partition, error) {
	partitionBy := spec.GetPartitionBy()

	if len(partitionBy) == 0 {
		// Single partition containing all rows
		indices := make([]int, df.height)
		for i := range indices {
			indices[i] = i
		}

		// Create series map for the partition
		seriesMap := make(map[string]series.Series)
		for i, col := range df.columns {
			seriesMap[df.schema.Fields[i].Name] = col
		}

		partition := window.NewPartition(seriesMap, indices)
		return []window.Partition{partition}, nil
	}

	// Use GroupBy logic for partitioning
	groups, err := df.partitionByColumns(partitionBy)
	if err != nil {
		return nil, err
	}

	// Convert groups to partitions
	partitions := make([]window.Partition, 0, len(groups))

	// Create series map once
	seriesMap := make(map[string]series.Series)
	for i, col := range df.columns {
		seriesMap[df.schema.Fields[i].Name] = col
	}

	for _, indices := range groups {
		partition := window.NewPartition(seriesMap, indices)
		partitions = append(partitions, partition)
	}

	return partitions, nil
}

// partitionByColumns groups rows by the specified columns
func (df *DataFrame) partitionByColumns(columns []string) (map[string][]int, error) {
	// Get the series for grouping columns
	groupSeries := make([]series.Series, len(columns))
	for i, col := range columns {
		s, err := df.Column(col)
		if err != nil {
			return nil, fmt.Errorf("column %s not found", col)
		}
		groupSeries[i] = s
	}

	// Build groups by hashing row values
	groups := make(map[string][]int)

	for i := 0; i < df.height; i++ {
		// Build a key from the group column values
		keyParts := make([]string, len(groupSeries))
		for j, s := range groupSeries {
			keyParts[j] = fmt.Sprintf("%v", s.Get(i))
		}
		key := strings.Join(keyParts, "|")

		groups[key] = append(groups[key], i)
	}

	return groups, nil
}

// createLiteralSeries creates a series filled with a literal value
func (df *DataFrame) createLiteralSeries(value interface{}) (series.Series, error) {
	// Create a series with the same length as the DataFrame
	switch v := value.(type) {
	case bool:
		values := make([]bool, df.height)
		for i := range values {
			values[i] = v
		}
		return series.NewBooleanSeries("literal", values), nil

	case int:
		values := make([]int64, df.height)
		for i := range values {
			values[i] = int64(v)
		}
		return series.NewInt64Series("literal", values), nil

	case int64:
		values := make([]int64, df.height)
		for i := range values {
			values[i] = v
		}
		return series.NewInt64Series("literal", values), nil

	case float64:
		values := make([]float64, df.height)
		for i := range values {
			values[i] = v
		}
		return series.NewFloat64Series("literal", values), nil

	case string:
		values := make([]string, df.height)
		for i := range values {
			values[i] = v
		}
		return series.NewStringSeries("literal", values), nil

	default:
		return nil, fmt.Errorf("unsupported literal type: %T", value)
	}
}

// evaluateBinaryOpExpr evaluates a binary operation expression
func (df *DataFrame) evaluateBinaryOpExpr(e *expr.BinaryExpr) (series.Series, error) {
	// Evaluate left and right expressions
	_, err := df.evaluateExpr(e.Left())
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate left expression: %w", err)
	}

	_, err = df.evaluateExpr(e.Right())
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate right expression: %w", err)
	}

	// Perform the operation
	// This would use the compute kernels
	// For now, return an error
	return nil, fmt.Errorf("binary operations not yet implemented in WithColumn")
}

// evaluateUnaryOpExpr evaluates a unary operation expression
func (df *DataFrame) evaluateUnaryOpExpr(e *expr.UnaryExpr) (series.Series, error) {
	// Evaluate the inner expression
	_, err := df.evaluateExpr(e.Expr())
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate inner expression: %w", err)
	}

	// Perform the operation
	// This would use the compute kernels
	// For now, return an error
	return nil, fmt.Errorf("unary operations not yet implemented in WithColumn")
}
