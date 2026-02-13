package lazy

import (
	"context"
	"fmt"
	"strings"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/internal/window"
	"github.com/tnn1t1s/golars/series"
)

// PhysicalPlan executes to produce a DataFrame.
type PhysicalPlan interface {
	Execute(ctx context.Context) (*frame.DataFrame, error)
}

// PhysicalScan reads from a data source.
type PhysicalScan struct {
	Source ExecutableSource
}

func (p *PhysicalScan) Execute(_ context.Context) (*frame.DataFrame, error) {
	return p.Source.DataFrame()
}

// PhysicalFilter applies a predicate.
type PhysicalFilter struct {
	Input     PhysicalPlan
	Predicate NodeID
	Arena     *Arena
}

func (p *PhysicalFilter) Execute(ctx context.Context) (*frame.DataFrame, error) {
	df, err := p.Input.Execute(ctx)
	if err != nil {
		return nil, err
	}
	filterExpr, err := exprFromNode(p.Arena, p.Predicate)
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}
	return df.Filter(filterExpr)
}

// PhysicalProjection selects columns.
type PhysicalProjection struct {
	Input PhysicalPlan
	Exprs []NodeID
	Arena *Arena
}

func (p *PhysicalProjection) Execute(ctx context.Context) (*frame.DataFrame, error) {
	df, err := p.Input.Execute(ctx)
	if err != nil {
		return nil, err
	}

	// Check if all expressions are simple column references
	cols, colErr := projectionColumns(p.Arena, p.Exprs)
	if colErr == nil {
		return df.Select(cols...)
	}

	// Fall back to expression evaluation for complex projections
	var resultCols []series.Series
	for _, eid := range p.Exprs {
		node, ok := p.Arena.Get(eid)
		if !ok {
			return nil, fmt.Errorf("invalid expression node")
		}

		// Handle window expressions directly
		if node.Kind == KindWindow {
			s, werr := executeWindowExpr(p.Arena, eid, df)
			if werr != nil {
				return nil, werr
			}
			resultCols = append(resultCols, s)
			continue
		}

		// Handle alias wrapping a window
		if node.Kind == KindAlias && len(node.Children) > 0 {
			child, cok := p.Arena.Get(node.Children[0])
			if cok && child.Kind == KindWindow {
				s, werr := executeWindowExpr(p.Arena, node.Children[0], df)
				if werr != nil {
					return nil, werr
				}
				alias := node.Payload.(Alias)
				name, _ := p.Arena.String(alias.NameID)
				resultCols = append(resultCols, s.Rename(name))
				continue
			}
		}

		e, eerr := exprFromNode(p.Arena, eid)
		if eerr != nil {
			return nil, eerr
		}

		name := OutputName(p.Arena, eid)
		result, eerr := evaluateExprOnFrame(df, e)
		if eerr != nil {
			return nil, eerr
		}
		resultCols = append(resultCols, result.Rename(name))
	}

	return frame.NewDataFrame(resultCols...)
}

// PhysicalAggregate groups and aggregates.
type PhysicalAggregate struct {
	Input PhysicalPlan
	Keys  []NodeID
	Aggs  []NodeID
	Arena *Arena
}

func (p *PhysicalAggregate) Execute(ctx context.Context) (*frame.DataFrame, error) {
	df, err := p.Input.Execute(ctx)
	if err != nil {
		return nil, err
	}

	// Get group-by key column names
	keyNames := make([]string, len(p.Keys))
	for i, kid := range p.Keys {
		keyNames[i] = OutputName(p.Arena, kid)
	}

	// Build aggregation map
	aggMap, err := aggExprs(p.Arena, p.Aggs)
	if err != nil {
		return nil, err
	}

	gb, err := df.GroupBy(keyNames...)
	if err != nil {
		return nil, err
	}

	return gb.Agg(aggMap)
}

func projectionColumns(a *Arena, exprs []NodeID) ([]string, error) {
	cols := make([]string, len(exprs))
	for i, id := range exprs {
		node, ok := a.Get(id)
		if !ok {
			return nil, fmt.Errorf("invalid node")
		}
		if node.Kind != KindColumn {
			return nil, fmt.Errorf("non-column expression at index %d", i)
		}
		col := node.Payload.(Column)
		name, ok := a.String(col.NameID)
		if !ok {
			return nil, fmt.Errorf("invalid column name at index %d", i)
		}
		cols[i] = name
	}
	return cols, nil
}

func aggExprs(a *Arena, aggs []NodeID) (map[string]expr.Expr, error) {
	result := make(map[string]expr.Expr)
	for _, id := range aggs {
		node, ok := a.Get(id)
		if !ok {
			return nil, fmt.Errorf("invalid agg node")
		}
		if node.Kind != KindAgg {
			return nil, fmt.Errorf("expected agg node, got %d", node.Kind)
		}
		agg := node.Payload.(Agg)

		// Get the column name from the child
		colName := "unknown"
		if len(node.Children) > 0 {
			colName = OutputName(a, node.Children[0])
		}

		outputName := colName + "_" + aggName(agg.Op)
		e := aggExprFromOp(colName, agg.Op)
		result[outputName] = e
	}
	return result, nil
}

func aggExprFromOp(name string, op AggOp) expr.Expr {
	col := expr.Col(name)
	switch op {
	case AggSum:
		return col.Sum()
	case AggMean:
		return col.Mean()
	case AggMin:
		return col.Min()
	case AggMax:
		return col.Max()
	case AggCount:
		return col.Count()
	case AggStd:
		return col.Std()
	case AggVar:
		return col.Var()
	case AggFirst:
		return col.First()
	case AggLast:
		return col.Last()
	case AggMedian:
		return col.Median()
	default:
		return col.Sum()
	}
}

func exprFromNode(a *Arena, id NodeID) (expr.Expr, error) {
	node, ok := a.Get(id)
	if !ok {
		return nil, fmt.Errorf("invalid node ID %d", id)
	}

	switch node.Kind {
	case KindColumn:
		col := node.Payload.(Column)
		name, ok := a.String(col.NameID)
		if !ok {
			return nil, fmt.Errorf("invalid column name ID")
		}
		return expr.Col(name), nil

	case KindLiteral:
		lit := node.Payload.(Literal)
		return expr.Lit(lit.Value), nil

	case KindBinary:
		if len(node.Children) != 2 {
			return nil, fmt.Errorf("binary node must have 2 children")
		}
		left, err := exprFromNode(a, node.Children[0])
		if err != nil {
			return nil, err
		}
		right, err := exprFromNode(a, node.Children[1])
		if err != nil {
			return nil, err
		}
		bin := node.Payload.(Binary)
		return applyBinary(bin.Op, left, right), nil

	case KindUnary:
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("unary node must have 1 child")
		}
		child, err := exprFromNode(a, node.Children[0])
		if err != nil {
			return nil, err
		}
		un := node.Payload.(Unary)
		return applyUnary(un.Op, child), nil

	case KindAlias:
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("alias node must have 1 child")
		}
		child, err := exprFromNode(a, node.Children[0])
		if err != nil {
			return nil, err
		}
		alias := node.Payload.(Alias)
		name, ok := a.String(alias.NameID)
		if !ok {
			return nil, fmt.Errorf("invalid alias name ID")
		}
		return child.Alias(name), nil

	case KindAgg:
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("agg node must have 1 child")
		}
		child, err := exprFromNode(a, node.Children[0])
		if err != nil {
			return nil, err
		}
		agg := node.Payload.(Agg)
		colExpr, ok := child.(*expr.ColumnExpr)
		if ok {
			return aggExprFromOp(colExpr.Name(), agg.Op), nil
		}
		return child, nil

	default:
		return nil, fmt.Errorf("unsupported node kind: %d", node.Kind)
	}
}

func applyBinary(op BinaryOp, left, right expr.Expr) expr.Expr {
	b := expr.NewBuilder(left)
	switch op {
	case OpAdd:
		return b.Add(right).Build()
	case OpSub:
		return b.Sub(right).Build()
	case OpMul:
		return b.Mul(right).Build()
	case OpDiv:
		return b.Div(right).Build()
	case OpEq:
		return b.Eq(right).Build()
	case OpNeq:
		return b.Ne(right).Build()
	case OpLt:
		return b.Lt(right).Build()
	case OpLte:
		return b.Le(right).Build()
	case OpGt:
		return b.Gt(right).Build()
	case OpGte:
		return b.Ge(right).Build()
	case OpAnd:
		return b.And(right).Build()
	case OpOr:
		return b.Or(right).Build()
	default:
		return left
	}
}

func applyUnary(op UnaryOp, child expr.Expr) expr.Expr {
	b := expr.NewBuilder(child)
	switch op {
	case OpNot:
		return b.Not().Build()
	case OpIsNull:
		return b.IsNull().Build()
	case OpIsNotNull:
		return b.IsNotNull().Build()
	default:
		return child
	}
}

// executeWindowExpr executes a window expression node against a DataFrame.
func executeWindowExpr(a *Arena, id NodeID, df *frame.DataFrame) (series.Series, error) {
	node, ok := a.Get(id)
	if !ok {
		return nil, fmt.Errorf("invalid window node")
	}
	win, ok := node.Payload.(Window)
	if !ok {
		return nil, fmt.Errorf("expected Window payload")
	}

	fn := win.Func
	spec := win.Spec
	if fn == nil {
		return nil, fmt.Errorf("window function is nil")
	}

	name := fn.Name()

	// For aggregate window functions (sum, min, max, count), compute directly
	// to avoid indexing issues in the window package's partition-scoped results.
	if isAggregateWindowFunc(name) && len(node.Children) > 0 {
		colName, colOk := columnName(a, node.Children[0])
		if colOk {
			return executeAggregateWindow(df, spec, name, colName)
		}
	}

	// Create partitions from the DataFrame
	partitions, err := createPartitions(df, spec)
	if err != nil {
		return nil, err
	}

	// For ranking/row-numbering functions, compute per partition and merge into global result
	height := df.Height()
	groups, err := getPartitionGroups(df, spec)
	if err != nil {
		return nil, err
	}

	// Use a per-partition compute, then place results into a global result array
	if len(partitions) == 1 {
		s, err := fn.Compute(partitions[0])
		if err != nil {
			return nil, err
		}
		return s.Rename(name), nil
	}

	// Multiple partitions: compute each and merge
	// Determine output type from a trial compute
	firstPart := partitions[0]
	firstResult, err := fn.Compute(firstPart)
	if err != nil {
		return nil, err
	}

	// Build grouped results
	type groupResult struct {
		indices []int
		values  series.Series
	}

	var groupResults []groupResult
	i := 0
	for _, indices := range groups {
		var s series.Series
		if i == 0 {
			s = firstResult
		} else {
			s, err = fn.Compute(partitions[i])
			if err != nil {
				return nil, err
			}
		}
		groupResults = append(groupResults, groupResult{indices: indices, values: s})
		i++
	}

	// Merge into global array based on first result type
	if firstResult.Len() > 0 {
		if _, ok := firstResult.Get(0).(int64); ok {
			values := make([]int64, height)
			for _, gr := range groupResults {
				for j, idx := range gr.indices {
					values[idx] = gr.values.Get(j).(int64)
				}
			}
			return series.NewInt64Series(name, values), nil
		}
		if _, ok := firstResult.Get(0).(float64); ok {
			values := make([]float64, height)
			for _, gr := range groupResults {
				for j, idx := range gr.indices {
					values[idx] = gr.values.Get(j).(float64)
				}
			}
			return series.NewFloat64Series(name, values), nil
		}
	}

	return nil, fmt.Errorf("unsupported window result type")
}

func isAggregateWindowFunc(name string) bool {
	switch name {
	case "sum", "min", "max", "count", "avg":
		return true
	default:
		return false
	}
}

// executeAggregateWindow computes an aggregate window function directly.
func executeAggregateWindow(df *frame.DataFrame, spec *window.Spec, funcName, colName string) (series.Series, error) {
	col, err := df.Column(colName)
	if err != nil {
		return nil, err
	}

	height := df.Height()
	groups, err := getPartitionGroups(df, spec)
	if err != nil {
		return nil, err
	}

	outputName := colName + "_" + funcName

	// For int64 columns
	switch col.DataType().(type) {
	case datatypes.Int64:
		result := make([]int64, height)
		for _, indices := range groups {
			var aggVal int64
			switch funcName {
			case "sum":
				for _, idx := range indices {
					aggVal += col.Get(idx).(int64)
				}
			case "min":
				aggVal = col.Get(indices[0]).(int64)
				for _, idx := range indices[1:] {
					v := col.Get(idx).(int64)
					if v < aggVal {
						aggVal = v
					}
				}
			case "max":
				aggVal = col.Get(indices[0]).(int64)
				for _, idx := range indices[1:] {
					v := col.Get(idx).(int64)
					if v > aggVal {
						aggVal = v
					}
				}
			case "count":
				aggVal = int64(len(indices))
			}
			for _, idx := range indices {
				result[idx] = aggVal
			}
		}
		return series.NewInt64Series(outputName, result), nil

	case datatypes.Float64:
		result := make([]float64, height)
		for _, indices := range groups {
			var aggVal float64
			switch funcName {
			case "sum":
				for _, idx := range indices {
					aggVal += col.Get(idx).(float64)
				}
			case "min":
				aggVal = col.Get(indices[0]).(float64)
				for _, idx := range indices[1:] {
					v := col.Get(idx).(float64)
					if v < aggVal {
						aggVal = v
					}
				}
			case "max":
				aggVal = col.Get(indices[0]).(float64)
				for _, idx := range indices[1:] {
					v := col.Get(idx).(float64)
					if v > aggVal {
						aggVal = v
					}
				}
			case "count":
				aggVal = float64(len(indices))
			}
			for _, idx := range indices {
				result[idx] = aggVal
			}
		}
		return series.NewFloat64Series(outputName, result), nil
	}

	return nil, fmt.Errorf("unsupported column type for window aggregate: %v", col.DataType())
}

func getPartitionGroups(df *frame.DataFrame, spec *window.Spec) (map[string][]int, error) {
	partBy := spec.GetPartitionBy()
	if len(partBy) == 0 {
		// Single partition: all rows
		indices := make([]int, df.Height())
		for i := range indices {
			indices[i] = i
		}
		return map[string][]int{"__all__": indices}, nil
	}
	return partitionByColumns(df, partBy)
}

func createPartitions(df *frame.DataFrame, spec *window.Spec) ([]window.Partition, error) {
	seriesMap := make(map[string]series.Series)
	cols := df.Columns()
	for _, colName := range cols {
		col, err := df.Column(colName)
		if err != nil {
			continue
		}
		seriesMap[colName] = col
	}

	partBy := spec.GetPartitionBy()
	if len(partBy) == 0 {
		indices := make([]int, df.Height())
		for i := range indices {
			indices[i] = i
		}
		return []window.Partition{window.NewPartition(seriesMap, indices)}, nil
	}

	groups, err := partitionByColumns(df, partBy)
	if err != nil {
		return nil, err
	}

	partitions := make([]window.Partition, 0, len(groups))
	for _, indices := range groups {
		partitions = append(partitions, window.NewPartition(seriesMap, indices))
	}
	return partitions, nil
}

func partitionByColumns(df *frame.DataFrame, columns []string) (map[string][]int, error) {
	cols := make([]series.Series, len(columns))
	for i, name := range columns {
		col, err := df.Column(name)
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}

	groups := make(map[string][]int)
	for row := 0; row < df.Height(); row++ {
		var keyParts []string
		for _, col := range cols {
			keyParts = append(keyParts, col.GetAsString(row))
		}
		key := strings.Join(keyParts, "\x00")
		groups[key] = append(groups[key], row)
	}
	return groups, nil
}

type partResult struct {
	indices []int
	s       series.Series
}

func mergePartResults(df *frame.DataFrame, results []partResult, name string) (series.Series, error) {
	// Determine the data type from the first result
	if len(results) == 0 {
		return nil, fmt.Errorf("no partition results")
	}

	first := results[0].s
	height := df.Height()

	// Try int64 merge
	if _, ok := first.Get(0).(int64); ok {
		values := make([]int64, height)
		for _, pr := range results {
			for j, idx := range pr.indices {
				values[idx] = pr.s.Get(j).(int64)
			}
		}
		return series.NewInt64Series(name, values), nil
	}

	// Try float64 merge
	if _, ok := first.Get(0).(float64); ok {
		values := make([]float64, height)
		for _, pr := range results {
			for j, idx := range pr.indices {
				values[idx] = pr.s.Get(j).(float64)
			}
		}
		return series.NewFloat64Series(name, values), nil
	}

	// Default: float64
	values := make([]float64, height)
	return series.NewFloat64Series(name, values), nil
}

// evaluateExprOnFrame evaluates an expr.Expr against a DataFrame to produce a series.
func evaluateExprOnFrame(df *frame.DataFrame, e expr.Expr) (series.Series, error) {
	// Use WithColumn with a temp name, then extract the column
	tempName := "__eval_temp__"
	result, err := df.WithColumn(tempName, e)
	if err != nil {
		return nil, err
	}
	col, err := result.Column(tempName)
	if err != nil {
		return nil, err
	}
	return col, nil
}
