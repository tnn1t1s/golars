package frame

import (
	"fmt"
	"strings"
	"sync"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	_ "github.com/tnn1t1s/golars/internal/parallel"
	_ "github.com/tnn1t1s/golars/internal/window"
	"github.com/tnn1t1s/golars/series"
)

// DataFrame is a table of data with named columns
type DataFrame struct {
	columns []series.Series
	schema  *datatypes.Schema
	height  int
	mu      sync.RWMutex
}

// NewDataFrame creates a new DataFrame from a list of series
func NewDataFrame(columns ...series.Series) (*DataFrame, error) {
	if len(columns) == 0 {
		return &DataFrame{
			columns: nil,
			schema:  &datatypes.Schema{},
			height:  0,
		}, nil
	}

	height := columns[0].Len()
	for i, col := range columns[1:] {
		if col.Len() != height {
			return nil, fmt.Errorf("all columns must have the same length: column %d has length %d, expected %d", i+1, col.Len(), height)
		}
	}

	fields := make([]datatypes.Field, len(columns))
	for i, col := range columns {
		fields[i] = datatypes.Field{Name: col.Name(), DataType: col.DataType()}
	}

	return &DataFrame{
		columns: columns,
		schema:  &datatypes.Schema{Fields: fields},
		height:  height,
	}, nil
}

// NewDataFrameFromMap creates a DataFrame from a map of column names to slices
func NewDataFrameFromMap(data map[string]interface{}) (*DataFrame, error) {
	if len(data) == 0 {
		return NewDataFrame()
	}

	var cols []series.Series
	for name, values := range data {
		switch v := values.(type) {
		case []int:
			vals := make([]int64, len(v))
			for i, x := range v {
				vals[i] = int64(x)
			}
			cols = append(cols, series.NewInt64Series(name, vals))
		case []int32:
			cols = append(cols, series.NewInt32Series(name, v))
		case []int64:
			cols = append(cols, series.NewInt64Series(name, v))
		case []float64:
			cols = append(cols, series.NewFloat64Series(name, v))
		case []float32:
			cols = append(cols, series.NewFloat32Series(name, v))
		case []string:
			cols = append(cols, series.NewStringSeries(name, v))
		case []bool:
			cols = append(cols, series.NewBooleanSeries(name, v))
		default:
			return nil, fmt.Errorf("unsupported value type for column %q: %T", name, values)
		}
	}
	return NewDataFrame(cols...)
}

// Schema returns the schema of the DataFrame
func (df *DataFrame) Schema() *datatypes.Schema {
	return df.schema
}

// Columns returns the column names
func (df *DataFrame) Columns() []string {
	names := make([]string, len(df.columns))
	for i, col := range df.columns {
		names[i] = col.Name()
	}
	return names
}

// Height returns the number of rows
func (df *DataFrame) Height() int {
	return df.height
}

// Width returns the number of columns
func (df *DataFrame) Width() int {
	return len(df.columns)
}

// Shape returns (height, width)
func (df *DataFrame) Shape() (int, int) {
	return df.height, len(df.columns)
}

// IsEmpty returns true if the DataFrame has no rows
func (df *DataFrame) IsEmpty() bool {
	return df.height == 0
}

// Column returns a column by name
func (df *DataFrame) Column(name string) (series.Series, error) {
	for _, col := range df.columns {
		if col.Name() == name {
			return col, nil
		}
	}
	return nil, fmt.Errorf("column %q not found", name)
}

// HasColumn returns true if the DataFrame contains a column with the given name
func (df *DataFrame) HasColumn(name string) bool {
	for _, col := range df.columns {
		if col.Name() == name {
			return true
		}
	}
	return false
}

// ColumnAt returns a column by index
func (df *DataFrame) ColumnAt(idx int) (series.Series, error) {
	if idx < 0 || idx >= len(df.columns) {
		return nil, fmt.Errorf("column index %d out of range [0, %d)", idx, len(df.columns))
	}
	return df.columns[idx], nil
}

// Select returns a new DataFrame with only the specified columns
func (df *DataFrame) Select(columns ...string) (*DataFrame, error) {
	selected := make([]series.Series, 0, len(columns))
	for _, name := range columns {
		col, err := df.Column(name)
		if err != nil {
			return nil, err
		}
		selected = append(selected, col)
	}
	return NewDataFrame(selected...)
}

// Drop returns a new DataFrame without the specified columns
func (df *DataFrame) Drop(columns ...string) (*DataFrame, error) {
	// Validate all columns exist
	for _, name := range columns {
		if !df.HasColumn(name) {
			return nil, fmt.Errorf("column %q not found", name)
		}
	}
	dropSet := make(map[string]bool, len(columns))
	for _, name := range columns {
		dropSet[name] = true
	}
	var remaining []series.Series
	for _, col := range df.columns {
		if !dropSet[col.Name()] {
			remaining = append(remaining, col)
		}
	}
	return NewDataFrame(remaining...)
}

// Head returns the first n rows
func (df *DataFrame) Head(n int) *DataFrame {
	if n >= df.height {
		result, _ := NewDataFrame()
		if df.height > 0 {
			result, _ = NewDataFrame(df.columns...)
		}
		return result
	}
	cols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		cols[i] = col.Head(n)
	}
	result, _ := NewDataFrame(cols...)
	return result
}

// Tail returns the last n rows
func (df *DataFrame) Tail(n int) *DataFrame {
	if n >= df.height {
		result, _ := NewDataFrame()
		if df.height > 0 {
			result, _ = NewDataFrame(df.columns...)
		}
		return result
	}
	cols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		cols[i] = col.Tail(n)
	}
	result, _ := NewDataFrame(cols...)
	return result
}

// Slice returns a new DataFrame with rows from start to end (exclusive)
func (df *DataFrame) Slice(start, end int) (*DataFrame, error) {
	if start < 0 || end > df.height || start > end {
		return nil, fmt.Errorf("invalid slice range [%d, %d) for DataFrame with %d rows", start, end, df.height)
	}
	cols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		sliced, err := col.Slice(start, end)
		if err != nil {
			return nil, err
		}
		cols[i] = sliced
	}
	return NewDataFrame(cols...)
}

// Clone returns a copy of the DataFrame
func (df *DataFrame) Clone() *DataFrame {
	cols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		cols[i] = col.Clone()
	}
	result, _ := NewDataFrame(cols...)
	return result
}

// GetRow returns a map of column names to values for a specific row
func (df *DataFrame) GetRow(idx int) (map[string]interface{}, error) {
	if idx < 0 || idx >= df.height {
		return nil, fmt.Errorf("row index %d out of range [0, %d)", idx, df.height)
	}
	row := make(map[string]interface{}, len(df.columns))
	for _, col := range df.columns {
		if col.IsNull(idx) {
			row[col.Name()] = nil
		} else {
			row[col.Name()] = col.Get(idx)
		}
	}
	return row, nil
}

// String returns a string representation of the DataFrame
func (df *DataFrame) String() string {
	if len(df.columns) == 0 {
		return "Empty DataFrame"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("DataFrame: %d × %d\n", df.height, len(df.columns)))

	// Column headers
	headers := make([]string, len(df.columns))
	for i, col := range df.columns {
		headers[i] = col.Name()
	}
	sb.WriteString(strings.Join(headers, "\t"))
	sb.WriteString("\n")

	// Data types
	types := make([]string, len(df.columns))
	for i, col := range df.columns {
		types[i] = fmt.Sprintf("%v", col.DataType())
	}
	sb.WriteString(strings.Join(types, "\t"))
	sb.WriteString("\n")

	// Data rows
	maxRows := 10
	if df.height <= maxRows {
		for row := 0; row < df.height; row++ {
			vals := make([]string, len(df.columns))
			for i, col := range df.columns {
				vals[i] = col.GetAsString(row)
			}
			sb.WriteString(strings.Join(vals, "\t"))
			sb.WriteString("\n")
		}
	} else {
		half := maxRows / 2
		for row := 0; row < half; row++ {
			vals := make([]string, len(df.columns))
			for i, col := range df.columns {
				vals[i] = col.GetAsString(row)
			}
			sb.WriteString(strings.Join(vals, "\t"))
			sb.WriteString("\n")
		}
		sb.WriteString("...\n")
		for row := df.height - half; row < df.height; row++ {
			vals := make([]string, len(df.columns))
			for i, col := range df.columns {
				vals[i] = col.GetAsString(row)
			}
			sb.WriteString(strings.Join(vals, "\t"))
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

// AddColumn adds a new column to the DataFrame
func (df *DataFrame) AddColumn(col series.Series) (*DataFrame, error) {
	if df.HasColumn(col.Name()) {
		return nil, fmt.Errorf("column %q already exists", col.Name())
	}
	if df.height > 0 && col.Len() != df.height {
		return nil, fmt.Errorf("column length %d does not match DataFrame height %d", col.Len(), df.height)
	}
	newCols := make([]series.Series, len(df.columns)+1)
	copy(newCols, df.columns)
	newCols[len(df.columns)] = col
	return NewDataFrame(newCols...)
}

// RenameColumn renames a column
func (df *DataFrame) RenameColumn(oldName, newName string) (*DataFrame, error) {
	if !df.HasColumn(oldName) {
		return nil, fmt.Errorf("column %q not found", oldName)
	}
	if oldName != newName && df.HasColumn(newName) {
		return nil, fmt.Errorf("column %q already exists", newName)
	}
	cols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		if col.Name() == oldName {
			cols[i] = col.Rename(newName)
		} else {
			cols[i] = col
		}
	}
	return NewDataFrame(cols...)
}

// WithColumn adds or replaces a column based on an expression
func (df *DataFrame) WithColumn(name string, e expr.Expr) (*DataFrame, error) {
	result, err := df.evaluateExpr(e)
	if err != nil {
		return nil, err
	}
	result = result.Rename(name)

	newCols := make([]series.Series, len(df.columns))
	copy(newCols, df.columns)

	// Check if the column already exists
	for i, col := range newCols {
		if col.Name() == name {
			newCols[i] = result
			return NewDataFrame(newCols...)
		}
	}

	// Add new column
	newCols = append(newCols, result)
	return NewDataFrame(newCols...)
}

// WithColumns adds or replaces multiple columns based on expressions.
// Handles dependencies between new columns using topological ordering.
func (df *DataFrame) WithColumns(exprs map[string]expr.Expr) (*DataFrame, error) {
	if !hasExprDependencies(exprs) {
		// No dependencies, simple iteration
		current := df
		for name, e := range exprs {
			var err error
			current, err = current.WithColumn(name, e)
			if err != nil {
				return nil, err
			}
		}
		return current, nil
	}

	// Topological sort: process columns that don't depend on other new columns first
	newNames := make(map[string]struct{}, len(exprs))
	for name := range exprs {
		newNames[name] = struct{}{}
	}

	ordered := make([]string, 0, len(exprs))
	remaining := make(map[string]expr.Expr)
	for k, v := range exprs {
		remaining[k] = v
	}

	for len(remaining) > 0 {
		progress := false
		for name, e := range remaining {
			deps, ok := exprDependencyNames(e, newNames)
			if !ok {
				// Can't analyze, just add it
				ordered = append(ordered, name)
				delete(remaining, name)
				delete(newNames, name)
				progress = true
				continue
			}
			// Check if all dependencies are already ordered
			allResolved := true
			for dep := range deps {
				if _, still := remaining[dep]; still {
					allResolved = false
					break
				}
			}
			if allResolved {
				ordered = append(ordered, name)
				delete(remaining, name)
				delete(newNames, name)
				progress = true
			}
		}
		if !progress {
			// Circular dependency or stuck; just add remaining
			for name := range remaining {
				ordered = append(ordered, name)
			}
			break
		}
	}

	current := df
	for _, name := range ordered {
		e := exprs[name]
		var err error
		current, err = current.WithColumn(name, e)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
}

// WithColumnsFrame adds or replaces columns from another DataFrame.
func (df *DataFrame) WithColumnsFrame(other *DataFrame) (*DataFrame, error) {
	if other.height != df.height {
		return nil, fmt.Errorf("height mismatch: %d vs %d", df.height, other.height)
	}

	newCols := make([]series.Series, len(df.columns))
	copy(newCols, df.columns)

	for _, otherCol := range other.columns {
		replaced := false
		for i, col := range newCols {
			if col.Name() == otherCol.Name() {
				newCols[i] = otherCol
				replaced = true
				break
			}
		}
		if !replaced {
			newCols = append(newCols, otherCol)
		}
	}
	return NewDataFrame(newCols...)
}

func hasExprDependencies(exprs map[string]expr.Expr) bool {
	names := make(map[string]struct{}, len(exprs))
	for name := range exprs {
		names[name] = struct{}{}
	}
	for _, e := range exprs {
		if exprReferencesNames(e, names) {
			return true
		}
	}
	return false
}

func orderExpressions(names []string, exprList []expr.Expr) ([]int, bool) {
	order := make([]int, len(names))
	for i := range order {
		order[i] = i
	}
	return order, true
}

func exprDependencyNames(e expr.Expr, names map[string]struct{}) (map[string]struct{}, bool) {
	deps := make(map[string]struct{})
	ok := collectExprDependencies(e, names, deps)
	return deps, ok
}

func collectExprDependencies(e expr.Expr, names map[string]struct{}, deps map[string]struct{}) bool {
	switch ex := e.(type) {
	case *expr.ColumnExpr:
		if _, ok := names[ex.Name()]; ok {
			deps[ex.Name()] = struct{}{}
		}
		return true
	case *expr.BinaryExpr:
		if !collectExprDependencies(ex.Left(), names, deps) {
			return false
		}
		return collectExprDependencies(ex.Right(), names, deps)
	case *expr.UnaryExpr:
		return collectExprDependencies(ex.Expr(), names, deps)
	case *expr.AliasExpr:
		return collectExprDependencies(ex.Expr(), names, deps)
	case *expr.LiteralExpr:
		return true
	default:
		return false
	}
}

func exprReferencesNames(e expr.Expr, names map[string]struct{}) bool {
	switch ex := e.(type) {
	case *expr.ColumnExpr:
		_, ok := names[ex.Name()]
		return ok
	case *expr.BinaryExpr:
		return exprReferencesNames(ex.Left(), names) || exprReferencesNames(ex.Right(), names)
	case *expr.UnaryExpr:
		return exprReferencesNames(ex.Expr(), names)
	case *expr.AliasExpr:
		return exprReferencesNames(ex.Expr(), names)
	case *expr.LiteralExpr:
		return false
	default:
		return false
	}
}

func shouldVerticalParallel(df *DataFrame, exprList []expr.Expr) bool {
	if df.height < 10000 {
		return false
	}
	for _, e := range exprList {
		if !exprIsRowIndependent(e) {
			return false
		}
	}
	return true
}

func exprIsRowIndependent(e expr.Expr) bool {
	switch ex := e.(type) {
	case *expr.ColumnExpr, *expr.LiteralExpr:
		return true
	case *expr.BinaryExpr:
		return exprIsRowIndependent(ex.Left()) && exprIsRowIndependent(ex.Right())
	case *expr.UnaryExpr:
		return exprIsRowIndependent(ex.Expr())
	case *expr.AliasExpr:
		return exprIsRowIndependent(ex.Expr())
	default:
		return false
	}
}

func (df *DataFrame) withColumnsVertical(names []string, exprList []expr.Expr) (*DataFrame, error) {
	return applyExpressionsToFrame(df, names, exprList, false)
}

func applyExpressionsToFrame(df *DataFrame, names []string, exprList []expr.Expr, parallelize bool) (*DataFrame, error) {
	current := df
	for i, e := range exprList {
		var err error
		current, err = current.WithColumn(names[i], e)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
}

// truncateString truncates a string to the specified length
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
