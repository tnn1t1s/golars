package frame

import (
	"fmt"
	"strings"
	"sync"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
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
			columns: []series.Series{},
			schema:  datatypes.NewSchema(),
			height:  0,
		}, nil
	}

	// Validate all columns have the same length
	height := columns[0].Len()
	fields := make([]datatypes.Field, len(columns))

	for i, col := range columns {
		if col.Len() != height {
			return nil, fmt.Errorf("all columns must have the same length, got %d and %d", height, col.Len())
		}

		fields[i] = datatypes.Field{
			Name:     col.Name(),
			DataType: col.DataType(),
			Nullable: col.NullCount() > 0,
		}
	}

	return &DataFrame{
		columns: columns,
		schema:  datatypes.NewSchema(fields...),
		height:  height,
	}, nil
}

// NewDataFrameFromMap creates a DataFrame from a map of column names to slices
func NewDataFrameFromMap(data map[string]interface{}) (*DataFrame, error) {
	columns := make([]series.Series, 0, len(data))

	for name, values := range data {
		var s series.Series

		switch v := values.(type) {
		case []bool:
			s = series.NewBooleanSeries(name, v)
		case []int:
			// Convert to int64 for consistency
			int64Values := make([]int64, len(v))
			for i, val := range v {
				int64Values[i] = int64(val)
			}
			s = series.NewInt64Series(name, int64Values)
		case []int8:
			s = series.NewInt8Series(name, v)
		case []int16:
			s = series.NewInt16Series(name, v)
		case []int32:
			s = series.NewInt32Series(name, v)
		case []int64:
			s = series.NewInt64Series(name, v)
		case []uint8:
			s = series.NewUInt8Series(name, v)
		case []uint16:
			s = series.NewUInt16Series(name, v)
		case []uint32:
			s = series.NewUInt32Series(name, v)
		case []uint64:
			s = series.NewUInt64Series(name, v)
		case []float32:
			s = series.NewFloat32Series(name, v)
		case []float64:
			s = series.NewFloat64Series(name, v)
		case []string:
			s = series.NewStringSeries(name, v)
		default:
			return nil, fmt.Errorf("unsupported type for column %s: %T", name, values)
		}

		columns = append(columns, s)
	}

	return NewDataFrame(columns...)
}

// Schema returns the schema of the DataFrame
func (df *DataFrame) Schema() *datatypes.Schema {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.schema
}

// Columns returns the column names
func (df *DataFrame) Columns() []string {
	df.mu.RLock()
	defer df.mu.RUnlock()

	names := make([]string, len(df.schema.Fields))
	for i, field := range df.schema.Fields {
		names[i] = field.Name
	}
	return names
}

// Height returns the number of rows
func (df *DataFrame) Height() int {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.height
}

// Width returns the number of columns
func (df *DataFrame) Width() int {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return len(df.columns)
}

// Shape returns (height, width)
func (df *DataFrame) Shape() (int, int) {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.height, len(df.columns)
}

// IsEmpty returns true if the DataFrame has no rows
func (df *DataFrame) IsEmpty() bool {
	return df.Height() == 0
}

// Column returns a column by name
func (df *DataFrame) Column(name string) (series.Series, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	for i, field := range df.schema.Fields {
		if field.Name == name {
			return df.columns[i], nil
		}
	}

	return nil, fmt.Errorf("column '%s' not found", name)
}

// HasColumn returns true if the DataFrame contains a column with the given name
func (df *DataFrame) HasColumn(name string) bool {
	df.mu.RLock()
	defer df.mu.RUnlock()

	for _, field := range df.schema.Fields {
		if field.Name == name {
			return true
		}
	}

	return false
}

// ColumnAt returns a column by index
func (df *DataFrame) ColumnAt(idx int) (series.Series, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	if idx < 0 || idx >= len(df.columns) {
		return nil, fmt.Errorf("column index %d out of range [0, %d)", idx, len(df.columns))
	}

	return df.columns[idx], nil
}

// Select returns a new DataFrame with only the specified columns
func (df *DataFrame) Select(columns ...string) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	selectedCols := make([]series.Series, 0, len(columns))

	for _, name := range columns {
		found := false
		for i, field := range df.schema.Fields {
			if field.Name == name {
				selectedCols = append(selectedCols, df.columns[i])
				found = true
				break
			}
		}

		if !found {
			return nil, fmt.Errorf("column '%s' not found", name)
		}
	}

	return NewDataFrame(selectedCols...)
}

// Drop returns a new DataFrame without the specified columns
func (df *DataFrame) Drop(columns ...string) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	dropSet := make(map[string]bool)
	for _, name := range columns {
		dropSet[name] = true
	}

	remainingCols := make([]series.Series, 0, len(df.columns))

	for i, field := range df.schema.Fields {
		if !dropSet[field.Name] {
			remainingCols = append(remainingCols, df.columns[i])
		}
	}

	if len(remainingCols) == len(df.columns) {
		return nil, fmt.Errorf("none of the specified columns found")
	}

	return NewDataFrame(remainingCols...)
}

// Head returns the first n rows
func (df *DataFrame) Head(n int) *DataFrame {
	df.mu.RLock()
	defer df.mu.RUnlock()

	if n < 0 || n > df.height {
		n = df.height
	}

	headCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		headCols[i] = col.Head(n)
	}

	result, _ := NewDataFrame(headCols...)
	return result
}

// Tail returns the last n rows
func (df *DataFrame) Tail(n int) *DataFrame {
	df.mu.RLock()
	defer df.mu.RUnlock()

	if n < 0 || n > df.height {
		n = df.height
	}

	tailCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		tailCols[i] = col.Tail(n)
	}

	result, _ := NewDataFrame(tailCols...)
	return result
}

// Slice returns a new DataFrame with rows from start to end (exclusive)
func (df *DataFrame) Slice(start, end int) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	if start < 0 || end > df.height || start > end {
		return nil, fmt.Errorf("invalid slice bounds: [%d:%d] for DataFrame of height %d", start, end, df.height)
	}

	slicedCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		sliced, err := col.Slice(start, end)
		if err != nil {
			return nil, err
		}
		slicedCols[i] = sliced
	}

	return NewDataFrame(slicedCols...)
}

// Clone returns a copy of the DataFrame
func (df *DataFrame) Clone() *DataFrame {
	df.mu.RLock()
	defer df.mu.RUnlock()

	clonedCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		clonedCols[i] = col.Clone()
	}

	result, _ := NewDataFrame(clonedCols...)
	return result
}

// GetRow returns a map of column names to values for a specific row
func (df *DataFrame) GetRow(idx int) (map[string]interface{}, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	if idx < 0 || idx >= df.height {
		return nil, fmt.Errorf("row index %d out of range [0, %d)", idx, df.height)
	}

	row := make(map[string]interface{})
	for i, field := range df.schema.Fields {
		row[field.Name] = df.columns[i].Get(idx)
	}

	return row, nil
}

// String returns a string representation of the DataFrame
func (df *DataFrame) String() string {
	df.mu.RLock()
	defer df.mu.RUnlock()

	var sb strings.Builder

	// Header
	sb.WriteString(fmt.Sprintf("DataFrame: %d × %d\n", df.height, len(df.columns)))

	// Column headers
	sb.WriteString("┌")
	for i := range df.columns {
		if i > 0 {
			sb.WriteString("┬")
		}
		sb.WriteString("─────────────")
	}
	sb.WriteString("┐\n")

	sb.WriteString("│")
	for _, field := range df.schema.Fields {
		sb.WriteString(fmt.Sprintf(" %-11s │", truncateString(field.Name, 11)))
	}
	sb.WriteString("\n")

	// Data types
	sb.WriteString("│")
	for _, field := range df.schema.Fields {
		sb.WriteString(fmt.Sprintf(" %-11s │", truncateString(field.DataType.String(), 11)))
	}
	sb.WriteString("\n")

	sb.WriteString("├")
	for i := range df.columns {
		if i > 0 {
			sb.WriteString("┼")
		}
		sb.WriteString("─────────────")
	}
	sb.WriteString("┤\n")

	// Data rows
	displayRows := 10
	if df.height < displayRows {
		displayRows = df.height
	}

	for row := 0; row < displayRows; row++ {
		sb.WriteString("│")
		for _, col := range df.columns {
			val := col.GetAsString(row)
			sb.WriteString(fmt.Sprintf(" %-11s │", truncateString(val, 11)))
		}
		sb.WriteString("\n")
	}

	if df.height > displayRows {
		sb.WriteString("│")
		for range df.columns {
			sb.WriteString(" ...         │")
		}
		sb.WriteString(fmt.Sprintf("\n[%d more rows]\n", df.height-displayRows))
	}

	sb.WriteString("└")
	for i := range df.columns {
		if i > 0 {
			sb.WriteString("┴")
		}
		sb.WriteString("─────────────")
	}
	sb.WriteString("┘")

	return sb.String()
}

// AddColumn adds a new column to the DataFrame
func (df *DataFrame) AddColumn(col series.Series) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	if col.Len() != df.height {
		return nil, fmt.Errorf("column length %d does not match DataFrame height %d", col.Len(), df.height)
	}

	// Check if column name already exists
	for _, field := range df.schema.Fields {
		if field.Name == col.Name() {
			return nil, fmt.Errorf("column '%s' already exists", col.Name())
		}
	}

	newCols := make([]series.Series, len(df.columns)+1)
	copy(newCols, df.columns)
	newCols[len(df.columns)] = col

	return NewDataFrame(newCols...)
}

// RenameColumn renames a column
func (df *DataFrame) RenameColumn(oldName, newName string) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	colIdx := -1
	for i, field := range df.schema.Fields {
		if field.Name == oldName {
			colIdx = i
			break
		}
	}

	if colIdx == -1 {
		return nil, fmt.Errorf("column '%s' not found", oldName)
	}

	// Check if new name already exists
	for i, field := range df.schema.Fields {
		if i != colIdx && field.Name == newName {
			return nil, fmt.Errorf("column '%s' already exists", newName)
		}
	}

	newCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		if i == colIdx {
			newCols[i] = col.Rename(newName)
		} else {
			newCols[i] = col
		}
	}

	return NewDataFrame(newCols...)
}

// WithColumn adds or replaces a column based on an expression
func (df *DataFrame) WithColumn(name string, expr expr.Expr) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	// Evaluate the expression to get a new series
	newSeries, err := df.evaluateExpr(expr)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression: %w", err)
	}

	// Rename the series to the specified name
	newSeries = newSeries.Rename(name)

	// Check if the column already exists
	existingIdx := -1
	for i, field := range df.schema.Fields {
		if field.Name == name {
			existingIdx = i
			break
		}
	}

	// Create new columns slice
	var newCols []series.Series
	if existingIdx >= 0 {
		// Replace existing column
		newCols = make([]series.Series, len(df.columns))
		copy(newCols, df.columns)
		newCols[existingIdx] = newSeries
	} else {
		// Add new column
		newCols = make([]series.Series, len(df.columns)+1)
		copy(newCols, df.columns)
		newCols[len(df.columns)] = newSeries
	}

	return NewDataFrame(newCols...)
}

// WithColumns adds or replaces multiple columns based on expressions
func (df *DataFrame) WithColumns(exprs map[string]expr.Expr) (*DataFrame, error) {
	result := df

	// Apply each expression
	for name, e := range exprs {
		var err error
		result, err = result.WithColumn(name, e)
		if err != nil {
			return nil, fmt.Errorf("failed to create column '%s': %w", name, err)
		}
	}

	return result, nil
}

// truncateString truncates a string to the specified length
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
