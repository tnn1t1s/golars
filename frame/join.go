package frame

import (
	"fmt"
	"strings"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// JoinType specifies the type of join operation
type JoinType string

const (
	InnerJoin JoinType = "inner"
	LeftJoin  JoinType = "left"
	RightJoin JoinType = "right"
	OuterJoin JoinType = "outer"
	CrossJoin JoinType = "cross"
	AntiJoin  JoinType = "anti"
	SemiJoin  JoinType = "semi"
)

// JoinConfig contains configuration for join operations
type JoinConfig struct {
	How     JoinType
	LeftOn  []string
	RightOn []string
	Suffix  string // Default: "_right"
}

// Join performs a join operation on a single column
func (df *DataFrame) Join(other *DataFrame, on string, how JoinType) (*DataFrame, error) {
	return df.JoinWithConfig(other, JoinConfig{
		How:     how,
		LeftOn:  []string{on},
		RightOn: []string{on},
	})
}

// JoinOn performs a join operation on specified columns
func (df *DataFrame) JoinOn(other *DataFrame, leftOn []string, rightOn []string, how JoinType) (*DataFrame, error) {
	return df.JoinWithConfig(other, JoinConfig{
		How:     how,
		LeftOn:  leftOn,
		RightOn: rightOn,
	})
}

// JoinWithConfig performs a join operation with full configuration
func (df *DataFrame) JoinWithConfig(other *DataFrame, config JoinConfig) (*DataFrame, error) {
	if config.Suffix == "" {
		config.Suffix = "_right"
	}

	if err := validateJoinColumns(df, other, config); err != nil {
		return nil, err
	}

	switch config.How {
	case InnerJoin:
		return innerJoin(df, other, config)
	case LeftJoin:
		return leftJoin(df, other, config)
	case RightJoin:
		// Right join is left join with swapped sides
		swapped := JoinConfig{
			How:     LeftJoin,
			LeftOn:  config.RightOn,
			RightOn: config.LeftOn,
			Suffix:  config.Suffix,
		}
		return leftJoin(other, df, swapped)
	case OuterJoin:
		return outerJoin(df, other, config)
	case CrossJoin:
		return nil, fmt.Errorf("cross join is not supported")
	case SemiJoin:
		return nil, fmt.Errorf("semi join is not supported")
	case AntiJoin:
		return nil, fmt.Errorf("anti join is not supported")
	default:
		return nil, fmt.Errorf("unsupported join type: %s", config.How)
	}
}

// validateJoinColumns ensures join columns exist and have compatible types
func validateJoinColumns(left, right *DataFrame, config JoinConfig) error {
	if len(config.LeftOn) != len(config.RightOn) {
		return fmt.Errorf("left and right join columns must have the same length")
	}
	for _, name := range config.LeftOn {
		if !left.HasColumn(name) {
			return fmt.Errorf("left column %q not found", name)
		}
	}
	for _, name := range config.RightOn {
		if !right.HasColumn(name) {
			return fmt.Errorf("right column %q not found", name)
		}
	}
	// Check type compatibility
	for i := range config.LeftOn {
		leftCol, _ := left.Column(config.LeftOn[i])
		rightCol, _ := right.Column(config.RightOn[i])
		if !joinTypesCompatible(leftCol.DataType(), rightCol.DataType()) {
			return fmt.Errorf("incompatible types for join columns %q and %q: %v vs %v",
				config.LeftOn[i], config.RightOn[i], leftCol.DataType(), rightCol.DataType())
		}
	}
	return nil
}

// joinTypesCompatible checks if two data types are compatible for joining
func joinTypesCompatible(a, b datatypes.DataType) bool {
	// Same type is always compatible
	if fmt.Sprintf("%T", a) == fmt.Sprintf("%T", b) {
		return true
	}
	// Numeric types are compatible with each other
	aNum := isNumericType(a)
	bNum := isNumericType(b)
	if aNum && bNum {
		return true
	}
	// Otherwise incompatible
	return false
}

// getJoinColumns extracts the specified columns for joining
func getJoinColumns(df *DataFrame, columns []string) ([]series.Series, error) {
	result := make([]series.Series, len(columns))
	for i, name := range columns {
		col, err := df.Column(name)
		if err != nil {
			return nil, err
		}
		result[i] = col
	}
	return result, nil
}

// buildHashGroups builds a hash map from key columns to row indices.
// Rows with null keys are excluded.
func buildHashGroups(cols []series.Series, height int) map[string][]int {
	groups := make(map[string][]int)
	for row := 0; row < height; row++ {
		key, valid := makeJoinKey(cols, row)
		if !valid {
			continue // skip null keys
		}
		groups[key] = append(groups[key], row)
	}
	return groups
}

// makeJoinKey creates a string key from the values of multiple columns at a given row.
// Returns the key and whether the key is valid (false if any column is null).
func makeJoinKey(cols []series.Series, row int) (string, bool) {
	parts := make([]string, len(cols))
	for i, col := range cols {
		if col.IsNull(row) {
			return "", false // null keys never match
		}
		parts[i] = col.GetAsString(row)
	}
	return strings.Join(parts, "\x01"), true
}

// innerJoin performs an inner join operation
func innerJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	leftCols, _ := getJoinColumns(left, config.LeftOn)
	rightCols, _ := getJoinColumns(right, config.RightOn)

	// Build hash table on right side
	rightGroups := buildHashGroups(rightCols, right.height)

	// Probe left side
	var leftIndices, rightIndices []int
	for leftRow := 0; leftRow < left.height; leftRow++ {
		key, valid := makeJoinKey(leftCols, leftRow)
		if !valid {
			continue // skip null keys
		}
		if rightRows, ok := rightGroups[key]; ok {
			for _, rightRow := range rightRows {
				leftIndices = append(leftIndices, leftRow)
				rightIndices = append(rightIndices, rightRow)
			}
		}
	}

	return buildJoinResult(left, right, leftIndices, rightIndices, config)
}

// leftJoin performs a left join operation
func leftJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	leftCols, _ := getJoinColumns(left, config.LeftOn)
	rightCols, _ := getJoinColumns(right, config.RightOn)

	// Build hash table on right side
	rightGroups := buildHashGroups(rightCols, right.height)

	// Probe left side
	var leftIndices, rightIndices []int
	for leftRow := 0; leftRow < left.height; leftRow++ {
		key, valid := makeJoinKey(leftCols, leftRow)
		if !valid {
			// Null key: no match possible, emit with null right side
			leftIndices = append(leftIndices, leftRow)
			rightIndices = append(rightIndices, -1)
			continue
		}
		if rightRows, ok := rightGroups[key]; ok {
			for _, rightRow := range rightRows {
				leftIndices = append(leftIndices, leftRow)
				rightIndices = append(rightIndices, rightRow)
			}
		} else {
			leftIndices = append(leftIndices, leftRow)
			rightIndices = append(rightIndices, -1) // null marker
		}
	}

	return buildJoinResult(left, right, leftIndices, rightIndices, config)
}

// outerJoin performs a full outer join
func outerJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	leftCols, _ := getJoinColumns(left, config.LeftOn)
	rightCols, _ := getJoinColumns(right, config.RightOn)

	rightGroups := buildHashGroups(rightCols, right.height)

	var leftIndices, rightIndices []int
	rightMatched := make(map[int]bool)

	// Probe left side
	for leftRow := 0; leftRow < left.height; leftRow++ {
		key, valid := makeJoinKey(leftCols, leftRow)
		if !valid {
			// Null key: no match
			leftIndices = append(leftIndices, leftRow)
			rightIndices = append(rightIndices, -1)
			continue
		}
		if rightRows, ok := rightGroups[key]; ok {
			for _, rightRow := range rightRows {
				leftIndices = append(leftIndices, leftRow)
				rightIndices = append(rightIndices, rightRow)
				rightMatched[rightRow] = true
			}
		} else {
			leftIndices = append(leftIndices, leftRow)
			rightIndices = append(rightIndices, -1)
		}
	}

	// Add unmatched right rows
	for rightRow := 0; rightRow < right.height; rightRow++ {
		if !rightMatched[rightRow] {
			leftIndices = append(leftIndices, -1)
			rightIndices = append(rightIndices, rightRow)
		}
	}

	return buildJoinResult(left, right, leftIndices, rightIndices, config)
}

// crossJoin performs a cross join (cartesian product)
func crossJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	var leftIndices, rightIndices []int
	for l := 0; l < left.height; l++ {
		for r := 0; r < right.height; r++ {
			leftIndices = append(leftIndices, l)
			rightIndices = append(rightIndices, r)
		}
	}
	return buildJoinResult(left, right, leftIndices, rightIndices, config)
}

// semiJoin returns left rows that have a match in right
func semiJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	leftCols, _ := getJoinColumns(left, config.LeftOn)
	rightCols, _ := getJoinColumns(right, config.RightOn)

	rightGroups := buildHashGroups(rightCols, right.height)

	var indices []int
	for leftRow := 0; leftRow < left.height; leftRow++ {
		key, valid := makeJoinKey(leftCols, leftRow)
		if !valid {
			continue
		}
		if _, ok := rightGroups[key]; ok {
			indices = append(indices, leftRow)
		}
	}

	if len(indices) == 0 {
		cols := make([]series.Series, len(left.columns))
		for i, col := range left.columns {
			cols[i] = col.Head(0)
		}
		return NewDataFrame(cols...)
	}

	cols := make([]series.Series, len(left.columns))
	for i, col := range left.columns {
		taken, ok := series.TakeFast(col, indices)
		if ok {
			cols[i] = taken
		} else {
			cols[i] = col.Take(indices)
		}
	}
	return NewDataFrame(cols...)
}

// antiJoin returns left rows that do NOT have a match in right
func antiJoin(left, right *DataFrame, config JoinConfig) (*DataFrame, error) {
	leftCols, _ := getJoinColumns(left, config.LeftOn)
	rightCols, _ := getJoinColumns(right, config.RightOn)

	rightGroups := buildHashGroups(rightCols, right.height)

	var indices []int
	for leftRow := 0; leftRow < left.height; leftRow++ {
		key, valid := makeJoinKey(leftCols, leftRow)
		if !valid {
			continue
		}
		if _, ok := rightGroups[key]; !ok {
			indices = append(indices, leftRow)
		}
	}

	if len(indices) == 0 {
		cols := make([]series.Series, len(left.columns))
		for i, col := range left.columns {
			cols[i] = col.Head(0)
		}
		return NewDataFrame(cols...)
	}

	cols := make([]series.Series, len(left.columns))
	for i, col := range left.columns {
		taken, ok := series.TakeFast(col, indices)
		if ok {
			cols[i] = taken
		} else {
			cols[i] = col.Take(indices)
		}
	}
	return NewDataFrame(cols...)
}

// buildJoinResult constructs the result DataFrame from join indices
func buildJoinResult(left, right *DataFrame, leftIndices, rightIndices []int, config JoinConfig) (*DataFrame, error) {
	if len(leftIndices) == 0 {
		return NewDataFrame()
	}

	// Build set of right join columns to skip (they duplicate left join columns)
	rightJoinCols := make(map[string]bool)
	for i, name := range config.RightOn {
		if i < len(config.LeftOn) && config.LeftOn[i] == name {
			rightJoinCols[name] = true
		}
	}

	var resultCols []series.Series

	// Add left columns
	_, hasNullLeft := scanIndices(leftIndices, left.height)
	for _, col := range left.columns {
		if hasNullLeft {
			taken, err := takeSeriesWithNulls(col, leftIndices)
			if err != nil {
				return nil, err
			}
			resultCols = append(resultCols, taken)
		} else {
			taken, ok := series.TakeFast(col, leftIndices)
			if ok {
				resultCols = append(resultCols, taken)
			} else {
				resultCols = append(resultCols, col.Take(leftIndices))
			}
		}
	}

	// Add right columns (handle name conflicts)
	leftColNames := make(map[string]bool)
	for _, col := range left.columns {
		leftColNames[col.Name()] = true
	}

	for _, col := range right.columns {
		// Skip join columns from right
		if rightJoinCols[col.Name()] {
			continue
		}

		name := col.Name()
		if leftColNames[name] {
			name = name + config.Suffix
		}

		var taken series.Series
		var err error
		taken, err = takeSeriesWithNulls(col, rightIndices)
		if err != nil {
			return nil, err
		}
		if name != col.Name() {
			taken = taken.Rename(name)
		}
		resultCols = append(resultCols, taken)
	}

	return NewDataFrame(resultCols...)
}

func scanIndices(indices []int, expectedLen int) (bool, bool) {
	hasValid := false
	hasNull := false
	for _, idx := range indices {
		if idx < 0 {
			hasNull = true
		} else {
			hasValid = true
		}
	}
	return hasValid, hasNull
}

// takeSeriesWithNulls takes values from a series using indices, with -1 meaning null
func takeSeriesWithNulls(s series.Series, indices []int) (series.Series, error) {
	// Try fast path first
	result, ok := series.TakeFast(s, indices)
	if ok {
		return result, nil
	}

	// Fall back to slow path
	hasNull := false
	for _, idx := range indices {
		if idx < 0 {
			hasNull = true
			break
		}
	}

	if !hasNull {
		return s.Take(indices), nil
	}

	// Build values and validity arrays
	n := len(indices)
	values := make([]interface{}, n)
	validity := make([]bool, n)
	for i, idx := range indices {
		if idx < 0 {
			values[i] = getZeroValue(s.DataType())
			validity[i] = false
		} else {
			if s.IsNull(idx) {
				values[i] = getZeroValue(s.DataType())
				validity[i] = false
			} else {
				values[i] = s.Get(idx)
				validity[i] = true
			}
		}
	}

	return createSeriesFromValues(s.Name(), values, validity, s.DataType()), nil
}

// getZeroValue returns the zero value for a data type
func getZeroValue(dtype datatypes.DataType) interface{} {
	switch dtype.(type) {
	case datatypes.Int32:
		return int32(0)
	case datatypes.Int64:
		return int64(0)
	case datatypes.Float64:
		return float64(0)
	case datatypes.Float32:
		return float32(0)
	case datatypes.String:
		return ""
	case datatypes.Boolean:
		return false
	default:
		return int64(0)
	}
}

// createSeriesFromValues creates a series from interface values with validity
func createSeriesFromValues(name string, values []interface{}, validity []bool, dtype datatypes.DataType) series.Series {
	return createSeriesFromInterface(name, values, validity, dtype)
}

// renameSeries creates a new series with a different name
func renameSeries(s series.Series, newName string) series.Series {
	return s.Rename(newName)
}

// createNullSeries creates a series filled with nulls
func createNullSeries(name string, dtype datatypes.DataType, length int) series.Series {
	validity := make([]bool, length) // All false = all nulls

	switch dtype.(type) {
	case datatypes.Int32:
		return series.NewSeriesWithValidity(name, make([]int32, length), validity, dtype)
	case datatypes.Int64:
		return series.NewSeriesWithValidity(name, make([]int64, length), validity, dtype)
	case datatypes.Float64:
		return series.NewSeriesWithValidity(name, make([]float64, length), validity, dtype)
	case datatypes.Float32:
		return series.NewSeriesWithValidity(name, make([]float32, length), validity, dtype)
	case datatypes.String:
		return series.NewSeriesWithValidity(name, make([]string, length), validity, dtype)
	case datatypes.Boolean:
		return series.NewSeriesWithValidity(name, make([]bool, length), validity, dtype)
	default:
		return series.NewSeriesWithValidity(name, make([]int32, length), validity, datatypes.Int32{})
	}
}

// JoinWhere performs an inequality join based on predicates
func (df *DataFrame) JoinWhere(other *DataFrame, predicates ...expr.Expr) (*DataFrame, error) {
	if len(predicates) == 0 {
		return crossJoin(df, other, JoinConfig{Suffix: "_right"})
	}

	// Try IEJoin algorithm for inequality predicates
	result, err := df.JoinWhereIEJoin(other, predicates...)
	if err == nil {
		return result, nil
	}

	// Fallback to naive nested loop
	var leftIndices, rightIndices []int
	for l := 0; l < df.height; l++ {
		for r := 0; r < other.height; r++ {
			match := true
			for _, pred := range predicates {
				ok, evalErr := evaluateJoinPredicate(df, other, l, r, pred)
				if evalErr != nil {
					return nil, evalErr
				}
				if !ok {
					match = false
					break
				}
			}
			if match {
				leftIndices = append(leftIndices, l)
				rightIndices = append(rightIndices, r)
			}
		}
	}

	return buildJoinWhereResult(df, other, leftIndices, rightIndices, JoinConfig{Suffix: "_right"})
}

// evaluateJoinPredicate evaluates a predicate for a pair of rows
func evaluateJoinPredicate(left, right *DataFrame, leftIdx, rightIdx int, pred expr.Expr) (bool, error) {
	binExpr, ok := pred.(*expr.BinaryExpr)
	if !ok {
		return false, fmt.Errorf("predicate must be a binary expression, got %T", pred)
	}

	leftVal, err := getValueForPredicate(left, right, leftIdx, rightIdx, binExpr.Left())
	if err != nil {
		return false, err
	}
	rightVal, err := getValueForPredicate(left, right, leftIdx, rightIdx, binExpr.Right())
	if err != nil {
		return false, err
	}

	if leftVal == nil || rightVal == nil {
		return false, nil
	}

	return compareValues(leftVal, rightVal, binExpr.Op()), nil
}

// getValueForPredicate gets a value from a row for predicate evaluation
func getValueForPredicate(left, right *DataFrame, leftIdx, rightIdx int, e expr.Expr) (interface{}, error) {
	switch ex := e.(type) {
	case *expr.ColumnExpr:
		name := ex.Name()
		// Try left DataFrame first
		if left.HasColumn(name) {
			col, _ := left.Column(name)
			if col.IsNull(leftIdx) {
				return nil, nil
			}
			return col.Get(leftIdx), nil
		}
		// Try right DataFrame
		if right.HasColumn(name) {
			col, _ := right.Column(name)
			if col.IsNull(rightIdx) {
				return nil, nil
			}
			return col.Get(rightIdx), nil
		}
		// Try with _right suffix
		rightName := name + "_right"
		if right.HasColumn(rightName) {
			col, _ := right.Column(rightName)
			if col.IsNull(rightIdx) {
				return nil, nil
			}
			return col.Get(rightIdx), nil
		}
		return nil, fmt.Errorf("column %q not found in either DataFrame", name)
	case *expr.LiteralExpr:
		return ex.Value(), nil
	default:
		return nil, fmt.Errorf("unsupported expression type in predicate: %T", e)
	}
}

// buildJoinWhereResult builds the result for JoinWhere
func buildJoinWhereResult(left, right *DataFrame, leftIndices, rightIndices []int, config JoinConfig) (*DataFrame, error) {
	if len(leftIndices) == 0 {
		return NewDataFrame()
	}

	if config.Suffix == "" {
		config.Suffix = "_right"
	}

	var resultCols []series.Series

	// Add all left columns
	for _, col := range left.columns {
		taken, ok := series.TakeFast(col, leftIndices)
		if ok {
			resultCols = append(resultCols, taken)
		} else {
			resultCols = append(resultCols, col.Take(leftIndices))
		}
	}

	// Add all right columns (with suffix for conflicts)
	leftNames := make(map[string]bool)
	for _, col := range left.columns {
		leftNames[col.Name()] = true
	}

	for _, col := range right.columns {
		name := col.Name()
		if leftNames[name] {
			name = name + config.Suffix
		}
		taken, ok := series.TakeFast(col, rightIndices)
		if ok {
			resultCols = append(resultCols, taken.Rename(name))
		} else {
			resultCols = append(resultCols, col.Take(rightIndices).Rename(name))
		}
	}

	return NewDataFrame(resultCols...)
}

// concatenateDataFrames combines two DataFrames vertically
func concatenateDataFrames(df1, df2 *DataFrame) (*DataFrame, error) {
	return Concat([]*DataFrame{df1, df2}, ConcatOptions{Axis: 0, Join: "outer"})
}
