package window

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// rowNumberFunc implements the ROW_NUMBER() window function
type rowNumberFunc struct {
	spec *Spec
}

// RowNumber creates a ROW_NUMBER() window function
func RowNumber() WindowFunc {
	return WindowFunc{&rowNumberFunc{}}
}

// SetSpec sets the window specification
func (f *rowNumberFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates row numbers for each row in the partition
func (f *rowNumberFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()
	result := make([]int64, size)
	
	if partition.IsOrdered() {
		// Use the order indices
		orderIndices := partition.OrderIndices()
		for i, idx := range orderIndices {
			// Map back to original position
			for j, origIdx := range partition.Indices() {
				if origIdx == idx {
					result[j] = int64(i + 1)
					break
				}
			}
		}
	} else {
		// No ordering, just assign sequential numbers
		for i := range result {
			result[i] = int64(i + 1)
		}
	}
	
	return series.NewInt64Series("row_number", result), nil
}

// DataType returns Int64 as row numbers are always integers
func (f *rowNumberFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return datatypes.Int64{}
}

// Name returns the function name
func (f *rowNumberFunc) Name() string {
	return "row_number"
}

// Validate checks if the window specification is valid
func (f *rowNumberFunc) Validate(spec *Spec) error {
	// ROW_NUMBER() doesn't require ORDER BY, but it's usually used with it
	return nil
}

// rankFunc implements the RANK() window function
type rankFunc struct {
	spec *Spec
}

// Rank creates a RANK() window function
func Rank() WindowFunc {
	return WindowFunc{&rankFunc{}}
}

// SetSpec sets the window specification
func (f *rankFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates ranks for each row in the partition
func (f *rankFunc) Compute(partition Partition) (series.Series, error) {
	if !partition.IsOrdered() {
		return nil, fmt.Errorf("RANK() requires ORDER BY clause")
	}
	
	size := partition.Size()
	result := make([]int64, size)
	orderIndices := partition.OrderIndices()
	
	// Get the ORDER BY columns for tie detection
	if f.spec == nil || !f.spec.HasOrderBy() {
		return nil, fmt.Errorf("RANK() requires window specification with ORDER BY")
	}
	
	// Extract values from ORDER BY columns for comparison
	orderByValues := make([][]interface{}, size)
	for i, idx := range orderIndices {
		orderByValues[i] = make([]interface{}, len(f.spec.orderBy))
		for j, orderClause := range f.spec.orderBy {
			if orderClause.Column != "" {
				s, err := partition.Column(orderClause.Column)
				if err == nil {
					orderByValues[i][j] = s.Get(idx)
				}
			}
		}
	}
	
	currentRank := int64(1)
	
	for i, idx := range orderIndices {
		// Check if this row has the same values as the previous row (tie)
		if i > 0 && f.valuesEqual(orderByValues[i-1], orderByValues[i]) {
			// Same rank as previous row (tie)
			// currentRank stays the same
		} else if i > 0 {
			// Different values, update rank to current position + 1
			currentRank = int64(i + 1)
		}
		
		// Map back to original position
		for j, origIdx := range partition.Indices() {
			if origIdx == idx {
				result[j] = currentRank
				break
			}
		}
	}
	
	return series.NewInt64Series("rank", result), nil
}

// valuesEqual compares two sets of ORDER BY values
func (f *rankFunc) valuesEqual(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !f.compareValues(a[i], b[i]) {
			return false
		}
	}
	return true
}

// compareValues compares two values for equality
func (f *rankFunc) compareValues(a, b interface{}) bool {
	// Handle nil values
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	
	// Use fmt.Sprintf for generic comparison
	// In a production implementation, we'd use type-specific comparisons
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// DataType returns Int64 as ranks are always integers
func (f *rankFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return datatypes.Int64{}
}

// Name returns the function name
func (f *rankFunc) Name() string {
	return "rank"
}

// Validate checks if the window specification is valid
func (f *rankFunc) Validate(spec *Spec) error {
	if !spec.HasOrderBy() {
		return fmt.Errorf("RANK() requires ORDER BY clause")
	}
	return nil
}

// denseRankFunc implements the DENSE_RANK() window function
type denseRankFunc struct {
	spec *Spec
}

// DenseRank creates a DENSE_RANK() window function
func DenseRank() WindowFunc {
	return WindowFunc{&denseRankFunc{}}
}

// SetSpec sets the window specification
func (f *denseRankFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates dense ranks for each row in the partition
func (f *denseRankFunc) Compute(partition Partition) (series.Series, error) {
	if !partition.IsOrdered() {
		return nil, fmt.Errorf("DENSE_RANK() requires ORDER BY clause")
	}
	
	size := partition.Size()
	result := make([]int64, size)
	orderIndices := partition.OrderIndices()
	
	// Get the ORDER BY columns for tie detection
	if f.spec == nil || !f.spec.HasOrderBy() {
		return nil, fmt.Errorf("DENSE_RANK() requires window specification with ORDER BY")
	}
	
	// Extract values from ORDER BY columns for comparison
	orderByValues := make([][]interface{}, size)
	for i, idx := range orderIndices {
		orderByValues[i] = make([]interface{}, len(f.spec.orderBy))
		for j, orderClause := range f.spec.orderBy {
			if orderClause.Column != "" {
				s, err := partition.Column(orderClause.Column)
				if err == nil {
					orderByValues[i][j] = s.Get(idx)
				}
			}
		}
	}
	
	currentRank := int64(1)
	
	for i, idx := range orderIndices {
		// Check if this row has different values than the previous row
		if i > 0 && !f.valuesEqual(orderByValues[i-1], orderByValues[i]) {
			// Different values, increment dense rank
			currentRank++
		}
		// If values are equal (tie), keep the same rank
		
		// Map back to original position
		for j, origIdx := range partition.Indices() {
			if origIdx == idx {
				result[j] = currentRank
				break
			}
		}
	}
	
	return series.NewInt64Series("dense_rank", result), nil
}

// valuesEqual compares two sets of ORDER BY values
func (f *denseRankFunc) valuesEqual(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !f.compareValues(a[i], b[i])  {
			return false
		}
	}
	return true
}

// compareValues compares two values for equality
func (f *denseRankFunc) compareValues(a, b interface{}) bool {
	// Handle nil values
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	
	// Use fmt.Sprintf for generic comparison
	// In a production implementation, we'd use type-specific comparisons
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// DataType returns Int64 as dense ranks are always integers
func (f *denseRankFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return datatypes.Int64{}
}

// Name returns the function name
func (f *denseRankFunc) Name() string {
	return "dense_rank"
}

// Validate checks if the window specification is valid
func (f *denseRankFunc) Validate(spec *Spec) error {
	if !spec.HasOrderBy() {
		return fmt.Errorf("DENSE_RANK() requires ORDER BY clause")
	}
	return nil
}

// percentRankFunc implements the PERCENT_RANK() window function
type percentRankFunc struct {
	spec *Spec
}

// PercentRank creates a PERCENT_RANK() window function
func PercentRank() WindowFunc {
	return WindowFunc{&percentRankFunc{}}
}

// SetSpec sets the window specification
func (f *percentRankFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates percent ranks for each row in the partition
func (f *percentRankFunc) Compute(partition Partition) (series.Series, error) {
	if !partition.IsOrdered() {
		return nil, fmt.Errorf("PERCENT_RANK() requires ORDER BY clause")
	}
	
	size := partition.Size()
	if size == 1 {
		// Single row always gets 0.0
		return series.NewFloat64Series("percent_rank", []float64{0.0}), nil
	}
	
	result := make([]float64, size)
	orderIndices := partition.OrderIndices()
	
	// Get the ORDER BY columns for tie detection
	if f.spec == nil || !f.spec.HasOrderBy() {
		return nil, fmt.Errorf("PERCENT_RANK() requires window specification with ORDER BY")
	}
	
	// Extract values from ORDER BY columns for comparison
	orderByValues := make([][]interface{}, size)
	for i, idx := range orderIndices {
		orderByValues[i] = make([]interface{}, len(f.spec.orderBy))
		for j, orderClause := range f.spec.orderBy {
			if orderClause.Column != "" {
				s, err := partition.Column(orderClause.Column)
				if err == nil {
					orderByValues[i][j] = s.Get(idx)
				}
			}
		}
	}
	
	// First pass: compute ranks with tie handling
	ranks := make([]int64, size)
	currentRank := int64(1)
	
	for i := range orderIndices {
		if i > 0 && f.valuesEqual(orderByValues[i-1], orderByValues[i]) {
			// Same rank as previous row (tie)
		} else if i > 0 {
			// Different values, update rank to current position + 1
			currentRank = int64(i + 1)
		}
		ranks[i] = currentRank
	}
	
	// Second pass: convert ranks to percent ranks
	// percent_rank = (rank - 1) / (total_rows - 1)
	for i, idx := range orderIndices {
		percentRank := float64(ranks[i] - 1) / float64(size - 1)
		
		// Map back to original position
		for j, origIdx := range partition.Indices() {
			if origIdx == idx {
				result[j] = percentRank
				break
			}
		}
	}
	
	return series.NewFloat64Series("percent_rank", result), nil
}

// valuesEqual compares two sets of ORDER BY values
func (f *percentRankFunc) valuesEqual(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !f.compareValues(a[i], b[i]) {
			return false
		}
	}
	return true
}

// compareValues compares two values for equality
func (f *percentRankFunc) compareValues(a, b interface{}) bool {
	// Handle nil values
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	
	// Use fmt.Sprintf for generic comparison
	// In a production implementation, we'd use type-specific comparisons
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// DataType returns Float64 as percent ranks are always floats
func (f *percentRankFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return datatypes.Float64{}
}

// Name returns the function name
func (f *percentRankFunc) Name() string {
	return "percent_rank"
}

// Validate checks if the window specification is valid
func (f *percentRankFunc) Validate(spec *Spec) error {
	if !spec.HasOrderBy() {
		return fmt.Errorf("PERCENT_RANK() requires ORDER BY clause")
	}
	return nil
}

// ntileFunc implements the NTILE() window function
type ntileFunc struct {
	buckets int
	spec    *Spec
}

// lagFunc implements the LAG() window function
type lagFunc struct {
	column       string
	offset       int
	defaultValue interface{}
	spec         *Spec
}

// leadFunc implements the LEAD() window function
type leadFunc struct {
	column       string
	offset       int
	defaultValue interface{}
	spec         *Spec
}

// firstValueFunc implements the FIRST_VALUE() window function
type firstValueFunc struct {
	column string
	spec   *Spec
}

// lastValueFunc implements the LAST_VALUE() window function
type lastValueFunc struct {
	column string
	spec   *Spec
}

// nthValueFunc implements the NTH_VALUE() window function
type nthValueFunc struct {
	column string
	n      int
	spec   *Spec
}

// NTile creates an NTILE() window function
func NTile(buckets int) WindowFunc {
	if buckets <= 0 {
		buckets = 1
	}
	return WindowFunc{&ntileFunc{buckets: buckets}}
}

// SetSpec sets the window specification
func (f *ntileFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute divides the partition into n buckets
func (f *ntileFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()
	result := make([]int64, size)
	
	if partition.IsOrdered() {
		orderIndices := partition.OrderIndices()
		
		// Calculate base size and remainder
		baseSize := size / f.buckets
		remainder := size % f.buckets
		
		currentBucket := 1
		rowsInCurrentBucket := 0
		bucketSize := baseSize
		if remainder > 0 {
			bucketSize++
		}
		
		for _, idx := range orderIndices {
			// Map back to original position
			for j, origIdx := range partition.Indices() {
				if origIdx == idx {
					result[j] = int64(currentBucket)
					break
				}
			}
			
			rowsInCurrentBucket++
			
			// Check if we need to move to next bucket
			if rowsInCurrentBucket >= bucketSize {
				currentBucket++
				rowsInCurrentBucket = 0
				
				// Adjust bucket size for remaining buckets
				if currentBucket-1 == remainder {
					bucketSize = baseSize
				}
			}
		}
	} else {
		// No ordering, distribute evenly
		baseSize := size / f.buckets
		remainder := size % f.buckets
		
		currentBucket := 1
		rowsInCurrentBucket := 0
		bucketSize := baseSize
		if remainder > 0 {
			bucketSize++
		}
		
		for i := 0; i < size; i++ {
			result[i] = int64(currentBucket)
			rowsInCurrentBucket++
			
			if rowsInCurrentBucket >= bucketSize {
				currentBucket++
				rowsInCurrentBucket = 0
				
				if currentBucket-1 == remainder {
					bucketSize = baseSize
				}
			}
		}
	}
	
	return series.NewInt64Series("ntile", result), nil
}

// DataType returns Int64 as bucket numbers are integers
func (f *ntileFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return datatypes.Int64{}
}

// Name returns the function name
func (f *ntileFunc) Name() string {
	return fmt.Sprintf("ntile(%d)", f.buckets)
}

// Validate checks if the window specification is valid
func (f *ntileFunc) Validate(spec *Spec) error {
	// NTILE doesn't require ORDER BY but works better with it
	return nil
}

// Lag creates a LAG() window function
func Lag(column string, offset int, defaultValue ...interface{}) WindowFunc {
	f := &lagFunc{
		column: column,
		offset: offset,
	}
	if len(defaultValue) > 0 {
		f.defaultValue = defaultValue[0]
	}
	return WindowFunc{f}
}

// SetSpec sets the window specification
func (f *lagFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates lag values for each row in the partition
func (f *lagFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()
	
	// Get the column to lag
	columnSeries, err := partition.Column(f.column)
	if err != nil {
		return nil, fmt.Errorf("column %s not found", f.column)
	}
	
	// Create result based on the column's data type
	dataType := columnSeries.DataType()
	
	if partition.IsOrdered() {
		orderIndices := partition.OrderIndices()
		
		// Create a mapping from original index to position in ordered sequence
		positionMap := make(map[int]int)
		for pos, idx := range orderIndices {
			positionMap[idx] = pos
		}
		
		// Build result based on data type
		switch dataType.String() {
		case "i32":
			result := make([]int32, size)
			defaultVal := int32(0)
			if f.defaultValue != nil {
				if v, ok := f.defaultValue.(int32); ok {
					defaultVal = v
				} else if v, ok := f.defaultValue.(int); ok {
					defaultVal = int32(v)
				}
			}
			
			for i, idx := range partition.Indices() {
				pos := positionMap[idx]
				lagPos := pos - f.offset
				
				if lagPos >= 0 && lagPos < len(orderIndices) {
					lagIdx := orderIndices[lagPos]
					result[i] = columnSeries.Get(lagIdx).(int32)
				} else {
					result[i] = defaultVal
				}
			}
			return series.NewInt32Series("lag", result), nil
			
		case "i64":
			result := make([]int64, size)
			defaultVal := int64(0)
			if f.defaultValue != nil {
				if v, ok := f.defaultValue.(int64); ok {
					defaultVal = v
				} else if v, ok := f.defaultValue.(int); ok {
					defaultVal = int64(v)
				}
			}
			
			for i, idx := range partition.Indices() {
				pos := positionMap[idx]
				lagPos := pos - f.offset
				
				if lagPos >= 0 && lagPos < len(orderIndices) {
					lagIdx := orderIndices[lagPos]
					result[i] = columnSeries.Get(lagIdx).(int64)
				} else {
					result[i] = defaultVal
				}
			}
			return series.NewInt64Series("lag", result), nil
			
		case "str":
			result := make([]string, size)
			defaultVal := ""
			if f.defaultValue != nil {
				if v, ok := f.defaultValue.(string); ok {
					defaultVal = v
				}
			}
			
			for i, idx := range partition.Indices() {
				pos := positionMap[idx]
				lagPos := pos - f.offset
				
				if lagPos >= 0 && lagPos < len(orderIndices) {
					lagIdx := orderIndices[lagPos]
					result[i] = columnSeries.Get(lagIdx).(string)
				} else {
					result[i] = defaultVal
				}
			}
			return series.NewStringSeries("lag", result), nil
			
		default:
			return nil, fmt.Errorf("unsupported data type for LAG: %s", dataType)
		}
	} else {
		// No ordering - lag based on natural row order
		switch dataType.String() {
		case "i32":
			result := make([]int32, size)
			defaultVal := int32(0)
			if f.defaultValue != nil {
				if v, ok := f.defaultValue.(int32); ok {
					defaultVal = v
				} else if v, ok := f.defaultValue.(int); ok {
					defaultVal = int32(v)
				}
			}
			
			for i := 0; i < size; i++ {
				lagIdx := i - f.offset
				if lagIdx >= 0 && lagIdx < size {
					result[i] = columnSeries.Get(partition.Indices()[lagIdx]).(int32)
				} else {
					result[i] = defaultVal
				}
			}
			return series.NewInt32Series("lag", result), nil
			
		default:
			return nil, fmt.Errorf("unsupported data type for LAG: %s", dataType)
		}
	}
}

// DataType returns the same type as the input column
func (f *lagFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}

// Name returns the function name
func (f *lagFunc) Name() string {
	return fmt.Sprintf("lag(%d)", f.offset)
}

// Validate checks if the window specification is valid
func (f *lagFunc) Validate(spec *Spec) error {
	// LAG typically requires ORDER BY to be meaningful
	return nil
}

// Lead creates a LEAD() window function
func Lead(column string, offset int, defaultValue ...interface{}) WindowFunc {
	f := &leadFunc{
		column: column,
		offset: offset,
	}
	if len(defaultValue) > 0 {
		f.defaultValue = defaultValue[0]
	}
	return WindowFunc{f}
}

// SetSpec sets the window specification
func (f *leadFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates lead values for each row in the partition
func (f *leadFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()
	
	// Get the column to lead
	columnSeries, err := partition.Column(f.column)
	if err != nil {
		return nil, fmt.Errorf("column %s not found", f.column)
	}
	
	// Create result based on the column's data type
	dataType := columnSeries.DataType()
	
	if partition.IsOrdered() {
		orderIndices := partition.OrderIndices()
		
		// Create a mapping from original index to position in ordered sequence
		positionMap := make(map[int]int)
		for pos, idx := range orderIndices {
			positionMap[idx] = pos
		}
		
		// Build result based on data type
		switch dataType.String() {
		case "i32":
			result := make([]int32, size)
			defaultVal := int32(0)
			if f.defaultValue != nil {
				if v, ok := f.defaultValue.(int32); ok {
					defaultVal = v
				} else if v, ok := f.defaultValue.(int); ok {
					defaultVal = int32(v)
				}
			}
			
			for i, idx := range partition.Indices() {
				pos := positionMap[idx]
				leadPos := pos + f.offset // LEAD looks forward
				
				if leadPos >= 0 && leadPos < len(orderIndices) {
					leadIdx := orderIndices[leadPos]
					result[i] = columnSeries.Get(leadIdx).(int32)
				} else {
					result[i] = defaultVal
				}
			}
			return series.NewInt32Series("lead", result), nil
			
		case "i64":
			result := make([]int64, size)
			defaultVal := int64(0)
			if f.defaultValue != nil {
				if v, ok := f.defaultValue.(int64); ok {
					defaultVal = v
				} else if v, ok := f.defaultValue.(int); ok {
					defaultVal = int64(v)
				}
			}
			
			for i, idx := range partition.Indices() {
				pos := positionMap[idx]
				leadPos := pos + f.offset
				
				if leadPos >= 0 && leadPos < len(orderIndices) {
					leadIdx := orderIndices[leadPos]
					result[i] = columnSeries.Get(leadIdx).(int64)
				} else {
					result[i] = defaultVal
				}
			}
			return series.NewInt64Series("lead", result), nil
			
		case "str":
			result := make([]string, size)
			defaultVal := ""
			if f.defaultValue != nil {
				if v, ok := f.defaultValue.(string); ok {
					defaultVal = v
				}
			}
			
			for i, idx := range partition.Indices() {
				pos := positionMap[idx]
				leadPos := pos + f.offset
				
				if leadPos >= 0 && leadPos < len(orderIndices) {
					leadIdx := orderIndices[leadPos]
					result[i] = columnSeries.Get(leadIdx).(string)
				} else {
					result[i] = defaultVal
				}
			}
			return series.NewStringSeries("lead", result), nil
			
		default:
			return nil, fmt.Errorf("unsupported data type for LEAD: %s", dataType)
		}
	} else {
		// No ordering - lead based on natural row order
		return nil, fmt.Errorf("LEAD requires ORDER BY clause")
	}
}

// DataType returns the same type as the input column
func (f *leadFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}

// Name returns the function name
func (f *leadFunc) Name() string {
	return fmt.Sprintf("lead(%d)", f.offset)
}

// Validate checks if the window specification is valid
func (f *leadFunc) Validate(spec *Spec) error {
	// LEAD typically requires ORDER BY to be meaningful
	return nil
}

// FirstValue creates a FIRST_VALUE() window function
func FirstValue(column string) WindowFunc {
	return WindowFunc{&firstValueFunc{column: column}}
}

// SetSpec sets the window specification
func (f *firstValueFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute returns the first value in the window frame
func (f *firstValueFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()
	
	// Get the column
	columnSeries, err := partition.Column(f.column)
	if err != nil {
		return nil, fmt.Errorf("column %s not found", f.column)
	}
	
	// Create result based on the column's data type
	dataType := columnSeries.DataType()
	
	if partition.IsOrdered() {
		orderIndices := partition.OrderIndices()
		if len(orderIndices) == 0 {
			return nil, fmt.Errorf("empty partition")
		}
		
		// Get the first value in ordered sequence
		firstIdx := orderIndices[0]
		firstValue := columnSeries.Get(firstIdx)
		
		// Build result based on data type
		switch dataType.String() {
		case "i32":
			result := make([]int32, size)
			val := firstValue.(int32)
			for i := range result {
				result[i] = val
			}
			return series.NewInt32Series("first_value", result), nil
			
		case "i64":
			result := make([]int64, size)
			val := firstValue.(int64)
			for i := range result {
				result[i] = val
			}
			return series.NewInt64Series("first_value", result), nil
			
		case "str":
			result := make([]string, size)
			val := firstValue.(string)
			for i := range result {
				result[i] = val
			}
			return series.NewStringSeries("first_value", result), nil
			
		default:
			return nil, fmt.Errorf("unsupported data type for FIRST_VALUE: %s", dataType)
		}
	} else {
		// No ordering - use first row in partition
		if size == 0 {
			return nil, fmt.Errorf("empty partition")
		}
		
		firstValue := columnSeries.Get(partition.Indices()[0])
		
		switch dataType.String() {
		case "i32":
			result := make([]int32, size)
			val := firstValue.(int32)
			for i := range result {
				result[i] = val
			}
			return series.NewInt32Series("first_value", result), nil
			
		case "i64":
			result := make([]int64, size)
			val := firstValue.(int64)
			for i := range result {
				result[i] = val
			}
			return series.NewInt64Series("first_value", result), nil
			
		case "str":
			result := make([]string, size)
			val := firstValue.(string)
			for i := range result {
				result[i] = val
			}
			return series.NewStringSeries("first_value", result), nil
			
		default:
			return nil, fmt.Errorf("unsupported data type for FIRST_VALUE: %s", dataType)
		}
	}
}

// DataType returns the same type as the input column
func (f *firstValueFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}

// Name returns the function name
func (f *firstValueFunc) Name() string {
	return "first_value"
}

// Validate checks if the window specification is valid
func (f *firstValueFunc) Validate(spec *Spec) error {
	return nil
}

// LastValue creates a LAST_VALUE() window function
func LastValue(column string) WindowFunc {
	return WindowFunc{&lastValueFunc{column: column}}
}

// SetSpec sets the window specification
func (f *lastValueFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute returns the last value in the window frame
func (f *lastValueFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()
	
	// Get the column
	columnSeries, err := partition.Column(f.column)
	if err != nil {
		return nil, fmt.Errorf("column %s not found", f.column)
	}
	
	// Create result based on the column's data type
	dataType := columnSeries.DataType()
	
	if partition.IsOrdered() {
		orderIndices := partition.OrderIndices()
		if len(orderIndices) == 0 {
			return nil, fmt.Errorf("empty partition")
		}
		
		// Get the last value in ordered sequence
		lastIdx := orderIndices[len(orderIndices)-1]
		lastValue := columnSeries.Get(lastIdx)
		
		// Build result based on data type
		switch dataType.String() {
		case "i32":
			result := make([]int32, size)
			val := lastValue.(int32)
			for i := range result {
				result[i] = val
			}
			return series.NewInt32Series("last_value", result), nil
			
		case "i64":
			result := make([]int64, size)
			val := lastValue.(int64)
			for i := range result {
				result[i] = val
			}
			return series.NewInt64Series("last_value", result), nil
			
		case "str":
			result := make([]string, size)
			val := lastValue.(string)
			for i := range result {
				result[i] = val
			}
			return series.NewStringSeries("last_value", result), nil
			
		default:
			return nil, fmt.Errorf("unsupported data type for LAST_VALUE: %s", dataType)
		}
	} else {
		// No ordering - use last row in partition
		if size == 0 {
			return nil, fmt.Errorf("empty partition")
		}
		
		lastValue := columnSeries.Get(partition.Indices()[size-1])
		
		switch dataType.String() {
		case "i32":
			result := make([]int32, size)
			val := lastValue.(int32)
			for i := range result {
				result[i] = val
			}
			return series.NewInt32Series("last_value", result), nil
			
		case "i64":
			result := make([]int64, size)
			val := lastValue.(int64)
			for i := range result {
				result[i] = val
			}
			return series.NewInt64Series("last_value", result), nil
			
		case "str":
			result := make([]string, size)
			val := lastValue.(string)
			for i := range result {
				result[i] = val
			}
			return series.NewStringSeries("last_value", result), nil
			
		default:
			return nil, fmt.Errorf("unsupported data type for LAST_VALUE: %s", dataType)
		}
	}
}

// DataType returns the same type as the input column
func (f *lastValueFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}

// Name returns the function name
func (f *lastValueFunc) Name() string {
	return "last_value"
}

// Validate checks if the window specification is valid
func (f *lastValueFunc) Validate(spec *Spec) error {
	return nil
}

// NthValue creates a NTH_VALUE() window function
func NthValue(column string, n int) WindowFunc {
	return WindowFunc{&nthValueFunc{column: column, n: n}}
}

// SetSpec sets the window specification
func (f *nthValueFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute returns the nth value in the window for each row
func (f *nthValueFunc) Compute(partition Partition) (series.Series, error) {
	col, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	// Get window frame
	var frame *FrameSpec
	if f.spec != nil && f.spec.GetFrame() != nil {
		frame = f.spec.GetFrame()
	} else {
		// Default frame
		if partition.IsOrdered() {
			frame = &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: UnboundedPreceding},
				End:   FrameBound{Type: CurrentRow},
			}
		} else {
			frame = &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: UnboundedPreceding},
				End:   FrameBound{Type: UnboundedFollowing},
			}
		}
	}

	// Get indices
	indices := partition.Indices()
	var orderIndices []int
	if partition.IsOrdered() && len(partition.OrderIndices()) > 0 {
		orderIndices = partition.OrderIndices()
	} else {
		orderIndices = indices
	}

	// Build result based on data type
	dataType := col.DataType()
	size := partition.Size()
	
	switch dataType.(type) {
	case datatypes.Int32:
		values := make([]int32, size)
		validity := make([]bool, size)
		
		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			
			// Calculate the actual position
			// n is 1-based, so nth=1 means first value
			targetIdx := start + f.n - 1
			
			// Check if the nth position is within the window
			if targetIdx >= start && targetIdx < end && targetIdx < len(orderIndices) {
				actualIdx := orderIndices[targetIdx]
				if !col.IsNull(actualIdx) {
					values[i] = col.Get(actualIdx).(int32)
					validity[i] = true
				}
			}
		}
		
		return series.NewSeriesWithValidity(col.Name()+"_nth_value", values, validity, dataType), nil
		
	case datatypes.Int64:
		values := make([]int64, size)
		validity := make([]bool, size)
		
		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			targetIdx := start + f.n - 1
			
			if targetIdx >= start && targetIdx < end && targetIdx < len(orderIndices) {
				actualIdx := orderIndices[targetIdx]
				if !col.IsNull(actualIdx) {
					values[i] = col.Get(actualIdx).(int64)
					validity[i] = true
				}
			}
		}
		
		return series.NewSeriesWithValidity(col.Name()+"_nth_value", values, validity, dataType), nil
		
	case datatypes.Float64:
		values := make([]float64, size)
		validity := make([]bool, size)
		
		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			targetIdx := start + f.n - 1
			
			if targetIdx >= start && targetIdx < end && targetIdx < len(orderIndices) {
				actualIdx := orderIndices[targetIdx]
				if !col.IsNull(actualIdx) {
					values[i] = col.Get(actualIdx).(float64)
					validity[i] = true
				}
			}
		}
		
		return series.NewSeriesWithValidity(col.Name()+"_nth_value", values, validity, dataType), nil
		
	case datatypes.String:
		values := make([]string, size)
		validity := make([]bool, size)
		
		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			targetIdx := start + f.n - 1
			
			if targetIdx >= start && targetIdx < end && targetIdx < len(orderIndices) {
				actualIdx := orderIndices[targetIdx]
				if !col.IsNull(actualIdx) {
					values[i] = col.Get(actualIdx).(string)
					validity[i] = true
				}
			}
		}
		
		return series.NewSeriesWithValidity(col.Name()+"_nth_value", values, validity, dataType), nil
		
	default:
		return nil, fmt.Errorf("unsupported data type for NTH_VALUE: %v", dataType)
	}
}

// DataType returns the output data type
func (f *nthValueFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}

// String returns a string representation
func (f *nthValueFunc) String() string {
	return fmt.Sprintf("nth_value(%s, %d)", f.column, f.n)
}

// Name returns the function name
func (f *nthValueFunc) Name() string {
	return "nth_value"
}

// Validate checks if the window specification is valid
func (f *nthValueFunc) Validate(spec *Spec) error {
	if f.n <= 0 {
		return fmt.Errorf("NTH_VALUE: n must be positive, got %d", f.n)
	}
	return nil
}