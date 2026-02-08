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
	return WrapFunction(&rowNumberFunc{})
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
		for rank, origIdx := range orderIndices {
			// Map back to original position
			result[origIdx] = int64(rank + 1)
		}
	} else {
		// No ordering, just assign sequential numbers
		for i := 0; i < size; i++ {
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
	return WrapFunction(&rankFunc{})
}

// SetSpec sets the window specification
func (f *rankFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates ranks for each row in the partition
func (f *rankFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()

	if f.spec == nil || !f.spec.HasOrderBy() {
		return nil, fmt.Errorf("RANK requires ORDER BY")
	}

	result := make([]int64, size)
	orderIndices := partition.OrderIndices()

	// Get the ORDER BY columns for tie detection
	orderCols := f.spec.GetOrderBy()

	// Extract values from ORDER BY columns for comparison
	getValues := func(origIdx int) []interface{} {
		vals := make([]interface{}, len(orderCols))
		for c, col := range orderCols {
			s, err := partition.Column(col.Column)
			if err != nil {
				vals[c] = nil
				continue
			}
			vals[c] = s.Get(origIdx)
		}
		return vals
	}

	currentRank := int64(1)
	var prevValues []interface{}

	for i, origIdx := range orderIndices {
		currentValues := getValues(origIdx)

		if i == 0 {
			currentRank = 1
		} else {
			// Check if this row has the same values as the previous row (tie)
			if f.valuesEqual(currentValues, prevValues) {
				// Same rank as previous row (tie)
				// currentRank stays the same
			} else {
				// Different values, update rank to current position + 1
				currentRank = int64(i + 1)
			}
		}

		// Map back to original position
		result[origIdx] = currentRank
		prevValues = currentValues
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
	if spec == nil || !spec.HasOrderBy() {
		return fmt.Errorf("RANK requires ORDER BY")
	}
	return nil
}

// denseRankFunc implements the DENSE_RANK() window function
type denseRankFunc struct {
	spec *Spec
}

// DenseRank creates a DENSE_RANK() window function
func DenseRank() WindowFunc {
	return WrapFunction(&denseRankFunc{})
}

// SetSpec sets the window specification
func (f *denseRankFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates dense ranks for each row in the partition
func (f *denseRankFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()

	if f.spec == nil || !f.spec.HasOrderBy() {
		return nil, fmt.Errorf("DENSE_RANK requires ORDER BY")
	}

	result := make([]int64, size)
	orderIndices := partition.OrderIndices()

	// Get the ORDER BY columns for tie detection
	orderCols := f.spec.GetOrderBy()

	// Extract values from ORDER BY columns for comparison
	getValues := func(origIdx int) []interface{} {
		vals := make([]interface{}, len(orderCols))
		for c, col := range orderCols {
			s, err := partition.Column(col.Column)
			if err != nil {
				vals[c] = nil
				continue
			}
			vals[c] = s.Get(origIdx)
		}
		return vals
	}

	currentRank := int64(1)
	var prevValues []interface{}

	for i, origIdx := range orderIndices {
		currentValues := getValues(origIdx)

		if i == 0 {
			currentRank = 1
		} else {
			// Check if this row has different values than the previous row
			if !f.valuesEqual(currentValues, prevValues) {
				// Different values, increment dense rank
				currentRank++
			}
			// If values are equal (tie), keep the same rank
		}

		// Map back to original position
		result[origIdx] = currentRank
		prevValues = currentValues
	}

	return series.NewInt64Series("dense_rank", result), nil
}

// valuesEqual compares two sets of ORDER BY values
func (f *denseRankFunc) valuesEqual(a, b []interface{}) bool {
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
func (f *denseRankFunc) compareValues(a, b interface{}) bool {
	// Handle nil values
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Use fmt.Sprintf for generic comparison
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
	if spec == nil || !spec.HasOrderBy() {
		return fmt.Errorf("DENSE_RANK requires ORDER BY")
	}
	return nil
}

// percentRankFunc implements the PERCENT_RANK() window function
type percentRankFunc struct {
	spec *Spec
}

// PercentRank creates a PERCENT_RANK() window function
func PercentRank() WindowFunc {
	return WrapFunction(&percentRankFunc{})
}

// SetSpec sets the window specification
func (f *percentRankFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates percent ranks for each row in the partition
func (f *percentRankFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()

	if f.spec == nil || !f.spec.HasOrderBy() {
		return nil, fmt.Errorf("PERCENT_RANK requires ORDER BY")
	}

	result := make([]float64, size)

	// Single row always gets 0.0
	if size <= 1 {
		return series.NewFloat64Series("percent_rank", result), nil
	}

	orderIndices := partition.OrderIndices()

	// Get the ORDER BY columns for tie detection
	orderCols := f.spec.GetOrderBy()

	getValues := func(origIdx int) []interface{} {
		vals := make([]interface{}, len(orderCols))
		for c, col := range orderCols {
			s, err := partition.Column(col.Column)
			if err != nil {
				vals[c] = nil
				continue
			}
			vals[c] = s.Get(origIdx)
		}
		return vals
	}

	// First pass: compute ranks with tie handling
	ranks := make([]int64, size)
	currentRank := int64(1)
	var prevValues []interface{}

	for i, origIdx := range orderIndices {
		currentValues := getValues(origIdx)

		if i == 0 {
			currentRank = 1
		} else {
			if f.valuesEqual(currentValues, prevValues) {
				// Same rank as previous row (tie)
			} else {
				// Different values, update rank to current position + 1
				currentRank = int64(i + 1)
			}
		}

		ranks[i] = currentRank
		prevValues = currentValues
	}

	// Second pass: convert ranks to percent ranks
	// percent_rank = (rank - 1) / (total_rows - 1)
	denominator := float64(size - 1)
	for i, origIdx := range orderIndices {
		// Map back to original position
		result[origIdx] = float64(ranks[i]-1) / denominator
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
	if spec == nil || !spec.HasOrderBy() {
		return fmt.Errorf("PERCENT_RANK requires ORDER BY")
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
	return WrapFunction(&ntileFunc{buckets: buckets})
}

// SetSpec sets the window specification
func (f *ntileFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute divides the partition into n buckets
func (f *ntileFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()
	result := make([]int64, size)

	if size == 0 {
		return series.NewInt64Series("ntile", result), nil
	}

	buckets := f.buckets
	if buckets > size {
		buckets = size
	}

	if partition.IsOrdered() {
		orderIndices := partition.OrderIndices()
		// Calculate base size and remainder
		baseSize := size / buckets
		remainder := size % buckets

		bucket := int64(1)
		count := 0
		currentBucketSize := baseSize
		if remainder > 0 {
			currentBucketSize++
		}
		usedRemainder := 0

		for i, origIdx := range orderIndices {
			_ = i
			// Map back to original position
			result[origIdx] = bucket
			count++

			// Check if we need to move to next bucket
			if count >= currentBucketSize && bucket < int64(buckets) {
				bucket++
				count = 0
				usedRemainder++
				// Adjust bucket size for remaining buckets
				currentBucketSize = baseSize
				if usedRemainder < remainder {
					currentBucketSize++
				}
			}
		}
	} else {
		// No ordering, distribute evenly
		baseSize := size / buckets
		remainder := size % buckets

		bucket := int64(1)
		count := 0
		currentBucketSize := baseSize
		if remainder > 0 {
			currentBucketSize++
		}
		usedRemainder := 0

		for i := 0; i < size; i++ {
			result[i] = bucket
			count++

			if count >= currentBucketSize && bucket < int64(buckets) {
				bucket++
				count = 0
				usedRemainder++
				currentBucketSize = baseSize
				if usedRemainder < remainder {
					currentBucketSize++
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
	var defVal interface{}
	if len(defaultValue) > 0 {
		defVal = defaultValue[0]
	}
	return WrapFunction(&lagFunc{
		column:       column,
		offset:       offset,
		defaultValue: defVal,
	})
}

// SetSpec sets the window specification
func (f *lagFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates lag values for each row in the partition
func (f *lagFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column to lag
	col, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	size := partition.Size()

	if partition.IsOrdered() {
		orderIndices := partition.OrderIndices()

		// Create a mapping from original index to position in ordered sequence
		positionOf := make(map[int]int, size)
		for pos, origIdx := range orderIndices {
			positionOf[origIdx] = pos
		}

		// Build result based on data type
		switch col.DataType().(type) {
		case datatypes.Int32:
			result := make([]int32, size)
			for _, origIdx := range orderIndices {
				pos := positionOf[origIdx]
				lagPos := pos - f.offset
				if lagPos >= 0 && lagPos < size {
					lagOrigIdx := orderIndices[lagPos]
					result[origIdx] = col.Get(lagOrigIdx).(int32)
				} else {
					if f.defaultValue != nil {
						result[origIdx] = f.defaultValue.(int32)
					}
				}
			}
			return series.NewInt32Series(f.column, result), nil

		case datatypes.Int64:
			result := make([]int64, size)
			for _, origIdx := range orderIndices {
				pos := positionOf[origIdx]
				lagPos := pos - f.offset
				if lagPos >= 0 && lagPos < size {
					lagOrigIdx := orderIndices[lagPos]
					result[origIdx] = col.Get(lagOrigIdx).(int64)
				} else {
					if f.defaultValue != nil {
						result[origIdx] = f.defaultValue.(int64)
					}
				}
			}
			return series.NewInt64Series(f.column, result), nil

		case datatypes.Float64:
			result := make([]float64, size)
			for _, origIdx := range orderIndices {
				pos := positionOf[origIdx]
				lagPos := pos - f.offset
				if lagPos >= 0 && lagPos < size {
					lagOrigIdx := orderIndices[lagPos]
					result[origIdx] = col.Get(lagOrigIdx).(float64)
				} else {
					if f.defaultValue != nil {
						result[origIdx] = f.defaultValue.(float64)
					}
				}
			}
			return series.NewFloat64Series(f.column, result), nil

		case datatypes.String:
			result := make([]string, size)
			for _, origIdx := range orderIndices {
				pos := positionOf[origIdx]
				lagPos := pos - f.offset
				if lagPos >= 0 && lagPos < size {
					lagOrigIdx := orderIndices[lagPos]
					result[origIdx] = col.Get(lagOrigIdx).(string)
				} else {
					if f.defaultValue != nil {
						result[origIdx] = f.defaultValue.(string)
					}
				}
			}
			return series.NewStringSeries(f.column, result), nil
		}
	} else {
		// No ordering - lag based on natural row order
		indices := partition.Indices()
		switch col.DataType().(type) {
		case datatypes.Int32:
			result := make([]int32, size)
			for i, idx := range indices {
				lagI := i - f.offset
				if lagI >= 0 && lagI < size {
					lagIdx := indices[lagI]
					result[idx] = col.Get(lagIdx).(int32)
				} else {
					if f.defaultValue != nil {
						result[idx] = f.defaultValue.(int32)
					}
				}
			}
			return series.NewInt32Series(f.column, result), nil
		}
	}

	return nil, fmt.Errorf("unsupported data type for LAG")
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
	var defVal interface{}
	if len(defaultValue) > 0 {
		defVal = defaultValue[0]
	}
	return WrapFunction(&leadFunc{
		column:       column,
		offset:       offset,
		defaultValue: defVal,
	})
}

// SetSpec sets the window specification
func (f *leadFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates lead values for each row in the partition
func (f *leadFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column to lead
	col, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	size := partition.Size()

	if partition.IsOrdered() {
		orderIndices := partition.OrderIndices()

		// Create a mapping from original index to position in ordered sequence
		positionOf := make(map[int]int, size)
		for pos, origIdx := range orderIndices {
			positionOf[origIdx] = pos
		}

		// Build result based on data type
		switch col.DataType().(type) {
		case datatypes.Int32:
			result := make([]int32, size)
			for _, origIdx := range orderIndices {
				pos := positionOf[origIdx]
				// LEAD looks forward
				leadPos := pos + f.offset
				if leadPos >= 0 && leadPos < size {
					leadOrigIdx := orderIndices[leadPos]
					result[origIdx] = col.Get(leadOrigIdx).(int32)
				} else {
					if f.defaultValue != nil {
						result[origIdx] = f.defaultValue.(int32)
					}
				}
			}
			return series.NewInt32Series(f.column, result), nil

		case datatypes.Int64:
			result := make([]int64, size)
			for _, origIdx := range orderIndices {
				pos := positionOf[origIdx]
				leadPos := pos + f.offset
				if leadPos >= 0 && leadPos < size {
					leadOrigIdx := orderIndices[leadPos]
					result[origIdx] = col.Get(leadOrigIdx).(int64)
				} else {
					if f.defaultValue != nil {
						result[origIdx] = f.defaultValue.(int64)
					}
				}
			}
			return series.NewInt64Series(f.column, result), nil

		case datatypes.Float64:
			result := make([]float64, size)
			for _, origIdx := range orderIndices {
				pos := positionOf[origIdx]
				leadPos := pos + f.offset
				if leadPos >= 0 && leadPos < size {
					leadOrigIdx := orderIndices[leadPos]
					result[origIdx] = col.Get(leadOrigIdx).(float64)
				} else {
					if f.defaultValue != nil {
						result[origIdx] = f.defaultValue.(float64)
					}
				}
			}
			return series.NewFloat64Series(f.column, result), nil

		case datatypes.String:
			result := make([]string, size)
			for _, origIdx := range orderIndices {
				pos := positionOf[origIdx]
				leadPos := pos + f.offset
				if leadPos >= 0 && leadPos < size {
					leadOrigIdx := orderIndices[leadPos]
					result[origIdx] = col.Get(leadOrigIdx).(string)
				} else {
					if f.defaultValue != nil {
						result[origIdx] = f.defaultValue.(string)
					}
				}
			}
			return series.NewStringSeries(f.column, result), nil
		}
	} else {
		// No ordering - lead based on natural row order
		indices := partition.Indices()
		switch col.DataType().(type) {
		case datatypes.Int32:
			result := make([]int32, size)
			for i, idx := range indices {
				leadI := i + f.offset
				if leadI >= 0 && leadI < size {
					leadIdx := indices[leadI]
					result[idx] = col.Get(leadIdx).(int32)
				} else {
					if f.defaultValue != nil {
						result[idx] = f.defaultValue.(int32)
					}
				}
			}
			return series.NewInt32Series(f.column, result), nil
		}
	}

	return nil, fmt.Errorf("unsupported data type for LEAD")
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
	return WrapFunction(&firstValueFunc{column: column})
}

// SetSpec sets the window specification
func (f *firstValueFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute returns the first value in the window frame
func (f *firstValueFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column
	col, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	size := partition.Size()

	var firstIdx int
	if partition.IsOrdered() {
		// Get the first value in ordered sequence
		orderIndices := partition.OrderIndices()
		firstIdx = orderIndices[0]
	} else {
		// No ordering - use first row in partition
		indices := partition.Indices()
		firstIdx = indices[0]
	}

	firstVal := col.Get(firstIdx)

	// Build result based on data type
	switch col.DataType().(type) {
	case datatypes.Int32:
		result := make([]int32, size)
		v := firstVal.(int32)
		for i := range result {
			result[i] = v
		}
		return series.NewInt32Series(f.column, result), nil

	case datatypes.Int64:
		result := make([]int64, size)
		v := firstVal.(int64)
		for i := range result {
			result[i] = v
		}
		return series.NewInt64Series(f.column, result), nil

	case datatypes.Float64:
		result := make([]float64, size)
		v := firstVal.(float64)
		for i := range result {
			result[i] = v
		}
		return series.NewFloat64Series(f.column, result), nil

	case datatypes.String:
		result := make([]string, size)
		v := firstVal.(string)
		for i := range result {
			result[i] = v
		}
		return series.NewStringSeries(f.column, result), nil
	}

	return nil, fmt.Errorf("unsupported data type for FIRST_VALUE")
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
	return WrapFunction(&lastValueFunc{column: column})
}

// SetSpec sets the window specification
func (f *lastValueFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute returns the last value in the window frame
func (f *lastValueFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column
	col, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	size := partition.Size()

	var lastIdx int
	if partition.IsOrdered() {
		// Get the last value in ordered sequence
		orderIndices := partition.OrderIndices()
		lastIdx = orderIndices[size-1]
	} else {
		// No ordering - use last row in partition
		indices := partition.Indices()
		lastIdx = indices[size-1]
	}

	lastVal := col.Get(lastIdx)

	// Build result based on data type
	switch col.DataType().(type) {
	case datatypes.Int32:
		result := make([]int32, size)
		v := lastVal.(int32)
		for i := range result {
			result[i] = v
		}
		return series.NewInt32Series(f.column, result), nil

	case datatypes.Int64:
		result := make([]int64, size)
		v := lastVal.(int64)
		for i := range result {
			result[i] = v
		}
		return series.NewInt64Series(f.column, result), nil

	case datatypes.Float64:
		result := make([]float64, size)
		v := lastVal.(float64)
		for i := range result {
			result[i] = v
		}
		return series.NewFloat64Series(f.column, result), nil

	case datatypes.String:
		result := make([]string, size)
		v := lastVal.(string)
		for i := range result {
			result[i] = v
		}
		return series.NewStringSeries(f.column, result), nil
	}

	return nil, fmt.Errorf("unsupported data type for LAST_VALUE")
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
	return WrapFunction(&nthValueFunc{column: column, n: n})
}

// SetSpec sets the window specification
func (f *nthValueFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute returns the nth value in the window for each row
func (f *nthValueFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column
	col, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	size := partition.Size()

	// Get window frame
	var frame *FrameSpec
	if f.spec != nil {
		frame = f.spec.GetFrame()
	}

	// Default frame: UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING
	if frame == nil {
		frame = &FrameSpec{
			Type:  RowsFrame,
			Start: FrameBound{Type: UnboundedPreceding},
			End:   FrameBound{Type: UnboundedFollowing},
		}
	}

	// Get indices
	var indices []int
	if partition.IsOrdered() {
		indices = partition.OrderIndices()
	} else {
		indices = partition.Indices()
	}

	// Build result based on data type
	switch col.DataType().(type) {
	case datatypes.Int32:
		values := make([]int32, size)
		validity := make([]bool, size)

		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			frameSize := end - start

			// Calculate the actual position
			// n is 1-based, so nth=1 means first value
			nthPos := f.n - 1

			// Check if the nth position is within the window
			if nthPos >= 0 && nthPos < frameSize {
				actualIdx := indices[start+nthPos]
				val := col.Get(actualIdx)
				if col.IsNull(actualIdx) {
					validity[i] = false
				} else {
					values[i] = val.(int32)
					validity[i] = true
				}
			} else {
				validity[i] = false
			}
		}

		return series.NewSeriesWithValidity("nth_value", values, validity, datatypes.Int32{}), nil

	case datatypes.Int64:
		values := make([]int64, size)
		validity := make([]bool, size)

		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			frameSize := end - start
			nthPos := f.n - 1

			if nthPos >= 0 && nthPos < frameSize {
				actualIdx := indices[start+nthPos]
				val := col.Get(actualIdx)
				if col.IsNull(actualIdx) {
					validity[i] = false
				} else {
					values[i] = val.(int64)
					validity[i] = true
				}
			} else {
				validity[i] = false
			}
		}

		return series.NewSeriesWithValidity("nth_value", values, validity, datatypes.Int64{}), nil

	case datatypes.Float64:
		values := make([]float64, size)
		validity := make([]bool, size)

		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			frameSize := end - start
			nthPos := f.n - 1

			if nthPos >= 0 && nthPos < frameSize {
				actualIdx := indices[start+nthPos]
				val := col.Get(actualIdx)
				if col.IsNull(actualIdx) {
					validity[i] = false
				} else {
					values[i] = val.(float64)
					validity[i] = true
				}
			} else {
				validity[i] = false
			}
		}

		return series.NewSeriesWithValidity("nth_value", values, validity, datatypes.Float64{}), nil

	case datatypes.String:
		values := make([]string, size)
		validity := make([]bool, size)

		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			frameSize := end - start
			nthPos := f.n - 1

			if nthPos >= 0 && nthPos < frameSize {
				actualIdx := indices[start+nthPos]
				val := col.Get(actualIdx)
				if col.IsNull(actualIdx) {
					validity[i] = false
				} else {
					values[i] = val.(string)
					validity[i] = true
				}
			} else {
				validity[i] = false
			}
		}

		return series.NewSeriesWithValidity("nth_value", values, validity, datatypes.String{}), nil
	}

	return nil, fmt.Errorf("unsupported data type for NTH_VALUE")
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
	return fmt.Sprintf("nth_value(%d)", f.n)
}

// Validate checks if the window specification is valid
func (f *nthValueFunc) Validate(spec *Spec) error {
	if f.n <= 0 {
		return fmt.Errorf("n must be positive, got %d", f.n)
	}
	return nil
}
