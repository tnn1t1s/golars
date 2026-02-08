package window

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// sumFunc implements the SUM() window function
type sumFunc struct {
	column string
	spec   *Spec
}

// Sum creates a SUM() window function
func Sum(column string) WindowFunc {
	return WrapFunction(&sumFunc{column: column})
}

// SetSpec sets the window specification
func (f *sumFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates the sum over the window frame
func (f *sumFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column to sum
	col, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	// Get frame specification
	frame := f.getFrame()

	// Create result based on the column's data type
	switch col.DataType().(type) {
	case datatypes.Int32:
		return f.computeInt32Sum(partition, col, frame)
	case datatypes.Int64:
		return f.computeInt64Sum(partition, col, frame)
	case datatypes.Float64:
		return f.computeFloat64Sum(partition, col, frame)
	default:
		return nil, fmt.Errorf("unsupported data type for SUM: %v", col.DataType())
	}
}

func (f *sumFunc) getFrame() *FrameSpec {
	if f.spec != nil && f.spec.GetFrame() != nil {
		return f.spec.GetFrame()
	}
	if f.spec != nil && f.spec.HasOrderBy() {
		// Default frame: RANGE UNBOUNDED PRECEDING TO CURRENT ROW
		// But for computation, we use rows-based with same semantics
		return &FrameSpec{
			Type:  RowsFrame,
			Start: FrameBound{Type: UnboundedPreceding},
			End:   FrameBound{Type: CurrentRow},
		}
	}
	// No ordering - sum over entire partition
	return nil
}

// computeInt32Sum computes sum for int32 series
func (f *sumFunc) computeInt32Sum(partition Partition, columnSeries series.Series, frame *FrameSpec) (series.Series, error) {
	size := partition.Size()
	result := make([]int32, size)
	indices := partition.Indices()

	// For each row in the partition
	for i := 0; i < size; i++ {
		// Calculate frame bounds
		start, end := partition.FrameBounds(i, frame)

		// Sum values in the frame
		var sum int32
		for j := start; j < end; j++ {
			idx := indices[j]
			sum += columnSeries.Get(idx).(int32)
		}
		result[indices[i]] = sum
	}

	return series.NewInt32Series(f.column, result), nil
}

// computeInt64Sum computes sum for int64 series
func (f *sumFunc) computeInt64Sum(partition Partition, columnSeries series.Series, frame *FrameSpec) (series.Series, error) {
	size := partition.Size()
	result := make([]int64, size)
	indices := partition.Indices()

	// For each row in the partition
	for i := 0; i < size; i++ {
		// Calculate frame bounds
		start, end := partition.FrameBounds(i, frame)

		// Sum values in the frame
		var sum int64
		for j := start; j < end; j++ {
			idx := indices[j]
			sum += columnSeries.Get(idx).(int64)
		}
		result[indices[i]] = sum
	}

	return series.NewInt64Series(f.column, result), nil
}

// computeFloat64Sum computes sum for float64 series
func (f *sumFunc) computeFloat64Sum(partition Partition, columnSeries series.Series, frame *FrameSpec) (series.Series, error) {
	size := partition.Size()
	result := make([]float64, size)
	indices := partition.Indices()

	// For each row in the partition
	for i := 0; i < size; i++ {
		// Calculate frame bounds
		start, end := partition.FrameBounds(i, frame)

		// Sum values in the frame
		var sum float64
		for j := start; j < end; j++ {
			idx := indices[j]
			sum += columnSeries.Get(idx).(float64)
		}
		result[indices[i]] = sum
	}

	return series.NewFloat64Series(f.column, result), nil
}

// DataType returns the same type as the input column
func (f *sumFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}

// Name returns the function name
func (f *sumFunc) Name() string {
	return "sum"
}

// Validate checks if the window specification is valid
func (f *sumFunc) Validate(spec *Spec) error {
	return nil
}

// avgFunc implements the AVG() window function
type avgFunc struct {
	column string
	spec   *Spec
}

// Avg creates an AVG() window function
func Avg(column string) WindowFunc {
	return WrapFunction(&avgFunc{column: column})
}

// SetSpec sets the window specification
func (f *avgFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates the average over the window frame
func (f *avgFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column to average
	col, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	size := partition.Size()

	// Get frame specification
	frame := f.getFrame()

	// Always return float64 for averages
	result := make([]float64, size)
	indices := partition.Indices()

	// For each row in the partition
	for i := 0; i < size; i++ {
		// Calculate frame bounds
		start, end := partition.FrameBounds(i, frame)
		count := end - start

		if count == 0 {
			result[indices[i]] = 0
			continue
		}

		// Calculate average
		var sum float64
		for j := start; j < end; j++ {
			idx := indices[j]
			// Convert to float64 for averaging
			val := col.Get(idx)
			switch v := val.(type) {
			case int32:
				sum += float64(v)
			case int64:
				sum += float64(v)
			case float64:
				sum += v
			}
		}
		result[indices[i]] = sum / float64(count)
	}

	return series.NewFloat64Series(f.column, result), nil
}

func (f *avgFunc) getFrame() *FrameSpec {
	if f.spec != nil && f.spec.GetFrame() != nil {
		return f.spec.GetFrame()
	}
	if f.spec != nil && f.spec.HasOrderBy() {
		// Default frame
		return &FrameSpec{
			Type:  RowsFrame,
			Start: FrameBound{Type: UnboundedPreceding},
			End:   FrameBound{Type: CurrentRow},
		}
	}
	return nil
}

// DataType always returns Float64 for averages
func (f *avgFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return datatypes.Float64{}
}

// Name returns the function name
func (f *avgFunc) Name() string {
	return "avg"
}

// Validate checks if the window specification is valid
func (f *avgFunc) Validate(spec *Spec) error {
	return nil
}

// minFunc implements the MIN() window function
type minFunc struct {
	column string
	spec   *Spec
}

// Min creates a MIN() window function
func Min(column string) WindowFunc {
	return WrapFunction(&minFunc{column: column})
}

// SetSpec sets the window specification
func (f *minFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates the minimum over the window frame
func (f *minFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column
	col, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	// Get frame specification
	frame := f.getFrame()

	// Create result based on the column's data type
	switch col.DataType().(type) {
	case datatypes.Int32:
		return f.computeInt32Min(partition, col, frame)
	case datatypes.Int64:
		return f.computeInt64Min(partition, col, frame)
	case datatypes.Float64:
		return f.computeFloat64Min(partition, col, frame)
	default:
		return nil, fmt.Errorf("unsupported data type for MIN: %v", col.DataType())
	}
}

func (f *minFunc) getFrame() *FrameSpec {
	if f.spec != nil && f.spec.GetFrame() != nil {
		return f.spec.GetFrame()
	}
	if f.spec != nil && f.spec.HasOrderBy() {
		return &FrameSpec{
			Type:  RowsFrame,
			Start: FrameBound{Type: UnboundedPreceding},
			End:   FrameBound{Type: CurrentRow},
		}
	}
	return nil
}

// computeInt32Min computes minimum for int32 series
func (f *minFunc) computeInt32Min(partition Partition, columnSeries series.Series, frame *FrameSpec) (series.Series, error) {
	size := partition.Size()
	result := make([]int32, size)
	indices := partition.Indices()

	// For each row in the partition
	for i := 0; i < size; i++ {
		// Calculate frame bounds
		start, end := partition.FrameBounds(i, frame)

		if start >= end {
			// Default for empty frame
			result[indices[i]] = 0
			continue
		}

		// Find minimum in the frame
		minVal := columnSeries.Get(indices[start]).(int32)
		for j := start + 1; j < end; j++ {
			v := columnSeries.Get(indices[j]).(int32)
			if v < minVal {
				minVal = v
			}
		}
		result[indices[i]] = minVal
	}

	return series.NewInt32Series(f.column, result), nil
}

// computeInt64Min computes minimum for int64 series
func (f *minFunc) computeInt64Min(partition Partition, columnSeries series.Series, frame *FrameSpec) (series.Series, error) {
	size := partition.Size()
	result := make([]int64, size)
	indices := partition.Indices()

	// For each row in the partition
	for i := 0; i < size; i++ {
		// Calculate frame bounds
		start, end := partition.FrameBounds(i, frame)

		if start >= end {
			// Default for empty frame
			result[indices[i]] = 0
			continue
		}

		// Find minimum in the frame
		minVal := columnSeries.Get(indices[start]).(int64)
		for j := start + 1; j < end; j++ {
			v := columnSeries.Get(indices[j]).(int64)
			if v < minVal {
				minVal = v
			}
		}
		result[indices[i]] = minVal
	}

	return series.NewInt64Series(f.column, result), nil
}

// computeFloat64Min computes minimum for float64 series
func (f *minFunc) computeFloat64Min(partition Partition, columnSeries series.Series, frame *FrameSpec) (series.Series, error) {
	size := partition.Size()
	result := make([]float64, size)
	indices := partition.Indices()

	// For each row in the partition
	for i := 0; i < size; i++ {
		// Calculate frame bounds
		start, end := partition.FrameBounds(i, frame)

		if start >= end {
			// Default for empty frame
			result[indices[i]] = 0
			continue
		}

		// Find minimum in the frame
		minVal := columnSeries.Get(indices[start]).(float64)
		for j := start + 1; j < end; j++ {
			v := columnSeries.Get(indices[j]).(float64)
			if v < minVal {
				minVal = v
			}
		}
		result[indices[i]] = minVal
	}

	return series.NewFloat64Series(f.column, result), nil
}

// DataType returns the same type as the input column
func (f *minFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}

// Name returns the function name
func (f *minFunc) Name() string {
	return "min"
}

// Validate checks if the window specification is valid
func (f *minFunc) Validate(spec *Spec) error {
	return nil
}

// maxFunc implements the MAX() window function
type maxFunc struct {
	column string
	spec   *Spec
}

// Max creates a MAX() window function
func Max(column string) WindowFunc {
	return WrapFunction(&maxFunc{column: column})
}

// SetSpec sets the window specification
func (f *maxFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates the maximum over the window frame
func (f *maxFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column
	col, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	// Get frame specification
	frame := f.getFrame()

	// Create result based on the column's data type
	switch col.DataType().(type) {
	case datatypes.Int32:
		return f.computeInt32Max(partition, col, frame)
	case datatypes.Int64:
		return f.computeInt64Max(partition, col, frame)
	case datatypes.Float64:
		return f.computeFloat64Max(partition, col, frame)
	default:
		return nil, fmt.Errorf("unsupported data type for MAX: %v", col.DataType())
	}
}

func (f *maxFunc) getFrame() *FrameSpec {
	if f.spec != nil && f.spec.GetFrame() != nil {
		return f.spec.GetFrame()
	}
	if f.spec != nil && f.spec.HasOrderBy() {
		return &FrameSpec{
			Type:  RowsFrame,
			Start: FrameBound{Type: UnboundedPreceding},
			End:   FrameBound{Type: CurrentRow},
		}
	}
	return nil
}

func (f *maxFunc) computeInt32Max(partition Partition, columnSeries series.Series, frame *FrameSpec) (series.Series, error) {
	size := partition.Size()
	result := make([]int32, size)
	indices := partition.Indices()

	for i := 0; i < size; i++ {
		start, end := partition.FrameBounds(i, frame)

		if start >= end {
			result[indices[i]] = 0
			continue
		}

		maxVal := columnSeries.Get(indices[start]).(int32)
		for j := start + 1; j < end; j++ {
			v := columnSeries.Get(indices[j]).(int32)
			if v > maxVal {
				maxVal = v
			}
		}
		result[indices[i]] = maxVal
	}

	return series.NewInt32Series(f.column, result), nil
}

func (f *maxFunc) computeInt64Max(partition Partition, columnSeries series.Series, frame *FrameSpec) (series.Series, error) {
	size := partition.Size()
	result := make([]int64, size)
	indices := partition.Indices()

	for i := 0; i < size; i++ {
		start, end := partition.FrameBounds(i, frame)

		if start >= end {
			result[indices[i]] = 0
			continue
		}

		maxVal := columnSeries.Get(indices[start]).(int64)
		for j := start + 1; j < end; j++ {
			v := columnSeries.Get(indices[j]).(int64)
			if v > maxVal {
				maxVal = v
			}
		}
		result[indices[i]] = maxVal
	}

	return series.NewInt64Series(f.column, result), nil
}

func (f *maxFunc) computeFloat64Max(partition Partition, columnSeries series.Series, frame *FrameSpec) (series.Series, error) {
	size := partition.Size()
	result := make([]float64, size)
	indices := partition.Indices()

	for i := 0; i < size; i++ {
		start, end := partition.FrameBounds(i, frame)

		if start >= end {
			result[indices[i]] = 0
			continue
		}

		maxVal := columnSeries.Get(indices[start]).(float64)
		for j := start + 1; j < end; j++ {
			v := columnSeries.Get(indices[j]).(float64)
			if v > maxVal {
				maxVal = v
			}
		}
		result[indices[i]] = maxVal
	}

	return series.NewFloat64Series(f.column, result), nil
}

// DataType returns the same type as the input column
func (f *maxFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return inputType
}

// Name returns the function name
func (f *maxFunc) Name() string {
	return "max"
}

// Validate checks if the window specification is valid
func (f *maxFunc) Validate(spec *Spec) error {
	return nil
}

// countFunc implements the COUNT() window function
type countFunc struct {
	column string
	spec   *Spec
}

// Count creates a COUNT() window function
func Count(column string) WindowFunc {
	return WrapFunction(&countFunc{column: column})
}

// SetSpec sets the window specification
func (f *countFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates the count over the window frame
func (f *countFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column to count (for null checking)
	_, err := partition.Column(f.column)
	if err != nil {
		return nil, err
	}

	size := partition.Size()

	// Get frame specification
	frame := f.getFrame()

	result := make([]int64, size)
	indices := partition.Indices()

	// For each row in the partition
	for i := 0; i < size; i++ {
		// Calculate frame bounds
		start, end := partition.FrameBounds(i, frame)

		// Count non-null values in the frame
		// In the future, we'd check for nulls here
		// For now, count all values
		count := int64(end - start)
		result[indices[i]] = count
	}

	return series.NewInt64Series(f.column, result), nil
}

func (f *countFunc) getFrame() *FrameSpec {
	if f.spec != nil && f.spec.GetFrame() != nil {
		return f.spec.GetFrame()
	}
	if f.spec != nil && f.spec.HasOrderBy() {
		return &FrameSpec{
			Type:  RowsFrame,
			Start: FrameBound{Type: UnboundedPreceding},
			End:   FrameBound{Type: CurrentRow},
		}
	}
	return nil
}

// DataType always returns Int64 for counts
func (f *countFunc) DataType(inputType datatypes.DataType) datatypes.DataType {
	return datatypes.Int64{}
}

// Name returns the function name
func (f *countFunc) Name() string {
	return "count"
}

// Validate checks if the window specification is valid
func (f *countFunc) Validate(spec *Spec) error {
	return nil
}
