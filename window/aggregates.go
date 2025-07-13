package window

import (
	"fmt"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/series"
)

// sumFunc implements the SUM() window function
type sumFunc struct {
	column string
	spec   *Spec
}

// Sum creates a SUM() window function
func Sum(column string) WindowFunc {
	return WindowFunc{&sumFunc{column: column}}
}

// SetSpec sets the window specification
func (f *sumFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates the sum over the window frame
func (f *sumFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column to sum
	columnSeries, err := partition.Column(f.column)
	if err != nil {
		return nil, fmt.Errorf("column %s not found", f.column)
	}
	
	// Create result based on the column's data type
	dataType := columnSeries.DataType()
	
	// Get frame specification
	frame := f.spec.GetFrame()
	if frame == nil {
		// Default frame: UNBOUNDED PRECEDING to CURRENT ROW if ordered
		if partition.IsOrdered() {
			frame = &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: UnboundedPreceding},
				End:   FrameBound{Type: CurrentRow},
			}
		} else {
			// No ordering - sum over entire partition
			frame = &FrameSpec{
				Type:  RowsFrame,
				Start: FrameBound{Type: UnboundedPreceding},
				End:   FrameBound{Type: UnboundedFollowing},
			}
		}
	}
	
	switch dataType.String() {
	case "i32":
		return f.computeInt32Sum(partition, columnSeries, frame)
	case "i64":
		return f.computeInt64Sum(partition, columnSeries, frame)
	case "f64":
		return f.computeFloat64Sum(partition, columnSeries, frame)
	default:
		return nil, fmt.Errorf("unsupported data type for SUM: %s", dataType)
	}
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
		sum := int32(0)
		for j := start; j < end; j++ {
			idx := indices[j]
			sum += columnSeries.Get(idx).(int32)
		}
		
		result[i] = sum
	}
	
	return series.NewInt32Series("sum", result), nil
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
		sum := int64(0)
		for j := start; j < end; j++ {
			idx := indices[j]
			sum += columnSeries.Get(idx).(int64)
		}
		
		result[i] = sum
	}
	
	return series.NewInt64Series("sum", result), nil
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
		sum := 0.0
		for j := start; j < end; j++ {
			idx := indices[j]
			sum += columnSeries.Get(idx).(float64)
		}
		
		result[i] = sum
	}
	
	return series.NewFloat64Series("sum", result), nil
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
	return WindowFunc{&avgFunc{column: column}}
}

// SetSpec sets the window specification
func (f *avgFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates the average over the window frame
func (f *avgFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()
	
	// Get the column to average
	columnSeries, err := partition.Column(f.column)
	if err != nil {
		return nil, fmt.Errorf("column %s not found", f.column)
	}
	
	// Get frame specification
	frame := f.spec.GetFrame()
	if frame == nil {
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
	
	// Always return float64 for averages
	result := make([]float64, size)
	indices := partition.Indices()
	
	// For each row in the partition
	for i := 0; i < size; i++ {
		// Calculate frame bounds
		start, end := partition.FrameBounds(i, frame)
		
		// Calculate average
		sum := 0.0
		count := 0
		for j := start; j < end; j++ {
			idx := indices[j]
			// Convert to float64 for averaging
			switch v := columnSeries.Get(idx).(type) {
			case int32:
				sum += float64(v)
			case int64:
				sum += float64(v)
			case float64:
				sum += v
			}
			count++
		}
		
		if count > 0 {
			result[i] = sum / float64(count)
		} else {
			result[i] = 0.0
		}
	}
	
	return series.NewFloat64Series("avg", result), nil
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
	return WindowFunc{&minFunc{column: column}}
}

// SetSpec sets the window specification
func (f *minFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates the minimum over the window frame
func (f *minFunc) Compute(partition Partition) (series.Series, error) {
	// Get the column
	columnSeries, err := partition.Column(f.column)
	if err != nil {
		return nil, fmt.Errorf("column %s not found", f.column)
	}
	
	// Create result based on the column's data type
	dataType := columnSeries.DataType()
	
	// Get frame specification
	frame := f.spec.GetFrame()
	if frame == nil {
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
	
	switch dataType.String() {
	case "i32":
		return f.computeInt32Min(partition, columnSeries, frame)
	case "i64":
		return f.computeInt64Min(partition, columnSeries, frame)
	case "f64":
		return f.computeFloat64Min(partition, columnSeries, frame)
	default:
		return nil, fmt.Errorf("unsupported data type for MIN: %s", dataType)
	}
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
		
		// Find minimum in the frame
		if start < end {
			min := columnSeries.Get(indices[start]).(int32)
			for j := start + 1; j < end; j++ {
				idx := indices[j]
				val := columnSeries.Get(idx).(int32)
				if val < min {
					min = val
				}
			}
			result[i] = min
		} else {
			result[i] = 0 // Default for empty frame
		}
	}
	
	return series.NewInt32Series("min", result), nil
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
		
		// Find minimum in the frame
		if start < end {
			min := columnSeries.Get(indices[start]).(int64)
			for j := start + 1; j < end; j++ {
				idx := indices[j]
				val := columnSeries.Get(idx).(int64)
				if val < min {
					min = val
				}
			}
			result[i] = min
		} else {
			result[i] = 0 // Default for empty frame
		}
	}
	
	return series.NewInt64Series("min", result), nil
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
		
		// Find minimum in the frame
		if start < end {
			min := columnSeries.Get(indices[start]).(float64)
			for j := start + 1; j < end; j++ {
				idx := indices[j]
				val := columnSeries.Get(idx).(float64)
				if val < min {
					min = val
				}
			}
			result[i] = min
		} else {
			result[i] = 0.0 // Default for empty frame
		}
	}
	
	return series.NewFloat64Series("min", result), nil
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
	return WindowFunc{&maxFunc{column: column}}
}

// SetSpec sets the window specification
func (f *maxFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates the maximum over the window frame
func (f *maxFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()
	
	// Get the column
	columnSeries, err := partition.Column(f.column)
	if err != nil {
		return nil, fmt.Errorf("column %s not found", f.column)
	}
	
	// Create result based on the column's data type
	dataType := columnSeries.DataType()
	
	// Get frame specification
	frame := f.spec.GetFrame()
	if frame == nil {
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
	
	switch dataType.String() {
	case "i32":
		result := make([]int32, size)
		indices := partition.Indices()
		
		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			
			if start < end {
				max := columnSeries.Get(indices[start]).(int32)
				for j := start + 1; j < end; j++ {
					idx := indices[j]
					val := columnSeries.Get(idx).(int32)
					if val > max {
						max = val
					}
				}
				result[i] = max
			} else {
				result[i] = 0
			}
		}
		return series.NewInt32Series("max", result), nil
		
	case "i64":
		result := make([]int64, size)
		indices := partition.Indices()
		
		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			
			if start < end {
				max := columnSeries.Get(indices[start]).(int64)
				for j := start + 1; j < end; j++ {
					idx := indices[j]
					val := columnSeries.Get(idx).(int64)
					if val > max {
						max = val
					}
				}
				result[i] = max
			} else {
				result[i] = 0
			}
		}
		return series.NewInt64Series("max", result), nil
		
	case "f64":
		result := make([]float64, size)
		indices := partition.Indices()
		
		for i := 0; i < size; i++ {
			start, end := partition.FrameBounds(i, frame)
			
			if start < end {
				max := columnSeries.Get(indices[start]).(float64)
				for j := start + 1; j < end; j++ {
					idx := indices[j]
					val := columnSeries.Get(idx).(float64)
					if val > max {
						max = val
					}
				}
				result[i] = max
			} else {
				result[i] = 0.0
			}
		}
		return series.NewFloat64Series("max", result), nil
		
	default:
		return nil, fmt.Errorf("unsupported data type for MAX: %s", dataType)
	}
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
	return WindowFunc{&countFunc{column: column}}
}

// SetSpec sets the window specification
func (f *countFunc) SetSpec(spec *Spec) {
	f.spec = spec
}

// Compute calculates the count over the window frame
func (f *countFunc) Compute(partition Partition) (series.Series, error) {
	size := partition.Size()
	
	// Get the column to count (for null checking)
	columnSeries, err := partition.Column(f.column)
	if err != nil {
		return nil, fmt.Errorf("column %s not found", f.column)
	}
	
	// Get frame specification
	frame := f.spec.GetFrame()
	if frame == nil {
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
	
	result := make([]int64, size)
	indices := partition.Indices()
	
	// For each row in the partition
	for i := 0; i < size; i++ {
		// Calculate frame bounds
		start, end := partition.FrameBounds(i, frame)
		
		// Count non-null values in the frame
		count := int64(0)
		for j := start; j < end; j++ {
			idx := indices[j]
			// In the future, we'd check for nulls here
			// For now, count all values
			_ = columnSeries.Get(idx)
			count++
		}
		
		result[i] = count
	}
	
	return series.NewInt64Series("count", result), nil
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