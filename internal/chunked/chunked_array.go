package chunked

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/tnn1t1s/golars/internal/datatypes"
)

// StatisticsFlags tracks various statistics about the data
type StatisticsFlags struct {
	sorted     bool
	sortedDesc bool
	hasMin     bool
	hasMax     bool
	minValue   interface{}
	maxValue   interface{}
}

// ChunkedArray is a generic columnar data structure that can hold multiple Arrow arrays
type ChunkedArray[T datatypes.ArrayValue] struct {
	field      arrow.Field
	chunks     []arrow.Array
	flags      StatisticsFlags
	length     int64
	nullCount  int64
	dataType   datatypes.DataType
	polarsType datatypes.PolarsDataType
	mu         sync.RWMutex
}

// NewChunkedArray creates a new ChunkedArray with the given name and data type
func NewChunkedArray[T datatypes.ArrayValue](name string, dt datatypes.DataType) *ChunkedArray[T] {
	polarsType := datatypes.GetPolarsType(dt)
	arrowField := arrow.Field{
		Name:     name,
		Type:     polarsType.ArrowType(),
		Nullable: true,
	}

	return &ChunkedArray[T]{
		field:      arrowField,
		chunks:     make([]arrow.Array, 0),
		dataType:   dt,
		polarsType: polarsType,
	}
}

// Name returns the name of the array
func (ca *ChunkedArray[T]) Name() string {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	return ca.field.Name
}

// DataType returns the Golars data type
func (ca *ChunkedArray[T]) DataType() datatypes.DataType {
	return ca.dataType
}

// Len returns the total length of all chunks
func (ca *ChunkedArray[T]) Len() int64 {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	return ca.length
}

// NullCount returns the total null count across all chunks
func (ca *ChunkedArray[T]) NullCount() int64 {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	return ca.nullCount
}

// NumChunks returns the number of chunks
func (ca *ChunkedArray[T]) NumChunks() int {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	return len(ca.chunks)
}

// Chunks returns a copy of the chunks slice
func (ca *ChunkedArray[T]) Chunks() []arrow.Array {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	result := make([]arrow.Array, len(ca.chunks))
	copy(result, ca.chunks)
	return result
}

// AppendArray adds a new Arrow array as a chunk
func (ca *ChunkedArray[T]) AppendArray(arr arrow.Array) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	// Validate data type compatibility
	if !arrow.TypeEqual(arr.DataType(), ca.polarsType.ArrowType()) {
		return fmt.Errorf("incompatible array type: expected %s, got %s",
			ca.polarsType.ArrowType(), arr.DataType())
	}

	// Add reference to keep array alive
	arr.Retain()

	ca.chunks = append(ca.chunks, arr)
	ca.length += int64(arr.Len())
	ca.nullCount += int64(arr.NullN())

	// Invalidate statistics when new data is added
	ca.flags = StatisticsFlags{}

	return nil
}

// AppendSlice appends a slice of values to the array
func (ca *ChunkedArray[T]) AppendSlice(values []T, validity []bool) error {
	mem := memory.NewGoAllocator()
	builder := ca.polarsType.NewBuilder(mem)
	defer builder.Release()

	// Type-specific append logic
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if v, ok := any(values).([]bool); ok {
			b.AppendValues(v, validity)
		}
	case *array.Int8Builder:
		if v, ok := any(values).([]int8); ok {
			b.AppendValues(v, validity)
		}
	case *array.Int16Builder:
		if v, ok := any(values).([]int16); ok {
			b.AppendValues(v, validity)
		}
	case *array.Int32Builder:
		if v, ok := any(values).([]int32); ok {
			b.AppendValues(v, validity)
		}
	case *array.Int64Builder:
		if v, ok := any(values).([]int64); ok {
			b.AppendValues(v, validity)
		}
	case *array.Uint8Builder:
		if v, ok := any(values).([]uint8); ok {
			b.AppendValues(v, validity)
		}
	case *array.Uint16Builder:
		if v, ok := any(values).([]uint16); ok {
			b.AppendValues(v, validity)
		}
	case *array.Uint32Builder:
		if v, ok := any(values).([]uint32); ok {
			b.AppendValues(v, validity)
		}
	case *array.Uint64Builder:
		if v, ok := any(values).([]uint64); ok {
			b.AppendValues(v, validity)
		}
	case *array.Float32Builder:
		if v, ok := any(values).([]float32); ok {
			b.AppendValues(v, validity)
		}
	case *array.Float64Builder:
		if v, ok := any(values).([]float64); ok {
			b.AppendValues(v, validity)
		}
	case *array.StringBuilder:
		if v, ok := any(values).([]string); ok {
			b.AppendValues(v, validity)
		}
	case *array.BinaryBuilder:
		if v, ok := any(values).([][]byte); ok {
			for i, val := range v {
				if validity == nil || validity[i] {
					b.Append(val)
				} else {
					b.AppendNull()
				}
			}
		}
	case *array.Date32Builder:
		if v, ok := any(values).([]int32); ok {
			for i, val := range v {
				if validity == nil || validity[i] {
					b.Append(arrow.Date32(val))
				} else {
					b.AppendNull()
				}
			}
		}
	case *array.Time64Builder:
		if v, ok := any(values).([]int64); ok {
			for i, val := range v {
				if validity == nil || validity[i] {
					b.Append(arrow.Time64(val))
				} else {
					b.AppendNull()
				}
			}
		}
	case *array.TimestampBuilder:
		if v, ok := any(values).([]int64); ok {
			for i, val := range v {
				if validity == nil || validity[i] {
					b.Append(arrow.Timestamp(val))
				} else {
					b.AppendNull()
				}
			}
		}
	case *array.DurationBuilder:
		if v, ok := any(values).([]int64); ok {
			for i, val := range v {
				if validity == nil || validity[i] {
					b.Append(arrow.Duration(val))
				} else {
					b.AppendNull()
				}
			}
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	arr := builder.NewArray()
	return ca.AppendArray(arr)
}

// Get returns the value at the given index
func (ca *ChunkedArray[T]) Get(i int64) (T, bool) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	var zero T
	if i < 0 || i >= ca.length {
		return zero, false
	}

	// Find which chunk contains this index
	offset := int64(0)
	for _, chunk := range ca.chunks {
		if i < offset+int64(chunk.Len()) {
			localIdx := int(i - offset)
			if chunk.IsNull(localIdx) {
				return zero, false
			}
			return ca.getValue(chunk, localIdx), true
		}
		offset += int64(chunk.Len())
	}

	return zero, false
}

// getValue extracts a value from a specific chunk at a local index
func (ca *ChunkedArray[T]) getValue(chunk arrow.Array, idx int) T {
	var zero T

	switch arr := chunk.(type) {
	case *array.Boolean:
		if _, ok := any(zero).(bool); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Int8:
		if _, ok := any(zero).(int8); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Int16:
		if _, ok := any(zero).(int16); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Int32:
		if _, ok := any(zero).(int32); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Int64:
		if _, ok := any(zero).(int64); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Uint8:
		if _, ok := any(zero).(uint8); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Uint16:
		if _, ok := any(zero).(uint16); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Uint32:
		if _, ok := any(zero).(uint32); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Uint64:
		if _, ok := any(zero).(uint64); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Float32:
		if _, ok := any(zero).(float32); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Float64:
		if _, ok := any(zero).(float64); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.String:
		if _, ok := any(zero).(string); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Binary:
		if _, ok := any(zero).([]byte); ok {
			return any(arr.Value(idx)).(T)
		}
	case *array.Date32:
		if _, ok := any(zero).(int32); ok {
			return any(int32(arr.Value(idx))).(T)
		}
	case *array.Time64:
		if _, ok := any(zero).(int64); ok {
			return any(int64(arr.Value(idx))).(T)
		}
	case *array.Timestamp:
		if _, ok := any(zero).(int64); ok {
			return any(int64(arr.Value(idx))).(T)
		}
	case *array.Duration:
		if _, ok := any(zero).(int64); ok {
			return any(int64(arr.Value(idx))).(T)
		}
	}

	return zero
}

// Slice returns a new ChunkedArray containing elements from start to end (exclusive)
func (ca *ChunkedArray[T]) Slice(start, end int64) (*ChunkedArray[T], error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	if start < 0 || end > ca.length || start > end {
		return nil, fmt.Errorf("invalid slice bounds: [%d:%d] for array of length %d", start, end, ca.length)
	}

	result := NewChunkedArray[T](ca.field.Name, ca.dataType)

	if start == end {
		return result, nil
	}

	// Find chunks that overlap with the slice range
	offset := int64(0)
	for _, chunk := range ca.chunks {
		chunkEnd := offset + int64(chunk.Len())

		// Skip chunks before the start
		if chunkEnd <= start {
			offset = chunkEnd
			continue
		}

		// Stop if we've passed the end
		if offset >= end {
			break
		}

		// Calculate slice bounds within this chunk
		localStart := max(int64(0), start-offset)
		localEnd := min(int64(chunk.Len()), end-offset)

		if localStart < localEnd {
			slicedChunk := array.NewSlice(chunk, localStart, localEnd)
			if err := result.AppendArray(slicedChunk); err != nil {
				slicedChunk.Release()
				return nil, err
			}
			slicedChunk.Release()
		}

		offset = chunkEnd
	}

	return result, nil
}

// ToSlice converts the ChunkedArray to a Go slice
func (ca *ChunkedArray[T]) ToSlice() ([]T, []bool) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	values := make([]T, ca.length)
	validity := make([]bool, ca.length)

	offset := 0
	for _, chunk := range ca.chunks {
		ca.copyChunkToSlice(chunk, values[offset:], validity[offset:])
		offset += chunk.Len()
	}

	return values, validity
}

// copyChunkToSlice copies values from a chunk to the output slice
func (ca *ChunkedArray[T]) copyChunkToSlice(chunk arrow.Array, values []T, validity []bool) {
	for i := 0; i < chunk.Len(); i++ {
		validity[i] = !chunk.IsNull(i)
		if validity[i] {
			values[i] = ca.getValue(chunk, i)
		}
	}
}

// IsValid returns whether the value at index i is valid (not null)
func (ca *ChunkedArray[T]) IsValid(i int64) bool {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	if i < 0 || i >= ca.length {
		return false
	}

	offset := int64(0)
	for _, chunk := range ca.chunks {
		if i < offset+int64(chunk.Len()) {
			return !chunk.IsNull(int(i - offset))
		}
		offset += int64(chunk.Len())
	}

	return false
}

// Release decrements the reference count of all chunks
func (ca *ChunkedArray[T]) Release() {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	for _, chunk := range ca.chunks {
		chunk.Release()
	}
	ca.chunks = nil
	ca.length = 0
	ca.nullCount = 0
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
