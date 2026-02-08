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
	pt := datatypes.GetPolarsType(dt)
	return &ChunkedArray[T]{
		field:      arrow.Field{Name: name, Type: pt.ArrowType()},
		chunks:     nil,
		dataType:   dt,
		polarsType: pt,
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
	ca.mu.RLock()
	defer ca.mu.RUnlock()
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
	out := make([]arrow.Array, len(ca.chunks))
	copy(out, ca.chunks)
	return out
}

// AppendArray adds a new Arrow array as a chunk
func (ca *ChunkedArray[T]) AppendArray(arr arrow.Array) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

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

	n := len(values)

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(bool))
			}
		}
	case *array.Int8Builder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(int8))
			}
		}
	case *array.Int16Builder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(int16))
			}
		}
	case *array.Int32Builder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(int32))
			}
		}
	case *array.Int64Builder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(int64))
			}
		}
	case *array.Uint8Builder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(uint8))
			}
		}
	case *array.Uint16Builder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(uint16))
			}
		}
	case *array.Uint32Builder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(uint32))
			}
		}
	case *array.Uint64Builder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(uint64))
			}
		}
	case *array.Float32Builder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(float32))
			}
		}
	case *array.Float64Builder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(float64))
			}
		}
	case *array.StringBuilder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).(string))
			}
		}
	case *array.BinaryBuilder:
		for i := 0; i < n; i++ {
			if validity != nil && !validity[i] {
				b.AppendNull()
			} else {
				b.Append(any(values[i]).([]byte))
			}
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	arr := builder.NewArray()
	// AppendArray will Retain, so we defer Release to balance NewArray's implicit retain
	defer arr.Release()

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
		chunkLen := int64(chunk.Len())
		if i < offset+chunkLen {
			localIdx := int(i - offset)
			if chunk.IsNull(localIdx) {
				return zero, false
			}
			return ca.getValue(chunk, localIdx), true
		}
		offset += chunkLen
	}

	return zero, false
}

// getValue extracts a value from a specific chunk at a local index
func (ca *ChunkedArray[T]) getValue(chunk arrow.Array, idx int) T {
	switch arr := chunk.(type) {
	case *array.Boolean:
		return any(arr.Value(idx)).(T)
	case *array.Int8:
		return any(arr.Value(idx)).(T)
	case *array.Int16:
		return any(arr.Value(idx)).(T)
	case *array.Int32:
		return any(arr.Value(idx)).(T)
	case *array.Int64:
		return any(arr.Value(idx)).(T)
	case *array.Uint8:
		return any(arr.Value(idx)).(T)
	case *array.Uint16:
		return any(arr.Value(idx)).(T)
	case *array.Uint32:
		return any(arr.Value(idx)).(T)
	case *array.Uint64:
		return any(arr.Value(idx)).(T)
	case *array.Float32:
		return any(arr.Value(idx)).(T)
	case *array.Float64:
		return any(arr.Value(idx)).(T)
	case *array.String:
		return any(arr.Value(idx)).(T)
	case *array.Binary:
		return any(arr.Value(idx)).(T)
	default:
		var zero T
		return zero
	}
}

// Slice returns a new ChunkedArray containing elements from start to end (exclusive)
func (ca *ChunkedArray[T]) Slice(start, end int64) (*ChunkedArray[T], error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	if start < 0 || end > ca.length || start > end {
		return nil, fmt.Errorf("invalid slice bounds [%d:%d] for length %d", start, end, ca.length)
	}

	result := NewChunkedArray[T](ca.field.Name, ca.dataType)

	if start == end {
		return result, nil
	}

	// Walk chunks to find the overlap with [start, end)
	offset := int64(0)
	for _, chunk := range ca.chunks {
		chunkLen := int64(chunk.Len())
		chunkStart := offset
		chunkEnd := offset + chunkLen

		// Skip chunks before the start
		if chunkEnd <= start {
			offset += chunkLen
			continue
		}
		// Stop if we've passed the end
		if chunkStart >= end {
			break
		}

		// Calculate slice bounds within this chunk
		lo := max(start-chunkStart, 0)
		hi := min(end-chunkStart, chunkLen)

		sliced := array.NewSlice(chunk, lo, hi)
		sliced.Retain()
		result.chunks = append(result.chunks, sliced)
		result.length += int64(sliced.Len())
		result.nullCount += int64(sliced.NullN())
		sliced.Release() // balance the extra Retain; result.chunks still holds a ref

		offset += chunkLen
	}

	return result, nil
}

// ToSlice converts the ChunkedArray to a Go slice
func (ca *ChunkedArray[T]) ToSlice() ([]T, []bool) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	values := make([]T, 0, ca.length)
	validity := make([]bool, 0, ca.length)

	for _, chunk := range ca.chunks {
		chunkLen := chunk.Len()
		vTmp := make([]T, chunkLen)
		bTmp := make([]bool, chunkLen)
		ca.copyChunkToSlice(chunk, vTmp, bTmp)
		values = append(values, vTmp...)
		validity = append(validity, bTmp...)
	}

	return values, validity
}

// copyChunkToSlice copies values from a chunk to the output slice
func (ca *ChunkedArray[T]) copyChunkToSlice(chunk arrow.Array, values []T, validity []bool) {
	n := chunk.Len()
	for i := 0; i < n; i++ {
		validity[i] = chunk.IsValid(i)
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
		chunkLen := int64(chunk.Len())
		if i < offset+chunkLen {
			return chunk.IsValid(int(i - offset))
		}
		offset += chunkLen
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
