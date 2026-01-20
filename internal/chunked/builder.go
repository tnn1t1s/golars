package chunked

import (
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/tnn1t1s/golars/internal/datatypes"
)

// ChunkedBuilder builds a ChunkedArray incrementally
type ChunkedBuilder[T datatypes.ArrayValue] struct {
	builders []array.Builder
	dataType datatypes.DataType
}

// NewChunkedBuilder creates a new ChunkedBuilder
func NewChunkedBuilder[T datatypes.ArrayValue](dt datatypes.DataType) *ChunkedBuilder[T] {
	return &ChunkedBuilder[T]{
		builders: []array.Builder{},
		dataType: dt,
	}
}

// Append appends a value to the builder
func (cb *ChunkedBuilder[T]) Append(value T) {
	// Get current builder or create new one
	builder := cb.getCurrentBuilder()

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		b.Append(any(value).(bool))
	case *array.Int8Builder:
		b.Append(any(value).(int8))
	case *array.Int16Builder:
		b.Append(any(value).(int16))
	case *array.Int32Builder:
		b.Append(any(value).(int32))
	case *array.Int64Builder:
		b.Append(any(value).(int64))
	case *array.Uint8Builder:
		b.Append(any(value).(uint8))
	case *array.Uint16Builder:
		b.Append(any(value).(uint16))
	case *array.Uint32Builder:
		b.Append(any(value).(uint32))
	case *array.Uint64Builder:
		b.Append(any(value).(uint64))
	case *array.Float32Builder:
		b.Append(any(value).(float32))
	case *array.Float64Builder:
		b.Append(any(value).(float64))
	case *array.StringBuilder:
		b.Append(any(value).(string))
	case *array.BinaryBuilder:
		b.Append(any(value).([]byte))
	}
}

// AppendNull appends a null value to the builder
func (cb *ChunkedBuilder[T]) AppendNull() {
	builder := cb.getCurrentBuilder()
	builder.AppendNull()
}

// getCurrentBuilder gets the current builder or creates a new one
func (cb *ChunkedBuilder[T]) getCurrentBuilder() array.Builder {
	// For simplicity, we'll use a single builder
	// In a real implementation, you might want to chunk at certain sizes
	if len(cb.builders) == 0 {
		builder := cb.createBuilder()
		cb.builders = append(cb.builders, builder)
	}
	return cb.builders[len(cb.builders)-1]
}

// createBuilder creates a new Arrow builder based on the data type
func (cb *ChunkedBuilder[T]) createBuilder() array.Builder {
	allocator := memory.DefaultAllocator

	switch cb.dataType.(type) {
	case datatypes.Boolean:
		return array.NewBooleanBuilder(allocator)
	case datatypes.Int8:
		return array.NewInt8Builder(allocator)
	case datatypes.Int16:
		return array.NewInt16Builder(allocator)
	case datatypes.Int32:
		return array.NewInt32Builder(allocator)
	case datatypes.Int64:
		return array.NewInt64Builder(allocator)
	case datatypes.UInt8:
		return array.NewUint8Builder(allocator)
	case datatypes.UInt16:
		return array.NewUint16Builder(allocator)
	case datatypes.UInt32:
		return array.NewUint32Builder(allocator)
	case datatypes.UInt64:
		return array.NewUint64Builder(allocator)
	case datatypes.Float32:
		return array.NewFloat32Builder(allocator)
	case datatypes.Float64:
		return array.NewFloat64Builder(allocator)
	case datatypes.String:
		return array.NewStringBuilder(allocator)
	case datatypes.Binary:
		return array.NewBinaryBuilder(allocator, arrow.BinaryTypes.Binary)
	default:
		// Fallback to string builder
		return array.NewStringBuilder(allocator)
	}
}

// Finish builds the final ChunkedArray
func (cb *ChunkedBuilder[T]) Finish() *ChunkedArray[T] {
	chunks := make([]arrow.Array, len(cb.builders))

	for i, builder := range cb.builders {
		chunks[i] = builder.NewArray()
		builder.Release()
	}

	// Clear builders
	cb.builders = []array.Builder{}

	// Get Arrow type from first chunk if available
	var arrowType arrow.DataType
	if len(chunks) > 0 {
		arrowType = chunks[0].DataType()
	}

	return &ChunkedArray[T]{
		field:     arrow.Field{Name: "", Type: arrowType},
		chunks:    chunks,
		length:    calculateTotalLength(chunks),
		nullCount: calculateTotalNullCount(chunks),
		dataType:  cb.dataType,
	}
}

// calculateTotalLength calculates the total length of all chunks
func calculateTotalLength(chunks []arrow.Array) int64 {
	var total int64
	for _, chunk := range chunks {
		total += int64(chunk.Len())
	}
	return total
}

// calculateTotalNullCount calculates the total null count of all chunks
func calculateTotalNullCount(chunks []arrow.Array) int64 {
	var total int64
	for _, chunk := range chunks {
		total += int64(chunk.NullN())
	}
	return total
}
