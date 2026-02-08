package chunked

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

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
		dataType: dt,
	}
}

// Append appends a value to the builder
func (cb *ChunkedBuilder[T]) Append(value T) {
	b := cb.getCurrentBuilder()

	switch bb := b.(type) {
	case *array.BooleanBuilder:
		bb.Append(any(value).(bool))
	case *array.Int8Builder:
		bb.Append(any(value).(int8))
	case *array.Int16Builder:
		bb.Append(any(value).(int16))
	case *array.Int32Builder:
		bb.Append(any(value).(int32))
	case *array.Int64Builder:
		bb.Append(any(value).(int64))
	case *array.Uint8Builder:
		bb.Append(any(value).(uint8))
	case *array.Uint16Builder:
		bb.Append(any(value).(uint16))
	case *array.Uint32Builder:
		bb.Append(any(value).(uint32))
	case *array.Uint64Builder:
		bb.Append(any(value).(uint64))
	case *array.Float32Builder:
		bb.Append(any(value).(float32))
	case *array.Float64Builder:
		bb.Append(any(value).(float64))
	case *array.StringBuilder:
		bb.Append(any(value).(string))
	case *array.BinaryBuilder:
		bb.Append(any(value).([]byte))
	case *array.TimestampBuilder:
		bb.Append(arrow.Timestamp(any(value).(int64)))
	case *array.Date32Builder:
		bb.Append(arrow.Date32(any(value).(int32)))
	case *array.Time64Builder:
		bb.Append(arrow.Time64(any(value).(int64)))
	case *array.DurationBuilder:
		bb.Append(arrow.Duration(any(value).(int64)))
	}
}

// AppendNull appends a null value to the builder
func (cb *ChunkedBuilder[T]) AppendNull() {
	b := cb.getCurrentBuilder()
	b.AppendNull()
}

// getCurrentBuilder gets the current builder or creates a new one
func (cb *ChunkedBuilder[T]) getCurrentBuilder() array.Builder {
	if len(cb.builders) == 0 {
		cb.builders = append(cb.builders, cb.createBuilder())
	}
	return cb.builders[len(cb.builders)-1]
}

// createBuilder creates a new Arrow builder based on the data type
func (cb *ChunkedBuilder[T]) createBuilder() array.Builder {
	pt := datatypes.GetPolarsType(cb.dataType)
	return pt.NewBuilder(memory.DefaultAllocator)
}

// Finish builds the final ChunkedArray
func (cb *ChunkedBuilder[T]) Finish() *ChunkedArray[T] {
	pt := datatypes.GetPolarsType(cb.dataType)
	ca := &ChunkedArray[T]{
		field:      arrow.Field{Name: "", Type: pt.ArrowType()},
		dataType:   cb.dataType,
		polarsType: pt,
	}

	for _, b := range cb.builders {
		arr := b.NewArray()
		ca.chunks = append(ca.chunks, arr)
		ca.length += int64(arr.Len())
		ca.nullCount += int64(arr.NullN())
	}

	// Clear builders
	cb.builders = nil

	return ca
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
