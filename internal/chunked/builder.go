package chunked

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	_ "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/tnn1t1s/golars/internal/datatypes"
)

// ChunkedBuilder builds a ChunkedArray incrementally
type ChunkedBuilder[T datatypes.ArrayValue] struct {
	builders []array.Builder
	dataType datatypes.DataType
}

// NewChunkedBuilder creates a new ChunkedBuilder
func NewChunkedBuilder[T datatypes.ArrayValue](dt datatypes.DataType) *ChunkedBuilder[T] {
	panic("not implemented")

}

// Append appends a value to the builder
func (cb *ChunkedBuilder[T]) Append(value T) {
	panic(
		// Get current builder or create new one
		"not implemented")

}

// AppendNull appends a null value to the builder
func (cb *ChunkedBuilder[T]) AppendNull() {
	panic("not implemented")

}

// getCurrentBuilder gets the current builder or creates a new one
func (cb *ChunkedBuilder[T]) getCurrentBuilder() array.Builder {
	panic(
		// For simplicity, we'll use a single builder
		// In a real implementation, you might want to chunk at certain sizes
		"not implemented")

}

// createBuilder creates a new Arrow builder based on the data type
func (cb *ChunkedBuilder[T]) createBuilder() array.Builder {
	panic("not implemented")

	// Fallback to string builder

}

// Finish builds the final ChunkedArray
func (cb *ChunkedBuilder[T]) Finish() *ChunkedArray[T] {
	panic("not implemented")

	// Clear builders

	// Get Arrow type from first chunk if available

}

// calculateTotalLength calculates the total length of all chunks
func calculateTotalLength(chunks []arrow.Array) int64 {
	panic("not implemented")

}

// calculateTotalNullCount calculates the total null count of all chunks
func calculateTotalNullCount(chunks []arrow.Array) int64 {
	panic("not implemented")

}
