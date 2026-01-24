package chunked

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/tnn1t1s/golars/internal/datatypes"
)

func TestNewChunkedArray(t *testing.T) {
	ca := NewChunkedArray[int32]("test", datatypes.Int32{})

	assert.Equal(t, "test", ca.Name())
	assert.Equal(t, datatypes.Int32{}, ca.DataType())
	assert.Equal(t, int64(0), ca.Len())
	assert.Equal(t, int64(0), ca.NullCount())
	assert.Equal(t, 0, ca.NumChunks())
}

func TestAppendSlice(t *testing.T) {
	ca := NewChunkedArray[int32]("numbers", datatypes.Int32{})
	defer ca.Release()

	values := []int32{1, 2, 3, 4, 5}
	validity := []bool{true, true, false, true, true}

	err := ca.AppendSlice(values, validity)
	assert.NoError(t, err)

	assert.Equal(t, int64(5), ca.Len())
	assert.Equal(t, int64(1), ca.NullCount())
	assert.Equal(t, 1, ca.NumChunks())
}

func TestAppendArray(t *testing.T) {
	ca := NewChunkedArray[float64]("floats", datatypes.Float64{})
	defer ca.Release()

	mem := memory.NewGoAllocator()
	builder := array.NewFloat64Builder(mem)
	builder.AppendValues([]float64{1.1, 2.2, 3.3}, []bool{true, false, true})
	arr := builder.NewArray()
	defer arr.Release()

	err := ca.AppendArray(arr)
	assert.NoError(t, err)

	assert.Equal(t, int64(3), ca.Len())
	assert.Equal(t, int64(1), ca.NullCount())
}

func TestGet(t *testing.T) {
	ca := NewChunkedArray[string]("strings", datatypes.String{})
	defer ca.Release()

	// Add first chunk
	err := ca.AppendSlice([]string{"hello", "world", "foo"}, []bool{true, true, false})
	assert.NoError(t, err)

	// Add second chunk
	err = ca.AppendSlice([]string{"bar", "baz"}, []bool{true, true})
	assert.NoError(t, err)

	tests := []struct {
		idx      int64
		expected string
		valid    bool
	}{
		{0, "hello", true},
		{1, "world", true},
		{2, "", false},   // null value
		{3, "bar", true}, // from second chunk
		{4, "baz", true},
		{5, "", false},  // out of bounds
		{-1, "", false}, // negative index
	}

	for _, test := range tests {
		t.Run(string(rune(test.idx)), func(t *testing.T) {
			val, valid := ca.Get(test.idx)
			assert.Equal(t, test.valid, valid)
			if valid {
				assert.Equal(t, test.expected, val)
			}
		})
	}
}

func TestSlice(t *testing.T) {
	ca := NewChunkedArray[int64]("numbers", datatypes.Int64{})
	defer ca.Release()

	// Create array with multiple chunks
	for i := 0; i < 3; i++ {
		values := []int64{int64(i*10 + 1), int64(i*10 + 2), int64(i*10 + 3)}
		err := ca.AppendSlice(values, nil)
		assert.NoError(t, err)
	}

	// Test various slices
	tests := []struct {
		start, end int64
		expected   []int64
	}{
		{0, 3, []int64{1, 2, 3}},
		{2, 5, []int64{3, 11, 12}},
		{4, 7, []int64{12, 13, 21}},
		{0, 9, []int64{1, 2, 3, 11, 12, 13, 21, 22, 23}},
		{3, 3, []int64{}}, // empty slice
	}

	for _, test := range tests {
		t.Run(string(rune(test.start)), func(t *testing.T) {
			sliced, err := ca.Slice(test.start, test.end)
			assert.NoError(t, err)
			defer sliced.Release()

			values, _ := sliced.ToSlice()
			assert.Equal(t, test.expected, values)
		})
	}

	// Test invalid slices
	_, err := ca.Slice(-1, 5)
	assert.Error(t, err)

	_, err = ca.Slice(0, 10)
	assert.Error(t, err)

	_, err = ca.Slice(5, 3)
	assert.Error(t, err)
}

func TestToSlice(t *testing.T) {
	ca := NewChunkedArray[bool]("flags", datatypes.Boolean{})
	defer ca.Release()

	values1 := []bool{true, false, true}
	validity1 := []bool{true, false, true}
	err := ca.AppendSlice(values1, validity1)
	assert.NoError(t, err)

	values2 := []bool{false, true}
	validity2 := []bool{true, true}
	err = ca.AppendSlice(values2, validity2)
	assert.NoError(t, err)

	resultValues, resultValidity := ca.ToSlice()

	expectedValues := []bool{true, false, true, false, true}
	expectedValidity := []bool{true, false, true, true, true}

	assert.Equal(t, expectedValues, resultValues)
	assert.Equal(t, expectedValidity, resultValidity)
}

func TestIsValid(t *testing.T) {
	ca := NewChunkedArray[uint32]("uints", datatypes.UInt32{})
	defer ca.Release()

	values := []uint32{1, 2, 3, 4, 5}
	validity := []bool{true, false, true, false, true}
	err := ca.AppendSlice(values, validity)
	assert.NoError(t, err)

	assert.True(t, ca.IsValid(0))
	assert.False(t, ca.IsValid(1))
	assert.True(t, ca.IsValid(2))
	assert.False(t, ca.IsValid(3))
	assert.True(t, ca.IsValid(4))
	assert.False(t, ca.IsValid(5))  // out of bounds
	assert.False(t, ca.IsValid(-1)) // negative
}

func TestMultipleChunks(t *testing.T) {
	ca := NewChunkedArray[float32]("floats", datatypes.Float32{})
	defer ca.Release()

	// Add 5 chunks
	for i := 0; i < 5; i++ {
		values := []float32{float32(i), float32(i) + 0.5}
		err := ca.AppendSlice(values, nil)
		assert.NoError(t, err)
	}

	assert.Equal(t, int64(10), ca.Len())
	assert.Equal(t, 5, ca.NumChunks())

	// Verify values across chunks
	for i := int64(0); i < 10; i++ {
		val, valid := ca.Get(i)
		assert.True(t, valid)
		expected := float32(i / 2)
		if i%2 == 1 {
			expected += 0.5
		}
		assert.Equal(t, expected, val)
	}
}

func TestDifferentTypes(t *testing.T) {
	t.Run("Int8", func(t *testing.T) {
		ca := NewChunkedArray[int8]("i8", datatypes.Int8{})
		defer ca.Release()
		values := []int8{-128, 0, 127}
		err := ca.AppendSlice(values, nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), ca.Len())
	})

	t.Run("UInt64", func(t *testing.T) {
		ca := NewChunkedArray[uint64]("u64", datatypes.UInt64{})
		defer ca.Release()
		values := []uint64{0, 1 << 32, 1 << 63}
		err := ca.AppendSlice(values, nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), ca.Len())
	})

	t.Run("Binary", func(t *testing.T) {
		ca := NewChunkedArray[[]byte]("binary", datatypes.Binary{})
		defer ca.Release()
		values := [][]byte{[]byte("hello"), []byte("world"), nil}
		validity := []bool{true, true, false}
		err := ca.AppendSlice(values, validity)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), ca.Len())
		assert.Equal(t, int64(1), ca.NullCount())
	})
}

func BenchmarkAppendSlice(b *testing.B) {
	ca := NewChunkedArray[int64]("bench", datatypes.Int64{})
	defer ca.Release()

	values := make([]int64, 1000)
	for i := range values {
		values[i] = int64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ca.AppendSlice(values, nil)
	}
}

func BenchmarkGet(b *testing.B) {
	ca := NewChunkedArray[int64]("bench", datatypes.Int64{})
	defer ca.Release()

	// Create array with 10k elements
	values := make([]int64, 10000)
	for i := range values {
		values[i] = int64(i)
	}
	_ = ca.AppendSlice(values, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ca.Get(int64(i % 10000))
	}
}
