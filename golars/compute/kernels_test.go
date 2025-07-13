package compute

import (
	"math"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestArithmeticKernelInt32(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test arrays
	builder1 := array.NewInt32Builder(mem)
	builder1.AppendValues([]int32{10, 20, 30, 40, 50}, []bool{true, true, true, false, true})
	arr1 := builder1.NewArray()
	defer arr1.Release()

	builder2 := array.NewInt32Builder(mem)
	builder2.AppendValues([]int32{5, 4, 3, 2, 1}, []bool{true, true, false, true, true})
	arr2 := builder2.NewArray()
	defer arr2.Release()

	tests := []struct {
		name     string
		op       ArithmeticOp
		expected []int32
		validity []bool
	}{
		{
			name:     "Add",
			op:       OpAdd,
			expected: []int32{15, 24, 0, 0, 51},
			validity: []bool{true, true, false, false, true},
		},
		{
			name:     "Subtract",
			op:       OpSubtract,
			expected: []int32{5, 16, 0, 0, 49},
			validity: []bool{true, true, false, false, true},
		},
		{
			name:     "Multiply",
			op:       OpMultiply,
			expected: []int32{50, 80, 0, 0, 50},
			validity: []bool{true, true, false, false, true},
		},
		{
			name:     "Divide",
			op:       OpDivide,
			expected: []int32{2, 5, 0, 0, 50},
			validity: []bool{true, true, false, false, true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ArithmeticKernel(arr1, arr2, test.op, mem)
			assert.NoError(t, err)
			defer result.Release()

			resultInt32 := result.(*array.Int32)
			assert.Equal(t, len(test.expected), resultInt32.Len())

			for i := 0; i < resultInt32.Len(); i++ {
				if test.validity[i] {
					assert.False(t, resultInt32.IsNull(i))
					assert.Equal(t, test.expected[i], resultInt32.Value(i))
				} else {
					assert.True(t, resultInt32.IsNull(i))
				}
			}
		})
	}
}

func TestArithmeticKernelFloat64(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder1 := array.NewFloat64Builder(mem)
	builder1.AppendValues([]float64{10.5, 20.0, 30.5}, nil)
	arr1 := builder1.NewArray()
	defer arr1.Release()

	builder2 := array.NewFloat64Builder(mem)
	builder2.AppendValues([]float64{2.0, 4.0, 5.0}, nil)
	arr2 := builder2.NewArray()
	defer arr2.Release()

	// Test division
	result, err := ArithmeticKernel(arr1, arr2, OpDivide, mem)
	assert.NoError(t, err)
	defer result.Release()

	resultFloat64 := result.(*array.Float64)
	assert.Equal(t, 5.25, resultFloat64.Value(0))
	assert.Equal(t, 5.0, resultFloat64.Value(1))
	assert.Equal(t, 6.1, resultFloat64.Value(2))
}

func TestArithmeticKernelDivisionByZero(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder1 := array.NewInt32Builder(mem)
	builder1.AppendValues([]int32{10, 20, 30}, nil)
	arr1 := builder1.NewArray()
	defer arr1.Release()

	builder2 := array.NewInt32Builder(mem)
	builder2.AppendValues([]int32{2, 0, 3}, nil)
	arr2 := builder2.NewArray()
	defer arr2.Release()

	result, err := ArithmeticKernel(arr1, arr2, OpDivide, mem)
	assert.NoError(t, err)
	defer result.Release()

	resultInt32 := result.(*array.Int32)
	assert.False(t, resultInt32.IsNull(0))
	assert.Equal(t, int32(5), resultInt32.Value(0))
	assert.True(t, resultInt32.IsNull(1)) // Division by zero results in null
	assert.False(t, resultInt32.IsNull(2))
	assert.Equal(t, int32(10), resultInt32.Value(2))
}

func TestComparisonKernel(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder1 := array.NewInt64Builder(mem)
	builder1.AppendValues([]int64{10, 20, 30, 40, 50}, nil)
	arr1 := builder1.NewArray()
	defer arr1.Release()

	builder2 := array.NewInt64Builder(mem)
	builder2.AppendValues([]int64{15, 20, 25, 35, 60}, nil)
	arr2 := builder2.NewArray()
	defer arr2.Release()

	tests := []struct {
		name     string
		op       ComparisonOp
		expected []bool
	}{
		{
			name:     "Equal",
			op:       OpEqual,
			expected: []bool{false, true, false, false, false},
		},
		{
			name:     "NotEqual",
			op:       OpNotEqual,
			expected: []bool{true, false, true, true, true},
		},
		{
			name:     "Less",
			op:       OpLess,
			expected: []bool{true, false, false, false, true},
		},
		{
			name:     "LessEqual",
			op:       OpLessEqual,
			expected: []bool{true, true, false, false, true},
		},
		{
			name:     "Greater",
			op:       OpGreater,
			expected: []bool{false, false, true, true, false},
		},
		{
			name:     "GreaterEqual",
			op:       OpGreaterEqual,
			expected: []bool{false, true, true, true, false},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ComparisonKernel(arr1, arr2, test.op, mem)
			assert.NoError(t, err)
			defer result.Release()

			resultBool := result.(*array.Boolean)
			assert.Equal(t, len(test.expected), resultBool.Len())

			for i := 0; i < resultBool.Len(); i++ {
				assert.Equal(t, test.expected[i], resultBool.Value(i))
			}
		})
	}
}

func TestComparisonKernelString(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder1 := array.NewStringBuilder(mem)
	builder1.AppendValues([]string{"apple", "banana", "cherry"}, nil)
	arr1 := builder1.NewArray()
	defer arr1.Release()

	builder2 := array.NewStringBuilder(mem)
	builder2.AppendValues([]string{"banana", "banana", "apple"}, nil)
	arr2 := builder2.NewArray()
	defer arr2.Release()

	result, err := ComparisonKernel(arr1, arr2, OpLess, mem)
	assert.NoError(t, err)
	defer result.Release()

	resultBool := result.(*array.Boolean)
	assert.True(t, resultBool.Value(0))  // "apple" < "banana"
	assert.False(t, resultBool.Value(1)) // "banana" < "banana"
	assert.False(t, resultBool.Value(2)) // "cherry" < "apple"
}

func TestAggregateKernelInt32(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewInt32Builder(mem)
	builder.AppendValues([]int32{10, 20, 30, 40, 50}, []bool{true, true, false, true, true})
	arr := builder.NewArray()
	defer arr.Release()

	tests := []struct {
		name     string
		op       AggregateOp
		expected interface{}
	}{
		{
			name:     "Sum",
			op:       AggSum,
			expected: int64(120), // 10 + 20 + 40 + 50 (null excluded)
		},
		{
			name:     "Mean",
			op:       AggMean,
			expected: 30.0, // 120 / 4
		},
		{
			name:     "Min",
			op:       AggMin,
			expected: int32(10),
		},
		{
			name:     "Max",
			op:       AggMax,
			expected: int32(50),
		},
		{
			name:     "Count",
			op:       AggCount,
			expected: int64(4),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := AggregateKernel(arr, test.op)
			assert.NoError(t, err)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestAggregateKernelFloat64(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewFloat64Builder(mem)
	builder.AppendValues([]float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	// Test variance and standard deviation
	variance, err := AggregateKernel(arr, AggVar)
	assert.NoError(t, err)
	assert.Equal(t, 2.0, variance.(float64))

	std, err := AggregateKernel(arr, AggStd)
	assert.NoError(t, err)
	assert.InDelta(t, math.Sqrt(2.0), std.(float64), 0.0001)
}

func TestAggregateKernelEmpty(t *testing.T) {
	mem := memory.NewGoAllocator()

	// All null array
	builder := array.NewInt64Builder(mem)
	builder.AppendValues([]int64{1, 2, 3}, []bool{false, false, false})
	arr := builder.NewArray()
	defer arr.Release()

	result, err := AggregateKernel(arr, AggMean)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestScalarArithmeticKernel(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewFloat32Builder(mem)
	builder.AppendValues([]float32{10.0, 20.0, 30.0}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	// Multiply by scalar
	result, err := ScalarArithmeticKernel(arr, 2.5, OpMultiply, mem)
	assert.NoError(t, err)
	defer result.Release()

	resultFloat32 := result.(*array.Float32)
	assert.Equal(t, float32(25.0), resultFloat32.Value(0))
	assert.Equal(t, float32(50.0), resultFloat32.Value(1))
	assert.Equal(t, float32(75.0), resultFloat32.Value(2))
}

func TestTypeConversionHelpers(t *testing.T) {
	// Test toInt32
	val1, ok := toInt32(int64(42))
	assert.True(t, ok)
	assert.Equal(t, int32(42), val1)

	val2, ok := toInt32(3.14)
	assert.True(t, ok)
	assert.Equal(t, int32(3), val2)

	_, ok = toInt32("string")
	assert.False(t, ok)

	// Test toFloat64
	val3, ok := toFloat64(int32(42))
	assert.True(t, ok)
	assert.Equal(t, 42.0, val3)

	val4, ok := toFloat64(float32(3.14))
	assert.True(t, ok)
	assert.InDelta(t, 3.14, val4, 0.001)
}

func BenchmarkArithmeticKernel(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create large arrays
	size := 100000
	values1 := make([]float64, size)
	values2 := make([]float64, size)
	for i := 0; i < size; i++ {
		values1[i] = float64(i)
		values2[i] = float64(i) * 0.5
	}

	builder1 := array.NewFloat64Builder(mem)
	builder1.AppendValues(values1, nil)
	arr1 := builder1.NewArray()
	defer arr1.Release()

	builder2 := array.NewFloat64Builder(mem)
	builder2.AppendValues(values2, nil)
	arr2 := builder2.NewArray()
	defer arr2.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := ArithmeticKernel(arr1, arr2, OpAdd, mem)
		result.Release()
	}
}

func BenchmarkAggregateKernel(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create large array
	size := 100000
	values := make([]float64, size)
	for i := 0; i < size; i++ {
		values[i] = float64(i)
	}

	builder := array.NewFloat64Builder(mem)
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	defer arr.Release()

	b.Run("Sum", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = AggregateKernel(arr, AggSum)
		}
	})

	b.Run("Mean", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = AggregateKernel(arr, AggMean)
		}
	})

	b.Run("StdDev", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = AggregateKernel(arr, AggStd)
		}
	})
}