package compute

import (
	"fmt"
	"math"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

// ArithmeticOp represents arithmetic operations
type ArithmeticOp int

const (
	OpAdd ArithmeticOp = iota
	OpSubtract
	OpMultiply
	OpDivide
	OpModulo
)

// ComparisonOp represents comparison operations
type ComparisonOp int

const (
	OpEqual ComparisonOp = iota
	OpNotEqual
	OpLess
	OpLessEqual
	OpGreater
	OpGreaterEqual
)

// AggregateOp represents aggregation operations
type AggregateOp int

const (
	AggSum AggregateOp = iota
	AggMean
	AggMin
	AggMax
	AggCount
	AggStd
	AggVar
)

// ArithmeticKernel performs arithmetic operations on arrays
func ArithmeticKernel(left, right arrow.Array, op ArithmeticOp, mem memory.Allocator) (arrow.Array, error) {
	if left.Len() != right.Len() {
		return nil, fmt.Errorf("arrays must have same length")
	}

	// For simplicity, we'll implement for common numeric types
	switch l := left.(type) {
	case *array.Int32:
		return arithmeticInt32(l, right, op, mem)
	case *array.Int64:
		return arithmeticInt64(l, right, op, mem)
	case *array.Float32:
		return arithmeticFloat32(l, right, op, mem)
	case *array.Float64:
		return arithmeticFloat64(l, right, op, mem)
	default:
		return nil, fmt.Errorf("unsupported array type for arithmetic: %T", left)
	}
}

// arithmeticInt32 performs arithmetic on int32 arrays
func arithmeticInt32(left *array.Int32, right arrow.Array, op ArithmeticOp, mem memory.Allocator) (arrow.Array, error) {
	var rightInt32 *array.Int32
	switch r := right.(type) {
	case *array.Int32:
		rightInt32 = r
	default:
		return nil, fmt.Errorf("type mismatch: cannot perform arithmetic between int32 and %T", right)
	}

	builder := array.NewInt32Builder(mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) || rightInt32.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l, r := left.Value(i), rightInt32.Value(i)
		var result int32

		switch op {
		case OpAdd:
			result = l + r
		case OpSubtract:
			result = l - r
		case OpMultiply:
			result = l * r
		case OpDivide:
			if r == 0 {
				builder.AppendNull()
				continue
			}
			result = l / r
		case OpModulo:
			if r == 0 {
				builder.AppendNull()
				continue
			}
			result = l % r
		default:
			return nil, fmt.Errorf("unsupported arithmetic operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

// arithmeticInt64 performs arithmetic on int64 arrays
func arithmeticInt64(left *array.Int64, right arrow.Array, op ArithmeticOp, mem memory.Allocator) (arrow.Array, error) {
	var rightInt64 *array.Int64
	switch r := right.(type) {
	case *array.Int64:
		rightInt64 = r
	default:
		return nil, fmt.Errorf("type mismatch: cannot perform arithmetic between int64 and %T", right)
	}

	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) || rightInt64.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l, r := left.Value(i), rightInt64.Value(i)
		var result int64

		switch op {
		case OpAdd:
			result = l + r
		case OpSubtract:
			result = l - r
		case OpMultiply:
			result = l * r
		case OpDivide:
			if r == 0 {
				builder.AppendNull()
				continue
			}
			result = l / r
		case OpModulo:
			if r == 0 {
				builder.AppendNull()
				continue
			}
			result = l % r
		default:
			return nil, fmt.Errorf("unsupported arithmetic operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

// arithmeticFloat32 performs arithmetic on float32 arrays
func arithmeticFloat32(left *array.Float32, right arrow.Array, op ArithmeticOp, mem memory.Allocator) (arrow.Array, error) {
	var rightFloat32 *array.Float32
	switch r := right.(type) {
	case *array.Float32:
		rightFloat32 = r
	default:
		return nil, fmt.Errorf("type mismatch: cannot perform arithmetic between float32 and %T", right)
	}

	builder := array.NewFloat32Builder(mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) || rightFloat32.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l, r := left.Value(i), rightFloat32.Value(i)
		var result float32

		switch op {
		case OpAdd:
			result = l + r
		case OpSubtract:
			result = l - r
		case OpMultiply:
			result = l * r
		case OpDivide:
			if r == 0 {
				builder.AppendNull()
				continue
			}
			result = l / r
		case OpModulo:
			if r == 0 {
				builder.AppendNull()
				continue
			}
			result = float32(math.Mod(float64(l), float64(r)))
		default:
			return nil, fmt.Errorf("unsupported arithmetic operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

// arithmeticFloat64 performs arithmetic on float64 arrays
func arithmeticFloat64(left *array.Float64, right arrow.Array, op ArithmeticOp, mem memory.Allocator) (arrow.Array, error) {
	var rightFloat64 *array.Float64
	switch r := right.(type) {
	case *array.Float64:
		rightFloat64 = r
	default:
		return nil, fmt.Errorf("type mismatch: cannot perform arithmetic between float64 and %T", right)
	}

	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) || rightFloat64.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l, r := left.Value(i), rightFloat64.Value(i)
		var result float64

		switch op {
		case OpAdd:
			result = l + r
		case OpSubtract:
			result = l - r
		case OpMultiply:
			result = l * r
		case OpDivide:
			if r == 0 {
				builder.AppendNull()
				continue
			}
			result = l / r
		case OpModulo:
			if r == 0 {
				builder.AppendNull()
				continue
			}
			result = math.Mod(l, r)
		default:
			return nil, fmt.Errorf("unsupported arithmetic operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

// ComparisonKernel performs comparison operations on arrays
func ComparisonKernel(left, right arrow.Array, op ComparisonOp, mem memory.Allocator) (arrow.Array, error) {
	if left.Len() != right.Len() {
		return nil, fmt.Errorf("arrays must have same length")
	}

	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()

	// Type-specific comparison
	switch l := left.(type) {
	case *array.Int32:
		r, ok := right.(*array.Int32)
		if !ok {
			return nil, fmt.Errorf("type mismatch for comparison")
		}
		for i := 0; i < l.Len(); i++ {
			if l.IsNull(i) || r.IsNull(i) {
				builder.AppendNull()
				continue
			}
			builder.Append(compareInt32(l.Value(i), r.Value(i), op))
		}

	case *array.Int64:
		r, ok := right.(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for comparison")
		}
		for i := 0; i < l.Len(); i++ {
			if l.IsNull(i) || r.IsNull(i) {
				builder.AppendNull()
				continue
			}
			builder.Append(compareInt64(l.Value(i), r.Value(i), op))
		}

	case *array.Float32:
		r, ok := right.(*array.Float32)
		if !ok {
			return nil, fmt.Errorf("type mismatch for comparison")
		}
		for i := 0; i < l.Len(); i++ {
			if l.IsNull(i) || r.IsNull(i) {
				builder.AppendNull()
				continue
			}
			builder.Append(compareFloat32(l.Value(i), r.Value(i), op))
		}

	case *array.Float64:
		r, ok := right.(*array.Float64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for comparison")
		}
		for i := 0; i < l.Len(); i++ {
			if l.IsNull(i) || r.IsNull(i) {
				builder.AppendNull()
				continue
			}
			builder.Append(compareFloat64(l.Value(i), r.Value(i), op))
		}

	case *array.String:
		r, ok := right.(*array.String)
		if !ok {
			return nil, fmt.Errorf("type mismatch for comparison")
		}
		for i := 0; i < l.Len(); i++ {
			if l.IsNull(i) || r.IsNull(i) {
				builder.AppendNull()
				continue
			}
			builder.Append(compareString(l.Value(i), r.Value(i), op))
		}

	default:
		return nil, fmt.Errorf("unsupported array type for comparison: %T", left)
	}

	return builder.NewArray(), nil
}

// Comparison helper functions
func compareInt32(l, r int32, op ComparisonOp) bool {
	switch op {
	case OpEqual:
		return l == r
	case OpNotEqual:
		return l != r
	case OpLess:
		return l < r
	case OpLessEqual:
		return l <= r
	case OpGreater:
		return l > r
	case OpGreaterEqual:
		return l >= r
	default:
		return false
	}
}

func compareInt64(l, r int64, op ComparisonOp) bool {
	switch op {
	case OpEqual:
		return l == r
	case OpNotEqual:
		return l != r
	case OpLess:
		return l < r
	case OpLessEqual:
		return l <= r
	case OpGreater:
		return l > r
	case OpGreaterEqual:
		return l >= r
	default:
		return false
	}
}

func compareFloat32(l, r float32, op ComparisonOp) bool {
	switch op {
	case OpEqual:
		return l == r
	case OpNotEqual:
		return l != r
	case OpLess:
		return l < r
	case OpLessEqual:
		return l <= r
	case OpGreater:
		return l > r
	case OpGreaterEqual:
		return l >= r
	default:
		return false
	}
}

func compareFloat64(l, r float64, op ComparisonOp) bool {
	switch op {
	case OpEqual:
		return l == r
	case OpNotEqual:
		return l != r
	case OpLess:
		return l < r
	case OpLessEqual:
		return l <= r
	case OpGreater:
		return l > r
	case OpGreaterEqual:
		return l >= r
	default:
		return false
	}
}

func compareString(l, r string, op ComparisonOp) bool {
	switch op {
	case OpEqual:
		return l == r
	case OpNotEqual:
		return l != r
	case OpLess:
		return l < r
	case OpLessEqual:
		return l <= r
	case OpGreater:
		return l > r
	case OpGreaterEqual:
		return l >= r
	default:
		return false
	}
}

// AggregateKernel performs aggregation operations on arrays
func AggregateKernel(arr arrow.Array, op AggregateOp) (interface{}, error) {
	switch a := arr.(type) {
	case *array.Int32:
		return aggregateInt32(a, op)
	case *array.Int64:
		return aggregateInt64(a, op)
	case *array.Float32:
		return aggregateFloat32(a, op)
	case *array.Float64:
		return aggregateFloat64(a, op)
	default:
		return nil, fmt.Errorf("unsupported array type for aggregation: %T", arr)
	}
}

func aggregateInt32(arr *array.Int32, op AggregateOp) (interface{}, error) {
	switch op {
	case AggSum:
		var sum int64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += int64(arr.Value(i))
			}
		}
		return sum, nil

	case AggMean:
		var sum int64
		var count int
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += int64(arr.Value(i))
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		return float64(sum) / float64(count), nil

	case AggMin:
		var min *int32
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if min == nil || val < *min {
					min = &val
				}
			}
		}
		if min == nil {
			return nil, nil
		}
		return *min, nil

	case AggMax:
		var max *int32
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if max == nil || val > *max {
					max = &val
				}
			}
		}
		if max == nil {
			return nil, nil
		}
		return *max, nil

	case AggCount:
		count := 0
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				count++
			}
		}
		return int64(count), nil

	default:
		return nil, fmt.Errorf("unsupported aggregation operation: %v", op)
	}
}

func aggregateInt64(arr *array.Int64, op AggregateOp) (interface{}, error) {
	switch op {
	case AggSum:
		var sum int64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += arr.Value(i)
			}
		}
		return sum, nil

	case AggMean:
		var sum int64
		var count int
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += arr.Value(i)
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		return float64(sum) / float64(count), nil

	case AggMin:
		var min *int64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if min == nil || val < *min {
					min = &val
				}
			}
		}
		if min == nil {
			return nil, nil
		}
		return *min, nil

	case AggMax:
		var max *int64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if max == nil || val > *max {
					max = &val
				}
			}
		}
		if max == nil {
			return nil, nil
		}
		return *max, nil

	case AggCount:
		count := 0
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				count++
			}
		}
		return int64(count), nil

	default:
		return nil, fmt.Errorf("unsupported aggregation operation: %v", op)
	}
}

func aggregateFloat32(arr *array.Float32, op AggregateOp) (interface{}, error) {
	switch op {
	case AggSum:
		var sum float64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += float64(arr.Value(i))
			}
		}
		return sum, nil

	case AggMean:
		var sum float64
		var count int
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += float64(arr.Value(i))
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil

	case AggMin:
		var min *float32
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if min == nil || val < *min {
					min = &val
				}
			}
		}
		if min == nil {
			return nil, nil
		}
		return *min, nil

	case AggMax:
		var max *float32
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if max == nil || val > *max {
					max = &val
				}
			}
		}
		if max == nil {
			return nil, nil
		}
		return *max, nil

	case AggCount:
		count := 0
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				count++
			}
		}
		return int64(count), nil

	case AggStd, AggVar:
		// Calculate mean first
		var sum float64
		var count int
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += float64(arr.Value(i))
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		mean := sum / float64(count)

		// Calculate variance
		var variance float64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				diff := float64(arr.Value(i)) - mean
				variance += diff * diff
			}
		}
		variance /= float64(count)

		if op == AggVar {
			return variance, nil
		}
		return math.Sqrt(variance), nil

	default:
		return nil, fmt.Errorf("unsupported aggregation operation: %v", op)
	}
}

func aggregateFloat64(arr *array.Float64, op AggregateOp) (interface{}, error) {
	switch op {
	case AggSum:
		var sum float64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += arr.Value(i)
			}
		}
		return sum, nil

	case AggMean:
		var sum float64
		var count int
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += arr.Value(i)
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil

	case AggMin:
		var min *float64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if min == nil || val < *min {
					min = &val
				}
			}
		}
		if min == nil {
			return nil, nil
		}
		return *min, nil

	case AggMax:
		var max *float64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if max == nil || val > *max {
					max = &val
				}
			}
		}
		if max == nil {
			return nil, nil
		}
		return *max, nil

	case AggCount:
		count := 0
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				count++
			}
		}
		return int64(count), nil

	case AggStd, AggVar:
		// Calculate mean first
		var sum float64
		var count int
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += arr.Value(i)
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		mean := sum / float64(count)

		// Calculate variance
		var variance float64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				diff := arr.Value(i) - mean
				variance += diff * diff
			}
		}
		variance /= float64(count)

		if op == AggVar {
			return variance, nil
		}
		return math.Sqrt(variance), nil

	default:
		return nil, fmt.Errorf("unsupported aggregation operation: %v", op)
	}
}

// ScalarArithmeticKernel performs arithmetic between an array and a scalar
func ScalarArithmeticKernel(arr arrow.Array, scalar interface{}, op ArithmeticOp, mem memory.Allocator) (arrow.Array, error) {
	// Create a scalar array of the same length
	scalarArr, err := createScalarArray(scalar, arr.Len(), arr.DataType(), mem)
	if err != nil {
		return nil, err
	}
	defer scalarArr.Release()

	return ArithmeticKernel(arr, scalarArr, op, mem)
}

// createScalarArray creates an array filled with a scalar value
func createScalarArray(scalar interface{}, length int, dt arrow.DataType, mem memory.Allocator) (arrow.Array, error) {
	switch dt.ID() {
	case arrow.INT32:
		builder := array.NewInt32Builder(mem)
		defer builder.Release()
		val, ok := toInt32(scalar)
		if !ok {
			return nil, fmt.Errorf("cannot convert %v to int32", scalar)
		}
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return builder.NewArray(), nil

	case arrow.INT64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		val, ok := toInt64(scalar)
		if !ok {
			return nil, fmt.Errorf("cannot convert %v to int64", scalar)
		}
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return builder.NewArray(), nil

	case arrow.FLOAT32:
		builder := array.NewFloat32Builder(mem)
		defer builder.Release()
		val, ok := toFloat32(scalar)
		if !ok {
			return nil, fmt.Errorf("cannot convert %v to float32", scalar)
		}
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return builder.NewArray(), nil

	case arrow.FLOAT64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		val, ok := toFloat64(scalar)
		if !ok {
			return nil, fmt.Errorf("cannot convert %v to float64", scalar)
		}
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return builder.NewArray(), nil

	default:
		return nil, fmt.Errorf("unsupported data type for scalar array: %v", dt)
	}
}

// Type conversion helpers
func toInt32(v interface{}) (int32, bool) {
	switch val := v.(type) {
	case int32:
		return val, true
	case int:
		return int32(val), true
	case int64:
		return int32(val), true
	case float32:
		return int32(val), true
	case float64:
		return int32(val), true
	default:
		return 0, false
	}
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case float32:
		return int64(val), true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

func toFloat32(v interface{}) (float32, bool) {
	switch val := v.(type) {
	case float32:
		return val, true
	case float64:
		return float32(val), true
	case int:
		return float32(val), true
	case int32:
		return float32(val), true
	case int64:
		return float32(val), true
	default:
		return 0, false
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}
