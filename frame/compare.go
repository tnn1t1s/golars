package frame

import "github.com/tnn1t1s/golars/expr"

// compareValues compares two values based on the operation.
func compareValues(left, right interface{}, op expr.BinaryOp) bool {
	switch op {
	case expr.OpEqual:
		return compareEqual(left, right)
	case expr.OpNotEqual:
		return !compareEqual(left, right)
	case expr.OpLess:
		return compareLess(left, right)
	case expr.OpLessEqual:
		return compareEqual(left, right) || compareLess(left, right)
	case expr.OpGreater:
		return compareLess(right, left)
	case expr.OpGreaterEqual:
		return compareEqual(left, right) || compareLess(right, left)
	default:
		return false
	}
}

func compareEqual(left, right interface{}) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}
	switch lv := left.(type) {
	case string:
		if rv, ok := right.(string); ok {
			return lv == rv
		}
		return false
	case bool:
		if rv, ok := right.(bool); ok {
			return lv == rv
		}
		return false
	default:
		return toFloat64Value(left) == toFloat64Value(right)
	}
}

func compareLess(left, right interface{}) bool {
	if left == nil || right == nil {
		return false
	}
	switch lv := left.(type) {
	case string:
		if rv, ok := right.(string); ok {
			return lv < rv
		}
		return false
	default:
		return toFloat64Value(left) < toFloat64Value(right)
	}
}
