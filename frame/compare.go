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
		return compareLess(left, right) || compareEqual(left, right)
	case expr.OpGreater:
		return !compareLess(left, right) && !compareEqual(left, right)
	case expr.OpGreaterEqual:
		return !compareLess(left, right)
	default:
		return false
	}
}

func compareEqual(left, right interface{}) bool {
	switch l := left.(type) {
	case int32:
		switch r := right.(type) {
		case int32:
			return l == r
		case int64:
			return int64(l) == r
		case float64:
			return float64(l) == r
		}
	case int64:
		switch r := right.(type) {
		case int32:
			return l == int64(r)
		case int64:
			return l == r
		case float64:
			return float64(l) == r
		}
	case float64:
		switch r := right.(type) {
		case int32:
			return l == float64(r)
		case int64:
			return l == float64(r)
		case float64:
			return l == r
		}
	case string:
		if r, ok := right.(string); ok {
			return l == r
		}
	case bool:
		if r, ok := right.(bool); ok {
			return l == r
		}
	}
	return false
}

func compareLess(left, right interface{}) bool {
	switch l := left.(type) {
	case int32:
		switch r := right.(type) {
		case int32:
			return l < r
		case int64:
			return int64(l) < r
		case float64:
			return float64(l) < r
		}
	case int64:
		switch r := right.(type) {
		case int32:
			return l < int64(r)
		case int64:
			return l < r
		case float64:
			return float64(l) < r
		}
	case float64:
		switch r := right.(type) {
		case int32:
			return l < float64(r)
		case int64:
			return l < float64(r)
		case float64:
			return l < r
		}
	case string:
		if r, ok := right.(string); ok {
			return l < r
		}
	}
	return false
}
