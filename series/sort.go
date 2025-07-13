package series

import (
	"math"
	"sort"
	"strings"

	"github.com/tnn1t1s/golars/internal/chunked"
	"github.com/tnn1t1s/golars/internal/datatypes"
)

// SortOrder represents the order of sorting
type SortOrder int

const (
	Ascending SortOrder = iota
	Descending
)

// SortConfig contains sorting configuration
type SortConfig struct {
	Order      SortOrder
	NullsFirst bool
	Stable     bool
}

// Sort sorts the series and returns a new sorted series
func (s *TypedSeries[T]) Sort(ascending bool) Series {
	config := SortConfig{
		Order:      ifThenElse(ascending, Ascending, Descending),
		NullsFirst: false,
		Stable:     true,
	}
	return s.SortWithConfig(config)
}

// SortWithConfig sorts the series with custom configuration
func (s *TypedSeries[T]) SortWithConfig(config SortConfig) Series {
	// Get sort indices
	indices := s.ArgSort(config)

	// Take values in sorted order
	return s.Take(indices)
}

// ArgSort returns the indices that would sort the series
func (s *TypedSeries[T]) ArgSort(config SortConfig) []int {
	n := s.Len()
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = i
	}

	// Create comparator
	less := s.makeComparator(config)

	// Use stable or unstable sort
	if config.Stable {
		sort.SliceStable(indices, func(i, j int) bool {
			return less(indices[i], indices[j])
		})
	} else {
		sort.Slice(indices, func(i, j int) bool {
			return less(indices[i], indices[j])
		})
	}

	return indices
}

// makeComparator creates a comparison function based on the config
func (s *TypedSeries[T]) makeComparator(config SortConfig) func(i, j int) bool {
	return func(i, j int) bool {
		// Handle nulls
		iNull := s.IsNull(i)
		jNull := s.IsNull(j)

		if iNull && jNull {
			return false // Equal
		}
		if iNull {
			return config.NullsFirst
		}
		if jNull {
			return !config.NullsFirst
		}

		// Compare values
		iVal, _ := s.chunkedArray.Get(int64(i))
		jVal, _ := s.chunkedArray.Get(int64(j))

		cmp := compareValues(iVal, jVal)
		if config.Order == Ascending {
			return cmp < 0
		} else {
			return cmp > 0
		}
	}
}

// compareValues compares two values of type T
func compareValues[T datatypes.ArrayValue](a, b T) int {
	switch v1 := any(a).(type) {
	case bool:
		v2 := any(b).(bool)
		if !v1 && v2 {
			return -1
		}
		if v1 && !v2 {
			return 1
		}
		return 0
	case int8:
		v2 := any(b).(int8)
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case int16:
		v2 := any(b).(int16)
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case int32:
		v2 := any(b).(int32)
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case int64:
		v2 := any(b).(int64)
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case uint8:
		v2 := any(b).(uint8)
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case uint16:
		v2 := any(b).(uint16)
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case uint32:
		v2 := any(b).(uint32)
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case uint64:
		v2 := any(b).(uint64)
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case float32:
		v2 := any(b).(float32)
		// Handle NaN
		if v1 != v1 && v2 != v2 { // Both NaN
			return 0
		}
		if v1 != v1 { // v1 is NaN
			return 1
		}
		if v2 != v2 { // v2 is NaN
			return -1
		}
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case float64:
		v2 := any(b).(float64)
		// Handle NaN
		if math.IsNaN(v1) && math.IsNaN(v2) {
			return 0
		}
		if math.IsNaN(v1) {
			return 1
		}
		if math.IsNaN(v2) {
			return -1
		}
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case string:
		v2 := any(b).(string)
		return strings.Compare(v1, v2)
	case []byte:
		v2 := any(b).([]byte)
		return strings.Compare(string(v1), string(v2))
	default:
		return 0
	}
}

// Take takes values at the given indices and returns a new series
func (s *TypedSeries[T]) Take(indices []int) Series {
	// Validate indices
	for _, idx := range indices {
		if idx < 0 || idx >= s.Len() {
			// Handle out of bounds - could return error or panic
			// For now, we'll skip invalid indices
			continue
		}
	}

	// Create builder for new array
	builder := chunked.NewChunkedBuilder[T](s.chunkedArray.DataType())

	// Gather values at indices
	for _, idx := range indices {
		if idx >= 0 && idx < s.Len() {
			if s.IsValid(idx) {
				val, _ := s.chunkedArray.Get(int64(idx))
				builder.Append(val)
			} else {
				builder.AppendNull()
			}
		}
	}

	newChunked := builder.Finish()
	return &TypedSeries[T]{
		chunkedArray: newChunked,
		name:         s.name,
	}
}

// Helper function for ternary operator
func ifThenElse[T any](condition bool, ifTrue, ifFalse T) T {
	if condition {
		return ifTrue
	}
	return ifFalse
}