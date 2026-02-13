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
	order := Ascending
	if !ascending {
		order = Descending
	}
	return s.SortWithConfig(SortConfig{
		Order:      order,
		NullsFirst: false,
		Stable:     false,
	})
}

// SortWithConfig sorts the series with custom configuration
func (s *TypedSeries[T]) SortWithConfig(config SortConfig) Series {
	indices := s.ArgSort(config)
	return s.Take(indices)
}

// ArgSort returns the indices that would sort the series
func (s *TypedSeries[T]) ArgSort(config SortConfig) []int {
	n := s.Len()
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	values, validity := s.chunkedArray.ToSlice()

	less := func(i, j int) bool {
		ai := indices[i]
		aj := indices[j]

		iNull := !validity[ai]
		jNull := !validity[aj]

		if iNull && jNull {
			return false
		}
		if iNull {
			return config.NullsFirst
		}
		if jNull {
			return !config.NullsFirst
		}

		cmp := compareValues(values[ai], values[aj])

		if config.Order == Descending {
			return cmp > 0
		}
		return cmp < 0
	}

	if config.Stable {
		sort.SliceStable(indices, less)
	} else {
		sort.Slice(indices, less)
	}

	return indices
}

// makeComparator creates a comparison function based on the config
func (s *TypedSeries[T]) makeComparator(config SortConfig) func(i, j int) bool {
	values, validity := s.chunkedArray.ToSlice()

	return func(i, j int) bool {
		iNull := !validity[i]
		jNull := !validity[j]

		if iNull && jNull {
			return false
		}
		if iNull {
			return config.NullsFirst
		}
		if jNull {
			return !config.NullsFirst
		}

		cmp := compareValues(values[i], values[j])

		if config.Order == Descending {
			return cmp > 0
		}
		return cmp < 0
	}
}

// compareValues compares two values of type T
func compareValues[T datatypes.ArrayValue](a, b T) int {
	switch av := any(a).(type) {
	case bool:
		bv := any(b).(bool)
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1
	case int8:
		bv := any(b).(int8)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int16:
		bv := any(b).(int16)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int32:
		bv := any(b).(int32)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int64:
		bv := any(b).(int64)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case uint8:
		bv := any(b).(uint8)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case uint16:
		bv := any(b).(uint16)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case uint32:
		bv := any(b).(uint32)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case uint64:
		bv := any(b).(uint64)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float32:
		bv := any(b).(float32)
		aNaN := math.IsNaN(float64(av))
		bNaN := math.IsNaN(float64(bv))
		if aNaN && bNaN {
			return 0
		}
		if aNaN {
			return 1
		}
		if bNaN {
			return -1
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float64:
		bv := any(b).(float64)
		aNaN := math.IsNaN(av)
		bNaN := math.IsNaN(bv)
		if aNaN && bNaN {
			return 0
		}
		if aNaN {
			return 1
		}
		if bNaN {
			return -1
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv := any(b).(string)
		return strings.Compare(av, bv)
	default:
		return 0
	}
}

// Take takes values at the given indices and returns a new series
func (s *TypedSeries[T]) Take(indices []int) Series {
	values, validity := s.chunkedArray.ToSlice()
	n := len(values)

	newValues := make([]T, 0, len(indices))
	newValidity := make([]bool, 0, len(indices))

	for _, idx := range indices {
		if idx < 0 || idx >= n {
			continue
		}
		newValues = append(newValues, values[idx])
		newValidity = append(newValidity, validity[idx])
	}

	ca := chunked.NewChunkedArray[T](s.name, s.DataType())
	if len(newValues) > 0 {
		ca.AppendSlice(newValues, newValidity)
	}
	return &TypedSeries[T]{
		chunkedArray: ca,
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
