package frame

import (
	"fmt"
	"math"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/tnn1t1s/golars/series"
)

// SortOptions contains options for sorting a DataFrame
type SortOptions struct {
	Columns    []string
	Orders     []series.SortOrder
	NullsFirst bool
	Stable     bool
}

// Sort sorts the DataFrame by the specified columns in ascending order
func (df *DataFrame) Sort(columns ...string) (*DataFrame, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("at least one column must be specified for sorting")
	}
	orders := make([]series.SortOrder, len(columns))
	for i := range orders {
		orders[i] = series.Ascending
	}
	return df.SortBy(SortOptions{
		Columns: columns,
		Orders:  orders,
	})
}

// SortDesc sorts the DataFrame by the specified columns in descending order
func (df *DataFrame) SortDesc(columns ...string) (*DataFrame, error) {
	orders := make([]series.SortOrder, len(columns))
	for i := range orders {
		orders[i] = series.Descending
	}
	return df.SortBy(SortOptions{
		Columns: columns,
		Orders:  orders,
	})
}

// SortBy sorts the DataFrame with custom options
func (df *DataFrame) SortBy(options SortOptions) (*DataFrame, error) {
	if len(options.Columns) == 0 {
		return df.Clone(), nil
	}

	// Validate columns and get series
	sortSeries := make([]series.Series, len(options.Columns))
	for i, name := range options.Columns {
		col, err := df.Column(name)
		if err != nil {
			return nil, err
		}
		sortSeries[i] = col
	}

	// Ensure orders array matches columns
	if len(options.Orders) < len(options.Columns) {
		extended := make([]series.SortOrder, len(options.Columns))
		copy(extended, options.Orders)
		for i := len(options.Orders); i < len(options.Columns); i++ {
			extended[i] = series.Ascending
		}
		options.Orders = extended
	}

	// Build sort indices
	indices := make([]int, df.height)
	for i := range indices {
		indices[i] = i
	}

	less := func(i, j int) bool {
		for k, s := range sortSeries {
			cmp := compareSeriesValues(s, indices[i], indices[j], options.Orders[k], options.NullsFirst)
			if cmp != 0 {
				return cmp < 0
			}
		}
		return indices[i] < indices[j] // stable tie-break
	}

	if options.Stable {
		sort.SliceStable(indices, less)
	} else {
		sort.Slice(indices, less)
	}

	// Take rows in sorted order
	return df.Take(indices)
}

func (df *DataFrame) arrowSortIndices(sortSeries []series.Series, options SortOptions) ([]int, error) {
	// Arrow sort path not available; fall back to Go sort
	return nil, fmt.Errorf("arrow sort not implemented")
}

func arrowSortIndexArrayToInts(arr arrow.Array) ([]int, error) {
	switch a := arr.(type) {
	case *array.Uint64:
		n := a.Len()
		result := make([]int, n)
		for i := 0; i < n; i++ {
			result[i] = int(a.Value(i))
		}
		return result, nil
	case *array.Int64:
		n := a.Len()
		result := make([]int, n)
		for i := 0; i < n; i++ {
			result[i] = int(a.Value(i))
		}
		return result, nil
	default:
		return nil, nil
	}
}

// compareSeriesValues compares two values from a series
func compareSeriesValues(s series.Series, i, j int, order series.SortOrder, nullsFirst bool) int {
	iNull := s.IsNull(i)
	jNull := s.IsNull(j)

	if iNull && jNull {
		return 0
	}
	if iNull {
		if nullsFirst {
			return -1
		}
		return 1
	}
	if jNull {
		if nullsFirst {
			return 1
		}
		return -1
	}

	v1 := s.Get(i)
	v2 := s.Get(j)

	cmp := 0
	switch a := v1.(type) {
	case int32:
		b := v2.(int32)
		if a < b {
			cmp = -1
		} else if a > b {
			cmp = 1
		}
	case int64:
		b := v2.(int64)
		if a < b {
			cmp = -1
		} else if a > b {
			cmp = 1
		}
	case float64:
		b := v2.(float64)
		aNaN := math.IsNaN(a)
		bNaN := math.IsNaN(b)
		if aNaN && bNaN {
			return 0
		}
		if aNaN {
			if nullsFirst {
				return -1
			}
			return 1
		}
		if bNaN {
			if nullsFirst {
				return 1
			}
			return -1
		}
		if a < b {
			cmp = -1
		} else if a > b {
			cmp = 1
		}
	case float32:
		b := v2.(float32)
		aNaN := math.IsNaN(float64(a))
		bNaN := math.IsNaN(float64(b))
		if aNaN && bNaN {
			return 0
		}
		if aNaN {
			if nullsFirst {
				return -1
			}
			return 1
		}
		if bNaN {
			if nullsFirst {
				return 1
			}
			return -1
		}
		if a < b {
			cmp = -1
		} else if a > b {
			cmp = 1
		}
	case string:
		b := v2.(string)
		if a < b {
			cmp = -1
		} else if a > b {
			cmp = 1
		}
	case bool:
		b := v2.(bool)
		if !a && b {
			cmp = -1
		} else if a && !b {
			cmp = 1
		}
	default:
		af := toFloat64Value(v1)
		bf := toFloat64Value(v2)
		if af < bf {
			cmp = -1
		} else if af > bf {
			cmp = 1
		}
	}

	if order == series.Descending {
		return -cmp
	}
	return cmp
}

// Take creates a new DataFrame with rows at the specified indices
func (df *DataFrame) Take(indices []int) (*DataFrame, error) {
	if len(indices) == 0 {
		cols := make([]series.Series, len(df.columns))
		for i, col := range df.columns {
			cols[i] = col.Head(0)
		}
		return NewDataFrame(cols...)
	}

	// Validate indices
	for _, idx := range indices {
		if idx < 0 || idx >= df.height {
			return nil, fmt.Errorf("index %d out of bounds [0, %d)", idx, df.height)
		}
	}

	// Create new columns with gathered values
	cols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		taken, ok := series.TakeFast(col, indices)
		if ok {
			cols[i] = taken
		} else {
			cols[i] = col.Take(indices)
		}
	}
	return NewDataFrame(cols...)
}
