package frame

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowcompute "github.com/apache/arrow-go/v18/arrow/compute"
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
	orders := make([]series.SortOrder, len(columns))
	for i := range orders {
		orders[i] = series.Ascending
	}

	return df.SortBy(SortOptions{
		Columns: columns,
		Orders:  orders,
		Stable:  true,
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
		Stable:  true,
	})
}

// SortBy sorts the DataFrame with custom options
func (df *DataFrame) SortBy(options SortOptions) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	if len(options.Columns) == 0 {
		return nil, fmt.Errorf("at least one column must be specified for sorting")
	}

	// Validate columns and get series
	sortSeries := make([]series.Series, len(options.Columns))
	for i, col := range options.Columns {
		s, err := df.Column(col)
		if err != nil {
			return nil, fmt.Errorf("column %s not found", col)
		}
		sortSeries[i] = s
	}

	// Ensure orders array matches columns
	if len(options.Orders) < len(options.Columns) {
		// Extend with ascending order
		newOrders := make([]series.SortOrder, len(options.Columns))
		copy(newOrders, options.Orders)
		for i := len(options.Orders); i < len(options.Columns); i++ {
			newOrders[i] = series.Ascending
		}
		options.Orders = newOrders
	}

	indices, err := df.arrowSortIndices(sortSeries, options)
	if err != nil {
		return nil, err
	}

	return df.Take(indices)
}

func (df *DataFrame) arrowSortIndices(sortSeries []series.Series, options SortOptions) ([]int, error) {
	if len(sortSeries) == 0 {
		return nil, fmt.Errorf("sort requires at least one column")
	}

	chunked := make([]*arrow.Chunked, len(sortSeries))
	for i, s := range sortSeries {
		arr, ok := series.ArrowChunked(s)
		if !ok {
			return nil, fmt.Errorf("arrow sort requires Arrow-backed series")
		}
		chunked[i] = arr
	}
	defer func() {
		for _, arr := range chunked {
			if arr != nil {
				arr.Release()
			}
		}
	}()

	opts := arrowcompute.DefaultSortOptions()
	opts.NullsFirst = options.NullsFirst
	opts.Stable = options.Stable
	opts.Orders = make([]arrowcompute.SortOrder, len(chunked))
	for i := range chunked {
		order := series.Ascending
		if i < len(options.Orders) {
			order = options.Orders[i]
		}
		opts.Orders[i] = toArrowSortOrder(order)
	}

	var indicesArr arrow.Array
	var err error
	if len(chunked) == 1 {
		indicesArr, err = arrowcompute.SortIndicesChunked(chunked[0], opts)
	} else {
		indicesArr, err = arrowcompute.SortIndicesChunkedMulti(chunked, opts)
	}
	if err != nil {
		return nil, err
	}
	defer indicesArr.Release()

	return arrowSortIndexArrayToInts(indicesArr)
}

func toArrowSortOrder(order series.SortOrder) arrowcompute.SortOrder {
	if order == series.Descending {
		return arrowcompute.SortDescending
	}
	return arrowcompute.SortAscending
}

func arrowSortIndexArrayToInts(arr arrow.Array) ([]int, error) {
	switch a := arr.(type) {
	case *array.Int64:
		values := a.Int64Values()
		out := make([]int, len(values))
		for i, v := range values {
			out[i] = int(v)
		}
		return out, nil
	case *array.Int32:
		values := a.Int32Values()
		out := make([]int, len(values))
		for i, v := range values {
			out[i] = int(v)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported sort index type %s", arr.DataType().String())
	}
}

// compareSeriesValues compares two values from a series
func compareSeriesValues(s series.Series, i, j int, order series.SortOrder, nullsFirst bool) int {
	// Handle nulls
	iNull := s.IsNull(i)
	jNull := s.IsNull(j)

	if iNull && jNull {
		return 0 // Equal
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

	// Get values
	iVal := s.Get(i)
	jVal := s.Get(j)

	// Compare based on type
	var cmp int
	switch v1 := iVal.(type) {
	case bool:
		v2 := jVal.(bool)
		if !v1 && v2 {
			cmp = -1
		} else if v1 && !v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case int8:
		v2 := jVal.(int8)
		if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case int16:
		v2 := jVal.(int16)
		if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case int32:
		v2 := jVal.(int32)
		if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case int64:
		v2 := jVal.(int64)
		if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case uint8:
		v2 := jVal.(uint8)
		if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case uint16:
		v2 := jVal.(uint16)
		if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case uint32:
		v2 := jVal.(uint32)
		if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case uint64:
		v2 := jVal.(uint64)
		if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case float32:
		v2 := jVal.(float32)
		// Handle NaN
		if v1 != v1 && v2 != v2 { // Both NaN
			cmp = 0
		} else if v1 != v1 { // v1 is NaN
			cmp = 1
		} else if v2 != v2 { // v2 is NaN
			cmp = -1
		} else if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case float64:
		v2 := jVal.(float64)
		// Handle NaN
		if v1 != v1 && v2 != v2 { // Both NaN
			cmp = 0
		} else if v1 != v1 { // v1 is NaN
			cmp = 1
		} else if v2 != v2 { // v2 is NaN
			cmp = -1
		} else if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	case string:
		v2 := jVal.(string)
		if v1 < v2 {
			cmp = -1
		} else if v1 > v2 {
			cmp = 1
		} else {
			cmp = 0
		}
	default:
		cmp = 0
	}

	// Apply order
	if order == series.Descending {
		cmp = -cmp
	}

	return cmp
}

// Take creates a new DataFrame with rows at the specified indices
func (df *DataFrame) Take(indices []int) (*DataFrame, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	// Validate indices
	for _, idx := range indices {
		if idx < 0 || idx >= df.height {
			return nil, fmt.Errorf("index %d out of bounds [0, %d)", idx, df.height)
		}
	}

	// Create new columns with gathered values
	newColumns := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		newColumns[i] = col.Take(indices)
	}

	return NewDataFrame(newColumns...)
}
