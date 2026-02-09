package frame

import (
	"fmt"
	"math"
	"sort"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Mode calculates the mode (most frequent value) of each column
func (df *DataFrame) Mode(axis int, numeric bool, dropNaN bool) (*DataFrame, error) {
	if axis != 0 {
		return nil, fmt.Errorf("only axis=0 is supported for Mode")
	}

	var resultCols []series.Series
	for _, col := range df.columns {
		if numeric && !isNumericType(col.DataType()) {
			continue
		}
		modeVal := calculateMode(col, dropNaN)
		if modeVal == nil {
			resultCols = append(resultCols, createNullSeries(col.Name(), col.DataType(), 1))
		} else {
			switch v := modeVal.(type) {
			case int32:
				resultCols = append(resultCols, series.NewInt32Series(col.Name(), []int32{v}))
			case int64:
				resultCols = append(resultCols, series.NewInt64Series(col.Name(), []int64{v}))
			case float64:
				resultCols = append(resultCols, series.NewFloat64Series(col.Name(), []float64{v}))
			case float32:
				resultCols = append(resultCols, series.NewFloat32Series(col.Name(), []float32{v}))
			case string:
				resultCols = append(resultCols, series.NewStringSeries(col.Name(), []string{v}))
			case bool:
				resultCols = append(resultCols, series.NewBooleanSeries(col.Name(), []bool{v}))
			default:
				fv := toFloat64Value(modeVal)
				resultCols = append(resultCols, series.NewFloat64Series(col.Name(), []float64{fv}))
			}
		}
	}

	if len(resultCols) == 0 {
		return NewDataFrame()
	}
	return NewDataFrame(resultCols...)
}

// Skew calculates the skewness of numeric columns
func (df *DataFrame) Skew(axis int, skipNA bool) (*DataFrame, error) {
	var resultCols []series.Series
	for _, col := range df.columns {
		if !isNumericType(col.DataType()) {
			continue
		}
		skew, err := calculateSkewness(col, skipNA)
		if err != nil {
			skew = math.NaN()
		}
		resultCols = append(resultCols, series.NewFloat64Series(col.Name(), []float64{skew}))
	}

	if len(resultCols) == 0 {
		return NewDataFrame()
	}
	return NewDataFrame(resultCols...)
}

// Kurtosis calculates the kurtosis of numeric columns
func (df *DataFrame) Kurtosis(axis int, skipNA bool) (*DataFrame, error) {
	var resultCols []series.Series
	for _, col := range df.columns {
		if !isNumericType(col.DataType()) {
			continue
		}
		kurt, err := calculateKurtosis(col, skipNA)
		if err != nil {
			kurt = math.NaN()
		}
		resultCols = append(resultCols, series.NewFloat64Series(col.Name(), []float64{kurt}))
	}

	if len(resultCols) == 0 {
		return NewDataFrame()
	}
	return NewDataFrame(resultCols...)
}

// Helper function to calculate mode
func calculateMode(s series.Series, dropNaN bool) interface{} {
	counts := make(map[string]int)
	valueMap := make(map[string]interface{})

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			continue
		}
		v := s.Get(i)
		if dropNaN {
			if fv, ok := v.(float64); ok && math.IsNaN(fv) {
				continue
			}
		}
		key := s.GetAsString(i)
		counts[key]++
		valueMap[key] = v
	}

	if len(counts) == 0 {
		return nil
	}

	var maxKey string
	maxCount := 0
	for key, count := range counts {
		if count > maxCount {
			maxCount = count
			maxKey = key
		}
	}

	return valueMap[maxKey]
}

// Helper function to calculate skewness
func calculateSkewness(s series.Series, skipNA bool) (float64, error) {
	var values []float64
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			continue
		}
		values = append(values, toFloat64Value(s.Get(i)))
	}

	n := float64(len(values))
	if n < 3 {
		return math.NaN(), fmt.Errorf("need at least 3 values for skewness")
	}

	// Calculate mean
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / n

	// Calculate moments
	var m2, m3 float64
	for _, v := range values {
		d := v - mean
		m2 += d * d
		m3 += d * d * d
	}
	m2 /= n
	m3 /= n

	if m2 == 0 {
		return 0, nil
	}

	skew := m3 / math.Pow(m2, 1.5)

	// Apply bias correction (sample skewness)
	skew = skew * math.Sqrt(n*(n-1)) / (n - 2)

	return skew, nil
}

// Helper function to calculate kurtosis
func calculateKurtosis(s series.Series, skipNA bool) (float64, error) {
	var values []float64
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			continue
		}
		values = append(values, toFloat64Value(s.Get(i)))
	}

	n := float64(len(values))
	if n < 4 {
		return math.NaN(), fmt.Errorf("need at least 4 values for kurtosis")
	}

	// Calculate mean
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / n

	// Calculate moments
	var m2, m4 float64
	for _, v := range values {
		d := v - mean
		d2 := d * d
		m2 += d2
		m4 += d2 * d2
	}
	m2 /= n
	m4 /= n

	if m2 == 0 {
		return 0, nil
	}

	// Excess kurtosis (subtract 3 for normal distribution)
	kurt := m4/(m2*m2) - 3.0

	// Apply bias correction (sample kurtosis)
	kurt = ((n-1)/((n-2)*(n-3)))*((n+1)*kurt+6) + 0
	// Simplified Fisher correction
	kurt = ((n + 1) * (m4/(m2*m2) - 3.0) + 6) * (n - 1) / ((n - 2) * (n - 3))

	return kurt, nil
}

// ValueCounts returns a DataFrame with unique values and their counts
func (df *DataFrame) ValueCounts(columns []string, normalize bool, sortCounts bool, ascending bool, dropNaN bool) (*DataFrame, error) {
	if len(columns) == 0 {
		columns = df.Columns()
	}

	if len(columns) == 1 {
		col, err := df.Column(columns[0])
		if err != nil {
			return nil, err
		}
		return valueCountsSingle(col, normalize, sortCounts, ascending, dropNaN)
	}

	// For multiple columns, use groupby
	gb, err := df.GroupBy(columns...)
	if err != nil {
		return nil, err
	}
	return gb.Count()
}

// Helper function for single column value counts
func valueCountsSingle(s series.Series, normalize bool, sortCounts bool, ascending bool, dropNaN bool) (*DataFrame, error) {
	counts := make(map[string]int64)
	valueMap := make(map[string]interface{})

	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			if dropNaN {
				continue
			}
			counts["null"]++
			valueMap["null"] = nil
			continue
		}
		key := s.GetAsString(i)
		counts[key]++
		valueMap[key] = s.Get(i)
	}

	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}

	if sortCounts {
		sort.Slice(keys, func(i, j int) bool {
			if ascending {
				return counts[keys[i]] < counts[keys[j]]
			}
			return counts[keys[i]] > counts[keys[j]]
		})
	}

	// Build value and count series
	values := make([]interface{}, len(keys))
	countVals := make([]float64, len(keys))
	validity := make([]bool, len(keys))
	total := float64(0)
	for _, c := range counts {
		total += float64(c)
	}

	for i, key := range keys {
		values[i] = valueMap[key]
		validity[i] = values[i] != nil
		if normalize && total > 0 {
			countVals[i] = float64(counts[key]) / total
		} else {
			countVals[i] = float64(counts[key])
		}
	}

	valSeries := createSeriesFromInterface(s.Name(), values, validity, s.DataType())
	countName := "count"
	countSeries := series.NewFloat64Series(countName, countVals)

	return NewDataFrame(valSeries, countSeries)
}

// NUnique returns the number of unique values in each column
func (df *DataFrame) NUnique(axis int, dropNaN bool) (*DataFrame, error) {
	var resultCols []series.Series
	for _, col := range df.columns {
		n := countUnique(col, dropNaN)
		resultCols = append(resultCols, series.NewInt64Series(col.Name(), []int64{n}))
	}
	return NewDataFrame(resultCols...)
}

// Helper function to count unique values
func countUnique(s series.Series, dropNaN bool) int64 {
	seen := make(map[string]bool)
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			if !dropNaN {
				seen["null"] = true
			}
			continue
		}
		seen[s.GetAsString(i)] = true
	}
	return int64(len(seen))
}

// RankOptions configures rank calculations
type RankOptions struct {
	Method    string   // Method: "average", "min", "max", "dense", "ordinal"
	Ascending bool     // Sort ascending (true) or descending (false)
	NaOption  string   // How to handle NaN: "keep", "top", "bottom"
	Pct       bool     // Whether to return percentile ranks
	Columns   []string // Specific columns to rank
}

// Rank assigns ranks to entries
func (df *DataFrame) Rank(options RankOptions) (*DataFrame, error) {
	if options.Method == "" {
		options.Method = "average"
	}

	colSet := make(map[string]bool)
	if len(options.Columns) > 0 {
		for _, name := range options.Columns {
			colSet[name] = true
		}
	}

	newCols := make([]series.Series, len(df.columns))
	for i, col := range df.columns {
		shouldRank := false
		if len(options.Columns) == 0 {
			shouldRank = isNumericType(col.DataType())
		} else {
			shouldRank = colSet[col.Name()]
		}

		if shouldRank {
			newCols[i] = rankSeries(col, options)
		} else {
			newCols[i] = col
		}
	}

	return NewDataFrame(newCols...)
}

type indexValuePair struct {
	index int
	value float64
	isNull bool
}

// Helper function to rank a single series
func rankSeries(s series.Series, options RankOptions) series.Series {
	n := s.Len()
	pairs := make([]indexValuePair, n)
	for i := 0; i < n; i++ {
		pairs[i].index = i
		if s.IsNull(i) {
			pairs[i].isNull = true
		} else {
			pairs[i].value = toFloat64Value(s.Get(i))
		}
	}

	// Sort pairs
	sort.SliceStable(pairs, func(i, j int) bool {
		if pairs[i].isNull && pairs[j].isNull {
			return false
		}
		if pairs[i].isNull {
			return options.NaOption == "top"
		}
		if pairs[j].isNull {
			return options.NaOption != "top"
		}
		if options.Ascending {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].value > pairs[j].value
	})

	ranks := make([]float64, n)
	validity := make([]bool, n)

	switch options.Method {
	case "average":
		i := 0
		for i < n {
			if pairs[i].isNull {
				if options.NaOption == "keep" {
					validity[pairs[i].index] = false
				} else {
					ranks[pairs[i].index] = float64(i + 1)
					validity[pairs[i].index] = true
				}
				i++
				continue
			}
			j := i
			for j < n && !pairs[j].isNull && pairs[j].value == pairs[i].value {
				j++
			}
			avgRank := float64(i+j+1) / 2.0
			for k := i; k < j; k++ {
				ranks[pairs[k].index] = avgRank
				validity[pairs[k].index] = true
			}
			i = j
		}
	case "min":
		i := 0
		for i < n {
			if pairs[i].isNull {
				if options.NaOption == "keep" {
					validity[pairs[i].index] = false
				} else {
					ranks[pairs[i].index] = float64(i + 1)
					validity[pairs[i].index] = true
				}
				i++
				continue
			}
			j := i
			for j < n && !pairs[j].isNull && pairs[j].value == pairs[i].value {
				j++
			}
			for k := i; k < j; k++ {
				ranks[pairs[k].index] = float64(i + 1)
				validity[pairs[k].index] = true
			}
			i = j
		}
	case "max":
		i := 0
		for i < n {
			if pairs[i].isNull {
				if options.NaOption == "keep" {
					validity[pairs[i].index] = false
				} else {
					ranks[pairs[i].index] = float64(i + 1)
					validity[pairs[i].index] = true
				}
				i++
				continue
			}
			j := i
			for j < n && !pairs[j].isNull && pairs[j].value == pairs[i].value {
				j++
			}
			for k := i; k < j; k++ {
				ranks[pairs[k].index] = float64(j)
				validity[pairs[k].index] = true
			}
			i = j
		}
	case "dense":
		denseRank := float64(0)
		i := 0
		for i < n {
			if pairs[i].isNull {
				if options.NaOption == "keep" {
					validity[pairs[i].index] = false
				} else {
					denseRank++
					ranks[pairs[i].index] = denseRank
					validity[pairs[i].index] = true
				}
				i++
				continue
			}
			denseRank++
			j := i
			for j < n && !pairs[j].isNull && pairs[j].value == pairs[i].value {
				j++
			}
			for k := i; k < j; k++ {
				ranks[pairs[k].index] = denseRank
				validity[pairs[k].index] = true
			}
			i = j
		}
	case "ordinal":
		for i := 0; i < n; i++ {
			if pairs[i].isNull {
				if options.NaOption == "keep" {
					validity[pairs[i].index] = false
				} else {
					ranks[pairs[i].index] = float64(i + 1)
					validity[pairs[i].index] = true
				}
			} else {
				ranks[pairs[i].index] = float64(i + 1)
				validity[pairs[i].index] = true
			}
		}
	}

	// Convert to percentile ranks if requested
	if options.Pct {
		maxRank := float64(0)
		for _, r := range ranks {
			if r > maxRank {
				maxRank = r
			}
		}
		if maxRank > 0 {
			for i := range ranks {
				if validity[i] {
					ranks[i] = ranks[i] / maxRank
				}
			}
		}
	}

	return series.NewSeriesWithValidity(s.Name(), ranks, validity, datatypes.Float64{})
}
