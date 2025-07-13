package group

import (
	"fmt"
	"math"
	"sort"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/series"
)

// AggregationResult holds the results of aggregation operations
type AggregationResult struct {
	GroupKeys map[uint64][]interface{}
	Results   map[string][]interface{}
	DataTypes map[string]datatypes.DataType
}

// AggResult represents the result of an aggregation operation
type AggResult struct {
	Columns []series.Series
}

// Agg performs multiple aggregations on the grouped data
func (gb *GroupBy) Agg(aggregations map[string]expr.Expr) (*AggResult, error) {
	gb.mu.RLock()
	defer gb.mu.RUnlock()

	result := &AggregationResult{
		GroupKeys: gb.groupKeys,
		Results:   make(map[string][]interface{}),
		DataTypes: make(map[string]datatypes.DataType),
	}

	// Initialize result slices
	for colName := range aggregations {
		result.Results[colName] = make([]interface{}, 0, len(gb.groups))
	}

	// Process each group
	for hash, indices := range gb.groups {
		// Apply aggregations
		for colName, aggExpr := range aggregations {
			if err := gb.applyAggregation(result, hash, indices, colName, aggExpr); err != nil {
				return nil, err
			}
		}
	}

	// Build result DataFrame
	return gb.buildResultDataFrame(result)
}

// Sum performs sum aggregation on specified columns
func (gb *GroupBy) Sum(columns ...string) (*AggResult, error) {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		aggs[col+"_sum"] = expr.ColBuilder(col).Sum().Build()
	}
	return gb.Agg(aggs)
}

// Mean performs mean aggregation on specified columns
func (gb *GroupBy) Mean(columns ...string) (*AggResult, error) {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		aggs[col+"_mean"] = expr.ColBuilder(col).Mean().Build()
	}
	return gb.Agg(aggs)
}

// Count returns the count of rows in each group
func (gb *GroupBy) Count() (*AggResult, error) {
	gb.mu.RLock()
	defer gb.mu.RUnlock()

	// Create result columns
	resultCols := make([]series.Series, 0, len(gb.groupCols)+1)

	// Add group columns
	for i, groupCol := range gb.groupCols {
		values := make([]interface{}, 0, len(gb.groups))
		for _, keyValues := range gb.groupKeys {
			values = append(values, keyValues[i])
		}

		// Get original column to determine type
		origCol, _ := gb.df.Column(groupCol)
		s := createSeriesFromInterface(groupCol, values, origCol.DataType())
		resultCols = append(resultCols, s)
	}

	// Add count column
	counts := make([]int64, 0, len(gb.groups))
	for _, indices := range gb.groups {
		counts = append(counts, int64(len(indices)))
	}
	countSeries := series.NewInt64Series("count", counts)
	resultCols = append(resultCols, countSeries)

	return &AggResult{Columns: resultCols}, nil
}

// Min performs min aggregation on specified columns
func (gb *GroupBy) Min(columns ...string) (*AggResult, error) {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		aggs[col+"_min"] = expr.ColBuilder(col).Min().Build()
	}
	return gb.Agg(aggs)
}

// Max performs max aggregation on specified columns
func (gb *GroupBy) Max(columns ...string) (*AggResult, error) {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		aggs[col+"_max"] = expr.ColBuilder(col).Max().Build()
	}
	return gb.Agg(aggs)
}

// applyAggregation applies a single aggregation to a group
func (gb *GroupBy) applyAggregation(result *AggregationResult, hash uint64,
	indices []int, colName string, aggExpr expr.Expr) error {

	// Extract the column being aggregated from the expression
	var targetCol string
	switch e := aggExpr.(type) {
	case *expr.AggExpr:
		if colExpr, ok := e.Input().(*expr.ColumnExpr); ok {
			targetCol = colExpr.Name()
		}
	default:
		return fmt.Errorf("unsupported aggregation expression type: %T", aggExpr)
	}

	// Get the column to aggregate
	col, err := gb.df.Column(targetCol)
	if err != nil {
		return fmt.Errorf("column %s not found", targetCol)
	}

	// Extract values for this group
	groupValues := make([]interface{}, len(indices))
	for i, idx := range indices {
		groupValues[i] = col.Get(idx)
	}

	// Apply aggregation based on expression type
	var aggResult interface{}
	if agg, ok := aggExpr.(*expr.AggExpr); ok {
		switch agg.AggType() {
		case expr.AggSum:
			aggResult = computeSum(groupValues, col.DataType())
			result.DataTypes[colName] = col.DataType() // Sum preserves type
		case expr.AggMean:
			aggResult = computeMean(groupValues, col.DataType())
			result.DataTypes[colName] = datatypes.Float64{} // Mean always returns float64
		case expr.AggMin:
			aggResult = computeMin(groupValues, col.DataType())
			result.DataTypes[colName] = col.DataType() // Min preserves type
		case expr.AggMax:
			aggResult = computeMax(groupValues, col.DataType())
			result.DataTypes[colName] = col.DataType() // Max preserves type
		case expr.AggCount:
			aggResult = int64(len(indices))
			result.DataTypes[colName] = datatypes.Int64{}
		case expr.AggMedian:
			aggResult = computeMedian(groupValues, col.DataType())
			result.DataTypes[colName] = datatypes.Float64{} // Median always returns float64
		case expr.AggStd:
			aggResult = computeStd(groupValues, col.DataType(), 1) // Sample standard deviation (ddof=1)
			result.DataTypes[colName] = datatypes.Float64{} // Std always returns float64
		case expr.AggVar:
			aggResult = computeVar(groupValues, col.DataType(), 1) // Sample variance (ddof=1)
			result.DataTypes[colName] = datatypes.Float64{} // Var always returns float64
		case expr.AggFirst:
			aggResult = computeFirst(groupValues, col.DataType())
			result.DataTypes[colName] = col.DataType() // First preserves type
		case expr.AggLast:
			aggResult = computeLast(groupValues, col.DataType())
			result.DataTypes[colName] = col.DataType() // Last preserves type
		default:
			return fmt.Errorf("unsupported aggregation type: %v", agg.AggType())
		}
	}

	// Store result
	result.Results[colName] = append(result.Results[colName], aggResult)

	return nil
}

// buildResultDataFrame builds the final DataFrame from aggregation results
func (gb *GroupBy) buildResultDataFrame(result *AggregationResult) (*AggResult, error) {
	columns := make([]series.Series, 0)

	// Add group columns
	for i, groupCol := range gb.groupCols {
		values := make([]interface{}, 0, len(result.GroupKeys))
		for _, key := range result.GroupKeys {
			values = append(values, key[i])
		}

		// Get original column to determine type
		origCol, _ := gb.df.Column(groupCol)
		s := createSeriesFromInterface(groupCol, values, origCol.DataType())
		columns = append(columns, s)
	}

	// Add aggregation result columns
	for colName, values := range result.Results {
		dtype := result.DataTypes[colName]
		s := createSeriesFromInterface(colName, values, dtype)
		columns = append(columns, s)
	}

	return &AggResult{Columns: columns}, nil
}

// Aggregation compute functions

func computeSum(values []interface{}, dtype datatypes.DataType) interface{} {
	if dtype.IsInteger() && dtype.IsSigned() {
		var sum int64
		for _, v := range values {
			if v != nil {
				sum += toInt64(v)
			}
		}
		return convertToType(sum, dtype)
	} else if dtype.IsInteger() && !dtype.IsSigned() {
		var sum uint64
		for _, v := range values {
			if v != nil {
				sum += toUint64(v)
			}
		}
		return convertToType(sum, dtype)
	} else if dtype.IsFloat() {
		var sum float64
		for _, v := range values {
			if v != nil {
				sum += toFloat64(v)
			}
		}
		return convertToType(sum, dtype)
	}
	return nil
}

func computeMean(values []interface{}, dtype datatypes.DataType) interface{} {
	var sum float64
	var count int
	for _, v := range values {
		if v != nil {
			sum += toFloat64(v)
			count++
		}
	}
	if count == 0 {
		return nil
	}
	return sum / float64(count)
}

func computeMin(values []interface{}, dtype datatypes.DataType) interface{} {
	var min interface{}
	for _, v := range values {
		if v != nil {
			if min == nil || compareValues(v, min, dtype) < 0 {
				min = v
			}
		}
	}
	return min
}

func computeMax(values []interface{}, dtype datatypes.DataType) interface{} {
	var max interface{}
	for _, v := range values {
		if v != nil {
			if max == nil || compareValues(v, max, dtype) > 0 {
				max = v
			}
		}
	}
	return max
}

func computeMedian(values []interface{}, dtype datatypes.DataType) interface{} {
	// Filter out nil values and convert to float64
	validValues := make([]float64, 0, len(values))
	for _, v := range values {
		if v != nil {
			validValues = append(validValues, toFloat64(v))
		}
	}
	
	if len(validValues) == 0 {
		return nil
	}
	
	// Sort the values
	sort.Float64s(validValues)
	
	// Calculate median
	n := len(validValues)
	if n%2 == 0 {
		// Even number of values: average of two middle values
		return (validValues[n/2-1] + validValues[n/2]) / 2.0
	} else {
		// Odd number of values: middle value
		return validValues[n/2]
	}
}

func computeVar(values []interface{}, dtype datatypes.DataType, ddof int) interface{} {
	// Filter out nil values and convert to float64
	validValues := make([]float64, 0, len(values))
	for _, v := range values {
		if v != nil {
			validValues = append(validValues, toFloat64(v))
		}
	}
	
	n := len(validValues)
	if n == 0 || n <= ddof {
		return nil
	}
	
	// Calculate mean
	mean := 0.0
	for _, v := range validValues {
		mean += v
	}
	mean /= float64(n)
	
	// Calculate variance
	var sumSq float64
	for _, v := range validValues {
		diff := v - mean
		sumSq += diff * diff
	}
	
	return sumSq / float64(n - ddof)
}

func computeStd(values []interface{}, dtype datatypes.DataType, ddof int) interface{} {
	variance := computeVar(values, dtype, ddof)
	if variance == nil {
		return nil
	}
	return math.Sqrt(variance.(float64))
}

func computeFirst(values []interface{}, dtype datatypes.DataType) interface{} {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

func computeLast(values []interface{}, dtype datatypes.DataType) interface{} {
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] != nil {
			return values[i]
		}
	}
	return nil
}

// Helper functions for type conversion

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case int:
		return int64(val)
	default:
		return 0
	}
}

func toUint64(v interface{}) uint64 {
	switch val := v.(type) {
	case uint8:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint32:
		return uint64(val)
	case uint64:
		return val
	case uint:
		return uint64(val)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float32:
		return float64(val)
	case float64:
		return val
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	default:
		return math.NaN()
	}
}

func compareValues(a, b interface{}, dtype datatypes.DataType) int {
	if dtype.IsInteger() && dtype.IsSigned() {
		aVal := toInt64(a)
		bVal := toInt64(b)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	} else if dtype.IsInteger() && !dtype.IsSigned() {
		aVal := toUint64(a)
		bVal := toUint64(b)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	} else if dtype.IsFloat() {
		aVal := toFloat64(a)
		bVal := toFloat64(b)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	} else if dtype.Equals(datatypes.String{}) {
		aStr := a.(string)
		bStr := b.(string)
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}
	return 0
}

func convertToType(v interface{}, dtype datatypes.DataType) interface{} {
	if dtype.Equals(datatypes.Int8{}) {
		return int8(toInt64(v))
	} else if dtype.Equals(datatypes.Int16{}) {
		return int16(toInt64(v))
	} else if dtype.Equals(datatypes.Int32{}) {
		return int32(toInt64(v))
	} else if dtype.Equals(datatypes.Int64{}) {
		return toInt64(v)
	} else if dtype.Equals(datatypes.UInt8{}) {
		return uint8(toUint64(v))
	} else if dtype.Equals(datatypes.UInt16{}) {
		return uint16(toUint64(v))
	} else if dtype.Equals(datatypes.UInt32{}) {
		return uint32(toUint64(v))
	} else if dtype.Equals(datatypes.UInt64{}) {
		return toUint64(v)
	} else if dtype.Equals(datatypes.Float32{}) {
		return float32(toFloat64(v))
	} else if dtype.Equals(datatypes.Float64{}) {
		return toFloat64(v)
	}
	return v
}

// createSeriesFromInterface creates a series from a slice of interface{} values
func createSeriesFromInterface(name string, values []interface{}, dtype datatypes.DataType) series.Series {
	if dtype.Equals(datatypes.Boolean{}) {
		data := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(bool)
			}
		}
		return series.NewBooleanSeries(name, data)
	} else if dtype.Equals(datatypes.Int8{}) {
		data := make([]int8, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(int8)
			}
		}
		return series.NewInt8Series(name, data)
	} else if dtype.Equals(datatypes.Int16{}) {
		data := make([]int16, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(int16)
			}
		}
		return series.NewInt16Series(name, data)
	} else if dtype.Equals(datatypes.Int32{}) {
		data := make([]int32, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(int32)
			}
		}
		return series.NewInt32Series(name, data)
	} else if dtype.Equals(datatypes.Int64{}) {
		data := make([]int64, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(int64)
			}
		}
		return series.NewInt64Series(name, data)
	} else if dtype.Equals(datatypes.UInt8{}) {
		data := make([]uint8, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(uint8)
			}
		}
		return series.NewUInt8Series(name, data)
	} else if dtype.Equals(datatypes.UInt16{}) {
		data := make([]uint16, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(uint16)
			}
		}
		return series.NewUInt16Series(name, data)
	} else if dtype.Equals(datatypes.UInt32{}) {
		data := make([]uint32, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(uint32)
			}
		}
		return series.NewUInt32Series(name, data)
	} else if dtype.Equals(datatypes.UInt64{}) {
		data := make([]uint64, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(uint64)
			}
		}
		return series.NewUInt64Series(name, data)
	} else if dtype.Equals(datatypes.Float32{}) {
		data := make([]float32, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(float32)
			}
		}
		return series.NewFloat32Series(name, data)
	} else if dtype.Equals(datatypes.Float64{}) {
		data := make([]float64, len(values))
		validity := make([]bool, len(values))
		hasNulls := false
		for i, v := range values {
			if v != nil {
				data[i] = v.(float64)
				validity[i] = true
			} else {
				hasNulls = true
				validity[i] = false
			}
		}
		if hasNulls {
			return series.NewSeriesWithValidity(name, data, validity, dtype)
		}
		return series.NewFloat64Series(name, data)
	} else if dtype.Equals(datatypes.String{}) {
		data := make([]string, len(values))
		for i, v := range values {
			if v != nil {
				data[i] = v.(string)
			}
		}
		return series.NewStringSeries(name, data)
	}
	// Fallback - create string series
	data := make([]string, len(values))
	for i, v := range values {
		if v != nil {
			data[i] = fmt.Sprint(v)
		}
	}
	return series.NewStringSeries(name, data)
}