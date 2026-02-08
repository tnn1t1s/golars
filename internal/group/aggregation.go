package group

import (
	"fmt"
	"math"
	"sort"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
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
	// Try arrow path first
	if result, ok, err := gb.tryAggArrow(aggregations); ok {
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// Try typed path
	if result, ok, err := gb.tryAggTyped(aggregations); ok {
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// Fallback to generic path
	aggResult := &AggregationResult{
		GroupKeys: gb.groupKeys,
		Results:   make(map[string][]interface{}),
		DataTypes: make(map[string]datatypes.DataType),
	}

	// Initialize result slices
	for colName := range aggregations {
		aggResult.Results[colName] = make([]interface{}, 0, len(gb.groupOrder))
	}

	// Process each group in order
	for _, hash := range gb.groupOrder {
		indices := gb.groups[hash]
		for colName, aggExpr := range aggregations {
			if err := gb.applyAggregation(aggResult, hash, indices, colName, aggExpr); err != nil {
				return nil, err
			}
		}
	}

	return gb.buildResultDataFrame(aggResult)
}

// Sum performs sum aggregation on specified columns
func (gb *GroupBy) Sum(columns ...string) (*AggResult, error) {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		if _, err := gb.df.Column(col); err != nil {
			return nil, fmt.Errorf("column %s not found", col)
		}
		aggs[col+"_sum"] = expr.Col(col).Sum()
	}
	return gb.Agg(aggs)
}

// Mean performs mean aggregation on specified columns
func (gb *GroupBy) Mean(columns ...string) (*AggResult, error) {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		if _, err := gb.df.Column(col); err != nil {
			return nil, fmt.Errorf("column %s not found", col)
		}
		aggs[col+"_mean"] = expr.Col(col).Mean()
	}
	return gb.Agg(aggs)
}

// Count returns the count of rows in each group
func (gb *GroupBy) Count() (*AggResult, error) {
	// Build key columns
	var columns []series.Series
	for _, colName := range gb.groupCols {
		col, err := gb.df.Column(colName)
		if err != nil {
			return nil, err
		}
		keyValues := make([]interface{}, len(gb.groupOrder))
		for i, hash := range gb.groupOrder {
			keys := gb.groupKeys[hash]
			idx := 0
			for j, name := range gb.groupCols {
				if name == colName {
					idx = j
					break
				}
			}
			keyValues[i] = keys[idx]
		}
		columns = append(columns, createSeriesFromInterface(colName, keyValues, col.DataType()))
	}

	// Build count column
	counts := make([]int64, len(gb.groupOrder))
	for i, hash := range gb.groupOrder {
		counts[i] = int64(len(gb.groups[hash]))
	}
	columns = append(columns, series.NewInt64Series("count", counts))

	return &AggResult{Columns: columns}, nil
}

// Min performs min aggregation on specified columns
func (gb *GroupBy) Min(columns ...string) (*AggResult, error) {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		if _, err := gb.df.Column(col); err != nil {
			return nil, fmt.Errorf("column %s not found", col)
		}
		aggs[col+"_min"] = expr.Col(col).Min()
	}
	return gb.Agg(aggs)
}

// Max performs max aggregation on specified columns
func (gb *GroupBy) Max(columns ...string) (*AggResult, error) {
	aggs := make(map[string]expr.Expr)
	for _, col := range columns {
		if _, err := gb.df.Column(col); err != nil {
			return nil, fmt.Errorf("column %s not found", col)
		}
		aggs[col+"_max"] = expr.Col(col).Max()
	}
	return gb.Agg(aggs)
}

// applyAggregation applies a single aggregation to a group
func (gb *GroupBy) applyAggregation(result *AggregationResult, hash uint64,
	indices []int, colName string, aggExpr expr.Expr) error {

	val, dtype, err := gb.evaluateAggExpr(indices, aggExpr)
	if err != nil {
		return err
	}
	result.Results[colName] = append(result.Results[colName], val)
	result.DataTypes[colName] = dtype
	return nil
}

// evaluateAggExpr recursively evaluates an aggregation expression
func (gb *GroupBy) evaluateAggExpr(indices []int, e expr.Expr) (interface{}, datatypes.DataType, error) {
	switch ex := e.(type) {
	case *expr.AggExpr:
		return gb.evaluateSimpleAgg(indices, ex)
	case *expr.TopKExpr:
		return gb.evaluateTopK(indices, ex)
	case *expr.CorrExpr:
		return gb.evaluateCorr(indices, ex)
	case *expr.BinaryExpr:
		// Handle arithmetic on aggregations (e.g., Max() - Min())
		leftVal, leftDtype, err := gb.evaluateAggExpr(indices, ex.Left())
		if err != nil {
			return nil, datatypes.Unknown{}, err
		}
		rightVal, rightDtype, err := gb.evaluateAggExpr(indices, ex.Right())
		if err != nil {
			return nil, datatypes.Unknown{}, err
		}
		result := applyBinaryOp(ex.Op(), leftVal, rightVal)
		// Use left type unless it's unknown
		dtype := leftDtype
		if _, ok := dtype.(datatypes.Unknown); ok {
			dtype = rightDtype
		}
		return result, dtype, nil
	case *expr.AliasExpr:
		return gb.evaluateAggExpr(indices, ex.Expr())
	default:
		return nil, datatypes.Unknown{}, fmt.Errorf("unsupported expression type: %T", e)
	}
}

func sameCorr(left, right *expr.CorrExpr) bool {
	return left.Col1().Name() == right.Col1().Name() &&
		left.Col2().Name() == right.Col2().Name()
}

// evaluateSimpleAgg evaluates a simple aggregation expression
func (gb *GroupBy) evaluateSimpleAgg(indices []int, agg *expr.AggExpr) (interface{}, datatypes.DataType, error) {
	// Get the column name from the input expression
	colExpr, ok := agg.Input().(*expr.ColumnExpr)
	if !ok {
		return nil, datatypes.Unknown{}, fmt.Errorf("aggregation input must be a column reference")
	}

	col, err := gb.df.Column(colExpr.Name())
	if err != nil {
		return nil, datatypes.Unknown{}, err
	}

	// Gather values for this group
	values := make([]interface{}, len(indices))
	for i, idx := range indices {
		if col.IsNull(idx) {
			values[i] = nil
		} else {
			values[i] = col.Get(idx)
		}
	}

	dtype := col.DataType()

	switch agg.AggType() {
	case expr.AggSum:
		return computeSum(values, dtype), dtype, nil
	case expr.AggMean:
		return computeMean(values, dtype), datatypes.Float64{}, nil
	case expr.AggMin:
		return computeMin(values, dtype), dtype, nil
	case expr.AggMax:
		return computeMax(values, dtype), dtype, nil
	case expr.AggCount:
		count := int64(0)
		for _, v := range values {
			if v != nil {
				count++
			}
		}
		return count, datatypes.Int64{}, nil
	case expr.AggStd:
		return computeStd(values, dtype, 1), datatypes.Float64{}, nil
	case expr.AggVar:
		return computeVar(values, dtype, 1), datatypes.Float64{}, nil
	case expr.AggFirst:
		return computeFirst(values, dtype), dtype, nil
	case expr.AggLast:
		return computeLast(values, dtype), dtype, nil
	case expr.AggMedian:
		return computeMedian(values, dtype), datatypes.Float64{}, nil
	default:
		return nil, datatypes.Unknown{}, fmt.Errorf("unsupported aggregation: %v", agg.AggType())
	}
}

// evaluateTopK evaluates a top-k aggregation
func (gb *GroupBy) evaluateTopK(indices []int, topk *expr.TopKExpr) (interface{}, datatypes.DataType, error) {
	colExpr, ok := topk.Input().(*expr.ColumnExpr)
	if !ok {
		return nil, datatypes.Unknown{}, fmt.Errorf("topk input must be a column reference")
	}

	col, err := gb.df.Column(colExpr.Name())
	if err != nil {
		return nil, datatypes.Unknown{}, err
	}

	// Extract and sort values
	var floatVals []float64
	for _, idx := range indices {
		if !col.IsNull(idx) {
			floatVals = append(floatVals, toFloat64(col.Get(idx)))
		}
	}

	// Sort in appropriate order
	if topk.IsLargest() {
		sort.Float64s(floatVals)
		// Reverse for largest
		for i, j := 0, len(floatVals)-1; i < j; i, j = i+1, j-1 {
			floatVals[i], floatVals[j] = floatVals[j], floatVals[i]
		}
	} else {
		sort.Float64s(floatVals)
	}

	// Take top k
	k := topk.K()
	if k > len(floatVals) {
		k = len(floatVals)
	}
	result := floatVals[:k]

	return result, datatypes.Float64{}, nil
}

// evaluateCorr evaluates a correlation between two columns
func (gb *GroupBy) evaluateCorr(indices []int, corr *expr.CorrExpr) (interface{}, datatypes.DataType, error) {
	col1, err := gb.df.Column(corr.Col1().Name())
	if err != nil {
		return nil, datatypes.Unknown{}, err
	}
	col2, err := gb.df.Column(corr.Col2().Name())
	if err != nil {
		return nil, datatypes.Unknown{}, err
	}

	// Extract paired values (skip if either is null)
	var xVals, yVals []float64
	for _, idx := range indices {
		if !col1.IsNull(idx) && !col2.IsNull(idx) {
			xVals = append(xVals, toFloat64(col1.Get(idx)))
			yVals = append(yVals, toFloat64(col2.Get(idx)))
		}
	}

	if len(xVals) < 2 {
		return nil, datatypes.Float64{}, nil
	}

	// Compute Pearson correlation
	r := computeCorrelation(xVals, yVals)
	return r, datatypes.Float64{}, nil
}

// computeCorrelation computes Pearson correlation coefficient
func computeCorrelation(x, y []float64) float64 {
	n := len(x)
	if n < 2 {
		return 0
	}

	// Calculate means
	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += x[i]
		sumY += y[i]
	}
	meanX := sumX / float64(n)
	meanY := sumY / float64(n)

	// Calculate covariance and standard deviations
	var cov, varX, varY float64
	for i := 0; i < n; i++ {
		dx := x[i] - meanX
		dy := y[i] - meanY
		cov += dx * dy
		varX += dx * dx
		varY += dy * dy
	}

	denom := math.Sqrt(varX * varY)
	if denom == 0 {
		return 0
	}
	return cov / denom
}

// applyBinaryOp applies a binary operation to two values
func applyBinaryOp(op expr.BinaryOp, left, right interface{}) interface{} {
	lf := toFloat64(left)
	rf := toFloat64(right)

	switch op {
	case expr.OpAdd:
		return lf + rf
	case expr.OpSubtract:
		return lf - rf
	case expr.OpMultiply:
		return lf * rf
	case expr.OpDivide:
		if rf == 0 {
			return math.NaN()
		}
		return lf / rf
	case expr.OpModulo:
		if rf == 0 {
			return math.NaN()
		}
		return math.Mod(lf, rf)
	default:
		return nil
	}
}

// buildResultDataFrame builds the final DataFrame from aggregation results
func (gb *GroupBy) buildResultDataFrame(result *AggregationResult) (*AggResult, error) {
	var columns []series.Series

	// Add group columns in order
	for colIdx, colName := range gb.groupCols {
		// Get original column to determine type
		col, err := gb.df.Column(colName)
		if err != nil {
			return nil, err
		}

		keyValues := make([]interface{}, len(gb.groupOrder))
		for i, hash := range gb.groupOrder {
			keys := gb.groupKeys[hash]
			keyValues[i] = keys[colIdx]
		}
		columns = append(columns, createSeriesFromInterface(colName, keyValues, col.DataType()))
	}

	// Add aggregation result columns in a stable order
	// We need deterministic ordering of aggregation columns
	aggNames := make([]string, 0, len(result.Results))
	for name := range result.Results {
		aggNames = append(aggNames, name)
	}
	sort.Strings(aggNames)

	for _, colName := range aggNames {
		values := result.Results[colName]
		dtype := result.DataTypes[colName]
		columns = append(columns, createSeriesFromInterface(colName, values, dtype))
	}

	return &AggResult{Columns: columns}, nil
}

// Aggregation compute functions

func computeSum(values []interface{}, dtype datatypes.DataType) interface{} {
	switch dtype.(type) {
	case datatypes.Int8:
		var sum int8
		for _, v := range values {
			if v != nil {
				sum += v.(int8)
			}
		}
		return sum
	case datatypes.Int16:
		var sum int16
		for _, v := range values {
			if v != nil {
				sum += v.(int16)
			}
		}
		return sum
	case datatypes.Int32:
		var sum int32
		for _, v := range values {
			if v != nil {
				sum += v.(int32)
			}
		}
		return sum
	case datatypes.Int64:
		var sum int64
		for _, v := range values {
			if v != nil {
				sum += v.(int64)
			}
		}
		return sum
	case datatypes.UInt8:
		var sum uint8
		for _, v := range values {
			if v != nil {
				sum += v.(uint8)
			}
		}
		return sum
	case datatypes.UInt16:
		var sum uint16
		for _, v := range values {
			if v != nil {
				sum += v.(uint16)
			}
		}
		return sum
	case datatypes.UInt32:
		var sum uint32
		for _, v := range values {
			if v != nil {
				sum += v.(uint32)
			}
		}
		return sum
	case datatypes.UInt64:
		var sum uint64
		for _, v := range values {
			if v != nil {
				sum += v.(uint64)
			}
		}
		return sum
	case datatypes.Float32:
		var sum float32
		for _, v := range values {
			if v != nil {
				sum += v.(float32)
			}
		}
		return sum
	case datatypes.Float64:
		var sum float64
		for _, v := range values {
			if v != nil {
				sum += v.(float64)
			}
		}
		return sum
	default:
		return nil
	}
}

func computeMean(values []interface{}, dtype datatypes.DataType) interface{} {
	var sum float64
	count := 0
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
	var result interface{}
	for _, v := range values {
		if v == nil {
			continue
		}
		if result == nil || compareValues(v, result, dtype) < 0 {
			result = v
		}
	}
	return result
}

func computeMax(values []interface{}, dtype datatypes.DataType) interface{} {
	var result interface{}
	for _, v := range values {
		if v == nil {
			continue
		}
		if result == nil || compareValues(v, result, dtype) > 0 {
			result = v
		}
	}
	return result
}

func computeMedian(values []interface{}, dtype datatypes.DataType) interface{} {
	// Filter out nil values and convert to float64
	var floats []float64
	for _, v := range values {
		if v != nil {
			floats = append(floats, toFloat64(v))
		}
	}

	if len(floats) == 0 {
		return nil
	}

	// Sort the values
	sort.Float64s(floats)

	n := len(floats)
	// Even number of values: average of two middle values
	if n%2 == 0 {
		return (floats[n/2-1] + floats[n/2]) / 2.0
	}

	// Odd number of values: middle value
	return floats[n/2]
}

func computeVar(values []interface{}, dtype datatypes.DataType, ddof int) interface{} {
	// Filter out nil values and convert to float64
	var floats []float64
	for _, v := range values {
		if v != nil {
			floats = append(floats, toFloat64(v))
		}
	}

	if len(floats) <= ddof {
		return nil
	}

	// Calculate mean
	var sum float64
	for _, f := range floats {
		sum += f
	}
	mean := sum / float64(len(floats))

	// Calculate variance
	var variance float64
	for _, f := range floats {
		diff := f - mean
		variance += diff * diff
	}
	return variance / float64(len(floats)-ddof)
}

func computeStd(values []interface{}, dtype datatypes.DataType, ddof int) interface{} {
	v := computeVar(values, dtype, ddof)
	if v == nil {
		return nil
	}
	return math.Sqrt(v.(float64))
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
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case float32:
		return int64(val)
	case float64:
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
	case int8:
		return uint64(val)
	case int16:
		return uint64(val)
	case int32:
		return uint64(val)
	case int64:
		return uint64(val)
	case float32:
		return uint64(val)
	case float64:
		return uint64(val)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
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
		return 0
	}
}

func compareValues(a, b interface{}, dtype datatypes.DataType) int {
	switch dtype.(type) {
	case datatypes.Int8:
		av, bv := a.(int8), b.(int8)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case datatypes.Int16:
		av, bv := a.(int16), b.(int16)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case datatypes.Int32:
		av, bv := a.(int32), b.(int32)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case datatypes.Int64:
		av, bv := a.(int64), b.(int64)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case datatypes.UInt8:
		av, bv := a.(uint8), b.(uint8)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case datatypes.UInt16:
		av, bv := a.(uint16), b.(uint16)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case datatypes.UInt32:
		av, bv := a.(uint32), b.(uint32)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case datatypes.UInt64:
		av, bv := a.(uint64), b.(uint64)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case datatypes.Float32:
		av, bv := a.(float32), b.(float32)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case datatypes.Float64:
		av, bv := a.(float64), b.(float64)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case datatypes.String:
		av, bv := a.(string), b.(string)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	default:
		// Fallback: convert to float64
		af := toFloat64(a)
		bf := toFloat64(b)
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
		return 0
	}
}

func convertToType(v interface{}, dtype datatypes.DataType) interface{} {
	switch dtype.(type) {
	case datatypes.Int8:
		return int8(toInt64(v))
	case datatypes.Int16:
		return int16(toInt64(v))
	case datatypes.Int32:
		return int32(toInt64(v))
	case datatypes.Int64:
		return toInt64(v)
	case datatypes.UInt8:
		return uint8(toUint64(v))
	case datatypes.UInt16:
		return uint16(toUint64(v))
	case datatypes.UInt32:
		return uint32(toUint64(v))
	case datatypes.UInt64:
		return toUint64(v)
	case datatypes.Float32:
		return float32(toFloat64(v))
	case datatypes.Float64:
		return toFloat64(v)
	default:
		return v
	}
}

// createSeriesFromInterface creates a series from a slice of interface{} values
func createSeriesFromInterface(name string, values []interface{}, dtype datatypes.DataType) series.Series {
	switch dtype.(type) {
	case datatypes.Int8:
		typed := make([]int8, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(int8)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.Int16:
		typed := make([]int16, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(int16)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.Int32:
		typed := make([]int32, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(int32)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.Int64:
		typed := make([]int64, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(int64)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.UInt8:
		typed := make([]uint8, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(uint8)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.UInt16:
		typed := make([]uint16, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(uint16)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.UInt32:
		typed := make([]uint32, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(uint32)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.UInt64:
		typed := make([]uint64, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(uint64)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.Float32:
		typed := make([]float32, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(float32)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.Float64:
		typed := make([]float64, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(float64)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.String:
		typed := make([]string, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(string)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	case datatypes.Boolean:
		typed := make([]bool, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = v.(bool)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, dtype)
	default:
		// Check if this is a list of float64 slices (e.g., from TopK)
		if len(values) > 0 {
			if _, ok := values[0].([]float64); ok {
				// This is a list column - store slices as interface{}
				return series.NewInterfaceSeries(name, values, nil, dtype)
			}
		}
		// Fallback - create string series
		typed := make([]string, len(values))
		validity := make([]bool, len(values))
		for i, v := range values {
			if v != nil {
				typed[i] = fmt.Sprintf("%v", v)
				validity[i] = true
			}
		}
		return series.NewSeriesWithValidity(name, typed, validity, datatypes.String{})
	}
}
