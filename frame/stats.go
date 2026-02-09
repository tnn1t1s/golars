package frame

import (
	"fmt"
	"math"
	"sort"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// QuantileOptions configures quantile calculations
type QuantileOptions struct {
	Quantiles []float64 // Quantiles to compute (e.g., [0.25, 0.5, 0.75])
	Method    string    // Interpolation method: "linear", "lower", "higher", "midpoint", "nearest"
	Axis      int       // 0 for index (rows), 1 for columns
	Numeric   bool      // Only include numeric columns (default: true)
}

// Quantile calculates the quantiles of numeric columns
func (df *DataFrame) Quantile(options QuantileOptions) (*DataFrame, error) {
	if options.Method == "" {
		options.Method = "linear"
	}
	if len(options.Quantiles) == 0 {
		options.Quantiles = []float64{0.5}
	}

	for _, q := range options.Quantiles {
		if q < 0 || q > 1 {
			return nil, fmt.Errorf("quantile must be between 0 and 1, got %f", q)
		}
	}

	// Get numeric columns
	var numCols []series.Series
	for _, col := range df.columns {
		if isNumericType(col.DataType()) {
			numCols = append(numCols, col)
		}
	}

	if len(numCols) == 0 {
		return nil, fmt.Errorf("no numeric columns found")
	}

	// Create result columns
	var resultCols []series.Series

	// Add quantile column
	qVals := make([]float64, len(options.Quantiles))
	copy(qVals, options.Quantiles)
	resultCols = append(resultCols, series.NewFloat64Series("quantile", qVals))

	// Calculate quantiles for each numeric column
	allFailed := true
	for _, col := range numCols {
		vals := make([]float64, len(options.Quantiles))
		colFailed := false
		for i, q := range options.Quantiles {
			v, err := calculateQuantile(col, q, options.Method)
			if err != nil {
				vals[i] = math.NaN()
				colFailed = true
			} else {
				vals[i] = v
			}
		}
		if !colFailed {
			allFailed = false
		}
		resultCols = append(resultCols, series.NewFloat64Series(col.Name(), vals))
	}

	if allFailed {
		return nil, fmt.Errorf("no valid values in numeric columns")
	}

	return NewDataFrame(resultCols...)
}

// Percentile is a convenience method for quantile with percentages
func (df *DataFrame) Percentile(percentiles []float64, method string) (*DataFrame, error) {
	quantiles := make([]float64, len(percentiles))
	for i, p := range percentiles {
		quantiles[i] = p / 100.0
	}
	return df.Quantile(QuantileOptions{
		Quantiles: quantiles,
		Method:    method,
	})
}

// Helper function to calculate quantile for a series
func calculateQuantile(s series.Series, quantile float64, method string) (float64, error) {
	// Collect non-null values
	var values []float64
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			values = append(values, toFloat64Value(s.Get(i)))
		}
	}
	if len(values) == 0 {
		return math.NaN(), fmt.Errorf("no valid values")
	}

	sort.Float64s(values)
	n := len(values)

	if n == 1 {
		return values[0], nil
	}

	pos := quantile * float64(n-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))

	if lower < 0 {
		lower = 0
	}
	if upper >= n {
		upper = n - 1
	}

	switch method {
	case "linear":
		if lower == upper {
			return values[lower], nil
		}
		frac := pos - float64(lower)
		return values[lower] + frac*(values[upper]-values[lower]), nil
	case "lower":
		return values[lower], nil
	case "higher":
		return values[upper], nil
	case "midpoint":
		return (values[lower] + values[upper]) / 2.0, nil
	case "nearest":
		if pos-float64(lower) < float64(upper)-pos {
			return values[lower], nil
		} else if pos-float64(lower) > float64(upper)-pos {
			return values[upper], nil
		}
		// Exactly halfway: round down
		return values[lower], nil
	default:
		return 0, fmt.Errorf("unsupported quantile method: %s", method)
	}
}

// CorrelationOptions configures correlation calculations
type CorrelationOptions struct {
	Method   string   // Method: "pearson", "kendall", "spearman"
	MinValid int      // Minimum number of valid observations
	Columns  []string // Specific columns to include
}

// Correlation calculates pairwise correlation of columns
func (df *DataFrame) Correlation(options CorrelationOptions) (*DataFrame, error) {
	if options.Method == "" {
		options.Method = "pearson"
	}

	// Get columns to correlate
	var cols []series.Series
	if len(options.Columns) > 0 {
		for _, name := range options.Columns {
			col, err := df.Column(name)
			if err != nil {
				return nil, err
			}
			cols = append(cols, col)
		}
	} else {
		for _, col := range df.columns {
			if isNumericType(col.DataType()) {
				cols = append(cols, col)
			}
		}
	}

	n := len(cols)
	corrMatrix := make([][]float64, n)
	for i := range corrMatrix {
		corrMatrix[i] = make([]float64, n)
	}

	for i := 0; i < n; i++ {
		corrMatrix[i][i] = 1.0
		for j := i + 1; j < n; j++ {
			corr, err := calculateCorrelation(cols[i], cols[j], options.Method, options.MinValid)
			if err != nil {
				corr = math.NaN()
			}
			corrMatrix[i][j] = corr
			corrMatrix[j][i] = corr
		}
	}

	// Create result DataFrame
	var resultCols []series.Series

	// Index column (column names)
	names := make([]string, n)
	for i, col := range cols {
		names[i] = col.Name()
	}
	resultCols = append(resultCols, series.NewStringSeries("column", names))

	// Correlation columns
	for i, col := range cols {
		resultCols = append(resultCols, series.NewFloat64Series(col.Name(), corrMatrix[i]))
	}

	return NewDataFrame(resultCols...)
}

// Covariance calculates pairwise covariance of columns
func (df *DataFrame) Covariance(options CorrelationOptions) (*DataFrame, error) {
	if options.Method == "" {
		options.Method = "pearson"
	}

	var cols []series.Series
	if len(options.Columns) > 0 {
		for _, name := range options.Columns {
			col, err := df.Column(name)
			if err != nil {
				return nil, err
			}
			cols = append(cols, col)
		}
	} else {
		for _, col := range df.columns {
			if isNumericType(col.DataType()) {
				cols = append(cols, col)
			}
		}
	}

	n := len(cols)
	covMatrix := make([][]float64, n)
	for i := range covMatrix {
		covMatrix[i] = make([]float64, n)
	}

	for i := 0; i < n; i++ {
		for j := i; j < n; j++ {
			cov, err := calculateCovariance(cols[i], cols[j], options.MinValid)
			if err != nil {
				cov = math.NaN()
			}
			covMatrix[i][j] = cov
			covMatrix[j][i] = cov
		}
	}

	var resultCols []series.Series
	names := make([]string, n)
	for i, col := range cols {
		names[i] = col.Name()
	}
	resultCols = append(resultCols, series.NewStringSeries("column", names))

	for i, col := range cols {
		resultCols = append(resultCols, series.NewFloat64Series(col.Name(), covMatrix[i]))
	}

	return NewDataFrame(resultCols...)
}

// Helper function to calculate Pearson correlation
func calculateCorrelation(s1, s2 series.Series, method string, minValid int) (float64, error) {
	// Collect paired non-null values
	var x, y []float64
	n := s1.Len()
	if s2.Len() < n {
		n = s2.Len()
	}
	for i := 0; i < n; i++ {
		if !s1.IsNull(i) && !s2.IsNull(i) {
			x = append(x, toFloat64Value(s1.Get(i)))
			y = append(y, toFloat64Value(s2.Get(i)))
		}
	}

	if len(x) < 2 || (minValid > 0 && len(x) < minValid) {
		return math.NaN(), fmt.Errorf("insufficient valid values")
	}

	// Calculate means
	var sumX, sumY float64
	for i := range x {
		sumX += x[i]
		sumY += y[i]
	}
	meanX := sumX / float64(len(x))
	meanY := sumY / float64(len(y))

	// Calculate correlation
	var cov, varX, varY float64
	for i := range x {
		dx := x[i] - meanX
		dy := y[i] - meanY
		cov += dx * dy
		varX += dx * dx
		varY += dy * dy
	}

	denom := math.Sqrt(varX * varY)
	if denom == 0 {
		return math.NaN(), fmt.Errorf("no variation")
	}
	return cov / denom, nil
}

// Helper function to calculate covariance
func calculateCovariance(s1, s2 series.Series, minValid int) (float64, error) {
	var x, y []float64
	n := s1.Len()
	if s2.Len() < n {
		n = s2.Len()
	}
	for i := 0; i < n; i++ {
		if !s1.IsNull(i) && !s2.IsNull(i) {
			x = append(x, toFloat64Value(s1.Get(i)))
			y = append(y, toFloat64Value(s2.Get(i)))
		}
	}

	if len(x) < 2 || (minValid > 0 && len(x) < minValid) {
		return math.NaN(), fmt.Errorf("insufficient valid values")
	}

	var sumX, sumY float64
	for i := range x {
		sumX += x[i]
		sumY += y[i]
	}
	meanX := sumX / float64(len(x))
	meanY := sumY / float64(len(y))

	var cov float64
	for i := range x {
		cov += (x[i] - meanX) * (y[i] - meanY)
	}

	// Sample covariance (n-1 denominator)
	return cov / float64(len(x)-1), nil
}

// Helper to check if a data type is numeric
func isNumericType(dt datatypes.DataType) bool {
	switch dt.(type) {
	case datatypes.Int8, datatypes.Int16, datatypes.Int32, datatypes.Int64,
		datatypes.UInt8, datatypes.UInt16, datatypes.UInt32, datatypes.UInt64,
		datatypes.Float32, datatypes.Float64:
		return true
	default:
		return false
	}
}
