package frame

import (
	"fmt"
	"math"
	"sort"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
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
	// Set defaults
	if len(options.Quantiles) == 0 {
		options.Quantiles = []float64{0.5} // Default to median
	}
	if options.Method == "" {
		options.Method = "linear"
	}
	
	// Validate quantiles
	for _, q := range options.Quantiles {
		if q < 0 || q > 1 {
			return nil, fmt.Errorf("quantile must be between 0 and 1, got %f", q)
		}
	}
	
	// Get numeric columns
	numericCols := make([]series.Series, 0)
	numericNames := make([]string, 0)
	
	for _, col := range df.columns {
		if isNumericType(col.DataType()) {
			numericCols = append(numericCols, col)
			numericNames = append(numericNames, col.Name())
		}
	}
	
	if len(numericCols) == 0 {
		return nil, fmt.Errorf("no numeric columns found")
	}
	
	// Create result columns
	resultColumns := make([]series.Series, 0)
	
	// Add quantile column
	quantileCol := series.NewFloat64Series("quantile", options.Quantiles)
	resultColumns = append(resultColumns, quantileCol)
	
	// Calculate quantiles for each numeric column
	for i, col := range numericCols {
		quantileValues := make([]float64, len(options.Quantiles))
		
		for j, q := range options.Quantiles {
			val, err := calculateQuantile(col, q, options.Method)
			if err != nil {
				return nil, fmt.Errorf("failed to calculate quantile for column %s: %w", numericNames[i], err)
			}
			quantileValues[j] = val
		}
		
		resultColumns = append(resultColumns, series.NewFloat64Series(numericNames[i], quantileValues))
	}
	
	return NewDataFrame(resultColumns...)
}

// Percentile is a convenience method for quantile with percentages
func (df *DataFrame) Percentile(percentiles []float64, method string) (*DataFrame, error) {
	// Convert percentiles to quantiles
	quantiles := make([]float64, len(percentiles))
	for i, p := range percentiles {
		quantiles[i] = p / 100.0
	}
	
	return df.Quantile(QuantileOptions{
		Quantiles: quantiles,
		Method:    method,
		Numeric:   true,
	})
}

// Helper function to calculate quantile for a series
func calculateQuantile(s series.Series, quantile float64, method string) (float64, error) {
	// Collect non-null values
	values := make([]float64, 0, s.Len())
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			val := toFloat64Value(s.Get(i))
			values = append(values, val)
		}
	}
	
	if len(values) == 0 {
		return 0, fmt.Errorf("no non-null values")
	}
	
	// Sort values
	sort.Float64s(values)
	
	// Calculate quantile position
	n := len(values)
	pos := quantile * float64(n-1)
	
	switch method {
	case "linear":
		// Linear interpolation between closest ranks
		lower := int(math.Floor(pos))
		upper := int(math.Ceil(pos))
		
		if lower == upper {
			return values[lower], nil
		}
		
		// Interpolate
		fraction := pos - float64(lower)
		return values[lower] + fraction*(values[upper]-values[lower]), nil
		
	case "lower":
		// Largest value smaller than or equal to quantile
		return values[int(math.Floor(pos))], nil
		
	case "higher":
		// Smallest value greater than or equal to quantile
		return values[int(math.Ceil(pos))], nil
		
	case "midpoint":
		// Average of lower and higher
		lower := int(math.Floor(pos))
		upper := int(math.Ceil(pos))
		return (values[lower] + values[upper]) / 2, nil
		
	case "nearest":
		// Nearest rank - round to nearest index
		// For exact halfway, round down to match expected behavior
		lower := int(math.Floor(pos))
		upper := int(math.Ceil(pos))
		
		if upper == lower {
			return values[lower], nil
		}
		
		// Check which is closer
		if pos - float64(lower) < float64(upper) - pos {
			return values[lower], nil
		} else if pos - float64(lower) > float64(upper) - pos {
			return values[upper], nil
		} else {
			// Exactly halfway - round down
			return values[lower], nil
		}
		
	default:
		return 0, fmt.Errorf("unknown method: %s", method)
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
	// Set defaults
	if options.Method == "" {
		options.Method = "pearson"
	}
	if options.MinValid <= 0 {
		options.MinValid = 1
	}
	
	// Get columns to correlate
	cols := options.Columns
	if len(cols) == 0 {
		// Use all numeric columns
		for _, col := range df.columns {
			if isNumericType(col.DataType()) {
				cols = append(cols, col.Name())
			}
		}
	}
	
	if len(cols) < 2 {
		return nil, fmt.Errorf("need at least 2 columns for correlation")
	}
	
	// Create correlation matrix
	n := len(cols)
	corrMatrix := make([][]float64, n)
	for i := range corrMatrix {
		corrMatrix[i] = make([]float64, n)
	}
	
	// Calculate pairwise correlations
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				corrMatrix[i][j] = 1.0 // Self-correlation is always 1
			} else if i < j {
				// Calculate correlation
				col1, err := df.Column(cols[i])
				if err != nil {
					return nil, err
				}
				col2, err := df.Column(cols[j])
				if err != nil {
					return nil, err
				}
				
				corr, err := calculateCorrelation(col1, col2, options.Method, options.MinValid)
				if err != nil {
					corrMatrix[i][j] = math.NaN()
					corrMatrix[j][i] = math.NaN()
				} else {
					corrMatrix[i][j] = corr
					corrMatrix[j][i] = corr // Correlation matrix is symmetric
				}
			}
		}
	}
	
	// Create result DataFrame
	resultColumns := make([]series.Series, n+1)
	
	// Index column (column names)
	resultColumns[0] = series.NewStringSeries("index", cols)
	
	// Correlation columns
	for i := 0; i < n; i++ {
		resultColumns[i+1] = series.NewFloat64Series(cols[i], corrMatrix[i])
	}
	
	return NewDataFrame(resultColumns...)
}

// Covariance calculates pairwise covariance of columns
func (df *DataFrame) Covariance(options CorrelationOptions) (*DataFrame, error) {
	// Set defaults
	if options.MinValid <= 0 {
		options.MinValid = 1
	}
	
	// Get columns
	cols := options.Columns
	if len(cols) == 0 {
		// Use all numeric columns
		for _, col := range df.columns {
			if isNumericType(col.DataType()) {
				cols = append(cols, col.Name())
			}
		}
	}
	
	if len(cols) < 2 {
		return nil, fmt.Errorf("need at least 2 columns for covariance")
	}
	
	// Create covariance matrix
	n := len(cols)
	covMatrix := make([][]float64, n)
	for i := range covMatrix {
		covMatrix[i] = make([]float64, n)
	}
	
	// Calculate pairwise covariances
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i <= j {
				// Calculate covariance
				col1, err := df.Column(cols[i])
				if err != nil {
					return nil, err
				}
				col2, err := df.Column(cols[j])
				if err != nil {
					return nil, err
				}
				
				cov, err := calculateCovariance(col1, col2, options.MinValid)
				if err != nil {
					covMatrix[i][j] = math.NaN()
					if i != j {
						covMatrix[j][i] = math.NaN()
					}
				} else {
					covMatrix[i][j] = cov
					if i != j {
						covMatrix[j][i] = cov // Covariance matrix is symmetric
					}
				}
			}
		}
	}
	
	// Create result DataFrame
	resultColumns := make([]series.Series, n+1)
	
	// Index column (column names)
	resultColumns[0] = series.NewStringSeries("index", cols)
	
	// Covariance columns
	for i := 0; i < n; i++ {
		resultColumns[i+1] = series.NewFloat64Series(cols[i], covMatrix[i])
	}
	
	return NewDataFrame(resultColumns...)
}

// Helper function to calculate Pearson correlation
func calculateCorrelation(s1, s2 series.Series, method string, minValid int) (float64, error) {
	if method != "pearson" {
		return 0, fmt.Errorf("only Pearson correlation is currently supported")
	}
	
	// Collect paired non-null values
	x := make([]float64, 0)
	y := make([]float64, 0)
	
	for i := 0; i < s1.Len() && i < s2.Len(); i++ {
		if !s1.IsNull(i) && !s2.IsNull(i) {
			x = append(x, toFloat64Value(s1.Get(i)))
			y = append(y, toFloat64Value(s2.Get(i)))
		}
	}
	
	if len(x) < minValid {
		return 0, fmt.Errorf("insufficient valid observations: %d < %d", len(x), minValid)
	}
	
	// Calculate means
	meanX, meanY := 0.0, 0.0
	for i := range x {
		meanX += x[i]
		meanY += y[i]
	}
	meanX /= float64(len(x))
	meanY /= float64(len(y))
	
	// Calculate correlation
	var sumXY, sumX2, sumY2 float64
	for i := range x {
		dx := x[i] - meanX
		dy := y[i] - meanY
		sumXY += dx * dy
		sumX2 += dx * dx
		sumY2 += dy * dy
	}
	
	if sumX2 == 0 || sumY2 == 0 {
		return 0, nil // No variation in one or both variables
	}
	
	return sumXY / math.Sqrt(sumX2*sumY2), nil
}

// Helper function to calculate covariance
func calculateCovariance(s1, s2 series.Series, minValid int) (float64, error) {
	// Collect paired non-null values
	x := make([]float64, 0)
	y := make([]float64, 0)
	
	for i := 0; i < s1.Len() && i < s2.Len(); i++ {
		if !s1.IsNull(i) && !s2.IsNull(i) {
			x = append(x, toFloat64Value(s1.Get(i)))
			y = append(y, toFloat64Value(s2.Get(i)))
		}
	}
	
	if len(x) < minValid {
		return 0, fmt.Errorf("insufficient valid observations: %d < %d", len(x), minValid)
	}
	
	// Calculate means
	meanX, meanY := 0.0, 0.0
	for i := range x {
		meanX += x[i]
		meanY += y[i]
	}
	meanX /= float64(len(x))
	meanY /= float64(len(y))
	
	// Calculate covariance
	var sumXY float64
	for i := range x {
		sumXY += (x[i] - meanX) * (y[i] - meanY)
	}
	
	// Sample covariance (n-1 denominator)
	return sumXY / float64(len(x)-1), nil
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