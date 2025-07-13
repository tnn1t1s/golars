package frame

import (
	"math"
	"testing"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuantile(t *testing.T) {
	t.Run("Basic quantile calculation", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("col1", []float64{1, 2, 3, 4, 5}),
			series.NewFloat64Series("col2", []float64{10, 20, 30, 40, 50}),
		)
		require.NoError(t, err)
		
		// Calculate quartiles
		result, err := df.Quantile(QuantileOptions{
			Quantiles: []float64{0.25, 0.5, 0.75},
			Method:    "linear",
		})
		require.NoError(t, err)
		
		// Check dimensions
		assert.Equal(t, 3, result.Height()) // 3 quantiles
		assert.Equal(t, 3, result.Width())  // quantile + 2 columns
		
		// Check quantile column
		qCol, err := result.Column("quantile")
		require.NoError(t, err)
		assert.Equal(t, 0.25, qCol.Get(0))
		assert.Equal(t, 0.5, qCol.Get(1))
		assert.Equal(t, 0.75, qCol.Get(2))
		
		// Check col1 quantiles
		col1, err := result.Column("col1")
		require.NoError(t, err)
		assert.Equal(t, 2.0, col1.Get(0))  // Q1
		assert.Equal(t, 3.0, col1.Get(1))  // Median
		assert.Equal(t, 4.0, col1.Get(2))  // Q3
		
		// Check col2 quantiles
		col2, err := result.Column("col2")
		require.NoError(t, err)
		assert.Equal(t, 20.0, col2.Get(0)) // Q1
		assert.Equal(t, 30.0, col2.Get(1)) // Median
		assert.Equal(t, 40.0, col2.Get(2)) // Q3
	})
	
	t.Run("Different interpolation methods", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("values", []float64{1, 2, 3, 4}),
		)
		require.NoError(t, err)
		
		// Test different methods for 0.5 quantile
		methods := []string{"linear", "lower", "higher", "midpoint", "nearest"}
		expected := []float64{2.5, 2.0, 3.0, 2.5, 2.0}
		
		for i, method := range methods {
			result, err := df.Quantile(QuantileOptions{
				Quantiles: []float64{0.5},
				Method:    method,
			})
			require.NoError(t, err)
			
			values, err := result.Column("values")
			require.NoError(t, err)
			assert.Equal(t, expected[i], values.Get(0), "Method: %s", method)
		}
	})
	
	t.Run("With null values", func(t *testing.T) {
		values := []float64{1, 0, 3, 0, 5}
		validity := []bool{true, false, true, false, true}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		// Quantiles should ignore null values
		result, err := df.Quantile(QuantileOptions{
			Quantiles: []float64{0.5},
			Method:    "linear",
		})
		require.NoError(t, err)
		
		col, err := result.Column("col")
		require.NoError(t, err)
		assert.Equal(t, 3.0, col.Get(0)) // Median of [1, 3, 5]
	})
	
	t.Run("Mixed numeric types", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewInt64Series("ints", []int64{1, 2, 3, 4, 5}),
			series.NewFloat64Series("floats", []float64{1.5, 2.5, 3.5, 4.5, 5.5}),
			series.NewStringSeries("strings", []string{"a", "b", "c", "d", "e"}),
		)
		require.NoError(t, err)
		
		// Should only include numeric columns
		result, err := df.Quantile(QuantileOptions{
			Quantiles: []float64{0.5},
		})
		require.NoError(t, err)
		
		// Should have quantile + 2 numeric columns
		assert.Equal(t, 3, result.Width())
		assert.True(t, result.HasColumn("ints"))
		assert.True(t, result.HasColumn("floats"))
		assert.False(t, result.HasColumn("strings"))
	})
	
	t.Run("Invalid quantile values", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("col", []float64{1, 2, 3}),
		)
		require.NoError(t, err)
		
		// Quantile < 0
		_, err = df.Quantile(QuantileOptions{
			Quantiles: []float64{-0.1},
		})
		assert.Error(t, err)
		
		// Quantile > 1
		_, err = df.Quantile(QuantileOptions{
			Quantiles: []float64{1.1},
		})
		assert.Error(t, err)
	})
}

func TestPercentile(t *testing.T) {
	t.Run("Basic percentile calculation", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("col", []float64{1, 2, 3, 4, 5}),
		)
		require.NoError(t, err)
		
		// Calculate 25th, 50th, 75th percentiles
		result, err := df.Percentile([]float64{25, 50, 75}, "linear")
		require.NoError(t, err)
		
		col, err := result.Column("col")
		require.NoError(t, err)
		assert.Equal(t, 2.0, col.Get(0))  // 25th percentile
		assert.Equal(t, 3.0, col.Get(1))  // 50th percentile
		assert.Equal(t, 4.0, col.Get(2))  // 75th percentile
	})
}

func TestCorrelation(t *testing.T) {
	t.Run("Perfect positive correlation", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("x", []float64{1, 2, 3, 4, 5}),
			series.NewFloat64Series("y", []float64{2, 4, 6, 8, 10}), // y = 2x
		)
		require.NoError(t, err)
		
		corr, err := df.Correlation(CorrelationOptions{})
		require.NoError(t, err)
		
		// Check correlation matrix
		assert.Equal(t, 2, corr.Height())
		assert.Equal(t, 3, corr.Width()) // index + 2 columns
		
		// Get correlation values
		xCol, err := corr.Column("x")
		require.NoError(t, err)
		yCol, err := corr.Column("y")
		require.NoError(t, err)
		
		// Self-correlation should be 1
		assert.Equal(t, 1.0, xCol.Get(0))
		assert.Equal(t, 1.0, yCol.Get(1))
		
		// Cross-correlation should be 1 (perfect positive)
		assert.Equal(t, 1.0, xCol.Get(1))
		assert.Equal(t, 1.0, yCol.Get(0))
	})
	
	t.Run("Perfect negative correlation", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("x", []float64{1, 2, 3, 4, 5}),
			series.NewFloat64Series("y", []float64{5, 4, 3, 2, 1}), // y = -x + 6
		)
		require.NoError(t, err)
		
		corr, err := df.Correlation(CorrelationOptions{})
		require.NoError(t, err)
		
		xCol, err := corr.Column("x")
		require.NoError(t, err)
		
		// Cross-correlation should be -1 (perfect negative)
		assert.Equal(t, -1.0, xCol.Get(1))
	})
	
	t.Run("No correlation", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("x", []float64{1, 2, 3, 4, 5}),
			series.NewFloat64Series("y", []float64{3, 1, 4, 2, 5}), // Random
		)
		require.NoError(t, err)
		
		corr, err := df.Correlation(CorrelationOptions{})
		require.NoError(t, err)
		
		xCol, err := corr.Column("x")
		require.NoError(t, err)
		
		// Correlation should be close to 0
		assert.InDelta(t, 0.3, xCol.Get(1), 0.5) // Weak correlation
	})
	
	t.Run("With null values", func(t *testing.T) {
		xVals := []float64{1, 0, 3, 4, 5}
		xValid := []bool{true, false, true, true, true}
		yVals := []float64{2, 3, 0, 8, 10}
		yValid := []bool{true, true, false, true, true}
		
		df, err := NewDataFrame(
			series.NewSeriesWithValidity("x", xVals, xValid, datatypes.Float64{}),
			series.NewSeriesWithValidity("y", yVals, yValid, datatypes.Float64{}),
		)
		require.NoError(t, err)
		
		// Should calculate correlation using only paired non-null values
		// Pairs: (1,2), (4,8), (5,10)
		corr, err := df.Correlation(CorrelationOptions{})
		require.NoError(t, err)
		
		xCol, err := corr.Column("x")
		require.NoError(t, err)
		
		// Should still be perfect correlation with remaining pairs
		assert.Equal(t, 1.0, xCol.Get(1))
	})
	
	t.Run("Multiple columns", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("a", []float64{1, 2, 3, 4, 5}),
			series.NewFloat64Series("b", []float64{2, 4, 6, 8, 10}),
			series.NewFloat64Series("c", []float64{5, 4, 3, 2, 1}),
		)
		require.NoError(t, err)
		
		corr, err := df.Correlation(CorrelationOptions{})
		require.NoError(t, err)
		
		// Should be 3x3 correlation matrix
		assert.Equal(t, 3, corr.Height())
		assert.Equal(t, 4, corr.Width()) // index + 3 columns
		
		// Check some correlations
		bCol, err := corr.Column("b")
		require.NoError(t, err)
		assert.Equal(t, 1.0, bCol.Get(0))  // a-b correlation
		assert.Equal(t, -1.0, bCol.Get(2)) // c-b correlation
	})
}

func TestCovariance(t *testing.T) {
	t.Run("Basic covariance", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("x", []float64{1, 2, 3, 4, 5}),
			series.NewFloat64Series("y", []float64{2, 4, 6, 8, 10}),
		)
		require.NoError(t, err)
		
		cov, err := df.Covariance(CorrelationOptions{})
		require.NoError(t, err)
		
		// Check covariance matrix
		xCol, err := cov.Column("x")
		require.NoError(t, err)
		yCol, err := cov.Column("y")
		require.NoError(t, err)
		
		// Variance of x should be 2.5
		assert.Equal(t, 2.5, xCol.Get(0))
		
		// Variance of y should be 10 (4 times variance of x)
		assert.Equal(t, 10.0, yCol.Get(1))
		
		// Covariance should be 5
		assert.Equal(t, 5.0, xCol.Get(1))
		assert.Equal(t, 5.0, yCol.Get(0))
	})
	
	t.Run("Zero variance", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("constant", []float64{5, 5, 5, 5}),
			series.NewFloat64Series("varying", []float64{1, 2, 3, 4}),
		)
		require.NoError(t, err)
		
		cov, err := df.Covariance(CorrelationOptions{})
		require.NoError(t, err)
		
		constCol, err := cov.Column("constant")
		require.NoError(t, err)
		
		// Variance of constant should be 0
		assert.Equal(t, 0.0, constCol.Get(0))
		
		// Covariance with constant should be 0
		assert.Equal(t, 0.0, constCol.Get(1))
	})
	
	t.Run("With specific columns", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("a", []float64{1, 2, 3, 4}),
			series.NewFloat64Series("b", []float64{2, 4, 6, 8}),
			series.NewFloat64Series("c", []float64{1, 1, 1, 1}),
			series.NewStringSeries("d", []string{"x", "y", "z", "w"}),
		)
		require.NoError(t, err)
		
		// Calculate covariance only for a and b
		cov, err := df.Covariance(CorrelationOptions{
			Columns: []string{"a", "b"},
		})
		require.NoError(t, err)
		
		// Should be 2x2 matrix
		assert.Equal(t, 2, cov.Height())
		assert.Equal(t, 3, cov.Width()) // index + 2 columns
		
		// Should not include c or d
		assert.False(t, cov.HasColumn("c"))
		assert.False(t, cov.HasColumn("d"))
	})
}

func TestStatisticalEdgeCases(t *testing.T) {
	t.Run("Empty DataFrame", func(t *testing.T) {
		df, err := NewDataFrame()
		require.NoError(t, err)
		
		_, err = df.Quantile(QuantileOptions{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no numeric columns")
	})
	
	t.Run("All null values", func(t *testing.T) {
		values := []float64{0, 0, 0}
		validity := []bool{false, false, false}
		s := series.NewSeriesWithValidity("col", values, validity, datatypes.Float64{})
		
		df, err := NewDataFrame(s)
		require.NoError(t, err)
		
		_, err = df.Quantile(QuantileOptions{})
		assert.Error(t, err)
	})
	
	t.Run("Single value", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("col", []float64{42}),
		)
		require.NoError(t, err)
		
		result, err := df.Quantile(QuantileOptions{
			Quantiles: []float64{0.0, 0.5, 1.0},
		})
		require.NoError(t, err)
		
		col, err := result.Column("col")
		require.NoError(t, err)
		
		// All quantiles should be the same value
		assert.Equal(t, 42.0, col.Get(0))
		assert.Equal(t, 42.0, col.Get(1))
		assert.Equal(t, 42.0, col.Get(2))
	})
	
	t.Run("Insufficient data for correlation", func(t *testing.T) {
		df, err := NewDataFrame(
			series.NewFloat64Series("x", []float64{1}),
			series.NewFloat64Series("y", []float64{2}),
		)
		require.NoError(t, err)
		
		corr, err := df.Correlation(CorrelationOptions{
			MinValid: 2, // Require at least 2 observations
		})
		require.NoError(t, err)
		
		xCol, err := corr.Column("x")
		require.NoError(t, err)
		
		// Correlation should be NaN due to insufficient data
		val := xCol.Get(1).(float64)
		assert.True(t, math.IsNaN(val))
	})
}