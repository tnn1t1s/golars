package series_test

import (
	"math"
	"testing"
	
	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/series"
	"github.com/davidpalaitis/golars/testutil"
)

func TestSeriesAggregations(t *testing.T) {
	tests := []struct {
		name         string
		series       series.Series
		wantSum      float64
		wantMean     float64
		wantMin      interface{}
		wantMax      interface{}
		wantCount    int
		wantStd      float64
		wantVar      float64
		wantMedian   float64
		skipStd      bool // Skip std/var checks for some types
	}{
		{
			name:       "int32_series",
			series:     series.NewInt32Series("values", []int32{1, 2, 3, 4, 5}),
			wantSum:    15.0,
			wantMean:   3.0,
			wantMin:    int32(1),
			wantMax:    int32(5),
			wantCount:  5,
			wantStd:    1.5811,
			wantVar:    2.5,
			wantMedian: 3.0,
		},
		{
			name:       "float64_series",
			series:     series.NewFloat64Series("values", []float64{1.5, 2.5, 3.5, 4.5, 5.5}),
			wantSum:    17.5,
			wantMean:   3.5,
			wantMin:    1.5,
			wantMax:    5.5,
			wantCount:  5,
			wantStd:    1.5811,
			wantVar:    2.5,
			wantMedian: 3.5,
		},
		{
			name: "series_with_nulls",
			series: series.NewSeriesWithValidity(
				"values",
				[]int32{1, 2, 3, 4, 5},
				[]bool{true, false, true, false, true},
				datatypes.Int32{},
			),
			wantSum:    9.0,  // 1 + 3 + 5
			wantMean:   3.0,  // (1 + 3 + 5) / 3
			wantMin:    int32(1),
			wantMax:    int32(5),
			wantCount:  3,
			wantStd:    2.0,
			wantVar:    4.0,
			wantMedian: 3.0,
		},
		{
			name:       "empty_series",
			series:     series.NewInt32Series("empty", []int32{}),
			wantSum:    0.0,
			wantMean:   math.NaN(),
			wantMin:    nil,
			wantMax:    nil,
			wantCount:  0,
			wantStd:    math.NaN(),
			wantVar:    math.NaN(),
			wantMedian: math.NaN(),
		},
		{
			name:       "boolean_series",
			series:     series.NewBooleanSeries("flags", []bool{true, false, true, true, false}),
			wantSum:    3.0, // true = 1, false = 0
			wantMean:   0.6,
			wantMin:    false,
			wantMax:    true,
			wantCount:  5,
			wantStd:    0.5477,
			wantVar:    0.3,
			wantMedian: 1.0,
		},
		{
			name:       "string_series",
			series:     series.NewStringSeries("names", []string{"Alice", "Bob", "Charlie"}),
			wantSum:    math.NaN(),
			wantMean:   math.NaN(),
			wantMin:    "Alice",   // Lexicographic min
			wantMax:    "Charlie", // Lexicographic max
			wantCount:  3,
			wantStd:    math.NaN(),
			wantVar:    math.NaN(),
			wantMedian: math.NaN(),
			skipStd:    true,
		},
		{
			name: "all_nulls",
			series: series.NewSeriesWithValidity(
				"nulls",
				[]int32{1, 2, 3},
				[]bool{false, false, false},
				datatypes.Int32{},
			),
			wantSum:    0.0,
			wantMean:   math.NaN(),
			wantMin:    nil,
			wantMax:    nil,
			wantCount:  0,
			wantStd:    math.NaN(),
			wantVar:    math.NaN(),
			wantMedian: math.NaN(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			
			// Test Sum
			gotSum := tt.series.Sum()
			if math.IsNaN(tt.wantSum) {
				if !math.IsNaN(gotSum) {
					t.Errorf("Sum() = %v, want NaN", gotSum)
				}
			} else if gotSum != tt.wantSum {
				t.Errorf("Sum() = %v, want %v", gotSum, tt.wantSum)
			}
			
			// Test Mean
			gotMean := tt.series.Mean()
			if math.IsNaN(tt.wantMean) {
				if !math.IsNaN(gotMean) {
					t.Errorf("Mean() = %v, want NaN", gotMean)
				}
			} else {
				testutil.AssertInDelta(t, tt.wantMean, gotMean, 0.0001, "Mean()")
			}
			
			// Test Min
			gotMin := tt.series.Min()
			if gotMin != tt.wantMin {
				t.Errorf("Min() = %v, want %v", gotMin, tt.wantMin)
			}
			
			// Test Max
			gotMax := tt.series.Max()
			if gotMax != tt.wantMax {
				t.Errorf("Max() = %v, want %v", gotMax, tt.wantMax)
			}
			
			// Test Count
			if got := tt.series.Count(); got != tt.wantCount {
				t.Errorf("Count() = %v, want %v", got, tt.wantCount)
			}
			
			// Test Std and Var (skip for types that don't support it)
			if !tt.skipStd {
				gotStd := tt.series.Std()
				if math.IsNaN(tt.wantStd) {
					if !math.IsNaN(gotStd) {
						t.Errorf("Std() = %v, want NaN", gotStd)
					}
				} else {
					testutil.AssertInDelta(t, tt.wantStd, gotStd, 0.0001, "Std()")
				}
				
				gotVar := tt.series.Var()
				if math.IsNaN(tt.wantVar) {
					if !math.IsNaN(gotVar) {
						t.Errorf("Var() = %v, want NaN", gotVar)
					}
				} else {
					testutil.AssertInDelta(t, tt.wantVar, gotVar, 0.0001, "Var()")
				}
			}
			
			// Test Median
			gotMedian := tt.series.Median()
			if math.IsNaN(tt.wantMedian) {
				if !math.IsNaN(gotMedian) {
					t.Errorf("Median() = %v, want NaN", gotMedian)
				}
			} else {
				testutil.AssertInDelta(t, tt.wantMedian, gotMedian, 0.0001, "Median()")
			}
		})
	}
}

func TestMedianEdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		series     series.Series
		wantMedian float64
	}{
		{
			name:       "even_count",
			series:     series.NewInt32Series("values", []int32{1, 2, 3, 4}),
			wantMedian: 2.5, // (2 + 3) / 2
		},
		{
			name:       "single_value",
			series:     series.NewInt32Series("single", []int32{42}),
			wantMedian: 42.0,
		},
		{
			name:       "two_values",
			series:     series.NewFloat64Series("two", []float64{10.0, 20.0}),
			wantMedian: 15.0,
		},
		{
			name:       "odd_count_sorted",
			series:     series.NewInt64Series("odd", []int64{1, 3, 5, 7, 9}),
			wantMedian: 5.0,
		},
		{
			name:       "with_duplicates",
			series:     series.NewInt32Series("dups", []int32{1, 2, 2, 3, 3, 3, 4}),
			wantMedian: 3.0,
		},
		{
			name: "nulls_in_middle",
			series: series.NewSeriesWithValidity(
				"nulls_middle",
				[]int32{1, 2, 3, 4, 5},
				[]bool{true, true, false, true, true},
				datatypes.Int32{},
			),
			wantMedian: 2.5, // (2 + 4) / 2 for values [1, 2, 4, 5]
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			
			got := tt.series.Median()
			if math.IsNaN(tt.wantMedian) {
				if !math.IsNaN(got) {
					t.Errorf("Median() = %v, want NaN", got)
				}
			} else {
				testutil.AssertInDelta(t, tt.wantMedian, got, 0.0001, "Median()")
			}
		})
	}
}

func TestAggregationsWithSpecialValues(t *testing.T) {
	t.Run("float_with_inf_nan", func(t *testing.T) {
		// Test series with special float values
		values := []float64{1.0, 2.0, math.Inf(1), math.NaN(), 3.0}
		s := series.NewFloat64Series("special", values)
		
		// Most aggregations should handle Inf and NaN appropriately
		if got := s.Count(); got != 5 {
			t.Errorf("Count() = %v, want 5", got)
		}
		
		// Sum with Inf should be Inf
		if got := s.Sum(); !math.IsInf(got, 1) {
			t.Errorf("Sum() = %v, want +Inf", got)
		}
		
		// Mean with Inf should be Inf
		if got := s.Mean(); !math.IsInf(got, 1) {
			t.Errorf("Mean() = %v, want +Inf", got)
		}
	})
	
	t.Run("negative_values", func(t *testing.T) {
		s := series.NewInt32Series("negative", []int32{-5, -3, -1, 0, 2})
		
		if got := s.Sum(); got != -7.0 {
			t.Errorf("Sum() = %v, want -7", got)
		}
		
		if got := s.Min(); got != int32(-5) {
			t.Errorf("Min() = %v, want -5", got)
		}
		
		if got := s.Max(); got != int32(2) {
			t.Errorf("Max() = %v, want 2", got)
		}
		
		testutil.AssertInDelta(t, -1.4, s.Mean(), 0.0001, "Mean()")
	})
}

// Benchmarks for aggregation methods
func BenchmarkSeriesAggregations(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	
	for _, size := range sizes {
		// Generate test data
		gen := testutil.NewDataGenerator(42)
		values := gen.GenerateFloats(size, 0, 1000)
		s := series.NewFloat64Series("bench", values)
		
		b.Run("Sum/"+string(rune(size)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = s.Sum()
			}
		})
		
		b.Run("Mean/"+string(rune(size)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = s.Mean()
			}
		})
		
		b.Run("Median/"+string(rune(size)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = s.Median()
			}
		})
		
		b.Run("Std/"+string(rune(size)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = s.Std()
			}
		})
	}
}