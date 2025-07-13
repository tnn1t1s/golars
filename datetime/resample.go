package datetime

import (
	"fmt"
	"time"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/series"
)

// ResampleRule defines how to resample time series data
type ResampleRule struct {
	Frequency string     // Frequency string like "1D", "1H", "5min"
	Closed    string     // "left" or "right" - which side of bin is closed
	Label     string     // "left" or "right" - which edge to use for result label
	Origin    *time.Time // Origin time for grouping
}

// NewResampleRule creates a new resample rule with defaults
func NewResampleRule(frequency string) *ResampleRule {
	return &ResampleRule{
		Frequency: frequency,
		Closed:    "left",
		Label:     "left",
		Origin:    nil,
	}
}

// Resample groups datetime series by time frequency
func (dts *DateTimeSeries) Resample(rule *ResampleRule) (*ResampleGrouper, error) {
	if rule == nil {
		return nil, fmt.Errorf("resample rule cannot be nil")
	}
	
	// Parse frequency
	duration, unit, err := parseFrequency(rule.Frequency)
	if err != nil {
		return nil, fmt.Errorf("invalid frequency %s: %w", rule.Frequency, err)
	}
	
	// Create bins based on frequency
	bins, err := createTimeBins(dts.s, duration, unit, rule)
	if err != nil {
		return nil, err
	}
	
	return &ResampleGrouper{
		series:    dts.s,
		bins:      bins,
		rule:      rule,
		duration:  duration,
		unit:      unit,
	}, nil
}

// ResampleGrouper handles resampled groups
type ResampleGrouper struct {
	series   series.Series
	bins     series.Series // Series of bin edges
	rule     *ResampleRule
	duration int64
	unit     TimeUnit
}

// Aggregate applies an aggregation function to resampled groups
func (rg *ResampleGrouper) Aggregate(agg string, targetSeries series.Series) (series.Series, error) {
	// Group indices by bins
	groups := make(map[int64][]int)
	
	for i := 0; i < rg.series.Len(); i++ {
		if !rg.series.IsNull(i) {
			val := rg.series.Get(i)
			ts, ok := val.(int64)
			if !ok {
				// Skip non-timestamp values
				continue
			}
			bin := getBinForTimestamp(ts, rg.duration, rg.unit, rg.rule)
			groups[bin] = append(groups[bin], i)
		}
	}
	
	// Create result arrays
	binKeys := make([]int64, 0, len(groups))
	for k := range groups {
		binKeys = append(binKeys, k)
	}
	
	// Sort bins
	sortInt64Slice(binKeys)
	
	// Apply aggregation
	switch agg {
	case "sum":
		return aggregateSum(binKeys, groups, targetSeries)
	case "mean":
		return aggregateMean(binKeys, groups, targetSeries)
	case "count":
		return aggregateCount(binKeys, groups, targetSeries)
	case "min":
		return aggregateMin(binKeys, groups, targetSeries)
	case "max":
		return aggregateMax(binKeys, groups, targetSeries)
	case "first":
		return aggregateFirst(binKeys, groups, targetSeries)
	case "last":
		return aggregateLast(binKeys, groups, targetSeries)
	default:
		return nil, fmt.Errorf("unsupported aggregation: %s", agg)
	}
}

// Sum aggregates by sum
func (rg *ResampleGrouper) Sum(targetSeries series.Series) (series.Series, error) {
	return rg.Aggregate("sum", targetSeries)
}

// Mean aggregates by mean
func (rg *ResampleGrouper) Mean(targetSeries series.Series) (series.Series, error) {
	return rg.Aggregate("mean", targetSeries)
}

// Count returns count of values in each bin
func (rg *ResampleGrouper) Count() (series.Series, error) {
	return rg.Aggregate("count", nil)
}

// Helper functions

func parseFrequency(freq string) (int64, TimeUnit, error) {
	// Parse frequency strings like "1D", "2H", "30min", "5s"
	if len(freq) < 1 {
		return 0, Nanosecond, fmt.Errorf("frequency too short")
	}
	
	// Extract number and unit
	var num int64
	var unitStr string
	
	// Find where the unit starts
	i := 0
	for i < len(freq) && (freq[i] >= '0' && freq[i] <= '9') {
		i++
	}
	
	if i == 0 {
		num = 1
		unitStr = freq
	} else {
		fmt.Sscanf(freq[:i], "%d", &num)
		unitStr = freq[i:]
	}
	
	// Map unit string to TimeUnit
	var unit TimeUnit
	switch unitStr {
	case "ns":
		unit = Nanosecond
	case "us":
		unit = Microsecond
	case "ms":
		unit = Millisecond
	case "s":
		unit = Second
	case "min", "m":
		unit = Minute
	case "h", "H":
		unit = Hour
	case "D", "d":
		unit = Day
	case "W", "w":
		unit = Week
	case "M":
		unit = Month
	case "Y", "y":
		unit = Year
	default:
		return 0, Nanosecond, fmt.Errorf("unknown time unit: %s", unitStr)
	}
	
	return num, unit, nil
}

func createTimeBins(s series.Series, duration int64, unit TimeUnit, rule *ResampleRule) (series.Series, error) {
	// Find min and max timestamps
	var minTS, maxTS int64
	first := true
	
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			// Handle non-datetime series gracefully
			val := s.Get(i)
			ts, ok := val.(int64)
			if !ok {
				// Not a datetime series, return empty bins
				return series.NewSeries("bins", []int64{}, datatypes.Datetime{Unit: datatypes.Nanoseconds}), nil
			}
			if first {
				minTS = ts
				maxTS = ts
				first = false
			} else {
				if ts < minTS {
					minTS = ts
				}
				if ts > maxTS {
					maxTS = ts
				}
			}
		}
	}
	
	if first {
		return series.NewSeries("bins", []int64{}, datatypes.Datetime{Unit: datatypes.Nanoseconds}), nil
	}
	
	// Create bins from min to max
	bins := []int64{}
	current := getBinStart(minTS, duration, unit, rule)
	
	for current <= maxTS {
		bins = append(bins, current)
		current = getNextBin(current, duration, unit)
	}
	
	return series.NewSeries("bins", bins, datatypes.Datetime{Unit: datatypes.Nanoseconds}), nil
}

func getBinStart(ts int64, duration int64, unit TimeUnit, rule *ResampleRule) int64 {
	dt := DateTime{timestamp: ts, timezone: time.UTC}
	
	// Floor to the unit
	floored := dt.Floor(unit)
	
	// Adjust for duration
	if duration > 1 {
		// For multi-unit durations, align to duration boundaries
		switch unit {
		case Minute:
			min := floored.Minute()
			alignedMin := (min / int(duration)) * int(duration)
			if alignedMin != min {
				t := floored.Time()
				t = t.Add(-time.Duration(min-alignedMin) * time.Minute)
				floored = NewDateTime(t)
			}
		case Hour:
			hour := floored.Hour()
			alignedHour := (hour / int(duration)) * int(duration)
			if alignedHour != hour {
				t := floored.Time()
				t = t.Add(-time.Duration(hour-alignedHour) * time.Hour)
				floored = NewDateTime(t)
			}
		}
	}
	
	return floored.timestamp
}

func getNextBin(current int64, duration int64, unit TimeUnit) int64 {
	dt := DateTime{timestamp: current, timezone: time.UTC}
	
	switch unit {
	case Nanosecond:
		return current + duration
	case Microsecond:
		return current + duration*1000
	case Millisecond:
		return current + duration*1000000
	case Second:
		return current + duration*1000000000
	case Minute:
		return dt.Add(Minutes(int(duration))).timestamp
	case Hour:
		return dt.Add(Hours(int(duration))).timestamp
	case Day:
		return dt.Add(Days(int(duration))).timestamp
	case Week:
		return dt.Add(Weeks(int(duration))).timestamp
	case Month:
		return dt.Add(Months(int(duration))).timestamp
	case Year:
		return dt.Add(Years(int(duration))).timestamp
	default:
		return current + duration*1000000000 // Default to seconds
	}
}

func getBinForTimestamp(ts int64, duration int64, unit TimeUnit, rule *ResampleRule) int64 {
	bin := getBinStart(ts, duration, unit, rule)
	
	// Adjust for closed/label
	if rule.Closed == "right" {
		// If closed on right, timestamp belongs to previous bin
		nextBin := getNextBin(bin, duration, unit)
		if ts >= nextBin {
			bin = nextBin
		}
	}
	
	return bin
}

// Aggregation functions

func aggregateSum(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	if target == nil {
		return nil, fmt.Errorf("target series required for sum aggregation")
	}
	
	values := make([]float64, len(bins))
	validity := make([]bool, len(bins))
	
	for i, bin := range bins {
		indices := groups[bin]
		if len(indices) == 0 {
			validity[i] = false
			continue
		}
		
		sum := 0.0
		hasValue := false
		
		for _, idx := range indices {
			if !target.IsNull(idx) {
				val, err := toFloat64(target.Get(idx))
				if err == nil {
					sum += val
					hasValue = true
				}
			}
		}
		
		if hasValue {
			values[i] = sum
			validity[i] = true
		} else {
			validity[i] = false
		}
	}
	
	return series.NewSeriesWithValidity("sum", values, validity, datatypes.Float64{}), nil
}

func aggregateMean(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	if target == nil {
		return nil, fmt.Errorf("target series required for mean aggregation")
	}
	
	values := make([]float64, len(bins))
	validity := make([]bool, len(bins))
	
	for i, bin := range bins {
		indices := groups[bin]
		if len(indices) == 0 {
			validity[i] = false
			continue
		}
		
		sum := 0.0
		count := 0
		
		for _, idx := range indices {
			if !target.IsNull(idx) {
				val, err := toFloat64(target.Get(idx))
				if err == nil {
					sum += val
					count++
				}
			}
		}
		
		if count > 0 {
			values[i] = sum / float64(count)
			validity[i] = true
		} else {
			validity[i] = false
		}
	}
	
	return series.NewSeriesWithValidity("mean", values, validity, datatypes.Float64{}), nil
}

func aggregateCount(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	values := make([]uint32, len(bins))
	
	for i, bin := range bins {
		indices := groups[bin]
		values[i] = uint32(len(indices))
	}
	
	return series.NewSeries("count", values, datatypes.UInt32{}), nil
}

func aggregateMin(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	if target == nil {
		return nil, fmt.Errorf("target series required for min aggregation")
	}
	
	values := make([]float64, len(bins))
	validity := make([]bool, len(bins))
	
	for i, bin := range bins {
		indices := groups[bin]
		if len(indices) == 0 {
			validity[i] = false
			continue
		}
		
		var min float64
		hasValue := false
		
		for _, idx := range indices {
			if !target.IsNull(idx) {
				val, err := toFloat64(target.Get(idx))
				if err == nil {
					if !hasValue || val < min {
						min = val
						hasValue = true
					}
				}
			}
		}
		
		if hasValue {
			values[i] = min
			validity[i] = true
		} else {
			validity[i] = false
		}
	}
	
	return series.NewSeriesWithValidity("min", values, validity, datatypes.Float64{}), nil
}

func aggregateMax(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	if target == nil {
		return nil, fmt.Errorf("target series required for max aggregation")
	}
	
	values := make([]float64, len(bins))
	validity := make([]bool, len(bins))
	
	for i, bin := range bins {
		indices := groups[bin]
		if len(indices) == 0 {
			validity[i] = false
			continue
		}
		
		var max float64
		hasValue := false
		
		for _, idx := range indices {
			if !target.IsNull(idx) {
				val, err := toFloat64(target.Get(idx))
				if err == nil {
					if !hasValue || val > max {
						max = val
						hasValue = true
					}
				}
			}
		}
		
		if hasValue {
			values[i] = max
			validity[i] = true
		} else {
			validity[i] = false
		}
	}
	
	return series.NewSeriesWithValidity("max", values, validity, datatypes.Float64{}), nil
}

func aggregateFirst(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	if target == nil {
		return nil, fmt.Errorf("target series required for first aggregation")
	}
	
	// Need to determine the target data type
	targetType := target.DataType()
	
	switch targetType.(type) {
	case datatypes.Float64:
		values := make([]float64, len(bins))
		validity := make([]bool, len(bins))
		
		for i, bin := range bins {
			indices := groups[bin]
			if len(indices) == 0 {
				validity[i] = false
				continue
			}
			
			// Get first non-null value
			found := false
			for _, idx := range indices {
				if !target.IsNull(idx) {
					values[i] = target.Get(idx).(float64)
					validity[i] = true
					found = true
					break
				}
			}
			
			if !found {
				validity[i] = false
			}
		}
		
		return series.NewSeriesWithValidity("first", values, validity, targetType), nil
		
	default:
		return nil, fmt.Errorf("first aggregation not implemented for type %s", targetType)
	}
}

func aggregateLast(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	if target == nil {
		return nil, fmt.Errorf("target series required for last aggregation")
	}
	
	// Need to determine the target data type
	targetType := target.DataType()
	
	switch targetType.(type) {
	case datatypes.Float64:
		values := make([]float64, len(bins))
		validity := make([]bool, len(bins))
		
		for i, bin := range bins {
			indices := groups[bin]
			if len(indices) == 0 {
				validity[i] = false
				continue
			}
			
			// Get last non-null value
			found := false
			for j := len(indices) - 1; j >= 0; j-- {
				idx := indices[j]
				if !target.IsNull(idx) {
					values[i] = target.Get(idx).(float64)
					validity[i] = true
					found = true
					break
				}
			}
			
			if !found {
				validity[i] = false
			}
		}
		
		return series.NewSeriesWithValidity("last", values, validity, targetType), nil
		
	default:
		return nil, fmt.Errorf("last aggregation not implemented for type %s", targetType)
	}
}

// Utility functions

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func sortInt64Slice(s []int64) {
	// Simple bubble sort for now
	n := len(s)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if s[j] > s[j+1] {
				s[j], s[j+1] = s[j+1], s[j]
			}
		}
	}
}

// Expression API for resampling

// ResampleExpr represents a resample operation in the expression API
type ResampleExpr struct {
	expr expr.Expr
	rule *ResampleRule
}

func (e *ResampleExpr) String() string {
	return fmt.Sprintf("%s.resample(%q)", e.expr.String(), e.rule.Frequency)
}

func (e *ResampleExpr) DataType() datatypes.DataType {
	return e.expr.DataType()
}

func (e *ResampleExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *ResampleExpr) IsColumn() bool {
	return false
}

func (e *ResampleExpr) Name() string {
	return fmt.Sprintf("%s_resample_%s", e.expr.Name(), e.rule.Frequency)
}

// Resample creates a resample expression
func (dte *DateTimeExpr) Resample(frequency string) *ResampleExpr {
	return &ResampleExpr{
		expr: dte.expr,
		rule: NewResampleRule(frequency),
	}
}