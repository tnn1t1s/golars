package datetime

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
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
	}
}

// Resample groups datetime series by time frequency
func (dts *DateTimeSeries) Resample(rule *ResampleRule) (*ResampleGrouper, error) {
	if rule == nil {
		return nil, fmt.Errorf("rule cannot be nil")
	}

	// Parse frequency
	duration, unit, err := parseFrequency(rule.Frequency)
	if err != nil {
		return nil, fmt.Errorf("invalid frequency %q: %w", rule.Frequency, err)
	}

	// Create bins based on frequency
	bins, err := createTimeBins(dts.s, duration, unit, rule)
	if err != nil {
		return nil, err
	}

	return &ResampleGrouper{
		series:   dts.s,
		bins:     bins,
		rule:     rule,
		duration: duration,
		unit:     unit,
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
	n := rg.series.Len()

	for i := 0; i < n; i++ {
		if rg.series.IsNull(i) {
			continue
		}
		val := rg.series.Get(i)
		ts, ok := val.(int64)
		if !ok {
			// Skip non-timestamp values
			continue
		}
		bin := getBinForTimestamp(ts, rg.duration, rg.unit, rg.rule)
		groups[bin] = append(groups[bin], i)
	}

	if len(groups) == 0 {
		return series.NewSeries("result", []float64{}, datatypes.Float64{}), nil
	}

	// Sort bins
	bins := make([]int64, 0, len(groups))
	for bin := range groups {
		bins = append(bins, bin)
	}
	sortInt64Slice(bins)

	// Apply aggregation
	switch agg {
	case "sum":
		return aggregateSum(bins, groups, targetSeries)
	case "mean":
		return aggregateMean(bins, groups, targetSeries)
	case "count":
		return aggregateCount(bins, groups, targetSeries)
	case "min":
		return aggregateMin(bins, groups, targetSeries)
	case "max":
		return aggregateMax(bins, groups, targetSeries)
	case "first":
		return aggregateFirst(bins, groups, targetSeries)
	case "last":
		return aggregateLast(bins, groups, targetSeries)
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
	groups := make(map[int64][]int)
	n := rg.series.Len()

	for i := 0; i < n; i++ {
		if rg.series.IsNull(i) {
			continue
		}
		val := rg.series.Get(i)
		ts, ok := val.(int64)
		if !ok {
			continue
		}
		bin := getBinForTimestamp(ts, rg.duration, rg.unit, rg.rule)
		groups[bin] = append(groups[bin], i)
	}

	if len(groups) == 0 {
		return series.NewSeries("count", []uint32{}, datatypes.UInt32{}), nil
	}

	bins := make([]int64, 0, len(groups))
	for bin := range groups {
		bins = append(bins, bin)
	}
	sortInt64Slice(bins)

	counts := make([]uint32, len(bins))
	for i, bin := range bins {
		counts[i] = uint32(len(groups[bin]))
	}

	return series.NewSeries("count", counts, datatypes.UInt32{}), nil
}

// Helper functions

func parseFrequency(freq string) (int64, TimeUnit, error) {
	if freq == "" {
		return 0, Nanosecond, fmt.Errorf("empty frequency string")
	}

	// Find where the unit starts
	i := 0
	for i < len(freq) && freq[i] >= '0' && freq[i] <= '9' {
		i++
	}

	numStr := freq[:i]
	unitStr := freq[i:]

	var n int64
	if numStr == "" {
		n = 1
	} else {
		var err error
		n, err = strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, Nanosecond, fmt.Errorf("invalid frequency number: %q", numStr)
		}
	}

	// Map unit string to TimeUnit
	unitStr = strings.ToLower(unitStr)
	switch unitStr {
	case "ns":
		return n, Nanosecond, nil
	case "us":
		return n, Microsecond, nil
	case "ms":
		return n, Millisecond, nil
	case "s":
		return n, Second, nil
	case "min", "t":
		return n, Minute, nil
	case "h":
		return n, Hour, nil
	case "d":
		return n, Day, nil
	case "w":
		return n, Week, nil
	case "m":
		return n, Month, nil
	default:
		return 0, Nanosecond, fmt.Errorf("unknown frequency unit: %q", unitStr)
	}
}

func createTimeBins(s series.Series, duration int64, unit TimeUnit, rule *ResampleRule) (series.Series, error) {
	n := s.Len()
	if n == 0 {
		return series.NewSeries("bins", []int64{}, datatypes.Int64{}), nil
	}

	// Handle non-datetime series gracefully
	if s.Get(0) == nil {
		return series.NewSeries("bins", []int64{}, datatypes.Int64{}), nil
	}

	// Find min and max timestamps
	var minTS, maxTS int64
	first := true
	for i := 0; i < n; i++ {
		if s.IsNull(i) {
			continue
		}
		val := s.Get(i)
		ts, ok := val.(int64)
		if !ok {
			// Not a datetime series, return empty bins
			return series.NewSeries("bins", []int64{}, datatypes.Int64{}), nil
		}
		if first {
			minTS = ts
			maxTS = ts
			first = true
		}
		if ts < minTS {
			minTS = ts
		}
		if ts > maxTS {
			maxTS = ts
		}
		first = false
	}

	if first {
		return series.NewSeries("bins", []int64{}, datatypes.Int64{}), nil
	}

	// Create bins from min to max
	startBin := getBinStart(minTS, duration, unit, rule)
	var bins []int64
	current := startBin
	for current <= maxTS {
		bins = append(bins, current)
		current = getNextBin(current, duration, unit)
	}

	return series.NewSeries("bins", bins, datatypes.Int64{}), nil
}

func getBinStart(ts int64, duration int64, unit TimeUnit, rule *ResampleRule) int64 {
	t := time.Unix(0, ts).UTC()

	// Floor to the unit
	switch unit {
	case Nanosecond:
		return ts - (ts % (duration * int64(time.Nanosecond)))
	case Microsecond:
		return ts - (ts % (duration * int64(time.Microsecond)))
	case Millisecond:
		return ts - (ts % (duration * int64(time.Millisecond)))
	case Second:
		return ts - (ts % (duration * int64(time.Second)))
	case Minute:
		// For multi-unit durations, align to duration boundaries
		floored := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
		minutesSinceHourStart := int64(t.Minute())
		alignedMinutes := (minutesSinceHourStart / duration) * duration
		return floored.Add(time.Duration(alignedMinutes) * time.Minute).UnixNano()
	case Hour:
		floored := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		hoursSinceDayStart := int64(t.Hour())
		alignedHours := (hoursSinceDayStart / duration) * duration
		return floored.Add(time.Duration(alignedHours) * time.Hour).UnixNano()
	case Day:
		floored := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		return floored.UnixNano()
	case Week:
		wd := t.Weekday()
		daysBack := int(wd) - int(time.Monday)
		if daysBack < 0 {
			daysBack += 7
		}
		floored := time.Date(t.Year(), t.Month(), t.Day()-daysBack, 0, 0, 0, 0, t.Location())
		return floored.UnixNano()
	case Month:
		floored := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
		return floored.UnixNano()
	default:
		// Default to seconds
		return ts - (ts % (duration * int64(time.Second)))
	}
}

func getNextBin(current int64, duration int64, unit TimeUnit) int64 {
	switch unit {
	case Nanosecond:
		return current + duration*int64(time.Nanosecond)
	case Microsecond:
		return current + duration*int64(time.Microsecond)
	case Millisecond:
		return current + duration*int64(time.Millisecond)
	case Second:
		return current + duration*int64(time.Second)
	case Minute:
		return current + duration*int64(time.Minute)
	case Hour:
		return current + duration*int64(time.Hour)
	case Day:
		t := time.Unix(0, current).UTC()
		return t.AddDate(0, 0, int(duration)).UnixNano()
	case Week:
		t := time.Unix(0, current).UTC()
		return t.AddDate(0, 0, int(duration)*7).UnixNano()
	case Month:
		t := time.Unix(0, current).UTC()
		return t.AddDate(0, int(duration), 0).UnixNano()
	default:
		// Default to seconds
		return current + duration*int64(time.Second)
	}
}

func getBinForTimestamp(ts int64, duration int64, unit TimeUnit, rule *ResampleRule) int64 {
	return getBinStart(ts, duration, unit, rule)
}

// Aggregation functions

func aggregateSum(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	values := make([]float64, len(bins))
	for i, bin := range bins {
		sum := 0.0
		for _, idx := range groups[bin] {
			if target.IsNull(idx) {
				continue
			}
			v, err := toFloat64(target.Get(idx))
			if err != nil {
				continue
			}
			sum += v
		}
		values[i] = sum
	}
	return series.NewSeries("sum", values, datatypes.Float64{}), nil
}

func aggregateMean(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	values := make([]float64, len(bins))
	for i, bin := range bins {
		sum := 0.0
		count := 0
		for _, idx := range groups[bin] {
			if target.IsNull(idx) {
				continue
			}
			v, err := toFloat64(target.Get(idx))
			if err != nil {
				continue
			}
			sum += v
			count++
		}
		if count > 0 {
			values[i] = sum / float64(count)
		}
	}
	return series.NewSeries("mean", values, datatypes.Float64{}), nil
}

func aggregateCount(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	values := make([]uint32, len(bins))
	for i, bin := range bins {
		values[i] = uint32(len(groups[bin]))
	}
	return series.NewSeries("count", values, datatypes.UInt32{}), nil
}

func aggregateMin(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	values := make([]float64, len(bins))
	for i, bin := range bins {
		minVal := math.Inf(1)
		for _, idx := range groups[bin] {
			if target.IsNull(idx) {
				continue
			}
			v, err := toFloat64(target.Get(idx))
			if err != nil {
				continue
			}
			if v < minVal {
				minVal = v
			}
		}
		values[i] = minVal
	}
	return series.NewSeries("min", values, datatypes.Float64{}), nil
}

func aggregateMax(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	values := make([]float64, len(bins))
	for i, bin := range bins {
		maxVal := math.Inf(-1)
		for _, idx := range groups[bin] {
			if target.IsNull(idx) {
				continue
			}
			v, err := toFloat64(target.Get(idx))
			if err != nil {
				continue
			}
			if v > maxVal {
				maxVal = v
			}
		}
		values[i] = maxVal
	}
	return series.NewSeries("max", values, datatypes.Float64{}), nil
}

func aggregateFirst(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	values := make([]float64, len(bins))
	for i, bin := range bins {
		// Get first non-null value
		for _, idx := range groups[bin] {
			if target.IsNull(idx) {
				continue
			}
			v, err := toFloat64(target.Get(idx))
			if err != nil {
				continue
			}
			values[i] = v
			break
		}
	}
	return series.NewSeries("first", values, datatypes.Float64{}), nil
}

func aggregateLast(bins []int64, groups map[int64][]int, target series.Series) (series.Series, error) {
	values := make([]float64, len(bins))
	for i, bin := range bins {
		// Get last non-null value
		indices := groups[bin]
		for j := len(indices) - 1; j >= 0; j-- {
			idx := indices[j]
			if target.IsNull(idx) {
				continue
			}
			v, err := toFloat64(target.Get(idx))
			if err != nil {
				continue
			}
			values[i] = v
			break
		}
	}
	return series.NewSeries("last", values, datatypes.Float64{}), nil
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
	case int8:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case uint8:
		return float64(val), nil
	case uint16:
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
	// Simple insertion sort for now
	for i := 1; i < len(s); i++ {
		key := s[i]
		j := i - 1
		for j >= 0 && s[j] > key {
			s[j+1] = s[j]
			j--
		}
		s[j+1] = key
	}
}

// Expression API for resampling

// ResampleExpr represents a resample operation in the expression API
type ResampleExpr struct {
	expr expr.Expr
	rule *ResampleRule
}

func (e *ResampleExpr) String() string {
	return fmt.Sprintf("%s.dt.resample(%q)", e.expr.String(), e.rule.Frequency)
}

func (e *ResampleExpr) DataType() datatypes.DataType {
	return datatypes.Datetime{Unit: datatypes.Nanoseconds}
}

func (e *ResampleExpr) Alias(name string) expr.Expr {
	return &dateTimeAliasExpr{expr: e, alias: name}
}

func (e *ResampleExpr) IsColumn() bool {
	return false
}

func (e *ResampleExpr) Name() string {
	return e.expr.Name() + "_resampled_" + e.rule.Frequency
}

// Resample creates a resample expression
func (dte *DateTimeExpr) Resample(frequency string) *ResampleExpr {
	return &ResampleExpr{
		expr: dte.expr,
		rule: NewResampleRule(frequency),
	}
}
