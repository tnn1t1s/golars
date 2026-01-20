package testutil

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

// DataGenerator provides methods for generating test data
type DataGenerator struct {
	rand *rand.Rand
}

// NewDataGenerator creates a new data generator with a deterministic seed
func NewDataGenerator(seed int64) *DataGenerator {
	return &DataGenerator{
		rand: rand.New(rand.NewSource(seed)),
	}
}

// GenerateInts generates a slice of random integers
func (g *DataGenerator) GenerateInts(size int, min, max int32) []int32 {
	result := make([]int32, size)
	rangeSize := max - min + 1

	for i := 0; i < size; i++ {
		result[i] = min + int32(g.rand.Intn(int(rangeSize)))
	}

	return result
}

// GenerateFloats generates a slice of random floats
func (g *DataGenerator) GenerateFloats(size int, min, max float64) []float64 {
	result := make([]float64, size)
	rangeSize := max - min

	for i := 0; i < size; i++ {
		result[i] = min + g.rand.Float64()*rangeSize
	}

	return result
}

// GenerateStrings generates a slice of random strings
func (g *DataGenerator) GenerateStrings(size int, prefix string, uniqueCount int) []string {
	result := make([]string, size)

	for i := 0; i < size; i++ {
		suffix := g.rand.Intn(uniqueCount)
		result[i] = fmt.Sprintf("%s_%d", prefix, suffix)
	}

	return result
}

// GenerateBools generates a slice of random booleans
func (g *DataGenerator) GenerateBools(size int, trueProb float64) []bool {
	result := make([]bool, size)

	for i := 0; i < size; i++ {
		result[i] = g.rand.Float64() < trueProb
	}

	return result
}

// GenerateDates generates a slice of dates as strings
func (g *DataGenerator) GenerateDates(size int, start, end time.Time) []string {
	result := make([]string, size)
	duration := end.Sub(start)

	for i := 0; i < size; i++ {
		randomDuration := time.Duration(g.rand.Int63n(int64(duration)))
		date := start.Add(randomDuration)
		result[i] = date.Format("2006-01-02")
	}

	return result
}

// GenerateCategories generates categorical data
func (g *DataGenerator) GenerateCategories(size int, categories []string) []string {
	result := make([]string, size)

	for i := 0; i < size; i++ {
		idx := g.rand.Intn(len(categories))
		result[i] = categories[idx]
	}

	return result
}

// TestData provides common test data patterns
var TestData = struct {
	// Small datasets for unit tests
	SmallInts    []int32
	SmallFloats  []float64
	SmallStrings []string
	SmallBools   []bool

	// Common categories
	Colors      []string
	Departments []string
	Statuses    []string

	// Names for testing
	FirstNames []string
	LastNames  []string

	// Special values
	NaNFloat    float64
	InfFloat    float64
	NegInfFloat float64
}{
	SmallInts:    []int32{1, 2, 3, 4, 5},
	SmallFloats:  []float64{1.1, 2.2, 3.3, 4.4, 5.5},
	SmallStrings: []string{"a", "b", "c", "d", "e"},
	SmallBools:   []bool{true, false, true, false, true},

	Colors:      []string{"red", "blue", "green", "yellow", "orange"},
	Departments: []string{"Sales", "Marketing", "Engineering", "HR", "Finance"},
	Statuses:    []string{"active", "inactive", "pending", "completed"},

	FirstNames: []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry"},
	LastNames:  []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Miller", "Wilson"},

	NaNFloat:    math.NaN(),
	InfFloat:    math.Inf(1),
	NegInfFloat: math.Inf(-1),
}

// GenerateMixedData creates a map suitable for DataFrame creation with mixed types
func GenerateMixedData(rows int) map[string]interface{} {
	gen := NewDataGenerator(42) // Fixed seed for reproducibility

	return map[string]interface{}{
		"id":         generateSequence(1, rows),
		"name":       generateNames(rows),
		"age":        gen.GenerateInts(rows, 18, 80),
		"salary":     gen.GenerateFloats(rows, 30000, 150000),
		"department": gen.GenerateCategories(rows, TestData.Departments),
		"active":     gen.GenerateBools(rows, 0.8),
		"start_date": gen.GenerateDates(rows, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Now()),
	}
}

// generateSequence creates a sequence of integers
func generateSequence(start, count int) []int {
	result := make([]int, count)
	for i := 0; i < count; i++ {
		result[i] = start + i
	}
	return result
}

// generateNames creates realistic names
func generateNames(count int) []string {
	gen := NewDataGenerator(42)
	result := make([]string, count)

	for i := 0; i < count; i++ {
		firstName := TestData.FirstNames[gen.rand.Intn(len(TestData.FirstNames))]
		lastName := TestData.LastNames[gen.rand.Intn(len(TestData.LastNames))]
		result[i] = fmt.Sprintf("%s %s", firstName, lastName)
	}

	return result
}

// GenerateGroupedData creates data suitable for testing groupby operations
func GenerateGroupedData(rows int, groupCardinality int) map[string]interface{} {
	gen := NewDataGenerator(42)

	// Generate group keys with specified cardinality
	groups := make([]string, groupCardinality)
	for i := 0; i < groupCardinality; i++ {
		groups[i] = fmt.Sprintf("Group_%c", 'A'+i)
	}

	return map[string]interface{}{
		"group":  gen.GenerateCategories(rows, groups),
		"value1": gen.GenerateInts(rows, 1, 100),
		"value2": gen.GenerateFloats(rows, 0, 1),
		"label":  gen.GenerateCategories(rows, TestData.Colors),
	}
}

// GenerateTimeSeriesData creates time series data for testing
func GenerateTimeSeriesData(points int, interval time.Duration) map[string]interface{} {
	gen := NewDataGenerator(42)

	timestamps := make([]time.Time, points)
	values := make([]float64, points)

	start := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < points; i++ {
		timestamps[i] = start.Add(time.Duration(i) * interval)
		// Generate values with trend and noise
		trend := float64(i) * 0.1
		noise := (gen.rand.Float64() - 0.5) * 10
		values[i] = 100 + trend + noise
	}

	return map[string]interface{}{
		"timestamp": timestamps,
		"value":     values,
		"sensor_id": gen.GenerateCategories(points, []string{"sensor_1", "sensor_2", "sensor_3"}),
	}
}
