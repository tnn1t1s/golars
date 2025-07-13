package data

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/series"
)

// H2OAIConfig configures the H2O.ai benchmark data generation
type H2OAIConfig struct {
	NRows      int
	NGroups    int
	NullRatio  float64
	Seed       int64
	Sort       bool
}

// Preset configurations matching Polars benchmarks
var (
	H2OAISmall = H2OAIConfig{
		NRows:     10_000,
		NGroups:   100,
		NullRatio: 0.05,
		Seed:      0,
		Sort:      false,
	}

	H2OAIMedium = H2OAIConfig{
		NRows:     1_000_000,
		NGroups:   1_000,
		NullRatio: 0.05,
		Seed:      0,
		Sort:      false,
	}

	H2OAILarge = H2OAIConfig{
		NRows:     10_000_000,
		NGroups:   10_000,
		NullRatio: 0.05,
		Seed:      0,
		Sort:      false,
	}

	H2OAIXLarge = H2OAIConfig{
		NRows:     100_000_000,
		NGroups:   100_000,
		NullRatio: 0.05,
		Seed:      0,
		Sort:      false,
	}
)

// GenerateH2OAIData generates data matching the H2O.ai benchmark dataset
// This matches the Python implementation in datagen_groupby.py
func GenerateH2OAIData(config H2OAIConfig) (*frame.DataFrame, error) {
	rng := rand.New(rand.NewSource(config.Seed))
	
	N := config.NRows
	K := config.NGroups

	// Generate group strings
	groupStrSmall := make([]string, K)
	for i := 0; i < K; i++ {
		groupStrSmall[i] = fmt.Sprintf("id%03d", i+1)
	}

	groupStrLarge := make([]string, N/K)
	for i := 0; i < N/K; i++ {
		groupStrLarge[i] = fmt.Sprintf("id%010d", i+1)
	}

	// Generate column data
	id1 := make([]string, N)
	id2 := make([]string, N)
	id3 := make([]string, N)
	id4 := make([]int32, N)
	id5 := make([]int32, N)
	id6 := make([]int32, N)
	v1 := make([]int32, N)
	v2 := make([]int32, N)
	v3 := make([]float64, N)

	// Fill data matching Python's rng.choice behavior
	for i := 0; i < N; i++ {
		id1[i] = groupStrSmall[rng.Intn(K)]
		id2[i] = groupStrSmall[rng.Intn(K)]
		id3[i] = groupStrLarge[rng.Intn(N/K)]
		id4[i] = int32(rng.Intn(K) + 1)
		id5[i] = int32(rng.Intn(K) + 1)
		id6[i] = int32(rng.Intn(N/K) + 1)
		v1[i] = int32(rng.Intn(5) + 1)
		v2[i] = int32(rng.Intn(15) + 1)
		v3[i] = math.Round(rng.Float64()*100*1e6) / 1e6 // Round to 6 decimal places
	}

	// Create series
	seriesList := []series.Series{
		series.NewStringSeries("id1", id1),
		series.NewStringSeries("id2", id2),
		series.NewStringSeries("id3", id3),
		series.NewInt32Series("id4", id4),
		series.NewInt32Series("id5", id5),
		series.NewInt32Series("id6", id6),
		series.NewInt32Series("v1", v1),
		series.NewInt32Series("v2", v2),
		series.NewFloat64Series("v3", v3),
	}

	// Create dataframe
	df, err := frame.NewDataFrame(seriesList...)
	if err != nil {
		return nil, err
	}

	// Apply nulls if requested
	if config.NullRatio > 0 {
		df = setNulls(df, config.NullRatio, rng)
	}

	// Sort if requested
	if config.Sort {
		// Sort by all id columns
		sortCols := []string{"id1", "id2", "id3", "id4", "id5", "id6"}
		var err error
		df, err = df.Sort(sortCols...)
		if err != nil {
			return nil, fmt.Errorf("failed to sort dataframe: %w", err)
		}
	}

	return df, nil
}

// setNulls applies null values to the dataframe according to the ratio
func setNulls(df *frame.DataFrame, nullRatio float64, rng *rand.Rand) *frame.DataFrame {
	// This is a simplified version - in production you'd want to properly
	// handle nulls by modifying the underlying arrays
	// For now, we'll return the dataframe as-is
	// TODO: Implement proper null handling when golars supports it
	return df
}

// GetConfigBySize returns a config for the given size name
func GetConfigBySize(size string) (H2OAIConfig, error) {
	switch size {
	case "small":
		return H2OAISmall, nil
	case "medium":
		return H2OAIMedium, nil
	case "large":
		return H2OAILarge, nil
	case "xlarge":
		return H2OAIXLarge, nil
	default:
		return H2OAIConfig{}, fmt.Errorf("unknown size: %s", size)
	}
}

// DataSizeInfo provides information about the data size
type DataSizeInfo struct {
	Name        string
	Rows        int
	Groups      int
	SizeInBytes int64
}

// GetDataSizeInfo returns information about available data sizes
func GetDataSizeInfo() []DataSizeInfo {
	return []DataSizeInfo{
		{
			Name:        "small",
			Rows:        H2OAISmall.NRows,
			Groups:      H2OAISmall.NGroups,
			SizeInBytes: int64(H2OAISmall.NRows) * 9 * 8, // Rough estimate
		},
		{
			Name:        "medium",
			Rows:        H2OAIMedium.NRows,
			Groups:      H2OAIMedium.NGroups,
			SizeInBytes: int64(H2OAIMedium.NRows) * 9 * 8,
		},
		{
			Name:        "large",
			Rows:        H2OAILarge.NRows,
			Groups:      H2OAILarge.NGroups,
			SizeInBytes: int64(H2OAILarge.NRows) * 9 * 8,
		},
		{
			Name:        "xlarge",
			Rows:        H2OAIXLarge.NRows,
			Groups:      H2OAIXLarge.NGroups,
			SizeInBytes: int64(H2OAIXLarge.NRows) * 9 * 8,
		},
	}
}