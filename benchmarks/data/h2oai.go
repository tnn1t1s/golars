package data

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// H2OAIConfig configures the H2O.ai benchmark data generation
type H2OAIConfig struct {
	NRows     int
	NGroups   int
	NullRatio float64
	Seed      int64
	Sort      bool
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

	H2OAIMediumSafe = H2OAIConfig{
		NRows:     250_000,
		NGroups:   500,
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
	validID1 := allValid(len(id1))
	validID2 := allValid(len(id2))
	validID3 := allValid(len(id3))
	validID4 := allValid(len(id4))
	validID5 := allValid(len(id5))
	validID6 := allValid(len(id6))
	validV1 := allValid(len(v1))
	validV2 := allValid(len(v2))
	validV3 := allValid(len(v3))

	if config.NullRatio > 0 {
		validID1 = nullMaskGroupStrings(id1, config.NullRatio, rng)
		validID2 = nullMaskGroupStrings(id2, config.NullRatio, rng)
		validID3 = nullMaskGroupStrings(id3, config.NullRatio, rng)
		validID4 = nullMaskGroupInt32(id4, config.NullRatio, rng)
		validID5 = nullMaskGroupInt32(id5, config.NullRatio, rng)
		validID6 = nullMaskGroupInt32(id6, config.NullRatio, rng)
		validV1 = nullMaskValues(len(v1), config.NullRatio, rng)
		validV2 = nullMaskValues(len(v2), config.NullRatio, rng)
		validV3 = nullMaskValues(len(v3), config.NullRatio, rng)
	}

	seriesList := []series.Series{
		series.NewStringSeriesWithValidity("id1", id1, validID1),
		series.NewStringSeriesWithValidity("id2", id2, validID2),
		series.NewStringSeriesWithValidity("id3", id3, validID3),
		series.NewSeriesWithValidity("id4", id4, validID4, datatypes.Int32{}),
		series.NewSeriesWithValidity("id5", id5, validID5, datatypes.Int32{}),
		series.NewSeriesWithValidity("id6", id6, validID6, datatypes.Int32{}),
		series.NewSeriesWithValidity("v1", v1, validV1, datatypes.Int32{}),
		series.NewSeriesWithValidity("v2", v2, validV2, datatypes.Int32{}),
		series.NewSeriesWithValidity("v3", v3, validV3, datatypes.Float64{}),
	}

	// Create dataframe
	df, err := frame.NewDataFrame(seriesList...)
	if err != nil {
		return nil, err
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

func allValid(length int) []bool {
	validity := make([]bool, length)
	for i := range validity {
		validity[i] = true
	}
	return validity
}

func nullMaskValues(length int, ratio float64, rng *rand.Rand) []bool {
	validity := make([]bool, length)
	if ratio <= 0 {
		for i := range validity {
			validity[i] = true
		}
		return validity
	}
	for i := range validity {
		validity[i] = rng.Float64() >= ratio
	}
	return validity
}

func nullMaskGroupStrings(values []string, ratio float64, rng *rand.Rand) []bool {
	unique := make(map[string]struct{}, len(values))
	for _, v := range values {
		unique[v] = struct{}{}
	}
	uniq := make([]string, 0, len(unique))
	for v := range unique {
		uniq = append(uniq, v)
	}
	nNull := int(float64(len(uniq)) * ratio)
	if nNull <= 0 {
		return allValid(len(values))
	}
	rng.Shuffle(len(uniq), func(i, j int) {
		uniq[i], uniq[j] = uniq[j], uniq[i]
	})
	nullSet := make(map[string]struct{}, nNull)
	for i := 0; i < nNull; i++ {
		nullSet[uniq[i]] = struct{}{}
	}
	validity := make([]bool, len(values))
	for i, v := range values {
		_, isNull := nullSet[v]
		validity[i] = !isNull
	}
	return validity
}

func nullMaskGroupInt32(values []int32, ratio float64, rng *rand.Rand) []bool {
	unique := make(map[int32]struct{}, len(values))
	for _, v := range values {
		unique[v] = struct{}{}
	}
	uniq := make([]int32, 0, len(unique))
	for v := range unique {
		uniq = append(uniq, v)
	}
	nNull := int(float64(len(uniq)) * ratio)
	if nNull <= 0 {
		return allValid(len(values))
	}
	rng.Shuffle(len(uniq), func(i, j int) {
		uniq[i], uniq[j] = uniq[j], uniq[i]
	})
	nullSet := make(map[int32]struct{}, nNull)
	for i := 0; i < nNull; i++ {
		nullSet[uniq[i]] = struct{}{}
	}
	validity := make([]bool, len(values))
	for i, v := range values {
		_, isNull := nullSet[v]
		validity[i] = !isNull
	}
	return validity
}

// GetConfigBySize returns a config for the given size name
func GetConfigBySize(size string) (H2OAIConfig, error) {
	switch size {
	case "small":
		return H2OAISmall, nil
	case "medium":
		return H2OAIMedium, nil
	case "medium-safe":
		return H2OAIMediumSafe, nil
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
			Name:        "medium-safe",
			Rows:        H2OAIMediumSafe.NRows,
			Groups:      H2OAIMediumSafe.NGroups,
			SizeInBytes: int64(H2OAIMediumSafe.NRows) * 9 * 8,
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
