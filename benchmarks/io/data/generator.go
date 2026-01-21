// Package data provides test data generation for IO benchmarks.
package data

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

// Size presets for benchmarks
type Size struct {
	Name    string
	Rows    int
	Groups  int
}

var (
	Small  = Size{"small", 10_000, 100}
	Medium = Size{"medium", 100_000, 1_000}
	Large  = Size{"large", 1_000_000, 10_000}
	Huge   = Size{"huge", 10_000_000, 100_000}
)

// AllSizes returns all size presets
func AllSizes() []Size {
	return []Size{Small, Medium, Large, Huge}
}

// Width presets for benchmarks
type Width struct {
	Name       string
	NumStrCols int
	NumIntCols int
	NumFltCols int
}

var (
	Narrow = Width{"narrow", 1, 1, 1}       // 3 columns
	Medium9 = Width{"medium", 3, 4, 2}      // 9 columns (H2O.ai style)
	Wide   = Width{"wide", 10, 20, 20}      // 50 columns
	VeryWide = Width{"very_wide", 50, 100, 50} // 200 columns
)

// AllWidths returns all width presets
func AllWidths() []Width {
	return []Width{Narrow, Medium9, Wide, VeryWide}
}

// TotalColumns returns total column count for a width preset
func (w Width) TotalColumns() int {
	return w.NumStrCols + w.NumIntCols + w.NumFltCols
}

// GenerateData creates a DataFrame with specified size and width
func GenerateData(size Size, width Width, seed int64) (*frame.DataFrame, error) {
	rng := rand.New(rand.NewSource(seed))
	N := size.Rows
	K := size.Groups

	// Generate group strings
	groupStrSmall := make([]string, K)
	for i := 0; i < K; i++ {
		groupStrSmall[i] = fmt.Sprintf("id%06d", i+1)
	}

	seriesList := make([]series.Series, 0, width.TotalColumns())

	// String columns
	for c := 0; c < width.NumStrCols; c++ {
		data := make([]string, N)
		for i := 0; i < N; i++ {
			data[i] = groupStrSmall[rng.Intn(K)]
		}
		seriesList = append(seriesList, series.NewStringSeries(fmt.Sprintf("str_%d", c), data))
	}

	// Integer columns
	for c := 0; c < width.NumIntCols; c++ {
		data := make([]int64, N)
		for i := 0; i < N; i++ {
			data[i] = int64(rng.Intn(1000))
		}
		seriesList = append(seriesList, series.NewInt64Series(fmt.Sprintf("int_%d", c), data))
	}

	// Float columns
	for c := 0; c < width.NumFltCols; c++ {
		data := make([]float64, N)
		for i := 0; i < N; i++ {
			data[i] = math.Round(rng.Float64()*1000*1e6) / 1e6
		}
		seriesList = append(seriesList, series.NewFloat64Series(fmt.Sprintf("flt_%d", c), data))
	}

	return frame.NewDataFrame(seriesList...)
}

// GenerateH2OStyle creates H2O.ai benchmark style data (9 columns)
func GenerateH2OStyle(size Size, seed int64) (*frame.DataFrame, error) {
	rng := rand.New(rand.NewSource(seed))
	N := size.Rows
	K := size.Groups

	groupStrSmall := make([]string, K)
	for i := 0; i < K; i++ {
		groupStrSmall[i] = fmt.Sprintf("id%03d", i+1)
	}

	groupStrLarge := make([]string, N/K)
	for i := 0; i < N/K; i++ {
		groupStrLarge[i] = fmt.Sprintf("id%010d", i+1)
	}

	id1 := make([]string, N)
	id2 := make([]string, N)
	id3 := make([]string, N)
	id4 := make([]int32, N)
	id5 := make([]int32, N)
	id6 := make([]int32, N)
	v1 := make([]int32, N)
	v2 := make([]int32, N)
	v3 := make([]float64, N)

	for i := 0; i < N; i++ {
		id1[i] = groupStrSmall[rng.Intn(K)]
		id2[i] = groupStrSmall[rng.Intn(K)]
		id3[i] = groupStrLarge[rng.Intn(N/K)]
		id4[i] = int32(rng.Intn(K) + 1)
		id5[i] = int32(rng.Intn(K) + 1)
		id6[i] = int32(rng.Intn(N/K) + 1)
		v1[i] = int32(rng.Intn(5) + 1)
		v2[i] = int32(rng.Intn(15) + 1)
		v3[i] = math.Round(rng.Float64()*100*1e6) / 1e6
	}

	return frame.NewDataFrame(
		series.NewStringSeries("id1", id1),
		series.NewStringSeries("id2", id2),
		series.NewStringSeries("id3", id3),
		series.NewInt32Series("id4", id4),
		series.NewInt32Series("id5", id5),
		series.NewInt32Series("id6", id6),
		series.NewInt32Series("v1", v1),
		series.NewInt32Series("v2", v2),
		series.NewFloat64Series("v3", v3),
	)
}
