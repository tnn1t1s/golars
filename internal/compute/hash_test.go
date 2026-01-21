package compute

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tnn1t1s/golars/internal/parallel"
	"github.com/tnn1t1s/golars/series"
)

func TestBuildHashTable(t *testing.T) {
	// Test single column hash table
	t.Run("SingleColumn", func(t *testing.T) {
		s := series.NewInt32Series("id", []int32{1, 2, 3, 2, 1})
		ht, err := BuildHashTable([]series.Series{s})
		assert.NoError(t, err)
		assert.NotNil(t, ht)

		// Should have 3 unique keys
		assert.Equal(t, 3, ht.Size())
		// Should have 5 total rows
		assert.Equal(t, 5, ht.TotalRows())
	})

	// Test multi-column hash table
	t.Run("MultiColumn", func(t *testing.T) {
		s1 := series.NewInt32Series("year", []int32{2020, 2020, 2021, 2021})
		s2 := series.NewInt32Series("month", []int32{1, 2, 1, 2})

		ht, err := BuildHashTable([]series.Series{s1, s2})
		assert.NoError(t, err)
		assert.NotNil(t, ht)

		// Should have 4 unique combinations
		assert.Equal(t, 4, ht.Size())
		assert.Equal(t, 4, ht.TotalRows())
	})

	// Test with null values
	t.Run("WithNulls", func(t *testing.T) {
		// Create series with nulls using a mock approach
		// In real implementation, we'd use proper null handling
		s := series.NewStringSeries("key", []string{"A", "B", "", "A"})
		ht, err := BuildHashTable([]series.Series{s})
		assert.NoError(t, err)
		assert.NotNil(t, ht)

		// Should handle empty string as a valid key
		assert.Equal(t, 3, ht.Size()) // "A", "B", ""
	})

	// Test empty series
	t.Run("EmptySeries", func(t *testing.T) {
		s := series.NewInt32Series("id", []int32{})
		ht, err := BuildHashTable([]series.Series{s})
		assert.NoError(t, err)
		assert.NotNil(t, ht)
		assert.Equal(t, 0, ht.Size())
		assert.Equal(t, 0, ht.TotalRows())
	})

	// Test error cases
	t.Run("ErrorCases", func(t *testing.T) {
		// Empty series list
		_, err := BuildHashTable([]series.Series{})
		assert.Error(t, err)

		// Mismatched lengths
		s1 := series.NewInt32Series("a", []int32{1, 2, 3})
		s2 := series.NewInt32Series("b", []int32{1, 2})
		_, err = BuildHashTable([]series.Series{s1, s2})
		assert.Error(t, err)
	})
}

func TestProbe(t *testing.T) {
	// Build hash table
	s1 := series.NewInt32Series("id", []int32{1, 2, 3, 2, 1})
	s2 := series.NewStringSeries("name", []string{"A", "B", "C", "B", "A"})
	ht, err := BuildHashTable([]series.Series{s1, s2})
	assert.NoError(t, err)

	// Test exact matches
	t.Run("ExactMatches", func(t *testing.T) {
		probe1 := series.NewInt32Series("id", []int32{2})
		probe2 := series.NewStringSeries("name", []string{"B"})

		matches := ht.Probe([]series.Series{probe1, probe2}, 0)
		assert.Len(t, matches, 2) // Should find indices 1 and 3

		// Verify the matched indices
		expectedIndices := map[int]bool{1: true, 3: true}
		for _, idx := range matches {
			assert.True(t, expectedIndices[idx])
		}
	})

	// Test no match
	t.Run("NoMatch", func(t *testing.T) {
		probe1 := series.NewInt32Series("id", []int32{4})
		probe2 := series.NewStringSeries("name", []string{"D"})

		matches := ht.Probe([]series.Series{probe1, probe2}, 0)
		assert.Len(t, matches, 0)
	})

	// Test partial match (hash collision but values differ)
	t.Run("PartialMatch", func(t *testing.T) {
		probe1 := series.NewInt32Series("id", []int32{1})
		probe2 := series.NewStringSeries("name", []string{"B"}) // id=1 but name=B doesn't exist

		matches := ht.Probe([]series.Series{probe1, probe2}, 0)
		assert.Len(t, matches, 0)
	})

	// Test out of bounds
	t.Run("OutOfBounds", func(t *testing.T) {
		probe1 := series.NewInt32Series("id", []int32{1})
		probe2 := series.NewStringSeries("name", []string{"A"})

		matches := ht.Probe([]series.Series{probe1, probe2}, -1)
		assert.Nil(t, matches)

		matches = ht.Probe([]series.Series{probe1, probe2}, 10)
		assert.Nil(t, matches)
	})
}

func TestHashValueTypes(t *testing.T) {
	// Test that different types hash correctly
	testCases := []struct {
		name   string
		series series.Series
	}{
		{"Int8", series.NewInt8Series("test", []int8{1, 2, 3})},
		{"Int16", series.NewInt16Series("test", []int16{1, 2, 3})},
		{"Int32", series.NewInt32Series("test", []int32{1, 2, 3})},
		{"Int64", series.NewInt64Series("test", []int64{1, 2, 3})},
		{"Float32", series.NewFloat32Series("test", []float32{1.0, 2.0, 3.0})},
		{"Float64", series.NewFloat64Series("test", []float64{1.0, 2.0, 3.0})},
		{"String", series.NewStringSeries("test", []string{"a", "b", "c"})},
		{"Bool", series.NewBooleanSeries("test", []bool{true, false, true})},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ht, err := BuildHashTable([]series.Series{tc.series})
			assert.NoError(t, err)
			assert.NotNil(t, ht)

			// Boolean has only 2 unique values
			expectedSize := 3
			if tc.name == "Bool" {
				expectedSize = 2
			}
			assert.Equal(t, expectedSize, ht.Size())
		})
	}
}

func TestValuesEqual(t *testing.T) {
	// Test type-safe equality
	assert.True(t, valuesEqual(int32(1), int32(1)))
	assert.False(t, valuesEqual(int32(1), int32(2)))

	// Different types should not be equal
	assert.False(t, valuesEqual(int32(1), int64(1)))
	assert.False(t, valuesEqual(int32(1), float64(1.0)))

	// String equality
	assert.True(t, valuesEqual("hello", "hello"))
	assert.False(t, valuesEqual("hello", "world"))

	// Bool equality
	assert.True(t, valuesEqual(true, true))
	assert.False(t, valuesEqual(true, false))
}

func TestKeysEqual(t *testing.T) {
	// Test equal keys
	key1 := []interface{}{int32(1), "A", true}
	key2 := []interface{}{int32(1), "A", true}
	assert.True(t, keysEqual(key1, key2))

	// Test different values
	key3 := []interface{}{int32(1), "B", true}
	assert.False(t, keysEqual(key1, key3))

	// Test different lengths
	key4 := []interface{}{int32(1), "A"}
	assert.False(t, keysEqual(key1, key4))

	// Test with nils
	key5 := []interface{}{nil, "A", true}
	key6 := []interface{}{nil, "A", true}
	assert.True(t, keysEqual(key5, key6))

	key7 := []interface{}{int32(1), "A", true}
	assert.False(t, keysEqual(key5, key7))
}

func BenchmarkBuildHashTable(b *testing.B) {
	// Create test data
	size := 10000
	ids := make([]int32, size)
	names := make([]string, size)

	for i := 0; i < size; i++ {
		ids[i] = int32(i % 1000) // 1000 unique values
		names[i] = string(rune('A' + (i % 26)))
	}

	s1 := series.NewInt32Series("id", ids)
	s2 := series.NewStringSeries("name", names)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BuildHashTable([]series.Series{s1, s2})
	}
}

func BenchmarkProbe(b *testing.B) {
	// Build hash table
	size := 10000
	ids := make([]int32, size)
	for i := 0; i < size; i++ {
		ids[i] = int32(i)
	}

	s := series.NewInt32Series("id", ids)
	ht, _ := BuildHashTable([]series.Series{s})

	// Create probe series
	probeS := series.NewInt32Series("id", []int32{5000})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ht.Probe([]series.Series{probeS}, 0)
	}
}

// =============================================================================
// Fast Hash Benchmarks - Compare Original vs Optimized
// =============================================================================

func BenchmarkBuildHashTable_Comparison(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		// Generate test data
		data := make([]int64, size)
		for i := range data {
			data[i] = int64(i % 1000) // Some duplicates
		}
		s := series.NewInt64Series("test", data)

		b.Run(fmt.Sprintf("Original_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ht, _ := BuildHashTable([]series.Series{s})
				_ = ht
			}
		})

		b.Run(fmt.Sprintf("Fast_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ht, _ := BuildInt64HashTable(s)
				_ = ht
			}
		})
	}
}

func TestInt64JoinIndicesParallel(t *testing.T) {
	if os.Getenv("GOLARS_RUN_PAR_TESTS") != "1" {
		t.Skip("set GOLARS_RUN_PAR_TESTS=1 to run parallel join test")
	}

	leftData := make([]int64, 10000)
	rightData := make([]int64, 10000)
	for i := 0; i < len(leftData); i++ {
		leftData[i] = int64(i)
		rightData[i] = int64(i + 5000)
	}

	t.Setenv("GOLARS_MAX_THREADS", "1")
	t.Setenv("GOLARS_NO_PARALLEL", "")
	parallel.ResetForTests()
	seqLeft, seqRight, err := Int64JoinIndices(
		series.NewInt64Series("left", leftData),
		series.NewInt64Series("right", rightData),
	)
	assert.NoError(t, err)

	t.Setenv("GOLARS_MAX_THREADS", "4")
	parallel.ResetForTests()
	parLeft, parRight, err := Int64JoinIndices(
		series.NewInt64Series("left", leftData),
		series.NewInt64Series("right", rightData),
	)
	assert.NoError(t, err)

	seqPairs := make(map[string]int)
	for i := range seqLeft {
		key := fmt.Sprintf("%d:%d", seqLeft[i], seqRight[i])
		seqPairs[key]++
	}
	parPairs := make(map[string]int)
	for i := range parLeft {
		key := fmt.Sprintf("%d:%d", parLeft[i], parRight[i])
		parPairs[key]++
	}

	assert.Equal(t, seqPairs, parPairs)
}

func BenchmarkProbe_Comparison(b *testing.B) {
	// Build side: 10000 rows with 1000 unique values
	buildData := make([]int64, 10000)
	for i := range buildData {
		buildData[i] = int64(i % 1000)
	}
	buildSeries := series.NewInt64Series("build", buildData)

	// Probe side: 50000 rows
	probeData := make([]int64, 50000)
	for i := range probeData {
		probeData[i] = int64(i % 1000)
	}
	probeSeries := series.NewInt64Series("probe", probeData)

	// Build hash tables
	htOrig, _ := BuildHashTable([]series.Series{buildSeries})
	htFast, _ := BuildInt64HashTable(buildSeries)

	b.Run("Original_Probe_All", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < probeSeries.Len(); j++ {
				_ = htOrig.Probe([]series.Series{probeSeries}, j)
			}
		}
	})

	b.Run("Fast_ProbeMany", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = htFast.ProbeMany(probeData)
		}
	})

	b.Run("Fast_ProbeMany_SIMD", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = htFast.ProbeManySimd(probeData)
		}
	})
}

func BenchmarkBatchHash(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		data := make([]int64, size)
		hashes := make([]uint64, size)
		for i := range data {
			data[i] = int64(i * 17) // Some pattern
		}

		b.Run(fmt.Sprintf("Scalar_%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			for i := 0; i < b.N; i++ {
				BatchHashInt64(data, hashes)
			}
		})

		b.Run(fmt.Sprintf("SIMD_%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			for i := 0; i < b.N; i++ {
				BatchHashInt64Simd(data, hashes)
			}
		})
	}
}

func BenchmarkEndToEndJoin_Comparison(b *testing.B) {
	// Simulate realistic join: 50K x 5K with 1000 unique keys
	leftData := make([]int64, 50000)
	rightData := make([]int64, 5000)

	for i := range leftData {
		leftData[i] = int64(i % 1000)
	}
	for i := range rightData {
		rightData[i] = int64(i % 1000)
	}

	leftSeries := series.NewInt64Series("left", leftData)
	rightSeries := series.NewInt64Series("right", rightData)

	b.Run("Original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Build on smaller side
			ht, _ := BuildHashTable([]series.Series{rightSeries})

			// Probe
			leftIdx := make([]int, 0)
			rightIdx := make([]int, 0)
			for j := 0; j < leftSeries.Len(); j++ {
				matches := ht.Probe([]series.Series{leftSeries}, j)
				for _, m := range matches {
					leftIdx = append(leftIdx, j)
					rightIdx = append(rightIdx, m)
				}
			}
			_ = leftIdx
			_ = rightIdx
		}
	})

	b.Run("Fast", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			leftIdx, rightIdx, _ := Int64JoinIndices(leftSeries, rightSeries)
			_ = leftIdx
			_ = rightIdx
		}
	})
}

// Test correctness of fast hash
func TestFastHashCorrectness(t *testing.T) {
	// Generate test data with duplicates
	data := make([]int64, 1000)
	for i := range data {
		data[i] = int64(i % 100) // 100 unique values, 10 each
	}
	s := series.NewInt64Series("test", data)

	// Build fast hash table
	htFast, err := BuildInt64HashTable(s)
	assert.NoError(t, err)

	// Test probing for each unique value
	for key := int64(0); key < 100; key++ {
		matches := htFast.Probe(key)

		// Should find 10 matches for each key (0-99 appear 10 times each)
		expectedCount := 10
		if len(matches) != expectedCount {
			t.Errorf("key %d: expected %d matches, got %d", key, expectedCount, len(matches))
		}

		// Verify all matches have correct value
		for _, idx := range matches {
			if data[idx] != key {
				t.Errorf("key %d: match at index %d has value %d", key, idx, data[idx])
			}
		}
	}

	// Test non-existent key
	matches := htFast.Probe(999)
	assert.Empty(t, matches)
}

func TestInt64JoinIndicesCorrectness(t *testing.T) {
	leftData := []int64{1, 2, 3, 1, 2}
	rightData := []int64{2, 3, 4}

	leftSeries := series.NewInt64Series("left", leftData)
	rightSeries := series.NewInt64Series("right", rightData)

	leftIdx, rightIdx, err := Int64JoinIndices(leftSeries, rightSeries)
	assert.NoError(t, err)

	// Expected matches:
	// left[1]=2 matches right[0]=2
	// left[2]=3 matches right[1]=3
	// left[4]=2 matches right[0]=2
	assert.Len(t, leftIdx, 3)
	assert.Len(t, rightIdx, 3)

	// Verify each match
	for i := range leftIdx {
		assert.Equal(t, leftData[leftIdx[i]], rightData[rightIdx[i]],
			"match %d: left[%d]=%d should equal right[%d]=%d",
			i, leftIdx[i], leftData[leftIdx[i]], rightIdx[i], rightData[rightIdx[i]])
	}
}
