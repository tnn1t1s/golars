// Package interop contains exact translations of Polars benchmark tests.
//
// Source: https://github.com/pola-rs/polars/blob/main/py-polars/tests/benchmark/interop/test_numpy.py
//
// SKIPPED: Go does not have NumPy. These benchmarks document the Polars
// functionality but cannot be implemented in golars.
package interop

import (
	"testing"
)

// =============================================================================
// Polars interop/test_numpy.py - Exact Translations
// =============================================================================

// BenchmarkToNumpySeriesZeroCopy matches test_to_numpy_series_zero_copy
// Polars:
//
//	floats.to_numpy()  # where floats is a Series of 10,000 random floats
//
// SKIPPED: Go does not have NumPy interop
func BenchmarkToNumpySeriesZeroCopy(b *testing.B) {
	b.Skip("LANGUAGE GAP: Go does not have NumPy - use ToSlice() for similar functionality")
}

// BenchmarkToNumpySeriesWithNulls matches test_to_numpy_series_with_nulls
// Polars:
//
//	floats_with_nulls.to_numpy()  # Series with 10% null probability
//
// SKIPPED: Go does not have NumPy interop
func BenchmarkToNumpySeriesWithNulls(b *testing.B) {
	b.Skip("LANGUAGE GAP: Go does not have NumPy - use ToSlice() for similar functionality")
}

// BenchmarkToNumpySeriesChunked matches test_to_numpy_series_chunked
// Polars:
//
//	floats_chunked.to_numpy()  # Series with 5 chunks
//
// SKIPPED: Go does not have NumPy interop
func BenchmarkToNumpySeriesChunked(b *testing.B) {
	b.Skip("LANGUAGE GAP: Go does not have NumPy - golars uses contiguous arrays")
}
