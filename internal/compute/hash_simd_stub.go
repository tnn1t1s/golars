//go:build !goexperiment.simd || !amd64

// Stub file for when SIMD is not available.
// Falls back to scalar implementations.
package compute

// HasSIMD returns false when SIMD is not available
func HasSIMD() bool {
	return false
}

// BatchHashInt64Simd falls back to scalar when SIMD unavailable
func BatchHashInt64Simd(values []int64, hashes []uint64) {
	BatchHashInt64(values, hashes)
}

// ProbeManySimd falls back to scalar ProbeMany
func (ht *Int64HashTable) ProbeManySimd(keys []int64) ([]int, []int) {
	return ht.ProbeMany(keys)
}
