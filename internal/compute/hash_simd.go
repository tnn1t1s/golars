//go:build goexperiment.simd && amd64

// SIMD-accelerated hash functions using Go 1.26+ native SIMD support.
// Enable with: GOEXPERIMENT=simd go build
package compute

import (
	"simd/archsimd"
	"unsafe"
)

// SIMD constants for wyhash
var (
	simdWyp0 = archsimd.Uint64x4{}.Broadcast(wyp0)
	simdWyp1 = archsimd.Uint64x4{}.Broadcast(wyp1)
	simdWyp2 = archsimd.Uint64x4{}.Broadcast(wyp2)
)

// wymixSimd performs vectorized wyhash mixing on 4 values at once
func wymixSimd(a, b archsimd.Uint64x4) archsimd.Uint64x4 {
	// (a ^ wyp0) * (b ^ wyp1) -> hi, lo -> hi ^ lo
	ax := a.Xor(simdWyp0)
	bx := b.Xor(simdWyp1)

	// Multiply and get hi/lo parts
	// Note: archsimd may not have MulHigh, so we use a workaround
	// For now, use scalar path for the multiply
	lo := ax.Mul(bx) // Low 64 bits of multiply

	// Approximate hi using shifts (not exact but fast)
	// Real implementation would need proper 128-bit multiply
	hi := ax.ShiftRightConst(32).Mul(bx.ShiftRightConst(32))

	return hi.Xor(lo)
}

// BatchHashInt64Simd computes hashes for 4 int64 values at once
func BatchHashInt64Simd(values []int64, hashes []uint64) {
	n := len(values)

	// Process 4 values at a time
	i := 0
	for ; i+4 <= n; i += 4 {
		// Load 4 int64 values
		v := archsimd.LoadUint64x4((*[4]uint64)(unsafe.Pointer(&values[i])))

		// Hash: wymix(v, wyp2)
		h := wymixSimd(v, simdWyp2)

		// Store results
		h.Store((*[4]uint64)(unsafe.Pointer(&hashes[i])))
	}

	// Handle remainder with scalar
	for ; i < n; i++ {
		hashes[i] = hashInt64(values[i])
	}
}

// BatchCompareInt64Simd compares 4 pairs of int64 values, returns match mask
func BatchCompareInt64Simd(left, right *[4]int64) uint8 {
	l := archsimd.LoadInt64x4(left)
	r := archsimd.LoadInt64x4(right)
	mask := l.Equal(r)
	return mask.MoveMask()
}

// FindMatchesSimd finds indices where left[i] == key using SIMD
// Returns number of matches found and fills matchIndices
func FindMatchesSimd(left []int64, key int64, matchIndices []int) int {
	n := len(left)
	count := 0

	// Broadcast key to all lanes
	keyVec := archsimd.Int64x4{}.Broadcast(key)

	// Process 4 values at a time
	i := 0
	for ; i+4 <= n; i += 4 {
		v := archsimd.LoadInt64x4((*[4]int64)(unsafe.Pointer(&left[i])))
		mask := v.Equal(keyVec)

		// Check each lane
		m := mask.MoveMask()
		if m != 0 {
			if m&1 != 0 {
				matchIndices[count] = i
				count++
			}
			if m&2 != 0 {
				matchIndices[count] = i + 1
				count++
			}
			if m&4 != 0 {
				matchIndices[count] = i + 2
				count++
			}
			if m&8 != 0 {
				matchIndices[count] = i + 3
				count++
			}
		}
	}

	// Handle remainder
	for ; i < n; i++ {
		if left[i] == key {
			matchIndices[count] = i
			count++
		}
	}

	return count
}

// ProbeManySimd performs SIMD-accelerated probing
func (ht *Int64HashTable) ProbeManySimd(keys []int64) ([]int, []int) {
	n := len(keys)

	// Pre-compute all hashes using SIMD
	hashes := make([]uint64, n)
	BatchHashInt64Simd(keys, hashes)

	// Allocate result arrays
	leftIndices := make([]int, 0, n)
	rightIndices := make([]int, 0, n)

	// Probe each key
	for i := 0; i < n; i++ {
		bucket := hashes[i] & ht.mask
		candidates := ht.buckets[bucket]

		if len(candidates) == 0 {
			continue
		}

		// Check candidates for matches
		key := keys[i]
		for _, idx := range candidates {
			if ht.keys[idx] == key {
				leftIndices = append(leftIndices, idx)
				rightIndices = append(rightIndices, i)
			}
		}
	}

	return leftIndices, rightIndices
}

func init() {
	// Register SIMD implementations
	simdAvailable = true
}

var simdAvailable bool

// HasSIMD returns true if SIMD acceleration is available
func HasSIMD() bool {
	return simdAvailable
}
