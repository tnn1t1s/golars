// Fast hash table implementation optimized for join operations.
// Uses wyhash-style mixing for speed and prepares for SIMD acceleration.
package compute

import (
	"math/bits"
	"unsafe"

	"github.com/tnn1t1s/golars/internal/parallel"
	"github.com/tnn1t1s/golars/series"
)

// Hash constants (wyhash-style)
const (
	wyp0 = 0xa0761d6478bd642f
	wyp1 = 0xe7037ed1a0b428db
	wyp2 = 0x8ebc6af09c88c6e3
	wyp3 = 0x589965cc75374cc3
)

// wymix performs the wyhash mixing function
func wymix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a^wyp0, b^wyp1)
	return hi ^ lo
}

// hashInt64 hashes a single int64 value
func hashInt64(v int64) uint64 {
	return wymix(uint64(v), wyp2)
}

// hashInt64Slice hashes a slice of int64 values (for multi-column keys)
func hashInt64Slice(values []int64) uint64 {
	h := uint64(wyp3)
	for _, v := range values {
		h = wymix(h, uint64(v))
	}
	return h
}

// hashInt32 hashes a single int32 value
func hashInt32(v int32) uint64 {
	return wymix(uint64(v), wyp2)
}

// hashFloat64 hashes a float64 by reinterpreting bits
func hashFloat64(v float64) uint64 {
	bits := *(*uint64)(unsafe.Pointer(&v))
	return wymix(bits, wyp2)
}

// hashFloat32 hashes a float32 by reinterpreting bits
func hashFloat32(v float32) uint64 {
	bits := *(*uint32)(unsafe.Pointer(&v))
	return wymix(uint64(bits), wyp2)
}

// hashString hashes a string
func hashString(s string) uint64 {
	h := uint64(wyp3)
	b := []byte(s)

	// Process 8 bytes at a time
	for len(b) >= 8 {
		v := *(*uint64)(unsafe.Pointer(&b[0]))
		h = wymix(h, v)
		b = b[8:]
	}

	// Handle remainder
	if len(b) > 0 {
		var v uint64
		for i, c := range b {
			v |= uint64(c) << (i * 8)
		}
		h = wymix(h, v)
	}

	return h
}

// FastHashTable is an optimized hash table for typed data
type FastHashTable struct {
	buckets    [][]int // hash -> row indices
	numBuckets uint64
	mask       uint64
}

// NewFastHashTable creates a new hash table with the given capacity
func NewFastHashTable(capacity int) *FastHashTable {
	// Use power of 2 for fast modulo
	numBuckets := uint64(1)
	for numBuckets < uint64(capacity*2) {
		numBuckets <<= 1
	}

	return &FastHashTable{
		buckets:    make([][]int, numBuckets),
		numBuckets: numBuckets,
		mask:       numBuckets - 1,
	}
}

func int64ValuesFromSeries(s series.Series) ([]int64, []bool) {
	if values, validity, ok := series.Int64ValuesWithValidity(s); ok {
		return values, validity
	}

	n := s.Len()
	values := make([]int64, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		v := s.Get(i)
		if v == nil {
			continue
		}
		switch val := v.(type) {
		case int64:
			values[i] = val
		case int32:
			values[i] = int64(val)
		case int:
			values[i] = int64(val)
		default:
			continue
		}
		validity[i] = true
	}

	return values, validity
}

func int32ValuesFromSeries(s series.Series) ([]int32, []bool) {
	if values, validity, ok := series.Int32ValuesWithValidity(s); ok {
		return values, validity
	}

	n := s.Len()
	values := make([]int32, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		v := s.Get(i)
		if v == nil {
			continue
		}
		switch val := v.(type) {
		case int32:
			values[i] = val
		case int:
			values[i] = int32(val)
		default:
			continue
		}
		validity[i] = true
	}

	return values, validity
}

func uint64ValuesFromSeries(s series.Series) ([]uint64, []bool) {
	if values, validity, ok := series.Uint64ValuesWithValidity(s); ok {
		return values, validity
	}

	n := s.Len()
	values := make([]uint64, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		v := s.Get(i)
		if v == nil {
			continue
		}
		switch val := v.(type) {
		case uint64:
			values[i] = val
		case uint32:
			values[i] = uint64(val)
		case int:
			if val >= 0 {
				values[i] = uint64(val)
			} else {
				continue
			}
		default:
			continue
		}
		validity[i] = true
	}

	return values, validity
}

func uint32ValuesFromSeries(s series.Series) ([]uint32, []bool) {
	if values, validity, ok := series.Uint32ValuesWithValidity(s); ok {
		return values, validity
	}

	n := s.Len()
	values := make([]uint32, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		v := s.Get(i)
		if v == nil {
			continue
		}
		switch val := v.(type) {
		case uint32:
			values[i] = val
		case uint16:
			values[i] = uint32(val)
		case int:
			if val >= 0 {
				values[i] = uint32(val)
			} else {
				continue
			}
		default:
			continue
		}
		validity[i] = true
	}

	return values, validity
}

func float64ValuesFromSeries(s series.Series) ([]float64, []bool) {
	if values, validity, ok := series.Float64ValuesWithValidity(s); ok {
		return values, validity
	}

	n := s.Len()
	values := make([]float64, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		v := s.Get(i)
		if v == nil {
			continue
		}
		switch val := v.(type) {
		case float64:
			values[i] = val
		case float32:
			values[i] = float64(val)
		default:
			continue
		}
		validity[i] = true
	}

	return values, validity
}

func float32ValuesFromSeries(s series.Series) ([]float32, []bool) {
	if values, validity, ok := series.Float32ValuesWithValidity(s); ok {
		return values, validity
	}

	n := s.Len()
	values := make([]float32, n)
	validity := make([]bool, n)
	for i := 0; i < n; i++ {
		v := s.Get(i)
		if v == nil {
			continue
		}
		switch val := v.(type) {
		case float32:
			values[i] = val
		case float64:
			values[i] = float32(val)
		default:
			continue
		}
		validity[i] = true
	}

	return values, validity
}

// Int64HashTable is a specialized hash table for int64 keys
type Int64HashTable struct {
	*FastHashTable
	keys []int64
}

// BuildInt64HashTable builds a hash table from int64 series
func BuildInt64HashTable(s series.Series) (*Int64HashTable, error) {
	n := s.Len()
	values, validity := int64ValuesFromSeries(s)
	ht := &Int64HashTable{
		FastHashTable: NewFastHashTable(n),
		keys:          values,
	}

	hashes := make([]uint64, n)
	if HasSIMD() {
		BatchHashInt64Simd(values, hashes)
	} else {
		BatchHashInt64(values, hashes)
	}

	for i := range values {
		if !validity[i] {
			continue
		}
		bucket := hashes[i] & ht.mask
		ht.buckets[bucket] = append(ht.buckets[bucket], i)
	}

	return ht, nil
}

// Probe finds all rows matching the given key
func (ht *Int64HashTable) Probe(key int64) []int {
	hash := hashInt64(key)
	bucket := hash & ht.mask
	candidates := ht.buckets[bucket]

	if len(candidates) == 0 {
		return nil
	}

	// Filter by actual key equality
	matches := make([]int, 0, len(candidates))
	for _, idx := range candidates {
		if ht.keys[idx] == key {
			matches = append(matches, idx)
		}
	}

	return matches
}

// ProbeMany probes multiple keys and returns matches
// Returns: leftIndices, rightIndices (parallel arrays)
func (ht *Int64HashTable) ProbeMany(keys []int64) ([]int, []int) {
	leftIndices := make([]int, 0, len(keys))
	rightIndices := make([]int, 0, len(keys))

	for i, key := range keys {
		matches := ht.Probe(key)
		for _, match := range matches {
			leftIndices = append(leftIndices, match)
			rightIndices = append(rightIndices, i)
		}
	}

	return leftIndices, rightIndices
}

// BatchHashInt64 computes hashes for a batch of int64 values
// This is the scalar fallback - SIMD version in hash_simd.go
func BatchHashInt64(values []int64, hashes []uint64) {
	for i, v := range values {
		hashes[i] = hashInt64(v)
	}
}

// BatchHashInt64Parallel computes hashes using multiple goroutines
func BatchHashInt64Parallel(values []int64, hashes []uint64, numWorkers int) {
	n := len(values)
	useSimd := HasSIMD()
	if n < 1000 || numWorkers <= 1 {
		if useSimd {
			BatchHashInt64Simd(values, hashes)
		} else {
			BatchHashInt64(values, hashes)
		}
		return
	}

	chunkSize := (n + numWorkers - 1) / numWorkers
	done := make(chan struct{}, numWorkers)

	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		if start >= n {
			done <- struct{}{}
			continue
		}

		go func(s, e int) {
			if useSimd {
				BatchHashInt64Simd(values[s:e], hashes[s:e])
			} else {
				BatchHashInt64(values[s:e], hashes[s:e])
			}
			done <- struct{}{}
		}(start, end)
	}

	for w := 0; w < numWorkers; w++ {
		<-done
	}
}

// Int64JoinIndices performs a hash join on int64 columns and returns indices
func Int64JoinIndices(left, right series.Series) ([]int, []int, error) {
	// Build hash table on smaller side
	var buildSeries, probeSeries series.Series
	var swapped bool

	if left.Len() <= right.Len() {
		buildSeries = left
		probeSeries = right
		swapped = false
	} else {
		buildSeries = right
		probeSeries = left
		swapped = true
	}

	// Build hash table
	ht, err := BuildInt64HashTable(buildSeries)
	if err != nil {
		return nil, nil, err
	}

	// Extract probe keys
	probeKeys, _ := int64ValuesFromSeries(probeSeries)

	buildIndices, probeIndices := probeManyInt64(ht, probeKeys)

	// Swap back if needed
	if swapped {
		return probeIndices, buildIndices, nil
	}
	return buildIndices, probeIndices, nil
}

// =============================================================================
// Int32 Hash Table
// =============================================================================

// Int32HashTable is a specialized hash table for int32 keys
type Int32HashTable struct {
	*FastHashTable
	keys []int32
}

// BuildInt32HashTable builds a hash table from int32 series
func BuildInt32HashTable(s series.Series) (*Int32HashTable, error) {
	n := s.Len()
	values, validity := int32ValuesFromSeries(s)
	ht := &Int32HashTable{
		FastHashTable: NewFastHashTable(n),
		keys:          values,
	}

	for i, key := range values {
		if !validity[i] {
			continue
		}
		hash := hashInt32(key)
		bucket := hash & ht.mask
		ht.buckets[bucket] = append(ht.buckets[bucket], i)
	}

	return ht, nil
}

// Probe finds all rows matching the given key
func (ht *Int32HashTable) Probe(key int32) []int {
	hash := hashInt32(key)
	bucket := hash & ht.mask
	candidates := ht.buckets[bucket]

	if len(candidates) == 0 {
		return nil
	}

	// Filter by actual key equality
	matches := make([]int, 0, len(candidates))
	for _, idx := range candidates {
		if ht.keys[idx] == key {
			matches = append(matches, idx)
		}
	}

	return matches
}

// ProbeMany probes multiple keys and returns matches
func (ht *Int32HashTable) ProbeMany(keys []int32) ([]int, []int) {
	leftIndices := make([]int, 0, len(keys))
	rightIndices := make([]int, 0, len(keys))

	for i, key := range keys {
		matches := ht.Probe(key)
		for _, match := range matches {
			leftIndices = append(leftIndices, match)
			rightIndices = append(rightIndices, i)
		}
	}

	return leftIndices, rightIndices
}

// Int32JoinIndices performs a hash join on int32 columns and returns indices
func Int32JoinIndices(left, right series.Series) ([]int, []int, error) {
	// Build hash table on smaller side
	var buildSeries, probeSeries series.Series
	var swapped bool

	if left.Len() <= right.Len() {
		buildSeries = left
		probeSeries = right
		swapped = false
	} else {
		buildSeries = right
		probeSeries = left
		swapped = true
	}

	// Build hash table
	ht, err := BuildInt32HashTable(buildSeries)
	if err != nil {
		return nil, nil, err
	}

	// Extract probe keys
	probeKeys, _ := int32ValuesFromSeries(probeSeries)

	buildIndices, probeIndices := probeManyInt32(ht, probeKeys)

	// Swap back if needed
	if swapped {
		return probeIndices, buildIndices, nil
	}
	return buildIndices, probeIndices, nil
}

// =============================================================================
// Left Join Support
// =============================================================================

// Int64LeftJoinIndices performs a left hash join on int64 columns
// Returns: leftIndices, rightIndices (rightIndices contains -1 for non-matches)
func Int64LeftJoinIndices(left, right series.Series) ([]int, []int, error) {
	// Build hash table on right side (always)
	ht, err := BuildInt64HashTable(right)
	if err != nil {
		return nil, nil, err
	}

	// Extract left keys
	leftKeys, _ := int64ValuesFromSeries(left)
	n := len(leftKeys)

	// Pre-compute all hashes using SIMD when available
	hashes := make([]uint64, n)
	if HasSIMD() {
		BatchHashInt64Simd(leftKeys, hashes)
	} else {
		BatchHashInt64(leftKeys, hashes)
	}

	if shouldParallelProbe(n) {
		leftIndices, rightIndices := leftJoinParallelInt64(ht, leftKeys, hashes)
		return leftIndices, rightIndices, nil
	}

	leftIndices, rightIndices := leftJoinSequentialInt64(ht, leftKeys, hashes)
	return leftIndices, rightIndices, nil
}

// Int32LeftJoinIndices performs a left hash join on int32 columns
// Returns: leftIndices, rightIndices (rightIndices contains -1 for non-matches)
func Int32LeftJoinIndices(left, right series.Series) ([]int, []int, error) {
	// Build hash table on right side (always)
	ht, err := BuildInt32HashTable(right)
	if err != nil {
		return nil, nil, err
	}

	// Extract left keys
	leftKeys, _ := int32ValuesFromSeries(left)

	if shouldParallelProbe(len(leftKeys)) {
		leftIndices, rightIndices := leftJoinParallelInt32(ht, leftKeys)
		return leftIndices, rightIndices, nil
	}

	leftIndices, rightIndices := leftJoinSequentialInt32(ht, leftKeys)
	return leftIndices, rightIndices, nil
}

func probeManyInt64(ht *Int64HashTable, keys []int64) ([]int, []int) {
	if shouldParallelProbe(len(keys)) {
		return probeManyParallelInt64(ht, keys)
	}
	if HasSIMD() {
		return ht.ProbeManySimd(keys)
	}
	return ht.ProbeMany(keys)
}

func probeManyInt32(ht *Int32HashTable, keys []int32) ([]int, []int) {
	if shouldParallelProbe(len(keys)) {
		return probeManyParallelInt32(ht, keys)
	}
	return ht.ProbeMany(keys)
}

func probeManyParallelInt64(ht *Int64HashTable, keys []int64) ([]int, []int) {
	n := len(keys)
	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > n {
		chunks = n
	}
	if chunks <= 1 {
		return probeManyInt64(ht, keys)
	}
	chunkSize := (n + chunks - 1) / chunks

	type part struct {
		left  []int
		right []int
	}
	parts := make([]part, chunks)

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= n {
				continue
			}
			limit := offset + chunkSize
			if limit > n {
				limit = n
			}
			var left, right []int
			if HasSIMD() {
				left, right = ht.ProbeManySimd(keys[offset:limit])
			} else {
				left, right = ht.ProbeMany(keys[offset:limit])
			}
			if offset != 0 {
				for i := range right {
					right[i] += offset
				}
			}
			parts[idx] = part{left: left, right: right}
		}
		return nil
	})

	total := 0
	for _, p := range parts {
		total += len(p.left)
	}
	buildIndices := make([]int, 0, total)
	probeIndices := make([]int, 0, total)
	for _, p := range parts {
		buildIndices = append(buildIndices, p.left...)
		probeIndices = append(probeIndices, p.right...)
	}
	return buildIndices, probeIndices
}

func probeManyParallelInt32(ht *Int32HashTable, keys []int32) ([]int, []int) {
	n := len(keys)
	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > n {
		chunks = n
	}
	if chunks <= 1 {
		return probeManyInt32(ht, keys)
	}
	chunkSize := (n + chunks - 1) / chunks

	type part struct {
		left  []int
		right []int
	}
	parts := make([]part, chunks)

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= n {
				continue
			}
			limit := offset + chunkSize
			if limit > n {
				limit = n
			}
			left, right := ht.ProbeMany(keys[offset:limit])
			if offset != 0 {
				for i := range right {
					right[i] += offset
				}
			}
			parts[idx] = part{left: left, right: right}
		}
		return nil
	})

	total := 0
	for _, p := range parts {
		total += len(p.left)
	}
	buildIndices := make([]int, 0, total)
	probeIndices := make([]int, 0, total)
	for _, p := range parts {
		buildIndices = append(buildIndices, p.left...)
		probeIndices = append(probeIndices, p.right...)
	}
	return buildIndices, probeIndices
}

func leftJoinSequentialInt64(ht *Int64HashTable, leftKeys []int64, hashes []uint64) ([]int, []int) {
	leftIndices := make([]int, 0, len(leftKeys))
	rightIndices := make([]int, 0, len(leftKeys))

	for i, key := range leftKeys {
		bucket := hashes[i] & ht.mask
		candidates := ht.buckets[bucket]

		if len(candidates) == 0 {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
			continue
		}

		matched := false
		for _, idx := range candidates {
			if ht.keys[idx] == key {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, idx)
				matched = true
			}
		}
		if !matched {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
		}
	}

	return leftIndices, rightIndices
}

func leftJoinParallelInt64(ht *Int64HashTable, leftKeys []int64, hashes []uint64) ([]int, []int) {
	n := len(leftKeys)
	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > n {
		chunks = n
	}
	if chunks <= 1 {
		return leftJoinSequentialInt64(ht, leftKeys, hashes)
	}
	chunkSize := (n + chunks - 1) / chunks

	type part struct {
		left  []int
		right []int
	}
	parts := make([]part, chunks)

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= n {
				continue
			}
			limit := offset + chunkSize
			if limit > n {
				limit = n
			}
			localLeft := make([]int, 0, limit-offset)
			localRight := make([]int, 0, limit-offset)
			for i := offset; i < limit; i++ {
				key := leftKeys[i]
				bucket := hashes[i] & ht.mask
				candidates := ht.buckets[bucket]
				if len(candidates) == 0 {
					localLeft = append(localLeft, i)
					localRight = append(localRight, -1)
					continue
				}
				matched := false
				for _, idx := range candidates {
					if ht.keys[idx] == key {
						localLeft = append(localLeft, i)
						localRight = append(localRight, idx)
						matched = true
					}
				}
				if !matched {
					localLeft = append(localLeft, i)
					localRight = append(localRight, -1)
				}
			}
			parts[idx] = part{left: localLeft, right: localRight}
		}
		return nil
	})

	total := 0
	for _, p := range parts {
		total += len(p.left)
	}
	leftIndices := make([]int, 0, total)
	rightIndices := make([]int, 0, total)
	for _, p := range parts {
		leftIndices = append(leftIndices, p.left...)
		rightIndices = append(rightIndices, p.right...)
	}
	return leftIndices, rightIndices
}

func leftJoinSequentialInt32(ht *Int32HashTable, leftKeys []int32) ([]int, []int) {
	leftIndices := make([]int, 0, len(leftKeys))
	rightIndices := make([]int, 0, len(leftKeys))

	for i, key := range leftKeys {
		matches := ht.Probe(key)
		if len(matches) == 0 {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
		} else {
			for _, match := range matches {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, match)
			}
		}
	}

	return leftIndices, rightIndices
}

func leftJoinParallelInt32(ht *Int32HashTable, leftKeys []int32) ([]int, []int) {
	n := len(leftKeys)
	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > n {
		chunks = n
	}
	if chunks <= 1 {
		return leftJoinSequentialInt32(ht, leftKeys)
	}
	chunkSize := (n + chunks - 1) / chunks

	type part struct {
		left  []int
		right []int
	}
	parts := make([]part, chunks)

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= n {
				continue
			}
			limit := offset + chunkSize
			if limit > n {
				limit = n
			}
			localLeft := make([]int, 0, limit-offset)
			localRight := make([]int, 0, limit-offset)
			for i := offset; i < limit; i++ {
				matches := ht.Probe(leftKeys[i])
				if len(matches) == 0 {
					localLeft = append(localLeft, i)
					localRight = append(localRight, -1)
				} else {
					for _, match := range matches {
						localLeft = append(localLeft, i)
						localRight = append(localRight, match)
					}
				}
			}
			parts[idx] = part{left: localLeft, right: localRight}
		}
		return nil
	})

	total := 0
	for _, p := range parts {
		total += len(p.left)
	}
	leftIndices := make([]int, 0, total)
	rightIndices := make([]int, 0, total)
	for _, p := range parts {
		leftIndices = append(leftIndices, p.left...)
		rightIndices = append(rightIndices, p.right...)
	}
	return leftIndices, rightIndices
}

func shouldParallelProbe(n int) bool {
	if !parallel.Enabled() {
		return false
	}
	return n >= parallel.MaxThreads()*2048
}
