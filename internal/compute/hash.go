package compute

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"

	"github.com/tnn1t1s/golars/series"
)

// HashTable for efficient join operations
type HashTable struct {
	indices map[uint64][]int
	keys    [][]interface{}
}

// BuildHashTable creates a hash table from multiple series
func BuildHashTable(series []series.Series) (*HashTable, error) {
	if len(series) == 0 {
		return nil, fmt.Errorf("cannot build hash table from empty series")
	}

	nRows := series[0].Len()
	for _, s := range series {
		if s.Len() != nRows {
			return nil, fmt.Errorf("series must have same length")
		}
	}

	ht := &HashTable{
		indices: make(map[uint64][]int),
		keys:    make([][]interface{}, 0, nRows),
	}

	for i := 0; i < nRows; i++ {
		key := make([]interface{}, len(series))
		h := fnv.New64a()

		for j, s := range series {
			val := s.Get(i)
			key[j] = val
			hashValue(h, val)
		}

		hash := h.Sum64()
		ht.indices[hash] = append(ht.indices[hash], i)
		ht.keys = append(ht.keys, key)
	}

	return ht, nil
}

// Probe looks up matching rows for given series values at a specific row
func (ht *HashTable) Probe(series []series.Series, row int) []int {
	if row < 0 || row >= series[0].Len() {
		return nil
	}

	key := make([]interface{}, len(series))
	h := fnv.New64a()

	for j, s := range series {
		val := s.Get(row)
		key[j] = val
		hashValue(h, val)
	}

	hash := h.Sum64()
	candidates := ht.indices[hash]

	// Verify actual equality (handle hash collisions)
	matches := make([]int, 0)
	for _, idx := range candidates {
		if keysEqual(key, ht.keys[idx]) {
			matches = append(matches, idx)
		}
	}

	return matches
}

// hashValue hashes a single value into the hash state
func hashValue(h hash.Hash64, val interface{}) {
	switch v := val.(type) {
	case int8:
		binary.Write(h, binary.LittleEndian, v)
	case int16:
		binary.Write(h, binary.LittleEndian, v)
	case int32:
		binary.Write(h, binary.LittleEndian, v)
	case int64:
		binary.Write(h, binary.LittleEndian, v)
	case uint8:
		binary.Write(h, binary.LittleEndian, v)
	case uint16:
		binary.Write(h, binary.LittleEndian, v)
	case uint32:
		binary.Write(h, binary.LittleEndian, v)
	case uint64:
		binary.Write(h, binary.LittleEndian, v)
	case float32:
		binary.Write(h, binary.LittleEndian, v)
	case float64:
		binary.Write(h, binary.LittleEndian, v)
	case string:
		h.Write([]byte(v))
	case bool:
		if v {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case nil:
		h.Write([]byte("__null__"))
	default:
		// Fallback to string representation
		h.Write([]byte(fmt.Sprintf("%v", v)))
	}
}

// keysEqual checks if two key arrays are equal
func keysEqual(key1, key2 []interface{}) bool {
	if len(key1) != len(key2) {
		return false
	}

	for i := range key1 {
		// Handle nil values specially
		if key1[i] == nil && key2[i] == nil {
			continue
		}
		if key1[i] == nil || key2[i] == nil {
			return false
		}

		// Compare values
		if !valuesEqual(key1[i], key2[i]) {
			return false
		}
	}

	return true
}

// valuesEqual compares two values for equality
func valuesEqual(v1, v2 interface{}) bool {
	// Type assertion to handle different types
	switch val1 := v1.(type) {
	case int8:
		val2, ok := v2.(int8)
		return ok && val1 == val2
	case int16:
		val2, ok := v2.(int16)
		return ok && val1 == val2
	case int32:
		val2, ok := v2.(int32)
		return ok && val1 == val2
	case int64:
		val2, ok := v2.(int64)
		return ok && val1 == val2
	case uint8:
		val2, ok := v2.(uint8)
		return ok && val1 == val2
	case uint16:
		val2, ok := v2.(uint16)
		return ok && val1 == val2
	case uint32:
		val2, ok := v2.(uint32)
		return ok && val1 == val2
	case uint64:
		val2, ok := v2.(uint64)
		return ok && val1 == val2
	case float32:
		val2, ok := v2.(float32)
		return ok && val1 == val2
	case float64:
		val2, ok := v2.(float64)
		return ok && val1 == val2
	case string:
		val2, ok := v2.(string)
		return ok && val1 == val2
	case bool:
		val2, ok := v2.(bool)
		return ok && val1 == val2
	default:
		// Fallback to interface equality
		return v1 == v2
	}
}

// Size returns the number of unique hash values in the table
func (ht *HashTable) Size() int {
	return len(ht.indices)
}

// TotalRows returns the total number of rows indexed
func (ht *HashTable) TotalRows() int {
	return len(ht.keys)
}