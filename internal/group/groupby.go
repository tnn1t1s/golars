package group

import (
	"fmt"
	"hash"
	"hash/fnv"
	"sync"
	"unsafe"

	"github.com/davidpalaitis/golars/series"
)

// DataFrameInterface represents the methods needed from DataFrame
type DataFrameInterface interface {
	Column(name string) (series.Series, error)
	Height() int
}

// GroupBy represents a grouped DataFrame
type GroupBy struct {
	df         DataFrameInterface
	groupCols  []string
	groups     map[uint64][]int // group hash -> row indices
	groupKeys  map[uint64][]interface{} // group hash -> key values
	groupOrder []uint64 // maintains order of first occurrence of each group
	mu         sync.RWMutex
}

// GroupKey represents a single group's key values
type GroupKey struct {
	Values []interface{}
	Hash   uint64
}

// NewGroupBy creates a new GroupBy from a DataFrame and group columns
func NewGroupBy(df DataFrameInterface, columns []string) (*GroupBy, error) {
	gb := &GroupBy{
		df:         df,
		groupCols:  columns,
		groups:     make(map[uint64][]int),
		groupKeys:  make(map[uint64][]interface{}),
		groupOrder: make([]uint64, 0),
	}

	// Build groups
	if err := gb.buildGroups(); err != nil {
		return nil, err
	}

	return gb, nil
}

// buildGroups creates the groups by hashing row values
func (gb *GroupBy) buildGroups() error {
	// Get group columns
	groupSeries := make([]series.Series, len(gb.groupCols))
	for i, col := range gb.groupCols {
		s, err := gb.df.Column(col)
		if err != nil {
			return fmt.Errorf("column %s not found", col)
		}
		groupSeries[i] = s
	}

	// Build groups by hashing row values
	for i := 0; i < gb.df.Height(); i++ {
		key := gb.getGroupKey(groupSeries, i)
		
		// Check if we've seen this key before
		if _, exists := gb.groups[key.Hash]; !exists {
			gb.groupKeys[key.Hash] = key.Values
			gb.groupOrder = append(gb.groupOrder, key.Hash)
		}
		
		gb.groups[key.Hash] = append(gb.groups[key.Hash], i)
	}

	return nil
}

// getGroupKey extracts and hashes the group key for a given row
func (gb *GroupBy) getGroupKey(groupSeries []series.Series, row int) GroupKey {
	values := make([]interface{}, len(groupSeries))
	h := fnv.New64a()

	for i, s := range groupSeries {
		val := s.Get(row)
		values[i] = val

		// Hash the value
		gb.hashValue(h, val)
	}

	return GroupKey{
		Values: values,
		Hash:   h.Sum64(),
	}
}

// hashValue hashes a single value
func (gb *GroupBy) hashValue(h hash.Hash64, val interface{}) {
	switch v := val.(type) {
	case int8:
		h.Write([]byte{byte(v)})
	case int16:
		h.Write([]byte{byte(v >> 8), byte(v)})
	case int32:
		h.Write([]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
	case int64:
		h.Write([]byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
			byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
	case uint8:
		h.Write([]byte{v})
	case uint16:
		h.Write([]byte{byte(v >> 8), byte(v)})
	case uint32:
		h.Write([]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
	case uint64:
		h.Write([]byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
			byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
	case float32:
		bits := float32ToBytes(v)
		h.Write(bits[:])
	case float64:
		bits := float64ToBytes(v)
		h.Write(bits[:])
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
		h.Write([]byte(fmt.Sprint(v)))
	}
}

// Groups returns the number of groups
func (gb *GroupBy) Groups() int {
	gb.mu.RLock()
	defer gb.mu.RUnlock()
	return len(gb.groups)
}

// GetGroup returns the row indices for a specific group
func (gb *GroupBy) GetGroup(hash uint64) ([]int, bool) {
	gb.mu.RLock()
	defer gb.mu.RUnlock()
	indices, exists := gb.groups[hash]
	return indices, exists
}

// helper functions for float to byte conversion
func float32ToBytes(f float32) [4]byte {
	var buf [4]byte
	bits := float32ToUint32(f)
	buf[0] = byte(bits >> 24)
	buf[1] = byte(bits >> 16)
	buf[2] = byte(bits >> 8)
	buf[3] = byte(bits)
	return buf
}

func float64ToBytes(f float64) [8]byte {
	var buf [8]byte
	bits := float64ToUint64(f)
	buf[0] = byte(bits >> 56)
	buf[1] = byte(bits >> 48)
	buf[2] = byte(bits >> 40)
	buf[3] = byte(bits >> 32)
	buf[4] = byte(bits >> 24)
	buf[5] = byte(bits >> 16)
	buf[6] = byte(bits >> 8)
	buf[7] = byte(bits)
	return buf
}

// Type conversion helpers
func float32ToUint32(f float32) uint32 {
	return *(*uint32)(unsafe.Pointer(&f))
}

func float64ToUint64(f float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&f))
}