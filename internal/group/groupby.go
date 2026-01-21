package group

import (
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"unsafe"

	"github.com/tnn1t1s/golars/internal/parallel"
	"github.com/tnn1t1s/golars/series"
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
	groups     map[uint64][]int         // group hash -> row indices
	groupKeys  map[uint64][]interface{} // group hash -> key values
	groupOrder []uint64                 // maintains order of first occurrence of each group
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

	if len(groupSeries) == 1 {
		if gb.buildGroupsSingleInt64(groupSeries[0]) ||
			gb.buildGroupsSingleInt32(groupSeries[0]) ||
			gb.buildGroupsSingleUint64(groupSeries[0]) ||
			gb.buildGroupsSingleUint32(groupSeries[0]) ||
			gb.buildGroupsSingleFloat64(groupSeries[0]) ||
			gb.buildGroupsSingleFloat32(groupSeries[0]) {
			return nil
		}
	}

	if shouldParallelGroupBy(gb.df.Height()) {
		return gb.buildGroupsParallel(groupSeries)
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

type groupPartial struct {
	groups map[uint64][]int
	keys   map[uint64][]interface{}
	first  map[uint64]int
}

func (gb *GroupBy) buildGroupsParallel(groupSeries []series.Series) error {
	n := gb.df.Height()
	if n == 0 {
		return nil
	}

	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > n {
		chunks = n
	}
	chunkSize := (n + chunks - 1) / chunks

	parts := make([]groupPartial, chunks)
	if err := parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			rowStart := idx * chunkSize
			if rowStart >= n {
				continue
			}
			rowEnd := rowStart + chunkSize
			if rowEnd > n {
				rowEnd = n
			}
			local := groupPartial{
				groups: make(map[uint64][]int),
				keys:   make(map[uint64][]interface{}),
				first:  make(map[uint64]int),
			}
			for i := rowStart; i < rowEnd; i++ {
				key := gb.getGroupKey(groupSeries, i)
				if _, exists := local.groups[key.Hash]; !exists {
					local.keys[key.Hash] = key.Values
					local.first[key.Hash] = i
				}
				local.groups[key.Hash] = append(local.groups[key.Hash], i)
			}
			parts[idx] = local
		}
		return nil
	}); err != nil {
		return err
	}

	firstIndex := make(map[uint64]int, len(parts))
	for _, part := range parts {
		for hash, idxs := range part.groups {
			if existing, ok := gb.groups[hash]; ok {
				gb.groups[hash] = append(existing, idxs...)
			} else {
				gb.groups[hash] = append([]int(nil), idxs...)
			}

			if _, ok := gb.groupKeys[hash]; !ok {
				gb.groupKeys[hash] = part.keys[hash]
			}

			if first, ok := part.first[hash]; ok {
				if current, ok := firstIndex[hash]; !ok || first < current {
					firstIndex[hash] = first
				}
			}
		}
	}

	type orderedGroup struct {
		hash  uint64
		first int
	}

	ordered := make([]orderedGroup, 0, len(firstIndex))
	for hash, first := range firstIndex {
		ordered = append(ordered, orderedGroup{hash: hash, first: first})
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].first < ordered[j].first
	})

	gb.groupOrder = gb.groupOrder[:0]
	for _, entry := range ordered {
		gb.groupOrder = append(gb.groupOrder, entry.hash)
	}

	return nil
}

func shouldParallelGroupBy(rows int) bool {
	if !parallel.Enabled() {
		return false
	}
	return rows >= parallel.MaxThreads()*2048
}

func (gb *GroupBy) buildGroupsSingleInt64(col series.Series) bool {
	values, validity, ok := series.Int64ValuesWithValidity(col)
	if !ok {
		return false
	}

	keyToGroup := make(map[int64]uint64, len(values))
	var nullGroupID uint64
	hasNull := false
	nextID := uint64(0)

	for i, val := range values {
		if !validity[i] {
			if !hasNull {
				nullGroupID = nextID
				nextID++
				hasNull = true
				gb.groupKeys[nullGroupID] = []interface{}{nil}
				gb.groupOrder = append(gb.groupOrder, nullGroupID)
			}
			gb.groups[nullGroupID] = append(gb.groups[nullGroupID], i)
			continue
		}

		groupID, exists := keyToGroup[val]
		if !exists {
			groupID = nextID
			nextID++
			keyToGroup[val] = groupID
			gb.groupKeys[groupID] = []interface{}{val}
			gb.groupOrder = append(gb.groupOrder, groupID)
		}
		gb.groups[groupID] = append(gb.groups[groupID], i)
	}

	return true
}

func (gb *GroupBy) buildGroupsSingleInt32(col series.Series) bool {
	values, validity, ok := series.Int32ValuesWithValidity(col)
	if !ok {
		return false
	}

	keyToGroup := make(map[int32]uint64, len(values))
	var nullGroupID uint64
	hasNull := false
	nextID := uint64(0)

	for i, val := range values {
		if !validity[i] {
			if !hasNull {
				nullGroupID = nextID
				nextID++
				hasNull = true
				gb.groupKeys[nullGroupID] = []interface{}{nil}
				gb.groupOrder = append(gb.groupOrder, nullGroupID)
			}
			gb.groups[nullGroupID] = append(gb.groups[nullGroupID], i)
			continue
		}

		groupID, exists := keyToGroup[val]
		if !exists {
			groupID = nextID
			nextID++
			keyToGroup[val] = groupID
			gb.groupKeys[groupID] = []interface{}{val}
			gb.groupOrder = append(gb.groupOrder, groupID)
		}
		gb.groups[groupID] = append(gb.groups[groupID], i)
	}

	return true
}

func (gb *GroupBy) buildGroupsSingleUint64(col series.Series) bool {
	values, validity, ok := series.Uint64ValuesWithValidity(col)
	if !ok {
		return false
	}

	keyToGroup := make(map[uint64]uint64, len(values))
	var nullGroupID uint64
	hasNull := false
	nextID := uint64(0)

	for i, val := range values {
		if !validity[i] {
			if !hasNull {
				nullGroupID = nextID
				nextID++
				hasNull = true
				gb.groupKeys[nullGroupID] = []interface{}{nil}
				gb.groupOrder = append(gb.groupOrder, nullGroupID)
			}
			gb.groups[nullGroupID] = append(gb.groups[nullGroupID], i)
			continue
		}

		groupID, exists := keyToGroup[val]
		if !exists {
			groupID = nextID
			nextID++
			keyToGroup[val] = groupID
			gb.groupKeys[groupID] = []interface{}{val}
			gb.groupOrder = append(gb.groupOrder, groupID)
		}
		gb.groups[groupID] = append(gb.groups[groupID], i)
	}

	return true
}

func (gb *GroupBy) buildGroupsSingleUint32(col series.Series) bool {
	values, validity, ok := series.Uint32ValuesWithValidity(col)
	if !ok {
		return false
	}

	keyToGroup := make(map[uint32]uint64, len(values))
	var nullGroupID uint64
	hasNull := false
	nextID := uint64(0)

	for i, val := range values {
		if !validity[i] {
			if !hasNull {
				nullGroupID = nextID
				nextID++
				hasNull = true
				gb.groupKeys[nullGroupID] = []interface{}{nil}
				gb.groupOrder = append(gb.groupOrder, nullGroupID)
			}
			gb.groups[nullGroupID] = append(gb.groups[nullGroupID], i)
			continue
		}

		groupID, exists := keyToGroup[val]
		if !exists {
			groupID = nextID
			nextID++
			keyToGroup[val] = groupID
			gb.groupKeys[groupID] = []interface{}{val}
			gb.groupOrder = append(gb.groupOrder, groupID)
		}
		gb.groups[groupID] = append(gb.groups[groupID], i)
	}

	return true
}

func (gb *GroupBy) buildGroupsSingleFloat64(col series.Series) bool {
	values, validity, ok := series.Float64ValuesWithValidity(col)
	if !ok {
		return false
	}

	keyToGroup := make(map[uint64]uint64, len(values))
	var nullGroupID uint64
	hasNull := false
	nextID := uint64(0)

	for i, val := range values {
		if !validity[i] {
			if !hasNull {
				nullGroupID = nextID
				nextID++
				hasNull = true
				gb.groupKeys[nullGroupID] = []interface{}{nil}
				gb.groupOrder = append(gb.groupOrder, nullGroupID)
			}
			gb.groups[nullGroupID] = append(gb.groups[nullGroupID], i)
			continue
		}

		key := math.Float64bits(val)
		groupID, exists := keyToGroup[key]
		if !exists {
			groupID = nextID
			nextID++
			keyToGroup[key] = groupID
			gb.groupKeys[groupID] = []interface{}{val}
			gb.groupOrder = append(gb.groupOrder, groupID)
		}
		gb.groups[groupID] = append(gb.groups[groupID], i)
	}

	return true
}

func (gb *GroupBy) buildGroupsSingleFloat32(col series.Series) bool {
	values, validity, ok := series.Float32ValuesWithValidity(col)
	if !ok {
		return false
	}

	keyToGroup := make(map[uint32]uint64, len(values))
	var nullGroupID uint64
	hasNull := false
	nextID := uint64(0)

	for i, val := range values {
		if !validity[i] {
			if !hasNull {
				nullGroupID = nextID
				nextID++
				hasNull = true
				gb.groupKeys[nullGroupID] = []interface{}{nil}
				gb.groupOrder = append(gb.groupOrder, nullGroupID)
			}
			gb.groups[nullGroupID] = append(gb.groups[nullGroupID], i)
			continue
		}

		key := math.Float32bits(val)
		groupID, exists := keyToGroup[key]
		if !exists {
			groupID = nextID
			nextID++
			keyToGroup[key] = groupID
			gb.groupKeys[groupID] = []interface{}{val}
			gb.groupOrder = append(gb.groupOrder, groupID)
		}
		gb.groups[groupID] = append(gb.groups[groupID], i)
	}

	return true
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
