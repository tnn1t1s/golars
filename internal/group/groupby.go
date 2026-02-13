package group

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/tnn1t1s/golars/internal/datatypes"
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
	df          DataFrameInterface
	groupCols   []string
	groups      map[uint64][]int         // group hash -> row indices
	groupKeys   map[uint64][]interface{} // group hash -> key values
	groupOrder  []uint64                 // maintains order of first occurrence of each group
	rowGroupIDs []uint32                 // row -> compact group id (single-key fast path)
	mu          sync.RWMutex
}

// GroupKey represents a single group's key values
type GroupKey struct {
	Values []interface{}
	Hash   uint64
}

// NewGroupBy creates a new GroupBy from a DataFrame and group columns
func NewGroupBy(df DataFrameInterface, columns []string) (*GroupBy, error) {
	// Validate columns exist
	for _, col := range columns {
		if _, err := df.Column(col); err != nil {
			return nil, fmt.Errorf("column %s not found", col)
		}
	}

	gb := &GroupBy{
		df:         df,
		groupCols:  columns,
		groups:     make(map[uint64][]int),
		groupKeys:  make(map[uint64][]interface{}),
		groupOrder: nil,
	}

	// Build groups
	if err := gb.buildGroups(); err != nil {
		return nil, err
	}

	return gb, nil
}

// buildGroups creates the groups using Arrow compute.
func (gb *GroupBy) buildGroups() error {
	height := gb.df.Height()
	if height == 0 {
		return nil
	}

	// Get group series
	groupSeries := make([]series.Series, len(gb.groupCols))
	for i, colName := range gb.groupCols {
		col, err := gb.df.Column(colName)
		if err != nil {
			return err
		}
		groupSeries[i] = col
	}

	// Try single-column fast paths
	if len(groupSeries) == 1 {
		col := groupSeries[0]
		switch col.DataType().(type) {
		case datatypes.Int64:
			if gb.buildGroupsSingleInt64(col) {
				return nil
			}
		case datatypes.Int32:
			if gb.buildGroupsSingleInt32(col) {
				return nil
			}
		case datatypes.UInt64:
			if gb.buildGroupsSingleUint64(col) {
				return nil
			}
		case datatypes.UInt32:
			if gb.buildGroupsSingleUint32(col) {
				return nil
			}
		case datatypes.Float64:
			if gb.buildGroupsSingleFloat64(col) {
				return nil
			}
		case datatypes.Float32:
			if gb.buildGroupsSingleFloat32(col) {
				return nil
			}
		case datatypes.String:
			if gb.buildGroupsSingleString(col) {
				return nil
			}
		}
	}

	// General path: build key columns and hash
	keyCols, ok := buildGroupKeyColumns(groupSeries)
	if !ok {
		// Fallback to interface-based grouping
		return gb.buildGroupsGeneric(groupSeries)
	}

	// Compute hashes
	hashes := make([]uint64, height)
	h := fnv.New64a()
	for row := 0; row < height; row++ {
		h.Reset()
		for c := 0; c < len(keyCols); c++ {
			hashKeyColumnValue(h, &keyCols[c], row)
		}
		hashes[row] = h.Sum64()
	}

	if shouldParallelGroupBy(height) {
		return gb.buildGroupsParallelHashed(hashes, keyCols)
	}

	gb.buildGroupsHashed(hashes, keyCols)
	return nil
}

func arrowGroupKeyValue(arr arrow.Array, idx int) (interface{}, error) {
	if arr.IsNull(idx) {
		return nil, nil
	}
	switch a := arr.(type) {
	case *array.Int64:
		return a.Value(idx), nil
	case *array.Int32:
		return a.Value(idx), nil
	case *array.Int16:
		return a.Value(idx), nil
	case *array.Int8:
		return a.Value(idx), nil
	case *array.Uint64:
		return a.Value(idx), nil
	case *array.Uint32:
		return a.Value(idx), nil
	case *array.Uint16:
		return a.Value(idx), nil
	case *array.Uint8:
		return a.Value(idx), nil
	case *array.Float64:
		return a.Value(idx), nil
	case *array.Float32:
		return a.Value(idx), nil
	case *array.String:
		return a.Value(idx), nil
	case *array.Boolean:
		return a.Value(idx), nil
	default:
		return nil, fmt.Errorf("unsupported arrow type: %T", arr)
	}
}

type groupPartial struct {
	groups map[uint64][]int
	keys   map[uint64][]interface{}
	first  map[uint64]int
}

type groupPartialHashed struct {
	groups map[uint64][]int
	first  map[uint64]int
}

type groupKeyColumn struct {
	dtype       datatypes.DataType
	validity    []bool
	int64Vals   []int64
	int32Vals   []int32
	int16Vals   []int16
	int8Vals    []int8
	uint64Vals  []uint64
	uint32Vals  []uint32
	uint16Vals  []uint16
	uint8Vals   []uint8
	float64Vals []float64
	float32Vals []float32
	stringVals  []string
	boolVals    []bool
}

func (gb *GroupBy) buildGroupsHashed(hashes []uint64, keyCols []groupKeyColumn) {
	height := gb.df.Height()
	for row := 0; row < height; row++ {
		h := hashes[row]
		if _, exists := gb.groups[h]; !exists {
			gb.groupKeys[h] = extractGroupKeyValues(keyCols, row)
			gb.groupOrder = append(gb.groupOrder, h)
		}
		gb.groups[h] = append(gb.groups[h], row)
	}
}

func (gb *GroupBy) buildGroupsParallel(groupSeries []series.Series) error {
	// For the parallel path, just use generic single-threaded for now
	return gb.buildGroupsGeneric(groupSeries)
}

func (gb *GroupBy) buildGroupsParallelHashed(hashes []uint64, keyCols []groupKeyColumn) error {
	height := gb.df.Height()
	nThreads := parallel.MaxThreads()
	if nThreads < 2 {
		gb.buildGroupsHashed(hashes, keyCols)
		return nil
	}

	chunkSize := (height + nThreads - 1) / nThreads
	partials := make([]groupPartialHashed, nThreads)

	var wg sync.WaitGroup
	for t := 0; t < nThreads; t++ {
		start := t * chunkSize
		end := start + chunkSize
		if end > height {
			end = height
		}
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(tid, s, e int) {
			defer wg.Done()
			p := groupPartialHashed{
				groups: make(map[uint64][]int),
				first:  make(map[uint64]int),
			}
			for row := s; row < e; row++ {
				h := hashes[row]
				if _, exists := p.groups[h]; !exists {
					p.first[h] = row
				}
				p.groups[h] = append(p.groups[h], row)
			}
			partials[tid] = p
		}(t, start, end)
	}
	wg.Wait()

	// Merge partials
	for _, p := range partials {
		for h, indices := range p.groups {
			if _, exists := gb.groups[h]; !exists {
				gb.groupKeys[h] = extractGroupKeyValues(keyCols, p.first[h])
				gb.groupOrder = append(gb.groupOrder, h)
			}
			gb.groups[h] = append(gb.groups[h], indices...)
		}
	}
	return nil
}

func shouldParallelGroupBy(rows int) bool {
	return parallel.Enabled() && rows >= 50000
}

func (gb *GroupBy) buildGroupsSingleInt64(col series.Series) bool {
	values, validity, ok := series.Int64ValuesWithValidity(col)
	if !ok {
		return false
	}
	buildGroupsSingleTypedHelper(gb,values, validity, func(v int64) uint64 { return uint64(v) }, func(v int64) interface{} { return v })
	return true
}

func (gb *GroupBy) buildGroupsSingleInt32(col series.Series) bool {
	values, validity, ok := series.Int32ValuesWithValidity(col)
	if !ok {
		return false
	}
	buildGroupsSingleTypedHelper(gb,values, validity, func(v int32) uint64 { return uint64(v) }, func(v int32) interface{} { return v })
	return true
}

func (gb *GroupBy) buildGroupsSingleUint64(col series.Series) bool {
	values, validity, ok := series.Uint64ValuesWithValidity(col)
	if !ok {
		return false
	}
	buildGroupsSingleTypedHelper(gb,values, validity, func(v uint64) uint64 { return v }, func(v uint64) interface{} { return v })
	return true
}

func (gb *GroupBy) buildGroupsSingleUint32(col series.Series) bool {
	values, validity, ok := series.Uint32ValuesWithValidity(col)
	if !ok {
		return false
	}
	buildGroupsSingleTypedHelper(gb,values, validity, func(v uint32) uint64 { return uint64(v) }, func(v uint32) interface{} { return v })
	return true
}

func (gb *GroupBy) buildGroupsSingleFloat64(col series.Series) bool {
	values, validity, ok := series.Float64ValuesWithValidity(col)
	if !ok {
		return false
	}
	buildGroupsSingleTypedHelper(gb,values, validity, func(v float64) uint64 { return float64ToUint64(v) }, func(v float64) interface{} { return v })
	return true
}

func (gb *GroupBy) buildGroupsSingleFloat32(col series.Series) bool {
	values, validity, ok := series.Float32ValuesWithValidity(col)
	if !ok {
		return false
	}
	buildGroupsSingleTypedHelper(gb,values, validity, func(v float32) uint64 { return uint64(float32ToUint32(v)) }, func(v float32) interface{} { return v })
	return true
}

func (gb *GroupBy) buildGroupsSingleString(col series.Series) bool {
	values, validity, ok := series.StringValuesWithValidity(col)
	if !ok {
		return false
	}
	h := fnv.New64a()
	buildGroupsSingleTypedHelper(gb,values, validity, func(v string) uint64 {
		h.Reset()
		h.Write([]byte(v))
		return h.Sum64()
	}, func(v string) interface{} { return v })
	return true
}

func buildGroupsSingleTypedHelper[T any](gb *GroupBy, values []T, validity []bool, hashFn func(T) uint64, toIface func(T) interface{}) {
	n := len(values)
	groupID := uint32(0)
	gb.rowGroupIDs = make([]uint32, n)
	hashToGroupID := make(map[uint64]uint32)

	for i := 0; i < n; i++ {
		if validity != nil && !validity[i] {
			// null value - use special hash
			h := uint64(0)
			if _, exists := gb.groups[h]; !exists {
				hashToGroupID[h] = groupID
				gb.groupKeys[h] = []interface{}{nil}
				gb.groupOrder = append(gb.groupOrder, h)
				groupID++
			}
			gb.groups[h] = append(gb.groups[h], i)
			gb.rowGroupIDs[i] = hashToGroupID[h]
			continue
		}
		h := hashFn(values[i])
		if _, exists := gb.groups[h]; !exists {
			hashToGroupID[h] = groupID
			gb.groupKeys[h] = []interface{}{toIface(values[i])}
			gb.groupOrder = append(gb.groupOrder, h)
			groupID++
		}
		gb.groups[h] = append(gb.groups[h], i)
		gb.rowGroupIDs[i] = hashToGroupID[h]
	}
}


// buildGroupsGeneric is the fallback using interface-based key extraction
func (gb *GroupBy) buildGroupsGeneric(groupSeries []series.Series) error {
	height := gb.df.Height()
	for row := 0; row < height; row++ {
		gk := gb.getGroupKey(groupSeries, row)
		if _, exists := gb.groups[gk.Hash]; !exists {
			gb.groupKeys[gk.Hash] = gk.Values
			gb.groupOrder = append(gb.groupOrder, gk.Hash)
		}
		gb.groups[gk.Hash] = append(gb.groups[gk.Hash], row)
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

func buildGroupKeyColumns(cols []series.Series) ([]groupKeyColumn, bool) {
	result := make([]groupKeyColumn, len(cols))
	for i, col := range cols {
		kc := groupKeyColumn{
			dtype: col.DataType(),
		}
		switch col.DataType().(type) {
		case datatypes.Int64:
			vals, validity, ok := series.Int64ValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.int64Vals = vals
			kc.validity = validity
		case datatypes.Int32:
			vals, validity, ok := series.Int32ValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.int32Vals = vals
			kc.validity = validity
		case datatypes.Int16:
			vals, validity, ok := series.Int16ValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.int16Vals = vals
			kc.validity = validity
		case datatypes.Int8:
			vals, validity, ok := series.Int8ValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.int8Vals = vals
			kc.validity = validity
		case datatypes.UInt64:
			vals, validity, ok := series.Uint64ValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.uint64Vals = vals
			kc.validity = validity
		case datatypes.UInt32:
			vals, validity, ok := series.Uint32ValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.uint32Vals = vals
			kc.validity = validity
		case datatypes.UInt16:
			vals, validity, ok := series.Uint16ValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.uint16Vals = vals
			kc.validity = validity
		case datatypes.UInt8:
			vals, validity, ok := series.Uint8ValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.uint8Vals = vals
			kc.validity = validity
		case datatypes.Float64:
			vals, validity, ok := series.Float64ValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.float64Vals = vals
			kc.validity = validity
		case datatypes.Float32:
			vals, validity, ok := series.Float32ValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.float32Vals = vals
			kc.validity = validity
		case datatypes.String:
			vals, validity, ok := series.StringValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.stringVals = vals
			kc.validity = validity
		case datatypes.Boolean:
			vals, validity, ok := series.BoolValuesWithValidity(col)
			if !ok {
				return nil, false
			}
			kc.boolVals = vals
			kc.validity = validity
		default:
			return nil, false
		}
		result[i] = kc
	}
	return result, true
}

func extractGroupKeyValues(cols []groupKeyColumn, row int) []interface{} {
	result := make([]interface{}, len(cols))
	for i, col := range cols {
		if col.validity != nil && !col.validity[row] {
			result[i] = nil
			continue
		}
		switch col.dtype.(type) {
		case datatypes.Int64:
			result[i] = col.int64Vals[row]
		case datatypes.Int32:
			result[i] = col.int32Vals[row]
		case datatypes.Int16:
			result[i] = col.int16Vals[row]
		case datatypes.Int8:
			result[i] = col.int8Vals[row]
		case datatypes.UInt64:
			result[i] = col.uint64Vals[row]
		case datatypes.UInt32:
			result[i] = col.uint32Vals[row]
		case datatypes.UInt16:
			result[i] = col.uint16Vals[row]
		case datatypes.UInt8:
			result[i] = col.uint8Vals[row]
		case datatypes.Float64:
			result[i] = col.float64Vals[row]
		case datatypes.Float32:
			result[i] = col.float32Vals[row]
		case datatypes.String:
			result[i] = col.stringVals[row]
		case datatypes.Boolean:
			result[i] = col.boolVals[row]
		default:
			result[i] = nil
		}
	}
	return result
}

func hashKeyColumnValue(h hash.Hash64, col *groupKeyColumn, row int) {
	if col.validity != nil && !col.validity[row] {
		h.Write([]byte{0})
		return
	}
	var buf [8]byte
	switch col.dtype.(type) {
	case datatypes.Int64:
		binary.LittleEndian.PutUint64(buf[:], uint64(col.int64Vals[row]))
		h.Write(buf[:])
	case datatypes.Int32:
		binary.LittleEndian.PutUint32(buf[:4], uint32(col.int32Vals[row]))
		h.Write(buf[:4])
	case datatypes.Int16:
		binary.LittleEndian.PutUint16(buf[:2], uint16(col.int16Vals[row]))
		h.Write(buf[:2])
	case datatypes.Int8:
		buf[0] = byte(col.int8Vals[row])
		h.Write(buf[:1])
	case datatypes.UInt64:
		binary.LittleEndian.PutUint64(buf[:], col.uint64Vals[row])
		h.Write(buf[:])
	case datatypes.UInt32:
		binary.LittleEndian.PutUint32(buf[:4], col.uint32Vals[row])
		h.Write(buf[:4])
	case datatypes.UInt16:
		binary.LittleEndian.PutUint16(buf[:2], col.uint16Vals[row])
		h.Write(buf[:2])
	case datatypes.UInt8:
		buf[0] = col.uint8Vals[row]
		h.Write(buf[:1])
	case datatypes.Float64:
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(col.float64Vals[row]))
		h.Write(buf[:])
	case datatypes.Float32:
		binary.LittleEndian.PutUint32(buf[:4], math.Float32bits(col.float32Vals[row]))
		h.Write(buf[:4])
	case datatypes.String:
		h.Write([]byte(col.stringVals[row]))
	case datatypes.Boolean:
		if col.boolVals[row] {
			buf[0] = 1
		} else {
			buf[0] = 0
		}
		h.Write(buf[:1])
	}
}

// hashValue hashes a single value
func (gb *GroupBy) hashValue(h hash.Hash64, val interface{}) {
	if val == nil {
		h.Write([]byte{0})
		return
	}
	var buf [8]byte
	switch v := val.(type) {
	case int64:
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		h.Write(buf[:])
	case int32:
		binary.LittleEndian.PutUint32(buf[:4], uint32(v))
		h.Write(buf[:4])
	case int16:
		binary.LittleEndian.PutUint16(buf[:2], uint16(v))
		h.Write(buf[:2])
	case int8:
		buf[0] = byte(v)
		h.Write(buf[:1])
	case uint64:
		binary.LittleEndian.PutUint64(buf[:], v)
		h.Write(buf[:])
	case uint32:
		binary.LittleEndian.PutUint32(buf[:4], v)
		h.Write(buf[:4])
	case uint16:
		binary.LittleEndian.PutUint16(buf[:2], v)
		h.Write(buf[:2])
	case uint8:
		buf[0] = v
		h.Write(buf[:1])
	case float64:
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
		h.Write(buf[:])
	case float32:
		binary.LittleEndian.PutUint32(buf[:4], math.Float32bits(v))
		h.Write(buf[:4])
	case string:
		h.Write([]byte(v))
	case bool:
		if v {
			buf[0] = 1
		} else {
			buf[0] = 0
		}
		h.Write(buf[:1])
	default:
		// Fallback to string representation
		h.Write([]byte(fmt.Sprintf("%v", v)))
	}
}

// Groups returns the number of groups
func (gb *GroupBy) Groups() int {
	gb.mu.RLock()
	defer gb.mu.RUnlock()
	return len(gb.groupOrder)
}

// GetGroup returns the row indices for a specific group
func (gb *GroupBy) GetGroup(hash uint64) ([]int, bool) {
	gb.mu.RLock()
	defer gb.mu.RUnlock()
	indices, ok := gb.groups[hash]
	return indices, ok
}

// helper functions for float to byte conversion
func float32ToBytes(f float32) [4]byte {
	bits := math.Float32bits(f)
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], bits)
	return buf
}

func float64ToBytes(f float64) [8]byte {
	bits := math.Float64bits(f)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], bits)
	return buf
}

// Type conversion helpers
func float32ToUint32(f float32) uint32 {
	return math.Float32bits(f)
}

func float64ToUint64(f float64) uint64 {
	return math.Float64bits(f)
}

func stringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

// ensure unused imports are used
var _ = array.NewBooleanBuilder
