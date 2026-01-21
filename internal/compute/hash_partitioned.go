package compute

import (
	"fmt"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/internal/parallel"
	"github.com/tnn1t1s/golars/series"
)

type joinKeyKind uint8

const (
	joinInt64 joinKeyKind = iota
	joinInt32
	joinUint64
	joinUint32
	joinFloat64
	joinFloat32
	joinString
)

const nullHash = uint64(0x9e3779b97f4a7c15)

type joinKeyColumn struct {
	kind     joinKeyKind
	i64      []int64
	i32      []int32
	u64      []uint64
	u32      []uint32
	f64      []float64
	f32      []float32
	str      []string
	validity []bool
}

func buildJoinKeyColumns(keys []series.Series) ([]joinKeyColumn, bool, error) {
	if len(keys) == 0 {
		return nil, false, fmt.Errorf("join keys required")
	}

	cols := make([]joinKeyColumn, len(keys))
	for i, key := range keys {
		switch key.DataType().(type) {
		case datatypes.Int64:
			values, validity := int64ValuesFromSeries(key)
			cols[i] = joinKeyColumn{kind: joinInt64, i64: values, validity: validity}
		case datatypes.Int32:
			values, validity := int32ValuesFromSeries(key)
			cols[i] = joinKeyColumn{kind: joinInt32, i32: values, validity: validity}
		case datatypes.UInt64:
			values, validity := uint64ValuesFromSeries(key)
			cols[i] = joinKeyColumn{kind: joinUint64, u64: values, validity: validity}
		case datatypes.UInt32:
			values, validity := uint32ValuesFromSeries(key)
			cols[i] = joinKeyColumn{kind: joinUint32, u32: values, validity: validity}
		case datatypes.Float64:
			values, validity := float64ValuesFromSeries(key)
			cols[i] = joinKeyColumn{kind: joinFloat64, f64: values, validity: validity}
		case datatypes.Float32:
			values, validity := float32ValuesFromSeries(key)
			cols[i] = joinKeyColumn{kind: joinFloat32, f32: values, validity: validity}
		case datatypes.String:
			values, validity := stringValuesFromSeries(key)
			cols[i] = joinKeyColumn{kind: joinString, str: values, validity: validity}
		default:
			return nil, false, nil
		}
	}

	return cols, true, nil
}

func joinKeyCount(cols []joinKeyColumn) int {
	if len(cols) == 0 {
		return 0
	}
	return len(cols[0].validity)
}

func hashRow(cols []joinKeyColumn, row int) uint64 {
	h := uint64(wyp3)
	for _, col := range cols {
		if !col.validity[row] {
			h = wymix(h, nullHash)
			continue
		}
		switch col.kind {
		case joinInt64:
			h = wymix(h, uint64(col.i64[row]))
		case joinInt32:
			h = wymix(h, uint64(col.i32[row]))
		case joinUint64:
			h = wymix(h, col.u64[row])
		case joinUint32:
			h = wymix(h, uint64(col.u32[row]))
		case joinFloat64:
			h = wymix(h, hashFloat64(col.f64[row]))
		case joinFloat32:
			h = wymix(h, hashFloat32(col.f32[row]))
		case joinString:
			h = wymix(h, hashString(col.str[row]))
		}
	}
	return h
}

func joinKeysEqual(leftCols, rightCols []joinKeyColumn, leftRow, rightRow int) bool {
	if len(leftCols) != len(rightCols) {
		return false
	}
	for i, left := range leftCols {
		right := rightCols[i]
		if left.kind != right.kind {
			return false
		}
		if left.validity[leftRow] != right.validity[rightRow] {
			return false
		}
		if !left.validity[leftRow] {
			continue
		}
		switch left.kind {
		case joinInt64:
			if left.i64[leftRow] != right.i64[rightRow] {
				return false
			}
		case joinInt32:
			if left.i32[leftRow] != right.i32[rightRow] {
				return false
			}
		case joinUint64:
			if left.u64[leftRow] != right.u64[rightRow] {
				return false
			}
		case joinUint32:
			if left.u32[leftRow] != right.u32[rightRow] {
				return false
			}
		case joinFloat64:
			if left.f64[leftRow] != right.f64[rightRow] {
				return false
			}
		case joinFloat32:
			if left.f32[leftRow] != right.f32[rightRow] {
				return false
			}
		case joinString:
			if left.str[leftRow] != right.str[rightRow] {
				return false
			}
		}
	}
	return true
}

type partitionedHashTable struct {
	tables []map[uint64][]int
	mask   uint64
	cols   []joinKeyColumn
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func buildPartitionedHashTable(cols []joinKeyColumn) *partitionedHashTable {
	rows := joinKeyCount(cols)
	partitions := nextPow2(parallel.MaxThreads() * 2)
	if partitions > rows && rows > 0 {
		partitions = nextPow2(rows)
	}
	if partitions < 1 {
		partitions = 1
	}
	mask := uint64(partitions - 1)

	tables := make([]map[uint64][]int, partitions)
	est := 0
	if partitions > 0 {
		est = rows / partitions
	}
	for i := range tables {
		tables[i] = make(map[uint64][]int, est)
	}

	for i := 0; i < rows; i++ {
		h := hashRow(cols, i)
		p := int(h & mask)
		tables[p][h] = append(tables[p][h], i)
	}

	return &partitionedHashTable{
		tables: tables,
		mask:   mask,
		cols:   cols,
	}
}

func partitionedInnerJoin(leftCols, rightCols []joinKeyColumn) ([]int, []int) {
	leftRows := joinKeyCount(leftCols)
	rightRows := joinKeyCount(rightCols)

	buildCols := leftCols
	probeCols := rightCols
	swapped := false
	if rightRows < leftRows {
		buildCols = rightCols
		probeCols = leftCols
		swapped = true
	}

	ht := buildPartitionedHashTable(buildCols)
	probeRows := joinKeyCount(probeCols)

	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > probeRows {
		chunks = probeRows
	}
	if chunks <= 1 {
		leftIndices, rightIndices := probePartition(ht, buildCols, probeCols, 0, probeRows, swapped)
		return leftIndices, rightIndices
	}
	chunkSize := (probeRows + chunks - 1) / chunks

	type part struct {
		left  []int
		right []int
	}
	parts := make([]part, chunks)

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= probeRows {
				continue
			}
			limit := offset + chunkSize
			if limit > probeRows {
				limit = probeRows
			}
			leftIdx, rightIdx := probePartition(ht, buildCols, probeCols, offset, limit, swapped)
			parts[idx] = part{left: leftIdx, right: rightIdx}
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

func probePartition(
	ht *partitionedHashTable,
	buildCols []joinKeyColumn,
	probeCols []joinKeyColumn,
	start int,
	end int,
	swapped bool,
) ([]int, []int) {
	leftIndices := make([]int, 0, end-start)
	rightIndices := make([]int, 0, end-start)

	for i := start; i < end; i++ {
		h := hashRow(probeCols, i)
		candidates := ht.tables[int(h&ht.mask)][h]
		for _, candidate := range candidates {
			if !joinKeysEqual(buildCols, probeCols, candidate, i) {
				continue
			}
			if swapped {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, candidate)
			} else {
				leftIndices = append(leftIndices, candidate)
				rightIndices = append(rightIndices, i)
			}
		}
	}
	return leftIndices, rightIndices
}

func partitionedLeftJoin(leftCols, rightCols []joinKeyColumn) ([]int, []int) {
	ht := buildPartitionedHashTable(rightCols)
	leftRows := joinKeyCount(leftCols)

	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > leftRows {
		chunks = leftRows
	}
	if chunks <= 1 {
		return probeLeftPartition(ht, rightCols, leftCols, 0, leftRows)
	}
	chunkSize := (leftRows + chunks - 1) / chunks

	type part struct {
		left  []int
		right []int
	}
	parts := make([]part, chunks)

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= leftRows {
				continue
			}
			limit := offset + chunkSize
			if limit > leftRows {
				limit = leftRows
			}
			leftIdx, rightIdx := probeLeftPartition(ht, rightCols, leftCols, offset, limit)
			parts[idx] = part{left: leftIdx, right: rightIdx}
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

func probeLeftPartition(
	ht *partitionedHashTable,
	buildCols []joinKeyColumn,
	leftCols []joinKeyColumn,
	start int,
	end int,
) ([]int, []int) {
	leftIndices := make([]int, 0, end-start)
	rightIndices := make([]int, 0, end-start)

	for i := start; i < end; i++ {
		h := hashRow(leftCols, i)
		candidates := ht.tables[int(h&ht.mask)][h]
		if len(candidates) == 0 {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
			continue
		}
		matched := false
		for _, candidate := range candidates {
			if !joinKeysEqual(buildCols, leftCols, candidate, i) {
				continue
			}
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, candidate)
			matched = true
		}
		if !matched {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
		}
	}
	return leftIndices, rightIndices
}

func partitionedMatchExists(buildCols, probeCols []joinKeyColumn) []bool {
	ht := buildPartitionedHashTable(buildCols)
	probeRows := joinKeyCount(probeCols)
	matches := make([]bool, probeRows)

	chunks := parallel.MaxThreads() * 2
	if chunks < 1 {
		chunks = 1
	}
	if chunks > probeRows {
		chunks = probeRows
	}
	if chunks <= 1 {
		probeExistsRange(ht, buildCols, probeCols, 0, probeRows, matches)
		return matches
	}
	chunkSize := (probeRows + chunks - 1) / chunks

	_ = parallel.For(chunks, func(start, end int) error {
		for idx := start; idx < end; idx++ {
			offset := idx * chunkSize
			if offset >= probeRows {
				continue
			}
			limit := offset + chunkSize
			if limit > probeRows {
				limit = probeRows
			}
			probeExistsRange(ht, buildCols, probeCols, offset, limit, matches)
		}
		return nil
	})
	return matches
}

func probeExistsRange(
	ht *partitionedHashTable,
	buildCols []joinKeyColumn,
	probeCols []joinKeyColumn,
	start int,
	end int,
	out []bool,
) {
	for i := start; i < end; i++ {
		h := hashRow(probeCols, i)
		candidates := ht.tables[int(h&ht.mask)][h]
		for _, candidate := range candidates {
			if joinKeysEqual(buildCols, probeCols, candidate, i) {
				out[i] = true
				break
			}
		}
	}
}

// PartitionedInnerJoinIndices performs a typed partitioned hash join when possible.
func PartitionedInnerJoinIndices(leftKeys, rightKeys []series.Series) ([]int, []int, bool, error) {
	leftCols, ok, err := buildJoinKeyColumns(leftKeys)
	if err != nil {
		return nil, nil, false, err
	}
	if !ok {
		return nil, nil, false, nil
	}
	rightCols, ok, err := buildJoinKeyColumns(rightKeys)
	if err != nil {
		return nil, nil, false, err
	}
	if !ok {
		return nil, nil, false, nil
	}
	if len(leftCols) != len(rightCols) {
		return nil, nil, false, fmt.Errorf("join keys must have same length")
	}
	for i := range leftCols {
		if leftCols[i].kind != rightCols[i].kind {
			return nil, nil, false, fmt.Errorf("join key types must match")
		}
	}
	leftIdx, rightIdx := partitionedInnerJoin(leftCols, rightCols)
	return leftIdx, rightIdx, true, nil
}

// PartitionedLeftJoinIndices performs a typed partitioned left join when possible.
func PartitionedLeftJoinIndices(leftKeys, rightKeys []series.Series) ([]int, []int, bool, error) {
	leftCols, ok, err := buildJoinKeyColumns(leftKeys)
	if err != nil {
		return nil, nil, false, err
	}
	if !ok {
		return nil, nil, false, nil
	}
	rightCols, ok, err := buildJoinKeyColumns(rightKeys)
	if err != nil {
		return nil, nil, false, err
	}
	if !ok {
		return nil, nil, false, nil
	}
	if len(leftCols) != len(rightCols) {
		return nil, nil, false, fmt.Errorf("join keys must have same length")
	}
	for i := range leftCols {
		if leftCols[i].kind != rightCols[i].kind {
			return nil, nil, false, fmt.Errorf("join key types must match")
		}
	}
	leftIdx, rightIdx := partitionedLeftJoin(leftCols, rightCols)
	return leftIdx, rightIdx, true, nil
}

// PartitionedMatchExists returns a match mask for probe rows using a typed hash table when possible.
func PartitionedMatchExists(buildKeys, probeKeys []series.Series) ([]bool, bool, error) {
	buildCols, ok, err := buildJoinKeyColumns(buildKeys)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	probeCols, ok, err := buildJoinKeyColumns(probeKeys)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	if len(buildCols) != len(probeCols) {
		return nil, false, fmt.Errorf("join keys must have same length")
	}
	for i := range buildCols {
		if buildCols[i].kind != probeCols[i].kind {
			return nil, false, fmt.Errorf("join key types must match")
		}
	}
	matches := partitionedMatchExists(buildCols, probeCols)
	return matches, true, nil
}
