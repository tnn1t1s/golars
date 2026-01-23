package group

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func hashGroupByChunkedIndicesMulti(keys []*arrow.Chunked) ([]uint32, []arrow.Array, error) {
	if len(keys) == 0 {
		return nil, nil, fmt.Errorf("groupby requires at least one key column")
	}
	mem := memory.NewGoAllocator()
	arrs := make([]arrow.Array, len(keys))
	for i, key := range keys {
		if key == nil {
			return nil, nil, fmt.Errorf("groupby keys chunked array is nil")
		}
		concat, err := array.Concatenate(key.Chunks(), mem)
		if err != nil {
			return nil, nil, err
		}
		arrs[i] = concat
	}
	defer func() {
		for _, arr := range arrs {
			arr.Release()
		}
	}()

	return hashGroupByIndicesMulti(arrs, mem)
}

func hashGroupByIndicesMulti(keys []arrow.Array, mem memory.Allocator) ([]uint32, []arrow.Array, error) {
	if len(keys) == 0 {
		return nil, nil, fmt.Errorf("groupby requires at least one key column")
	}

	cols := make([]arrowGroupKeyColumn, len(keys))
	for i, arr := range keys {
		col, err := newArrowGroupKeyColumn(arr, mem)
		if err != nil {
			return nil, nil, err
		}
		if arr.Len() != keys[0].Len() {
			return nil, nil, fmt.Errorf("groupby key columns length mismatch")
		}
		cols[i] = col
	}

	groupIDs := make([]uint32, keys[0].Len())
	keyToGroup := make(map[string]uint32, keys[0].Len())
	nextID := uint32(0)

	for i := 0; i < keys[0].Len(); i++ {
		var sb strings.Builder
		for _, col := range cols {
			col.appendEncoded(&sb, i)
		}
		key := sb.String()
		if gid, ok := keyToGroup[key]; ok {
			groupIDs[i] = gid
			continue
		}
		gid := nextID
		nextID++
		keyToGroup[key] = gid
		groupIDs[i] = gid
		for idx := range cols {
			cols[idx].appendValue(i)
		}
	}

	keyArrays := make([]arrow.Array, len(cols))
	for i, col := range cols {
		keyArrays[i] = col.finish()
	}

	return groupIDs, keyArrays, nil
}

type arrowGroupKeyColumn struct {
	kind    string
	i64     *array.Int64
	i32     *array.Int32
	str     *array.String
	builder array.Builder
}

func newArrowGroupKeyColumn(arr arrow.Array, mem memory.Allocator) (arrowGroupKeyColumn, error) {
	switch typed := arr.(type) {
	case *array.Int64:
		return arrowGroupKeyColumn{kind: "i64", i64: typed, builder: array.NewInt64Builder(mem)}, nil
	case *array.Int32:
		return arrowGroupKeyColumn{kind: "i32", i32: typed, builder: array.NewInt32Builder(mem)}, nil
	case *array.String:
		return arrowGroupKeyColumn{kind: "str", str: typed, builder: array.NewStringBuilder(mem)}, nil
	default:
		return arrowGroupKeyColumn{}, fmt.Errorf("groupby unsupported key type %s", arr.DataType().String())
	}
}

func (c arrowGroupKeyColumn) isNull(i int) bool {
	switch c.kind {
	case "i64":
		return c.i64.IsNull(i)
	case "i32":
		return c.i32.IsNull(i)
	case "str":
		return c.str.IsNull(i)
	default:
		return true
	}
}

func (c arrowGroupKeyColumn) appendEncoded(b *strings.Builder, i int) {
	if c.isNull(i) {
		b.WriteString("null;")
		return
	}
	switch c.kind {
	case "i64":
		b.WriteString("i64:")
		b.WriteString(strconv.FormatInt(c.i64.Value(i), 10))
		b.WriteByte(';')
	case "i32":
		b.WriteString("i32:")
		b.WriteString(strconv.FormatInt(int64(c.i32.Value(i)), 10))
		b.WriteByte(';')
	case "str":
		val := c.str.Value(i)
		b.WriteString("str:")
		b.WriteString(strconv.Itoa(len(val)))
		b.WriteByte(':')
		b.WriteString(val)
		b.WriteByte(';')
	}
}

func (c arrowGroupKeyColumn) appendValue(i int) {
	if c.isNull(i) {
		c.builder.AppendNull()
		return
	}
	switch c.kind {
	case "i64":
		c.builder.(*array.Int64Builder).Append(c.i64.Value(i))
	case "i32":
		c.builder.(*array.Int32Builder).Append(c.i32.Value(i))
	case "str":
		c.builder.(*array.StringBuilder).Append(c.str.Value(i))
	}
}

func (c arrowGroupKeyColumn) finish() arrow.Array {
	arr := c.builder.NewArray()
	c.builder.Release()
	return arr
}
