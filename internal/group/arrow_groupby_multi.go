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
		return nil, nil, fmt.Errorf("no keys provided")
	}

	// Flatten chunks into single arrays
	mem := memory.DefaultAllocator
	flatKeys := make([]arrow.Array, len(keys))
	for i, key := range keys {
		if key.Len() == 0 {
			return nil, nil, fmt.Errorf("empty key column")
		}
		chunks := key.Chunks()
		if len(chunks) == 1 {
			flatKeys[i] = chunks[0]
		} else {
			// Concatenate chunks
			concat, err := array.Concatenate(chunks, mem)
			if err != nil {
				return nil, nil, err
			}
			defer concat.Release()
			flatKeys[i] = concat
		}
	}

	return hashGroupByIndicesMulti(flatKeys, mem)
}

func hashGroupByIndicesMulti(keys []arrow.Array, mem memory.Allocator) ([]uint32, []arrow.Array, error) {
	if len(keys) == 0 {
		return nil, nil, fmt.Errorf("no keys provided")
	}

	n := keys[0].Len()
	cols := make([]arrowGroupKeyColumn, len(keys))
	for i, key := range keys {
		c, err := newArrowGroupKeyColumn(key, mem)
		if err != nil {
			return nil, nil, err
		}
		cols[i] = c
	}

	// Build encoded key -> group ID mapping
	groupIDs := make([]uint32, n)
	keyMap := make(map[string]uint32)
	var nextGroupID uint32
	var buf strings.Builder

	for row := 0; row < n; row++ {
		buf.Reset()
		for _, c := range cols {
			c.appendEncoded(&buf, row)
			buf.WriteByte(0)
		}
		key := buf.String()

		gid, exists := keyMap[key]
		if !exists {
			gid = nextGroupID
			keyMap[key] = gid
			nextGroupID++
			for _, c := range cols {
				c.appendValue(row)
			}
		}
		groupIDs[row] = gid
	}

	uniqueKeys := make([]arrow.Array, len(cols))
	for i, c := range cols {
		uniqueKeys[i] = c.finish()
	}

	return groupIDs, uniqueKeys, nil
}

type arrowGroupKeyColumn struct {
	kind    string
	i64     *array.Int64
	i32     *array.Int32
	str     *array.String
	builder array.Builder
}

func newArrowGroupKeyColumn(arr arrow.Array, mem memory.Allocator) (arrowGroupKeyColumn, error) {
	c := arrowGroupKeyColumn{}
	switch a := arr.(type) {
	case *array.Int64:
		c.kind = "i64"
		c.i64 = a
		c.builder = array.NewInt64Builder(mem)
	case *array.Int32:
		c.kind = "i32"
		c.i32 = a
		c.builder = array.NewInt32Builder(mem)
	case *array.String:
		c.kind = "str"
		c.str = a
		c.builder = array.NewStringBuilder(mem)
	default:
		return c, fmt.Errorf("unsupported key type: %T", arr)
	}
	return c, nil
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
		b.WriteString("N")
		return
	}
	switch c.kind {
	case "i64":
		b.WriteString(strconv.FormatInt(c.i64.Value(i), 10))
	case "i32":
		b.WriteString(strconv.FormatInt(int64(c.i32.Value(i)), 10))
	case "str":
		b.WriteString(c.str.Value(i))
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
	return c.builder.NewArray()
}
