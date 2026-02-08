package group

import (
	_ "fmt"
	_ "strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func hashGroupByChunkedIndicesMulti(keys []*arrow.Chunked) ([]uint32, []arrow.Array, error) {
	panic("not implemented")

}

func hashGroupByIndicesMulti(keys []arrow.Array, mem memory.Allocator) ([]uint32, []arrow.Array, error) {
	panic("not implemented")

}

type arrowGroupKeyColumn struct {
	kind    string
	i64     *array.Int64
	i32     *array.Int32
	str     *array.String
	builder array.Builder
}

func newArrowGroupKeyColumn(arr arrow.Array, mem memory.Allocator) (arrowGroupKeyColumn, error) {
	panic("not implemented")

}

func (c arrowGroupKeyColumn) isNull(i int) bool {
	panic("not implemented")

}

func (c arrowGroupKeyColumn) appendEncoded(b *strings.Builder, i int) {
	panic("not implemented")

}

func (c arrowGroupKeyColumn) appendValue(i int) {
	panic("not implemented")

}

func (c arrowGroupKeyColumn) finish() arrow.Array {
	panic("not implemented")

}
