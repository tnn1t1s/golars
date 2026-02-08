package frame

import (
	_ "fmt"
	_ "reflect"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Explode expands list-like values into multiple rows.
func (df *DataFrame) Explode(column string) (*DataFrame, error) {
	panic("not implemented")

}

type columnBuffer struct {
	name     string
	dtype    datatypes.DataType
	values   interface{}
	validity []bool
}

func newColumnBuffer(name string, dtype datatypes.DataType, size int) (*columnBuffer, error) {
	panic("not implemented")

}

func (b *columnBuffer) set(idx int, value interface{}) error {
	panic("not implemented")

}

func (b *columnBuffer) build() (series.Series, error) {
	panic("not implemented")

}

func listLength(value interface{}) (int, error) {
	panic("not implemented")

}

func listValue(value interface{}, idx int) (interface{}, error) {
	panic("not implemented")

}
