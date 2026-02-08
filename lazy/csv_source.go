package lazy

import (
	_ "path/filepath"
	"sync"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
	gio "github.com/tnn1t1s/golars/io"
)

// CSVSource loads a CSV file on demand for lazy execution.
type CSVSource struct {
	Path    string
	Options []gio.CSVReadOption

	loadOnce sync.Once
	loadErr  error
	frame    *frame.DataFrame
}

// NewCSVSource creates a CSV-backed data source.
func NewCSVSource(path string, options ...gio.CSVReadOption) *CSVSource {
	panic("not implemented")

}

func (s *CSVSource) Name() string {
	panic("not implemented")

}

func (s *CSVSource) Schema() (*datatypes.Schema, error) {
	panic("not implemented")

}

func (s *CSVSource) DataFrame() (*frame.DataFrame, error) {
	panic("not implemented")

}

func (s *CSVSource) load() (*frame.DataFrame, error) {
	panic("not implemented")

}
