package lazy

import (
	"path/filepath"
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
	return &CSVSource{
		Path:    path,
		Options: options,
	}
}

func (s *CSVSource) Name() string {
	return filepath.Base(s.Path)
}

func (s *CSVSource) Schema() (*datatypes.Schema, error) {
	df, err := s.load()
	if err != nil {
		return nil, err
	}
	return df.Schema(), nil
}

func (s *CSVSource) DataFrame() (*frame.DataFrame, error) {
	return s.load()
}

func (s *CSVSource) load() (*frame.DataFrame, error) {
	s.loadOnce.Do(func() {
		s.frame, s.loadErr = gio.ReadCSV(s.Path, s.Options...)
	})
	return s.frame, s.loadErr
}
