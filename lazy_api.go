package golars

import (
	"github.com/tnn1t1s/golars/io"
	"github.com/tnn1t1s/golars/lazy"
)

// LazyFrame is a lazy query builder.
type LazyFrame = lazy.LazyFrame

// ScanCSV creates a lazy CSV scan that reads on Collect.
func ScanCSV(filename string, options ...io.CSVReadOption) *lazy.LazyFrame {
	return lazy.NewLazyFrame(lazy.NewCSVSource(filename, options...))
}
