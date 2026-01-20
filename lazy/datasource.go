package lazy

import (
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
)

// DataSource provides schema and identification for scans.
type DataSource interface {
	Name() string
	Schema() (*datatypes.Schema, error)
}

// ExecutableSource can materialize a DataFrame.
type ExecutableSource interface {
	DataSource
	DataFrame() (*frame.DataFrame, error)
}
