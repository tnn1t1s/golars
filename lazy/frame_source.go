package lazy

import (
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/internal/datatypes"
)

// FrameSource is an in-memory data source for lazy execution.
type FrameSource struct {
	NameValue string
	Frame     *frame.DataFrame
}

func (s *FrameSource) Name() string {
	return s.NameValue
}

func (s *FrameSource) Schema() (*datatypes.Schema, error) {
	return s.Frame.Schema(), nil
}

func (s *FrameSource) DataFrame() (*frame.DataFrame, error) {
	return s.Frame, nil
}
