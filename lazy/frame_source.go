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
	panic("not implemented")

}

func (s *FrameSource) Schema() (*datatypes.Schema, error) {
	panic("not implemented")

}

func (s *FrameSource) DataFrame() (*frame.DataFrame, error) {
	panic("not implemented")

}
