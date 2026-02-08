package json

import (
	"github.com/tnn1t1s/golars/frame"
)

// ReadJSON reads a JSON file into a DataFrame
func ReadJSON(filename string, opts ...func(*ReadOptions)) (*frame.DataFrame, error) {
	panic("not implemented")

}

// ReadNDJSON reads an NDJSON file into a DataFrame
func ReadNDJSON(filename string, opts ...func(*ReadOptions)) (*frame.DataFrame, error) {
	panic("not implemented")

}

// WriteJSON writes a DataFrame to a JSON file
func WriteJSON(df *frame.DataFrame, filename string, opts ...func(*WriteOptions)) error {
	panic("not implemented")

}

// WriteNDJSON writes a DataFrame to an NDJSON file
func WriteNDJSON(df *frame.DataFrame, filename string, opts ...func(*WriteOptions)) error {
	panic("not implemented")

}
