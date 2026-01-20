package json

import (
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

// Helper function to get row count from DataFrame
func dfLen(df *frame.DataFrame) int {
	rows, _ := df.Shape()
	return rows
}

// Helper function to check if column exists
func hasColumn(df *frame.DataFrame, name string) bool {
	_, err := df.Column(name)
	return err == nil
}

// Helper function to get column, ignoring error
func getColumn(df *frame.DataFrame, name string) series.Series {
	col, _ := df.Column(name)
	return col
}
