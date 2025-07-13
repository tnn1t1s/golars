package golars

import (
	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/io/csv"
	"github.com/davidpalaitis/golars/io/json"
	"github.com/davidpalaitis/golars/io/parquet"
)

// CSV I/O

// ReadCSV reads a CSV file into a DataFrame
func ReadCSV(filename string, opts ...csv.Option) (*frame.DataFrame, error) {
	return csv.ReadCSV(filename, opts...)
}

// WriteCSV writes a DataFrame to a CSV file
func WriteCSV(df *frame.DataFrame, filename string, opts ...csv.Option) error {
	return csv.WriteCSV(df, filename, opts...)
}

// Parquet I/O

// ReadParquet reads a Parquet file into a DataFrame
func ReadParquet(filename string) (*frame.DataFrame, error) {
	return parquet.ReadParquet(filename)
}

// WriteParquet writes a DataFrame to a Parquet file
func WriteParquet(df *frame.DataFrame, filename string, opts ...parquet.WriteOption) error {
	return parquet.WriteParquet(df, filename, opts...)
}

// JSON I/O

// ReadJSON reads a JSON file into a DataFrame
func ReadJSON(filename string, opts ...func(*json.ReadOptions)) (*frame.DataFrame, error) {
	return json.ReadJSON(filename, opts...)
}

// ReadNDJSON reads an NDJSON file into a DataFrame
func ReadNDJSON(filename string, opts ...func(*json.ReadOptions)) (*frame.DataFrame, error) {
	return json.ReadNDJSON(filename, opts...)
}

// WriteJSON writes a DataFrame to a JSON file
func WriteJSON(df *frame.DataFrame, filename string, opts ...func(*json.WriteOptions)) error {
	return json.WriteJSON(df, filename, opts...)
}

// WriteNDJSON writes a DataFrame to an NDJSON file
func WriteNDJSON(df *frame.DataFrame, filename string, opts ...func(*json.WriteOptions)) error {
	return json.WriteNDJSON(df, filename, opts...)
}