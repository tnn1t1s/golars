// Package io provides input/output functionality for DataFrames
package io

import (
	"os"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/io/csv"
	"github.com/tnn1t1s/golars/io/parquet"
)

// CSV Read Options

// CSVReadOption is a function that modifies CSV read options
type CSVReadOption func(*csv.ReadOptions)

// WithDelimiter sets the field delimiter
func WithDelimiter(d rune) CSVReadOption {
	return func(o *csv.ReadOptions) {
		o.Delimiter = d
	}
}

// WithHeader specifies whether the first row contains headers
func WithHeader(h bool) CSVReadOption {
	return func(o *csv.ReadOptions) {
		o.Header = h
	}
}

// WithSkipRows sets the number of rows to skip at the start
func WithSkipRows(n int) CSVReadOption {
	return func(o *csv.ReadOptions) {
		o.SkipRows = n
	}
}

// WithColumns specifies which columns to read
func WithColumns(cols []string) CSVReadOption {
	return func(o *csv.ReadOptions) {
		o.Columns = cols
	}
}

// WithNullValues sets strings that should be treated as null
func WithNullValues(values []string) CSVReadOption {
	return func(o *csv.ReadOptions) {
		o.NullValues = values
	}
}

// WithInferSchemaRows sets the number of rows to use for type inference
func WithInferSchemaRows(n int) CSVReadOption {
	return func(o *csv.ReadOptions) {
		o.InferSchemaRows = n
	}
}

// WithComment sets the comment character
func WithComment(c rune) CSVReadOption {
	return func(o *csv.ReadOptions) {
		o.Comment = c
	}
}

// CSV Write Options

// CSVWriteOption is a function that modifies CSV write options
type CSVWriteOption func(*csv.WriteOptions)

// WithWriteDelimiter sets the field delimiter for writing
func WithWriteDelimiter(d rune) CSVWriteOption {
	return func(o *csv.WriteOptions) {
		o.Delimiter = d
	}
}

// WithWriteHeader specifies whether to write column headers
func WithWriteHeader(h bool) CSVWriteOption {
	return func(o *csv.WriteOptions) {
		o.Header = h
	}
}

// WithNullValue sets the string to use for null values
func WithNullValue(s string) CSVWriteOption {
	return func(o *csv.WriteOptions) {
		o.NullValue = s
	}
}

// WithFloatFormat sets the format string for floating point numbers
func WithFloatFormat(f string) CSVWriteOption {
	return func(o *csv.WriteOptions) {
		o.FloatFormat = f
	}
}

// WithQuote specifies whether to quote all fields
func WithQuote(q bool) CSVWriteOption {
	return func(o *csv.WriteOptions) {
		o.Quote = q
	}
}

// ReadCSV reads a CSV file into a DataFrame
func ReadCSV(filename string, options ...CSVReadOption) (*frame.DataFrame, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	opts := csv.DefaultReadOptions()
	for _, opt := range options {
		opt(&opts)
	}

	reader := csv.NewReader(file, opts)
	return reader.Read()
}

// WriteCSV writes a DataFrame to a CSV file
func WriteCSV(df *frame.DataFrame, filename string, options ...CSVWriteOption) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	opts := csv.DefaultWriteOptions()
	for _, opt := range options {
		opt(&opts)
	}

	writer := csv.NewWriter(file, opts)
	return writer.Write(df)
}

// Parquet Read Options

// ParquetReadOption is a function that modifies Parquet read options
type ParquetReadOption func(*parquet.ReaderOptions)

// WithParquetColumns specifies which columns to read
func WithParquetColumns(cols []string) ParquetReadOption {
	return func(o *parquet.ReaderOptions) {
		o.Columns = cols
	}
}

// WithRowGroups specifies which row groups to read
func WithRowGroups(groups []int) ParquetReadOption {
	return func(o *parquet.ReaderOptions) {
		o.RowGroups = groups
	}
}

// WithNumRows limits the number of rows to read
func WithNumRows(n int64) ParquetReadOption {
	return func(o *parquet.ReaderOptions) {
		o.NumRows = n
	}
}

// ReadParquet reads a Parquet file into a DataFrame
func ReadParquet(filename string, options ...ParquetReadOption) (*frame.DataFrame, error) {
	opts := parquet.DefaultReaderOptions()
	for _, opt := range options {
		opt(&opts)
	}
	
	reader := parquet.NewReader(opts)
	return reader.ReadFile(filename)
}

// Parquet Write Options

// ParquetWriteOption is a function that modifies Parquet write options
type ParquetWriteOption func(*parquet.WriterOptions)

// WithCompression sets the compression type
func WithCompression(compression parquet.CompressionType) ParquetWriteOption {
	return func(o *parquet.WriterOptions) {
		o.Compression = compression
	}
}

// WithCompressionLevel sets the compression level (for gzip and zstd)
func WithCompressionLevel(level int) ParquetWriteOption {
	return func(o *parquet.WriterOptions) {
		o.CompressionLevel = level
	}
}

// WithRowGroupSize sets the row group size in bytes
func WithRowGroupSize(size int64) ParquetWriteOption {
	return func(o *parquet.WriterOptions) {
		o.RowGroupSize = size
	}
}

// WithPageSize sets the page size in bytes
func WithPageSize(size int64) ParquetWriteOption {
	return func(o *parquet.WriterOptions) {
		o.PageSize = size
	}
}

// WithDictionary enables or disables dictionary encoding
func WithDictionary(enabled bool) ParquetWriteOption {
	return func(o *parquet.WriterOptions) {
		o.UseDictionary = enabled
	}
}

// WriteParquet writes a DataFrame to a Parquet file
func WriteParquet(df *frame.DataFrame, filename string, options ...ParquetWriteOption) error {
	opts := parquet.DefaultWriterOptions()
	for _, opt := range options {
		opt(&opts)
	}
	
	writer := parquet.NewWriter(opts)
	return writer.WriteFile(df, filename)
}