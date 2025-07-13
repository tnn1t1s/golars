// Package golars provides a high-performance DataFrame library for Go,
// inspired by Polars. It offers columnar data storage using Apache Arrow,
// lazy evaluation, and parallel execution.
package golars

import (
	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/io"
	"github.com/davidpalaitis/golars/io/parquet"
	"github.com/davidpalaitis/golars/lazy"
	"github.com/davidpalaitis/golars/series"
	"github.com/davidpalaitis/golars/window"
)

// Re-export main types for convenient access

// DataFrame is a table of data with named columns
type DataFrame = frame.DataFrame

// Series is a named column with a data type
type Series = series.Series

// DataType represents all possible data types
type DataType = datatypes.DataType

// Schema represents a collection of fields
type Schema = datatypes.Schema

// Field represents a field in a schema
type Field = datatypes.Field

// SortOptions contains options for sorting
type SortOptions = frame.SortOptions

// LazyFrame represents a lazy computation on a DataFrame
type LazyFrame = lazy.LazyFrame

// JoinType specifies the type of join operation
type JoinType = frame.JoinType

// JoinConfig contains configuration for join operations
type JoinConfig = frame.JoinConfig

// Re-export join types
const (
	InnerJoin = frame.InnerJoin
	LeftJoin  = frame.LeftJoin
	RightJoin = frame.RightJoin
	OuterJoin = frame.OuterJoin
	CrossJoin = frame.CrossJoin
	AntiJoin  = frame.AntiJoin
	SemiJoin  = frame.SemiJoin
)

// Re-export DataFrame constructors

// NewDataFrame creates a new DataFrame from a list of series
func NewDataFrame(columns ...Series) (*DataFrame, error) {
	return frame.NewDataFrame(columns...)
}

// NewDataFrameFromMap creates a DataFrame from a map of column names to slices
func NewDataFrameFromMap(data map[string]interface{}) (*DataFrame, error) {
	return frame.NewDataFrameFromMap(data)
}

// Re-export Series constructors

// NewSeries creates a new series from a slice of values
func NewSeries[T datatypes.ArrayValue](name string, values []T, dt DataType) Series {
	return series.NewSeries(name, values, dt)
}

// NewSeriesWithValidity creates a new series with explicit null values
func NewSeriesWithValidity[T datatypes.ArrayValue](name string, values []T, validity []bool, dt DataType) Series {
	return series.NewSeriesWithValidity(name, values, validity, dt)
}

// Type-specific series constructors

func NewBooleanSeries(name string, values []bool) Series {
	return series.NewBooleanSeries(name, values)
}

func NewInt8Series(name string, values []int8) Series {
	return series.NewInt8Series(name, values)
}

func NewInt16Series(name string, values []int16) Series {
	return series.NewInt16Series(name, values)
}

func NewInt32Series(name string, values []int32) Series {
	return series.NewInt32Series(name, values)
}

func NewInt64Series(name string, values []int64) Series {
	return series.NewInt64Series(name, values)
}

func NewUInt8Series(name string, values []uint8) Series {
	return series.NewUInt8Series(name, values)
}

func NewUInt16Series(name string, values []uint16) Series {
	return series.NewUInt16Series(name, values)
}

func NewUInt32Series(name string, values []uint32) Series {
	return series.NewUInt32Series(name, values)
}

func NewUInt64Series(name string, values []uint64) Series {
	return series.NewUInt64Series(name, values)
}

func NewFloat32Series(name string, values []float32) Series {
	return series.NewFloat32Series(name, values)
}

func NewFloat64Series(name string, values []float64) Series {
	return series.NewFloat64Series(name, values)
}

func NewStringSeries(name string, values []string) Series {
	return series.NewStringSeries(name, values)
}

func NewBinarySeries(name string, values [][]byte) Series {
	return series.NewBinarySeries(name, values)
}

// Re-export data types

var (
	Boolean = datatypes.Boolean{}
	Int8    = datatypes.Int8{}
	Int16   = datatypes.Int16{}
	Int32   = datatypes.Int32{}
	Int64   = datatypes.Int64{}
	UInt8   = datatypes.UInt8{}
	UInt16  = datatypes.UInt16{}
	UInt32  = datatypes.UInt32{}
	UInt64  = datatypes.UInt64{}
	Float32 = datatypes.Float32{}
	Float64 = datatypes.Float64{}
	String  = datatypes.String{}
	Binary  = datatypes.Binary{}
	Date    = datatypes.Date{}
	Time    = datatypes.Time{}
	Null    = datatypes.Null{}
)

// Re-export expression types and functions

// Expr represents an expression
type Expr = expr.Expr

// Col creates a column reference expression
func Col(name string) Expr {
	return expr.Col(name)
}

// Lit creates a literal expression
func Lit(value interface{}) Expr {
	return expr.Lit(value)
}

// ColBuilder creates a column expression builder for fluent API
func ColBuilder(name string) *expr.ExprBuilder {
	return expr.ColBuilder(name)
}

// When creates a conditional expression
func When(condition interface{}) *expr.WhenBuilder {
	return expr.When(condition)
}

// Re-export I/O functions

// ReadCSV reads a CSV file into a DataFrame
func ReadCSV(filename string, options ...io.CSVReadOption) (*DataFrame, error) {
	return io.ReadCSV(filename, options...)
}

// WriteCSV writes a DataFrame to a CSV file
func WriteCSV(df *DataFrame, filename string, options ...io.CSVWriteOption) error {
	return io.WriteCSV(df, filename, options...)
}

// ReadParquet reads a Parquet file into a DataFrame
func ReadParquet(filename string, options ...io.ParquetReadOption) (*DataFrame, error) {
	return io.ReadParquet(filename, options...)
}

// WriteParquet writes a DataFrame to a Parquet file
func WriteParquet(df *DataFrame, filename string, options ...io.ParquetWriteOption) error {
	return io.WriteParquet(df, filename, options...)
}

// CSV Read Options
type CSVReadOption = io.CSVReadOption

var (
	WithDelimiter       = io.WithDelimiter
	WithHeader          = io.WithHeader
	WithSkipRows        = io.WithSkipRows
	WithColumns         = io.WithColumns
	WithNullValues      = io.WithNullValues
	WithInferSchemaRows = io.WithInferSchemaRows
	WithComment         = io.WithComment
)

// CSV Write Options
type CSVWriteOption = io.CSVWriteOption

var (
	WithWriteDelimiter = io.WithWriteDelimiter
	WithWriteHeader    = io.WithWriteHeader
	WithNullValue      = io.WithNullValue
	WithFloatFormat    = io.WithFloatFormat
	WithQuote          = io.WithQuote
)

// Parquet Read Options
type ParquetReadOption = io.ParquetReadOption

var (
	WithParquetColumns = io.WithParquetColumns
	WithRowGroups      = io.WithRowGroups
	WithNumRows        = io.WithNumRows
)

// Parquet Write Options
type ParquetWriteOption = io.ParquetWriteOption
type CompressionType = parquet.CompressionType

// Re-export Parquet compression types
const (
	CompressionNone   = parquet.CompressionNone
	CompressionSnappy = parquet.CompressionSnappy
	CompressionGzip   = parquet.CompressionGzip
	CompressionZstd   = parquet.CompressionZstd
	CompressionLz4    = parquet.CompressionLz4
)

var (
	WithCompression      = io.WithCompression
	WithCompressionLevel = io.WithCompressionLevel
	WithRowGroupSize     = io.WithRowGroupSize
	WithPageSize         = io.WithPageSize
	WithDictionary       = io.WithDictionary
)

// LazyFromDataFrame creates a LazyFrame from an existing DataFrame
func LazyFromDataFrame(df *DataFrame) *LazyFrame {
	return lazy.NewLazyFrameFromDataFrame(df)
}

// ScanCSV creates a LazyFrame from a CSV file without reading it immediately
func ScanCSV(path string) *LazyFrame {
	return lazy.NewLazyFrame(lazy.NewScanNode(lazy.NewCSVSource(path)))
}

// ScanParquet creates a LazyFrame from a Parquet file without reading it immediately
func ScanParquet(path string) *LazyFrame {
	return lazy.NewLazyFrame(lazy.NewScanNode(lazy.NewParquetSource(path)))
}

// Window functions

// Window creates a new window specification
func Window() *window.Spec {
	return window.NewSpec()
}

// RowNumber creates a ROW_NUMBER() window function
func RowNumber() window.WindowFunc {
	return window.RowNumber()
}

// Rank creates a RANK() window function
func Rank() window.WindowFunc {
	return window.Rank()
}

// DenseRank creates a DENSE_RANK() window function
func DenseRank() window.WindowFunc {
	return window.DenseRank()
}

// PercentRank creates a PERCENT_RANK() window function
func PercentRank() window.WindowFunc {
	return window.PercentRank()
}

// NTile creates an NTILE() window function
func NTile(buckets int) window.WindowFunc {
	return window.NTile(buckets)
}

// Lag creates a LAG() window function
func Lag(column string, offset int, defaultValue ...interface{}) window.WindowFunc {
	return window.Lag(column, offset, defaultValue...)
}

// Lead creates a LEAD() window function
func Lead(column string, offset int, defaultValue ...interface{}) window.WindowFunc {
	return window.Lead(column, offset, defaultValue...)
}

// FirstValue creates a FIRST_VALUE() window function
func FirstValue(column string) window.WindowFunc {
	return window.FirstValue(column)
}

// LastValue creates a LAST_VALUE() window function
func LastValue(column string) window.WindowFunc {
	return window.LastValue(column)
}

// Sum creates a SUM() window function
func Sum(column string) window.WindowFunc {
	return window.Sum(column)
}

// Avg creates an AVG() window function
func Avg(column string) window.WindowFunc {
	return window.Avg(column)
}

// Min creates a MIN() window function
func Min(column string) window.WindowFunc {
	return window.Min(column)
}

// Max creates a MAX() window function
func Max(column string) window.WindowFunc {
	return window.Max(column)
}

// Count creates a COUNT() window function
func Count(column string) window.WindowFunc {
	return window.Count(column)
}