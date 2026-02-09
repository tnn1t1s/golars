package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/tnn1t1s/golars/frame"
)

// Writer writes DataFrame data to CSV format
type Writer struct {
	writer  *csv.Writer
	options WriteOptions
}

// WriteOptions configures CSV writing behavior
type WriteOptions struct {
	Delimiter   rune   // Field delimiter (default: ',')
	Header      bool   // Write column names as first row
	NullValue   string // String to use for null values
	FloatFormat string // Format string for floats (e.g., "%.2f")
	Quote       bool   // Quote all fields
}

// DefaultWriteOptions returns default CSV write options
func DefaultWriteOptions() WriteOptions {
	return WriteOptions{
		Delimiter:   ',',
		Header:      true,
		NullValue:   "",
		FloatFormat: "",
		Quote:       false,
	}
}

// NewWriter creates a new CSV writer
func NewWriter(w io.Writer, options WriteOptions) *Writer {
	csvWriter := csv.NewWriter(w)
	csvWriter.Comma = options.Delimiter
	return &Writer{
		writer:  csvWriter,
		options: options,
	}
}

// Write writes a DataFrame to CSV format
func (w *Writer) Write(df *frame.DataFrame) error {
	// Handle empty DataFrame
	if df.Width() == 0 {
		return nil
	}

	// Write header if requested
	if w.options.Header {
		if err := w.writer.Write(df.Columns()); err != nil {
			return fmt.Errorf("error writing header: %w", err)
		}
	}

	// Write data rows
	numRows := df.Height()
	numCols := df.Width()
	for i := 0; i < numRows; i++ {
		record := make([]string, numCols)
		for j := 0; j < numCols; j++ {
			col, err := df.ColumnAt(j)
			if err != nil {
				return fmt.Errorf("error getting column %d: %w", j, err)
			}
			record[j] = w.formatValue(col, i)
		}
		if err := w.writer.Write(record); err != nil {
			return fmt.Errorf("error writing row %d: %w", i, err)
		}
	}

	// Flush buffer
	w.writer.Flush()
	return w.writer.Error()
}

// formatValue formats a single value for CSV output
func (w *Writer) formatValue(col interface {
	Get(int) interface{}
	IsNull(int) bool
}, idx int) string {
	if col.IsNull(idx) {
		return w.options.NullValue
	}

	val := col.Get(idx)
	if val == nil {
		return w.options.NullValue
	}

	// Handle float formatting
	if w.options.FloatFormat != "" {
		switch v := val.(type) {
		case float64:
			return fmt.Sprintf(w.options.FloatFormat, v)
		case float32:
			return fmt.Sprintf(w.options.FloatFormat, v)
		}
	}

	// Default formatting
	switch v := val.(type) {
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return v
	default:
		// Fallback to fmt.Sprint
		return fmt.Sprint(v)
	}
}
