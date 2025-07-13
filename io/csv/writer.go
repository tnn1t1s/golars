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
		FloatFormat: "", // Use Go's default
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
	if df == nil {
		return fmt.Errorf("cannot write nil DataFrame")
	}

	// Handle empty DataFrame
	if df.Height() == 0 && df.Width() == 0 {
		return nil
	}

	// Write header if requested
	if w.options.Header && df.Width() > 0 {
		headers := df.Columns()
		if err := w.writer.Write(headers); err != nil {
			return fmt.Errorf("error writing header: %w", err)
		}
	}

	// Write data rows
	record := make([]string, df.Width())
	for i := 0; i < df.Height(); i++ {
		for j := 0; j < df.Width(); j++ {
			col, err := df.ColumnAt(j)
			if err != nil {
				return fmt.Errorf("error accessing column %d: %w", j, err)
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
func (w *Writer) formatValue(col interface{ Get(int) interface{}; IsNull(int) bool }, idx int) string {
	if col.IsNull(idx) {
		return w.options.NullValue
	}

	val := col.Get(idx)

	switch v := val.(type) {
	case float32:
		if w.options.FloatFormat != "" {
			return fmt.Sprintf(w.options.FloatFormat, v)
		}
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
		
	case float64:
		if w.options.FloatFormat != "" {
			return fmt.Sprintf(w.options.FloatFormat, v)
		}
		return strconv.FormatFloat(v, 'g', -1, 64)
		
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
		
	case bool:
		return strconv.FormatBool(v)
		
	case string:
		return v
		
	default:
		// Fallback to fmt.Sprint
		return fmt.Sprint(v)
	}
}