package strings

import (
	_ "fmt"
	_ "github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	_ "strings"
	_ "text/template"
)

// Format applies printf-style formatting to strings
func (so *StringOps) Format(formatStr string, args ...series.Series) (series.Series, error) {
	panic("not implemented")

	// No arguments, just return format string repeated

	// Ensure all series have the same length

	// Check if any argument is null

	// Collect arguments for this row

	// Apply formatting

}

// FormatTemplate applies template-based formatting
func (so *StringOps) FormatTemplate(templateStr string, data map[string]series.Series) (series.Series, error) {
	panic(
		// Parse template
		"not implemented")

	// Ensure all series have the same length

	// Check for nulls

	// Execute template

}

// Join concatenates multiple string series with a separator
func (so *StringOps) Join(separator string, others ...series.Series) (series.Series, error) {
	panic(
		// Ensure all series have the same length
		"not implemented")

	// Check for nulls

}

// Center centers strings in a field of specified width
func (so *StringOps) Center(width int, fillChar ...string) series.Series {
	panic("not implemented")

}

// LJust left-justifies strings in a field of specified width
func (so *StringOps) LJust(width int, fillChar ...string) series.Series {
	panic("not implemented")

}

// RJust right-justifies strings in a field of specified width
func (so *StringOps) RJust(width int, fillChar ...string) series.Series {
	panic("not implemented")

}

// ExpandTabs expands tab characters to spaces
func (so *StringOps) ExpandTabs(tabsize ...int) series.Series {
	panic("not implemented")

}

// Wrap wraps long strings at word boundaries
func (so *StringOps) Wrap(width int) series.Series {
	panic("not implemented")

	// Start new line

	// Add to current line

}
