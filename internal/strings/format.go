package strings

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Format applies printf-style formatting to strings
func (so *StringOps) Format(formatStr string, args ...series.Series) (series.Series, error) {
	if len(args) == 0 {
		// No arguments, just return format string repeated
		return applyUnaryOp(so.s, func(str string) interface{} {
			return formatStr
		}, "format"), nil
	}

	// Ensure all series have the same length
	length := so.s.Len()
	for _, arg := range args {
		if arg.Len() != length {
			return nil, fmt.Errorf("all series must have the same length")
		}
	}

	values := make([]string, length)
	validity := make([]bool, length)

	for i := 0; i < length; i++ {
		// Check if any argument is null
		if so.s.IsNull(i) {
			validity[i] = false
			continue
		}

		anyNull := false
		for _, arg := range args {
			if arg.IsNull(i) {
				anyNull = true
				break
			}
		}

		if anyNull {
			validity[i] = false
			continue
		}

		// Collect arguments for this row
		formatArgs := make([]interface{}, len(args)+1)
		formatArgs[0] = so.s.Get(i)
		for j, arg := range args {
			formatArgs[j+1] = arg.Get(i)
		}

		// Apply formatting
		values[i] = fmt.Sprintf(formatStr, formatArgs...)
		validity[i] = true
	}

	return series.NewSeriesWithValidity(so.s.Name()+"_formatted", values, validity, datatypes.String{}), nil
}

// FormatTemplate applies template-based formatting
func (so *StringOps) FormatTemplate(templateStr string, data map[string]series.Series) (series.Series, error) {
	// Parse template
	tmpl, err := template.New("format").Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("invalid template: %w", err)
	}

	// Ensure all series have the same length
	length := so.s.Len()
	for _, s := range data {
		if s.Len() != length {
			return nil, fmt.Errorf("all series must have the same length")
		}
	}

	values := make([]string, length)
	validity := make([]bool, length)

	for i := 0; i < length; i++ {
		// Check for nulls
		if so.s.IsNull(i) {
			validity[i] = false
			continue
		}

		anyNull := false
		rowData := make(map[string]interface{})
		rowData["Value"] = so.s.Get(i)

		for name, s := range data {
			if s.IsNull(i) {
				anyNull = true
				break
			}
			rowData[name] = s.Get(i)
		}

		if anyNull {
			validity[i] = false
			continue
		}

		// Execute template
		var buf strings.Builder
		err := tmpl.Execute(&buf, rowData)
		if err != nil {
			validity[i] = false
			continue
		}

		values[i] = buf.String()
		validity[i] = true
	}

	return series.NewSeriesWithValidity(so.s.Name()+"_templated", values, validity, datatypes.String{}), nil
}

// Join concatenates multiple string series with a separator
func (so *StringOps) Join(separator string, others ...series.Series) (series.Series, error) {
	// Ensure all series have the same length
	length := so.s.Len()
	for _, other := range others {
		if other.Len() != length {
			return nil, fmt.Errorf("all series must have the same length")
		}
	}

	values := make([]string, length)
	validity := make([]bool, length)

	for i := 0; i < length; i++ {
		// Check for nulls
		if so.s.IsNull(i) {
			validity[i] = false
			continue
		}

		parts := make([]string, 0, len(others)+1)
		parts = append(parts, fmt.Sprint(so.s.Get(i)))

		anyNull := false
		for _, other := range others {
			if other.IsNull(i) {
				anyNull = true
				break
			}
			parts = append(parts, fmt.Sprint(other.Get(i)))
		}

		if anyNull {
			validity[i] = false
			continue
		}

		values[i] = strings.Join(parts, separator)
		validity[i] = true
	}

	return series.NewSeriesWithValidity(so.s.Name()+"_joined", values, validity, datatypes.String{}), nil
}

// Center centers strings in a field of specified width
func (so *StringOps) Center(width int, fillChar ...string) series.Series {
	fill := " "
	if len(fillChar) > 0 && len(fillChar[0]) > 0 {
		fill = string(fillChar[0][0])
	}

	return applyUnaryOp(so.s, func(str string) interface{} {
		if len(str) >= width {
			return str
		}

		totalPad := width - len(str)
		leftPad := totalPad / 2
		rightPad := totalPad - leftPad

		return strings.Repeat(fill, leftPad) + str + strings.Repeat(fill, rightPad)
	}, "center")
}

// LJust left-justifies strings in a field of specified width
func (so *StringOps) LJust(width int, fillChar ...string) series.Series {
	fill := " "
	if len(fillChar) > 0 && len(fillChar[0]) > 0 {
		fill = string(fillChar[0][0])
	}

	return applyUnaryOp(so.s, func(str string) interface{} {
		if len(str) >= width {
			return str
		}
		return str + strings.Repeat(fill, width-len(str))
	}, "ljust")
}

// RJust right-justifies strings in a field of specified width
func (so *StringOps) RJust(width int, fillChar ...string) series.Series {
	fill := " "
	if len(fillChar) > 0 && len(fillChar[0]) > 0 {
		fill = string(fillChar[0][0])
	}

	return applyUnaryOp(so.s, func(str string) interface{} {
		if len(str) >= width {
			return str
		}
		return strings.Repeat(fill, width-len(str)) + str
	}, "rjust")
}

// ExpandTabs expands tab characters to spaces
func (so *StringOps) ExpandTabs(tabsize ...int) series.Series {
	size := 8
	if len(tabsize) > 0 {
		size = tabsize[0]
	}

	return applyUnaryOp(so.s, func(str string) interface{} {
		if !strings.Contains(str, "\t") {
			return str
		}

		var result strings.Builder
		col := 0

		for _, ch := range str {
			if ch == '\t' {
				spaces := size - (col % size)
				result.WriteString(strings.Repeat(" ", spaces))
				col += spaces
			} else if ch == '\n' || ch == '\r' {
				result.WriteRune(ch)
				col = 0
			} else {
				result.WriteRune(ch)
				col++
			}
		}

		return result.String()
	}, "expand_tabs")
}

// Wrap wraps long strings at word boundaries
func (so *StringOps) Wrap(width int) series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		if len(str) <= width {
			return str
		}

		words := strings.Fields(str)
		if len(words) == 0 {
			return str
		}

		var lines []string
		var currentLine strings.Builder
		currentLen := 0

		for _, word := range words {
			wordLen := len(word)

			if currentLen > 0 && currentLen+1+wordLen > width {
				// Start new line
				lines = append(lines, currentLine.String())
				currentLine.Reset()
				currentLine.WriteString(word)
				currentLen = wordLen
			} else {
				// Add to current line
				if currentLen > 0 {
					currentLine.WriteString(" ")
					currentLen++
				}
				currentLine.WriteString(word)
				currentLen += wordLen
			}
		}

		if currentLen > 0 {
			lines = append(lines, currentLine.String())
		}

		return strings.Join(lines, "\n")
	}, "wrap")
}
