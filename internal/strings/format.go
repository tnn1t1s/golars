package strings

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
	"unicode/utf8"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Format applies printf-style formatting to strings
func (so *StringOps) Format(formatStr string, args ...series.Series) (series.Series, error) {
	values, validity := getStringValuesWithValidity(so.s)
	n := len(values)

	if len(args) == 0 {
		// No arguments, just return format string repeated
		result := make([]string, n)
		resultValidity := make([]bool, n)
		for i := 0; i < n; i++ {
			if validity[i] {
				result[i] = formatStr
				resultValidity[i] = true
			}
		}
		return series.NewSeriesWithValidity("format", result, resultValidity, datatypes.String{}), nil
	}

	// Ensure all series have the same length
	for _, arg := range args {
		if arg.Len() != n {
			return nil, fmt.Errorf("all series must have the same length")
		}
	}

	result := make([]string, n)
	resultValidity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !validity[i] {
			continue
		}

		// Check if any argument is null
		anyNull := false
		for _, arg := range args {
			if arg.IsNull(i) {
				anyNull = true
				break
			}
		}
		if anyNull {
			continue
		}

		// Collect arguments for this row
		fmtArgs := make([]interface{}, 0, 1+len(args))
		fmtArgs = append(fmtArgs, values[i])
		for _, arg := range args {
			fmtArgs = append(fmtArgs, arg.Get(i))
		}

		result[i] = fmt.Sprintf(formatStr, fmtArgs...)
		resultValidity[i] = true
	}

	return series.NewSeriesWithValidity("format", result, resultValidity, datatypes.String{}), nil
}

// FormatTemplate applies template-based formatting
func (so *StringOps) FormatTemplate(templateStr string, data map[string]series.Series) (series.Series, error) {
	tmpl, err := template.New("fmt").Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("template parse error: %w", err)
	}

	values, validity := getStringValuesWithValidity(so.s)
	n := len(values)

	// Ensure all series have the same length
	for _, s := range data {
		if s.Len() != n {
			return nil, fmt.Errorf("all series must have the same length")
		}
	}

	result := make([]string, n)
	resultValidity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !validity[i] {
			continue
		}

		// Check for nulls
		anyNull := false
		for _, s := range data {
			if s.IsNull(i) {
				anyNull = true
				break
			}
		}
		if anyNull {
			continue
		}

		// Build template data
		templateData := map[string]interface{}{
			"Value": values[i],
		}
		for key, s := range data {
			templateData[key] = s.Get(i)
		}

		var buf bytes.Buffer
		err := tmpl.Execute(&buf, templateData)
		if err != nil {
			continue
		}
		result[i] = buf.String()
		resultValidity[i] = true
	}

	return series.NewSeriesWithValidity("format_template", result, resultValidity, datatypes.String{}), nil
}

// Join concatenates multiple string series with a separator
func (so *StringOps) Join(separator string, others ...series.Series) (series.Series, error) {
	values, validity := getStringValuesWithValidity(so.s)
	n := len(values)

	// Ensure all series have the same length
	for _, s := range others {
		if s.Len() != n {
			return nil, fmt.Errorf("all series must have the same length")
		}
	}

	result := make([]string, n)
	resultValidity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !validity[i] {
			continue
		}

		anyNull := false
		for _, s := range others {
			if s.IsNull(i) {
				anyNull = true
				break
			}
		}
		if anyNull {
			continue
		}

		parts := make([]string, 0, 1+len(others))
		parts = append(parts, values[i])
		for _, s := range others {
			parts = append(parts, fmt.Sprintf("%v", s.Get(i)))
		}
		result[i] = strings.Join(parts, separator)
		resultValidity[i] = true
	}

	return series.NewSeriesWithValidity("join", result, resultValidity, datatypes.String{}), nil
}

// Center centers strings in a field of specified width
func (so *StringOps) Center(width int, fillChar ...string) series.Series {
	fc := " "
	if len(fillChar) > 0 && fillChar[0] != "" {
		fc = fillChar[0]
	}
	fcRune, _ := utf8.DecodeRuneInString(fc)

	return applyUnaryStringOp(so.s, func(s string) string {
		runeLen := utf8.RuneCountInString(s)
		if runeLen >= width {
			return s
		}
		pad := width - runeLen
		left := pad / 2
		right := pad - left
		return strings.Repeat(string(fcRune), left) + s + strings.Repeat(string(fcRune), right)
	}, "center")
}

// LJust left-justifies strings in a field of specified width
func (so *StringOps) LJust(width int, fillChar ...string) series.Series {
	fc := " "
	if len(fillChar) > 0 && fillChar[0] != "" {
		fc = fillChar[0]
	}
	fcRune, _ := utf8.DecodeRuneInString(fc)

	return applyUnaryStringOp(so.s, func(s string) string {
		runeLen := utf8.RuneCountInString(s)
		if runeLen >= width {
			return s
		}
		return s + strings.Repeat(string(fcRune), width-runeLen)
	}, "ljust")
}

// RJust right-justifies strings in a field of specified width
func (so *StringOps) RJust(width int, fillChar ...string) series.Series {
	fc := " "
	if len(fillChar) > 0 && fillChar[0] != "" {
		fc = fillChar[0]
	}
	fcRune, _ := utf8.DecodeRuneInString(fc)

	return applyUnaryStringOp(so.s, func(s string) string {
		runeLen := utf8.RuneCountInString(s)
		if runeLen >= width {
			return s
		}
		return strings.Repeat(string(fcRune), width-runeLen) + s
	}, "rjust")
}

// ExpandTabs expands tab characters to spaces
func (so *StringOps) ExpandTabs(tabsize ...int) series.Series {
	ts := 8
	if len(tabsize) > 0 {
		ts = tabsize[0]
	}

	return applyUnaryStringOp(so.s, func(s string) string {
		var buf strings.Builder
		col := 0
		for _, r := range s {
			if r == '\t' {
				spaces := ts - (col % ts)
				for j := 0; j < spaces; j++ {
					buf.WriteByte(' ')
				}
				col += spaces
			} else {
				buf.WriteRune(r)
				col++
				if r == '\n' {
					col = 0
				}
			}
		}
		return buf.String()
	}, "expand_tabs")
}

// Wrap wraps long strings at word boundaries
func (so *StringOps) Wrap(width int) series.Series {
	return applyUnaryStringOp(so.s, func(s string) string {
		words := strings.Fields(s)
		if len(words) == 0 {
			return s
		}

		var lines []string
		currentLine := words[0]

		for _, word := range words[1:] {
			if len(currentLine)+1+len(word) <= width {
				currentLine += " " + word
			} else {
				lines = append(lines, currentLine)
				currentLine = word
			}
		}
		lines = append(lines, currentLine)

		return strings.Join(lines, "\n")
	}, "wrap")
}
