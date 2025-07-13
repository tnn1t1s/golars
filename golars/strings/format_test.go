package strings

import (
	"testing"

	"github.com/davidpalaitis/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringFormat(t *testing.T) {
	t.Run("Printf-style formatting", func(t *testing.T) {
		names := series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"})
		ages := series.NewInt32Series("age", []int32{25, 30, 35})
		ops := NewStringOps(names)
		
		formatted, err := ops.Format("%s is %d years old", ages)
		require.NoError(t, err)
		assert.Equal(t, 3, formatted.Len())
		assert.Equal(t, "Alice is 25 years old", formatted.Get(0))
		assert.Equal(t, "Bob is 30 years old", formatted.Get(1))
		assert.Equal(t, "Charlie is 35 years old", formatted.Get(2))
	})

	t.Run("Multiple arguments", func(t *testing.T) {
		items := series.NewStringSeries("item", []string{"apple", "banana", "orange"})
		quantities := series.NewInt32Series("qty", []int32{5, 3, 7})
		prices := series.NewFloat64Series("price", []float64{1.20, 0.50, 0.80})
		ops := NewStringOps(items)
		
		formatted, err := ops.Format("%s: %d @ $%.2f each", quantities, prices)
		require.NoError(t, err)
		assert.Equal(t, 3, formatted.Len())
		assert.Equal(t, "apple: 5 @ $1.20 each", formatted.Get(0))
		assert.Equal(t, "banana: 3 @ $0.50 each", formatted.Get(1))
		assert.Equal(t, "orange: 7 @ $0.80 each", formatted.Get(2))
	})

	t.Run("With nulls", func(t *testing.T) {
		names := []string{"Alice", "Bob", "Charlie"}
		namesValidity := []bool{true, false, true}
		nameSeries := series.NewSeriesWithValidity("name", names, namesValidity, nil)
		ages := series.NewInt32Series("age", []int32{25, 30, 35})
		ops := NewStringOps(nameSeries)
		
		formatted, err := ops.Format("%s is %d years old", ages)
		require.NoError(t, err)
		assert.Equal(t, 3, formatted.Len())
		assert.False(t, formatted.IsNull(0))
		assert.True(t, formatted.IsNull(1))  // Bob is null
		assert.False(t, formatted.IsNull(2))
		assert.Equal(t, "Alice is 25 years old", formatted.Get(0))
		assert.Equal(t, "Charlie is 35 years old", formatted.Get(2))
	})
}

func TestStringJoin(t *testing.T) {
	t.Run("Join multiple series", func(t *testing.T) {
		first := series.NewStringSeries("first", []string{"John", "Jane", "Bob"})
		last := series.NewStringSeries("last", []string{"Doe", "Smith", "Johnson"})
		age := series.NewInt32Series("age", []int32{30, 25, 35})
		
		ops := NewStringOps(first)
		joined, err := ops.Join(" ", last, age)
		require.NoError(t, err)
		assert.Equal(t, 3, joined.Len())
		assert.Equal(t, "John Doe 30", joined.Get(0))
		assert.Equal(t, "Jane Smith 25", joined.Get(1))
		assert.Equal(t, "Bob Johnson 35", joined.Get(2))
	})

	t.Run("Join with custom separator", func(t *testing.T) {
		parts := []series.Series{
			series.NewStringSeries("p1", []string{"A", "B", "C"}),
			series.NewStringSeries("p2", []string{"X", "Y", "Z"}),
			series.NewStringSeries("p3", []string{"1", "2", "3"}),
		}
		
		ops := NewStringOps(parts[0])
		joined, err := ops.Join("-", parts[1], parts[2])
		require.NoError(t, err)
		assert.Equal(t, 3, joined.Len())
		assert.Equal(t, "A-X-1", joined.Get(0))
		assert.Equal(t, "B-Y-2", joined.Get(1))
		assert.Equal(t, "C-Z-3", joined.Get(2))
	})
}

func TestStringJustify(t *testing.T) {
	t.Run("Center", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{"hello", "hi", "world"})
		ops := NewStringOps(s)
		
		centered := ops.Center(10)
		assert.Equal(t, 3, centered.Len())
		assert.Equal(t, "  hello   ", centered.Get(0))
		assert.Equal(t, "    hi    ", centered.Get(1))
		assert.Equal(t, "  world   ", centered.Get(2))
		
		// With custom fill char
		centered2 := ops.Center(10, "*")
		assert.Equal(t, "**hello***", centered2.Get(0))
		assert.Equal(t, "****hi****", centered2.Get(1))
		assert.Equal(t, "**world***", centered2.Get(2))
	})

	t.Run("Left justify", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{"hello", "hi", "world"})
		ops := NewStringOps(s)
		
		ljust := ops.LJust(8)
		assert.Equal(t, 3, ljust.Len())
		assert.Equal(t, "hello   ", ljust.Get(0))
		assert.Equal(t, "hi      ", ljust.Get(1))
		assert.Equal(t, "world   ", ljust.Get(2))
		
		// With custom fill char
		ljust2 := ops.LJust(8, ".")
		assert.Equal(t, "hello...", ljust2.Get(0))
		assert.Equal(t, "hi......", ljust2.Get(1))
		assert.Equal(t, "world...", ljust2.Get(2))
	})

	t.Run("Right justify", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{"hello", "hi", "world"})
		ops := NewStringOps(s)
		
		rjust := ops.RJust(8)
		assert.Equal(t, 3, rjust.Len())
		assert.Equal(t, "   hello", rjust.Get(0))
		assert.Equal(t, "      hi", rjust.Get(1))
		assert.Equal(t, "   world", rjust.Get(2))
		
		// With custom fill char
		rjust2 := ops.RJust(8, "0")
		assert.Equal(t, "000hello", rjust2.Get(0))
		assert.Equal(t, "000000hi", rjust2.Get(1))
		assert.Equal(t, "000world", rjust2.Get(2))
	})
}

func TestStringExpandTabs(t *testing.T) {
	t.Run("Default tab size", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{"hello\tworld", "a\tb\tc", "no\ttabs\there"})
		ops := NewStringOps(s)
		
		expanded := ops.ExpandTabs()
		assert.Equal(t, 3, expanded.Len())
		assert.Equal(t, "hello   world", expanded.Get(0))
		assert.Equal(t, "a       b       c", expanded.Get(1))
		assert.Equal(t, "no      tabs    here", expanded.Get(2))
	})

	t.Run("Custom tab size", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{"a\tb", "12\t34", "123\t456"})
		ops := NewStringOps(s)
		
		expanded := ops.ExpandTabs(4)
		assert.Equal(t, 3, expanded.Len())
		assert.Equal(t, "a   b", expanded.Get(0))
		assert.Equal(t, "12  34", expanded.Get(1))
		assert.Equal(t, "123 456", expanded.Get(2))
	})
}

func TestStringWrap(t *testing.T) {
	t.Run("Word wrap", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{
			"This is a long string that needs to be wrapped",
			"Short",
			"Another long string with many words that should be wrapped at word boundaries",
		})
		ops := NewStringOps(s)
		
		wrapped := ops.Wrap(20)
		assert.Equal(t, 3, wrapped.Len())
		
		// First string should be wrapped
		expected1 := "This is a long\nstring that needs to\nbe wrapped"
		assert.Equal(t, expected1, wrapped.Get(0))
		
		// Short string unchanged
		assert.Equal(t, "Short", wrapped.Get(1))
		
		// Third string wrapped
		assert.Contains(t, wrapped.Get(2).(string), "\n")
	})

	t.Run("No spaces", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{"verylongstringwithoutspaces"})
		ops := NewStringOps(s)
		
		wrapped := ops.Wrap(10)
		assert.Equal(t, 1, wrapped.Len())
		// Should return unchanged when no word boundaries
		assert.Equal(t, "verylongstringwithoutspaces", wrapped.Get(0))
	})
}

func TestStringTemplate(t *testing.T) {
	t.Run("Template formatting", func(t *testing.T) {
		names := series.NewStringSeries("name", []string{"Alice", "Bob", "Charlie"})
		ages := series.NewInt32Series("age", []int32{25, 30, 35})
		cities := series.NewStringSeries("city", []string{"NYC", "LA", "Chicago"})
		
		ops := NewStringOps(names)
		data := map[string]series.Series{
			"Age":  ages,
			"City": cities,
		}
		
		template := "{{.Value}} ({{.Age}}) lives in {{.City}}"
		formatted, err := ops.FormatTemplate(template, data)
		require.NoError(t, err)
		assert.Equal(t, 3, formatted.Len())
		assert.Equal(t, "Alice (25) lives in NYC", formatted.Get(0))
		assert.Equal(t, "Bob (30) lives in LA", formatted.Get(1))
		assert.Equal(t, "Charlie (35) lives in Chicago", formatted.Get(2))
	})
}