package strings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tnn1t1s/golars/series"
)

func TestStringOps_Length(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello", "world", "test", ""})
	ops := NewStringOps(s)

	result := ops.Length()
	assert.Equal(t, "length", result.Name())
	assert.Equal(t, 4, result.Len())

	expected := []int32{5, 5, 4, 0}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_RuneLength(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello", "世界", "test", "🚀"})
	ops := NewStringOps(s)

	result := ops.RuneLength()
	assert.Equal(t, "rune_length", result.Name())
	assert.Equal(t, 4, result.Len())

	expected := []int32{5, 2, 4, 1}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_Concat(t *testing.T) {
	s1 := series.NewStringSeries("s1", []string{"hello", "foo", "test"})
	s2 := series.NewStringSeries("s2", []string{" world", "bar", "ing"})
	ops := NewStringOps(s1)

	result := ops.Concat(s2)
	assert.Equal(t, "concat", result.Name())
	assert.Equal(t, 3, result.Len())

	expected := []string{"hello world", "foobar", "testing"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_Repeat(t *testing.T) {
	s := series.NewStringSeries("test", []string{"a", "bc", "xyz"})
	ops := NewStringOps(s)

	result := ops.Repeat(3)
	assert.Equal(t, "repeat", result.Name())
	assert.Equal(t, 3, result.Len())

	expected := []string{"aaa", "bcbcbc", "xyzxyzxyz"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_Slice(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello world", "testing", "example"})
	ops := NewStringOps(s)

	// Test positive start and length
	result := ops.Slice(0, 5)
	expected := []string{"hello", "testi", "examp"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}

	// Test negative start
	result = ops.Slice(-5, 3)
	expected = []string{"wor", "sti", "amp"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_Replace(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello world", "foo foo foo", "test"})
	ops := NewStringOps(s)

	// Replace all occurrences
	result := ops.Replace("o", "0", -1)
	expected := []string{"hell0 w0rld", "f00 f00 f00", "test"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}

	// Replace limited occurrences
	result = ops.Replace("o", "0", 2)
	expected = []string{"hell0 w0rld", "f00 foo foo", "test"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_Reverse(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello", "world", "12345", "🚀🌟"})
	ops := NewStringOps(s)

	result := ops.Reverse()
	expected := []string{"olleh", "dlrow", "54321", "🌟🚀"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_Contains(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello world", "testing", "example", "world"})
	ops := NewStringOps(s)

	result := ops.Contains("world", true)
	assert.Equal(t, "contains", result.Name())

	expected := []bool{true, false, false, true}
	for i := 0; i < result.Len(); i++ {
		val := result.Get(i)
		if val != nil {
			assert.Equal(t, expected[i], val.(bool))
		}
	}
}

func TestStringOps_StartsWith(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello", "world", "help", "test"})
	ops := NewStringOps(s)

	result := ops.StartsWith("hel")
	expected := []bool{true, false, true, false}
	for i := 0; i < result.Len(); i++ {
		val := result.Get(i)
		if val != nil {
			assert.Equal(t, expected[i], val.(bool))
		}
	}
}

func TestStringOps_EndsWith(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello", "world", "testing", "test"})
	ops := NewStringOps(s)

	result := ops.EndsWith("ing")
	expected := []bool{false, false, true, false}
	for i := 0; i < result.Len(); i++ {
		val := result.Get(i)
		if val != nil {
			assert.Equal(t, expected[i], val.(bool))
		}
	}
}

func TestStringOps_Find(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello world", "testing", "example", "not found"})
	ops := NewStringOps(s)

	result := ops.Find("or")
	expected := []int32{7, -1, -1, -1}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_ToUpper(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello", "World", "TEST", "123"})
	ops := NewStringOps(s)

	result := ops.ToUpper()
	expected := []string{"HELLO", "WORLD", "TEST", "123"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_ToLower(t *testing.T) {
	s := series.NewStringSeries("test", []string{"HELLO", "World", "TEST", "123"})
	ops := NewStringOps(s)

	result := ops.ToLower()
	expected := []string{"hello", "world", "test", "123"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_Capitalize(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello world", "TESTING", "example", "123abc"})
	ops := NewStringOps(s)

	result := ops.Capitalize()
	expected := []string{"Hello world", "Testing", "Example", "123abc"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_Trim(t *testing.T) {
	s := series.NewStringSeries("test", []string{"  hello  ", "\tworld\n", "test", "  "})
	ops := NewStringOps(s)

	result := ops.Trim()
	expected := []string{"hello", "world", "test", ""}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_Strip(t *testing.T) {
	s := series.NewStringSeries("test", []string{"  hello  ", "\tworld\n", "test", "  \t\n  "})
	ops := NewStringOps(s)

	result := ops.Strip()
	expected := []string{"hello", "world", "test", ""}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_Pad(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello", "hi", "test"})
	ops := NewStringOps(s)

	// Left padding
	result := ops.LPad(10, "*")
	expected := []string{"*****hello", "********hi", "******test"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}

	// Right padding
	result = ops.RPad(10, "-")
	expected = []string{"hello-----", "hi--------", "test------"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}

	// Center padding
	result = ops.Center(10, "=")
	expected = []string{"==hello===", "====hi====", "===test==="}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_ZFill(t *testing.T) {
	s := series.NewStringSeries("test", []string{"42", "-123", "+99", "test"})
	ops := NewStringOps(s)

	result := ops.ZFill(8)
	expected := []string{"00000042", "-0000123", "+0000099", "0000test"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expected[i], result.Get(i))
	}
}

func TestStringOps_IsAlpha(t *testing.T) {
	s := series.NewStringSeries("test", []string{"hello", "world123", "TEST", "123", ""})
	ops := NewStringOps(s)

	result := ops.IsAlpha()
	expected := []bool{true, false, true, false, false}
	for i := 0; i < result.Len(); i++ {
		val := result.Get(i)
		if val != nil {
			assert.Equal(t, expected[i], val.(bool))
		}
	}
}

func TestStringOps_IsNumeric(t *testing.T) {
	s := series.NewStringSeries("test", []string{"123", "456.78", "abc", "123abc", ""})
	ops := NewStringOps(s)

	result := ops.IsNumeric()
	expected := []bool{true, false, false, false, false}
	for i := 0; i < result.Len(); i++ {
		val := result.Get(i)
		if val != nil {
			assert.Equal(t, expected[i], val.(bool))
		}
	}
}
