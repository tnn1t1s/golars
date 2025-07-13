package strings

import (
	"testing"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
)

func TestStringOps_DataFrameIntegration(t *testing.T) {
	// Create a DataFrame with string data
	df, err := frame.NewDataFrame(
		series.NewStringSeries("names", []string{"Alice", "Bob", "Charlie", "David"}),
		series.NewStringSeries("emails", []string{"alice@example.com", "bob@test.com", "charlie@demo.org", "david@test.com"}),
		series.NewStringSeries("messages", []string{"  Hello World  ", "TESTING 123", "example text", "  FINAL  "}),
	)
	assert.NoError(t, err)
	
	// Test 1: String length operation
	names, err := df.Column("names")
	assert.NoError(t, err)
	nameOps := NewStringOps(names)
	lengths := nameOps.Length()
	
	assert.Equal(t, "length", lengths.Name())
	assert.Equal(t, 4, lengths.Len())
	assert.Equal(t, int32(5), lengths.Get(0)) // "Alice"
	assert.Equal(t, int32(3), lengths.Get(1)) // "Bob"
	assert.Equal(t, int32(7), lengths.Get(2)) // "Charlie"
	assert.Equal(t, int32(5), lengths.Get(3)) // "David"
	
	// Test 2: Case transformations
	messages, err := df.Column("messages")
	assert.NoError(t, err)
	msgOps := NewStringOps(messages)
	
	upper := msgOps.ToUpper()
	assert.Equal(t, "  HELLO WORLD  ", upper.Get(0))
	assert.Equal(t, "TESTING 123", upper.Get(1))
	assert.Equal(t, "EXAMPLE TEXT", upper.Get(2))
	assert.Equal(t, "  FINAL  ", upper.Get(3))
	
	lower := msgOps.ToLower()
	assert.Equal(t, "  hello world  ", lower.Get(0))
	assert.Equal(t, "testing 123", lower.Get(1))
	assert.Equal(t, "example text", lower.Get(2))
	assert.Equal(t, "  final  ", lower.Get(3))
	
	// Test 3: String trimming
	stripped := msgOps.Strip()
	assert.Equal(t, "Hello World", stripped.Get(0))
	assert.Equal(t, "TESTING 123", stripped.Get(1))
	assert.Equal(t, "example text", stripped.Get(2))
	assert.Equal(t, "FINAL", stripped.Get(3))
	
	// Test 4: Pattern matching
	emails, err := df.Column("emails")
	assert.NoError(t, err)
	emailOps := NewStringOps(emails)
	
	containsTest := emailOps.Contains("test", true)
	assert.Equal(t, false, containsTest.Get(0).(bool)) // alice@example.com
	assert.Equal(t, true, containsTest.Get(1).(bool))  // bob@test.com
	assert.Equal(t, false, containsTest.Get(2).(bool)) // charlie@demo.org
	assert.Equal(t, true, containsTest.Get(3).(bool))  // david@test.com
	
	endsWith := emailOps.EndsWith(".com")
	assert.Equal(t, true, endsWith.Get(0).(bool))  // alice@example.com
	assert.Equal(t, true, endsWith.Get(1).(bool))  // bob@test.com
	assert.Equal(t, false, endsWith.Get(2).(bool)) // charlie@demo.org
	assert.Equal(t, true, endsWith.Get(3).(bool))  // david@test.com
	
	// Test 5: String replacement
	replaced := emailOps.Replace("test", "work", -1)
	assert.Equal(t, "alice@example.com", replaced.Get(0))
	assert.Equal(t, "bob@work.com", replaced.Get(1))
	assert.Equal(t, "charlie@demo.org", replaced.Get(2))
	assert.Equal(t, "david@work.com", replaced.Get(3))
	
	// Test 6: String slicing
	firstThree := nameOps.Left(3)
	assert.Equal(t, "Ali", firstThree.Get(0))
	assert.Equal(t, "Bob", firstThree.Get(1))
	assert.Equal(t, "Cha", firstThree.Get(2))
	assert.Equal(t, "Dav", firstThree.Get(3))
	
	// Test 7: String padding
	padded := nameOps.LPad(10, "*")
	assert.Equal(t, "*****Alice", padded.Get(0))
	assert.Equal(t, "*******Bob", padded.Get(1))
	assert.Equal(t, "***Charlie", padded.Get(2))
	assert.Equal(t, "*****David", padded.Get(3))
	
	// Test 8: Regular expressions
	emailDomains := emailOps.Extract(`@([^.]+)`, 1)
	assert.Equal(t, "example", emailDomains.Get(0))
	assert.Equal(t, "test", emailDomains.Get(1))
	assert.Equal(t, "demo", emailDomains.Get(2))
	assert.Equal(t, "test", emailDomains.Get(3))
	
	// Test 9: Multiple operations chained
	// Convert names to uppercase, then get first 3 chars
	upperNames := nameOps.ToUpper()
	upperOps := NewStringOps(upperNames)
	upperFirst3 := upperOps.Left(3)
	assert.Equal(t, "ALI", upperFirst3.Get(0))
	assert.Equal(t, "BOB", upperFirst3.Get(1))
	assert.Equal(t, "CHA", upperFirst3.Get(2))
	assert.Equal(t, "DAV", upperFirst3.Get(3))
}

func TestStringOps_EmptyStrings(t *testing.T) {
	// Create series with empty strings
	s := series.NewStringSeries("test", []string{"hello", "", "world", ""})
	
	ops := NewStringOps(s)
	
	// Test that empty strings are handled correctly
	lengths := ops.Length()
	assert.Equal(t, int32(5), lengths.Get(0))
	assert.Equal(t, int32(0), lengths.Get(1))
	assert.Equal(t, int32(5), lengths.Get(2))
	assert.Equal(t, int32(0), lengths.Get(3))
	
	upper := ops.ToUpper()
	assert.Equal(t, "HELLO", upper.Get(0))
	assert.Equal(t, "", upper.Get(1))
	assert.Equal(t, "WORLD", upper.Get(2))
	assert.Equal(t, "", upper.Get(3))
}

func TestStringOps_Unicode(t *testing.T) {
	// Test with Unicode strings
	s := series.NewStringSeries("unicode", []string{
		"Hello 世界",
		"🚀 Rocket",
		"Café ☕",
		"नमस्ते",
	})
	
	ops := NewStringOps(s)
	
	// Test rune length vs byte length
	byteLengths := ops.Length()
	runeLengths := ops.RuneLength()
	
	// "Hello 世界" - 12 bytes (世=3, 界=3), 8 runes
	assert.Equal(t, int32(12), byteLengths.Get(0))
	assert.Equal(t, int32(8), runeLengths.Get(0))
	
	// "🚀 Rocket" - 11 bytes (🚀=4), 8 runes
	assert.Equal(t, int32(11), byteLengths.Get(1))
	assert.Equal(t, int32(8), runeLengths.Get(1))
	
	// Test Unicode-aware operations
	reversed := ops.Reverse()
	assert.Equal(t, "界世 olleH", reversed.Get(0))
	assert.Equal(t, "tekcoR 🚀", reversed.Get(1))
	assert.Equal(t, "☕ éfaC", reversed.Get(2))
	assert.Equal(t, "ेत्समन", reversed.Get(3))
	
	// Test slicing with Unicode
	firstThree := ops.Slice(0, 3)
	assert.Equal(t, "Hel", firstThree.Get(0))
	assert.Equal(t, "🚀 R", firstThree.Get(1))
	assert.Equal(t, "Caf", firstThree.Get(2))
	assert.Equal(t, "नमस", firstThree.Get(3))
}