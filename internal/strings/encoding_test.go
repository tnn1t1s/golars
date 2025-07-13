package strings

import (
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringEncoding(t *testing.T) {
	t.Run("Base64 encode/decode", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{"hello", "world", "test"})
		ops := NewStringOps(s)
		
		// Encode
		encoded, err := ops.Encode("base64")
		require.NoError(t, err)
		assert.Equal(t, 3, encoded.Len())
		assert.Equal(t, "aGVsbG8=", encoded.Get(0))
		assert.Equal(t, "d29ybGQ=", encoded.Get(1))
		assert.Equal(t, "dGVzdA==", encoded.Get(2))
		
		// Decode
		encodedOps := NewStringOps(encoded)
		decoded, err := encodedOps.Decode("base64")
		require.NoError(t, err)
		assert.Equal(t, 3, decoded.Len())
		assert.Equal(t, "hello", decoded.Get(0))
		assert.Equal(t, "world", decoded.Get(1))
		assert.Equal(t, "test", decoded.Get(2))
	})

	t.Run("Hex encode/decode", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{"abc", "123", "xyz"})
		ops := NewStringOps(s)
		
		// Encode
		encoded, err := ops.Encode("hex")
		require.NoError(t, err)
		assert.Equal(t, 3, encoded.Len())
		assert.Equal(t, "616263", encoded.Get(0))
		assert.Equal(t, "313233", encoded.Get(1))
		assert.Equal(t, "78797a", encoded.Get(2))
		
		// Decode
		encodedOps := NewStringOps(encoded)
		decoded, err := encodedOps.Decode("hex")
		require.NoError(t, err)
		assert.Equal(t, 3, decoded.Len())
		assert.Equal(t, "abc", decoded.Get(0))
		assert.Equal(t, "123", decoded.Get(1))
		assert.Equal(t, "xyz", decoded.Get(2))
	})

	t.Run("ASCII validation", func(t *testing.T) {
		values := []string{"hello", "world", "café", "test"}
		validity := []bool{true, true, true, true}
		s := series.NewSeriesWithValidity("text", values, validity, datatypes.String{})
		ops := NewStringOps(s)
		
		// Check ASCII
		isAscii := ops.IsASCII()
		assert.Equal(t, 4, isAscii.Len())
		assert.Equal(t, true, isAscii.Get(0))
		assert.Equal(t, true, isAscii.Get(1))
		assert.Equal(t, false, isAscii.Get(2)) // café has non-ASCII
		assert.Equal(t, true, isAscii.Get(3))
	})

	t.Run("UTF-8 validation", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{"hello", "世界", "café"})
		ops := NewStringOps(s)
		
		isUtf8 := ops.IsUTF8()
		assert.Equal(t, 3, isUtf8.Len())
		assert.Equal(t, true, isUtf8.Get(0))
		assert.Equal(t, true, isUtf8.Get(1))
		assert.Equal(t, true, isUtf8.Get(2))
	})

	t.Run("Byte length", func(t *testing.T) {
		s := series.NewStringSeries("text", []string{"hello", "世界", "café"})
		ops := NewStringOps(s)
		
		lengths := ops.ByteLength()
		assert.Equal(t, 3, lengths.Len())
		assert.Equal(t, int32(5), lengths.Get(0))  // "hello" = 5 bytes
		assert.Equal(t, int32(6), lengths.Get(1))  // "世界" = 6 bytes (3 bytes per character)
		assert.Equal(t, int32(5), lengths.Get(2))  // "café" = 5 bytes (é is 2 bytes)
	})

	t.Run("Encoding with nulls", func(t *testing.T) {
		values := []string{"hello", "", "world"}
		validity := []bool{true, false, true}
		s := series.NewSeriesWithValidity("text", values, validity, datatypes.String{})
		ops := NewStringOps(s)
		
		encoded, err := ops.Encode("base64")
		require.NoError(t, err)
		assert.Equal(t, 3, encoded.Len())
		assert.False(t, encoded.IsNull(0))
		assert.True(t, encoded.IsNull(1))
		assert.False(t, encoded.IsNull(2))
		assert.Equal(t, "aGVsbG8=", encoded.Get(0))
		assert.Equal(t, "d29ybGQ=", encoded.Get(2))
	})
}