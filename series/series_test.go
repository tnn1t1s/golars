package series

import (
	"fmt"
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/stretchr/testify/assert"
)

func TestNewSeries(t *testing.T) {
	s := NewInt32Series("numbers", []int32{1, 2, 3, 4, 5})
	
	assert.Equal(t, "numbers", s.Name())
	assert.Equal(t, datatypes.Int32{}, s.DataType())
	assert.Equal(t, 5, s.Len())
	assert.Equal(t, 0, s.NullCount())
}

func TestNewSeriesWithValidity(t *testing.T) {
	values := []string{"a", "b", "c", "d"}
	validity := []bool{true, false, true, false}
	
	s := NewSeriesWithValidity("strings", values, validity, datatypes.String{})
	
	assert.Equal(t, 4, s.Len())
	assert.Equal(t, 2, s.NullCount())
	assert.True(t, s.IsValid(0))
	assert.False(t, s.IsValid(1))
	assert.True(t, s.IsValid(2))
	assert.False(t, s.IsValid(3))
}

func TestSeriesGet(t *testing.T) {
	s := NewFloat64Series("floats", []float64{1.1, 2.2, 3.3})
	
	assert.Equal(t, 1.1, s.Get(0))
	assert.Equal(t, 2.2, s.Get(1))
	assert.Equal(t, 3.3, s.Get(2))
	assert.Nil(t, s.Get(3)) // out of bounds
}

func TestSeriesGetAsString(t *testing.T) {
	values := []int64{100, 200, 300}
	validity := []bool{true, false, true}
	s := NewSeriesWithValidity("ints", values, validity, datatypes.Int64{})
	
	assert.Equal(t, "100", s.GetAsString(0))
	assert.Equal(t, "null", s.GetAsString(1))
	assert.Equal(t, "300", s.GetAsString(2))
}

func TestSeriesSlice(t *testing.T) {
	s := NewStringSeries("words", []string{"hello", "world", "foo", "bar", "baz"})
	
	sliced, err := s.Slice(1, 4)
	assert.NoError(t, err)
	assert.Equal(t, 3, sliced.Len())
	assert.Equal(t, "world", sliced.Get(0))
	assert.Equal(t, "foo", sliced.Get(1))
	assert.Equal(t, "bar", sliced.Get(2))
	
	// Test invalid slice
	_, err = s.Slice(-1, 3)
	assert.Error(t, err)
	
	_, err = s.Slice(2, 10)
	assert.Error(t, err)
}

func TestSeriesHeadTail(t *testing.T) {
	s := NewInt8Series("bytes", []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	
	head := s.Head(3)
	assert.Equal(t, 3, head.Len())
	assert.Equal(t, int8(1), head.Get(0))
	assert.Equal(t, int8(3), head.Get(2))
	
	tail := s.Tail(3)
	assert.Equal(t, 3, tail.Len())
	assert.Equal(t, int8(8), tail.Get(0))
	assert.Equal(t, int8(10), tail.Get(2))
	
	// Test edge cases
	assert.Equal(t, 10, s.Head(20).Len())
	assert.Equal(t, 10, s.Tail(20).Len())
	assert.Equal(t, 0, s.Head(0).Len())
	assert.Equal(t, 0, s.Tail(0).Len())
}

func TestSeriesRename(t *testing.T) {
	s := NewBooleanSeries("flags", []bool{true, false, true})
	renamed := s.Rename("new_flags")
	
	assert.Equal(t, "flags", s.Name())
	assert.Equal(t, "new_flags", renamed.Name())
	assert.Equal(t, s.Len(), renamed.Len())
	
	// Verify data is copied
	for i := 0; i < s.Len(); i++ {
		assert.Equal(t, s.Get(i), renamed.Get(i))
	}
}

func TestSeriesEquals(t *testing.T) {
	s1 := NewUInt32Series("a", []uint32{1, 2, 3})
	s2 := NewUInt32Series("b", []uint32{1, 2, 3})
	s3 := NewUInt32Series("c", []uint32{1, 2, 4})
	s4 := NewUInt32Series("d", []uint32{1, 2})
	
	assert.True(t, s1.Equals(s2))  // same values
	assert.False(t, s1.Equals(s3)) // different values
	assert.False(t, s1.Equals(s4)) // different length
	assert.False(t, s1.Equals(nil)) // nil comparison
	
	// Test with nulls
	values := []float32{1.0, 2.0, 3.0}
	validity1 := []bool{true, false, true}
	validity2 := []bool{true, false, true}
	validity3 := []bool{true, true, true}
	
	s5 := NewSeriesWithValidity("e", values, validity1, datatypes.Float32{})
	s6 := NewSeriesWithValidity("f", values, validity2, datatypes.Float32{})
	s7 := NewSeriesWithValidity("g", values, validity3, datatypes.Float32{})
	
	assert.True(t, s5.Equals(s6))  // same nulls
	assert.False(t, s5.Equals(s7)) // different nulls
}

func TestSeriesClone(t *testing.T) {
	s := NewInt16Series("original", []int16{10, 20, 30})
	cloned := s.Clone()
	
	assert.Equal(t, s.Name(), cloned.Name())
	assert.Equal(t, s.Len(), cloned.Len())
	assert.True(t, s.Equals(cloned))
}

func TestSeriesToSlice(t *testing.T) {
	values := []uint64{100, 200, 300, 400, 500}
	s := NewUInt64Series("uints", values)
	
	slice := s.ToSlice()
	assert.Equal(t, values, slice)
}

func TestSeriesString(t *testing.T) {
	s := NewStringSeries("test", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"})
	str := s.String()
	
	assert.Contains(t, str, "Series: test [str]")
	assert.Contains(t, str, "a")
	assert.Contains(t, str, "j")
	assert.Contains(t, str, "2 more values")
}

func TestDifferentSeriesTypes(t *testing.T) {
	t.Run("Boolean", func(t *testing.T) {
		s := NewBooleanSeries("bool", []bool{true, false, true})
		assert.Equal(t, datatypes.Boolean{}, s.DataType())
		assert.Equal(t, 3, s.Len())
	})
	
	t.Run("UInt8", func(t *testing.T) {
		s := NewUInt8Series("u8", []uint8{0, 255, 128})
		assert.Equal(t, datatypes.UInt8{}, s.DataType())
		assert.Equal(t, uint8(255), s.Get(1))
	})
	
	t.Run("Binary", func(t *testing.T) {
		s := NewBinarySeries("bin", [][]byte{[]byte("hello"), []byte("world")})
		assert.Equal(t, datatypes.Binary{}, s.DataType())
		assert.Equal(t, []byte("hello"), s.Get(0))
	})
}

func TestEmptySeries(t *testing.T) {
	s := NewInt64Series("empty", []int64{})
	
	assert.Equal(t, 0, s.Len())
	assert.Equal(t, 0, s.NullCount())
	assert.Nil(t, s.Get(0))
	assert.Equal(t, "null", s.GetAsString(0))
	
	head := s.Head(5)
	assert.Equal(t, 0, head.Len())
	
	tail := s.Tail(5)
	assert.Equal(t, 0, tail.Len())
}

func BenchmarkSeriesCreation(b *testing.B) {
	values := make([]int64, 10000)
	for i := range values {
		values[i] = int64(i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := NewInt64Series("bench", values)
		_ = s.Len()
	}
}

func BenchmarkSeriesGet(b *testing.B) {
	values := make([]float64, 10000)
	for i := range values {
		values[i] = float64(i) * 1.5
	}
	s := NewFloat64Series("bench", values)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Get(i % 10000)
	}
}

func BenchmarkSeriesEquals(b *testing.B) {
	values := make([]string, 1000)
	for i := range values {
		values[i] = fmt.Sprintf("value_%d", i)
	}
	s1 := NewStringSeries("s1", values)
	s2 := NewStringSeries("s2", values)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s1.Equals(s2)
	}
}