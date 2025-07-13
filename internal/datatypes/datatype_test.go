package datatypes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDataTypeString(t *testing.T) {
	tests := []struct {
		dt       DataType
		expected string
	}{
		{Boolean{}, "bool"},
		{Int8{}, "i8"},
		{Int16{}, "i16"},
		{Int32{}, "i32"},
		{Int64{}, "i64"},
		{UInt8{}, "u8"},
		{UInt16{}, "u16"},
		{UInt32{}, "u32"},
		{UInt64{}, "u64"},
		{Float32{}, "f32"},
		{Float64{}, "f64"},
		{String{}, "str"},
		{Binary{}, "binary"},
		{Date{}, "date"},
		{Time{}, "time"},
		{Null{}, "null"},
		{Unknown{}, "unknown"},
		{Duration{Unit: Milliseconds}, "duration[ms]"},
		{List{Inner: Int32{}}, "list[i32]"},
		{Array{Inner: Float64{}, Width: 3}, "array[f64, 3]"},
		{Categorical{Ordered: false}, "cat"},
		{Decimal{Precision: 10, Scale: 2}, "decimal[10, 2]"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			assert.Equal(t, test.expected, test.dt.String())
		})
	}
}

func TestDataTypeEquals(t *testing.T) {
	tests := []struct {
		name     string
		dt1      DataType
		dt2      DataType
		expected bool
	}{
		{"same boolean", Boolean{}, Boolean{}, true},
		{"different types", Boolean{}, Int32{}, false},
		{"same int32", Int32{}, Int32{}, true},
		{"same list", List{Inner: Int32{}}, List{Inner: Int32{}}, true},
		{"different list inner", List{Inner: Int32{}}, List{Inner: Float64{}}, false},
		{"same array", Array{Inner: Int32{}, Width: 3}, Array{Inner: Int32{}, Width: 3}, true},
		{"different array width", Array{Inner: Int32{}, Width: 3}, Array{Inner: Int32{}, Width: 5}, false},
		{"same datetime no tz", Datetime{Unit: Milliseconds}, Datetime{Unit: Milliseconds}, true},
		{"different datetime unit", Datetime{Unit: Milliseconds}, Datetime{Unit: Microseconds}, false},
		{"same decimal", Decimal{Precision: 10, Scale: 2}, Decimal{Precision: 10, Scale: 2}, true},
		{"different decimal", Decimal{Precision: 10, Scale: 2}, Decimal{Precision: 18, Scale: 4}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.dt1.Equals(test.dt2))
		})
	}
}

func TestDataTypeProperties(t *testing.T) {
	tests := []struct {
		dt         DataType
		isNumeric  bool
		isNested   bool
		isTemporal bool
		isFloat    bool
		isInteger  bool
		isSigned   bool
	}{
		{Boolean{}, false, false, false, false, false, false},
		{Int32{}, true, false, false, false, true, true},
		{UInt32{}, true, false, false, false, true, false},
		{Float64{}, true, false, false, true, false, true},
		{String{}, false, false, false, false, false, false},
		{Date{}, false, false, true, false, false, false},
		{Datetime{Unit: Milliseconds}, false, false, true, false, false, false},
		{List{Inner: Int32{}}, false, true, false, false, false, false},
		{Array{Inner: Float64{}, Width: 3}, false, true, false, false, false, false},
		{Struct{Fields: []Field{}}, false, true, false, false, false, false},
		{Decimal{Precision: 10, Scale: 2}, true, false, false, false, false, true},
	}

	for _, test := range tests {
		t.Run(test.dt.String(), func(t *testing.T) {
			assert.Equal(t, test.isNumeric, test.dt.IsNumeric(), "IsNumeric")
			assert.Equal(t, test.isNested, test.dt.IsNested(), "IsNested")
			assert.Equal(t, test.isTemporal, test.dt.IsTemporal(), "IsTemporal")
			assert.Equal(t, test.isFloat, test.dt.IsFloat(), "IsFloat")
			assert.Equal(t, test.isInteger, test.dt.IsInteger(), "IsInteger")
			assert.Equal(t, test.isSigned, test.dt.IsSigned(), "IsSigned")
		})
	}
}

func TestSchema(t *testing.T) {
	schema := NewSchema(
		Field{Name: "id", DataType: Int64{}, Nullable: false},
		Field{Name: "name", DataType: String{}, Nullable: true},
		Field{Name: "age", DataType: Int32{}, Nullable: true},
		Field{Name: "balance", DataType: Float64{}, Nullable: false},
	)

	t.Run("FieldNames", func(t *testing.T) {
		names := schema.FieldNames()
		expected := []string{"id", "name", "age", "balance"}
		assert.Equal(t, expected, names)
	})

	t.Run("GetField", func(t *testing.T) {
		field, ok := schema.GetField("name")
		assert.True(t, ok)
		assert.Equal(t, "name", field.Name)
		assert.Equal(t, String{}, field.DataType)
		assert.True(t, field.Nullable)

		_, ok = schema.GetField("nonexistent")
		assert.False(t, ok)
	})
}

func TestTimeUnit(t *testing.T) {
	tests := []struct {
		unit     TimeUnit
		expected string
	}{
		{Nanoseconds, "ns"},
		{Microseconds, "us"},
		{Milliseconds, "ms"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			assert.Equal(t, test.expected, test.unit.String())
		})
	}
}

func TestDatetimeWithTimezone(t *testing.T) {
	nyLocation, err := time.LoadLocation("America/New_York")
	assert.NoError(t, err)

	dt1 := Datetime{Unit: Milliseconds, TimeZone: nyLocation}
	dt2 := Datetime{Unit: Milliseconds, TimeZone: nyLocation}
	dt3 := Datetime{Unit: Milliseconds, TimeZone: nil}

	assert.True(t, dt1.Equals(dt2))
	assert.False(t, dt1.Equals(dt3))
	assert.Contains(t, dt1.String(), "America/New_York")
}