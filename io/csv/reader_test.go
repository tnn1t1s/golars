package csv

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/stretchr/testify/assert"
)

func TestCSVReader(t *testing.T) {
	t.Run("BasicCSV", func(t *testing.T) {
		csvData := `name,age,score
Alice,25,95.5
Bob,30,87.0
Charlie,35,92.3`

		reader := NewReader(strings.NewReader(csvData), DefaultReadOptions())
		df, err := reader.Read()
		assert.NoError(t, err)
		assert.NotNil(t, df)

		// Check dimensions
		assert.Equal(t, 3, df.Height())
		assert.Equal(t, 3, df.Width())

		// Check column names
		assert.Equal(t, []string{"name", "age", "score"}, df.Columns())

		// Check data types
		nameCol, _ := df.Column("name")
		assert.Equal(t, datatypes.String{}, nameCol.DataType())

		ageCol, _ := df.Column("age")
		assert.Equal(t, datatypes.Int64{}, ageCol.DataType())

		scoreCol, _ := df.Column("score")
		assert.Equal(t, datatypes.Float64{}, scoreCol.DataType())

		// Check values
		assert.Equal(t, "Alice", nameCol.Get(0))
		assert.Equal(t, int64(25), ageCol.Get(0))
		assert.Equal(t, 95.5, scoreCol.Get(0))
	})

	t.Run("CSVWithNulls", func(t *testing.T) {
		csvData := `name,age,score
Alice,25,95.5
Bob,,87.0
Charlie,35,`

		reader := NewReader(strings.NewReader(csvData), DefaultReadOptions())
		df, err := reader.Read()
		assert.NoError(t, err)

		ageCol, _ := df.Column("age")
		assert.False(t, ageCol.IsNull(0))
		assert.True(t, ageCol.IsNull(1))
		assert.False(t, ageCol.IsNull(2))

		scoreCol, _ := df.Column("score")
		assert.False(t, scoreCol.IsNull(0))
		assert.False(t, scoreCol.IsNull(1))
		assert.True(t, scoreCol.IsNull(2))
	})

	t.Run("CustomDelimiter", func(t *testing.T) {
		csvData := `name;age;score
Alice;25;95.5
Bob;30;87.0`

		opts := DefaultReadOptions()
		opts.Delimiter = ';'
		reader := NewReader(strings.NewReader(csvData), opts)
		df, err := reader.Read()
		assert.NoError(t, err)
		assert.Equal(t, 2, df.Height())
		assert.Equal(t, 3, df.Width())
	})

	t.Run("NoHeader", func(t *testing.T) {
		csvData := `Alice,25,95.5
Bob,30,87.0`

		opts := DefaultReadOptions()
		opts.Header = false
		reader := NewReader(strings.NewReader(csvData), opts)
		df, err := reader.Read()
		assert.NoError(t, err)

		// Should generate column names
		assert.Equal(t, []string{"column_0", "column_1", "column_2"}, df.Columns())
		assert.Equal(t, 2, df.Height())
	})

	t.Run("SkipRows", func(t *testing.T) {
		csvData := `# Comment line
# Another comment
name,age,score
Alice,25,95.5
Bob,30,87.0`

		opts := DefaultReadOptions()
		opts.SkipRows = 2
		reader := NewReader(strings.NewReader(csvData), opts)
		df, err := reader.Read()
		assert.NoError(t, err)
		assert.Equal(t, 2, df.Height())
		assert.Equal(t, []string{"name", "age", "score"}, df.Columns())
	})

	t.Run("SelectColumns", func(t *testing.T) {
		csvData := `name,age,score,city
Alice,25,95.5,NYC
Bob,30,87.0,LA`

		opts := DefaultReadOptions()
		opts.Columns = []string{"name", "score"}
		reader := NewReader(strings.NewReader(csvData), opts)
		df, err := reader.Read()
		assert.NoError(t, err)
		assert.Equal(t, 2, df.Width())
		assert.Equal(t, []string{"name", "score"}, df.Columns())
	})

	t.Run("CustomNullValues", func(t *testing.T) {
		csvData := `name,age,score
Alice,25,95.5
Bob,NA,87.0
Charlie,35,N/A`

		opts := DefaultReadOptions()
		opts.NullValues = []string{"NA", "N/A"}
		reader := NewReader(strings.NewReader(csvData), opts)
		df, err := reader.Read()
		assert.NoError(t, err)

		ageCol, _ := df.Column("age")
		assert.True(t, ageCol.IsNull(1))

		scoreCol, _ := df.Column("score")
		assert.True(t, scoreCol.IsNull(2))
	})

	t.Run("BooleanInference", func(t *testing.T) {
		csvData := `active,verified
true,yes
false,no
1,0`

		reader := NewReader(strings.NewReader(csvData), DefaultReadOptions())
		df, err := reader.Read()
		assert.NoError(t, err)

		activeCol, _ := df.Column("active")
		assert.Equal(t, datatypes.Boolean{}, activeCol.DataType())
		assert.Equal(t, true, activeCol.Get(0))
		assert.Equal(t, false, activeCol.Get(1))
		assert.Equal(t, true, activeCol.Get(2))

		verifiedCol, _ := df.Column("verified")
		assert.Equal(t, datatypes.Boolean{}, verifiedCol.DataType())
		assert.Equal(t, true, verifiedCol.Get(0))
		assert.Equal(t, false, verifiedCol.Get(1))
		assert.Equal(t, false, verifiedCol.Get(2))
	})

	t.Run("EmptyCSV", func(t *testing.T) {
		csvData := ``
		reader := NewReader(strings.NewReader(csvData), DefaultReadOptions())
		df, err := reader.Read()
		assert.NoError(t, err)
		assert.NotNil(t, df)
		assert.Equal(t, 0, df.Height())
		assert.Equal(t, 0, df.Width())
	})

	t.Run("HeaderOnly", func(t *testing.T) {
		csvData := `name,age,score`
		reader := NewReader(strings.NewReader(csvData), DefaultReadOptions())
		df, err := reader.Read()
		assert.NoError(t, err)
		assert.NotNil(t, df)
		assert.Equal(t, 0, df.Height())
		assert.Equal(t, 3, df.Width())
		assert.Equal(t, []string{"name", "age", "score"}, df.Columns())
	})

	t.Run("RaggedCSV", func(t *testing.T) {
		csvData := `name,age,score
Alice,25,95.5
Bob,30
Charlie`

		reader := NewReader(strings.NewReader(csvData), DefaultReadOptions())
		df, err := reader.Read()
		assert.NoError(t, err)
		assert.Equal(t, 3, df.Height())

		scoreCol, _ := df.Column("score")
		assert.False(t, scoreCol.IsNull(0))
		assert.True(t, scoreCol.IsNull(1))
		assert.True(t, scoreCol.IsNull(2))

		ageCol, _ := df.Column("age")
		assert.False(t, ageCol.IsNull(0))
		assert.False(t, ageCol.IsNull(1))
		assert.True(t, ageCol.IsNull(2))
	})

	t.Run("Comments", func(t *testing.T) {
		csvData := `name,age,score
Alice,25,95.5
# This is a comment
Bob,30,87.0
# Another comment
Charlie,35,92.3`

		opts := DefaultReadOptions()
		opts.Comment = '#'
		reader := NewReader(strings.NewReader(csvData), opts)
		df, err := reader.Read()
		assert.NoError(t, err)
		assert.Equal(t, 3, df.Height()) // Comments should be skipped
	})
}

func TestTypeInference(t *testing.T) {
	t.Run("MixedTypes", func(t *testing.T) {
		csvData := `int_col,float_col,string_col,bool_col
123,45.6,hello,true
456,78.9,world,false
789,12.3,test,1`

		reader := NewReader(strings.NewReader(csvData), DefaultReadOptions())
		df, err := reader.Read()
		assert.NoError(t, err)

		intCol, _ := df.Column("int_col")
		assert.Equal(t, datatypes.Int64{}, intCol.DataType())

		floatCol, _ := df.Column("float_col")
		assert.Equal(t, datatypes.Float64{}, floatCol.DataType())

		stringCol, _ := df.Column("string_col")
		assert.Equal(t, datatypes.String{}, stringCol.DataType())

		boolCol, _ := df.Column("bool_col")
		assert.Equal(t, datatypes.Boolean{}, boolCol.DataType())
	})

	t.Run("IntToFloat", func(t *testing.T) {
		// When both int and float values exist, should infer float
		csvData := `mixed_col
123
45.6
789`

		reader := NewReader(strings.NewReader(csvData), DefaultReadOptions())
		df, err := reader.Read()
		assert.NoError(t, err)

		col, _ := df.Column("mixed_col")
		assert.Equal(t, datatypes.Float64{}, col.DataType())
		assert.Equal(t, 123.0, col.Get(0))
		assert.Equal(t, 45.6, col.Get(1))
		assert.Equal(t, 789.0, col.Get(2))
	})
}

func BenchmarkCSVReader(b *testing.B) {
	// Generate larger CSV data
	var sb strings.Builder
	sb.WriteString("id,name,value,active\n")
	for i := 0; i < 1000; i++ {
		sb.WriteString(fmt.Sprintf("%d,Name%d,%f,%t\n", i, i, float64(i)*1.5, i%2 == 0))
	}
	csvData := sb.String()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := NewReader(strings.NewReader(csvData), DefaultReadOptions())
		_, _ = reader.Read()
	}
}