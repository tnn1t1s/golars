package json

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"strings"
	"testing"

	"github.com/davidpalaitis/golars/internal/datatypes"
	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/series"
)

func createBenchmarkDataFrame(rows int) *frame.DataFrame {
	ids := make([]int64, rows)
	names := make([]string, rows)
	scores := make([]float64, rows)
	active := make([]bool, rows)
	
	for i := 0; i < rows; i++ {
		ids[i] = int64(i)
		names[i] = "User" + string(rune(i%26+'A'))
		scores[i] = float64(i%100) + 0.5
		active[i] = i%2 == 0
	}

	df, _ := frame.NewDataFrame(
		series.NewInt64Series("id", ids),
		series.NewStringSeries("name", names),
		series.NewFloat64Series("score", scores),
		series.NewBooleanSeries("active", active),
	)
	return df
}

func BenchmarkJSONReader(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		// Create JSON data
		df := createBenchmarkDataFrame(size)
		writer := NewWriter()
		var buf bytes.Buffer
		writer.Write(df, &buf)
		jsonData := buf.String()

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(jsonData)))
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				reader := NewReader()
				_, err := reader.Read(strings.NewReader(jsonData))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkJSONReaderWithInference(b *testing.B) {
	// Create JSON with mixed types that need inference
	jsonData := `[
		{"id": 1, "value": 10, "flag": true, "name": "Alice"},
		{"id": 2, "value": 10.5, "flag": false, "name": "Bob"},
		{"id": 3, "value": 20, "flag": true, "name": "Charlie"}
	]`
	
	b.Run("with_inference", func(b *testing.B) {
		b.SetBytes(int64(len(jsonData)))
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			reader := NewReader(WithInferSchema(true))
			_, err := reader.Read(strings.NewReader(jsonData))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("without_inference", func(b *testing.B) {
		b.SetBytes(int64(len(jsonData)))
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			reader := NewReader(WithInferSchema(false))
			_, err := reader.Read(strings.NewReader(jsonData))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkJSONWriter(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		df := createBenchmarkDataFrame(size)
		
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				writer := NewWriter()
				var buf bytes.Buffer
				err := writer.Write(df, &buf)
				if err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(buf.Len()))
			}
		})
	}
}

func BenchmarkJSONWriterOrientations(b *testing.B) {
	df := createBenchmarkDataFrame(1000)
	orientations := []string{"records", "columns", "values"}
	
	for _, orient := range orientations {
		b.Run(orient, func(b *testing.B) {
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				writer := NewWriter(WithOrient(orient))
				var buf bytes.Buffer
				err := writer.Write(df, &buf)
				if err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(buf.Len()))
			}
		})
	}
}

func BenchmarkNDJSONReader(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		// Create NDJSON data
		var buf bytes.Buffer
		for i := 0; i < size; i++ {
			buf.WriteString(`{"id":`)
			buf.WriteString(string(rune(i)))
			buf.WriteString(`,"name":"User`)
			buf.WriteByte(byte(i%26 + 'A'))
			buf.WriteString(`","score":`)
			buf.WriteString(string(rune(i%100)))
			buf.WriteString(`.5,"active":`)
			if i%2 == 0 {
				buf.WriteString("true")
			} else {
				buf.WriteString("false")
			}
			buf.WriteString("}\n")
		}
		ndjsonData := buf.String()

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(ndjsonData)))
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				reader := NewNDJSONReader()
				_, err := reader.Read(strings.NewReader(ndjsonData))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkNDJSONReaderChunked(b *testing.B) {
	// Create large NDJSON data
	size := 10000
	var buf bytes.Buffer
	for i := 0; i < size; i++ {
		buf.WriteString(fmt.Sprintf(`{"id":%d,"value":%d}\n`, i, i*2))
	}
	ndjsonData := buf.String()

	chunkSizes := []int{100, 1000, 5000}
	
	for _, chunkSize := range chunkSizes {
		b.Run(fmt.Sprintf("chunk=%d", chunkSize), func(b *testing.B) {
			b.SetBytes(int64(len(ndjsonData)))
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				reader := NewNDJSONReader().WithChunkSize(chunkSize)
				_, err := reader.Read(strings.NewReader(ndjsonData))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkNDJSONWriter(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		df := createBenchmarkDataFrame(size)
		
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				writer := NewNDJSONWriter()
				var buf bytes.Buffer
				err := writer.Write(df, &buf)
				if err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(buf.Len()))
			}
		})
	}
}

func BenchmarkCompression(b *testing.B) {
	df := createBenchmarkDataFrame(1000)
	
	b.Run("json_uncompressed", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer := NewWriter()
			var buf bytes.Buffer
			err := writer.Write(df, &buf)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(buf.Len()))
		}
	})

	b.Run("json_gzip", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer := NewWriter()
			var buf bytes.Buffer
			gzWriter := gzip.NewWriter(&buf)
			err := writer.Write(df, gzWriter)
			gzWriter.Close()
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(buf.Len()))
		}
	})

	b.Run("ndjson_uncompressed", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer := NewNDJSONWriter()
			var buf bytes.Buffer
			err := writer.Write(df, &buf)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(buf.Len()))
		}
	})

	b.Run("ndjson_gzip", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer := NewNDJSONWriter()
			var buf bytes.Buffer
			gzWriter := gzip.NewWriter(&buf)
			err := writer.Write(df, gzWriter)
			gzWriter.Close()
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(buf.Len()))
		}
	})
}

func BenchmarkNestedJSON(b *testing.B) {
	// Create nested JSON data
	nestedJSON := `[`
	for i := 0; i < 1000; i++ {
		if i > 0 {
			nestedJSON += ","
		}
		nestedJSON += `{
			"id": ` + string(rune(i)) + `,
			"user": {
				"name": "User` + string(rune(i%26+'A')) + `",
				"details": {
					"age": ` + string(rune(20+i%50)) + `,
					"city": "City` + string(rune(i%10)) + `"
				}
			}
		}`
	}
	nestedJSON += `]`

	b.Run("with_flattening", func(b *testing.B) {
		b.SetBytes(int64(len(nestedJSON)))
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			reader := NewReader(WithFlatten(true))
			_, err := reader.Read(strings.NewReader(nestedJSON))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("without_flattening", func(b *testing.B) {
		b.SetBytes(int64(len(nestedJSON)))
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			reader := NewReader(WithFlatten(false))
			_, err := reader.Read(strings.NewReader(nestedJSON))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkStreaming(b *testing.B) {
	// Create large NDJSON data
	size := 100000
	var buf bytes.Buffer
	for i := 0; i < size; i++ {
		buf.WriteString(fmt.Sprintf(`{"id":%d,"value":%d}\n`, i, i*2))
	}
	ndjsonData := buf.Bytes()

	b.Run("full_read", func(b *testing.B) {
		b.SetBytes(int64(len(ndjsonData)))
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			reader := NewNDJSONReader()
			_, err := reader.Read(bytes.NewReader(ndjsonData))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("streaming_read", func(b *testing.B) {
		b.SetBytes(int64(len(ndjsonData)))
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			reader := NewNDJSONReader().WithChunkSize(10000)
			totalRows := 0
			err := reader.ReadStream(bytes.NewReader(ndjsonData), func(df *frame.DataFrame) error {
				totalRows += dfLen(df)
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSeriesBuilder(b *testing.B) {
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = float64(i)
	}

	b.Run("int64_builder", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			builder := newSeriesBuilder("test", datatypes.Int64{}, len(values))
			for _, v := range values {
				builder.append(v)
			}
			_, err := builder.build()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("string_builder", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			builder := newSeriesBuilder("test", datatypes.String{}, len(values))
			for _, v := range values {
				builder.append(v)
			}
			_, err := builder.build()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}