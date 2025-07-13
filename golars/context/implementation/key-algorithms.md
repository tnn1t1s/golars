# Key Algorithms and Implementation Details

## 1. ChunkedArray Indexing

The ChunkedArray stores multiple Arrow arrays as chunks. Indexing requires:

```go
// Find which chunk contains index i
offset := int64(0)
for _, chunk := range ca.chunks {
    if i < offset + int64(chunk.Len()) {
        localIdx := int(i - offset)
        return chunk.Value(localIdx)
    }
    offset += int64(chunk.Len())
}
```

## 2. Series Type Erasure

Series uses dynamic dispatch through interfaces:

```go
type Series interface {
    // Methods...
}

type TypedSeries[T ArrayValue] struct {
    chunkedArray *ChunkedArray[T]
    name         string
}
```

Key insight: Generic struct implements non-generic interface.

## 3. Filter Algorithm

Filtering uses a two-pass approach:

1. **Evaluate expression** → boolean mask
2. **Gather values** where mask is true

```go
// Pass 1: Count true values
outputSize := 0
for _, keep := range mask {
    if keep { outputSize++ }
}

// Pass 2: Collect indices
indices := make([]int, 0, outputSize)
for i, keep := range mask {
    if keep { indices = append(indices, i) }
}

// Pass 3: Gather values at indices
```

## 4. Expression Evaluation

Expressions are evaluated recursively:

```go
func evaluateExpr(expr Expr) ([]interface{}, error) {
    switch e := expr.(type) {
    case *ColumnExpr:
        return getColumnValues(e.Name())
    case *LiteralExpr:
        return broadcastScalar(e.Value(), length)
    case *BinaryExpr:
        left := evaluateExpr(e.Left())
        right := evaluateExpr(e.Right())
        return applyBinaryOp(left, right, e.Op())
    }
}
```

## 5. Null Handling

Nulls are tracked using Arrow's validity bitmaps:
- Each array has a null bitmap
- Null count is cached for performance
- Operations propagate nulls (null + anything = null)

## 6. Memory Management

- Uses Arrow's memory allocator
- Builders for creating new arrays
- Reference counting on shared data
- Release() must be called to free memory

## 7. Type Dispatch

Many operations use type switches:

```go
switch arr := array.(type) {
case *array.Int32:
    return processInt32(arr)
case *array.Float64:
    return processFloat64(arr)
// etc...
}
```

## 8. String Comparison in Filters

String equality in filters converts literals to column references by default.
Use `Lit()` explicitly for string literals:

```go
// Wrong: treats "foo" as column name
df.Filter(Col("name").Eq("foo"))

// Correct: treats "foo" as string value
df.Filter(Col("name").Eq(Lit("foo")))
```

## 9. Performance Optimizations

1. **Chunk Size**: Larger chunks = better cache locality
2. **Null Checks**: Check null count first to skip null handling
3. **Type Specialization**: Avoid interface{} in hot paths
4. **Batch Operations**: Process entire arrays, not individual values

## 10. Thread Safety Pattern

```go
type SafeStruct struct {
    mu sync.RWMutex
    // fields...
}

func (s *SafeStruct) Read() {
    s.mu.RLock()
    defer s.mu.RUnlock()
    // read operations...
}

func (s *SafeStruct) Write() {
    s.mu.Lock()
    defer s.mu.Unlock()
    // write operations...
}
```