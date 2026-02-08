# internal/chunked -- Columnar Storage Layer

## Purpose

`ChunkedArray[T]` is the lowest-level storage primitive. It wraps one or more
Apache Arrow arrays ("chunks") behind a single generic interface. All data in
Golars ultimately lives in chunked arrays.

## Key Design Decisions

**Generic over `datatypes.ArrayValue`.** The type parameter `T` is constrained
to the union of Go primitives that Arrow supports (bool, int8..int64,
uint8..uint64, float32, float64, string, []byte). This lets a single struct
serve all column types while keeping value access type-safe.

**Multi-chunk layout.** A `ChunkedArray` holds a `[]arrow.Array` slice. Most
operations create a single chunk, but appends and slices can produce multiple
chunks. All index-based access (Get, IsValid) must walk the chunk list to find
the correct local index.

**Arrow bridge pattern.** Conversion between Go values and Arrow arrays uses a
large type-switch over Arrow builder/array concrete types (e.g.,
`*array.Int64Builder`, `*array.Int64`). This is the canonical pattern in
arrow-go; there is no way around the switch. The `datatypes.PolarsDataType`
mapping provides the Arrow type for a given Golars type via `ArrowType()` and
`NewBuilder(allocator)`.

**Concurrency.** A `sync.RWMutex` protects all reads and writes. Read methods
(Get, Len, Chunks) take RLock; mutating methods (AppendArray, Release) take
full Lock.

**Reference counting.** Arrow arrays are reference-counted. `AppendArray` calls
`arr.Retain()` to keep the array alive. `Release()` calls `chunk.Release()` on
every chunk. Forgetting Retain/Release causes use-after-free or memory leaks.

## ChunkedBuilder

`ChunkedBuilder[T]` is an incremental builder. It lazily creates a single Arrow
builder on first `Append` call, then `Finish()` materializes the builder into
an Arrow array and wraps it in a `ChunkedArray`. The builder factory
(`createBuilder`) maps `datatypes.DataType` to the corresponding
`array.*Builder` using `memory.DefaultAllocator`.
