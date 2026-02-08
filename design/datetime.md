# internal/datetime -- Temporal Operations

## Purpose

Provides date/time parsing, formatting, component extraction, arithmetic,
timezone handling, range generation, and resampling. Integrates with both
Series (for column-level operations) and DataFrame expressions.

## Key Design Decisions

**Time representation.** Dates are stored as `int32` (days since epoch) in
Arrow Date32 arrays. Timestamps are `int64` (microseconds since epoch) in
Arrow Timestamp arrays. Durations are `int64` in Arrow Duration arrays. All
conversions go through Go's `time.Time` as the intermediate representation.

**Component extraction.** `components.go` extracts Year, Month, Day, Hour,
Minute, Second, etc. from a datetime Series. Each component function converts
the underlying int64/int32 to `time.Time`, extracts the field, and returns a
new Series of the appropriate type.

**Arithmetic.** `arithmetic.go` supports adding/subtracting durations from
dates and timestamps, computing differences between timestamps, and
date offset operations. Operations are element-wise over the Series.

**Parsing.** `parser.go` converts string Series to datetime Series. It
supports multiple format strings (Go layout format) and attempts them in
order. Auto-detection tries common formats (RFC3339, ISO 8601, etc.)
when no explicit format is given.

**Formatting.** `format.go` converts datetime Series to string Series using
Go's time.Format layout strings.

**Timezone.** `timezone.go` handles timezone conversion and localization.
It converts between timezones by adjusting the underlying timestamp values
and storing the timezone in metadata.

**Range generation.** `range.go` generates date/time sequences with a given
start, end, and interval. Used for creating time-series indices.

**Resampling.** `resample.go` groups datetime-indexed data into time buckets
(e.g., "5min", "1h", "1d") and applies aggregations per bucket. It truncates
timestamps to bucket boundaries and delegates to the group-by engine.

**Expression integration.** `expr.go` defines datetime-specific expression
types that can be used in DataFrame.WithColumn and lazy evaluation contexts.

**DateTimeSeries.** `series.go` defines a `DateTimeSeries` wrapper around
a regular Series that provides datetime-specific methods (Year, Month,
AddDays, etc.) as a fluent API.
