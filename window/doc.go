// Package window provides window function support for Golars DataFrames.
//
// Window functions perform calculations across a set of rows that are related
// to the current row. Unlike aggregate functions that return a single result
// per group, window functions return a result for every row while still being
// able to access data from other rows in the window.
//
// # Basic Usage
//
// Window functions are created by combining a function with a window specification:
//
//	import (
//	    "github.com/davidpalaitis/golars"
//	    "github.com/davidpalaitis/golars/window"
//	)
//
//	// Create a window specification
//	spec := window.NewSpec().
//	    PartitionBy("category").
//	    OrderBy("date")
//
//	// Apply window function
//	df.WithColumn("row_num", window.RowNumber().Over(spec))
//
// # Window Specifications
//
// A window specification defines how rows are partitioned and ordered:
//
//	spec := window.NewSpec().
//	    PartitionBy("region", "product").  // Divide rows into groups
//	    OrderBy("date", true).             // Sort within each partition
//	    RowsBetween(-7, 0)                 // Define frame boundaries
//
// # Frame Specifications
//
// Window frames define which rows are included in calculations:
//
//	// Fixed window of 7 preceding rows to current row
//	spec.RowsBetween(-7, 0)
//
//	// All rows from start to current
//	spec.UnboundedPreceding().CurrentRow()
//
//	// Range-based window (not fully implemented yet)
//	spec.RangeBetween(startValue, endValue)
//
// # Function Types
//
// The package supports several types of window functions:
//
// Ranking functions:
//   - RowNumber(): Sequential row numbers
//   - Rank(): Ranking with gaps for ties
//   - DenseRank(): Ranking without gaps
//   - PercentRank(): Percentile ranking
//   - NTile(n): Divide rows into n buckets
//
// Value functions:
//   - FirstValue(): First value in window
//   - LastValue(): Last value in window
//   - Lag(n): Value from n rows before
//   - Lead(n): Value from n rows after
//   - NthValue(n): Value from nth row
//
// Aggregate functions (window-aware):
//   - Sum(), Mean(), Min(), Max(), Count(), etc.
//
package window