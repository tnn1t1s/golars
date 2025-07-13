// Package strings provides comprehensive string manipulation operations for Golars.
// It includes pattern matching, regular expressions, case transformations, parsing,
// and formatting operations that integrate seamlessly with the DataFrame and Series APIs.
//
// All string operations properly handle null values and support UTF-8 encoded strings.
// Operations are available both as Series methods and as expressions for use in
// DataFrame transformations.
//
// Example:
//
//	df := golars.NewDataFrame(
//	    golars.NewStringSeries("names", []string{"Alice", "Bob", "Charlie"}),
//	)
//	
//	result := df.WithColumn("upper",
//	    golars.Col("names").Str().ToUpper(),
//	).WithColumn("length",
//	    golars.Col("names").Str().Length(),
//	)
package strings