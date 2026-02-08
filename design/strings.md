# internal/strings -- String Operations

## Purpose

Provides string manipulation functions that operate element-wise on string
Series: case conversion, trimming, padding, splitting, regex matching,
encoding, formatting, and parsing.

## Key Design Decisions

**Element-wise pattern.** Every function follows the same pattern: extract the
string slice from the input Series, apply a Go stdlib string operation to each
element (respecting nulls), and return a new Series with the results. Null
values pass through unchanged.

**File organization by category:**
- `case.go` -- ToUpper, ToLower, ToTitle, Capitalize, SwapCase
- `trim.go` -- Trim, TrimLeft, TrimRight, Strip, LStrip, RStrip
- `ops.go` -- Contains, StartsWith, EndsWith, Replace, Split, Join, Repeat,
  Slice, Pad, Len, Reverse, Concat
- `regex.go` -- Match, Extract, ExtractAll, Replace (regex-based)
- `pattern.go` -- Glob/wildcard matching, like/ilike SQL patterns
- `parse.go` -- ParseInt, ParseFloat, ParseBool, ParseDate, and type detection
- `format.go` -- Sprintf-style formatting, number formatting, zfill
- `encoding.go` -- Base64 encode/decode, URL encode/decode, hex encode/decode

**Expression integration.** `expr.go` wraps string operations as expression
types compatible with the DataFrame expression evaluator, allowing them to be
used in `WithColumn` and lazy evaluation contexts.

**No Arrow dependency.** String operations work on Go `[]string` slices
extracted from Series via `ToSlice()`. They do not use Arrow string kernels.
This keeps the implementation simple and avoids CGO overhead for string work.

**Null handling.** Every operation checks validity before processing. If the
input value is null, the output is null. Functions that produce boolean results
(Contains, Match) return false for null inputs.
