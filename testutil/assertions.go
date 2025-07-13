package testutil

import (
	"fmt"
	"math"
	"testing"

	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

// AssertDataFrameEqual compares two DataFrames for equality
func AssertDataFrameEqual(t *testing.T, expected, actual *frame.DataFrame) {
	t.Helper()

	if expected == nil && actual == nil {
		return
	}

	if expected == nil {
		t.Errorf("expected DataFrame is nil, but actual is not")
		return
	}

	if actual == nil {
		t.Errorf("expected DataFrame is not nil, but actual is nil")
		return
	}

	// Check dimensions
	if expected.Height() != actual.Height() {
		t.Errorf("DataFrame height mismatch: expected %d, got %d", expected.Height(), actual.Height())
		return
	}

	if expected.Width() != actual.Width() {
		t.Errorf("DataFrame width mismatch: expected %d, got %d", expected.Width(), actual.Width())
		return
	}

	// Check column names
	expectedCols := expected.Columns()
	actualCols := actual.Columns()

	for i, expectedCol := range expectedCols {
		if expectedCol != actualCols[i] {
			t.Errorf("Column name mismatch at index %d: expected %q, got %q", i, expectedCol, actualCols[i])
			return
		}
	}

	// Compare each column
	for i := 0; i < expected.Width(); i++ {
		expectedSeries, _ := expected.ColumnAt(i)
		actualSeries, _ := actual.ColumnAt(i)
		AssertSeriesEqual(t, expectedSeries, actualSeries)
	}
}

// AssertSeriesEqual compares two Series for equality
func AssertSeriesEqual(t *testing.T, expected, actual series.Series) {
	t.Helper()

	if expected == nil && actual == nil {
		return
	}

	if expected == nil {
		t.Errorf("expected Series is nil, but actual is not")
		return
	}

	if actual == nil {
		t.Errorf("expected Series is not nil, but actual is nil")
		return
	}

	// Check basic properties
	if expected.Name() != actual.Name() {
		t.Errorf("Series name mismatch: expected %q, got %q", expected.Name(), actual.Name())
	}

	if expected.Len() != actual.Len() {
		t.Errorf("Series length mismatch for %q: expected %d, got %d", expected.Name(), expected.Len(), actual.Len())
		return
	}

	if !expected.DataType().Equals(actual.DataType()) {
		t.Errorf("Series data type mismatch for %q: expected %v, got %v", expected.Name(), expected.DataType(), actual.DataType())
	}

	if expected.NullCount() != actual.NullCount() {
		t.Errorf("Series null count mismatch for %q: expected %d, got %d", expected.Name(), expected.NullCount(), actual.NullCount())
	}

	// Compare values
	for i := 0; i < expected.Len(); i++ {
		expectedValid := expected.IsValid(i)
		actualValid := actual.IsValid(i)

		if expectedValid != actualValid {
			t.Errorf("Series %q validity mismatch at index %d: expected %v, got %v", expected.Name(), i, expectedValid, actualValid)
			continue
		}

		if !expectedValid {
			// Both are null, continue
			continue
		}

		expectedVal := expected.Get(i)
		actualVal := actual.Get(i)

		if !valuesEqual(expectedVal, actualVal) {
			t.Errorf("Series %q value mismatch at index %d: expected %v, got %v", expected.Name(), i, expectedVal, actualVal)
		}
	}
}

// valuesEqual compares two values for equality, handling floating point comparison
func valuesEqual(expected, actual interface{}) bool {
	// Handle nil cases
	if expected == nil && actual == nil {
		return true
	}
	if expected == nil || actual == nil {
		return false
	}

	// Type assertion based comparisons
	switch e := expected.(type) {
	case float64:
		if a, ok := actual.(float64); ok {
			return floatEquals(e, a)
		}
	case float32:
		if a, ok := actual.(float32); ok {
			return floatEquals(float64(e), float64(a))
		}
	}

	// Default equality
	return expected == actual
}

// floatEquals compares two floats with a small epsilon for precision issues
func floatEquals(a, b float64) bool {
	const epsilon = 1e-10
	
	// Handle special cases
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	if math.IsInf(a, 1) && math.IsInf(b, 1) {
		return true
	}
	if math.IsInf(a, -1) && math.IsInf(b, -1) {
		return true
	}
	
	return math.Abs(a-b) < epsilon
}

// AssertPanic checks that a function panics with an expected message
func AssertPanic(t *testing.T, expectedMsg string, fn func()) {
	t.Helper()

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected panic with message %q, but no panic occurred", expectedMsg)
			return
		}

		msg := fmt.Sprint(r)
		if expectedMsg != "" && msg != expectedMsg {
			t.Errorf("expected panic message %q, got %q", expectedMsg, msg)
		}
	}()

	fn()
}

// AssertNoPanic checks that a function does not panic
func AssertNoPanic(t *testing.T, fn func()) {
	t.Helper()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("unexpected panic: %v", r)
		}
	}()

	fn()
}

// AssertErrorContains checks that an error is not nil and contains the expected substring
func AssertErrorContains(t *testing.T, err error, substr string) {
	t.Helper()

	if err == nil {
		t.Errorf("expected error containing %q, but got nil", substr)
		return
	}

	if substr == "" {
		return
	}

	errMsg := err.Error()
	if !contains(errMsg, substr) {
		t.Errorf("error %q does not contain expected substring %q", errMsg, substr)
	}
}

// contains is a simple string contains check
func contains(s, substr string) bool {
	return len(substr) == 0 || len(s) >= len(substr) && hasSubstring(s, substr)
}

func hasSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// AssertInDelta asserts that two float values are within delta of each other
func AssertInDelta(t *testing.T, expected, actual, delta float64, msgAndArgs ...interface{}) {
	t.Helper()

	diff := math.Abs(expected - actual)
	if diff > delta {
		msg := "values not within delta"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprint(msgAndArgs...)
		}
		t.Errorf("%s: expected %v, actual %v, delta %v", msg, expected, actual, delta)
	}
}