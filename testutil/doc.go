// Package testutil provides common testing utilities for the Golars project.
// It includes helper functions for creating test data, custom assertions,
// and fixtures to reduce test code duplication and improve test readability.
//
// This package follows Go testing best practices:
//   - Uses t.Helper() to provide better error locations
//   - Provides table-driven test utilities
//   - Focuses on behavior testing over implementation details
//   - Uses standard library testing features
//
// Example usage:
//
//	func TestDataFrameOperation(t *testing.T) {
//	    df := testutil.CreateTestDataFrame(t, 100)
//	    // ... perform operations ...
//	    testutil.AssertDataFrameEqual(t, expected, result)
//	}
package testutil
