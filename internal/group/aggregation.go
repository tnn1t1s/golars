package group

import (
	_ "fmt"
	_ "math"
	_ "sort"

	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// AggregationResult holds the results of aggregation operations
type AggregationResult struct {
	GroupKeys map[uint64][]interface{}
	Results   map[string][]interface{}
	DataTypes map[string]datatypes.DataType
}

// AggResult represents the result of an aggregation operation
type AggResult struct {
	Columns []series.Series
}

// Agg performs multiple aggregations on the grouped data
func (gb *GroupBy) Agg(aggregations map[string]expr.Expr) (*AggResult, error) {
	panic("not implemented")

}

// Sum performs sum aggregation on specified columns
func (gb *GroupBy) Sum(columns ...string) (*AggResult, error) {
	panic("not implemented")

}

// Mean performs mean aggregation on specified columns
func (gb *GroupBy) Mean(columns ...string) (*AggResult, error) {
	panic("not implemented")

}

// Count returns the count of rows in each group
func (gb *GroupBy) Count() (*AggResult, error) {
	panic("not implemented")

}

// Min performs min aggregation on specified columns
func (gb *GroupBy) Min(columns ...string) (*AggResult, error) {
	panic("not implemented")

}

// Max performs max aggregation on specified columns
func (gb *GroupBy) Max(columns ...string) (*AggResult, error) {
	panic("not implemented")

}

// applyAggregation applies a single aggregation to a group
func (gb *GroupBy) applyAggregation(result *AggregationResult, hash uint64,
	indices []int, colName string, aggExpr expr.Expr) error {
	panic("not implemented")

}

// evaluateAggExpr recursively evaluates an aggregation expression
func (gb *GroupBy) evaluateAggExpr(indices []int, e expr.Expr) (interface{}, datatypes.DataType, error) {
	panic("not implemented")

	// Handle arithmetic on aggregations (e.g., Max() - Min())

}

func sameCorr(left, right *expr.CorrExpr) bool {
	panic("not implemented")

}

// evaluateSimpleAgg evaluates a simple aggregation expression
func (gb *GroupBy) evaluateSimpleAgg(indices []int, agg *expr.AggExpr) (interface{}, datatypes.DataType, error) {
	panic("not implemented")

}

// evaluateTopK evaluates a top-k aggregation
func (gb *GroupBy) evaluateTopK(indices []int, topk *expr.TopKExpr) (interface{}, datatypes.DataType, error) {
	panic("not implemented")

	// Extract and sort values

	// Sort in appropriate order

	// Take top k

}

// evaluateCorr evaluates a correlation between two columns
func (gb *GroupBy) evaluateCorr(indices []int, corr *expr.CorrExpr) (interface{}, datatypes.DataType, error) {
	panic("not implemented")

	// Extract paired values (skip if either is null)

	// Compute Pearson correlation

}

// computeCorrelation computes Pearson correlation coefficient
func computeCorrelation(x, y []float64) float64 {
	panic("not implemented")

	// Calculate means

	// Calculate covariance and standard deviations

}

// applyBinaryOp applies a binary operation to two values
func applyBinaryOp(op expr.BinaryOp, left, right interface{}) interface{} {
	panic("not implemented")

}

// buildResultDataFrame builds the final DataFrame from aggregation results
func (gb *GroupBy) buildResultDataFrame(result *AggregationResult) (*AggResult, error) {
	panic("not implemented")

	// Add group columns in order

	// Get original column to determine type

	// Add aggregation result columns

}

// Aggregation compute functions

func computeSum(values []interface{}, dtype datatypes.DataType) interface{} {
	panic("not implemented")

}

func computeMean(values []interface{}, dtype datatypes.DataType) interface{} {
	panic("not implemented")

}

func computeMin(values []interface{}, dtype datatypes.DataType) interface{} {
	panic("not implemented")

}

func computeMax(values []interface{}, dtype datatypes.DataType) interface{} {
	panic("not implemented")

}

func computeMedian(values []interface{}, dtype datatypes.DataType) interface{} {
	panic(
		// Filter out nil values and convert to float64
		"not implemented")

	// Sort the values

	// Calculate median

	// Even number of values: average of two middle values

	// Odd number of values: middle value

}

func computeVar(values []interface{}, dtype datatypes.DataType, ddof int) interface{} {
	panic(
		// Filter out nil values and convert to float64
		"not implemented")

	// Calculate mean

	// Calculate variance

}

func computeStd(values []interface{}, dtype datatypes.DataType, ddof int) interface{} {
	panic("not implemented")

}

func computeFirst(values []interface{}, dtype datatypes.DataType) interface{} {
	panic("not implemented")

}

func computeLast(values []interface{}, dtype datatypes.DataType) interface{} {
	panic("not implemented")

}

// Helper functions for type conversion

func toInt64(v interface{}) int64 {
	panic("not implemented")

}

func toUint64(v interface{}) uint64 {
	panic("not implemented")

}

func toFloat64(v interface{}) float64 {
	panic("not implemented")

}

func compareValues(a, b interface{}, dtype datatypes.DataType) int {
	panic("not implemented")

}

func convertToType(v interface{}, dtype datatypes.DataType) interface{} {
	panic("not implemented")

}

// createSeriesFromInterface creates a series from a slice of interface{} values
func createSeriesFromInterface(name string, values []interface{}, dtype datatypes.DataType) series.Series {
	panic("not implemented")

	// Check if this is a list of float64 slices (e.g., from TopK)

	// This is a list column - store slices as interface{}

	// Fallback - create string series

}
