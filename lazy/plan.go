package lazy

import (
	"fmt"
	"strings"

	"github.com/davidpalaitis/golars/datatypes"
	"github.com/davidpalaitis/golars/expr"
	"github.com/davidpalaitis/golars/frame"
)

// LogicalPlan represents a node in the query plan tree
type LogicalPlan interface {
	// Schema returns the expected output schema
	Schema() (*datatypes.Schema, error)
	
	// Children returns child nodes
	Children() []LogicalPlan
	
	// WithChildren returns a copy with new children
	WithChildren(children []LogicalPlan) LogicalPlan
	
	// String returns a string representation
	String() string
}

// JoinType represents the type of join operation
type JoinType = frame.JoinType

// DataSource represents a source of data
type DataSource interface {
	Schema() (*datatypes.Schema, error)
	String() string
}

// DataFrameSource wraps an existing DataFrame
type DataFrameSource struct {
	df *frame.DataFrame
}

func NewDataFrameSource(df *frame.DataFrame) DataSource {
	return &DataFrameSource{df: df}
}

func (s *DataFrameSource) Schema() (*datatypes.Schema, error) {
	return s.df.Schema(), nil
}

func (s *DataFrameSource) String() string {
	if s == nil || s.df == nil {
		return "DataFrame[nil]"
	}
	return fmt.Sprintf("DataFrame[%d × %d]", s.df.Height(), s.df.Width())
}

// CSVSource represents a CSV file to be read
type CSVSource struct {
	path    string
	columns []string
	// In a real implementation, we'd store CSV options here
}

func NewCSVSource(path string) DataSource {
	return &CSVSource{path: path}
}

func (s *CSVSource) Schema() (*datatypes.Schema, error) {
	// In a real implementation, we'd read the header or use stored schema
	return nil, fmt.Errorf("CSV schema inference not implemented in lazy mode")
}

func (s *CSVSource) String() string {
	return fmt.Sprintf("CSV[%s]", s.path)
}

// ParquetSource represents a Parquet file to be read
type ParquetSource struct {
	path    string
	columns []string
	// Parquet files have embedded schema, so we can read it lazily
}

func NewParquetSource(path string) DataSource {
	return &ParquetSource{path: path}
}

func (s *ParquetSource) Schema() (*datatypes.Schema, error) {
	// For Parquet, we can actually read the schema without reading the data
	// This is one of the advantages of the Parquet format
	return nil, fmt.Errorf("Parquet schema reading not implemented in lazy mode")
}

func (s *ParquetSource) String() string {
	return fmt.Sprintf("Parquet[%s]", s.path)
}

// ScanNode reads data from a source
type ScanNode struct {
	source  DataSource
	columns []string
	filters []expr.Expr
}

func NewScanNode(source DataSource) *ScanNode {
	return &ScanNode{
		source:  source,
		columns: nil, // nil means all columns
		filters: []expr.Expr{},
	}
}

func (n *ScanNode) Schema() (*datatypes.Schema, error) {
	schema, err := n.source.Schema()
	if err != nil {
		return nil, err
	}

	// If specific columns requested, filter schema
	if n.columns != nil {
		fields := make([]datatypes.Field, 0, len(n.columns))
		for _, colName := range n.columns {
			for _, field := range schema.Fields {
				if field.Name == colName {
					fields = append(fields, field)
					break
				}
			}
		}
		return datatypes.NewSchema(fields...), nil
	}

	return schema, nil
}

func (n *ScanNode) Children() []LogicalPlan {
	return nil
}

func (n *ScanNode) WithChildren(children []LogicalPlan) LogicalPlan {
	if len(children) != 0 {
		panic("ScanNode cannot have children")
	}
	return n
}

func (n *ScanNode) String() string {
	var parts []string
	parts = append(parts, fmt.Sprintf("Scan %s", n.source))
	
	if n.columns != nil {
		parts = append(parts, fmt.Sprintf("  Columns: %v", n.columns))
	}
	
	if len(n.filters) > 0 {
		filterStrs := make([]string, len(n.filters))
		for i, f := range n.filters {
			filterStrs[i] = f.String()
		}
		parts = append(parts, fmt.Sprintf("  Filters: %s", strings.Join(filterStrs, " AND ")))
	}
	
	return strings.Join(parts, "\n")
}

// FilterNode applies a predicate
type FilterNode struct {
	input     LogicalPlan
	predicate expr.Expr
}

func NewFilterNode(input LogicalPlan, predicate expr.Expr) *FilterNode {
	return &FilterNode{
		input:     input,
		predicate: predicate,
	}
}

func (n *FilterNode) Schema() (*datatypes.Schema, error) {
	return n.input.Schema()
}

func (n *FilterNode) Children() []LogicalPlan {
	return []LogicalPlan{n.input}
}

func (n *FilterNode) WithChildren(children []LogicalPlan) LogicalPlan {
	if len(children) != 1 {
		panic("FilterNode must have exactly one child")
	}
	return &FilterNode{
		input:     children[0],
		predicate: n.predicate,
	}
}

func (n *FilterNode) String() string {
	return fmt.Sprintf("Filter [%s]\n%s", n.predicate, indent(n.input.String()))
}

// ProjectNode selects and computes expressions
type ProjectNode struct {
	input   LogicalPlan
	exprs   []expr.Expr
	aliases []string
}

func NewProjectNode(input LogicalPlan, exprs []expr.Expr) *ProjectNode {
	aliases := make([]string, len(exprs))
	for i, e := range exprs {
		aliases[i] = getExprName(e)
	}
	return &ProjectNode{
		input:   input,
		exprs:   exprs,
		aliases: aliases,
	}
}

func (n *ProjectNode) Schema() (*datatypes.Schema, error) {
	// Build schema from expressions
	fields := make([]datatypes.Field, len(n.exprs))
	for i := range n.exprs {
		// In a real implementation, we'd infer the type from the expression
		fields[i] = datatypes.Field{
			Name:     n.aliases[i],
			DataType: datatypes.String{}, // Placeholder
			Nullable: true,
		}
	}
	return datatypes.NewSchema(fields...), nil
}

func (n *ProjectNode) Children() []LogicalPlan {
	return []LogicalPlan{n.input}
}

func (n *ProjectNode) WithChildren(children []LogicalPlan) LogicalPlan {
	if len(children) != 1 {
		panic("ProjectNode must have exactly one child")
	}
	return &ProjectNode{
		input:   children[0],
		exprs:   n.exprs,
		aliases: n.aliases,
	}
}

func (n *ProjectNode) String() string {
	exprStrs := make([]string, len(n.exprs))
	for i, e := range n.exprs {
		if n.aliases[i] != getExprName(e) {
			exprStrs[i] = fmt.Sprintf("%s as %s", e, n.aliases[i])
		} else {
			exprStrs[i] = e.String()
		}
	}
	return fmt.Sprintf("Project [%s]\n%s", strings.Join(exprStrs, ", "), indent(n.input.String()))
}

// GroupByNode performs grouping and aggregation
type GroupByNode struct {
	input    LogicalPlan
	keys     []expr.Expr
	aggs     []expr.Expr
	aggNames []string // Names for the aggregation results
}

func NewGroupByNode(input LogicalPlan, keys []expr.Expr, aggs []expr.Expr) *GroupByNode {
	// Generate default names for aggregations
	aggNames := make([]string, len(aggs))
	for i, agg := range aggs {
		aggNames[i] = getExprName(agg)
	}
	
	return &GroupByNode{
		input:    input,
		keys:     keys,
		aggs:     aggs,
		aggNames: aggNames,
	}
}

// NewGroupByNodeWithNames creates a GroupByNode with explicit aggregation names
func NewGroupByNodeWithNames(input LogicalPlan, keys []expr.Expr, aggs []expr.Expr, aggNames []string) *GroupByNode {
	return &GroupByNode{
		input:    input,
		keys:     keys,
		aggs:     aggs,
		aggNames: aggNames,
	}
}

func (n *GroupByNode) Schema() (*datatypes.Schema, error) {
	// Build schema from keys and aggregations
	fields := make([]datatypes.Field, 0, len(n.keys)+len(n.aggs))
	
	// Add key fields
	for _, k := range n.keys {
		fields = append(fields, datatypes.Field{
			Name:     getExprName(k),
			DataType: datatypes.String{}, // Placeholder
			Nullable: false,
		})
	}
	
	// Add aggregation fields
	for i := range n.aggs {
		fields = append(fields, datatypes.Field{
			Name:     n.aggNames[i],
			DataType: datatypes.Float64{}, // Placeholder
			Nullable: true,
		})
	}
	
	return datatypes.NewSchema(fields...), nil
}

func (n *GroupByNode) Children() []LogicalPlan {
	return []LogicalPlan{n.input}
}

func (n *GroupByNode) WithChildren(children []LogicalPlan) LogicalPlan {
	if len(children) != 1 {
		panic("GroupByNode must have exactly one child")
	}
	return &GroupByNode{
		input:    children[0],
		keys:     n.keys,
		aggs:     n.aggs,
		aggNames: n.aggNames,
	}
}

func (n *GroupByNode) String() string {
	keyStrs := make([]string, len(n.keys))
	for i, k := range n.keys {
		keyStrs[i] = k.String()
	}
	
	aggStrs := make([]string, len(n.aggs))
	for i, a := range n.aggs {
		aggStrs[i] = a.String()
	}
	
	return fmt.Sprintf("GroupBy [%s]\n  Aggs: [%s]\n%s", 
		strings.Join(keyStrs, ", "),
		strings.Join(aggStrs, ", "),
		indent(n.input.String()))
}

// JoinNode performs a join operation
type JoinNode struct {
	left  LogicalPlan
	right LogicalPlan
	on    []string
	how   JoinType
}

func NewJoinNode(left, right LogicalPlan, on []string, how JoinType) *JoinNode {
	return &JoinNode{
		left:  left,
		right: right,
		on:    on,
		how:   how,
	}
}

func (n *JoinNode) Schema() (*datatypes.Schema, error) {
	leftSchema, err := n.left.Schema()
	if err != nil {
		return nil, err
	}
	
	rightSchema, err := n.right.Schema()
	if err != nil {
		return nil, err
	}
	
	// Combine schemas based on join type
	fields := make([]datatypes.Field, 0)
	
	// Add left fields
	fields = append(fields, leftSchema.Fields...)
	
	// Add right fields (except join keys)
	joinKeys := make(map[string]bool)
	for _, k := range n.on {
		joinKeys[k] = true
	}
	
	for _, field := range rightSchema.Fields {
		if !joinKeys[field.Name] {
			fields = append(fields, field)
		}
	}
	
	return datatypes.NewSchema(fields...), nil
}

func (n *JoinNode) Children() []LogicalPlan {
	return []LogicalPlan{n.left, n.right}
}

func (n *JoinNode) WithChildren(children []LogicalPlan) LogicalPlan {
	if len(children) != 2 {
		panic("JoinNode must have exactly two children")
	}
	return &JoinNode{
		left:  children[0],
		right: children[1],
		on:    n.on,
		how:   n.how,
	}
}

func (n *JoinNode) String() string {
	return fmt.Sprintf("%s Join on [%s]\n%s\n%s",
		n.how,
		strings.Join(n.on, ", "),
		indent("Left:\n"+indent(n.left.String())),
		indent("Right:\n"+indent(n.right.String())))
}

// SortNode sorts the data
type SortNode struct {
	input   LogicalPlan
	by      []expr.Expr
	reverse []bool
}

func NewSortNode(input LogicalPlan, by []expr.Expr, reverse []bool) *SortNode {
	return &SortNode{
		input:   input,
		by:      by,
		reverse: reverse,
	}
}

func (n *SortNode) Schema() (*datatypes.Schema, error) {
	return n.input.Schema()
}

func (n *SortNode) Children() []LogicalPlan {
	return []LogicalPlan{n.input}
}

func (n *SortNode) WithChildren(children []LogicalPlan) LogicalPlan {
	if len(children) != 1 {
		panic("SortNode must have exactly one child")
	}
	return &SortNode{
		input:   children[0],
		by:      n.by,
		reverse: n.reverse,
	}
}

func (n *SortNode) String() string {
	sortStrs := make([]string, len(n.by))
	for i, e := range n.by {
		if n.reverse[i] {
			sortStrs[i] = fmt.Sprintf("%s DESC", e)
		} else {
			sortStrs[i] = fmt.Sprintf("%s ASC", e)
		}
	}
	return fmt.Sprintf("Sort [%s]\n%s", strings.Join(sortStrs, ", "), indent(n.input.String()))
}

// LimitNode limits the number of rows
type LimitNode struct {
	input LogicalPlan
	limit int
}

func NewLimitNode(input LogicalPlan, limit int) *LimitNode {
	return &LimitNode{
		input: input,
		limit: limit,
	}
}

func (n *LimitNode) Schema() (*datatypes.Schema, error) {
	return n.input.Schema()
}

func (n *LimitNode) Children() []LogicalPlan {
	return []LogicalPlan{n.input}
}

func (n *LimitNode) WithChildren(children []LogicalPlan) LogicalPlan {
	if len(children) != 1 {
		panic("LimitNode must have exactly one child")
	}
	return &LimitNode{
		input: children[0],
		limit: n.limit,
	}
}

func (n *LimitNode) String() string {
	return fmt.Sprintf("Limit [%d]\n%s", n.limit, indent(n.input.String()))
}

// Helper functions

func getExprName(e expr.Expr) string {
	// In a real implementation, expressions would have a Name() method
	// For now, extract column name from column expressions
	str := e.String()
	// Handle "col(name)" format
	if len(str) > 4 && str[:4] == "col(" && str[len(str)-1] == ')' {
		return str[4 : len(str)-1]
	}
	return str
}

func indent(s string) string {
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = "  " + lines[i]
	}
	return strings.Join(lines, "\n")
}