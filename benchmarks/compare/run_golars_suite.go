package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/expr"
	"github.com/tnn1t1s/golars/frame"
	"github.com/tnn1t1s/golars/series"
)

type suiteSpec struct {
	Datasets map[string]datasetSpec `json:"datasets"`
	Queries  []querySpec            `json:"queries"`
}

type datasetSpec struct {
	Path      string  `json:"path"`
	Rows      int     `json:"rows"`
	Groups    int     `json:"groups"`
	NullRatio float64 `json:"null_ratio"`
	Seed      int64   `json:"seed"`
	Sorted    bool    `json:"sorted"`
}

type querySpec struct {
	Name        string            `json:"name"`
	Dataset     string            `json:"dataset"`
	Type        string            `json:"type"`
	GroupBy     []string          `json:"group_by"`
	Aggs        []aggSpec         `json:"aggs"`
	Filter      *filterSpec       `json:"filter"`
	Join        *joinSpec         `json:"join"`
	Sort        *sortSpec         `json:"sort"`
	WithColumns *withColumnsSpec  `json:"with_columns"`
	Meta        map[string]string `json:"meta"`
}

type aggSpec struct {
	Op  string `json:"op"`
	Col string `json:"col"`
	As  string `json:"as"`
}

type filterSpec struct {
	Col       string      `json:"col"`
	Op        string      `json:"op"`
	Value     interface{} `json:"value"`
	ValueType string      `json:"value_type"`
}

type joinSpec struct {
	How       string   `json:"how"`
	LeftOn    []string `json:"left_on"`
	RightOn   []string `json:"right_on"`
	RightRows int      `json:"right_rows"`
}

type sortSpec struct {
	Columns    []string `json:"columns"`
	Descending bool     `json:"descending"`
}

type withColumnsSpec struct {
	Rows    int `json:"rows"`
	Columns int `json:"columns"`
}

type runResult struct {
	Name string  `json:"name"`
	Ms   float64 `json:"ms"`
	Rows int     `json:"rows"`
}

type runOutput struct {
	Engine     string      `json:"engine"`
	Dataset    string      `json:"dataset"`
	DataPath   string      `json:"data_path"`
	DataSha256 string      `json:"data_sha256"`
	Results    []runResult `json:"results"`
}

func main() {
	suitePath := flag.String("suite", "suite.json", "Path to benchmark suite JSON")
	dataset := flag.String("dataset", "small", "Dataset name from suite")
	iterations := flag.Int("iterations", 3, "Number of timing iterations")
	flag.Parse()

	suiteFile, err := resolveSuitePath(*suitePath)
	if err != nil {
		fatalf("resolve suite path: %v", err)
	}

	suite, err := loadSuite(suiteFile)
	if err != nil {
		fatalf("load suite: %v", err)
	}

	spec, ok := suite.Datasets[*dataset]
	if !ok && *dataset != "synthetic" {
		fatalf("unknown dataset: %s", *dataset)
	}

	dataPath := ""
	dataHash := ""
	var df *frame.DataFrame
	if *dataset != "synthetic" {
		dataPath = resolveDatasetPath(filepath.Dir(suiteFile), spec.Path)
		df, err = golars.ReadParquet(dataPath)
		if err != nil {
			fatalf("read dataset: %v", err)
		}
		dataHash, err = fileHash(dataPath)
		if err != nil {
			fatalf("hash dataset: %v", err)
		}
	}

	results := make([]runResult, 0)
	for _, q := range suite.Queries {
		if q.Dataset != *dataset {
			continue
		}
		rows, elapsed, err := runBenchmark(q, df, *iterations)
		if err != nil {
			fatalf("run %s: %v", q.Name, err)
		}
		results = append(results, runResult{
			Name: q.Name,
			Ms:   elapsed,
			Rows: rows,
		})
	}

	out := runOutput{
		Engine:     "golars",
		Dataset:    *dataset,
		DataPath:   dataPath,
		DataSha256: dataHash,
		Results:    results,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fatalf("encode output: %v", err)
	}
}

func resolveSuitePath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	return filepath.Abs(path)
}

func loadSuite(path string) (*suiteSpec, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var suite suiteSpec
	if err := json.Unmarshal(raw, &suite); err != nil {
		return nil, err
	}
	return &suite, nil
}

func resolveDatasetPath(base, rel string) string {
	if filepath.IsAbs(rel) {
		return rel
	}
	return filepath.Join(base, rel)
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func runBenchmark(q querySpec, df *frame.DataFrame, iterations int) (int, float64, error) {
	if iterations < 1 {
		iterations = 1
	}

	// Warmup
	if _, err := runQuery(q, df); err != nil {
		return 0, 0, err
	}

	times := make([]float64, 0, iterations)
	rows := 0
	for i := 0; i < iterations; i++ {
		start := time.Now()
		latestRows, err := runQuery(q, df)
		if err != nil {
			return 0, 0, err
		}
		elapsed := time.Since(start).Seconds() * 1000.0
		times = append(times, elapsed)
		rows = latestRows
	}

	total := 0.0
	for _, t := range times {
		total += t
	}
	avg := total / float64(len(times))
	return rows, avg, nil
}

func runQuery(q querySpec, df *frame.DataFrame) (int, error) {
	switch q.Type {
	case "groupby":
		exprs, err := buildAggExprs(q.Aggs)
		if err != nil {
			return 0, err
		}
		grouped, err := df.GroupBy(q.GroupBy...)
		if err != nil {
			return 0, err
		}
		result, err := grouped.Agg(exprs)
		if err != nil {
			return 0, err
		}
		return result.Height(), nil
	case "filter_agg":
		if q.Filter == nil {
			return 0, fmt.Errorf("missing filter spec")
		}
		predicate, err := buildFilterExpr(q.Filter)
		if err != nil {
			return 0, err
		}
		filtered, err := df.Filter(predicate)
		if err != nil {
			return 0, err
		}
		if err := applyAggs(filtered, q.Aggs); err != nil {
			return 0, err
		}
		return 1, nil
	case "join":
		if q.Join == nil {
			return 0, fmt.Errorf("missing join spec")
		}
		right := df.Head(q.Join.RightRows)
		how, err := joinTypeFromString(q.Join.How)
		if err != nil {
			return 0, err
		}
		result, err := df.JoinOn(right, q.Join.LeftOn, q.Join.RightOn, how)
		if err != nil {
			return 0, err
		}
		return result.Height(), nil
	case "sort":
		if q.Sort == nil {
			return 0, fmt.Errorf("missing sort spec")
		}
		var result *frame.DataFrame
		var err error
		if q.Sort.Descending {
			result, err = df.SortDesc(q.Sort.Columns...)
		} else {
			result, err = df.Sort(q.Sort.Columns...)
		}
		if err != nil {
			return 0, err
		}
		return result.Height(), nil
	case "with_columns":
		if q.WithColumns == nil {
			return 0, fmt.Errorf("missing with_columns spec")
		}
		result, err := runWithColumns(q.WithColumns)
		if err != nil {
			return 0, err
		}
		return result.Height(), nil
	default:
		return 0, fmt.Errorf("unsupported query type: %s", q.Type)
	}
}

func buildAggExprs(aggs []aggSpec) (map[string]expr.Expr, error) {
	exprs := make(map[string]expr.Expr, len(aggs))
	for _, agg := range aggs {
		if agg.As == "" {
			return nil, fmt.Errorf("missing agg alias for %s", agg.Col)
		}
		builder := expr.Col(agg.Col)
		switch agg.Op {
		case "sum":
			exprs[agg.As] = builder.Sum()
		case "mean":
			exprs[agg.As] = builder.Mean()
		case "median":
			exprs[agg.As] = builder.Median()
		case "std":
			exprs[agg.As] = builder.Std()
		case "min":
			exprs[agg.As] = builder.Min()
		case "max":
			exprs[agg.As] = builder.Max()
		case "count":
			exprs[agg.As] = builder.Count()
		default:
			return nil, fmt.Errorf("unsupported agg op: %s", agg.Op)
		}
	}
	return exprs, nil
}

func buildFilterExpr(f *filterSpec) (expr.Expr, error) {
	if f == nil {
		return nil, fmt.Errorf("missing filter spec")
	}
	value, err := literalValue(f.Value, f.ValueType)
	if err != nil {
		return nil, err
	}
	left := expr.Col(f.Col)
	right := expr.Lit(value)
	switch f.Op {
	case "eq":
		return left.Eq(right), nil
	case "ne":
		return left.Ne(right), nil
	case "lt":
		return left.Lt(right), nil
	case "lte":
		return left.Le(right), nil
	case "gt":
		return left.Gt(right), nil
	case "gte":
		return left.Ge(right), nil
	default:
		return nil, fmt.Errorf("unsupported filter op: %s", f.Op)
	}
}

func literalValue(raw interface{}, valueType string) (interface{}, error) {
	switch valueType {
	case "string":
		if s, ok := raw.(string); ok {
			return s, nil
		}
		return fmt.Sprintf("%v", raw), nil
	case "int":
		if v, ok := raw.(float64); ok {
			return int64(v), nil
		}
		return nil, fmt.Errorf("expected numeric value for int")
	case "float":
		if v, ok := raw.(float64); ok {
			return v, nil
		}
		return nil, fmt.Errorf("expected numeric value for float")
	case "bool":
		if v, ok := raw.(bool); ok {
			return v, nil
		}
		return nil, fmt.Errorf("expected boolean value")
	default:
		return raw, nil
	}
}

func applyAggs(df *frame.DataFrame, aggs []aggSpec) error {
	for _, agg := range aggs {
		col, err := df.Column(agg.Col)
		if err != nil {
			return err
		}
		switch agg.Op {
		case "sum":
			_ = col.Sum()
		case "mean":
			_ = col.Mean()
		case "median":
			_ = col.Median()
		case "std":
			_ = col.Std()
		case "min":
			_ = col.Min()
		case "max":
			_ = col.Max()
		case "count":
			_ = col.Count()
		default:
			return fmt.Errorf("unsupported agg op: %s", agg.Op)
		}
	}
	return nil
}

func joinTypeFromString(how string) (golars.JoinType, error) {
	switch how {
	case "inner":
		return golars.InnerJoin, nil
	case "left":
		return golars.LeftJoin, nil
	case "right":
		return golars.RightJoin, nil
	case "outer":
		return golars.OuterJoin, nil
	default:
		return "", fmt.Errorf("unsupported join type: %s", how)
	}
}

func runWithColumns(spec *withColumnsSpec) (*frame.DataFrame, error) {
	if spec == nil {
		return nil, fmt.Errorf("missing with_columns spec")
	}
	if spec.Rows <= 0 {
		spec.Rows = 1
	}
	cols := make([]series.Series, spec.Columns)
	for i := 0; i < spec.Columns; i++ {
		values := make([]int32, spec.Rows)
		cols[i] = series.NewInt32Series(fmt.Sprintf("col_%d", i), values)
	}
	df, err := frame.NewDataFrame(cols...)
	if err != nil {
		return nil, err
	}
	exprs := make(map[string]expr.Expr, spec.Columns)
	for i := 0; i < spec.Columns; i++ {
		exprs[fmt.Sprintf("feature_%d", i)] = expr.Lit(0)
	}
	return df.WithColumns(exprs)
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
