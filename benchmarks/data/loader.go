package data

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tnn1t1s/golars"
	"github.com/tnn1t1s/golars/frame"
)

// BenchDataDirEnv overrides the default benchmark data directory.
const BenchDataDirEnv = "GOLARS_BENCH_DATA_DIR"

// BenchDataDir returns the directory containing benchmark datasets.
func BenchDataDir() string {
	if dir := strings.TrimSpace(os.Getenv(BenchDataDirEnv)); dir != "" {
		return dir
	}
	return filepath.Clean(filepath.Join("..", "data"))
}

// H2OAIPath returns the parquet path for the given size.
func H2OAIPath(size string) string {
	return filepath.Join(BenchDataDir(), fmt.Sprintf("h2oai_%s.parquet", size))
}

// LoadH2OAI loads the H2O.ai dataset from disk.
func LoadH2OAI(size string) (*frame.DataFrame, error) {
	path := H2OAIPath(size)
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("benchmark data missing: %s (run: just generate-%s-data)", path, size)
	}
	return golars.ReadParquet(path)
}
