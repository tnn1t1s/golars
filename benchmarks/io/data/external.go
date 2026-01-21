package data

import (
	"os"
	"path/filepath"
	"strings"
)

// ExternalDirEnv defines the environment variable that points to shared benchmark files.
const ExternalDirEnv = "GOLARS_IO_BENCH_DATA_DIR"

// ExternalDir returns the configured external data directory, if any.
func ExternalDir() string {
	return strings.TrimSpace(os.Getenv(ExternalDirEnv))
}

// ExternalPath builds a path for an external benchmark file.
func ExternalPath(baseDir, category, name, ext string) string {
	filename := name + "." + ext
	return filepath.Join(baseDir, category, filename)
}

// EnsureDir makes sure the parent directory for a file exists.
func EnsureDir(path string) error {
	return os.MkdirAll(filepath.Dir(path), 0o755)
}

// FileExists reports whether the given path exists.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
