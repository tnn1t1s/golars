package parallel

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type pooler interface {
	Submit(task func()) error
	Release()
}

var (
	poolOnce   sync.Once
	pool       pooler
	enabled    atomic.Bool
	maxThreads atomic.Int64
)

// Enabled reports whether parallel execution is available and enabled.
func Enabled() bool {
	initPool()
	return enabled.Load()
}

// MaxThreads returns the configured parallelism limit.
func MaxThreads() int {
	initPool()
	return int(maxThreads.Load())
}

// Join runs two functions in parallel when possible.
func Join(left, right func() error) error {
	if !Enabled() || MaxThreads() < 2 {
		if err := left(); err != nil {
			return err
		}
		return right()
	}

	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(2)

	run := func(fn func() error) {
		defer wg.Done()
		if err := fn(); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}

	if err := pool.Submit(func() { run(left) }); err != nil {
		run(left)
	}
	if err := pool.Submit(func() { run(right) }); err != nil {
		run(right)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// For executes fn over the range [0, n) using chunked parallelism when enabled.
func For(n int, fn func(start, end int) error) error {
	if n <= 0 {
		return nil
	}
	if !Enabled() || n < MaxThreads()*2 {
		return fn(0, n)
	}

	chunks := MaxThreads() * 4
	if chunks > n {
		chunks = n
	}
	chunkSize := (n + chunks - 1) / chunks

	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for start := 0; start < n; start += chunkSize {
		end := start + chunkSize
		if end > n {
			end = n
		}

		s, e := start, end
		wg.Add(1)
		run := func() {
			defer wg.Done()
			if err := fn(s, e); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}

		if err := pool.Submit(run); err != nil {
			run()
		}
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func initPool() {
	poolOnce.Do(func() {
		if isParallelDisabled() {
			enabled.Store(false)
			maxThreads.Store(1)
			return
		}

		n := maxThreadsFromEnv()
		if n < 1 {
			n = 1
		}
		maxThreads.Store(int64(n))

		if n <= 1 {
			enabled.Store(false)
			return
		}

		p, err := newPool(n)
		if err != nil {
			enabled.Store(false)
			return
		}
		pool = p
		enabled.Store(true)
	})
}

func maxThreadsFromEnv() int {
	if raw := strings.TrimSpace(os.Getenv("GOLARS_MAX_THREADS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return runtime.GOMAXPROCS(0)
}

func isParallelDisabled() bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv("GOLARS_NO_PARALLEL")))
	return raw == "1" || raw == "true" || raw == "yes"
}

// ResetForTests clears cached pool state so tests can reconfigure via env.
// Use only in tests.
func ResetForTests() {
	if pool != nil {
		pool.Release()
	}
	pool = nil
	poolOnce = sync.Once{}
	enabled.Store(false)
	maxThreads.Store(0)
}
