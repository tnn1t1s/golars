package parallel

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestForAndJoin(t *testing.T) {
	t.Setenv("GOLARS_NO_PARALLEL", "")
	t.Setenv("GOLARS_MAX_THREADS", "2")
	ResetForTests()

	var sum int64
	err := For(1000, func(start, end int) error {
		for i := start; i < end; i++ {
			atomic.AddInt64(&sum, int64(i))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum != 499500 {
		t.Fatalf("unexpected sum: %d", sum)
	}

	wantErr := errors.New("boom")
	err = Join(func() error { return wantErr }, func() error { return nil })
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error propagation, got %v", err)
	}
}
