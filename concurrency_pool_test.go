package s3pgstore

// concurrency_pool_test.go covers the fanOutOrPool router
// helper that picks between the per-method fanOut backend
// (today's behavior, p == nil) and the shared
// pool.Pool-backed Group (multi-Store-shared-budget
// deployments, p != nil).
//
// Both backends must deliver the same observable contract:
//   - every item processed exactly once
//   - slot-indexed writes preserve caller-chosen order
//   - first-error-wins with sibling cancel
//   - panic in work() converts to error
//   - empty input is a no-op (no observer call, no work)
//   - single-item input goes through the inline fast path
//
// These tests parameterise over both backends so any future
// drift surfaces here rather than in production.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ueisele/s3pgstore/pool"
)

// newPoolForTest constructs a pool with the given concurrency
// for these tests. nil meter → noop instruments.
func newPoolForTest(t *testing.T, n int) *pool.Pool {
	t.Helper()
	p, err := pool.New(n, nil)
	if err != nil {
		t.Fatalf("pool.New(%d): %v", n, err)
	}
	return p
}

// runBothBackends runs fn against both backends so the same
// assertions cover the fanOut and pool paths.
func runBothBackends(t *testing.T, name string,
	fn func(t *testing.T, p *pool.Pool)) {
	t.Helper()
	t.Run(name+"/fanOut", func(t *testing.T) { fn(t, nil) })
	t.Run(name+"/pool", func(t *testing.T) {
		fn(t, newPoolForTest(t, 4))
	})
}

func TestFanOutOrPool_AllItemsProcessed(t *testing.T) {
	runBothBackends(t, "all-items", func(t *testing.T, p *pool.Pool) {
		const N = 50
		items := make([]int, N)
		for i := range items {
			items[i] = i
		}
		out := make([]int, N)
		err := fanOutOrPool(context.Background(), p, items, 8, nil,
			func(_ context.Context, i, item int) error {
				out[i] = item * 3
				return nil
			})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		for i, v := range out {
			if v != i*3 {
				t.Errorf("out[%d] = %d, want %d", i, v, i*3)
			}
		}
	})
}

func TestFanOutOrPool_EmptyInputObserverSilent(t *testing.T) {
	runBothBackends(t, "empty", func(t *testing.T, p *pool.Pool) {
		obsCalled := 0
		err := fanOutOrPool(context.Background(), p, []int{}, 4,
			func(_ context.Context, _, _ int) { obsCalled++ },
			func(context.Context, int, int) error {
				t.Fatal("work invoked on empty input")
				return nil
			})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if obsCalled != 0 {
			t.Errorf("observer fired on empty input")
		}
	})
}

func TestFanOutOrPool_SingleItemFastPath(t *testing.T) {
	runBothBackends(t, "single", func(t *testing.T, p *pool.Pool) {
		var obsItems, obsWorkers int
		called := false
		err := fanOutOrPool(context.Background(), p, []int{42}, 8,
			func(_ context.Context, items, workers int) {
				obsItems, obsWorkers = items, workers
			},
			func(_ context.Context, i, item int) error {
				called = true
				if i != 0 || item != 42 {
					t.Errorf("got (%d, %d), want (0, 42)", i, item)
				}
				return nil
			})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !called {
			t.Errorf("work was not called on single item")
		}
		if obsItems != 1 || obsWorkers != 1 {
			t.Errorf("observer = (%d,%d), want (1,1)",
				obsItems, obsWorkers)
		}
	})
}

func TestFanOutOrPool_FirstErrorWins(t *testing.T) {
	runBothBackends(t, "first-error", func(t *testing.T, p *pool.Pool) {
		wantErr := errors.New("boom")
		items := make([]int, 30)
		for i := range items {
			items[i] = i
		}
		err := fanOutOrPool(context.Background(), p, items, 4, nil,
			func(ctx context.Context, _, item int) error {
				if item == 5 {
					return wantErr
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(50 * time.Millisecond):
					return nil
				}
			})
		if !errors.Is(err, wantErr) {
			t.Errorf("err = %v, want %v", err, wantErr)
		}
	})
}

func TestFanOutOrPool_PanicConvertsToError(t *testing.T) {
	runBothBackends(t, "panic", func(t *testing.T, p *pool.Pool) {
		err := fanOutOrPool(context.Background(), p, []int{1, 2, 3, 4}, 2, nil,
			func(_ context.Context, _, item int) error {
				if item == 2 {
					panic("kaboom")
				}
				return nil
			})
		if err == nil {
			t.Fatal("expected panic-converted error, got nil")
		}
		if !strings.Contains(err.Error(), "panic") {
			t.Errorf("err = %v, want it to mention 'panic'", err)
		}
	})
}

func TestFanOutOrPool_ParentCancelSurfaces(t *testing.T) {
	runBothBackends(t, "parent-cancel", func(t *testing.T, p *pool.Pool) {
		// Pool path requires concurrency ≤ pool size to avoid
		// the test driver blocking on Acquire — the test pool
		// has 4 slots, so 4 items.
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{}, 4)
		done := make(chan error, 1)
		go func() {
			done <- fanOutOrPool(ctx, p, []int{1, 2, 3, 4}, 4, nil,
				func(ctx context.Context, _, _ int) error {
					started <- struct{}{}
					<-ctx.Done()
					return nil
				})
		}()
		// Wait for all four to start.
		for range 4 {
			<-started
		}
		cancel()
		err := <-done
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	})
}

func TestFanOutOrPool_ConcurrencyBoundRespected(t *testing.T) {
	// fanOut path: concurrency arg bounds workers locally.
	// pool path: pool MaxConcurrent bounds globally.
	t.Run("fanOut", func(t *testing.T) {
		var inFlight, peak atomic.Int64
		const limit = 4
		err := fanOutOrPool(context.Background(), nil,
			make([]int, 50), limit, nil,
			func(_ context.Context, _, _ int) error {
				n := inFlight.Add(1)
				defer inFlight.Add(-1)
				for {
					cur := peak.Load()
					if n <= cur || peak.CompareAndSwap(cur, n) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				return nil
			})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if got := peak.Load(); got > int64(limit) {
			t.Errorf("peak = %d, exceeds limit %d", got, limit)
		}
	})

	t.Run("pool", func(t *testing.T) {
		const limit = 4
		p := newPoolForTest(t, limit)
		var inFlight, peak atomic.Int64
		// concurrency arg ignored when p != nil; bound is
		// MaxConcurrent.
		err := fanOutOrPool(context.Background(), p,
			make([]int, 50), 99, nil,
			func(_ context.Context, _, _ int) error {
				n := inFlight.Add(1)
				defer inFlight.Add(-1)
				for {
					cur := peak.Load()
					if n <= cur || peak.CompareAndSwap(cur, n) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				return nil
			})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if got := peak.Load(); got > int64(limit) {
			t.Errorf("pool peak = %d, exceeds MaxConcurrent %d",
				got, limit)
		}
	})
}

// TestFanOutOrPool_PoolSharedAcrossCalls is the load-bearing
// multi-Store property: two parallel fanOutOrPool invocations
// against the same pool must respect ONE budget across both
// (not 2× MaxConcurrent).
func TestFanOutOrPool_PoolSharedAcrossCalls(t *testing.T) {
	const limit = 4
	p := newPoolForTest(t, limit)
	var inFlight, peak atomic.Int64
	work := func(_ context.Context, _, _ int) error {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			cur := peak.Load()
			if n <= cur || peak.CompareAndSwap(cur, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			items := make([]int, 30)
			err := fanOutOrPool(context.Background(), p, items, 99, nil, work)
			if err != nil {
				t.Errorf("err: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := peak.Load(); got > int64(limit) {
		t.Errorf("peak across both calls = %d, exceeds shared %d",
			got, limit)
	}
}

// TestFanOutOrPool_ObserverFiresOnce verifies the observer
// callback semantics match across both backends — exactly one
// observation per non-empty call.
func TestFanOutOrPool_ObserverFiresOnce(t *testing.T) {
	runBothBackends(t, "observer", func(t *testing.T, p *pool.Pool) {
		var calls atomic.Int64
		err := fanOutOrPool(context.Background(), p,
			[]int{1, 2, 3, 4, 5, 6}, 4,
			func(_ context.Context, items, _ int) {
				calls.Add(1)
				if items != 6 {
					t.Errorf("items = %d, want 6", items)
				}
			},
			func(context.Context, int, int) error { return nil })
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if got := calls.Load(); got != 1 {
			t.Errorf("observer fired %d times, want 1", got)
		}
	})
}
