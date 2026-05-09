package s3pgstore

// fanOutPool tests. The helper is the single fan-out router for
// every per-Store I/O call site; these tests pin down its
// observable contract:
//   - every item processed exactly once
//   - slot-indexed writes preserve caller-chosen order
//   - first-error-wins with sibling cancel
//   - panic in work() converts to error (multi-item AND single-
//     item fast path)
//   - empty input is a no-op (no observer call, no work)
//   - single-item input goes through the inline fast path
//   - shared pool budget bounds in-flight tasks across multiple
//     concurrent fanOutPool calls (multi-Store property)

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

func TestFanOutPool_AllItemsProcessed(t *testing.T) {
	p := newPoolForTest(t, 4)
	const N = 50
	items := make([]int, N)
	for i := range items {
		items[i] = i
	}
	out := make([]int, N)
	err := fanOutPool(context.Background(), p, items, nil,
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
}

func TestFanOutPool_EmptyInputObserverSilent(t *testing.T) {
	p := newPoolForTest(t, 4)
	obsCalled := 0
	err := fanOutPool(context.Background(), p, []int{},
		func(_ context.Context, _ int) { obsCalled++ },
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
}

func TestFanOutPool_SingleItemFastPath(t *testing.T) {
	p := newPoolForTest(t, 4)
	var obsItems int
	called := false
	err := fanOutPool(context.Background(), p, []int{42},
		func(_ context.Context, items int) {
			obsItems = items
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
	if obsItems != 1 {
		t.Errorf("observer items = %d, want 1", obsItems)
	}
}

func TestFanOutPool_FirstErrorWins(t *testing.T) {
	p := newPoolForTest(t, 4)
	wantErr := errors.New("boom")
	items := make([]int, 30)
	for i := range items {
		items[i] = i
	}
	err := fanOutPool(context.Background(), p, items, nil,
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
}

func TestFanOutPool_PanicConvertsToError(t *testing.T) {
	p := newPoolForTest(t, 2)
	err := fanOutPool(context.Background(), p, []int{1, 2, 3, 4}, nil,
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
}

func TestFanOutPool_PanicOnSingleItemFastPath(t *testing.T) {
	p := newPoolForTest(t, 4)
	err := fanOutPool(context.Background(), p, []int{1}, nil,
		func(context.Context, int, int) error {
			panic("kaboom")
		})
	if err == nil {
		t.Fatal("single-item fast path: want error from panic, got nil")
	}
	if !strings.Contains(err.Error(), "kaboom") {
		t.Errorf("err = %v, want it to mention panic value", err)
	}
}

func TestFanOutPool_PanicCancelsSiblings(t *testing.T) {
	p := newPoolForTest(t, 4)
	items := make([]int, 50)
	err := fanOutPool(context.Background(), p, items, nil,
		func(ctx context.Context, i int, _ int) error {
			if i == 0 {
				panic("boom")
			}
			<-ctx.Done()
			return ctx.Err()
		})
	if err == nil {
		t.Fatal("want error from panic, got nil")
	}
}

func TestFanOutPool_ParentCancelSurfaces(t *testing.T) {
	// Pool size = items so all submissions admit before the
	// cancel arrives. Otherwise the submitter would block on
	// pool acquire and never reach Wait until cancel propagates.
	p := newPoolForTest(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 4)
	done := make(chan error, 1)
	go func() {
		done <- fanOutPool(ctx, p, []int{1, 2, 3, 4}, nil,
			func(ctx context.Context, _, _ int) error {
				started <- struct{}{}
				<-ctx.Done()
				return nil
			})
	}()
	for range 4 {
		<-started
	}
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestFanOutPool_ConcurrencyBoundRespected(t *testing.T) {
	const limit = 4
	p := newPoolForTest(t, limit)
	var inFlight, peak atomic.Int64
	err := fanOutPool(context.Background(), p,
		make([]int, 50), nil,
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
		t.Errorf("peak = %d, exceeds MaxConcurrent %d", got, limit)
	}
}

// TestFanOutPool_SharedAcrossCalls is the load-bearing
// multi-Store property: two parallel fanOutPool invocations
// against the same pool must respect ONE budget across both
// (not 2× MaxConcurrent).
func TestFanOutPool_SharedAcrossCalls(t *testing.T) {
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
			err := fanOutPool(context.Background(), p, items, nil, work)
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

// TestFanOutPool_ObserverFiresOnce verifies the observer
// callback fires exactly one observation per non-empty call.
func TestFanOutPool_ObserverFiresOnce(t *testing.T) {
	p := newPoolForTest(t, 4)
	var calls atomic.Int64
	err := fanOutPool(context.Background(), p,
		[]int{1, 2, 3, 4, 5, 6},
		func(_ context.Context, items int) {
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
}
