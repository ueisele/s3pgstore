package s3pgstore

// Adapted from
// https://github.com/ueisele/s3store/blob/738a8bcbce870887833e158d4dc4e5116a29d4fc/concurrency_test.go
// Copyright (c) 2024-2026 Uwe Eisele. MIT License.
//
// Adapted to s3pgstore: dropped the *metrics arg; tests assert
// the new fanOutObserver callback fires exactly once per
// non-empty call. Added panic-recovery tests since the
// recover-and-surface-as-error behavior is new in this fork.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestFanOut_EmptyInput(t *testing.T) {
	called := 0
	err := fanOut(context.Background(), []int{}, 4,
		func(_ context.Context, _, _ int) {
			called++
		},
		func(context.Context, int, int) error {
			t.Fatal("work invoked on empty input")
			return nil
		})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if called != 0 {
		t.Errorf("observer fired on empty input")
	}
}

func TestFanOut_SingleItemFastPath(t *testing.T) {
	obsItems, obsWorkers := -1, -1
	called := false
	err := fanOut(context.Background(), []int{42}, 4,
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
		t.Error("work not invoked on single-item input")
	}
	if obsItems != 1 || obsWorkers != 1 {
		t.Errorf("observer: got (%d, %d), want (1, 1)",
			obsItems, obsWorkers)
	}
}

func TestFanOut_AllItemsProcessed(t *testing.T) {
	const n = 500
	items := make([]int, n)
	var seen [n]atomic.Int32

	err := fanOut(context.Background(), items, 8, nil,
		func(_ context.Context, i int, _ int) error {
			seen[i].Add(1)
			return nil
		})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for i := range n {
		if v := seen[i].Load(); v != 1 {
			t.Errorf("item %d seen %d times, want 1", i, v)
		}
	}
}

func TestFanOut_FirstErrorWinsAndCancelsSiblings(t *testing.T) {
	sentinel := errors.New("sentinel")
	items := make([]int, 100)
	err := fanOut(context.Background(), items, 8, nil,
		func(ctx context.Context, i int, _ int) error {
			if i == 0 {
				return sentinel
			}
			<-ctx.Done()
			return ctx.Err()
		})
	if !errors.Is(err, sentinel) {
		t.Errorf("got %v, want sentinel", err)
	}
}

func TestFanOut_ParentCancelSurfaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	items := make([]int, 8)
	err := fanOut(ctx, items, 4, nil,
		func(ctx context.Context, _, _ int) error {
			<-ctx.Done()
			return ctx.Err()
		})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want context.Canceled", err)
	}
}

func TestFanOut_BoundedGoroutineCount(t *testing.T) {
	const items = 1000
	const concurrency = 4

	in := make([]int, items)
	var inFlight atomic.Int32
	var peak atomic.Int32
	var releaseOnce sync.Once
	release := make(chan struct{})

	checkAndRelease := func() {
		cur := inFlight.Add(1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		if cur >= int32(concurrency) {
			releaseOnce.Do(func() { close(release) })
		}
		<-release
		inFlight.Add(-1)
	}

	err := fanOut(context.Background(), in, concurrency, nil,
		func(_ context.Context, _, _ int) error {
			checkAndRelease()
			return nil
		})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if peak.Load() != int32(concurrency) {
		t.Errorf("peak workers = %d, want %d", peak.Load(), concurrency)
	}
}

func TestFanOut_ConcurrencyZeroOrNegativeClamps(t *testing.T) {
	for _, c := range []int{0, -1, -100} {
		err := fanOut(context.Background(), []int{1, 2, 3}, c, nil,
			func(context.Context, int, int) error {
				return nil
			})
		if err != nil {
			t.Errorf("concurrency=%d: %v", c, err)
		}
	}
}

func TestFanOut_ObserverFiresOncePerCall(t *testing.T) {
	calls := 0
	for range 5 {
		_ = fanOut(context.Background(), []int{1, 2, 3, 4}, 2,
			func(context.Context, int, int) { calls++ },
			func(context.Context, int, int) error { return nil })
	}
	if calls != 5 {
		t.Errorf("observer fired %d times across 5 calls, want 5", calls)
	}
}

func TestFanOut_PanicConvertsToError(t *testing.T) {
	err := fanOut(context.Background(), []int{1, 2, 3}, 2, nil,
		func(_ context.Context, i int, _ int) error {
			if i == 1 {
				panic("boom")
			}
			return nil
		})
	if err == nil {
		t.Fatal("want panic to surface as error, got nil")
	}
	if !errorContains(err, "boom") {
		t.Errorf("error text should mention panic value: %v", err)
	}
}

func TestFanOut_PanicCancelsSiblings(t *testing.T) {
	items := make([]int, 50)
	err := fanOut(context.Background(), items, 4, nil,
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

func TestFanOut_PanicOnSingleItemFastPath(t *testing.T) {
	err := fanOut(context.Background(), []int{1}, 4, nil,
		func(context.Context, int, int) error {
			panic("boom")
		})
	if err == nil {
		t.Fatal("single-item fast path: want error from panic, got nil")
	}
}

func TestFanOutMapReduce_OK(t *testing.T) {
	got, err := fanOutMapReduce(context.Background(),
		[]int{1, 2, 3, 4}, 2, nil,
		func(_ context.Context, item int) ([]int, error) {
			return []int{item * 10, item * 100}, nil
		},
		func(parts [][]int) int {
			sum := 0
			for _, p := range parts {
				for _, v := range p {
					sum += v
				}
			}
			return sum
		},
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := 0
	for _, n := range []int{1, 2, 3, 4} {
		want += n*10 + n*100
	}
	if got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func TestFanOutMapReduce_ErrorSkipsReduce(t *testing.T) {
	sentinel := errors.New("sentinel")
	reduced := false
	_, err := fanOutMapReduce(context.Background(),
		[]int{1, 2, 3}, 2, nil,
		func(_ context.Context, _ int) ([]int, error) {
			return nil, sentinel
		},
		func([][]int) int {
			reduced = true
			return 0
		})
	if !errors.Is(err, sentinel) {
		t.Errorf("got %v, want sentinel", err)
	}
	if reduced {
		t.Error("reduceFn called despite map error")
	}
}

func errorContains(err error, sub string) bool {
	if err == nil {
		return false
	}
	for i := 0; i+len(sub) <= len(err.Error()); i++ {
		if err.Error()[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
