package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestNew_RejectsZeroOrNegative locks in the New invariant —
// a pool with zero capacity would deadlock every submitter and
// is never the user's intent.
func TestNew_RejectsZeroOrNegative(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		if _, err := New(n, nil); err == nil {
			t.Errorf("New(%d) succeeded; want error", n)
		}
	}
}

// TestNew_NilMeterUsesNoop verifies the meter-nil branch is
// safe — every record/add must be a no-op without panic.
func TestNew_NilMeterUsesNoop(t *testing.T) {
	p, err := New(4, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	g, _ := p.WithContext(context.Background())
	g.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

// TestSubmit_AllRunAndWaitReturnsNil verifies the happy path
// — every submitted task runs, slot indices are stable, Wait
// returns nil.
func TestSubmit_AllRunAndWaitReturnsNil(t *testing.T) {
	p, err := New(8, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const N = 100
	g, gctx := p.WithContext(context.Background())
	out := make([]int, N)
	for i := range N {
		g.Submit(gctx, func(ctx context.Context) error {
			out[i] = i * 2
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	for i, v := range out {
		if v != i*2 {
			t.Errorf("out[%d] = %d, want %d", i, v, i*2)
		}
	}
}

// TestSubmit_FirstErrorWins verifies first-error-cancels
// semantics: one task errors, siblings observe ctx cancel via
// the group ctx, Wait returns the original error.
func TestSubmit_FirstErrorWins(t *testing.T) {
	p, err := New(8, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	g, gctx := p.WithContext(context.Background())
	wantErr := errors.New("boom")
	g.Submit(gctx, func(ctx context.Context) error {
		return wantErr
	})
	// Siblings that observe ctx cancel
	for range 10 {
		g.Submit(gctx, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
	}
	err = g.Wait()
	if !errors.Is(err, wantErr) {
		t.Errorf("Wait err = %v, want %v", err, wantErr)
	}
}

// TestSubmit_PanicConvertsToError verifies a panicking fn is
// recovered and surfaced as a normal error, not a process tear-
// down. Sibling tasks should see the cancel and return.
func TestSubmit_PanicConvertsToError(t *testing.T) {
	p, err := New(4, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	g, gctx := p.WithContext(context.Background())
	g.Submit(gctx, func(ctx context.Context) error {
		panic("boom")
	})
	err = g.Wait()
	if err == nil {
		t.Fatal("Wait returned nil; want a panic-recovered error")
	}
	if !contains(err.Error(), "panicked") {
		t.Errorf("Wait err = %v, want it to mention 'panicked'", err)
	}
}

// TestSubmit_ParentCancelSurfaces verifies parent ctx cancel
// is returned from Wait even if no fn errored, so a caller-
// triggered cancel doesn't silently look like success.
//
// Submit count == pool capacity so the test driver never
// blocks on Acquire; cancel fires deterministically after the
// loop returns.
func TestSubmit_ParentCancelSurfaces(t *testing.T) {
	const limit = 2
	p, err := New(limit, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := p.WithContext(ctx)
	for range limit {
		g.Submit(gctx, func(ctx context.Context) error {
			<-ctx.Done()
			return nil // not an error — caller cancellation
		})
	}
	cancel()
	err = g.Wait()
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Wait err = %v, want context.Canceled", err)
	}
}

// TestSubmit_BlocksOnSaturationThenSucceedsAfterRelease
// verifies the backpressure contract: when the pool is full,
// Submit blocks until a slot is released. Drives this from a
// goroutine so the test itself doesn't deadlock; checks that
// the blocked Submit measurably waited.
func TestSubmit_BlocksOnSaturationThenSucceedsAfterRelease(t *testing.T) {
	const limit = 2
	p, err := New(limit, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	g, gctx := p.WithContext(context.Background())

	// Fill the pool with two long-running tasks.
	release := make(chan struct{})
	for range limit {
		g.Submit(gctx, func(ctx context.Context) error {
			<-release
			return nil
		})
	}

	// Submit one more from a goroutine; it must block on Acquire
	// until we release the first two.
	submitted := make(chan time.Time, 1)
	go func() {
		g.Submit(gctx, func(ctx context.Context) error {
			submitted <- time.Now()
			return nil
		})
	}()

	// Give the goroutine time to block.
	time.Sleep(20 * time.Millisecond)
	select {
	case <-submitted:
		t.Fatal("third Submit ran before pool freed up")
	default:
	}

	releaseTime := time.Now()
	close(release) // unblock the first two; releases two slots
	got := <-submitted

	if got.Before(releaseTime) {
		t.Errorf("third Submit unblocked before release at %v "+
			"(saw %v)", releaseTime, got)
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

// TestSubmit_ConcurrencyBoundRespected proves the pool never
// runs more than MaxConcurrent tasks at once. Each task
// increments an atomic gauge on entry, decrements on exit, and
// records the max it observed; that max must equal
// MaxConcurrent.
func TestSubmit_ConcurrencyBoundRespected(t *testing.T) {
	const limit = 4
	p, err := New(limit, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var inFlight, peak atomic.Int64
	g, gctx := p.WithContext(context.Background())
	for range 100 {
		g.Submit(gctx, func(ctx context.Context) error {
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
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := peak.Load(); got > int64(limit) {
		t.Errorf("peak in-flight = %d, exceeds limit %d", got, limit)
	}
	if got := peak.Load(); got != int64(limit) {
		// Strict equality is reasonable: 100 tasks × 2ms each
		// across a limit of 4 should saturate.
		t.Errorf("peak in-flight = %d, expected to saturate at %d",
			got, limit)
	}
}

// TestSubmit_GroupsSharePool proves two Groups bound to the
// same Pool share its concurrency budget. Two batches of 50
// each, limit=4 ⇒ peak across both ≤ 4.
func TestSubmit_GroupsSharePool(t *testing.T) {
	const limit = 4
	p, err := New(limit, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var inFlight, peak atomic.Int64
	work := func(_ context.Context) error {
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
			g, gctx := p.WithContext(context.Background())
			for range 50 {
				g.Submit(gctx, work)
			}
			if err := g.Wait(); err != nil {
				t.Errorf("Wait: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := peak.Load(); got > int64(limit) {
		t.Errorf("peak across both groups = %d, exceeds shared limit %d",
			got, limit)
	}
}

// TestSubmit_DeadlockDetection_PanicsOnSamePoolReentrancy is
// the load-bearing safety check: a worker that submits to the
// same pool would wedge it by holding a slot while waiting for
// another. The detector must panic at submit time with a
// clear message.
func TestSubmit_DeadlockDetection_PanicsOnSamePoolReentrancy(t *testing.T) {
	p, err := New(4, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	g, gctx := p.WithContext(context.Background())
	defer func() { _ = g.Wait() }()

	g.Submit(gctx, func(ctx context.Context) error {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("nested same-pool Submit did not panic")
				return
			}
			msg := fmt.Sprint(r)
			if !contains(msg, "deadlock") {
				t.Errorf("panic msg = %q, want it to mention 'deadlock'",
					msg)
			}
		}()
		// This SHOULD panic — we're inside a worker, ctx carries
		// the marker.
		g.Submit(ctx, func(ctx context.Context) error {
			return nil
		})
		return nil
	})
}

// TestSubmit_DeadlockDetection_AllowsCrossPoolReentrancy
// verifies the detection is per-pool, not global. A worker on
// Pool A submitting to Pool B is safe (different worker
// fleets) and must NOT panic.
func TestSubmit_DeadlockDetection_AllowsCrossPoolReentrancy(t *testing.T) {
	pA, err := New(4, nil)
	if err != nil {
		t.Fatalf("New(A): %v", err)
	}
	pB, err := New(4, nil)
	if err != nil {
		t.Fatalf("New(B): %v", err)
	}
	gA, gAct := pA.WithContext(context.Background())

	var inner atomic.Bool
	gA.Submit(gAct, func(ctx context.Context) error {
		// Cross-pool nested submit: should work, no panic.
		gB, gBct := pB.WithContext(ctx)
		gB.Submit(gBct, func(ctx context.Context) error {
			inner.Store(true)
			return nil
		})
		return gB.Wait()
	})

	if err := gA.Wait(); err != nil {
		t.Fatalf("gA.Wait: %v", err)
	}
	if !inner.Load() {
		t.Error("inner cross-pool task did not run")
	}
}

// TestSubmit_DeadlockDetection_AllowsFreshGoroutine verifies
// the documented escape hatch: spawning a fresh goroutine that
// submits to the same pool is safe (the fresh goroutine isn't
// a worker, doesn't carry the marker).
func TestSubmit_DeadlockDetection_AllowsFreshGoroutine(t *testing.T) {
	p, err := New(4, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	g, gctx := p.WithContext(context.Background())

	var inner atomic.Bool
	g.Submit(gctx, func(ctx context.Context) error {
		// Same-pool nested via fresh goroutine: safe — the
		// goroutine isn't a worker. Pass the GROUP ctx (not
		// the worker's marked ctx) so detection doesn't fire.
		done := make(chan struct{})
		go func() {
			defer close(done)
			g.Submit(gctx, func(ctx context.Context) error {
				inner.Store(true)
				return nil
			})
		}()
		<-done
		return nil
	})

	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !inner.Load() {
		t.Error("fresh-goroutine nested task did not run")
	}
}

// TestSubmit_FifoOrderUnderSaturation verifies the documented
// FIFO behavior of the buffered-channel semaphore — submitters
// are unblocked in the order they queued (Go runtime drains a
// channel's sendq strictly FIFO on every receive). We saturate
// the pool, then queue a dozen Submits each tagged with their
// submission index, and check that completion order matches
// submission order.
//
// The slot send happens on the submit goroutine (single
// goroutine in this test driving all Submits), so sends are
// serialized and FIFO is observable.
func TestSubmit_FifoOrderUnderSaturation(t *testing.T) {
	p, err := New(1, nil) // serialised channel
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	g, gctx := p.WithContext(context.Background())

	const N = 16
	var mu sync.Mutex
	order := make([]int, 0, N)
	for i := range N {
		g.Submit(gctx, func(ctx context.Context) error {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	for i, got := range order {
		if got != i {
			t.Errorf("order[%d] = %d, want %d "+
				"(FIFO violated)", i, got, i)
		}
	}
}

// TestMaxConcurrent verifies the accessor returns the
// configured value.
func TestMaxConcurrent(t *testing.T) {
	for _, n := range []int{1, 4, 100, 1000} {
		p, err := New(n, nil)
		if err != nil {
			t.Fatalf("New(%d): %v", n, err)
		}
		if got := p.MaxConcurrent(); got != n {
			t.Errorf("MaxConcurrent = %d, want %d", got, n)
		}
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
