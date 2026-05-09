package s3pgstore

// fanOutPool is the single fan-out helper for every per-Store
// I/O call site. It submits one task per item to the Store's
// shared *pool.Pool — slot-indexed result writes, first-error
// wins, sibling cancel via the pool's errgroup ctx, and panic
// recovery via pool.Group.Submit.
//
// Behavior contract (matched by every caller):
//
//   - Empty input returns nil without invoking obs.
//   - Single-item input fires obs(ctx, 1) and runs work inline
//     with panic recovery — same scheduler-overhead-free shape
//     the per-method fan-out used to take.
//   - Multi-item input fires obs(ctx, len(items)) once, then
//     submits each item's work as its own pool task. The pool's
//     MaxConcurrent slot budget bounds in-flight tasks across
//     every Group sharing the pool.
//
// work captures `i` in its closure to write into a caller-
// allocated result slice — workers never share write
// destinations, so no coordination is required for slot writes.

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/ueisele/s3pgstore/pool"
)

// fanOutObserver is the optional metric/log hook fired exactly
// once per fanOutPool call. nil is permitted.
type fanOutObserver func(ctx context.Context, items int)

// fanOutPool runs work across items via the supplied pool.
//
// Single-item fast path skips Submit and runs work inline with
// panic recovery — keeps sugar-wrapper callers free of pool
// admission overhead.
//
// On a hard error from any work invocation, the pool's
// errgroup ctx is cancelled, sibling tasks observe ctx.Done()
// and bail; the first error is returned from Group.Wait. If no
// work errored but the parent ctx is done, Wait returns the
// parent's cancel error so a caller-triggered cancel never
// surfaces silently as success.
func fanOutPool[I any](
	ctx context.Context,
	p *pool.Pool,
	items []I,
	obs fanOutObserver,
	work func(ctx context.Context, i int, item I) error,
) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) == 1 {
		if obs != nil {
			obs(ctx, 1)
		}
		return runOneRecover(ctx, 0, items[0], work)
	}
	if obs != nil {
		obs(ctx, len(items))
	}
	g, gctx := p.WithContext(ctx)
	for i, item := range items {
		g.Submit(gctx, func(ctx context.Context) error {
			return work(ctx, i, item)
		})
	}
	return g.Wait()
}

// runOneRecover invokes work(ctx, i, item) inside a recover
// guard. A panic is logged with stack and converted to an
// error so the single-item fast path matches the pool path's
// panic-to-error conversion (pool.Group.Submit recovers panics
// internally for the multi-item path).
func runOneRecover[I any](
	ctx context.Context,
	i int, item I,
	work func(ctx context.Context, i int, item I) error,
) (err error) {
	defer func() {
		if p := recover(); p != nil {
			stack := debug.Stack()
			slog.Warn("s3pgstore: fanOutPool worker panic recovered",
				"index", i,
				"panic", fmt.Sprint(p),
				"stack", string(stack))
			err = fmt.Errorf(
				"fanOutPool worker [%d] panicked: %v", i, p)
		}
	}()
	return work(ctx, i, item)
}
