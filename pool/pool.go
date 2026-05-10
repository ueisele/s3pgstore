// Package pool provides a shared, bounded I/O worker pool for
// fan-out work where the bottleneck is I/O concurrency (S3
// requests, network calls) rather than CPU.
//
// Why a shared pool. Multiple Stores in one process can share
// a single *s3.Client (and therefore one connection pool, one
// adaptive retryer, one rate limiter). Without a shared
// concurrency budget, every Store's per-method fan-out spawns
// independently, so N concurrent callers can spawn N×K
// goroutines that all queue at the underlying connection pool
// while holding pre-built request bodies in memory. A single
// Pool sized to the global S3 op budget caps total in-flight
// memory across callers without forcing the operator to
// recompute per-method limits each time the deployment
// topology changes.
//
// Why goroutine-per-task. The pool blocks at submit time on a
// shared buffered-channel semaphore; once admitted, it spawns
// one short-lived goroutine per task and releases the slot
// when the task returns. At I/O latencies (~10-100 ms per S3
// op), goroutine spawn cost (~1 µs) is rounding error —
// measurably no worse than persistent workers and avoids the
// worker-lifecycle state machine. The submitter blocks ⇒
// natural backpressure; no unbounded queue, no body-memory
// blow-up.
//
// Pattern at the call site:
//
//	g, gctx := pool.WithContext(ctx)
//	for i, item := range items {
//	    i, item := i, item
//	    g.Submit(gctx, func(ctx context.Context) error {
//	        result, err := work(ctx, item)
//	        if err != nil { return err }
//	        out[i] = result
//	        return nil
//	    })
//	}
//	return g.Wait()
package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
)

// Pool is a shared concurrency budget for I/O-bound work.
// Submissions across all Groups bound to the pool compete for
// its MaxConcurrent slots in FIFO order (Go's runtime drains a
// channel's sendq strictly FIFO on every receive).
//
// The zero value is not usable; construct via New.
type Pool struct {
	slots   chan struct{}
	n       int
	metrics *metrics
}

// New constructs a Pool with the given concurrency limit and
// OTel meter. Pass nil meter to disable instrumentation
// (instruments are registered against the noop provider, so
// every record/add is a no-op).
//
// maxConcurrent must be > 0; the pool does not auto-clamp
// (zero would deadlock all submitters).
func New(maxConcurrent int, meter metric.Meter) (*Pool, error) {
	if maxConcurrent <= 0 {
		return nil, fmt.Errorf(
			"pool: maxConcurrent must be > 0, got %d", maxConcurrent)
	}
	m, err := newMetrics(meter)
	if err != nil {
		return nil, err
	}
	return &Pool{
		slots:   make(chan struct{}, maxConcurrent),
		n:       maxConcurrent,
		metrics: m,
	}, nil
}

// MaxConcurrent returns the configured concurrency limit.
func (p *Pool) MaxConcurrent() int { return p.n }

// Group is a per-batch coordinator. Submissions on a Group
// share the parent Pool's concurrency budget but coordinate
// completion and error propagation via this Group's Wait.
//
// On the first error from any Submit, the Group's ctx is
// cancelled (errgroup-style); siblings observe ctx.Done() and
// can bail. The first error is returned from Wait; subsequent
// errors are dropped. If no fn errored but the parent ctx was
// cancelled, Wait returns the parent's cancel error so a
// caller-triggered cancel never silently surfaces as success.
type Group struct {
	pool      *Pool
	eg        *errgroup.Group
	parentCtx context.Context
	ctx       context.Context
}

// WithContext returns a new Group bound to this Pool, plus a
// derived ctx that is cancelled on the first error from any
// Submit (errgroup-style). Pass the returned ctx to call sites
// that should bail on first sibling failure.
func (p *Pool) WithContext(
	ctx context.Context,
) (*Group, context.Context) {
	eg, gctx := errgroup.WithContext(ctx)
	return &Group{
		pool:      p,
		eg:        eg,
		parentCtx: ctx,
		ctx:       gctx,
	}, gctx
}

// markerKey tags the worker ctx so Submit can detect a
// same-pool reentrant submission and panic, rather than letting
// it manifest as a load-dependent hang under saturation.
// Cross-pool reentrancy carries a different key and is safe.
type markerKey struct{ pool *Pool }

// Submit blocks until a Pool slot is available, then spawns a
// goroutine to run fn. Multiple Submits across all Groups
// compete for the pool's slots in FIFO order.
//
// Behavior:
//
//   - The submitter blocks on Acquire while the pool is
//     saturated. This is the backpressure mechanism — bound
//     in-flight goroutines and pre-built request bodies to
//     MaxConcurrent.
//   - On ctx cancel during the wait, the submission is
//     silently skipped and Wait returns the cancel error.
//   - fn receives the Group's ctx (cancelled on first error
//     from any sibling).
//   - Panic safety: a panic in fn is recovered, logged at
//     WARN with the goroutine's stack, and surfaced as a
//     normal error to Wait.
//   - Deadlock detection: panics if ctx carries the marker
//     (fn is already running on this pool's worker). Same-pool
//     reentrancy can wedge the pool under saturation; the
//     panic surfaces the risk deterministically. Use a
//     different pool or a fresh goroutine for nested work.
//
// ctx is used only for the marker check; the slot wait and fn
// observe Group's ctx. Pass the worker's incoming ctx from
// inside fn so reentrancy panics fire; pass any ctx (typically
// the WithContext-returned gctx) from outside.
func (g *Group) Submit(
	ctx context.Context,
	fn func(ctx context.Context) error,
) {
	if ctx != nil && ctx.Value(markerKey{g.pool}) != nil {
		panic("s3pgstore/pool: nested submission to same pool from " +
			"inside a worker may deadlock under saturation — use a " +
			"different pool or spawn a fresh goroutine for nested work")
	}
	start := time.Now()
	select {
	case g.pool.slots <- struct{}{}:
	case <-g.ctx.Done():
		// Group ctx cancelled (parent cancel or sibling error).
		// Wait surfaces the cause.
		return
	}
	g.pool.metrics.recordWait(g.ctx, time.Since(start))
	g.pool.metrics.addInFlight(g.ctx, 1)
	g.eg.Go(func() (err error) {
		defer func() {
			if p := recover(); p != nil {
				slog.Warn("s3pgstore/pool: worker panic recovered",
					"panic", fmt.Sprint(p),
					"stack", string(debug.Stack()))
				err = fmt.Errorf("pool worker panicked: %v", p)
			}
			g.pool.metrics.addInFlight(g.ctx, -1)
			<-g.pool.slots
		}()
		// Inject reentrancy marker.
		markedCtx := context.WithValue(g.ctx, markerKey{g.pool}, true)
		return fn(markedCtx)
	})
}

// Wait blocks until all submitted tasks complete. Returns:
//
//   - The first non-nil error from any task (fn-returned or
//     panic-converted), filtering out context.Canceled siblings
//     so callers see the root cause.
//   - The parent ctx error if no fn errored but the parent ctx
//     was cancelled (so a caller-triggered cancel never
//     silently surfaces as nil).
//   - nil if all tasks succeeded and the parent ctx is healthy.
func (g *Group) Wait() error {
	err := g.eg.Wait()
	if err == nil || errors.Is(err, context.Canceled) {
		if perr := g.parentCtx.Err(); perr != nil {
			return perr
		}
	}
	return err
}
