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
//
// Submitter passes ctx (the marked group ctx if we're already
// inside a worker) so the deadlock detector can fire if this
// is a same-pool reentrant submit. fn receives a fresh
// marked ctx so any nested Submit it makes will likewise
// detect.
package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"golang.org/x/sync/errgroup"
)

// Pool is a shared concurrency budget for I/O-bound work.
// Submissions across all Groups bound to the pool compete for
// its MaxConcurrent slots in FIFO order (Go's runtime drains a
// channel's sendq strictly FIFO on every receive).
//
// The zero value is not usable; construct via New.
type Pool struct {
	slots chan struct{}
	n     int

	// Metrics. Always non-nil after New (noop fallback when no
	// meter is provided), so call sites don't branch on nil.
	inFlight metric.Int64UpDownCounter
	waitDur  metric.Float64Histogram
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
	if meter == nil {
		meter = noop.NewMeterProvider().Meter(
			"github.com/ueisele/s3pgstore/pool")
	}
	inFlight, err := meter.Int64UpDownCounter(
		"s3pgstore.pool.in_flight",
		metric.WithDescription(
			"Concurrent I/O tasks executing on the shared pool. "+
				"Approaches MaxConcurrent under saturation."),
		metric.WithUnit("{task}"))
	if err != nil {
		return nil, fmt.Errorf("pool: register in_flight: %w", err)
	}
	// shortWaitBuckets mirrors the iter pipeline's body-slot
	// wait shape — sub-millisecond when not saturated, seconds
	// only under heavy contention.
	waitDur, err := meter.Float64Histogram(
		"s3pgstore.pool.queue.wait.duration",
		metric.WithDescription(
			"Time submitters spent waiting for a pool slot to free up "+
				"before their task started. Sustained non-zero p95 "+
				"indicates the pool is saturated."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5))
	if err != nil {
		return nil, fmt.Errorf("pool: register queue.wait.duration: %w", err)
	}
	return &Pool{
		slots:    make(chan struct{}, maxConcurrent),
		n:        maxConcurrent,
		inFlight: inFlight,
		waitDur:  waitDur,
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

// markerKey is per-pool — distinguishes "you're inside this
// pool's worker" from "you're inside some other pool's
// worker." Cross-pool reentrancy is safe (different worker
// fleets); same-pool reentrancy would deadlock the pool's
// budget by holding a slot while waiting for another.
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
//     indicating fn is already running on this pool's
//     worker. Same-pool reentrancy would wedge the pool —
//     the worker would hold a slot while waiting for another
//     slot it would never get. Use a different pool or spawn
//     a fresh goroutine for nested work.
//
// The ctx parameter is used only for the deadlock-detection
// marker check at submit time. The slot Acquire and fn both
// observe the Group's ctx (cancelled on first error from any
// sibling), so first-error semantics propagate uniformly
// regardless of which ctx the caller passes here. Callers
// inside a worker's fn should pass that fn's incoming ctx
// (which carries the marker) so reentrancy panics fire; new
// submissions from an unrelated goroutine can pass any ctx
// (typically the group ctx returned from WithContext).
func (g *Group) Submit(
	ctx context.Context,
	fn func(ctx context.Context) error,
) {
	if ctx != nil && ctx.Value(markerKey{g.pool}) != nil {
		panic("s3pgstore/pool: nested submission to same pool from " +
			"inside a worker would deadlock — use a different pool " +
			"or spawn a fresh goroutine for nested work")
	}
	start := time.Now()
	select {
	case g.pool.slots <- struct{}{}:
	case <-g.ctx.Done():
		// Group ctx cancelled (parent cancel or sibling error).
		// Wait surfaces the cause.
		return
	}
	g.pool.waitDur.Record(g.ctx, time.Since(start).Seconds())
	g.pool.inFlight.Add(g.ctx, 1)
	g.eg.Go(func() (err error) {
		defer func() {
			g.pool.inFlight.Add(g.ctx, -1)
			<-g.pool.slots
		}()
		defer func() {
			if p := recover(); p != nil {
				stack := debug.Stack()
				slog.Warn("s3pgstore/pool: worker panic recovered",
					"panic", fmt.Sprint(p),
					"stack", string(stack))
				err = fmt.Errorf("pool worker panicked: %v", p)
			}
		}()
		// Inject the per-pool marker so a nested Submit on the
		// same pool from inside fn panics with a clear message
		// rather than wedging the pool.
		markedCtx := context.WithValue(
			g.ctx, markerKey{g.pool}, true)
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
	if err == nil {
		return g.parentCtx.Err()
	}
	// Filter out context.Canceled — that's the cancel signal
	// propagating from a sibling's real error, not the root
	// cause. The first real error is what errgroup returns;
	// the .Canceled filter handles the rare case where
	// cancellation arrived before the real error.
	if errors.Is(err, context.Canceled) {
		if perr := g.parentCtx.Err(); perr != nil {
			return perr
		}
	}
	return err
}
