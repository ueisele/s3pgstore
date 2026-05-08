package s3pgstore

// metrics.go is the OTel instrumentation surface for the
// Store. Pattern lifted from s3store's metrics.go (the public-
// method scope, the s3 op classifier with API-code precedence,
// the curated error-type enumeration, the per-instrument bucket
// boundaries) — different metric names, same shape so operators
// running both libraries see consistent attribute keys.
//
// Adding a new instrument here requires adding the matching
// Grafana panel in dashboards/s3pgstore.json — see CLAUDE.md
// § Metrics ↔ dashboard sync. Drift is silent: an emitted
// metric without a panel is operationally invisible.
//
// Telemetry is opt-in: a nil Meter on Config resolves to a
// no-op meter and every record/add call becomes a no-op.

import (
	"context"
	"errors"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Outcome / error.type label values. Kept as constants so a
// dashboard regex never has to chase a typo.
const (
	outcomeSuccess  = "success"
	outcomeError    = "error"
	outcomeCanceled = "canceled"

	errTypeCanceled  = "canceled"
	errTypeNotFound  = "not_found"
	errTypeSlowDown  = "slowdown"
	errTypeServer    = "server"
	errTypeClient    = "client"
	errTypeTransport = "transport"
	errTypeOther     = "other"
)

// Attribute keys. Centralised so the dashboard's PromQL refers
// to label names that exactly match what the library emits.
const (
	attrKeyMethod    = "method"
	attrKeyOutcome   = "outcome"
	attrKeyOperation = "s3pgstore.operation"
	attrKeyAttempts  = "s3pgstore.attempts"
	attrKeyAttempt   = "s3pgstore.attempt"
	attrKeyErrorType = "error.type"
	attrKeyResult    = "result"
)

// MetricsConfig bundles the construction-time inputs for a
// Metrics handle. PollLagFn and PendingWritesDepthFn are
// observable-gauge data sources: each fires once per OTel
// collection cycle. nil disables the corresponding gauge —
// useful for tests or callers that don't want the per-collection
// SQL round trip.
type metricsConfig struct {
	Meter metric.Meter

	// PollLagFn returns the current "now() - latest feed_seq_at"
	// duration. ok=false suppresses the observation (e.g., no
	// sequenced rows yet — emitting 0 would lie). An error logs
	// at WARN inside the callback and suppresses the value.
	PollLagFn func(ctx context.Context) (lag time.Duration, ok bool, err error)

	// PendingWritesDepthFn returns the count of orphan-tracking
	// rows currently in s3pgstore_pending_writes. Sustained
	// growth = orphan creation outpacing GC reclamation, which
	// is incident-class.
	PendingWritesDepthFn func(ctx context.Context) (count int64, err error)
}

// Metrics is the per-Store instrumentation handle. All methods
// are safe for concurrent use; a nil receiver makes every
// scope/record call a no-op.
type Metrics struct {
	// Public-method instrumentation (every Store[T] entry point
	// and MaterializedView.Lookup get one observation each).
	methodDuration metric.Float64Histogram
	methodCalls    metric.Int64Counter
	methodInFlight metric.Int64UpDownCounter

	// Write volumes.
	writeBytes            metric.Int64Histogram
	writeRecords          metric.Int64Histogram
	writeEncodeBufDropped metric.Int64Counter
	writeTokenRaceRetry   metric.Int64Counter

	// S3 ops.
	s3RequestDuration metric.Float64Histogram
	s3RequestCount    metric.Int64Counter
	s3TransientCount  metric.Int64Counter
	s3BodySize        metric.Int64Histogram

	// Target saturation (s3target's MaxInflightRequests
	// semaphore).
	targetSemWaitDuration metric.Float64Histogram
	targetSemInflight     metric.Int64UpDownCounter
	targetSemWaiting      metric.Int64UpDownCounter

	// Fan-out shape (Write multi-partition, PollRecords).
	fanoutPartitions metric.Int64Histogram
	fanoutItems      metric.Int64Histogram

	// Catalog / locking.
	occVersionConflict metric.Int64Counter
	lookupByToken      metric.Int64Counter
}

// newMetrics registers every instrument in cfg.Meter and wires
// the observable-gauge callbacks. A nil meter resolves to the
// OTel no-op meter; every record path then short-circuits at
// near-zero cost. Errors propagate from instrument registration
// — the only realistic failure is provider-side configuration
// rejecting an instrument, which surfaces at New() so the
// operator sees it before the first write.
func newMetrics(cfg metricsConfig) (*Metrics, error) {
	meter := cfg.Meter
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("s3pgstore")
	}

	// Bucket boundaries — explicit so histogram_quantile resolves
	// percentiles within the realistic operating range. Defaults
	// (start at ~0, exponential up) collapse our observations
	// into the first bucket and report bucket geometry instead of
	// real percentiles.
	//
	// durationBuckets covers ~5 ms (typical PG single-statement)
	// through 30 s (a long fan-out Write under contention).
	durationBuckets := []float64{
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
	}
	// shortWaitBuckets is for in-process waits (sem acquire).
	// Most observations are sub-millisecond when not saturated.
	shortWaitBuckets := []float64{
		0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5,
	}
	// byteBuckets covers parquet body sizes from a few KB through
	// 1 GB outliers. 4× jumps with extra 2× resolution at 8/32/64
	// MiB so the bytes/Write P100 panel resolves cap-tuning
	// recommendations to "bump EncodeBufPoolMaxBytes to ~10 MiB
	// vs ~16 MiB" and "~80 MiB vs ~150 MiB" rather than
	// collapsing both to a single bucket. See CLAUDE.md
	// § Benchmarks.
	byteBuckets := []float64{
		1024, 4096, 16384, 65536, 262144, 1048576,
		4194304, 8388608, 16777216, 33554432,
		67108864, 134217728, 1073741824,
	}
	// recordCountBuckets covers per-call record counts from
	// single-row writes through million-row batches.
	recordCountBuckets := []float64{
		1, 10, 25, 50, 100, 250, 500, 1000,
		2500, 10000, 50000, 250000, 1000000,
	}
	// fanoutItemBuckets covers per-fan-out item counts: typical
	// Write touches 1-50 partitions; PollRecords typically fans
	// out to the page size (1-1000 entries).
	fanoutItemBuckets := []float64{
		1, 2, 5, 10, 25, 50, 100, 250, 1000, 5000,
	}
	// fanoutPartitionBuckets covers per-Write partition fan-out
	// width (almost always 1-50; tail to 1000+ for bulk imports).
	fanoutPartitionBuckets := []float64{
		1, 2, 4, 8, 16, 32, 64, 128, 512, 4096,
	}

	mustHist := func(
		name, desc, unit string,
		buckets ...float64,
	) (metric.Float64Histogram, error) {
		return meter.Float64Histogram(name,
			metric.WithDescription(desc),
			metric.WithUnit(unit),
			metric.WithExplicitBucketBoundaries(buckets...))
	}
	mustHistInt := func(
		name, desc, unit string,
		buckets ...float64,
	) (metric.Int64Histogram, error) {
		return meter.Int64Histogram(name,
			metric.WithDescription(desc),
			metric.WithUnit(unit),
			metric.WithExplicitBucketBoundaries(buckets...))
	}
	mustCounter := func(name, desc, unit string) (metric.Int64Counter, error) {
		return meter.Int64Counter(name,
			metric.WithDescription(desc),
			metric.WithUnit(unit))
	}
	mustUpDown := func(name, desc, unit string) (metric.Int64UpDownCounter, error) {
		return meter.Int64UpDownCounter(name,
			metric.WithDescription(desc),
			metric.WithUnit(unit))
	}

	m := &Metrics{}
	var err error

	if m.methodDuration, err = mustHist(
		"s3pgstore.method.duration",
		"Wall-clock duration of public Store[T] method calls.",
		"s", durationBuckets...); err != nil {
		return nil, err
	}
	if m.methodCalls, err = mustCounter(
		"s3pgstore.method.calls",
		"Total public Store[T] method calls, labeled by method "+
			"and outcome (success / error / canceled).",
		"{call}"); err != nil {
		return nil, err
	}
	if m.methodInFlight, err = mustUpDown(
		"s3pgstore.method.in_flight",
		"Public Store[T] method calls currently in flight, "+
			"labeled by method. Real-time gauge — does not "+
			"depend on rate-window estimation.",
		"{call}"); err != nil {
		return nil, err
	}

	if m.writeBytes, err = mustHistInt(
		"s3pgstore.write.bytes",
		"Compressed parquet bytes per Write/WriteWithKey call. "+
			"P100 drives EncodeBufPoolMaxBytes tuning — see "+
			"CLAUDE.md § Benchmarks.",
		"By", byteBuckets...); err != nil {
		return nil, err
	}
	if m.writeRecords, err = mustHistInt(
		"s3pgstore.write.records",
		"Records per Write/WriteWithKey call (total across all "+
			"partitions for the multi-partition Write).",
		"{record}", recordCountBuckets...); err != nil {
		return nil, err
	}
	if m.writeEncodeBufDropped, err = mustCounter(
		"s3pgstore.write.encode_buf_dropped",
		"Encode buffers dropped after exceeding "+
			"EncodeBufPoolMaxBytes. Non-zero rate indicates the "+
			"cap is undersized for the workload — see "+
			"CLAUDE.md § Benchmarks.",
		"{event}"); err != nil {
		return nil, err
	}
	if m.writeTokenRaceRetry, err = mustCounter(
		"s3pgstore.write.token_race.retry.count",
		"Token-race re-lookups: a concurrent writer hit the "+
			"(partition_key, idempotency_token) partial UNIQUE "+
			"first, our INSERT bounced, and we re-fetched their "+
			"canonical row. High rate = workload bug (shared "+
			"token across producers) or bad token-derivation.",
		"{event}"); err != nil {
		return nil, err
	}

	if m.s3RequestDuration, err = mustHist(
		"s3pgstore.s3.request.duration",
		"Wall-clock duration of one outer S3 wrapper call "+
			"(acquire + retry + release), labeled by op.",
		"s", durationBuckets...); err != nil {
		return nil, err
	}
	if m.s3RequestCount, err = mustCounter(
		"s3pgstore.s3.request.count",
		"Total S3 wrapper calls, labeled by operation, outcome, "+
			"error.type (terminal-error only), and attempts (the "+
			"outer retry() iteration count, 0..retryMaxAttempts; "+
			"0 means the call exited before the retry loop ran, "+
			"e.g. semaphore acquire failed).",
		"{request}"); err != nil {
		return nil, err
	}
	if m.s3TransientCount, err = mustCounter(
		"s3pgstore.s3.transient_error.count",
		"Total transient S3 errors observed inside retry() — "+
			"HTTP 5xx, HTTP 429 SlowDown (and 503 with API code "+
			"SlowDown), and transport-layer failures (DNS / TCP "+
			"/ TLS / connection reset). Incremented on every "+
			"transient failure regardless of whether a retry "+
			"follows or it's the final attempt that exhausted "+
			"the budget. Closes the visibility gap on "+
			"s3.request.count, which only carries error.type for "+
			"terminal errors. Labels: operation, error.type, "+
			"attempt (1..retryMaxAttempts, the index of the "+
			"failed attempt).",
		"{event}"); err != nil {
		return nil, err
	}
	if m.s3BodySize, err = mustHistInt(
		"s3pgstore.s3.body_size",
		"Wire-bytes per S3 PUT/GET — parquet body size as the "+
			"SDK saw it. PUT samples (compressed) sit alongside "+
			"the uncompressed write.bytes; GET samples reflect "+
			"per-file fetch size for capacity planning.",
		"By", byteBuckets...); err != nil {
		return nil, err
	}

	if m.targetSemWaitDuration, err = mustHist(
		"s3pgstore.target.sem_wait_duration",
		"Time spent waiting for an MaxInflightRequests semaphore "+
			"slot before issuing an S3 request. Sustained "+
			"non-zero P95 indicates the cap is undersized.",
		"s", shortWaitBuckets...); err != nil {
		return nil, err
	}
	if m.targetSemInflight, err = mustUpDown(
		"s3pgstore.target.sem_inflight",
		"In-flight S3 requests against the target's "+
			"MaxInflightRequests semaphore.",
		"{request}"); err != nil {
		return nil, err
	}
	if m.targetSemWaiting, err = mustUpDown(
		"s3pgstore.target.sem_waiting",
		"Goroutines currently blocked on the target's "+
			"MaxInflightRequests semaphore.",
		"{goroutine}"); err != nil {
		return nil, err
	}

	if m.fanoutPartitions, err = mustHistInt(
		"s3pgstore.fanout.partitions",
		"Partition count per fan-out call, labeled by method "+
			"(currently Write).",
		"{partition}", fanoutPartitionBuckets...); err != nil {
		return nil, err
	}
	if m.fanoutItems, err = mustHistInt(
		"s3pgstore.fanout.items",
		"Items dispatched per fan-out call, labeled by method "+
			"(Write — partitions; PollRecords — entries).",
		"{item}", fanoutItemBuckets...); err != nil {
		return nil, err
	}

	if m.occVersionConflict, err = mustCounter(
		"s3pgstore.occ.version_conflict.count",
		"WithExpectedVersion writes that failed because the "+
			"observed partition version did not match. High rate "+
			"= workload contention; review write fan-in or "+
			"partition strategy.",
		"{event}"); err != nil {
		return nil, err
	}
	if m.lookupByToken, err = mustCounter(
		"s3pgstore.lookup_by_token.count",
		"LookupByToken / write-path token short-circuit "+
			"outcomes, labeled by result (hit / miss).",
		"{lookup}"); err != nil {
		return nil, err
	}

	if cfg.PollLagFn != nil {
		pollLag, err := meter.Float64ObservableGauge(
			"s3pgstore.poll.lag",
			metric.WithDescription(
				"now() - latest feed_seq_at: end-to-end stream "+
					"lag from a write's commit to the sequencer "+
					"having stamped it. Sustained non-zero = "+
					"sequencer not keeping up."),
			metric.WithUnit("s"))
		if err != nil {
			return nil, err
		}
		_, err = meter.RegisterCallback(
			func(ctx context.Context, o metric.Observer) error {
				lag, ok, err := cfg.PollLagFn(ctx)
				if err != nil || !ok {
					// Errors / no-data are silent: sequencer is
					// optional and an unattended database
					// shouldn't spam logs every collection. The
					// callback signature requires a non-nil
					// return only on hard provider failure (we
					// have no such case here), so swallowing the
					// PG error is intentional.
					return nil //nolint:nilerr
				}
				o.ObserveFloat64(pollLag, lag.Seconds())
				return nil
			}, pollLag)
		if err != nil {
			return nil, err
		}
	}

	if cfg.PendingWritesDepthFn != nil {
		depth, err := meter.Int64ObservableGauge(
			"s3pgstore.pending_writes.depth",
			metric.WithDescription(
				"Current count of s3pgstore_pending_writes "+
					"rows. Sustained growth = orphan creation "+
					"outpacing GC reclaim — write-side bug or "+
					"stuck GC."),
			metric.WithUnit("{row}"))
		if err != nil {
			return nil, err
		}
		_, err = meter.RegisterCallback(
			func(ctx context.Context, o metric.Observer) error {
				count, err := cfg.PendingWritesDepthFn(ctx)
				if err != nil {
					// Same rationale as PollLag: a PG hiccup
					// shouldn't paint the OTel collector with
					// errors every cycle — observe-or-skip.
					return nil //nolint:nilerr
				}
				o.ObserveInt64(depth, count)
				return nil
			}, depth)
		if err != nil {
			return nil, err
		}
	}

	return m, nil
}

// methodScope is the per-method instrumentation primitive.
// Pattern vendored from s3store: defer scope.end at the top of
// a method, passing a pointer to the named return error. The
// scope records duration, calls, and in-flight on entry/exit.
//
// Usage:
//
//	func (s *Store[T]) Write(ctx, ...) (out []WriteResult, err error) {
//	    defer s.metrics.methodScope(ctx, "Write", &err).end()
//	    ...
//	}
//
// The scope's end records duration + calls with the outcome
// label resolved from *err — "success" if nil, "canceled" on
// ctx-canceled / deadline-exceeded, otherwise "error" — and
// decrements the in-flight gauge.
func (m *Metrics) methodScope(
	ctx context.Context, method string, err *error,
) methodScopeHandle {
	if m == nil {
		return methodScopeHandle{}
	}
	attrs := metric.WithAttributes(
		attribute.String(attrKeyMethod, method))
	m.methodInFlight.Add(ctx, 1, attrs)
	return methodScopeHandle{
		ctx:    ctx,
		method: method,
		err:    err,
		start:  time.Now(),
		m:      m,
	}
}

type methodScopeHandle struct {
	ctx    context.Context
	method string
	err    *error
	start  time.Time
	m      *Metrics
}

// end records the duration histogram + calls counter and
// decrements the in-flight gauge. Called via defer; the *err
// pointer is dereferenced here so the deferred call sees the
// final return value.
func (h methodScopeHandle) end() {
	if h.m == nil {
		return
	}
	outcome := outcomeSuccess
	if h.err != nil && *h.err != nil {
		switch {
		case errors.Is(*h.err, context.Canceled),
			errors.Is(*h.err, context.DeadlineExceeded):
			outcome = outcomeCanceled
		default:
			outcome = outcomeError
		}
	}
	methodAttr := attribute.String(attrKeyMethod, h.method)
	h.m.methodInFlight.Add(h.ctx, -1,
		metric.WithAttributes(methodAttr))
	attrs := metric.WithAttributes(
		methodAttr,
		attribute.String(attrKeyOutcome, outcome),
	)
	h.m.methodDuration.Record(h.ctx,
		time.Since(h.start).Seconds(), attrs)
	h.m.methodCalls.Add(h.ctx, 1, attrs)
}

// recordWriteVolume is fired once per successful Write /
// WriteWithKey call: bytes ships compressed parquet bytes;
// records ships the per-call record count.
func (m *Metrics) recordWriteVolume(
	ctx context.Context, bytesPut int64, records int64,
) {
	if m == nil {
		return
	}
	m.writeBytes.Record(ctx, bytesPut)
	m.writeRecords.Record(ctx, records)
}

// recordEncodeBufDropped fires when the encoder discards an
// oversized buffer instead of returning it to the pool. Plumbed
// through parquetEncoder.onBufDropped.
func (m *Metrics) recordEncodeBufDropped(ctx context.Context) {
	if m == nil {
		return
	}
	m.writeEncodeBufDropped.Add(ctx, 1)
}

// recordTokenRaceRetry fires when errTokenRaceLost was caught
// and the canonical row was re-fetched. One increment per retry,
// not per hit (which is lookupByToken{result=hit}).
func (m *Metrics) recordTokenRaceRetry(ctx context.Context) {
	if m == nil {
		return
	}
	m.writeTokenRaceRetry.Add(ctx, 1)
}

// recordOCCConflict fires when an OCC write returns
// ErrVersionConflict. One increment per conflict.
func (m *Metrics) recordOCCConflict(ctx context.Context) {
	if m == nil {
		return
	}
	m.occVersionConflict.Add(ctx, 1)
}

// recordLookupByToken fires once per token lookup with the
// canonicalised result label.
func (m *Metrics) recordLookupByToken(
	ctx context.Context, hit bool,
) {
	if m == nil {
		return
	}
	result := "miss"
	if hit {
		result = "hit"
	}
	m.lookupByToken.Add(ctx, 1, metric.WithAttributes(
		attribute.String(attrKeyResult, result)))
}

// fanOutObserverFor returns a fanOutObserver that records
// fanout.partitions / fanout.items for the named method. Wired
// at the call site (Write fan-out, PollRecords fan-out).
func (m *Metrics) fanOutObserverFor(method string) fanOutObserver {
	if m == nil {
		return nil
	}
	return func(ctx context.Context, items, _ int) {
		attrs := metric.WithAttributes(
			attribute.String(attrKeyMethod, method))
		m.fanoutItems.Record(ctx, int64(items), attrs)
		m.fanoutPartitions.Record(ctx, int64(items), attrs)
	}
}

// s3OpScope is the per-S3-op instrumentation primitive. The s3
// wrapper's PUT/GET/DELETE flows construct a scope at entry,
// record sem wait through scope.acquired, then defer end with
// the final attempt count + error.
type s3OpScope struct {
	ctx    context.Context
	op     string
	start  time.Time
	semBeg time.Time
	m      *Metrics
}

// startS3Op begins an op-scope. Pair with end via defer.
func (m *Metrics) startS3Op(
	ctx context.Context, op string,
) *s3OpScope {
	if m == nil {
		return &s3OpScope{m: nil}
	}
	now := time.Now()
	m.targetSemWaiting.Add(ctx, 1, metric.WithAttributes(
		attribute.String(attrKeyOperation, op)))
	return &s3OpScope{
		ctx:    ctx,
		op:     op,
		start:  now,
		semBeg: now,
		m:      m,
	}
}

// acquired marks the moment the semaphore slot was granted.
// Records sem_wait_duration and bumps sem_inflight; pair with
// release via release.
func (s *s3OpScope) acquired() {
	if s == nil || s.m == nil {
		return
	}
	opAttr := attribute.String(attrKeyOperation, s.op)
	s.m.targetSemWaiting.Add(s.ctx, -1,
		metric.WithAttributes(opAttr))
	s.m.targetSemWaitDuration.Record(s.ctx,
		time.Since(s.semBeg).Seconds(),
		metric.WithAttributes(opAttr))
	s.m.targetSemInflight.Add(s.ctx, 1,
		metric.WithAttributes(opAttr))
}

// acquireFailed marks the case where the semaphore wait was
// cancelled (ctx done) before a slot was granted. Decrements
// the waiting counter without bumping inflight.
func (s *s3OpScope) acquireFailed() {
	if s == nil || s.m == nil {
		return
	}
	s.m.targetSemWaiting.Add(s.ctx, -1, metric.WithAttributes(
		attribute.String(attrKeyOperation, s.op)))
}

// released decrements sem_inflight. Mirrors s3target.release().
func (s *s3OpScope) released() {
	if s == nil || s.m == nil {
		return
	}
	s.m.targetSemInflight.Add(s.ctx, -1, metric.WithAttributes(
		attribute.String(attrKeyOperation, s.op)))
}

// recordTransient fires once per transient failure observed
// inside retry() — every 5xx / 429 / SlowDown / transport-layer
// failure, whether the call eventually succeeds via retry or
// the budget is exhausted. attempt is 1-based (the index of
// the failed attempt).
func (s *s3OpScope) recordTransient(attempt int, err error) {
	if s == nil || s.m == nil {
		return
	}
	_, errType := classifyS3Error(err)
	s.m.s3TransientCount.Add(s.ctx, 1, metric.WithAttributes(
		attribute.String(attrKeyOperation, s.op),
		attribute.String(attrKeyErrorType, errType),
		attribute.Int(attrKeyAttempt, attempt),
	))
}

// recordBodySize records the wire-bytes for the op (PUT request
// body or GET response body). Skip when bytes <= 0 (DELETE has
// no body).
func (s *s3OpScope) recordBodySize(bytes int64) {
	if s == nil || s.m == nil || bytes <= 0 {
		return
	}
	s.m.s3BodySize.Record(s.ctx, bytes, metric.WithAttributes(
		attribute.String(attrKeyOperation, s.op)))
}

// end records the duration histogram + count counter for the
// op. attempts is the outer retry()'s iteration count
// (1..retryMaxAttempts on entered loops, 0 when the call
// exited before the loop ran — e.g. semaphore-acquire failure).
func (s *s3OpScope) end(attempts int, err error) {
	if s == nil || s.m == nil {
		return
	}
	outcome, errType := classifyS3Error(err)
	opAttr := attribute.String(attrKeyOperation, s.op)
	s.m.s3RequestDuration.Record(s.ctx,
		time.Since(s.start).Seconds(),
		metric.WithAttributes(opAttr))
	attrs := []attribute.KeyValue{
		opAttr,
		attribute.String(attrKeyOutcome, outcome),
		attribute.Int(attrKeyAttempts, attempts),
	}
	if outcome == outcomeError && errType != "" {
		attrs = append(attrs,
			attribute.String(attrKeyErrorType, errType))
	}
	s.m.s3RequestCount.Add(s.ctx, 1, metric.WithAttributes(attrs...))
}

// classifyS3Error maps a (possibly-wrapped) S3 SDK error to
// (outcome, error.type) labels. The two-level rule is
// load-bearing:
//
//  1. The smithy.APIError code takes precedence over HTTP status.
//     AWS S3 returns SlowDown as either HTTP 429 or HTTP 503; a
//     status-only switch would bucket the 503-flavoured throttle
//     as errTypeServer and dashboards lose the actual cause.
//  2. Then HTTP status: 429 → slowdown, 5xx → server, 4xx →
//     client.
//  3. No HTTP response attached → transport-level (DNS / TCP /
//     TLS / connection reset).
//
// nil err returns (outcomeSuccess, "").
func classifyS3Error(err error) (outcome, errType string) {
	if err == nil {
		return outcomeSuccess, ""
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return outcomeCanceled, errTypeCanceled
	}
	// API error code first: SlowDown can arrive as 429 or 503 and
	// both must classify as throttle. Without this precedence the
	// 503 flavour silently buckets as errTypeServer.
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch apiErr.ErrorCode() {
		case "SlowDown":
			return outcomeError, errTypeSlowDown
		case "NoSuchKey", "NotFound":
			return outcomeError, errTypeNotFound
		}
	}
	if respErr, ok := errors.AsType[*smithyhttp.ResponseError](err); ok {
		status := respErr.HTTPStatusCode()
		switch {
		case status == 429:
			return outcomeError, errTypeSlowDown
		case status >= 500:
			return outcomeError, errTypeServer
		case status >= 400:
			return outcomeError, errTypeClient
		default:
			return outcomeError, errTypeOther
		}
	}
	// No HTTP response and not a known sentinel — treat as
	// transport-layer (DNS / TCP / TLS / connection reset).
	return outcomeError, errTypeTransport
}
