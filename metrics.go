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
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// scopeName is the OTel meter scope this package's
// instruments register under when cfg.Meter is nil.
// Convention: the Go package import path (matches OTel's
// instrumentation-scope semantic).
const scopeName = "github.com/ueisele/s3pgstore"

// Outcome label values for method-scope instrumentation. Kept
// as constants so a dashboard regex never has to chase a typo.
// (s3.* outcome / error.type constants live in
// internal/s3client — the s3 surface is self-contained there.)
const (
	outcomeSuccess  = "success"
	outcomeError    = "error"
	outcomeCanceled = "canceled"
)

// Attribute keys for library-internal instrumentation
// (method.*, write.*, fanout.*, lookup.*, iter.*). The
// internal/s3client package owns its own attribute keys for
// the s3.* surface.
const (
	attrKeyMethod  = "method"
	attrKeyOutcome = "outcome"
	attrKeyResult  = "result"
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

// metrics is the per-Store instrumentation handle. Unexported
// because no external caller needs to hold one — store.go
// constructs it via newMetrics. The s3.* metrics surface lives
// entirely in internal/s3client; this struct does not own it.
// All methods are safe for concurrent use; a nil receiver makes
// every scope/record call a no-op.
type metrics struct {
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

	// Fan-out shape (Write multi-partition, PollRecords).
	fanoutPartitions metric.Int64Histogram
	fanoutItems      metric.Int64Histogram

	// Catalog / locking.
	occVersionConflict metric.Int64Counter
	lookupByToken      metric.Int64Counter

	// Iter pipeline (read_iter.go: producer/downloader/decoder).
	// Vendored shape from s3store — same instrument names with the
	// s3pgstore prefix. Saturation signals (body_slot, byte_budget)
	// are paired wait+exhausted observations; decode_duration is
	// per-partition wall-clock; stall.count is the watchdog's
	// "no-forward-progress" pulse.
	iterBodySlotWait        metric.Float64Histogram
	iterBodySlotExhausted   metric.Int64Counter
	iterByteBudgetWait      metric.Float64Histogram
	iterByteBudgetExhausted metric.Int64Counter
	iterDecodeDuration      metric.Float64Histogram
	iterStallCount          metric.Int64Counter
}

// newMetrics registers every instrument in cfg.Meter and wires
// the observable-gauge callbacks. A nil meter resolves to the
// OTel global provider; that itself defaults to a no-op
// meter when nothing is installed (so callers that don't wire
// telemetry pay near-zero cost), but flows through real
// telemetry when otelinit / autoexport / explicit
// MeterProvider construction has set a global. Errors
// propagate from instrument registration — the only realistic
// failure is provider-side configuration rejecting an
// instrument, which surfaces at New() so the operator sees
// it before the first write.
func newMetrics(cfg metricsConfig) (*metrics, error) {
	meter := cfg.Meter
	if meter == nil {
		meter = otel.GetMeterProvider().Meter(scopeName)
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

	m := &metrics{}
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

	// s3pgstore.s3.* instruments live in internal/s3client —
	// constructed inside s3client.WrapS3Client / BuildS3Client
	// from the caller's Meter. The library metrics struct
	// deliberately owns nothing under the s3.* namespace.

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

	// Iter pipeline. body_slot.wait fires only when a downloader
	// blocks AND eventually acquires; body_slot.exhausted is the
	// counter twin. byte_budget.* mirror the same shape for the
	// decoder's reserveBytes path. decode.duration is per-
	// partition wall-clock decode time. stall.count is the
	// watchdog's pure-observer pulse — non-zero rate means the
	// pipeline made no forward progress within the watchdog
	// window (deadlock or extremely slow consumer).
	if m.iterBodySlotWait, err = mustHist(
		"s3pgstore.read.iter.body_slot.wait.duration",
		"Time downloaders spent blocked acquiring a body-slot in "+
			"the iter pipeline (recorded only when a wait actually "+
			"occurred AND the acquire eventually succeeded — "+
			"cancel-during-wait is shutdown noise).",
		"s", shortWaitBuckets...); err != nil {
		return nil, err
	}
	if m.iterBodySlotExhausted, err = mustCounter(
		"s3pgstore.read.iter.body_slot.exhausted",
		"Times the iter pipeline's body-slot pool was full and a "+
			"downloader had to wait. Sustained non-zero rate = "+
			"the consumer's per-record work is the bottleneck.",
		"{event}"); err != nil {
		return nil, err
	}
	if m.iterByteBudgetWait, err = mustHist(
		"s3pgstore.read.iter.byte_budget.wait.duration",
		"Time the decoder spent blocked reserving uncompressed "+
			"bytes against ReadAheadBytes (recorded only when a "+
			"wait actually occurred AND the reservation succeeded).",
		"s", shortWaitBuckets...); err != nil {
		return nil, err
	}
	if m.iterByteBudgetExhausted, err = mustCounter(
		"s3pgstore.read.iter.byte_budget.exhausted",
		"Times the iter pipeline's byte budget was full and the "+
			"decoder had to wait. WithReadAheadBytes is binding.",
		"{event}"); err != nil {
		return nil, err
	}
	if m.iterDecodeDuration, err = mustHist(
		"s3pgstore.read.iter.partition.decode.duration",
		"Wall-clock parquet decode time per partition (excludes "+
			"byte-budget wait and download time).",
		"s", durationBuckets...); err != nil {
		return nil, err
	}
	if m.iterStallCount, err = mustCounter(
		"s3pgstore.read.iter.stall.count",
		"Times the iter pipeline made no forward progress "+
			"(markComplete or slot release) within the watchdog "+
			"window. Pure observer — the watchdog does not cancel. "+
			"Indicates a deadlock (library bug) or a slow consumer "+
			"(heavy yield-side processing). Labeled by method.",
		"{event}"); err != nil {
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
				if err != nil {
					// A real PG error (table missing, connection
					// refused, syntax error after a refactor) is
					// logged so operators can see something is
					// wrong; the OTel callback still returns nil
					// because a single broken gauge mustn't fail
					// the whole collection cycle. At typical
					// scrape intervals (15-60s) the log rate
					// during a real outage is bounded and useful.
					slog.Warn(
						"s3pgstore: poll-lag gauge query failed",
						"err", err)
					return nil
				}
				if !ok {
					// No-data (no sequenced rows yet) is the
					// normal cold-start state and stays silent.
					return nil
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
					// Log so a real failure is visible; return
					// nil so one broken gauge doesn't fail the
					// whole collection cycle.
					slog.Warn(
						"s3pgstore: pending-writes-depth gauge query failed",
						"err", err)
					return nil
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
func (m *metrics) methodScope(
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
	m      *metrics
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
func (m *metrics) recordWriteVolume(
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
func (m *metrics) recordEncodeBufDropped(ctx context.Context) {
	if m == nil {
		return
	}
	m.writeEncodeBufDropped.Add(ctx, 1)
}

// recordTokenRaceRetry fires when errTokenRaceLost was caught
// and the canonical row was re-fetched. One increment per retry,
// not per hit (which is lookupByToken{result=hit}).
func (m *metrics) recordTokenRaceRetry(ctx context.Context) {
	if m == nil {
		return
	}
	m.writeTokenRaceRetry.Add(ctx, 1)
}

// recordOCCConflict fires when an OCC write returns
// ErrVersionConflict. One increment per conflict.
func (m *metrics) recordOCCConflict(ctx context.Context) {
	if m == nil {
		return
	}
	m.occVersionConflict.Add(ctx, 1)
}

// recordLookupByToken fires once per token lookup with the
// canonicalised result label.
func (m *metrics) recordLookupByToken(
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
func (m *metrics) fanOutObserverFor(method string) fanOutObserver {
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

// recordIterBodySlotWait reports one acquireBodySlot call that
// blocked AND ended in a successful acquire. Records the wait
// duration on the histogram and increments the exhausted
// counter. Cancel-during-wait is intentionally NOT recorded —
// that path fires only during shutdown races, where a near-zero
// duration would drown out the saturation signal callers
// actually want to see.
func (m *metrics) recordIterBodySlotWait(
	ctx context.Context, dur time.Duration,
) {
	if m == nil {
		return
	}
	m.iterBodySlotWait.Record(ctx, dur.Seconds())
	m.iterBodySlotExhausted.Add(ctx, 1)
}

// recordIterByteBudgetWait reports one reserveBytes call that
// blocked AND ended in a successful reservation. Same shape as
// recordIterBodySlotWait — the cancel path is not recorded.
func (m *metrics) recordIterByteBudgetWait(
	ctx context.Context, dur time.Duration,
) {
	if m == nil {
		return
	}
	m.iterByteBudgetWait.Record(ctx, dur.Seconds())
	m.iterByteBudgetExhausted.Add(ctx, 1)
}

// recordIterDecodeDuration reports one partition's parquet
// decode wall-clock time. Recorded regardless of decode outcome
// — decode time is meaningful even on the error path so
// operators see how much time the decoder spent before failing.
func (m *metrics) recordIterDecodeDuration(
	ctx context.Context, dur time.Duration,
) {
	if m == nil {
		return
	}
	m.iterDecodeDuration.Record(ctx, dur.Seconds())
}

// recordIterStall reports one watchdog tick that observed a
// pipeline with no forward progress within the threshold
// window. Carries the public method as an attribute so
// operators can attribute stalls to a specific entry point
// (ReadIter / ReadPartitionIter / ReadRangeIter / ...).
//
// Pure observer — the caller logs slog.Warn alongside this
// counter increment but does not cancel the pipeline.
func (m *metrics) recordIterStall(
	ctx context.Context, method string,
) {
	if m == nil {
		return
	}
	m.iterStallCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String(attrKeyMethod, method)))
}
