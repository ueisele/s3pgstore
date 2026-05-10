package pool

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// scopeName is the OTel instrumentation scope name used when
// the caller passes a nil meter to New. Convention: the Go
// package import path (matches OTel's instrumentation-scope
// semantic).
const scopeName = "github.com/ueisele/s3pgstore/pool"

// metrics is the pool's instrumentation handle. Always non-nil
// after newMetrics — a nil meter is replaced with a noop
// fallback before instrument registration — so call sites never
// branch on nil.
type metrics struct {
	inFlight metric.Int64UpDownCounter
	waitDur  metric.Float64Histogram
}

// newMetrics registers the pool's instruments against meter. A
// nil meter is replaced with the noop provider so every record
// call becomes a no-op.
func newMetrics(meter metric.Meter) (*metrics, error) {
	if meter == nil {
		meter = noop.NewMeterProvider().Meter(scopeName)
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
	// Bucket boundaries mirror the iter pipeline's body-slot
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
	return &metrics{
		inFlight: inFlight,
		waitDur:  waitDur,
	}, nil
}

// addInFlight bumps the in-flight task counter by delta (+1
// when a task is admitted, -1 when it completes).
func (m *metrics) addInFlight(ctx context.Context, delta int64) {
	m.inFlight.Add(ctx, delta)
}

// recordWait records how long the submitter blocked acquiring
// a pool slot before its task started.
func (m *metrics) recordWait(ctx context.Context, d time.Duration) {
	m.waitDur.Record(ctx, d.Seconds())
}
