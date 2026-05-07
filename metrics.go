package s3pgstore

// metrics.go provides the minimal OTel instrumentation surface
// for s3pgstore. The full Phase 16 plan calls for ~12 metrics
// across method duration, write/read volumes, S3 ops,
// sequencer state, and iter-pipeline saturation — most of
// which depend on hooks not yet wired in v2.0's
// implementation. This file ships the foundation:
//
//   - Meter constructor that callers feed an OTel meter into,
//     so the library doesn't pin a global provider.
//   - methodScope helper for duration + outcome instrumentation
//     at method boundaries (the pattern vendored from s3store).
//   - The two headline instruments wired on Write and Read:
//     s3pgstore.method.duration (histogram) and
//     s3pgstore.method.calls (counter).
//
// Adding a new instrument here requires adding the matching
// Grafana panel in dashboards/s3pgstore.json — see CLAUDE.md
// § Metrics ↔ dashboard sync. Drift is silent; an emitted
// metric without a panel is operationally invisible.
//
// The Store wires a default no-op Meter when none is provided,
// so opting out of telemetry is the zero-config default.
// Callers wanting real telemetry pass cfg.Meter at New() time
// (Phase 16.x will surface the field; for now the Store
// constructs an internal no-op meter and the field is
// reserved).

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Metrics holds the registered OTel instruments. Constructed
// once per Store via newMetrics(meter); all methods are safe
// for concurrent use.
type Metrics struct {
	methodDuration metric.Float64Histogram
	methodCalls    metric.Int64Counter
}

// newMetrics registers every instrument against meter and
// returns a Metrics handle. A nil meter resolves to a no-op
// Meter so callers that don't wire telemetry still get a
// functioning (silent) handle.
//
// Errors from instrument registration propagate; the only
// realistic failure mode is a meter rejecting an instrument
// name due to provider-side configuration, in which case the
// operator should see the error at New() and fix wiring.
func newMetrics(meter metric.Meter) (*Metrics, error) {
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("s3pgstore")
	}
	dur, err := meter.Float64Histogram(
		"s3pgstore.method.duration",
		metric.WithUnit("s"),
		metric.WithDescription(
			"Duration of public Store[T] method calls, "+
				"labeled by method and outcome."))
	if err != nil {
		return nil, err
	}
	calls, err := meter.Int64Counter(
		"s3pgstore.method.calls",
		metric.WithDescription(
			"Total public Store[T] method calls, labeled "+
				"by method and outcome (success or error)."))
	if err != nil {
		return nil, err
	}
	return &Metrics{
		methodDuration: dur,
		methodCalls:    calls,
	}, nil
}

// methodScope is the per-method instrumentation primitive.
// Pattern vendored from s3store: the caller defers `scope.end`
// at the top of a method, passing a pointer to the named
// return error so cancel-vs-error vs success outcomes resolve
// correctly.
//
// Usage:
//
//	func (s *Store[T]) Write(ctx, ...) (out []WriteResult, err error) {
//	    defer s.metrics.methodScope(ctx, "Write", &err).end()
//	    ...
//	}
//
// The scope's end records duration and increments calls with
// the outcome label resolved from *err — "success" if nil,
// "canceled" if errors.Is(err, context.Canceled), or "error"
// otherwise.
func (m *Metrics) methodScope(
	ctx context.Context, method string, err *error,
) methodScopeHandle {
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

// end records the duration histogram + calls counter for the
// scope. Called via defer; the *err pointer is dereferenced
// here so the deferred call sees the final return value.
func (h methodScopeHandle) end() {
	if h.m == nil {
		return
	}
	outcome := "success"
	if h.err != nil && *h.err != nil {
		switch {
		case errors.Is(*h.err, context.Canceled),
			errors.Is(*h.err, context.DeadlineExceeded):
			outcome = "canceled"
		default:
			outcome = "error"
		}
	}
	attrs := metric.WithAttributes(
		attribute.String("method", h.method),
		attribute.String("outcome", outcome),
	)
	h.m.methodDuration.Record(h.ctx,
		time.Since(h.start).Seconds(), attrs)
	h.m.methodCalls.Add(h.ctx, 1, attrs)
}
