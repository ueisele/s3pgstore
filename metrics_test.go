package s3pgstore

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"
)

// TestNewMetrics_NilMeterIsNoOp verifies that passing a nil
// meter resolves to the no-op meter — instruments register
// successfully and method calls are silent.
func TestNewMetrics_NilMeterIsNoOp(t *testing.T) {
	m, err := newMetrics(nil)
	if err != nil {
		t.Fatalf("newMetrics(nil): %v", err)
	}
	if m == nil {
		t.Fatal("metrics is nil")
	}
	// methodScope.end runs the no-op record path; mustn't panic.
	var sentinel error
	m.methodScope(context.Background(), "Test", &sentinel).end()
}

// TestMethodScope_ResolvesOutcome verifies the outcome label
// resolution: nil err → success; ctx-canceled → canceled; other
// → error. We can't observe the recorded label without
// reflecting into the no-op meter, so this test exercises the
// code paths to confirm they don't panic and to lock in the
// switch shape.
func TestMethodScope_ResolvesOutcome(t *testing.T) {
	m, err := newMetrics(noop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}
	cases := []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"canceled", context.Canceled},
		{"deadline", context.DeadlineExceeded},
		{"error", errors.New("real-error")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.err
			m.methodScope(context.Background(),
				"Method", &err).end()
		})
	}
}

// TestMethodScope_NilMetricsSafe verifies that calling end on a
// scope whose Metrics is nil (e.g. test fixture didn't wire one)
// is silent rather than panicking.
func TestMethodScope_NilMetricsSafe(t *testing.T) {
	var m *Metrics
	var sentinel error
	m.methodScope(context.Background(), "NoStore", &sentinel).end()
}
