package s3client

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestWithStoreLabels_RoundTrip locks in the basic
// stash-and-retrieve contract. The metrics middleware depends on
// this round-trip working without surprises.
func TestWithStoreLabels_RoundTrip(t *testing.T) {
	cases := []struct {
		name           string
		bucket, prefix string
	}{
		{"both set", "warehouse", "billing"},
		{"bucket only", "warehouse", ""},
		{"prefix only", "", "billing"},
		{"both empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := WithStoreLabels(context.Background(),
				tc.bucket, tc.prefix)
			b, p := storeLabelsFromContext(ctx)
			if b != tc.bucket || p != tc.prefix {
				t.Errorf("round-trip: want (%q, %q), got (%q, %q)",
					tc.bucket, tc.prefix, b, p)
			}
		})
	}
}

// TestStoreLabelsFromContext_Unset verifies the absent-key case
// returns zero values (not a panic). Per-call zero-cost path
// for callers who don't use the labels.
func TestStoreLabelsFromContext_Unset(t *testing.T) {
	b, p := storeLabelsFromContext(context.Background())
	if b != "" || p != "" {
		t.Errorf("unset ctx: want (\"\", \"\"), got (%q, %q)", b, p)
	}
}

// TestStoreAttrs_OmitsEmpty verifies storeAttrs returns nil when
// no labels are set, and skips the empty side when only one is.
// Keeps the no-label-set caller path zero-attribute.
func TestStoreAttrs_OmitsEmpty(t *testing.T) {
	cases := []struct {
		name           string
		bucket, prefix string
		wantLen        int
	}{
		{"both empty → nil attrs", "", "", 0},
		{"bucket only → 1 attr", "warehouse", "", 1},
		{"prefix only → 1 attr", "", "billing", 1},
		{"both set → 2 attrs", "warehouse", "billing", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := WithStoreLabels(context.Background(),
				tc.bucket, tc.prefix)
			got := storeAttrs(ctx)
			if len(got) != tc.wantLen {
				t.Errorf("len: want %d, got %d (%v)",
					tc.wantLen, len(got), got)
			}
		})
	}
}

// TestS3Metrics_PerOpMetricsCarryStoreLabels verifies the
// per-op record* methods emit s3pgstore.bucket /
// s3pgstore.prefix labels when ctx carries them. This is the
// load-bearing test for the multi-Store-shared-client design —
// without these labels the dashboard can't distinguish callers.
func TestS3Metrics_PerOpMetricsCarryStoreLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader))
	meter := provider.Meter("s3pgstore-test")

	m, err := newS3Metrics(meter)
	if err != nil {
		t.Fatalf("newS3Metrics: %v", err)
	}

	ctx := WithStoreLabels(context.Background(),
		"warehouse", "billing")

	// Exercise every per-op recorder.
	m.recordOp(ctx, "PutObject", 50*time.Millisecond, 1, nil)
	m.recordAttemptError(ctx, "GetObject", 1,
		errors.New("server"), false)
	m.recordBodySize(ctx, "PutObject", 2048)
	m.recordRatelimitWait(ctx, "PutObject", 5*time.Millisecond)

	// Per-client recorders that should NOT see the labels even
	// though ctx carries them — load-bearing for the "shared
	// client = shared connection pool / adaptive retryer"
	// invariant. Splitting these per-Store would mis-attribute
	// (Store A's wait might be caused by Store B's prior throttle).
	m.recordAdaptiveRetryWait(ctx, "PutObject", 3*time.Millisecond)
	m.recordTCPConnOpen(ctx)
	m.recordConnectionReuse(ctx, true)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	// Per-op metrics MUST carry both labels.
	perOp := []string{
		"s3pgstore.s3.request.duration",
		"s3pgstore.s3.request.count",
		"s3pgstore.s3.attempt.error.count",
		"s3pgstore.s3.body_size",
		"s3pgstore.s3.ratelimit.wait.duration",
	}
	for _, name := range perOp {
		assertLabelPresent(t, rm, name, "s3pgstore.bucket", "warehouse")
		assertLabelPresent(t, rm, name, "s3pgstore.prefix", "billing")
	}

	// Per-client metrics MUST NOT carry the labels — splitting
	// them per-Store on a shared client would mis-attribute.
	perClient := []string{
		"s3pgstore.s3.adaptive_retry.wait.duration",
		"s3pgstore.s3.tcp.connections",
		"s3pgstore.s3.connection.reuse.count",
	}
	for _, name := range perClient {
		assertLabelAbsent(t, rm, name, "s3pgstore.bucket")
		assertLabelAbsent(t, rm, name, "s3pgstore.prefix")
	}
}

// TestS3Metrics_NoStoreLabels_NoLabelEmitted verifies the
// zero-cost path: a caller that never calls WithStoreLabels
// sees no s3pgstore.bucket / s3pgstore.prefix attributes on
// their metrics. Single-Store-per-process deployments stay
// label-free (resource attributes do the differentiation).
func TestS3Metrics_NoStoreLabels_NoLabelEmitted(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader))
	meter := provider.Meter("s3pgstore-test")

	m, err := newS3Metrics(meter)
	if err != nil {
		t.Fatalf("newS3Metrics: %v", err)
	}

	ctx := context.Background()
	m.recordOp(ctx, "PutObject", 50*time.Millisecond, 1, nil)
	m.recordBodySize(ctx, "PutObject", 2048)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	for _, name := range []string{
		"s3pgstore.s3.request.duration",
		"s3pgstore.s3.body_size",
	} {
		assertLabelAbsent(t, rm, name, "s3pgstore.bucket")
		assertLabelAbsent(t, rm, name, "s3pgstore.prefix")
	}
}

// assertLabelPresent fails if the named metric in rm doesn't
// carry an attribute matching (key, want) on at least one data
// point.
func assertLabelPresent(
	t *testing.T,
	rm metricdata.ResourceMetrics,
	metricName, key, want string,
) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			if hasAttr(m, key, want) {
				return
			}
			t.Errorf("metric %q: attribute %q=%q not found in any data point",
				metricName, key, want)
			return
		}
	}
	t.Errorf("metric %q: not found in collected data", metricName)
}

// assertLabelAbsent fails if the named metric has any data
// point carrying an attribute with the given key.
func assertLabelAbsent(
	t *testing.T,
	rm metricdata.ResourceMetrics,
	metricName, key string,
) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			if attrPresent(m, key) {
				t.Errorf("metric %q: attribute %q present "+
					"but expected absent (per-client metric)",
					metricName, key)
			}
			return
		}
	}
	// Metric not collected at all → vacuously absent. Caller
	// validates presence separately if needed.
}

// hasAttr returns true when any data point on m carries
// (key=want).
func hasAttr(
	m metricdata.Metrics, key, want string,
) bool {
	switch d := m.Data.(type) {
	case metricdata.Histogram[float64]:
		for _, dp := range d.DataPoints {
			if v, ok := dp.Attributes.Value(attribute.Key(key)); ok &&
				v.AsString() == want {
				return true
			}
		}
	case metricdata.Histogram[int64]:
		for _, dp := range d.DataPoints {
			if v, ok := dp.Attributes.Value(attribute.Key(key)); ok &&
				v.AsString() == want {
				return true
			}
		}
	case metricdata.Sum[int64]:
		for _, dp := range d.DataPoints {
			if v, ok := dp.Attributes.Value(attribute.Key(key)); ok &&
				v.AsString() == want {
				return true
			}
		}
	case metricdata.Sum[float64]:
		for _, dp := range d.DataPoints {
			if v, ok := dp.Attributes.Value(attribute.Key(key)); ok &&
				v.AsString() == want {
				return true
			}
		}
	}
	return false
}

// attrPresent returns true when any data point on m carries
// the given attribute key (regardless of value).
func attrPresent(m metricdata.Metrics, key string) bool {
	switch d := m.Data.(type) {
	case metricdata.Histogram[float64]:
		for _, dp := range d.DataPoints {
			if _, ok := dp.Attributes.Value(attribute.Key(key)); ok {
				return true
			}
		}
	case metricdata.Histogram[int64]:
		for _, dp := range d.DataPoints {
			if _, ok := dp.Attributes.Value(attribute.Key(key)); ok {
				return true
			}
		}
	case metricdata.Sum[int64]:
		for _, dp := range d.DataPoints {
			if _, ok := dp.Attributes.Value(attribute.Key(key)); ok {
				return true
			}
		}
	case metricdata.Sum[float64]:
		for _, dp := range d.DataPoints {
			if _, ok := dp.Attributes.Value(attribute.Key(key)); ok {
				return true
			}
		}
	}
	return false
}
