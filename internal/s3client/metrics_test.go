package s3client

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestNewS3Metrics_NilMeterFallsBackToGlobal verifies a nil
// meter resolves via otel.GetMeterProvider(), which itself
// defaults to a no-op meter — so every record* call
// short-circuits safely when no global has been installed.
func TestNewS3Metrics_NilMeterFallsBackToGlobal(t *testing.T) {
	m, err := newS3Metrics(nil)
	if err != nil {
		t.Fatalf("newS3Metrics(nil): %v", err)
	}
	if m == nil {
		t.Fatal("metrics is nil")
	}
	ctx := context.Background()
	m.recordOp(ctx, "PutObject", time.Millisecond, 1, nil)
	m.recordAttemptError(ctx, "PutObject", 1, errors.New("x"), false)
	m.recordBodySize(ctx, "PutObject", 123)
	m.recordRatelimitWait(ctx, "PutObject", time.Millisecond)
	m.recordAdaptiveRetryWait(ctx, "PutObject", time.Millisecond)
	m.recordTCPConnOpen(ctx)
	m.recordTCPConnClose(ctx)
	m.recordConnectionReuse(ctx, true)
}

// TestS3Metrics_NilReceiverSafe verifies every record* method
// short-circuits when the receiver is nil. Middleware / wraps
// can hold a nil *s3metrics as the "telemetry off" sentinel
// without per-call branching.
func TestS3Metrics_NilReceiverSafe(t *testing.T) {
	var m *s3metrics
	ctx := context.Background()
	m.recordOp(ctx, "PutObject", time.Millisecond, 1, nil)
	m.recordAttemptError(ctx, "PutObject", 1, errors.New("x"), false)
	m.recordBodySize(ctx, "PutObject", 123)
	m.recordRatelimitWait(ctx, "PutObject", time.Millisecond)
	m.recordAdaptiveRetryWait(ctx, "PutObject", time.Millisecond)
	m.recordTCPConnOpen(ctx)
	m.recordTCPConnClose(ctx)
	m.recordConnectionReuse(ctx, true)
}

// TestS3Metrics_RecordedInstruments verifies every registered
// instrument actually emits via a manual reader. Locks in the
// metric names + label keys (the dashboard's PromQL refers to
// them verbatim, so a rename here would silently break panels).
func TestS3Metrics_RecordedInstruments(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader))
	meter := provider.Meter("s3pgstore-test")

	m, err := newS3Metrics(meter)
	if err != nil {
		t.Fatalf("newS3Metrics: %v", err)
	}

	ctx := context.Background()
	// Full happy path.
	m.recordOp(ctx, "PutObject", 50*time.Millisecond, 1, nil)
	m.recordBodySize(ctx, "PutObject", 2048)
	// Retried + terminal attempt-error path.
	m.recordAttemptError(ctx, "GetObject", 1,
		newSmithyResponseError(503, "SlowDown"), false)
	m.recordAttemptError(ctx, "GetObject", 2,
		errors.New("server"), true)
	m.recordOp(ctx, "GetObject", 75*time.Millisecond, 2,
		errors.New("server"))
	// Rate-limiter wait sample.
	m.recordRatelimitWait(ctx, "PutObject", 5*time.Millisecond)
	// Adaptive-retry token-bucket wait sample.
	m.recordAdaptiveRetryWait(ctx, "PutObject", 3*time.Millisecond)
	// Connection-pool metrics.
	m.recordTCPConnOpen(ctx)
	m.recordTCPConnClose(ctx)
	m.recordConnectionReuse(ctx, true)
	m.recordConnectionReuse(ctx, false)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	want := []string{
		"s3pgstore.s3.request.duration",
		"s3pgstore.s3.request.count",
		"s3pgstore.s3.attempt.error.count",
		"s3pgstore.s3.body_size",
		"s3pgstore.s3.ratelimit.wait.duration",
		"s3pgstore.s3.adaptive_retry.wait.duration",
		"s3pgstore.s3.tcp.connections",
		"s3pgstore.s3.connection.reuse.count",
	}
	got := collectedMetricNames(rm)
	for _, w := range want {
		found := false
		for _, g := range got {
			if g == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected metric %q in %v", w, got)
		}
	}
}

// TestClassifyS3Error_APICodeBeforeStatus locks in the
// load-bearing precedence: the SlowDown API code wins over HTTP
// status, so a 503 SlowDown classifies as "slowdown" rather
// than "server" — operators reading the dashboard see
// throttling spikes on the throttling panel, not on the
// outage panel.
func TestClassifyS3Error_APICodeBeforeStatus(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantOutcome string
		wantType    string
	}{
		{
			name:        "nil → success",
			err:         nil,
			wantOutcome: outcomeSuccess,
			wantType:    "",
		},
		{
			name:        "context.Canceled → canceled",
			err:         context.Canceled,
			wantOutcome: outcomeCanceled,
			wantType:    errTypeCanceled,
		},
		{
			name:        "503 SlowDown → slowdown (NOT server)",
			err:         newSmithyResponseError(503, "SlowDown"),
			wantOutcome: outcomeError,
			wantType:    errTypeSlowDown,
		},
		{
			name:        "429 → slowdown",
			err:         newSmithyResponseError(429, "ThrottlingException"),
			wantOutcome: outcomeError,
			wantType:    errTypeSlowDown,
		},
		{
			name:        "500 → server",
			err:         newSmithyResponseError(500, "InternalError"),
			wantOutcome: outcomeError,
			wantType:    errTypeServer,
		},
		{
			name:        "404 NoSuchKey → not_found",
			err:         newSmithyResponseError(404, "NoSuchKey"),
			wantOutcome: outcomeError,
			wantType:    errTypeNotFound,
		},
		{
			name:        "400 → client",
			err:         newSmithyResponseError(400, "InvalidArgument"),
			wantOutcome: outcomeError,
			wantType:    errTypeClient,
		},
		{
			name:        "no HTTP response → transport",
			err:         errors.New("dial: connection reset"),
			wantOutcome: outcomeError,
			wantType:    errTypeTransport,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome, errType := classifyS3Error(tc.err)
			if outcome != tc.wantOutcome {
				t.Fatalf("outcome: want %q, got %q",
					tc.wantOutcome, outcome)
			}
			if errType != tc.wantType {
				t.Fatalf("errType: want %q, got %q",
					tc.wantType, errType)
			}
		})
	}
}

// newSmithyResponseError builds a synthetic smithyhttp
// ResponseError that wraps a smithy.GenericAPIError carrying
// the supplied API code. classifyS3Error inspects both
// (errors.As walks the wrapped chain), so passing
// (status=503, code="SlowDown") simulates AWS's flavour
// where the throttling response code arrives over a 503.
func newSmithyResponseError(status int, code string) error {
	apiErr := &smithy.GenericAPIError{
		Code:    code,
		Message: "synthetic",
	}
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: status},
		},
		Err: apiErr,
	}
}

func collectedMetricNames(rm metricdata.ResourceMetrics) []string {
	var out []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out = append(out, m.Name)
		}
	}
	return out
}
