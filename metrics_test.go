package s3pgstore

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

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

// TestNewMetrics_NilMeterIsNoOp verifies that passing a nil
// meter resolves to the no-op meter — instruments register
// successfully and method calls are silent.
func TestNewMetrics_NilMeterIsNoOp(t *testing.T) {
	m, err := newMetrics(metricsConfig{})
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
	m, err := newMetrics(metricsConfig{
		Meter: noop.NewMeterProvider().Meter("test"),
	})
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

// TestMetrics_NilReceiverHelpersSafe verifies every recordX
// helper short-circuits when the receiver is nil. This is the
// silent path the Store falls back on when newMetrics has not
// been constructed (zero-value tests).
func TestMetrics_NilReceiverHelpersSafe(t *testing.T) {
	var m *Metrics
	ctx := context.Background()
	m.recordWriteVolume(ctx, 100, 10)
	m.recordEncodeBufDropped(ctx)
	m.recordTokenRaceRetry(ctx)
	m.recordOCCConflict(ctx)
	m.recordLookupByToken(ctx, true)
	m.recordIterBodySlotWait(ctx, time.Millisecond)
	m.recordIterByteBudgetWait(ctx, time.Millisecond)
	m.recordIterDecodeDuration(ctx, time.Millisecond)
	m.recordIterStall(ctx, "ReadIter")
	if obs := m.fanOutObserverFor("X"); obs != nil {
		t.Fatalf("nil receiver should produce nil observer")
	}
	scope := m.startS3Op(ctx, "put")
	scope.acquired()
	scope.acquireFailed()
	scope.released()
	scope.recordTransient(1, errors.New("x"))
	scope.recordBodySize(123)
	scope.end(1, nil)
}

// TestMetrics_RecordedInstruments verifies every registered
// instrument actually emits via a manual reader. Locks in the
// metric names + label keys (the dashboard's PromQL refers to
// them verbatim, so a rename here would silently break panels).
func TestMetrics_RecordedInstruments(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader))
	meter := provider.Meter("s3pgstore-test")

	pollLagFn := func(ctx context.Context) (time.Duration, bool, error) {
		return 5 * time.Second, true, nil
	}
	depthFn := func(ctx context.Context) (int64, error) {
		return 42, nil
	}
	m, err := newMetrics(metricsConfig{
		Meter:                meter,
		PollLagFn:            pollLagFn,
		PendingWritesDepthFn: depthFn,
	})
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	ctx := context.Background()
	// methodScope path.
	var werr error
	m.methodScope(ctx, "Write", &werr).end()
	// Write volumes.
	m.recordWriteVolume(ctx, 1<<20, 1234)
	m.recordEncodeBufDropped(ctx)
	m.recordTokenRaceRetry(ctx)
	m.recordOCCConflict(ctx)
	m.recordLookupByToken(ctx, true)
	m.recordLookupByToken(ctx, false)
	if obs := m.fanOutObserverFor("Write"); obs != nil {
		obs(ctx, 5, 2)
	}
	// S3 op scope: full happy path.
	scope := m.startS3Op(ctx, "put")
	scope.acquired()
	scope.recordBodySize(2048)
	scope.released()
	scope.end(1, nil)
	// S3 op scope: transient + terminal-error path.
	scope = m.startS3Op(ctx, "get")
	scope.acquired()
	scope.recordTransient(1,
		newSmithyResponseError(503, "SlowDown"))
	scope.released()
	scope.end(2, errors.New("server"))

	// Iter pipeline saturation/observer signals.
	m.recordIterBodySlotWait(ctx, 5*time.Millisecond)
	m.recordIterByteBudgetWait(ctx, 7*time.Millisecond)
	m.recordIterDecodeDuration(ctx, 12*time.Millisecond)
	m.recordIterStall(ctx, "ReadIter")

	// Collect.
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	want := []string{
		"s3pgstore.method.duration",
		"s3pgstore.method.calls",
		"s3pgstore.method.in_flight",
		"s3pgstore.write.bytes",
		"s3pgstore.write.records",
		"s3pgstore.write.encode_buf_dropped",
		"s3pgstore.write.token_race.retry.count",
		"s3pgstore.s3.request.duration",
		"s3pgstore.s3.request.count",
		"s3pgstore.s3.transient_error.count",
		"s3pgstore.s3.body_size",
		"s3pgstore.target.sem_wait_duration",
		"s3pgstore.target.sem_inflight",
		"s3pgstore.target.sem_waiting",
		"s3pgstore.fanout.partitions",
		"s3pgstore.fanout.items",
		"s3pgstore.occ.version_conflict.count",
		"s3pgstore.lookup_by_token.count",
		"s3pgstore.poll.lag",
		"s3pgstore.pending_writes.depth",
		"s3pgstore.read.iter.body_slot.wait.duration",
		"s3pgstore.read.iter.body_slot.exhausted",
		"s3pgstore.read.iter.byte_budget.wait.duration",
		"s3pgstore.read.iter.byte_budget.exhausted",
		"s3pgstore.read.iter.partition.decode.duration",
		"s3pgstore.read.iter.stall.count",
	}
	got := collectedMetricNames(rm)
	for _, w := range want {
		if !contains(got, w) {
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

func collectedMetricNames(rm metricdata.ResourceMetrics) []string {
	var out []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out = append(out, m.Name)
		}
	}
	return out
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
