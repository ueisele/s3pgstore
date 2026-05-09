package s3client

// metrics.go owns the s3pgstore.s3.* OTel surface. Defines the
// per-client s3metrics handle that the SDK middleware (in
// metrics_s3op.go) and the connection-pool wraps (in
// metrics_connpool.go) call into. Self-contained: WithDefaults
// constructs one of these from opts.Meter; the library and
// operator binaries don't see the type, just pass a meter (or
// nothing — fall back to OTel's global provider).

import (
	"context"
	"errors"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// scopeName is the OTel meter scope all instruments register
// under. Operators see s3pgstore.s3.* as the metric *names*;
// the *scope* is a separate field used by some exporters for
// grouping.
const scopeName = "github.com/ueisele/s3pgstore/s3client"

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

// Attribute keys. Centralised so dashboard PromQL refers to
// label names that exactly match what the recorder emits.
const (
	attrKeyOperation = "s3pgstore.operation"
	attrKeyAttempts  = "s3pgstore.attempts"
	attrKeyAttempt   = "s3pgstore.attempt"
	attrKeyOutcome   = "outcome"
	attrKeyErrorType = "error.type"
	attrKeyReused    = "reused"
	attrKeyTerminal  = "terminal"
)

// Bucket boundaries chosen so histogram_quantile resolves
// percentiles within the realistic operating range.
//
//nolint:gochecknoglobals // immutable bucket boundaries
var (
	durationBuckets = []float64{
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
	}
	shortWaitBuckets = []float64{
		0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5,
	}
	byteBuckets = []float64{
		1024, 4096, 16384, 65536, 262144, 1048576,
		4194304, 8388608, 16777216, 33554432,
		67108864, 134217728, 1073741824,
	}
)

// s3metrics owns the s3pgstore.s3.* OTel instruments. Private
// — constructed only via newS3Metrics, only inside this
// package. The middleware / connection-pool wraps hold a
// *s3metrics directly; no interface indirection.
//
// A nil receiver makes every record* call a no-op. Callers can
// freely pass nil to the middleware / wraps to disable
// telemetry without per-call branching.
type s3metrics struct {
	requestDuration   metric.Float64Histogram
	requestCount      metric.Int64Counter
	attemptError      metric.Int64Counter
	bodySize          metric.Int64Histogram
	ratelimitWait     metric.Float64Histogram
	adaptiveRetryWait metric.Float64Histogram
	tcpConnections    metric.Int64UpDownCounter
	connectionReuse   metric.Int64Counter
}

// newS3Metrics registers the eight s3pgstore.s3.* instruments
// against meter and returns an s3metrics backed by them.
//
// meter == nil falls back to otel.GetMeterProvider().Meter(...) —
// the OTel global provider, which itself defaults to the no-op
// provider when nothing has been installed via otelinit /
// autoexport / explicit MeterProvider construction. Net effect:
// callers that wire OTel via env vars get telemetry for free;
// callers that don't get silence at near-zero cost. Same
// pattern the library's Config.Meter follows.
//
// Adding a new instrument here requires adding the matching
// Grafana panel in dashboards/s3pgstore.json — see CLAUDE.md
// § Metrics ↔ dashboard sync.
func newS3Metrics(meter metric.Meter) (*s3metrics, error) {
	if meter == nil {
		meter = otel.GetMeterProvider().Meter(scopeName)
	}
	m := &s3metrics{}
	var err error
	if m.requestDuration, err = meter.Float64Histogram(
		"s3pgstore.s3.request.duration",
		metric.WithDescription(
			"Wall-clock duration of one logical S3 operation "+
				"(including SDK-managed adaptive retries), labeled "+
				"by op."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBuckets...),
	); err != nil {
		return nil, err
	}
	if m.requestCount, err = meter.Int64Counter(
		"s3pgstore.s3.request.count",
		metric.WithDescription(
			"Total S3 operations (one row per logical op, after "+
				"all SDK-managed retries). Labels: operation, "+
				"outcome (success / error / canceled), attempts "+
				"(SDK retry-loop iteration count; 1 = first-try "+
				"success, more = retried). Per-attempt error "+
				"classification (HTTP 5xx / SlowDown / transport "+
				"/ etc.) lives in s3.attempt.error.count, not "+
				"here — keeps this rollup clean and avoids "+
				"double-counting against attempt.error.count."),
		metric.WithUnit("{request}"),
	); err != nil {
		return nil, err
	}
	if m.attemptError, err = meter.Int64Counter(
		"s3pgstore.s3.attempt.error.count",
		metric.WithDescription(
			"Total failed S3 attempts — HTTP 5xx, HTTP 429 "+
				"SlowDown (and 503 with API code SlowDown), "+
				"client errors, NoSuchKey, transport-layer "+
				"failures (DNS / TCP / TLS / connection reset), "+
				"and ctx-cancel. Sourced from the SDK's "+
				"AttemptResults metadata; fires once per failed "+
				"attempt — retried OR terminal. Operators query a "+
				"single counter for 'rate of <error_type> "+
				"events' without summing two metrics. Labels: "+
				"operation, error.type, attempt (1-based index of "+
				"the failed attempt), terminal (true = final / "+
				"budget-exhausted; false = retried intermediate)."),
		metric.WithUnit("{event}"),
	); err != nil {
		return nil, err
	}
	if m.bodySize, err = meter.Int64Histogram(
		"s3pgstore.s3.body_size",
		metric.WithDescription(
			"Wire-bytes per S3 PUT/GET — parquet body size as the "+
				"SDK saw it. PUT samples (compressed) sit alongside "+
				"the uncompressed write.bytes; GET samples reflect "+
				"per-file fetch size for capacity planning."),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(byteBuckets...),
	); err != nil {
		return nil, err
	}
	if m.ratelimitWait, err = meter.Float64Histogram(
		"s3pgstore.s3.ratelimit.wait.duration",
		metric.WithDescription(
			"Wallclock spent in the client-side rate limiter "+
				"before the SDK ran the op. Measured outside "+
				"s3.request.duration, so SDK-side latency stays "+
				"clean. Sub-millisecond observations are normal "+
				"when MaxRequestsPerSecond is unsaturated; "+
				"rising p99 means the per-second cap is the "+
				"bottleneck. Only emitted when the rate limiter "+
				"is configured (MaxRequestsPerSecond > 0); "+
				"recorded only on Wait success (cancelled waits "+
				"skip)."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(shortWaitBuckets...),
	); err != nil {
		return nil, err
	}
	if m.adaptiveRetryWait, err = meter.Float64Histogram(
		"s3pgstore.s3.adaptive_retry.wait.duration",
		metric.WithDescription(
			"Wallclock the SDK's adaptive-mode token bucket held "+
				"each attempt waiting for a token. Measured "+
				"around RetryerV2.GetAttemptToken, *inside* "+
				"s3.request.duration but isolated from "+
				"server-side latency. Sub-microsecond when the "+
				"bucket has tokens (steady state); rising p99 "+
				"means the SDK has shrunk the bucket in response "+
				"to recent SlowDown errors and is now actively "+
				"throttling outgoing attempts. Pair with "+
				"s3.attempt.error.count{error_type=\"slowdown\"} "+
				"to confirm the cause."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(shortWaitBuckets...),
	); err != nil {
		return nil, err
	}
	if m.tcpConnections, err = meter.Int64UpDownCounter(
		"s3pgstore.s3.tcp.connections",
		metric.WithDescription(
			"Current count of open TCP sockets to S3 (active + "+
				"idle in pool). Up-down counter driven by the "+
				"wrapped Dialer (+1 per successful dial) and "+
				"wrapped Conn.Close (-1, idempotent against "+
				"double-Close). Direct saturation signal vs the "+
				"configured MaxOpenConnections — when this gauge "+
				"sits at the cap during steady-state load, raise "+
				"the cap (or accept it and ensure callers throttle "+
				"their fan-out). Drift (gauge climbs without "+
				"load) indicates a Conn leak."),
		metric.WithUnit("{connection}"),
	); err != nil {
		return nil, err
	}
	if m.connectionReuse, err = meter.Int64Counter(
		"s3pgstore.s3.connection.reuse.count",
		metric.WithDescription(
			"Total HTTP requests, labeled by reused (true = the "+
				"request grabbed a connection from the idle "+
				"pool; false = had to dial fresh). Sourced from "+
				"httptrace.GotConn. The rate ratio "+
				"(reused=\"true\" / sum) is the idle-pool hit "+
				"rate — high (>0.95) under steady load means "+
				"the pool is sized well; low means either "+
				"MaxIdleConns starves the pool or the workload "+
				"bursts faster than IdleConnTimeout."),
		metric.WithUnit("{request}"),
	); err != nil {
		return nil, err
	}
	return m, nil
}

// recordOp records s3.request.duration + s3.request.count for
// one logical S3 op. Per-attempt error.type lives in
// s3.attempt.error.count, not here.
func (m *s3metrics) recordOp(
	ctx context.Context, op string,
	duration time.Duration, attempts int, err error,
) {
	if m == nil {
		return
	}
	outcome, _ := classifyS3Error(err)
	opAttr := attribute.String(attrKeyOperation, op)
	m.requestDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(opAttr))
	m.requestCount.Add(ctx, 1, metric.WithAttributes(
		opAttr,
		attribute.String(attrKeyOutcome, outcome),
		attribute.Int(attrKeyAttempts, attempts),
	))
}

// recordAttemptError fires once per failed attempt — retried
// OR terminal. Operators query a single counter for 'rate of
// <error_type> events' without summing two metrics. attempt
// is 1-based.
func (m *s3metrics) recordAttemptError(
	ctx context.Context, op string, attempt int,
	err error, terminal bool,
) {
	if m == nil {
		return
	}
	_, errType := classifyS3Error(err)
	m.attemptError.Add(ctx, 1, metric.WithAttributes(
		attribute.String(attrKeyOperation, op),
		attribute.String(attrKeyErrorType, errType),
		attribute.Int(attrKeyAttempt, attempt),
		attribute.Bool(attrKeyTerminal, terminal),
	))
}

// recordBodySize records the wire bytes for the op (PUT
// request body or GET response body) on success. Skipped
// when bytes <= 0 (DELETE has no body).
func (m *s3metrics) recordBodySize(
	ctx context.Context, op string, bytes int64,
) {
	if m == nil || bytes <= 0 {
		return
	}
	m.bodySize.Record(ctx, bytes, metric.WithAttributes(
		attribute.String(attrKeyOperation, op)))
}

// recordRatelimitWait records the time the client-side rate
// limiter held this op before letting it proceed. Sub-ms
// observations are normal at low load; rising p99 means the
// configured MaxRequestsPerSecond cap is now the bottleneck.
func (m *s3metrics) recordRatelimitWait(
	ctx context.Context, op string, waited time.Duration,
) {
	if m == nil {
		return
	}
	m.ratelimitWait.Record(ctx, waited.Seconds(),
		metric.WithAttributes(
			attribute.String(attrKeyOperation, op)))
}

// recordAdaptiveRetryWait records the wallclock the SDK's
// adaptive-mode token bucket held one attempt waiting for a
// token. Steady state is sub-microsecond; rising p99 means the
// adaptive retrier has shrunk the bucket and is now throttling.
// Skips when waited <= 0 to avoid recording the no-op fast path
// as a histogram observation that would skew low-percentile
// buckets.
func (m *s3metrics) recordAdaptiveRetryWait(
	ctx context.Context, op string, waited time.Duration,
) {
	if m == nil || waited <= 0 {
		return
	}
	m.adaptiveRetryWait.Record(ctx, waited.Seconds(),
		metric.WithAttributes(
			attribute.String(attrKeyOperation, op)))
}

// recordTCPConnOpen ticks the open-conn gauge up by one after
// a successful TCP dial from the wrapped Dialer.
func (m *s3metrics) recordTCPConnOpen(ctx context.Context) {
	if m == nil {
		return
	}
	m.tcpConnections.Add(ctx, 1)
}

// recordTCPConnClose ticks the open-conn gauge down by one
// when a wrapped net.Conn closes. The wrapper guards against
// double-Close so the gauge tracks the true socket count.
func (m *s3metrics) recordTCPConnClose(ctx context.Context) {
	if m == nil {
		return
	}
	m.tcpConnections.Add(ctx, -1)
}

// recordConnectionReuse fires once per HTTP request from
// httptrace.GotConn — reused=true means the request grabbed
// an idle pool entry, false means it dialed fresh.
func (m *s3metrics) recordConnectionReuse(
	ctx context.Context, reused bool,
) {
	if m == nil {
		return
	}
	m.connectionReuse.Add(ctx, 1, metric.WithAttributes(
		attribute.Bool(attrKeyReused, reused)))
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
