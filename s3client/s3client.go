// Package s3client provides the s3pgstore middleware stack as a
// drop-in option for awss3.NewFromConfig. The package's only
// public surface is Options + WithDefaults; everything else
// (metrics handle, middleware types, retryer wrap) is unexported.
//
// Typical use:
//
//	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
//	    awsconfig.WithRegion("us-east-1"))
//	if err != nil { return err }
//	client := awss3.NewFromConfig(awsCfg,
//	    s3client.WithDefaults(s3client.Options{
//	        MaxOpenConnections:   200,
//	        MaxRetryAttempts:     5,
//	        MaxRequestsPerSecond: 250,
//	        Meter:                meter,
//	    }),
//	)
//
// Override anything WithDefaults sets by appending another
// func(*awss3.Options) after it — the SDK applies optFns in
// order, so a later one wins:
//
//	client := awss3.NewFromConfig(awsCfg,
//	    s3client.WithDefaults(opts),
//	    func(o *awss3.Options) {
//	        o.BaseEndpoint = aws.String("https://minio.local:9000")
//	        o.UsePathStyle = true
//	    },
//	)
//
// What WithDefaults installs:
//
//   - aws.RetryModeAdaptive (default 5 attempts) wrapped with
//     instrumentedRetryer so the adaptive token bucket's wait
//     time emits as s3pgstore.s3.adaptive_retry.wait.duration.
//     SlowDown / 429 / 503 responses shrink the per-client send
//     rate; successes grow it back.
//   - The s3pgstore metrics middleware emitting
//     s3pgstore.s3.{request.duration,request.count,
//     attempt.error.count,body_size,ratelimit.wait.duration},
//     sourcing per-attempt classifications from
//     awsmiddleware.GetAttemptResults.
//   - The optional client-side rate-limiter middleware (when
//     MaxRequestsPerSecond > 0).
//   - StorageGRID-friendly ResponseChecksumValidation =
//     WhenRequired (avoids the "missing body checksum" warning
//     StorageGRID logs on every GET under the SDK default).
//   - An *http.Client whose Transport caps MaxConnsPerHost /
//     MaxIdleConnsPerHost / MaxIdleConns at MaxOpenConnections,
//     wrapped with httptrace + a tracking dialer to feed
//     s3pgstore.s3.tcp.connections and
//     s3pgstore.s3.connection.reuse.count.
//   - UsePathStyle, when Options.UsePathStyle is true (default
//     false leaves the SDK virtual-hosted-style behaviour
//     untouched). No AWS env var exposes this knob; surfacing
//     it on Options keeps the env-driven setup self-contained.
//
// Concurrency strategy:
//
//   - Hard global cap on TCP sockets to S3 → http.Transport's
//     MaxConnsPerHost / MaxIdleConnsPerHost / MaxIdleConns, all
//     equal to MaxOpenConnections. This is the only enforcement
//     layer for "no more than N concurrent S3 ops" — there is no
//     per-op semaphore middleware.
//   - Per-second rate shaping → rateLimiterMiddleware (when
//     MaxRequestsPerSecond > 0) plus adaptive retry's reactive
//     token bucket.
//   - Per-method goroutine pool sizing → caller's responsibility
//     (s3pgstore Config.WorkerPool, the shared pool every
//     fan-out call site submits S3 ops to).
package s3client

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithymw "github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/time/rate"
)

const (
	// defaultMaxOpenConnections is the fallback when callers
	// don't size MaxOpenConnections. 64 is a sensible modern
	// default for S3 workloads — high enough that typical
	// fan-out (parquet writes across N partitions, multi-file
	// reads) isn't immediately bottlenecked, low enough to
	// stay well under default OS file-descriptor limits.
	// Operators wanting "effectively unlimited" set this to a
	// large value (e.g. 1000).
	defaultMaxOpenConnections = 64

	// defaultMaxRetryAttempts is the SDK-level retry budget
	// when callers don't override Options.MaxRetryAttempts.
	// 5 = 1 initial + 4 retries; matches the historical
	// s3target retry policy.
	defaultMaxRetryAttempts = 5

	dialTimeout           = 10 * time.Second
	dialKeepAlive         = 30 * time.Second
	idleConnTimeout       = 90 * time.Second
	tlsHandshakeTimeout   = 10 * time.Second
	expectContinueTimeout = 1 * time.Second
)

// Options captures every knob WithDefaults exposes. Region,
// endpoint, credentials, and other AWS-layer concerns are not
// here — those belong on aws.Config (or on awss3.Options via a
// later optFn). The s3client surface is intentionally narrow:
// concurrency cap, retry budget, optional rate limit, meter.
type Options struct {
	// MaxOpenConnections caps the *http.Client's connection
	// pool. This is the global concurrency ceiling for every
	// goroutine sharing the returned *s3.Client. Set to your
	// share of the backend's per-client limit (e.g. STACKIT
	// 500 concurrent / N replicas with ~10% headroom). Zero →
	// 64 (defaultMaxOpenConnections). To run effectively
	// unlimited, set a large value like 1000.
	MaxOpenConnections int

	// MaxRetryAttempts is the SDK retry budget per logical S3
	// op (1 initial + N-1 retries). Combined with the
	// equal-jitter backoff schedule (100ms..10s window),
	// worst-case wallclock for an exhausted budget is ~6s at
	// MaxRetryAttempts=5. Zero → 5 (defaultMaxRetryAttempts).
	MaxRetryAttempts int

	// MaxRequestsPerSecond pre-throttles outgoing S3 ops at
	// this fixed rate via a token-bucket middleware. Set to
	// your share of the backend's per-second limit (e.g.
	// STACKIT 2500 RPS ÷ N replicas with ~10% headroom).
	// Zero or negative → no rate limiter installed (rely
	// solely on adaptive retry's reactive bucket).
	MaxRequestsPerSecond float64

	// MaxRequestsPerSecondBurst sizes the token bucket's
	// burst capacity. Zero defaults to max(1, rate * 0.1) —
	// 10% of rate, capping the post-idle 1-second excursion
	// at +10% over the sustained limit. Only meaningful when
	// MaxRequestsPerSecond > 0.
	MaxRequestsPerSecondBurst int

	// UsePathStyle selects path-style URLs
	// (https://endpoint/bucket/key) over the SDK default
	// virtual-hosted-style (https://bucket.endpoint/key).
	// Required only when the endpoint hostname can't carry a
	// bucket subdomain — local MinIO at localhost, IP-based
	// endpoints, or backends that disable virtual-hosted-style.
	// STACKIT, Cloudflare R2, StorageGRID, and AWS with proper
	// DNS work over the SDK default; leave false there.
	//
	// On Options because no AWS env var exposes it (unlike
	// region / endpoint / credentials, which AWS already wires
	// via aws.Config and the LoadDefaultConfig env-var chain).
	UsePathStyle bool

	// Meter is the OTel meter the s3pgstore.s3.* instruments
	// register against. Nil falls back to
	// otel.GetMeterProvider().Meter(...) — wires telemetry for
	// callers that installed a global provider via OTel
	// autoexport / explicit MeterProvider construction; stays
	// silent when no provider is installed (the global default
	// is itself a no-op meter). Same pattern the library's
	// Config.Meter follows.
	Meter metric.Meter
}

// WithDefaults returns a func(*awss3.Options) that installs the
// s3pgstore middleware stack onto an s3.Client constructed via
// awss3.NewFromConfig. Composes naturally with additional
// caller-supplied optFns: any optFn appended after WithDefaults
// can override anything WithDefaults sets (HTTPClient, Retryer,
// APIOptions, etc.).
//
// Caller overrides that silently disable instrumentation:
//
//   - HTTPClient → loses s3pgstore.s3.tcp.connections and
//     s3pgstore.s3.connection.reuse.count (httptrace and the
//     tracking dialer live in our *http.Client).
//   - Retryer → loses s3pgstore.s3.adaptive_retry.wait.duration
//     and the SlowDown coordination across goroutines that share
//     the client.
//   - APIOptions reset → loses every s3pgstore.s3.* metric
//     emitted by middleware (request.duration / request.count /
//     attempt.error.count / body_size / ratelimit.wait.duration).
//
// Panics on metric registration failure. The only realistic
// failure mode is a broken meter (duplicate-name registration
// with mismatched type), which is a developer/configuration
// error caught at startup, not a runtime condition. Same shape
// as regexp.MustCompile.
func WithDefaults(opts Options) func(*awss3.Options) {
	m, err := newS3Metrics(opts.Meter)
	if err != nil {
		panic(fmt.Sprintf(
			"s3pgstore/s3client: register s3 metrics: %v "+
				"(check your OTel meter — same instrument name "+
				"registered twice with different types?)", err))
	}

	var limiter *rate.Limiter
	if opts.MaxRequestsPerSecond > 0 {
		burst := opts.MaxRequestsPerSecondBurst
		if burst <= 0 {
			// Default burst = 10% of rate (with a floor of 1
			// for sub-10 rates). Conservative on purpose:
			// rate.Limiter's worst-case 1-second throughput
			// is `burst + rate`, so a burst of 10%-of-rate
			// caps the post-idle excursion at +10% over the
			// sustained limit. Operators on backends with
			// stricter windows tighten further by setting
			// MaxRequestsPerSecondBurst explicitly.
			burst = int(opts.MaxRequestsPerSecond / 10)
			if burst < 1 {
				burst = 1
			}
		}
		limiter = rate.NewLimiter(
			rate.Limit(opts.MaxRequestsPerSecond), burst)
	}

	mwFn := func(stack *smithymw.Stack) error {
		// Metrics first — Add(Before) prepends, so the next
		// rate-limit Add(Before) lands in front of metrics,
		// giving execution order [ratelimit, metrics, ...].
		if err := stack.Initialize.Add(
			metricsMiddleware{m: m},
			smithymw.Before,
		); err != nil {
			return err
		}
		if limiter != nil {
			if err := stack.Initialize.Add(
				rateLimiterMiddleware{limiter: limiter, m: m},
				smithymw.Before,
			); err != nil {
				return err
			}
		}
		// Body-size output mw at Deserialize.After — captures
		// GET response Content-Length into opCtx so the metrics
		// middleware can record it on op success.
		return stack.Deserialize.Add(
			bodySizeOutputMiddleware{},
			smithymw.After,
		)
	}

	return func(o *awss3.Options) {
		o.HTTPClient = buildHTTPClient(opts.MaxOpenConnections, m)
		o.Retryer = newAdaptiveRetryer(opts.MaxRetryAttempts, m)
		o.ResponseChecksumValidation =
			aws.ResponseChecksumValidationWhenRequired
		if opts.UsePathStyle {
			o.UsePathStyle = true
		}
		o.APIOptions = append(o.APIOptions, mwFn)
	}
}

// buildHTTPClient returns an *http.Client whose Transport caps
// MaxConnsPerHost / MaxIdleConnsPerHost / MaxIdleConns at
// maxOpen. This is the *only* enforcement layer for total S3
// concurrency post-cleanup (no semaphore middleware), so set
// maxOpen to the global limit you want to hold below the
// backend's per-client ceiling.
//
// HTTP/2 is intentionally disabled: the AWS SDK and most S3
// implementations (MinIO, StorageGRID) perform measurably
// better over HTTP/1.1 at high concurrency because of HTTP/2
// stream-throttling and connection-multiplexing quirks under
// AWS retry policies.
//
// The Dialer is wrapped to feed recordTCPConnOpen /
// recordTCPConnClose into m, surfacing the open-TCP gauge.
// The Transport is wrapped with tracingTransport, injecting an
// httptrace.ClientTrace so GotConn fires recordConnectionReuse
// per request. Pass nil for m when connection telemetry isn't
// wired (every record* call short-circuits on a nil receiver).
//
// maxOpen <= 0 → defaultMaxOpenConnections (64).
func buildHTTPClient(maxOpen int, m *s3metrics) *http.Client {
	if maxOpen <= 0 {
		maxOpen = defaultMaxOpenConnections
	}
	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: dialKeepAlive,
	}
	transport := &http.Transport{
		DialContext: trackedDialer{
			dialer: dialer, m: m,
		}.DialContext,
		MaxIdleConns:          maxOpen,
		MaxIdleConnsPerHost:   maxOpen,
		MaxConnsPerHost:       maxOpen,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ExpectContinueTimeout: expectContinueTimeout,
		ForceAttemptHTTP2:     false,
	}
	return &http.Client{
		Transport: tracingTransport{base: transport, m: m},
	}
}

// newAdaptiveRetryer constructs the SDK adaptive-mode retryer
// configured with the s3pgstore equal-jitter backoff and the
// supplied max-attempts budget, wrapped with instrumentedRetryer
// so the adaptive token bucket's wait time emits as
// s3.adaptive_retry.wait.duration.
//
// maxAttempts <= 0 → defaultMaxRetryAttempts. m may be nil; the
// recorder short-circuits on a nil receiver.
func newAdaptiveRetryer(maxAttempts int, m *s3metrics) aws.Retryer {
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxRetryAttempts
	}
	base := retry.NewAdaptiveMode(func(o *retry.AdaptiveModeOptions) {
		o.StandardOptions = append(o.StandardOptions,
			func(so *retry.StandardOptions) {
				so.MaxAttempts = maxAttempts
				so.Backoff = equalJitterBackoff{
					base: backoffBase,
					cap:  backoffCap,
				}
			},
		)
	})
	return instrumentedRetryer{RetryerV2: base, m: m}
}
