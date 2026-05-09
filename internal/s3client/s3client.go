// Package s3client builds the *http.Client and *s3.Client used
// by both the s3pgstore library and the operator binaries. The
// returned *s3.Client carries:
//
//   - aws.RetryModeAdaptive (5 attempts) for SDK-managed retry +
//     adaptive token-bucket rate shaping. Throttle responses
//     (HTTP 429 / HTTP 503 SlowDown) shrink the per-client send
//     rate; successes grow it back. The token bucket pre-throttles
//     outgoing requests so we don't keep hammering a backend that
//     just told us to slow down.
//   - The s3pgstore metrics middleware that emits
//     s3pgstore.s3.{request.duration,request.count,
//     attempt.error.count,body_size}, sourcing per-attempt
//     classifications from awsmiddleware.GetAttemptResults.
//   - StorageGRID-friendly ResponseChecksumValidation =
//     WhenRequired (avoids the "missing body checksum" warning
//     StorageGRID logs on every GET under the SDK default).
//   - HTTP transport tuning sized to maxOpen so connection pool
//     limits match the caller's intended global concurrency cap.
//
// Concurrency strategy (post-cleanup):
//
//   - Hard global cap on TCP sockets to S3 → http.Transport's
//     MaxConnsPerHost / MaxIdleConnsPerHost / MaxIdleConns, all
//     equal to maxOpen. This is the only enforcement layer for
//     "no more than N concurrent S3 ops" — there is no per-op
//     semaphore middleware.
//   - Per-second rate shaping → adaptive retry's token bucket.
//   - Per-method goroutine pool sizing → caller's responsibility
//     (s3pgstore Config.S3MaxConcurrentOpsPerMethod, used by the
//     library's FanOut callers).
package s3client

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
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

// Options captures every knob BuildS3Client exposes.
type Options struct {
	// Region is the AWS region passed to LoadDefaultConfig.
	// Empty → "us-east-1" (matches AWS SDK convention for
	// non-region-aware S3-compatible backends like MinIO).
	Region string

	// Endpoint, when non-empty, overrides the AWS endpoint —
	// required for MinIO, StorageGRID, and other S3-compatible
	// backends. Triggers UsePathStyle so bucket names are not
	// promoted into the hostname.
	Endpoint string

	// MaxOpenConnections caps the *http.Client's connection
	// pool. This is the global concurrency ceiling for every
	// goroutine sharing the returned *s3.Client. Set to your
	// share of the backend's per-client limit (e.g. STACKIT
	// 500 concurrent / N replicas with ~10% headroom). Zero →
	// defaultMaxOpenConnections (64). To run effectively
	// unlimited, set a large value like 1000.
	MaxOpenConnections int

	// MaxRetryAttempts is the SDK retry budget per logical S3
	// op (1 initial + N-1 retries). Combined with the
	// equal-jitter backoff schedule (100ms..10s window),
	// worst-case wallclock for an exhausted budget is ~6s at
	// MaxRetryAttempts=5. Zero → defaultMaxRetryAttempts (5).
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

// newAdaptiveRetryer constructs the SDK adaptive-mode retryer
// configured with the s3pgstore equal-jitter backoff and the
// supplied max-attempts budget, wrapped with instrumentedRetryer
// so the adaptive token bucket's wait time emits as
// s3.adaptive_retry.wait.duration. Used by BuildS3Client and
// WrapS3Client; exposed so that callers reconstructing the
// retryer for tests / customisation get the same shape.
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

// middlewareOptions captures the per-client middleware-stack
// configuration appendMiddlewares needs. Separated from the
// top-level Options struct because the same shape is needed by
// both BuildS3Client (which constructs its own awsConfig) and
// WrapS3Client (which inherits the caller's awsConfig).
type middlewareOptions struct {
	// metrics is the s3metrics handle the middleware records
	// into. Nil → middleware short-circuits (every record* call
	// is a no-op on a nil receiver).
	metrics *s3metrics

	// MaxRequestsPerSecond installs a client-side token-bucket
	// rate limiter. <= 0 → no limiter installed.
	MaxRequestsPerSecond float64

	// MaxRequestsPerSecondBurst sizes the token bucket's burst.
	// Only meaningful when MaxRequestsPerSecond > 0; <= 0 with
	// a non-zero rate falls back to a small default.
	MaxRequestsPerSecondBurst int
}

// appendMiddlewares returns an APIOption-shaped function that
// installs the s3pgstore middleware stack onto an s3.Client.
// Today that's:
//
//   - The optional rate-limiter middleware (Initialize.Before,
//     outermost) when MaxRequestsPerSecond > 0.
//   - The metrics middleware (Initialize.Before) emitting
//     request.duration / request.count / attempt.error.count /
//     body_size.
//   - The body-size output middleware (Deserialize.After) that
//     captures GET response Content-Length.
//
// Order at Initialize: rate-limit → metrics → SDK retry → ...
// so request.duration measures only SDK-driven latency, not
// time spent self-throttling.
//
// Used by BuildS3Client (operator binaries) and by the library
// when wrapping a user-supplied *s3.Client.
func appendMiddlewares(opts middlewareOptions) func(*smithymw.Stack) error {
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
	return func(stack *smithymw.Stack) error {
		// Metrics first — Add(Before) prepends, so the next
		// rate-limit Add(Before) lands in front of metrics,
		// giving execution order [ratelimit, metrics, ...].
		if err := stack.Initialize.Add(
			metricsMiddleware{m: opts.metrics},
			smithymw.Before,
		); err != nil {
			return err
		}
		if limiter != nil {
			if err := stack.Initialize.Add(
				rateLimiterMiddleware{limiter: limiter, m: opts.metrics},
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
}

// BuildS3Client constructs an AWS SDK v2 *s3.Client with
// adaptive retry, the s3pgstore metrics middleware,
// StorageGRID-friendly checksum policy, and a tuned HTTP
// transport. See the package doc for the full rationale.
//
// Credentials follow the standard AWS SDK chain (env vars,
// shared credentials file, IRSA, IMDS).
func BuildS3Client(
	ctx context.Context, opts Options,
) (*awss3.Client, error) {
	region := opts.Region
	if region == "" {
		region = "us-east-1"
	}
	maxOpen := opts.MaxOpenConnections
	if maxOpen <= 0 {
		maxOpen = defaultMaxOpenConnections
	}

	m, err := newS3Metrics(opts.Meter)
	if err != nil {
		return nil, fmt.Errorf("register s3 metrics: %w", err)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithHTTPClient(buildHTTPClient(maxOpen, m)),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	mwFn := appendMiddlewares(middlewareOptions{
		metrics:                   m,
		MaxRequestsPerSecond:      opts.MaxRequestsPerSecond,
		MaxRequestsPerSecondBurst: opts.MaxRequestsPerSecondBurst,
	})
	return awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		// Direct assignment mirrors WrapS3Client; both
		// construct exactly one s3.Client per call, so the
		// factory pattern at awsconfig level wouldn't earn
		// its keep — and it would cost one wasted default-
		// retryer instantiation that we'd immediately
		// overwrite.
		o.Retryer = newAdaptiveRetryer(opts.MaxRetryAttempts, m)
		o.ResponseChecksumValidation =
			aws.ResponseChecksumValidationWhenRequired
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
			o.UsePathStyle = true
		}
		o.APIOptions = append(o.APIOptions, mwFn)
	}), nil
}

// WrapOptions is the per-wrap configuration WrapS3Client
// applies on top of the base client's Options(). Bundles the
// retry budget, optional rate limiter, and meter so the
// library's store.go has a single struct to populate.
type WrapOptions struct {
	// MaxRetryAttempts — see Options.MaxRetryAttempts.
	MaxRetryAttempts int

	// MaxRequestsPerSecond — see Options.MaxRequestsPerSecond.
	MaxRequestsPerSecond float64

	// MaxRequestsPerSecondBurst — see Options.MaxRequestsPerSecondBurst.
	MaxRequestsPerSecondBurst int

	// Meter — see Options.Meter.
	Meter metric.Meter
}

// WrapS3Client returns a new *s3.Client derived from base
// (preserving credentials, endpoint, region, and any other
// Options the caller configured) but with adaptive retry +
// equal-jitter backoff + optional rate limiter + s3pgstore
// metrics middleware installed.
//
// The original client is not mutated — wrapping is by-value via
// the SDK's Options(). The returned client is the one the
// library's read/write paths use; the user's original stays
// available for any other S3 work they may want to do (sharing
// the same underlying *http.Client, credentials, etc.).
//
// Note: this overrides the base client's Retryer with our
// adaptive-mode configuration + equal-jitter backoff. If the
// caller wired their own Retryer for unrelated reasons, that
// customization is lost — the library's retry policy is
// load-bearing for STACKIT-style throttling and we can't
// compose it onto an unknown one.
func WrapS3Client(
	base *awss3.Client, opts WrapOptions,
) (*awss3.Client, error) {
	m, err := newS3Metrics(opts.Meter)
	if err != nil {
		return nil, fmt.Errorf("register s3 metrics: %w", err)
	}
	mwFn := appendMiddlewares(middlewareOptions{
		metrics:                   m,
		MaxRequestsPerSecond:      opts.MaxRequestsPerSecond,
		MaxRequestsPerSecondBurst: opts.MaxRequestsPerSecondBurst,
	})
	return awss3.New(base.Options(), func(o *awss3.Options) {
		o.Retryer = newAdaptiveRetryer(opts.MaxRetryAttempts, m)
		o.APIOptions = append(o.APIOptions, mwFn)
	}), nil
}
