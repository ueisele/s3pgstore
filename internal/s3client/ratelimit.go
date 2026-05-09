package s3client

import (
	"context"
	"time"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	smithymw "github.com/aws/smithy-go/middleware"
	"golang.org/x/time/rate"
)

// rateLimiterMiddleware pre-throttles outgoing S3 ops at a
// fixed per-second rate via a token-bucket. Sits at
// Initialize.Before, *outside* the SDK retry loop AND outside
// the metrics middleware — so request.duration measures only
// the SDK work (server-driven latency), not how long the
// caller spent self-throttling. The wait time itself is
// surfaced separately via s3metrics.recordRatelimitWait so
// operators can observe self-throttle saturation without it
// polluting the SDK-side latency histogram.
//
// Used when the operator knows the backend's RPS limit
// up-front (STACKIT 2500 RPS, AWS S3 per-prefix 5500 GET /
// 3500 PUT). Pre-shaping the rate avoids the adaptive-retry
// "discover via SlowDown" feedback loop that would otherwise
// keep attempt.error.count{error_type="slowdown"} ticking
// in steady state.
type rateLimiterMiddleware struct {
	limiter *rate.Limiter
	m       *s3metrics
}

// ID implements smithy-go middleware.InitializeMiddleware.
func (rateLimiterMiddleware) ID() string {
	return "s3pgstore.ratelimit"
}

// HandleInitialize implements smithy-go middleware.InitializeMiddleware.
func (m rateLimiterMiddleware) HandleInitialize(
	ctx context.Context,
	in smithymw.InitializeInput,
	next smithymw.InitializeHandler,
) (smithymw.InitializeOutput, smithymw.Metadata, error) {
	start := time.Now()
	// Wait blocks until a token is available, ctx is done, or
	// the would-exceed-deadline fast-path fires. rate.Limiter
	// returns its own synthetic "would exceed context deadline"
	// error in the fast-path case; we collapse that to ctx.Err()
	// (or context.DeadlineExceeded if ctx isn't yet done at the
	// instant of the check) so callers see the standard Go
	// cancellation error rather than a package-internal string.
	if err := m.limiter.Wait(ctx); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return smithymw.InitializeOutput{},
				smithymw.Metadata{}, ctxErr
		}
		return smithymw.InitializeOutput{},
			smithymw.Metadata{}, context.DeadlineExceeded
	}
	// Record only on success — cancelled waits would skew the
	// histogram toward truncation artefacts.
	m.m.recordRatelimitWait(ctx,
		awsmiddleware.GetOperationName(ctx), time.Since(start))
	return next.HandleInitialize(ctx, in)
}
