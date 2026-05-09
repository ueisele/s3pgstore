package s3client

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
)

// instrumentedRetryer wraps an aws.RetryerV2 to time
// GetAttemptToken — the only RetryerV2 method that blocks. For
// the adaptive-mode retryer this measures the wait the
// adaptive token bucket imposes on each attempt: sub-microsecond
// in steady state, rising into the millisecond+ range once the
// bucket has shrunk in response to SlowDown errors. The
// resulting s3.adaptive_retry.wait.duration histogram is the
// only direct OTel signal that the SDK is actively throttling
// (the SDK exposes no public hook into the bucket's state).
//
// Other RetryerV2 methods (IsErrorRetryable, MaxAttempts,
// RetryDelay, GetRetryToken, GetInitialToken) are forwarded
// verbatim — they don't block, so timing them would just emit
// flat-zero histogram points.
type instrumentedRetryer struct {
	aws.RetryerV2
	m *s3metrics
}

// GetAttemptToken delegates to the wrapped retryer, recording
// the wait duration into s3.adaptive_retry.wait.duration. The
// op name is sourced from the SDK middleware-context the same
// way the metrics middleware does it; if the context lacks one
// (no in-flight op) the recorder skips because waited <= 0
// would drop the sample anyway.
func (r instrumentedRetryer) GetAttemptToken(
	ctx context.Context,
) (func(error) error, error) {
	start := time.Now()
	release, err := r.RetryerV2.GetAttemptToken(ctx)
	r.m.recordAdaptiveRetryWait(ctx,
		awsmiddleware.GetOperationName(ctx),
		time.Since(start))
	return release, err
}
