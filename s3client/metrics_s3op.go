package s3client

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithymw "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// opCtx is the per-operation scratch space the metrics
// middleware uses to communicate between Initialize (where we
// can read input.Body for PUT body size) and Deserialize (where
// we can read response Content-Length for GET body size). The
// metrics middleware constructs it at Initialize.Before; the
// body-size output middleware writes the GET response size
// before Deserialize returns.
type opCtx struct {
	bodySize atomic.Int64 // PUT input length OR GET response Content-Length
}

type opCtxKeyT struct{}

//nolint:gochecknoglobals // context-key sentinel; never mutated
var opCtxKey = opCtxKeyT{}

func opCtxFrom(ctx context.Context) (*opCtx, bool) {
	oc, ok := ctx.Value(opCtxKey).(*opCtx)
	return oc, ok
}

// metricsMiddleware is the single middleware that emits every
// per-op s3pgstore.s3.* instrument. Lives at Initialize.Before
// — the outermost wrap, so it observes the entire SDK lifecycle
// (including all SDK-managed retries) as one logical operation.
//
// On exit it walks awsmiddleware.GetAttemptResults to:
//
//   - Count attempts (used as the s3.request.count "attempts"
//     label).
//   - Emit one recordAttemptError per failed attempt — both
//     retried (terminal=false) and the final budget-exhausted
//     one (terminal=true). A single counter answers
//     "rate of <error_type> events" without summing two metrics.
//
// Body-size capture for PUT happens here (input.Body is a
// *bytes.Reader at Initialize-time, before the SDK reads it).
// The GET response Content-Length is captured by the
// bodySizeOutputMiddleware at Deserialize.After and stashed in
// opCtx so this middleware can record it on op success.
type metricsMiddleware struct {
	m *s3metrics
}

// ID implements smithy-go middleware.InitializeMiddleware.
func (metricsMiddleware) ID() string { return "s3pgstore.metrics" }

// HandleInitialize implements smithy-go middleware.InitializeMiddleware.
func (m metricsMiddleware) HandleInitialize(
	ctx context.Context,
	in smithymw.InitializeInput,
	next smithymw.InitializeHandler,
) (smithymw.InitializeOutput, smithymw.Metadata, error) {
	op := awsmiddleware.GetOperationName(ctx)

	oc := &opCtx{}
	ctx = context.WithValue(ctx, opCtxKey, oc)

	// PUT body size — read once at Initialize. The body is a
	// *bytes.Reader for every PUT s3pgstore issues; the SDK
	// rewinds it across SDK-level retries via io.Seeker, so the
	// length is stable and capturing it here once is correct.
	if put, ok := in.Parameters.(*awss3.PutObjectInput); ok {
		if br, ok := put.Body.(*bytes.Reader); ok {
			oc.bodySize.Store(int64(br.Len()))
		}
	}

	start := time.Now()
	out, md, err := next.HandleInitialize(ctx, in)
	duration := time.Since(start)

	// Walk AttemptResults to surface per-attempt errors.
	// The SDK records one entry per HTTP attempt. We fire on
	// every entry whose Err is non-nil, marking the last one
	// terminal=true (it exhausted the retry budget or was a
	// non-retryable terminal failure) and earlier ones
	// terminal=false (the SDK retried them). One row per
	// failed attempt — operators query a single counter for
	// "rate of <error_type> events".
	attempts := 1
	firedTerminal := false
	if results, ok := retry.GetAttemptResults(md); ok {
		if n := len(results.Results); n > 0 {
			attempts = n
			for i, r := range results.Results {
				if r.Err == nil {
					continue
				}
				terminal := i == n-1
				m.m.recordAttemptError(ctx, op,
					i+1, r.Err, terminal)
				if terminal {
					firedTerminal = true
				}
			}
		}
	}
	// Edge case: middleware-layer failure before the retry
	// loop ran (e.g. the rate-limiter middleware returning a
	// ctx-cancel error) leaves AttemptResults empty but err
	// non-nil. Surface it as a single terminal attempt error
	// so request.count{outcome=error} always has a matching
	// attempt.error.count row.
	if err != nil && !firedTerminal {
		m.m.recordAttemptError(ctx, op, attempts, err, true)
	}

	m.m.recordOp(ctx, op, duration, attempts, err)
	if err == nil {
		if size := oc.bodySize.Load(); size > 0 {
			m.m.recordBodySize(ctx, op, size)
		}
	}
	return out, md, err
}

// bodySizeOutputMiddleware captures GET response Content-Length
// at Deserialize.After and writes it to opCtx.bodySize so the
// metrics middleware's deferred recordBodySize reports it on
// success. Separate middleware because the response body is
// only available at Deserialize step.
type bodySizeOutputMiddleware struct{}

// ID implements smithy-go middleware.DeserializeMiddleware.
func (bodySizeOutputMiddleware) ID() string {
	return "s3pgstore.bodysize.output"
}

// HandleDeserialize implements smithy-go middleware.DeserializeMiddleware.
func (bodySizeOutputMiddleware) HandleDeserialize(
	ctx context.Context,
	in smithymw.DeserializeInput,
	next smithymw.DeserializeHandler,
) (smithymw.DeserializeOutput, smithymw.Metadata, error) {
	out, md, err := next.HandleDeserialize(ctx, in)
	if err != nil {
		return out, md, err
	}
	// Only act on GetObject — PUT body size is captured at
	// Initialize, DELETE has no body.
	if awsmiddleware.GetOperationName(ctx) != "GetObject" {
		return out, md, nil
	}
	if resp, ok := out.RawResponse.(*smithyhttp.Response); ok &&
		resp.ContentLength > 0 {
		if oc, ok := opCtxFrom(ctx); ok {
			oc.bodySize.Store(resp.ContentLength)
		}
	}
	return out, md, nil
}
