package s3client

import (
	"context"
	"errors"
	"testing"
	"time"

	smithymw "github.com/aws/smithy-go/middleware"
	"golang.org/x/time/rate"
)

// TestRateLimiterMiddleware_Throttles verifies the limiter
// enforces its configured rate. With limit=10/sec + burst=2,
// the third request in a burst must wait ~100 ms (1/10 sec)
// after the first two consumed the burst tokens.
func TestRateLimiterMiddleware_Throttles(t *testing.T) {
	mw := rateLimiterMiddleware{
		m:       nil,
		limiter: rate.NewLimiter(10, 2),
	}
	next := smithymw.InitializeHandlerFunc(func(
		ctx context.Context, in smithymw.InitializeInput,
	) (smithymw.InitializeOutput, smithymw.Metadata, error) {
		return smithymw.InitializeOutput{}, smithymw.Metadata{}, nil
	})

	ctx := context.Background()
	start := time.Now()
	for range 3 {
		_, _, err := mw.HandleInitialize(
			ctx, smithymw.InitializeInput{}, next)
		if err != nil {
			t.Fatalf("HandleInitialize: %v", err)
		}
	}
	elapsed := time.Since(start)
	// First two are burst tokens (instant); third waits ~100ms.
	// Allow some slack for scheduling jitter on CI runners.
	if elapsed < 80*time.Millisecond {
		t.Errorf("3 ops at 10rps + burst=2: want >=80ms, got %v",
			elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("3 ops at 10rps + burst=2: want <500ms, got %v "+
			"(scheduling pressure?)", elapsed)
	}
}

// TestRateLimiterMiddleware_RespectsCtxCancel verifies that a
// ctx cancellation during the wait surfaces as ctx.Err and the
// downstream handler is never invoked. Important so a caller
// that cancels mid-wait doesn't see a phantom S3 op fire after.
func TestRateLimiterMiddleware_RespectsCtxCancel(t *testing.T) {
	// Tight limit: 1/sec + burst=1, so the second op must wait ~1s.
	mw := rateLimiterMiddleware{
		m:       nil,
		limiter: rate.NewLimiter(1, 1),
	}
	called := 0
	next := smithymw.InitializeHandlerFunc(func(
		ctx context.Context, in smithymw.InitializeInput,
	) (smithymw.InitializeOutput, smithymw.Metadata, error) {
		called++
		return smithymw.InitializeOutput{}, smithymw.Metadata{}, nil
	})

	ctx := context.Background()
	if _, _, err := mw.HandleInitialize(
		ctx, smithymw.InitializeInput{}, next); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if called != 1 {
		t.Fatalf("first call: handler invocations = %d, want 1", called)
	}

	// Second call must wait — cancel before it gets a token.
	ctx2, cancel := context.WithTimeout(
		context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := mw.HandleInitialize(
		ctx2, smithymw.InitializeInput{}, next)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("second call: want DeadlineExceeded, got %v", err)
	}
	if called != 1 {
		t.Errorf("second call: handler invocations = %d, want 1 "+
			"(downstream must not run when ctx-cancelled)",
			called)
	}
}
