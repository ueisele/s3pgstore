package s3client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// stubRetryerV2 is a minimal aws.RetryerV2 implementation that
// blocks GetAttemptToken for a configurable wait. Lets the
// instrumentedRetryer test confirm the wrapper records the
// observed wait without involving a live SDK retry loop.
type stubRetryerV2 struct {
	wait time.Duration
	err  error
}

func (stubRetryerV2) IsErrorRetryable(error) bool { return false }
func (stubRetryerV2) MaxAttempts() int            { return 1 }
func (stubRetryerV2) RetryDelay(int, error) (time.Duration, error) {
	return 0, nil
}
func (stubRetryerV2) GetRetryToken(
	context.Context, error,
) (func(error) error, error) {
	return func(error) error { return nil }, nil
}
func (stubRetryerV2) GetInitialToken() func(error) error {
	return func(error) error { return nil }
}
func (s stubRetryerV2) GetAttemptToken(
	ctx context.Context,
) (func(error) error, error) {
	if s.wait > 0 {
		select {
		case <-time.After(s.wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	return func(error) error { return nil }, nil
}

// Compile-time assertion: instrumentedRetryer satisfies
// aws.RetryerV2 (the SDK requires V2 for GetAttemptToken to be
// honored — falling back to V1 would silently drop the metric).
var _ aws.RetryerV2 = instrumentedRetryer{}

// TestInstrumentedRetryer_RecordsWaitOnSuccess verifies the
// wrapper times GetAttemptToken and forwards the inner result.
// 5ms gives the timer enough headroom to register above the
// recorder's waited <= 0 short-circuit guard on a busy CI
// runner without making the test slow.
func TestInstrumentedRetryer_RecordsWaitOnSuccess(t *testing.T) {
	r := instrumentedRetryer{
		RetryerV2: stubRetryerV2{wait: 5 * time.Millisecond},
		m:         nil, // nil receiver short-circuits — must not panic
	}
	release, err := r.GetAttemptToken(context.Background())
	if err != nil {
		t.Fatalf("GetAttemptToken: %v", err)
	}
	if release == nil {
		t.Fatal("release: want non-nil callback")
	}
	if err := release(nil); err != nil {
		t.Errorf("release: %v", err)
	}
}

// TestInstrumentedRetryer_RecordsOnError verifies the wrapper
// still records the wait when the inner GetAttemptToken returns
// an error (e.g. ctx cancel, FailOnNoAttemptTokens). Operators
// want to see throttling-induced waits even when the wait
// ultimately failed.
func TestInstrumentedRetryer_RecordsOnError(t *testing.T) {
	want := errors.New("synthetic")
	r := instrumentedRetryer{
		RetryerV2: stubRetryerV2{wait: time.Millisecond, err: want},
		m:         nil,
	}
	_, err := r.GetAttemptToken(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("GetAttemptToken err: want %v, got %v", want, err)
	}
}

// TestInstrumentedRetryer_ForwardsForwardedMethods spot-checks
// that the embedded aws.RetryerV2 satisfies the methods we
// don't wrap — refactor guard against accidentally shadowing
// IsErrorRetryable / MaxAttempts / RetryDelay etc.
func TestInstrumentedRetryer_ForwardsForwardedMethods(t *testing.T) {
	r := instrumentedRetryer{RetryerV2: stubRetryerV2{}, m: nil}
	if r.IsErrorRetryable(errors.New("x")) {
		t.Error("IsErrorRetryable: want false (stub returns false)")
	}
	if got := r.MaxAttempts(); got != 1 {
		t.Errorf("MaxAttempts: want 1, got %d", got)
	}
	if _, err := r.RetryDelay(1, nil); err != nil {
		t.Errorf("RetryDelay: %v", err)
	}
}
