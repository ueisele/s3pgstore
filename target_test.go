package s3pgstore

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// transientHTTP returns an error that mimics the SDK's
// smithyhttp.ResponseError for the given HTTP status code.
// Used by retry tests to simulate transient and terminal
// failures without an HTTP client.
func transientHTTP(status int) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: status},
		},
		Err: errors.New("synthetic"),
	}
}

func TestIsTransientS3Error(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context cancelled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"500", transientHTTP(500), true},
		{"503", transientHTTP(503), true},
		{"429 SlowDown", transientHTTP(429), true},
		{"404 not found", transientHTTP(404), false},
		{"403 forbidden", transientHTTP(403), false},
		{"transport error", errors.New("dial tcp: connection refused"),
			true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransientS3Error(tc.err); got != tc.want {
				t.Errorf("isTransientS3Error(%v): want %v, got %v",
					tc.err, tc.want, got)
			}
		})
	}
}

// TestRetry_FastPath: fn succeeds on first attempt, no retries.
func TestRetry_FastPath(t *testing.T) {
	calls := 0
	err := retry(context.Background(), "test", nil, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls: want 1, got %d", calls)
	}
}

// TestRetry_TransientThenSuccess: fn fails twice transient,
// succeeds on the third attempt. With our backoff schedule the
// total sleep is ~400ms-800ms, kept low enough to not slow the
// test suite materially.
func TestRetry_TransientThenSuccess(t *testing.T) {
	var calls atomic.Int32
	err := retry(context.Background(), "test", nil, func() error {
		n := calls.Add(1)
		if n < 3 {
			return transientHTTP(503)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls: want 3, got %d", calls.Load())
	}
}

// TestRetry_TerminalError: a non-transient error returns
// immediately, no backoff.
func TestRetry_TerminalError(t *testing.T) {
	calls := 0
	want := transientHTTP(404) // 404 is not transient
	err := retry(context.Background(), "test", nil, func() error {
		calls++
		return want
	})
	if err == nil {
		t.Fatal("retry: want error, got nil")
	}
	if calls != 1 {
		t.Fatalf("calls: want 1, got %d", calls)
	}
}

// TestRetry_ExhaustedBudget: every attempt fails transient. The
// caller sees the last error after retryMaxAttempts attempts.
func TestRetry_ExhaustedBudget(t *testing.T) {
	calls := 0
	err := retry(context.Background(), "test", nil, func() error {
		calls++
		return transientHTTP(500)
	})
	if err == nil {
		t.Fatal("retry: want error, got nil")
	}
	if calls != retryMaxAttempts {
		t.Fatalf("calls: want %d, got %d", retryMaxAttempts, calls)
	}
}

// TestRetry_ContextCancellation: ctx cancelled mid-backoff
// returns ctx.Err() (not the underlying transient error).
func TestRetry_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled immediately
	err := retry(ctx, "test", nil, func() error {
		return transientHTTP(500)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry: want context.Canceled, got %v", err)
	}
}

// TestRetry_OnTransientCallback fires once per masked transient
// failure (every attempt that returns transient, even if a
// later attempt succeeds).
func TestRetry_OnTransientCallback(t *testing.T) {
	transients := 0
	calls := 0
	err := retry(context.Background(), "test",
		func(error) { transients++ },
		func() error {
			calls++
			if calls < 3 {
				return transientHTTP(503)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if transients != 2 {
		t.Fatalf("onTransient fire count: want 2, got %d", transients)
	}
}

func TestRetryBackoffRange_Pick(t *testing.T) {
	r := retryBackoffRange{min: 100, max: 200}
	for range 100 {
		got := r.pick()
		if got < 100 || got >= 200 {
			t.Fatalf("pick out of range: %v", got)
		}
	}
	// Empty range falls back to min.
	r = retryBackoffRange{min: 50, max: 50}
	for range 10 {
		if got := r.pick(); got != 50 {
			t.Fatalf("empty-range pick: want 50, got %v", got)
		}
	}
}

func TestVerifyPutObjectETag_Match(t *testing.T) {
	out := &s3.PutObjectOutput{
		ETag: aws.String(`"abc123"`),
	}
	if err := verifyPutObjectETag(out, "abc123"); err != nil {
		t.Fatalf("matching ETag: %v", err)
	}
}

func TestVerifyPutObjectETag_Mismatch(t *testing.T) {
	out := &s3.PutObjectOutput{
		ETag: aws.String(`"abc123"`),
	}
	err := verifyPutObjectETag(out, "deadbeef")
	if err == nil {
		t.Fatal("mismatched ETag: want error, got nil")
	}
	if !strings.Contains(err.Error(), "integrity check failed") {
		t.Fatalf("error message: %q", err)
	}
}

func TestVerifyPutObjectETag_SkipsKMS(t *testing.T) {
	out := &s3.PutObjectOutput{
		ServerSideEncryption: s3types.ServerSideEncryptionAwsKms,
		ETag:                 aws.String(`"opaque"`),
	}
	if err := verifyPutObjectETag(out, "abc123"); err != nil {
		t.Fatalf("KMS skip: %v", err)
	}
}

func TestVerifyPutObjectETag_SkipsMultipart(t *testing.T) {
	out := &s3.PutObjectOutput{
		ETag: aws.String(`"abc-2"`),
	}
	if err := verifyPutObjectETag(out, "abc123"); err != nil {
		t.Fatalf("multipart skip: %v", err)
	}
}

func TestVerifyPutObjectETag_SkipsSSEC(t *testing.T) {
	out := &s3.PutObjectOutput{
		SSECustomerAlgorithm: aws.String("AES256"),
		ETag:                 aws.String(`"abc"`),
	}
	if err := verifyPutObjectETag(out, "deadbeef"); err != nil {
		t.Fatalf("SSE-C skip: %v", err)
	}
}

func TestNewS3Target_Validation(t *testing.T) {
	cases := []struct {
		name string
		cfg  s3TargetConfig
		ok   bool
	}{
		{
			name: "valid",
			cfg:  s3TargetConfig{S3Client: &s3.Client{}, Bucket: "b"},
			ok:   true,
		},
		{
			name: "missing client",
			cfg:  s3TargetConfig{Bucket: "b"},
		},
		{
			name: "missing bucket",
			cfg:  s3TargetConfig{S3Client: &s3.Client{}},
		},
		{
			name: "negative inflight",
			cfg: s3TargetConfig{S3Client: &s3.Client{}, Bucket: "b",
				MaxInflightRequests: -1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tgt, err := newS3Target(tc.cfg)
			if tc.ok {
				if err != nil {
					t.Fatalf("want OK, got %v", err)
				}
				if tgt == nil {
					t.Fatalf("want non-nil target")
				}
				return
			}
			if err == nil {
				t.Fatalf("want error, got nil")
			}
		})
	}
}

func TestS3Target_SemaphoreCap(t *testing.T) {
	tgt, err := newS3Target(s3TargetConfig{
		S3Client:            &s3.Client{},
		Bucket:              "b",
		MaxInflightRequests: 3,
	})
	if err != nil {
		t.Fatalf("newS3Target: %v", err)
	}
	// Acquire all 3 slots.
	for range 3 {
		if err := tgt.acquire(context.Background()); err != nil {
			t.Fatalf("acquire: %v", err)
		}
	}
	// 4th acquire would block — verify by using a cancelled
	// ctx; the select must take the ctx.Done branch.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tgt.acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("4th acquire: want context.Canceled, got %v", err)
	}
	tgt.release()
	// Now a 4th acquire (post-release) succeeds.
	if err := tgt.acquire(context.Background()); err != nil {
		t.Fatalf("post-release acquire: %v", err)
	}
}

func TestS3Target_DefaultInflight(t *testing.T) {
	tgt, err := newS3Target(s3TargetConfig{
		S3Client: &s3.Client{},
		Bucket:   "b",
	})
	if err != nil {
		t.Fatalf("newS3Target: %v", err)
	}
	if cap(tgt.sem) != defaultMaxInflightRequests {
		t.Fatalf("sem cap: want %d, got %d",
			defaultMaxInflightRequests, cap(tgt.sem))
	}
}
