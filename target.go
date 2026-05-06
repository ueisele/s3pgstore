package s3pgstore

// Adapted from
// https://github.com/ueisele/s3store/blob/738a8bcbce870887833e158d4dc4e5116a29d4fc/target.go
// Copyright (c) 2024-2026 Uwe Eisele. MIT License.
//
// Adapted to s3pgstore: stripped ConsistencyControl routing
// (s3pgstore needs only GET-after-PUT-on-new-keys, no header
// needed on AWS S3 / MinIO; StorageGRID's
// ConsistencyControl: read-after-new-write is set on the
// bucket); stripped timing-config seeding (no SettleWindow /
// CommitTimeout / MaxClockSkew in this protocol); stripped HEAD
// (the catalog tells us file existence, no runtime HEAD);
// stripped LIST (only cmd/s3pgstore-rebuild lists, and it does
// so directly through the SDK). Metrics are placeholder
// callbacks until Phase 16 wires OTel.

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // ETag integrity check, not a cryptographic primitive
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// retryMaxAttempts: 1 initial + 4 retries. Sized to keep flaky
// calls bounded — five attempts over 1.4-2.4 s of randomised
// backoff sleep — without exhausting the caller's overall
// budget. Same envelope as s3store's vendored retry policy.
const retryMaxAttempts = 5

// retryBackoffRange is the [min, max) sleep window for the i-th
// retry. Each retry samples uniformly within the range, which
// breaks correlation between concurrently failing callers: a
// burst of SlowDowns no longer translates into a synchronised
// retry storm against the same prefix on the next round.
type retryBackoffRange struct {
	min, max time.Duration
}

// pick returns a uniformly-random duration in [min, max). Falls
// back to min when the range is empty (test stubs use {0, 0}
// to skip the sleep entirely).
func (r retryBackoffRange) pick() time.Duration {
	span := r.max - r.min
	if span <= 0 {
		return r.min
	}
	return r.min + time.Duration(rand.Int64N(int64(span)))
}

//nolint:gochecknoglobals // package-level retry schedule
var retryBackoff = [retryMaxAttempts - 1]retryBackoffRange{
	{100 * time.Millisecond, 300 * time.Millisecond},
	{300 * time.Millisecond, 500 * time.Millisecond},
	{500 * time.Millisecond, 800 * time.Millisecond},
	{500 * time.Millisecond, 800 * time.Millisecond},
}

// retry runs fn up to retryMaxAttempts times on transient S3
// failures. Returns as soon as fn succeeds, returns a
// non-retryable error, or ctx is cancelled.
//
// op labels the wrapping S3 operation ("get" / "put" /
// "delete") for log lines. onTransient (when non-nil) fires
// once per masked transient failure — used by the metrics
// pipeline (Phase 16) to surface retry-cause diagnostics.
func retry(
	ctx context.Context, op string,
	onTransient func(err error),
	fn func() error,
) error {
	var err error
	for attempt := range retryMaxAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryBackoff[attempt-1].pick()):
			}
		}
		err = fn()
		if err == nil {
			return nil
		}
		if !isTransientS3Error(err) {
			return err
		}
		if onTransient != nil {
			onTransient(err)
		}
		if attempt < retryMaxAttempts-1 {
			slog.Warn("s3pgstore: transient S3 error, retrying",
				"op", op,
				"attempt", attempt+1,
				"max_attempts", retryMaxAttempts,
				"err", err)
		}
	}
	return err
}

// isTransientS3Error reports whether err is likely to succeed
// on retry: HTTP 5xx, HTTP 429 (SlowDown), or transport-layer
// errors without an HTTP response (DNS / TCP / TLS / connection
// reset). Context cancellation or deadline expiry is never
// retryable — the caller already decided to stop.
func isTransientS3Error(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if respErr, ok := errors.AsType[*smithyhttp.ResponseError](err); ok {
		status := respErr.HTTPStatusCode()
		return status >= 500 || status == 429
	}
	// No HTTP response attached — transport-layer error. Retry.
	return true
}

// defaultMaxInflightRequests is applied when
// s3TargetConfig.MaxInflightRequests is unset. 32 utilises
// typical S3 fan-out without saturating one HTTP/1.1 connection
// pool; matches s3store's chosen default.
const defaultMaxInflightRequests = 32

// s3TargetConfig is the construction-time bundle for s3target.
// All fields except MaxInflightRequests are required.
type s3TargetConfig struct {
	S3Client *s3.Client
	Bucket   string
	Prefix   string

	// MaxInflightRequests caps net in-flight requests across
	// all callers of this target. Zero → defaultMaxInflightRequests.
	MaxInflightRequests int

	// onBufDropped, onTransient, onPutBytes, onGetBytes,
	// onPutDuration, onGetDuration, onDeleteDuration are
	// metric hooks wired in Phase 16. Nil callers tolerate
	// missing hooks.
	OnTransient func(op string, err error)
}

// s3target is the concrete S3 wrapper used by s3pgstore's write
// and read paths. Holds the SDK client, target bucket / prefix,
// and a buffered-channel semaphore that caps net in-flight
// requests.
//
// Constructor returns an *s3target so the semaphore can be
// shared across copies — pointer identity matters for the
// channel-as-semaphore.
type s3target struct {
	cfg s3TargetConfig
	sem chan struct{}
}

// newS3Target validates cfg and constructs an *s3target.
// Validates only: no S3 round-trips at construction time.
func newS3Target(cfg s3TargetConfig) (*s3target, error) {
	if cfg.S3Client == nil {
		return nil, fmt.Errorf("s3pgstore: s3target: S3Client required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3pgstore: s3target: Bucket required")
	}
	if cfg.MaxInflightRequests < 0 {
		return nil, fmt.Errorf("s3pgstore: s3target: MaxInflightRequests must be >= 0")
	}
	cap := cfg.MaxInflightRequests
	if cap == 0 {
		cap = defaultMaxInflightRequests
	}
	return &s3target{
		cfg: cfg,
		sem: make(chan struct{}, cap),
	}, nil
}

// acquire blocks until a semaphore slot is available or ctx is
// cancelled. Paired with release in defer.
func (t *s3target) acquire(ctx context.Context) error {
	select {
	case t.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release returns a slot to the semaphore.
func (t *s3target) release() {
	<-t.sem
}

// onTransient wraps the caller's hook with a non-nil-safe
// closure. Returns nil when no hook is configured.
func (t *s3target) onTransient(op string) func(err error) {
	if t.cfg.OnTransient == nil {
		return nil
	}
	return func(err error) { t.cfg.OnTransient(op, err) }
}

// get downloads a single object into memory. Used by the read
// path (Read, PollRecords).
func (t *s3target) get(
	ctx context.Context, key string,
) ([]byte, error) {
	if err := t.acquire(ctx); err != nil {
		return nil, err
	}
	defer t.release()
	var data []byte
	err := retry(ctx, "get", t.onTransient("get"), func() error {
		resp, err := t.cfg.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(t.cfg.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		// Pre-size from ContentLength when available to skip
		// io.ReadAll's grow-and-copy doubling. S3 GetObject
		// returns a reliable ContentLength for complete-object
		// reads.
		if resp.ContentLength != nil && *resp.ContentLength > 0 {
			data = make([]byte, *resp.ContentLength)
			_, err = io.ReadFull(resp.Body, data)
		} else {
			data, err = io.ReadAll(resp.Body)
		}
		return err
	})
	return data, err
}

// put uploads data under key with the given content type.
// Verifies the response ETag matches MD5(data) when ETag
// equals MD5 by construction (single-part PUT, no SSE-KMS,
// no SSE-C) — defends against the "PUT reported success but
// the body that landed is not the body we passed in" failure
// shape (e.g. caller-side reader EOF, proxy truncation).
func (t *s3target) put(
	ctx context.Context, key string, data []byte, contentType string,
) error {
	if err := t.acquire(ctx); err != nil {
		return err
	}
	defer t.release()
	// Compute the expected MD5 once outside the retry loop —
	// every iteration uploads the same byte slice.
	expectedMD5 := md5.Sum(data) //nolint:gosec // integrity-only
	expectedETagHex := hex.EncodeToString(expectedMD5[:])
	return retry(ctx, "put", t.onTransient("put"), func() error {
		// Build a fresh PutObjectInput every iteration so each
		// attempt's Body is a *bytes.Reader at position 0.
		// Reusing one input across the outer retry loop is
		// unsafe: the previous attempt's HTTP transport read the
		// underlying reader to EOF, and the SDK does not seek
		// seekable bodies back to 0 across top-level invocations.
		// The result would be a successful PUT that ships zero
		// bytes (ETag d41d8cd9...) over the wire. Per-iteration
		// construction keeps the data slice captured in the
		// closure but rebuilds a fresh reader on every attempt.
		input := &s3.PutObjectInput{
			Bucket:      aws.String(t.cfg.Bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType),
		}
		out, err := t.cfg.S3Client.PutObject(ctx, input)
		if err != nil {
			return err
		}
		return verifyPutObjectETag(out, expectedETagHex)
	})
}

// delete removes a single object. Used only by cmd/s3pgstore-gc
// — runtime read/write paths never DELETE. Idempotent on S3
// (DELETE on a missing key returns 204, not 404), so retries
// are safe.
func (t *s3target) delete(ctx context.Context, key string) error {
	if err := t.acquire(ctx); err != nil {
		return err
	}
	defer t.release()
	return retry(ctx, "delete", t.onTransient("delete"), func() error {
		_, err := t.cfg.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(t.cfg.Bucket),
			Key:    aws.String(key),
		})
		return err
	})
}

// verifyPutObjectETag compares the PutObject response's ETag to
// the MD5 of the body we asked the SDK to send. They match for
// every PUT s3pgstore issues (single-part PutObject, no
// SSE-KMS, no SSE-C — SSE-S3 / AES256 still ETags as
// MD5(plaintext)), so a mismatch means the body that landed on
// S3 is not the body we passed in.
//
// Skips the comparison whenever the response carries a marker
// that breaks the ETag-equals-MD5 equality:
//
//   - SSE-KMS / SSE-KMS-DSSE: ETag is opaque, not MD5.
//   - SSE-C: ETag depends on the customer key.
//   - Multipart ETag (`<hex>-<N>`): hash-of-part-hashes, not MD5.
//   - Backend returned no ETag: nothing to compare against.
//
// Returns a plain error on mismatch (no smithyhttp.Response
// wrapped) so isTransientS3Error treats it as a transport-layer
// failure and the outer retry() re-uploads with a fresh
// *bytes.Reader. That recovers automatically from the
// EOF-body shape; for a persistent mismatch (proxy / backend
// bug) the retry budget is bounded and the final error
// surfaces to the caller with full diagnostics.
func verifyPutObjectETag(
	out *s3.PutObjectOutput, expectedETagHex string,
) error {
	if out == nil {
		return nil
	}
	switch out.ServerSideEncryption {
	case s3types.ServerSideEncryptionAwsKms,
		s3types.ServerSideEncryptionAwsKmsDsse:
		return nil
	}
	if aws.ToString(out.SSECustomerAlgorithm) != "" {
		return nil
	}
	gotETag := strings.Trim(aws.ToString(out.ETag), `"`)
	if gotETag == "" {
		return nil
	}
	if strings.Contains(gotETag, "-") {
		return nil
	}
	if !strings.EqualFold(gotETag, expectedETagHex) {
		return fmt.Errorf(
			"PUT body integrity check failed: response ETag %q "+
				"!= expected MD5 %q (body bytes did not reach S3 "+
				"intact — possible client-side reader corruption, "+
				"proxy truncation, or backend data loss)",
			gotETag, expectedETagHex)
	}
	return nil
}
