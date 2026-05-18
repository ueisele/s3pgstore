package s3pgstore

// target.go is the thin typed wrapper around an *s3.Client that
// the read/write/stream paths call into for PUT/GET. The
// load-bearing behavior — adaptive retry + rate limiting +
// per-op metrics (request.duration/count, attempt.error.count,
// body_size) — lives in the middleware stack the caller
// installed on the *s3.Client via s3client.WithDefaults. This
// wrapper exists for two things only:
//
//  1. ergonomic typed signatures (put/get take primitive args,
//     not s3.PutObjectInput-shaped structs);
//  2. inline ETag verification on PUT to defend against the
//     (now structurally impossible but still cheap to verify)
//     "PUT reported success but the body that landed is not the
//     body we passed in" failure shape.

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // ETag integrity check, not a cryptographic primitive
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/ueisele/s3pgstore/s3client"
)

// s3target wraps an *s3.Client (one composed via
// s3client.WithDefaults so it carries the adaptive-retry +
// rate-limit + metrics middleware stack) with the typed
// Put/Get/Delete shapes the library's read/write paths consume.
type s3target struct {
	s3     *s3.Client
	bucket string
	prefix string // for s3client.WithStoreLabels emission
}

// s3TargetConfig captures the small set of inputs s3target
// needs after the middleware refactor — the wrapped *s3.Client,
// the bucket, and the prefix (used as a metric label so
// multiple Stores sharing one *s3.Client surface as separate
// streams on the dashboard).
type s3TargetConfig struct {
	S3Client *s3.Client
	S3Bucket string
	S3Prefix string // empty allowed; metrics label only
}

func newS3Target(cfg s3TargetConfig) (*s3target, error) {
	if cfg.S3Client == nil {
		return nil, errors.New("s3target: S3Client required")
	}
	if cfg.S3Bucket == "" {
		return nil, errors.New("s3target: S3Bucket required")
	}
	return &s3target{
		s3:     cfg.S3Client,
		bucket: cfg.S3Bucket,
		prefix: cfg.S3Prefix,
	}, nil
}

// withLabels stamps the (bucket, prefix) tuple into ctx so the
// s3client metrics middleware emits them as s3pgstore.bucket /
// s3pgstore.prefix attributes. Centralising this in one helper
// keeps the per-method call sites short and ensures we never
// forget the labeling on a future code path.
func (t *s3target) withLabels(ctx context.Context) context.Context {
	return s3client.WithStoreLabels(ctx, t.bucket, t.prefix)
}

// get fetches a single object's body. The wrapped *s3.Client's
// middleware stack handles concurrency, retry, and metrics; this
// helper is just a typed convenience over GetObject.
func (t *s3target) get(
	ctx context.Context, key string,
) ([]byte, error) {
	ctx = t.withLabels(ctx)
	resp, err := t.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(t.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	// Pre-size from ContentLength when available to skip
	// io.ReadAll's grow-and-copy doubling. S3 GetObject returns
	// a reliable ContentLength for complete-object reads.
	var data []byte
	if resp.ContentLength != nil && *resp.ContentLength > 0 {
		data = make([]byte, *resp.ContentLength)
		_, err = io.ReadFull(resp.Body, data)
	} else {
		data, err = io.ReadAll(resp.Body)
	}
	return data, err
}

// put uploads data under key with the given content type and
// optional user metadata. Verifies the response ETag matches
// MD5(data) when ETag equals MD5 by construction (single-part
// PUT, no SSE-KMS, no SSE-C) — defends against the "PUT
// reported success but the body that landed is not the body we
// passed in" failure shape (proxy truncation, backend bug).
//
// metadata is the user-metadata map AWS stores under
// x-amz-meta-<key>. cmd/s3pgstore-rebuild reads it back via
// HEAD to reconstruct the catalog row without GETting the
// parquet body. nil/empty metadata is allowed.
//
// The middleware stack handles SDK-level retry; the SDK rewinds
// the *bytes.Reader body via io.Seeker between attempts, so —
// unlike the pre-middleware s3target — there's no need to
// reconstruct the input per attempt.
func (t *s3target) put(
	ctx context.Context, key string, data []byte,
	contentType string, metadata map[string]string,
) error {
	ctx = t.withLabels(ctx)
	expectedMD5 := md5.Sum(data) //nolint:gosec // integrity-only
	expectedETagHex := hex.EncodeToString(expectedMD5[:])
	out, err := t.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(t.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
		Metadata:    metadata,
	})
	if err != nil {
		return err
	}
	return verifyPutObjectETag(out, expectedETagHex)
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
// Returns a plain error on mismatch. The SDK's retry middleware
// has already done its work by the time we check — a mismatch
// surfaces directly to the caller as a hard error. (The
// pre-middleware EOF-body failure shape, where a stale
// *bytes.Reader sent zero bytes on retry and ETag mismatched
// d41d8cd9..., is structurally impossible now: the SDK
// rewinds io.Seeker bodies natively across SDK-level retries.)
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
				"intact — possible proxy truncation or backend "+
				"data loss)",
			gotETag, expectedETagHex)
	}
	return nil
}
