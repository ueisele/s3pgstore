//go:build integration

package s3pgstore

// Integration tests for the s3target wrapper. Lives in the
// `s3pgstore` package (not `_test`) so it can poke at the
// unexported s3target / newS3Target / s3TargetConfig surfaces.
// Wires its own MinIO + bucket via package-level helpers
// declared here (the cross-package fixture lives in
// fixture_test.go under s3pgstore_test).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

const (
	itMinioImage    = "pgsty/minio:RELEASE.2026-04-17T00-00-00Z"
	itMinioUsername = "minioadmin"
	itMinioPassword = "minioadmin"
)

//nolint:gochecknoglobals // test-only singleton
var (
	itMinioOnce sync.Once
	itMinioCli  *s3.Client
	itMinioErr  error
	itBucketSeq atomic.Int64
)

func itShareMinio(ctx context.Context) (*s3.Client, error) {
	itMinioOnce.Do(func() {
		container, err := tcminio.Run(ctx, itMinioImage,
			tcminio.WithUsername(itMinioUsername),
			tcminio.WithPassword(itMinioPassword),
			testcontainers.CustomizeRequestOption(
				func(req *testcontainers.GenericContainerRequest) error {
					req.Name = "s3pgstore-minio-" + testcontainers.SessionID()
					req.Reuse = true
					return nil
				},
			),
		)
		if err != nil {
			itMinioErr = fmt.Errorf("start MinIO: %w", err)
			return
		}
		connURL, err := container.ConnectionString(ctx)
		if err != nil {
			itMinioErr = fmt.Errorf("MinIO endpoint: %w", err)
			return
		}
		cfg, err := awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithRegion("us-east-1"),
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					itMinioUsername, itMinioPassword, "")),
		)
		if err != nil {
			itMinioErr = fmt.Errorf("aws config: %w", err)
			return
		}
		itMinioCli = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String("http://" + connURL)
			o.UsePathStyle = true
		})
	})
	return itMinioCli, itMinioErr
}

func itNewBucket(t *testing.T, cli *s3.Client) string {
	t.Helper()
	bucket := strings.ToLower(fmt.Sprintf("s3pgstore-tgt-%d-%d",
		time.Now().UnixNano(), itBucketSeq.Add(1)))
	if _, err := cli.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create bucket %q: %v", bucket, err)
	}
	return bucket
}

func TestS3Target_PutGetRoundTrip(t *testing.T) {
	cli, err := itShareMinio(t.Context())
	if err != nil {
		t.Fatalf("MinIO: %v", err)
	}
	bucket := itNewBucket(t, cli)
	tgt, err := newS3Target(s3TargetConfig{
		S3Client: cli,
		S3Bucket: bucket,
	})
	if err != nil {
		t.Fatalf("newS3Target: %v", err)
	}

	want := []byte("hello s3pgstore")
	if err := tgt.put(t.Context(), "data/x", want, "application/octet-stream", nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := tgt.get(t.Context(), "data/x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip: want %q, got %q", want, got)
	}
}

func TestS3Target_GetMissing(t *testing.T) {
	cli, err := itShareMinio(t.Context())
	if err != nil {
		t.Fatalf("MinIO: %v", err)
	}
	bucket := itNewBucket(t, cli)
	tgt, err := newS3Target(s3TargetConfig{S3Client: cli, S3Bucket: bucket})
	if err != nil {
		t.Fatalf("newS3Target: %v", err)
	}

	_, gotErr := tgt.get(t.Context(), "nope")
	if gotErr == nil {
		t.Fatal("get on missing key: want error, got nil")
	}
	var nsk *s3types.NoSuchKey
	if !errors.As(gotErr, &nsk) {
		t.Logf("note: got %T %v (expected NoSuchKey-shaped)", gotErr, gotErr)
	}
}

func TestS3Target_DeleteIdempotent(t *testing.T) {
	cli, err := itShareMinio(t.Context())
	if err != nil {
		t.Fatalf("MinIO: %v", err)
	}
	bucket := itNewBucket(t, cli)
	tgt, err := newS3Target(s3TargetConfig{S3Client: cli, S3Bucket: bucket})
	if err != nil {
		t.Fatalf("newS3Target: %v", err)
	}

	if err := tgt.put(t.Context(), "k", []byte("x"), "", nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := tgt.delete(t.Context(), "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Second delete on missing key is a no-op (DELETE is idempotent on S3).
	if err := tgt.delete(t.Context(), "k"); err != nil {
		t.Fatalf("delete on missing: %v", err)
	}
}

// TestS3Target_ConcurrentPutsSmokeTest exercises put/get under
// load to verify the typed wrapper behaves under concurrent
// goroutines. After the middleware refactor s3target itself no
// longer enforces a semaphore (concurrency is capped at the
// HTTP transport's MaxConnsPerHost on the wrapped client and at
// the shared Config.WorkerPool's MaxConcurrent in the library);
// this test just smoke-tests that 30 simultaneous PUTs against
// a raw MinIO client all complete and round-trip cleanly.
func TestS3Target_ConcurrentPutsSmokeTest(t *testing.T) {
	cli, err := itShareMinio(t.Context())
	if err != nil {
		t.Fatalf("MinIO: %v", err)
	}
	bucket := itNewBucket(t, cli)
	tgt, err := newS3Target(s3TargetConfig{
		S3Client: cli,
		S3Bucket: bucket,
	})
	if err != nil {
		t.Fatalf("newS3Target: %v", err)
	}

	const total = 30
	var wg sync.WaitGroup
	wg.Add(total)
	for i := range total {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("data/%d", i)
			if err := tgt.put(t.Context(), key, []byte{byte(i)}, "", nil); err != nil {
				t.Errorf("put %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	// Verify all keys round-tripped successfully.
	for i := range total {
		key := fmt.Sprintf("data/%d", i)
		if _, err := tgt.get(t.Context(), key); err != nil {
			t.Errorf("get %d (%s): %v", i, key, err)
		}
	}
}
