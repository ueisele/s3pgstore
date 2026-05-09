//go:build integration

package gc_test

import (
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
	smithy "github.com/aws/smithy-go"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ueisele/s3pgstore"
	"github.com/ueisele/s3pgstore/gc"
)

// gc-package integration tests need their own copy of the
// fixture machinery — package _test boundaries prevent us from
// importing the root package's fixture_test.go directly.

const (
	postgresImage    = "postgres:17.9-alpine"
	postgresUsername = "s3pgstore"
	postgresPassword = "s3pgstore"
	postgresDatabase = "s3pgstore"
	minioImage       = "pgsty/minio:RELEASE.2026-04-17T00-00-00Z"
	minioUsername    = "minioadmin"
	minioPassword    = "minioadmin"
)

type sharedPG struct{ pool *pgxpool.Pool }
type sharedMinio struct{ client *s3.Client }

//nolint:gochecknoglobals // test-only singleton
var (
	pgOnce    sync.Once
	pgShared  *sharedPG
	pgErr     error
	minioOnce sync.Once
	mShared   *sharedMinio
	minioErr  error
	schemaSeq atomic.Int64
	bucketSeq atomic.Int64
)

func sharePG(ctx context.Context) (*sharedPG, error) {
	pgOnce.Do(func() {
		container, err := tcpostgres.Run(ctx, postgresImage,
			tcpostgres.WithDatabase(postgresDatabase),
			tcpostgres.WithUsername(postgresUsername),
			tcpostgres.WithPassword(postgresPassword),
			tcpostgres.BasicWaitStrategies(),
			testcontainers.CustomizeRequestOption(
				func(req *testcontainers.GenericContainerRequest) error {
					req.Name = "s3pgstore-pg-" + testcontainers.SessionID()
					req.Reuse = true
					return nil
				},
			),
		)
		if err != nil {
			pgErr = fmt.Errorf("start PostgreSQL: %w", err)
			return
		}
		ds, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			pgErr = fmt.Errorf("get PG connection string: %w", err)
			return
		}
		pool, err := pgxpool.New(ctx, ds)
		if err != nil {
			pgErr = fmt.Errorf("pgxpool.New: %w", err)
			return
		}
		pgShared = &sharedPG{pool: pool}
	})
	return pgShared, pgErr
}

func shareMinio(ctx context.Context) (*sharedMinio, error) {
	minioOnce.Do(func() {
		container, err := tcminio.Run(ctx, minioImage,
			tcminio.WithUsername(minioUsername),
			tcminio.WithPassword(minioPassword),
			testcontainers.CustomizeRequestOption(
				func(req *testcontainers.GenericContainerRequest) error {
					req.Name = "s3pgstore-minio-" + testcontainers.SessionID()
					req.Reuse = true
					return nil
				},
			),
		)
		if err != nil {
			minioErr = fmt.Errorf("start MinIO: %w", err)
			return
		}
		connURL, err := container.ConnectionString(ctx)
		if err != nil {
			minioErr = fmt.Errorf("get MinIO endpoint: %w", err)
			return
		}
		cfg, err := awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithRegion("us-east-1"),
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					minioUsername, minioPassword, "")),
		)
		if err != nil {
			minioErr = fmt.Errorf("aws config: %w", err)
			return
		}
		client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String("http://" + connURL)
			o.UsePathStyle = true
		})
		mShared = &sharedMinio{client: client}
	})
	return mShared, minioErr
}

type fixture struct {
	Pool     *pgxpool.Pool
	S3Client *s3.Client
	Schema   string
	Bucket   string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := t.Context()

	pg, err := sharePG(ctx)
	if err != nil {
		t.Fatalf("PG fixture: %v", err)
	}
	mn, err := shareMinio(ctx)
	if err != nil {
		t.Fatalf("MinIO fixture: %v", err)
	}

	schema := fmt.Sprintf("s3pgstore_gc_%d_%d",
		time.Now().UnixNano(), schemaSeq.Add(1))
	if _, err := pg.pool.Exec(ctx,
		fmt.Sprintf(`CREATE SCHEMA %q`, schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pg.pool.Exec(context.Background(),
			fmt.Sprintf(`DROP SCHEMA %q CASCADE`, schema))
	})

	bucket := strings.ToLower(fmt.Sprintf("s3pgstore-gc-%d-%d",
		time.Now().UnixNano(), bucketSeq.Add(1)))
	if _, err := mn.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	return &fixture{
		Pool:     pg.pool,
		S3Client: mn.client,
		Schema:   schema,
		Bucket:   bucket,
	}
}

type gcRec struct {
	ID string `parquet:"id"`
}

func newGCStore(t *testing.T, f *fixture) *s3pgstore.Store[gcRec] {
	t.Helper()
	cfg := s3pgstore.Config[gcRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		S3Bucket:          f.Bucket,
		S3Prefix:          "gc",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"customer"},
		PartitionKeyOf: func(r gcRec) string {
			return "customer=" + r.ID
		},
	}
	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })
	store, err := s3pgstore.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

// seedOrphan writes a parquet body to S3 and inserts a
// pending_writes row pointing at it, but never commits a
// catalog row — simulating the state after a Write that PUT
// successfully but whose catalog tx rolled back.
func seedOrphan(t *testing.T, f *fixture, partitionKey string,
	intendedAt time.Time) string {
	t.Helper()
	s3Key := fmt.Sprintf("gc/data/%s/orphan-%d.parquet",
		partitionKey, time.Now().UnixNano())
	if _, err := f.S3Client.PutObject(t.Context(),
		&s3.PutObjectInput{
			Bucket: aws.String(f.Bucket),
			Key:    aws.String(s3Key),
			Body:   strings.NewReader("orphan body"),
		}); err != nil {
		t.Fatalf("seed PUT: %v", err)
	}
	if _, err := f.Pool.Exec(t.Context(),
		fmt.Sprintf(
			`INSERT INTO %q.%q (s3_key, intended_at) VALUES ($1, $2)`,
			f.Schema, "s3pgstore_pending_writes"),
		s3Key, intendedAt.UTC()); err != nil {
		t.Fatalf("seed pending_writes: %v", err)
	}
	return s3Key
}

// TestGC_RunOnce_ReclaimsAgedOrphan verifies the happy path:
// an orphan older than Grace is removed from S3 and from the
// catalog tracker.
func TestGC_RunOnce_ReclaimsAgedOrphan(t *testing.T) {
	f := newFixture(t)
	_ = newGCStore(t, f) // creates the catalog tables

	// Old orphan: intended_at way in the past so it satisfies
	// any reasonable Grace.
	s3Key := seedOrphan(t, f, "customer=alice",
		time.Now().Add(-7*24*time.Hour))

	n, err := gc.RunOnce(t.Context(), gc.Config{
		Pool:        f.Pool,
		S3Client:    f.S3Client,
		S3Bucket:    f.Bucket,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		Grace:       1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 1 {
		t.Errorf("reclaimed: got %d, want 1", n)
	}

	// Catalog row gone.
	var pendingCount int
	if err := f.Pool.QueryRow(t.Context(),
		fmt.Sprintf(`SELECT count(*) FROM %q.%q WHERE s3_key = $1`,
			f.Schema, "s3pgstore_pending_writes"),
		s3Key,
	).Scan(&pendingCount); err != nil {
		t.Fatalf("count: %v", err)
	}
	if pendingCount != 0 {
		t.Errorf("pending_writes after GC: got %d, want 0", pendingCount)
	}

	// S3 object gone.
	_, err = f.S3Client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: aws.String(f.Bucket),
		Key:    aws.String(s3Key),
	})
	if err == nil {
		t.Errorf("S3 object %s still exists after GC", s3Key)
	} else {
		var ae smithy.APIError
		if !errors.As(err, &ae) || ae.ErrorCode() != "NotFound" {
			t.Errorf("HeadObject after GC: got %v, want NotFound",
				err)
		}
	}
}

// TestGC_RunOnce_GracePeriodRespected verifies that a fresh
// orphan (younger than Grace) is left alone.
func TestGC_RunOnce_GracePeriodRespected(t *testing.T) {
	f := newFixture(t)
	_ = newGCStore(t, f)

	// Fresh orphan: intended_at == now.
	s3Key := seedOrphan(t, f, "customer=alice", time.Now())

	n, err := gc.RunOnce(t.Context(), gc.Config{
		Pool:        f.Pool,
		S3Client:    f.S3Client,
		S3Bucket:    f.Bucket,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		Grace:       1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 0 {
		t.Errorf("fresh orphan reclaimed prematurely: %d", n)
	}

	// Catalog row + S3 object both still there.
	var pendingCount int
	if err := f.Pool.QueryRow(t.Context(),
		fmt.Sprintf(`SELECT count(*) FROM %q.%q WHERE s3_key = $1`,
			f.Schema, "s3pgstore_pending_writes"),
		s3Key).Scan(&pendingCount); err != nil {
		t.Fatalf("count: %v", err)
	}
	if pendingCount != 1 {
		t.Errorf("pending_writes within grace: got %d, want 1",
			pendingCount)
	}
	if _, err := f.S3Client.HeadObject(t.Context(),
		&s3.HeadObjectInput{
			Bucket: aws.String(f.Bucket),
			Key:    aws.String(s3Key),
		}); err != nil {
		t.Errorf("S3 object missing within grace: %v", err)
	}
}

// TestGC_RunOnce_SkipsCommittedFiles: pending_writes rows
// inserted by Write are deleted at commit, so no row exists
// for committed files. Verify the steady-state has nothing
// for GC to reclaim.
func TestGC_RunOnce_SkipsCommittedFiles(t *testing.T) {
	f := newFixture(t)
	store := newGCStore(t, f)

	if _, err := store.Write(t.Context(),
		[]gcRec{{ID: "alice"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	n, err := gc.RunOnce(t.Context(), gc.Config{
		Pool:        f.Pool,
		S3Client:    f.S3Client,
		S3Bucket:    f.Bucket,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		Grace:       0, // would reclaim ANY orphan, but there are none
	})
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 0 {
		t.Errorf("committed file reclaimed: %d (want 0)", n)
	}
}

// TestGC_RunOnce_Idempotent verifies that running GC twice
// against an already-clean state is a no-op without errors.
func TestGC_RunOnce_Idempotent(t *testing.T) {
	f := newFixture(t)
	_ = newGCStore(t, f)
	seedOrphan(t, f, "customer=alice",
		time.Now().Add(-7*24*time.Hour))

	cfg := gc.Config{
		Pool:        f.Pool,
		S3Client:    f.S3Client,
		S3Bucket:    f.Bucket,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		Grace:       1 * time.Hour,
	}
	if _, err := gc.RunOnce(t.Context(), cfg); err != nil {
		t.Fatalf("RunOnce 1: %v", err)
	}
	n, err := gc.RunOnce(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunOnce 2: %v", err)
	}
	if n != 0 {
		t.Errorf("RunOnce 2 (clean state): got %d, want 0", n)
	}
}

// TestGC_RunOnce_BatchSizeLimitsScan: more orphans than
// BatchSize → first call reclaims BatchSize, second drains
// the rest.
func TestGC_RunOnce_BatchSizeLimitsScan(t *testing.T) {
	f := newFixture(t)
	_ = newGCStore(t, f)

	// Seed 3 aged orphans with monotonic intended_at so the
	// scan order matches insertion order.
	base := time.Now().Add(-7 * 24 * time.Hour)
	for i := range 3 {
		seedOrphan(t, f, fmt.Sprintf("customer=c%d", i),
			base.Add(time.Duration(i)*time.Second))
	}

	cfg := gc.Config{
		Pool:        f.Pool,
		S3Client:    f.S3Client,
		S3Bucket:    f.Bucket,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		Grace:       1 * time.Hour,
		BatchSize:   2,
	}
	n1, err := gc.RunOnce(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunOnce 1: %v", err)
	}
	if n1 != 2 {
		t.Errorf("RunOnce 1 with BatchSize=2: got %d, want 2", n1)
	}
	n2, err := gc.RunOnce(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunOnce 2: %v", err)
	}
	if n2 != 1 {
		t.Errorf("RunOnce 2: got %d, want 1 (remainder)", n2)
	}
}

// TestGC_RunOnce_MissingS3KeyStillReclaimsRow verifies the
// idempotent-S3-DELETE path: an orphan whose S3 object is
// already gone (e.g. external cleanup) still has its
// pending_writes row removed because S3 DELETE on a missing
// key is a no-op success.
func TestGC_RunOnce_MissingS3KeyStillReclaimsRow(t *testing.T) {
	f := newFixture(t)
	_ = newGCStore(t, f)

	// Manually insert a pending_writes row for an S3 key that
	// doesn't exist.
	missingKey := "gc/data/customer=ghost/missing.parquet"
	if _, err := f.Pool.Exec(t.Context(),
		fmt.Sprintf(
			`INSERT INTO %q.%q (s3_key, intended_at) VALUES ($1, $2)`,
			f.Schema, "s3pgstore_pending_writes"),
		missingKey, time.Now().Add(-7*24*time.Hour).UTC()); err != nil {
		t.Fatalf("seed: %v", err)
	}

	n, err := gc.RunOnce(t.Context(), gc.Config{
		Pool:        f.Pool,
		S3Client:    f.S3Client,
		S3Bucket:    f.Bucket,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		Grace:       1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 1 {
		t.Errorf("missing-key orphan: got %d, want 1", n)
	}
}

// TestGC_Run_LoopCancels verifies the loop exits cleanly on
// ctx cancel and reclaims orphans during its run.
func TestGC_Run_LoopCancels(t *testing.T) {
	f := newFixture(t)
	_ = newGCStore(t, f)
	seedOrphan(t, f, "customer=alice",
		time.Now().Add(-7*24*time.Hour))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- gc.Run(ctx, gc.Config{
			Pool:        f.Pool,
			S3Client:    f.S3Client,
			S3Bucket:    f.Bucket,
			SchemaName:  f.Schema,
			TablePrefix: "s3pgstore_",
			Grace:       1 * time.Hour,
			Interval:    50 * time.Millisecond,
		})
	}()

	// Wait for the orphan to be reclaimed (initial RunOnce
	// fires immediately on Run entry).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := f.Pool.QueryRow(t.Context(),
			fmt.Sprintf(`SELECT count(*) FROM %q.%q`,
				f.Schema, "s3pgstore_pending_writes"),
		).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Run did not exit within 5s")
	}
}

// TestGC_RequiresPool / RequiresS3Client / RequiresBucket
// exercise the validate guards.
func TestGC_RunOnce_Validation(t *testing.T) {
	cases := []struct {
		name string
		cfg  gc.Config
		want string
	}{
		{name: "no pool",
			cfg:  gc.Config{},
			want: "Pool is required"},
		{name: "no S3Client",
			cfg:  gc.Config{Pool: &pgxpool.Pool{}},
			want: "S3Client is required"},
		{name: "no S3Bucket",
			cfg: gc.Config{
				Pool: &pgxpool.Pool{}, S3Client: &s3.Client{}},
			want: "S3Bucket is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := gc.RunOnce(t.Context(), tc.cfg)
			if err == nil ||
				!strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want substring %q",
					err, tc.want)
			}
		})
	}
}
