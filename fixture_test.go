//go:build integration

package s3pgstore_test

import (
	"context"
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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Integration-test fixture: one PostgreSQL container and one
// MinIO container per `go test` invocation. Each test gets its
// own freshly-created PG schema and S3 bucket for isolation.
//
// Containers are reused across the invocation (and across
// packages in the same invocation) via testcontainers.SessionID
// and the Reuse flag — Ryuk reaps them when the invocation ends.
//
// Gated on the `integration` build tag so the testcontainers
// dependency never ends up in non-test builds.

const (
	postgresImage    = "postgres:17.5-alpine"
	postgresUsername = "s3pgstore"
	postgresPassword = "s3pgstore"
	postgresDatabase = "s3pgstore"

	// pgsty/minio fork; upstream minio/minio was archived in
	// Feb 2026 — the fork tracks original code and ships the
	// same image format.
	minioImage    = "pgsty/minio:RELEASE.2026-04-17T00-00-00Z"
	minioUsername = "minioadmin"
	minioPassword = "minioadmin"
)

type sharedPG struct {
	pool   *pgxpool.Pool
	connDS string
}

type sharedMinio struct {
	client   *s3.Client
	hostPort string
}

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
		pgShared = &sharedPG{pool: pool, connDS: ds}
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
		mShared = &sharedMinio{client: client, hostPort: connURL}
	})
	return mShared, minioErr
}

// fixture carries a per-test pgxpool, S3 client, schema name,
// and bucket. The schema and bucket are unique per test so
// concurrent tests can't observe each other's state.
type fixture struct {
	Pool     *pgxpool.Pool
	S3Client *s3.Client
	HostPort string
	Schema   string
	Bucket   string
}

// newFixture returns a fixture wired to the shared PG + MinIO
// containers. A fresh PG schema and S3 bucket are created for
// the test; both are dropped/deleted via t.Cleanup.
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

	schema := fmt.Sprintf("s3pgstore_it_%d_%d",
		time.Now().UnixNano(), schemaSeq.Add(1))
	if _, err := pg.pool.Exec(ctx,
		fmt.Sprintf(`CREATE SCHEMA %q`, schema)); err != nil {
		t.Fatalf("create schema %q: %v", schema, err)
	}
	t.Cleanup(func() {
		_, _ = pg.pool.Exec(context.Background(),
			fmt.Sprintf(`DROP SCHEMA %q CASCADE`, schema))
	})

	bucket := strings.ToLower(fmt.Sprintf("s3pgstore-it-%d-%d",
		time.Now().UnixNano(), bucketSeq.Add(1)))
	if _, err := mn.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create bucket %q: %v", bucket, err)
	}

	return &fixture{
		Pool:     pg.pool,
		S3Client: mn.client,
		HostPort: mn.hostPort,
		Schema:   schema,
		Bucket:   bucket,
	}
}
