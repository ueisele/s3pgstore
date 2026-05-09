//go:build integration

package main

import (
	"context"
	"fmt"
	"sort"
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

	"github.com/ueisele/s3pgstore"
)

// Local fixture machinery — can't import the root package's
// fixture_test.go across package main / s3pgstore_test
// boundaries.

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

	schema := fmt.Sprintf("s3pgstore_rb_%d_%d",
		time.Now().UnixNano(), schemaSeq.Add(1))
	if _, err := pg.pool.Exec(ctx,
		fmt.Sprintf(`CREATE SCHEMA %q`, schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pg.pool.Exec(context.Background(),
			fmt.Sprintf(`DROP SCHEMA %q CASCADE`, schema))
	})

	bucket := strings.ToLower(fmt.Sprintf("s3pgstore-rb-%d-%d",
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

type rbRec struct {
	Customer string `parquet:"customer"`
	Value    int64  `parquet:"value"`
}

func newRBStore(t *testing.T, f *fixture) *s3pgstore.Store[rbRec] {
	t.Helper()
	cfg := s3pgstore.Config[rbRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		S3Bucket:          f.Bucket,
		S3Prefix:          "rebuild",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"customer"},
		PartitionKeyOf: func(r rbRec) string {
			return "customer=" + r.Customer
		},
	}
	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create schema: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })
	store, err := s3pgstore.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

// TestRebuild_RoundTrip: write a corpus, drop the catalog rows
// (keeping the tables), run rebuild, verify Read returns the
// same record set.
func TestRebuild_RoundTrip(t *testing.T) {
	f := newFixture(t)
	store := newRBStore(t, f)

	// Seed corpus across three partitions, multiple writes
	// each so written_at_version > 1 in the original catalog.
	want := map[string][]int64{}
	for _, c := range []string{"alice", "bob", "carol"} {
		for v := int64(1); v <= 3; v++ {
			if _, err := store.Write(t.Context(),
				[]rbRec{{Customer: c, Value: v}}); err != nil {
				t.Fatalf("Write: %v", err)
			}
			want[c] = append(want[c], v)
		}
	}

	// Drop catalog rows but keep the tables.
	for _, tbl := range []string{
		"s3pgstore_files",
		"s3pgstore_partitions",
		"s3pgstore_pending_writes",
	} {
		if _, err := f.Pool.Exec(t.Context(),
			fmt.Sprintf(`TRUNCATE %q.%q`, f.Schema, tbl),
		); err != nil {
			t.Fatalf("truncate %s: %v", tbl, err)
		}
	}

	// Rebuild.
	res, err := Rebuild(t.Context(), RebuildConfig{
		Pool:              f.Pool,
		S3Client:          f.S3Client,
		S3Bucket:          f.Bucket,
		S3Prefix:          "rebuild",
		SchemaName:        f.Schema,
		TablePrefix:       "s3pgstore_",
		PartitionKeyParts: []string{"customer"},
	})
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if res.FilesInserted != 9 {
		t.Errorf("files inserted: got %d, want 9", res.FilesInserted)
	}
	if res.PartitionsInserted != 3 {
		t.Errorf("partitions inserted: got %d, want 3",
			res.PartitionsInserted)
	}

	// Read back and verify each partition has the same Values
	// (order-independent; rebuild assigns version by S3-key
	// lex, which may not match the original write order).
	parts, err := store.Read(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.GE("customer", ""),
		})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("partitions: got %d, want 3", len(parts))
	}

	for _, p := range parts {
		// Strip "customer=" prefix.
		c := strings.TrimPrefix(p.PartitionKey, "customer=")
		got := []int64{}
		for _, r := range p.Records {
			got = append(got, r.Value)
		}
		sort.Slice(got, func(i, j int) bool {
			return got[i] < got[j]
		})
		w := append([]int64{}, want[c]...)
		sort.Slice(w, func(i, j int) bool { return w[i] < w[j] })
		if len(got) != len(w) {
			t.Errorf("partition %q: got %d records, want %d",
				p.PartitionKey, len(got), len(w))
			continue
		}
		for i := range got {
			if got[i] != w[i] {
				t.Errorf("partition %q records[%d]: got %d, want %d",
					p.PartitionKey, i, got[i], w[i])
			}
		}
		if p.Version != int64(len(w)) {
			t.Errorf("partition %q version after rebuild: "+
				"got %d, want %d", p.PartitionKey,
				p.Version, len(w))
		}
	}
}

// TestRebuild_Idempotent: run rebuild twice on a fresh corpus.
// Second run inserts no new rows and doesn't error.
func TestRebuild_Idempotent(t *testing.T) {
	f := newFixture(t)
	store := newRBStore(t, f)

	for i := range 5 {
		if _, err := store.Write(t.Context(),
			[]rbRec{{Customer: fmt.Sprintf("c%d", i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// Truncate the catalog so rebuild has work the first time.
	for _, tbl := range []string{
		"s3pgstore_files",
		"s3pgstore_partitions",
	} {
		if _, err := f.Pool.Exec(t.Context(),
			fmt.Sprintf(`TRUNCATE %q.%q`, f.Schema, tbl),
		); err != nil {
			t.Fatalf("truncate: %v", err)
		}
	}

	cfg := RebuildConfig{
		Pool:              f.Pool,
		S3Client:          f.S3Client,
		S3Bucket:          f.Bucket,
		S3Prefix:          "rebuild",
		SchemaName:        f.Schema,
		TablePrefix:       "s3pgstore_",
		PartitionKeyParts: []string{"customer"},
	}
	res1, err := Rebuild(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Rebuild 1: %v", err)
	}
	if res1.FilesInserted != 5 {
		t.Errorf("first run files: got %d, want 5",
			res1.FilesInserted)
	}

	res2, err := Rebuild(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Rebuild 2: %v", err)
	}
	if res2.FilesInserted != 0 {
		t.Errorf("second run files: got %d, want 0 "+
			"(ON CONFLICT DO NOTHING)", res2.FilesInserted)
	}
}

// TestRebuild_EmptyBucket: rebuild against an empty bucket
// produces zero rows without error.
func TestRebuild_EmptyBucket(t *testing.T) {
	f := newFixture(t)
	_ = newRBStore(t, f) // creates the catalog tables, but no writes

	res, err := Rebuild(t.Context(), RebuildConfig{
		Pool:              f.Pool,
		S3Client:          f.S3Client,
		S3Bucket:          f.Bucket,
		S3Prefix:          "rebuild",
		SchemaName:        f.Schema,
		TablePrefix:       "s3pgstore_",
		PartitionKeyParts: []string{"customer"},
	})
	if err != nil {
		t.Fatalf("Rebuild empty: %v", err)
	}
	if res.FilesInserted != 0 || res.PartitionsInserted != 0 {
		t.Errorf("empty rebuild: %+v (want zeros)", res)
	}
}

// TestRebuild_RecoversTokenAndExtFromS3Metadata writes records
// with WithIdempotencyToken + WithMetadata, drops the catalog,
// and verifies rebuild recovers both fields from S3 user-
// metadata (no parquet body GET needed).
func TestRebuild_RecoversTokenAndExtFromS3Metadata(t *testing.T) {
	f := newFixture(t)
	cfg := s3pgstore.Config[rbRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		S3Bucket:          f.Bucket,
		S3Prefix:          "rebuild",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"customer"},
		PartitionKeyOf: func(r rbRec) string {
			return "customer=" + r.Customer
		},
		ExtensionColumns: []s3pgstore.ExtensionColumn{
			{Name: "tenant_id", Type: "TEXT"},
			{Name: "shard", Type: "INT"},
		},
	}
	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create schema: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })
	store, err := s3pgstore.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// One write with token + metadata so recovery has something
	// non-trivial to assert against.
	want, err := store.Write(t.Context(),
		[]rbRec{{Customer: "alice", Value: 1}},
		s3pgstore.WithIdempotencyToken("token-recovery"),
		s3pgstore.WithMetadata(map[string]any{
			"tenant_id": "acme-tenant",
			"shard":     int(7),
		}),
	)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(want) != 1 || want[0].FileID == 0 {
		t.Fatalf("Write returned unexpected result: %+v", want)
	}

	// Drop catalog rows; S3 stays.
	for _, tbl := range []string{
		"s3pgstore_files",
		"s3pgstore_partitions",
		"s3pgstore_pending_writes",
	} {
		if _, err := f.Pool.Exec(t.Context(),
			fmt.Sprintf(`TRUNCATE %q.%q`, f.Schema, tbl),
		); err != nil {
			t.Fatalf("truncate %s: %v", tbl, err)
		}
	}

	res, err := Rebuild(t.Context(), RebuildConfig{
		Pool:              f.Pool,
		S3Client:          f.S3Client,
		S3Bucket:          f.Bucket,
		S3Prefix:          "rebuild",
		SchemaName:        f.Schema,
		TablePrefix:       "s3pgstore_",
		PartitionKeyParts: []string{"customer"},
		ExtensionColumns:  cfg.ExtensionColumns,
	})
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if res.FilesInserted != 1 {
		t.Fatalf("FilesInserted: want 1, got %d", res.FilesInserted)
	}

	// Inspect the rebuilt catalog row directly — Read doesn't
	// surface idempotency_token or ext_<n>.
	var (
		token            string
		extTenant        string
		extShard         int64
		writtenAtVersion int64
	)
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT idempotency_token, ext_tenant_id, ext_shard,
		        written_at_version
		FROM "`+f.Schema+`"."s3pgstore_files"
		WHERE partition_key = $1`,
		"customer=alice",
	).Scan(&token, &extTenant, &extShard, &writtenAtVersion); err != nil {
		t.Fatalf("scan rebuilt row: %v", err)
	}
	if token != "token-recovery" {
		t.Errorf("token: want %q, got %q", "token-recovery", token)
	}
	if extTenant != "acme-tenant" {
		t.Errorf("ext_tenant_id: want %q, got %q", "acme-tenant", extTenant)
	}
	if extShard != 7 {
		t.Errorf("ext_shard: want 7, got %d", extShard)
	}
	if writtenAtVersion != want[0].Version {
		t.Errorf("written_at_version: want %d (recovered from S3), "+
			"got %d", want[0].Version, writtenAtVersion)
	}

	// Idempotency lookup using the recovered token must return
	// the rebuilt row — proves the partial UNIQUE index works
	// across DR.
	got, ok, err := store.LookupByToken(t.Context(),
		want[0].PartitionKey, "token-recovery")
	if err != nil {
		t.Fatalf("LookupByToken: %v", err)
	}
	if !ok {
		t.Fatalf("LookupByToken: want hit on recovered token")
	}
	if got.Version != want[0].Version {
		t.Errorf("LookupByToken Version: want %d, got %d",
			want[0].Version, got.Version)
	}
}

// TestRebuild_RejectsMissingPartitionParts: no
// PartitionKeyParts → validation error before any LIST.
func TestRebuild_RejectsMissingPartitionParts(t *testing.T) {
	f := newFixture(t)
	_ = newRBStore(t, f)
	_, err := Rebuild(t.Context(), RebuildConfig{
		Pool:        f.Pool,
		S3Client:    f.S3Client,
		S3Bucket:    f.Bucket,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
	})
	if err == nil ||
		!strings.Contains(err.Error(), "PartitionKeyParts") {
		t.Errorf("got %v, want PartitionKeyParts error", err)
	}
}
