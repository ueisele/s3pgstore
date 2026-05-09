//go:build integration

package sequencer_test

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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ueisele/s3pgstore"
	"github.com/ueisele/s3pgstore/sequencer"
)

// Sequencer integration tests run against a fresh PG schema +
// MinIO bucket per test. We seed rows via the public Store[T]
// write path, then exercise the sequencer directly. The
// fixture machinery is duplicated from the root package's
// fixture (which lives under build tag `integration` in
// package s3pgstore_test); we can't import _test packages
// across modules, so the sequencer subpackage gets its own copy.

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

	schema := fmt.Sprintf("s3pgstore_seq_%d_%d",
		time.Now().UnixNano(), schemaSeq.Add(1))
	if _, err := pg.pool.Exec(ctx,
		fmt.Sprintf(`CREATE SCHEMA %q`, schema)); err != nil {
		t.Fatalf("create schema %q: %v", schema, err)
	}
	t.Cleanup(func() {
		_, _ = pg.pool.Exec(context.Background(),
			fmt.Sprintf(`DROP SCHEMA %q CASCADE`, schema))
	})

	bucket := strings.ToLower(fmt.Sprintf("s3pgstore-seq-%d-%d",
		time.Now().UnixNano(), bucketSeq.Add(1)))
	if _, err := mn.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create bucket %q: %v", bucket, err)
	}

	return &fixture{
		Pool:     pg.pool,
		S3Client: mn.client,
		Schema:   schema,
		Bucket:   bucket,
	}
}

type seqRec struct {
	ID string `parquet:"id"`
}

func newSeqStore(t *testing.T, f *fixture) *s3pgstore.Store[seqRec] {
	t.Helper()
	cfg := s3pgstore.Config[seqRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		Bucket:            f.Bucket,
		Prefix:            "seq",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"customer"},
		PartitionKeyOf:    func(r seqRec) string { return "customer=" + r.ID },
	}
	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create schema: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })
	store, err := s3pgstore.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New store: %v", err)
	}
	return store
}

// seqCount returns the number of rows with non-NULL feed_seq in
// the schema's s3pgstore_files table. Used to wait for sequencer
// progress without depending on its return value.
func seqCount(t *testing.T, f *fixture) int {
	t.Helper()
	var n int
	if err := f.Pool.QueryRow(t.Context(),
		fmt.Sprintf(
			`SELECT count(*) FROM %q.%q WHERE feed_seq IS NOT NULL`,
			f.Schema, "s3pgstore_files"),
	).Scan(&n); err != nil {
		t.Fatalf("seqCount: %v", err)
	}
	return n
}

// TestSequencer_RunOnce_AssignsAllEligible writes a handful of
// rows, runs RunOnce once with a generous BatchSize, and
// verifies feed_seq landed gap-free starting at 1 in
// (written_at, file_id) order.
func TestSequencer_RunOnce_AssignsAllEligible(t *testing.T) {
	f := newFixture(t)
	store := newSeqStore(t, f)

	for i, id := range []string{"a", "b", "c", "d"} {
		if _, err := store.Write(t.Context(),
			[]seqRec{{ID: id}}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	cfg := sequencer.Config{
		Pool:        f.Pool,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		BatchSize:   100,
	}
	n, err := sequencer.RunOnce(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 4 {
		t.Errorf("RunOnce assigned %d, want 4", n)
	}

	// Gap-free 1..4 in (written_at, file_id) order.
	rows, err := f.Pool.Query(t.Context(),
		fmt.Sprintf(
			`SELECT feed_seq FROM %q.%q `+
				`ORDER BY written_at, file_id`,
			f.Schema, "s3pgstore_files"))
	if err != nil {
		t.Fatalf("query feed_seq: %v", err)
	}
	defer rows.Close()
	var seqs []int64
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		seqs = append(seqs, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	for i, got := range seqs {
		want := int64(i + 1)
		if got != want {
			t.Errorf("seqs[%d] = %d, want %d", i, got, want)
		}
	}
}

// TestSequencer_RunOnce_Idempotent verifies a second RunOnce
// against a fully-sequenced table is a no-op (returns 0, no
// error). This is load-bearing for the polling-fallback path —
// the sequencer wakes on the timer with no work and must not
// roll feed_seq forward.
func TestSequencer_RunOnce_Idempotent(t *testing.T) {
	f := newFixture(t)
	store := newSeqStore(t, f)

	if _, err := store.Write(t.Context(),
		[]seqRec{{ID: "alice"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	cfg := sequencer.Config{
		Pool:        f.Pool,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		BatchSize:   100,
	}
	if _, err := sequencer.RunOnce(t.Context(), cfg); err != nil {
		t.Fatalf("RunOnce 1: %v", err)
	}
	n, err := sequencer.RunOnce(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunOnce 2: %v", err)
	}
	if n != 0 {
		t.Errorf("RunOnce 2 (no eligible rows): got %d, want 0", n)
	}
}

// TestSequencer_BatchSize_LimitsAssignment writes more rows
// than BatchSize and verifies RunOnce assigns only BatchSize
// rows; a second call picks up the rest.
func TestSequencer_BatchSize_LimitsAssignment(t *testing.T) {
	f := newFixture(t)
	store := newSeqStore(t, f)

	for i := range 5 {
		if _, err := store.Write(t.Context(),
			[]seqRec{{ID: fmt.Sprintf("c%d", i)}}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	cfg := sequencer.Config{
		Pool:        f.Pool,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		BatchSize:   2,
	}
	n1, err := sequencer.RunOnce(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunOnce 1: %v", err)
	}
	if n1 != 2 {
		t.Errorf("RunOnce 1: got %d, want 2 (batch cap)", n1)
	}

	n2, err := sequencer.RunOnce(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunOnce 2: %v", err)
	}
	if n2 != 2 {
		t.Errorf("RunOnce 2: got %d, want 2 (batch cap)", n2)
	}
	n3, err := sequencer.RunOnce(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunOnce 3: %v", err)
	}
	if n3 != 1 {
		t.Errorf("RunOnce 3: got %d, want 1 (remainder)", n3)
	}
}

// TestSequencer_GapFreeUnderConcurrentWriters fan-outs N writers
// against a single sequencer Run. Verifies that after every
// write commits, the sequencer has assigned a gap-free 1..N
// feed_seq, and feed_seq order matches written_at order.
func TestSequencer_GapFreeUnderConcurrentWriters(t *testing.T) {
	f := newFixture(t)
	store := newSeqStore(t, f)

	const total = 50

	seqCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	seqDone := make(chan error, 1)
	go func() {
		seqDone <- sequencer.Run(seqCtx, sequencer.Config{
			Pool:         f.Pool,
			SchemaName:   f.Schema,
			TablePrefix:  "s3pgstore_",
			PollInterval: 50 * time.Millisecond,
			BatchSize:    32,
		})
	}()

	var wg sync.WaitGroup
	for i := range total {
		wg.Go(func() {
			if _, err := store.Write(t.Context(),
				[]seqRec{{ID: fmt.Sprintf("c%03d", i)}}); err != nil {
				t.Errorf("Write %d: %v", i, err)
			}
		})
	}
	wg.Wait()

	// Wait until the sequencer finishes draining. Tight loop
	// with 50ms PollInterval should converge fast.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if seqCount(t, f) == total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := seqCount(t, f); got != total {
		t.Fatalf("after fan-out + drain: feed_seq count = %d, "+
			"want %d", got, total)
	}

	// Cancel the sequencer and wait for clean exit.
	cancel()
	select {
	case err := <-seqDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("sequencer did not exit within 5s of cancel")
	}

	// Verify feed_seq is gap-free 1..total.
	rows, err := f.Pool.Query(t.Context(),
		fmt.Sprintf(
			`SELECT feed_seq FROM %q.%q ORDER BY feed_seq`,
			f.Schema, "s3pgstore_files"))
	if err != nil {
		t.Fatalf("query feed_seq: %v", err)
	}
	defer rows.Close()
	var seqs []int64
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		seqs = append(seqs, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(seqs) != total {
		t.Fatalf("rows: got %d, want %d", len(seqs), total)
	}
	for i, got := range seqs {
		want := int64(i + 1)
		if got != want {
			t.Errorf("gap at index %d: got %d, want %d", i, got, want)
		}
	}
}

// TestSequencer_NotifyWakesFasterThanPoll verifies that NOTIFY
// shortens latency vs. interval polling. We set a very long
// PollInterval (5s) so the only way to wake the sequencer
// promptly is via NOTIFY. After a Write commits, the sequencer
// should pick up the row well before the poll fires.
func TestSequencer_NotifyWakesFasterThanPoll(t *testing.T) {
	f := newFixture(t)
	store := newSeqStore(t, f)

	seqCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	seqDone := make(chan error, 1)
	go func() {
		seqDone <- sequencer.Run(seqCtx, sequencer.Config{
			Pool:          f.Pool,
			SchemaName:    f.Schema,
			TablePrefix:   "s3pgstore_",
			PollInterval:  5 * time.Second,
			NotifyChannel: "s3pgstore_writes",
			BatchSize:     32,
		})
	}()

	// Wait for the LISTEN connection to register before
	// emitting NOTIFY. Postgres delivers NOTIFY only to clients
	// that have already issued LISTEN at COMMIT time.
	time.Sleep(200 * time.Millisecond)

	start := time.Now()
	if _, err := store.Write(t.Context(),
		[]seqRec{{ID: "alice"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Expect feed_seq to land within 1s — well below
	// PollInterval (5s) — when NOTIFY is wired correctly.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if seqCount(t, f) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	if seqCount(t, f) != 1 {
		t.Fatalf("feed_seq not assigned within 2s; "+
			"polling-fallback was supposed to be 5s away. "+
			"elapsed=%v", elapsed)
	}
	if elapsed > 1500*time.Millisecond {
		t.Errorf("NOTIFY didn't wake fast enough: elapsed=%v "+
			"(should be << PollInterval=5s)", elapsed)
	}

	cancel()
	select {
	case <-seqDone:
	case <-time.After(5 * time.Second):
		t.Error("sequencer did not exit within 5s of cancel")
	}
}

// TestSequencer_AdvisoryLockExclusion verifies that two
// concurrent Run instances against the same (schema, prefix)
// serialize their assignment work — only one is making forward
// progress at any moment. We don't try to detect simultaneous
// in-flight assignment (hard to observe externally); we just
// confirm both Run instances make progress without producing
// corrupted feed_seq.
func TestSequencer_AdvisoryLockExclusion(t *testing.T) {
	f := newFixture(t)
	store := newSeqStore(t, f)

	const total = 30
	for i := range total {
		if _, err := store.Write(t.Context(),
			[]seqRec{{ID: fmt.Sprintf("c%03d", i)}}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	// Two RunOnce goroutines racing on the same scope. Both
	// should succeed; combined assignment count = total; no
	// duplicate feed_seq values.
	results := make(chan int, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			n, err := sequencer.RunOnce(t.Context(),
				sequencer.Config{
					Pool:        f.Pool,
					SchemaName:  f.Schema,
					TablePrefix: "s3pgstore_",
					BatchSize:   total,
				})
			if err != nil {
				errs <- err
				return
			}
			results <- n
		})
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Errorf("RunOnce error: %v", err)
	}

	sum := 0
	for n := range results {
		sum += n
	}
	if sum != total {
		t.Errorf("combined RunOnce assignments: got %d, want %d",
			sum, total)
	}

	// All feed_seq values are unique.
	var dupCount int
	if err := f.Pool.QueryRow(t.Context(),
		fmt.Sprintf(
			`SELECT count(*) - count(DISTINCT feed_seq) `+
				`FROM %q.%q WHERE feed_seq IS NOT NULL`,
			f.Schema, "s3pgstore_files"),
	).Scan(&dupCount); err != nil {
		t.Fatalf("dup check: %v", err)
	}
	if dupCount != 0 {
		t.Errorf("duplicate feed_seq values detected: %d", dupCount)
	}
}

// TestSequencer_RequiresPool verifies the validation guard.
func TestSequencer_RequiresPool(t *testing.T) {
	_, err := sequencer.RunOnce(t.Context(), sequencer.Config{})
	if err == nil || !strings.Contains(err.Error(), "Pool is required") {
		t.Fatalf("RunOnce with nil Pool: want validation error, got %v",
			err)
	}
}
