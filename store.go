package s3pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ueisele/s3pgstore/internal/catalog"
	"github.com/ueisele/s3pgstore/pool"
)

// defaultWorkerPoolSize is the slot count of the auto-installed
// Config.WorkerPool when the caller doesn't supply one. Mirrors
// s3client.defaultMaxOpenConnections so a single-Store
// deployment that tunes neither knob still gets a self-consistent
// configuration.
const defaultWorkerPoolSize = 64

// Offset is the gap-free monotonic stream cursor assigned by
// the sequencer. Consumers walk the feed by passing the last-
// observed offset back to Poll / PollRecords as `since`. Offsets
// are stable: once observed, an offset never reappears with
// different content (the sequencer assigns to committed rows
// only and feed_seq is UNIQUE NOT NULL once set).
type Offset = int64

// OffsetNone is the sentinel Offset value for "no feed_seq
// assigned" (or "not selected"). Sequencer assignment starts at
// OffsetEarliest (1), so OffsetNone (0) is never a live offset
// and is safe to use as a not-yet-sequenced marker on
// FileRef.Offset (set by Write and the non-Poll Read paths
// whose SELECT does not project feed_seq).
const OffsetNone Offset = 0

// OffsetEarliest is the lowest valid live offset — the first
// value the sequencer ever assigns. Pass it as Poll's `since`
// argument to drain from the very beginning of the feed:
//
//	tip, err := store.OffsetLatest(ctx)
//	if err != nil { return err }
//	for fr, err := range store.PollRecordsIter(ctx,
//	    s3pgstore.OffsetEarliest, tip) { … }
//
// Equivalent to passing 0 (since `feed_seq >= 0` and
// `feed_seq >= 1` return the same rows when the sequencer
// starts at 1), but reads more honestly at the call site.
//
// For bounded reads users typically pair OffsetEarliest with the
// dynamic tip returned by Store.OffsetLatest; for unbounded
// follow there's no upper-bound parameter at all (TailRecordsIter
// / TailIter take only `since`).
const OffsetEarliest Offset = 1

// FileRef is a reference to a single file in the catalog. It
// carries enough metadata to identify the file (FileID), locate
// it in S3 (S3Key), describe its version (Version, Offset),
// describe when and at what shape it was written (WrittenAt,
// RecordCount, FileSize, UncompressedSize), and filter it
// (Extensions) — but not the file's data; the records are
// fetched separately via ReadFileRefsIter.
//
// The unifying type across Write, Poll, and the read paths.
// Field names align with the catalog columns and across the
// public surface so the type a writer learns about a file from
// matches the type a reader walks files with: the result of
// Write can be passed straight into ReadFileRefsIter.
//
// Every field is populated from every source EXCEPT Offset,
// which is populated by Poll (always; Poll only walks sequenced
// rows) and by Read paths (when the underlying row's feed_seq is
// non-NULL); Write returns OffsetNone because the sequencer
// assigns feed_seq asynchronously after the write commits.
// Compare Offset against OffsetNone (or any value <= 0) to test
// whether it's meaningful.
//
// Field semantics:
//   - FileID: s3pgstore_files.file_id (BIGSERIAL primary key).
//   - PartitionKey: the partition this file belongs to.
//   - S3Key: full S3 key (the parquet object location).
//   - Version: the partition version this file was written at;
//     load-bearing for PartitionResult.Version derivation per
//     CLAUDE.md.
//   - WrittenAt: the row's INSERT-time timestamp (PostgreSQL
//     now() at the wrapping write tx), in UTC. The atomic-
//     visibility-on-commit invariant pins this to the moment the
//     Write call's tx committed.
//   - RecordCount: number of records (rows) the parquet file
//     contains.
//   - FileSize: compressed parquet bytes (the size of the object
//     on S3).
//   - UncompressedSize: sum of TotalUncompressedSize across every
//     column chunk; useful for compression-ratio dashboards and
//     for downstream consumers that materialize the decoded data.
//   - Extensions: ext_<n> column values declared on the Store's
//     Config.ExtensionColumns; missing/NULL columns are absent
//     from the map.
//   - Offset: assigned feed_seq, or OffsetNone if not populated by
//     the source.
type FileRef struct {
	FileID           int64
	PartitionKey     string
	S3Key            string
	Version          int64
	WrittenAt        time.Time
	RecordCount      int64
	FileSize         int64
	UncompressedSize int64
	Extensions       map[string]any
	Offset           Offset
}

// FileResult is the per-file decoded output yielded by
// PollRecords / PollRecordsIter. File carries the catalog row's
// FileRef (including Offset for checkpointing); Records carries
// the parquet bodies decoded into T. Records emit in their
// per-file decode order (no sort, no dedup — stream consumers see
// every observed version in commit order).
//
// Resume idiom: after processing a FileResult, the next-poll
// `since` is `fr.File.Offset + 1`. The caller maintains the
// cursor; if the iter never yields, the unchanged input `since`
// is the resume point.
type FileResult[T any] struct {
	File    FileRef
	Records []T
}

// Store is the typed entry point for writing and reading
// records. Construct via New[T]; New runs Config validation,
// constructs the parquet encoder, builds the S3 target, and
// validates the catalog schema via SchemaManager.Validate.
//
// All methods are safe for concurrent use.
type Store[T any] struct {
	cfg      Config[T]
	resolved Config[T]
	names    catalog.Names
	target   *s3target
	encoder  *parquetEncoder[T]
	metrics  *metrics
	sql      sqlCache
}

// sqlCache holds the SQL strings the hot write path renders on
// every call. Built once in New() because PartitionKeyParts,
// ExtensionColumns, and MaterializedViews are immutable after
// Store construction; the underlying catalog.Names methods
// fmt.Sprintf fresh every call, which would burn ~one alloc per
// render per Write at hundreds-of-writes/sec.
//
// Read-only after New() returns.
type sqlCache struct {
	pendingWriteInsert      string
	pendingWriteDelete      string
	partitionUpdateOCC      string
	partitionUpsertExpect   string // ExpectedVersionSet=true variant
	partitionUpsertNoExpect string // ExpectedVersionSet=false variant
	filesInsert             string
	idempotencyLookup       string
	pollLag                 string            // observable-gauge query
	pendingWritesDepth      string            // observable-gauge query
	mvInserts               map[string]string // MV.Name → INSERT SQL
}

// New constructs a Store[T] for cfg. Validates cfg, resolves
// defaults, validates the catalog schema (via
// SchemaManager.Validate) so DDL drift is caught before the
// first write rather than at first INSERT, and constructs the
// parquet encoder + S3 target.
//
// Returns *SchemaValidationError if the catalog schema doesn't
// match cfg. Operators apply schema via their migration tool
// (or SchemaManager.Create for tests) before calling New.
func New[T any](ctx context.Context, cfg Config[T]) (*Store[T], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	r := cfg.resolved()

	codec, err := resolveCompression(r.Compression)
	if err != nil {
		return nil, err
	}
	bufCap := r.EncodeBufPoolMaxBytes
	if bufCap == 0 {
		bufCap = defaultEncodeBufPoolMaxBytes
	}

	if err := NewSchemaManager(cfg).Validate(ctx); err != nil {
		return nil, err
	}

	names := catalog.NewNames(r.SchemaName, r.TablePrefix)
	extNames := make([]string, len(r.ExtensionColumns))
	for i, e := range r.ExtensionColumns {
		extNames[i] = e.Name
	}
	sql := sqlCache{
		pendingWriteInsert:      names.PendingWriteInsertSQL(),
		pendingWriteDelete:      names.PendingWriteDeleteSQL(),
		partitionUpdateOCC:      names.PartitionUpdateOCCSQL(),
		partitionUpsertExpect:   names.PartitionUpsertSQL(r.PartitionKeyParts, true),
		partitionUpsertNoExpect: names.PartitionUpsertSQL(r.PartitionKeyParts, false),
		filesInsert:             names.FilesInsertSQL(r.PartitionKeyParts, extNames),
		idempotencyLookup:       names.IdempotencyLookupSQL(extNames),
		pollLag:                 names.PollLagSQL(),
		pendingWritesDepth:      names.PendingWritesDepthSQL(),
		mvInserts:               make(map[string]string, len(cfg.MaterializedViews)),
	}
	for _, mv := range cfg.MaterializedViews {
		sql.mvInserts[mv.Name] = names.MVInsertSQL(mv.Name, mv.Columns)
	}

	// Observable-gauge data sources. Each closure runs once per
	// OTel collection cycle on the executor's pool — bounded SQL
	// against indexed columns; safe to invoke under load.
	pollLagFn := func(ctx context.Context) (time.Duration, bool, error) {
		var lagSec float64
		err := cfg.Executor.Run(ctx, func(d DBTX) error {
			return d.QueryRow(ctx, sql.pollLag).Scan(&lagSec)
		})
		// Empty s3pgstore_files (no sequenced rows yet) is the
		// cold-start state, not an error: the SELECT returns
		// zero rows and pgx surfaces ErrNoRows. Surface it as
		// "no observation" so the caller doesn't log a warning
		// every collection cycle on a fresh deployment.
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		if err != nil {
			return 0, false, err
		}
		return time.Duration(lagSec * float64(time.Second)), true, nil
	}
	pendingWritesDepthFn := func(ctx context.Context) (int64, error) {
		var n int64
		err := cfg.Executor.Run(ctx, func(d DBTX) error {
			return d.QueryRow(ctx, sql.pendingWritesDepth).Scan(&n)
		})
		return n, err
	}

	metrics, err := newMetrics(metricsConfig{
		Meter:                cfg.Meter,
		PollLagFn:            pollLagFn,
		PendingWritesDepthFn: pendingWritesDepthFn,
	})
	if err != nil {
		return nil, fmt.Errorf("register metrics: %w", err)
	}

	// The caller's S3Client must already carry the s3pgstore
	// middleware stack (adaptive retry + rate limit + metrics +
	// connection pool tuning) — install it via
	// s3client.WithDefaults at construction time. This shifts
	// the four client-level tuning knobs (max-open-connections,
	// retry budget, RPS, burst) to the s3.Client construction
	// site so multiple Stores sharing one client also share
	// one rate limiter, one adaptive token bucket, and one
	// connection pool. The s3pgstore.s3.* metric surface is
	// owned entirely by s3client and registers against the
	// caller-supplied meter at WithDefaults time.
	target, err := newS3Target(s3TargetConfig{
		S3Client: r.S3Client,
		S3Bucket: r.S3Bucket,
		S3Prefix: r.S3Prefix,
	})
	if err != nil {
		return nil, err
	}

	// WorkerPool default. Every fan-out call site routes through
	// this pool — there is no per-method goroutine fallback. When
	// the caller doesn't supply one, we install a private pool
	// sized to defaultWorkerPoolSize so a single Store's behavior
	// matches the pre-pool defaults; multi-Store deployments
	// share one pool across Stores by passing the same instance
	// via Config.WorkerPool.
	if r.WorkerPool == nil {
		p, err := pool.New(defaultWorkerPoolSize, cfg.Meter)
		if err != nil {
			return nil, fmt.Errorf("default WorkerPool: %w", err)
		}
		r.WorkerPool = p
	}

	return &Store[T]{
		cfg:      cfg,
		resolved: r,
		names:    names,
		target:   target,
		encoder: newParquetEncoder[T](codec, bufCap,
			metrics.recordEncodeBufDropped),
		metrics: metrics,
		sql:     sql,
	}, nil
}

// dataPath returns the S3-key prefix under which parquet data
// files live for this Store: "<S3Prefix>/data". A leading slash
// from cfg.S3Prefix is preserved verbatim — the library does
// not normalise prefixes.
func (s *Store[T]) dataPath() string {
	if s.resolved.S3Prefix == "" {
		return "data"
	}
	return s.resolved.S3Prefix + "/data"
}

// dataKey returns the S3 key for a single parquet file under
// partitionKey + uuid: "<Prefix>/data/<partitionKey>/<uuid>.parquet".
func (s *Store[T]) dataKey(partitionKey, uuid string) string {
	return s.dataPath() + "/" + partitionKey + "/" + uuid + ".parquet"
}

// validatePartitionKey runs the user's PartitionKeyOf and
// resolves it to per-part values via partitionKeyValues so the
// upstream contract failure ("PartitionKeyOf returned a key
// that doesn't match PartitionKeyParts") surfaces here, before
// any S3 PUT. Determinism / shape are caller responsibility per
// CLAUDE.md — the library validates the shape we observe on
// every call.
func (s *Store[T]) validatePartitionKey(rec T) (string, []string, error) {
	key := s.resolved.PartitionKeyOf(rec)
	values, err := partitionKeyValues(key, s.resolved.PartitionKeyParts)
	if err != nil {
		return "", nil, fmt.Errorf("PartitionKeyOf: %w", err)
	}
	return key, values, nil
}
