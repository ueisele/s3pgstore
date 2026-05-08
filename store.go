package s3pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/parquet-go/parquet-go/compress"

	"github.com/ueisele/s3pgstore/internal/catalog"
)

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
	metrics  *Metrics
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
		idempotencyLookup:       names.IdempotencyLookupSQL(),
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

	target, err := newS3Target(s3TargetConfig{
		S3Client: r.S3Client,
		Bucket:   r.Bucket,
		Prefix:   r.Prefix,
		Metrics:  metrics,
	})
	if err != nil {
		return nil, err
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
// files live for this Store: "<Prefix>/data". A leading slash
// from cfg.Prefix is preserved verbatim — the library does not
// normalise prefixes.
func (s *Store[T]) dataPath() string {
	if s.resolved.Prefix == "" {
		return "data"
	}
	return s.resolved.Prefix + "/data"
}

// dataKey returns the S3 key for a single parquet file under
// partitionKey + uuid: "<Prefix>/data/<partitionKey>/<uuid>.parquet".
func (s *Store[T]) dataKey(partitionKey, uuid string) string {
	return s.dataPath() + "/" + partitionKey + "/" + uuid + ".parquet"
}

// codecForRead returns the compression codec we'd resolve given
// resolved.Compression. Used internally for the lifetime test
// path; kept private.
//
//nolint:unused // kept for future Phase 6 use
func (s *Store[T]) codecForRead() (compress.Codec, error) {
	return resolveCompression(s.resolved.Compression)
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
