package s3pgstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Write groups records by PartitionKeyOf, encodes one parquet
// file per group, and inserts the catalog rows. Returns a
// FileRef per partition in lex order of partition key, fully
// populated except for Offset — feed_seq is assigned by the
// sequencer asynchronously, so the returned refs carry OffsetNone.
//
// The empty input case (records=nil or empty) returns nil, nil
// — no S3 PUTs, no catalog INSERTs.
//
// The returned slice can be passed straight to ReadFileRefsIter
// for a Write → Read round-trip without an intervening Poll.
func (s *Store[T]) Write(
	ctx context.Context, records []T, opts ...WriteOption,
) (out []FileRef, err error) {
	defer s.metrics.methodScope(ctx, "Write", &err).end()

	if len(records) == 0 {
		return nil, nil
	}
	o, err := s.resolveWriteOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Group records by partition key; preserve per-partition
	// insertion order. CLAUDE.md asserts deterministic emission
	// order — sort the keys lex before fan-out so the result
	// slice is stable across runs.
	groups := make(map[string][]T)
	keyValues := make(map[string][]string)
	for _, rec := range records {
		key, values, err := s.validatePartitionKey(rec)
		if err != nil {
			return nil, err
		}
		groups[key] = append(groups[key], rec)
		if _, seen := keyValues[key]; !seen {
			keyValues[key] = values
		}
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Multi-partition fan-out: one task per partition through the
	// shared Config.WorkerPool, slot-indexed results preserve lex
	// order regardless of completion order. Concurrency caps at
	// the pool's MaxConcurrent slot count.
	//
	// Partial-failure semantics: g.Wait() blocks until every
	// submitted task returns — Write never bails out early. On the
	// first non-nil error the group ctx is cancelled, so siblings
	// that observe ctx return ctx.Err(); siblings already mid-commit
	// or mid-S3-PUT run to completion. The returned slice has
	// length len(keys); slots for failed or ctx-cancelled partitions
	// carry the zero FileRef (FileID == 0), and slots for partitions
	// that committed before cancel arrived carry their real FileRef.
	// Callers that retry should rely on WithIdempotencyToken — the
	// partial-UNIQUE short-circuit collapses retries to the
	// canonical row regardless of which partitions committed first.
	out = make([]FileRef, len(keys))
	g, gctx := s.resolved.WorkerPool.WithContext(ctx)
	for i, key := range keys {
		g.Submit(gctx, func(ctx context.Context) error {
			res, err := s.writePartition(ctx, key,
				keyValues[key], groups[key], o)
			if err != nil {
				return err
			}
			out[i] = res
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return out, err
	}
	return out, nil
}

// WriteWithKey is the single-partition shortcut: encode the
// given records as one parquet file under partitionKey,
// regardless of what PartitionKeyOf would return for them.
// Useful when the caller has already grouped records and
// doesn't want a redundant pass through PartitionKeyOf, or
// when the canonical key doesn't fit PartitionKeyOf's grammar.
//
// Validates partitionKey against PartitionKeyParts (same parser
// the Write path uses) so a typo'd key fails at call time, not
// at INSERT.
func (s *Store[T]) WriteWithKey(
	ctx context.Context, partitionKey string, records []T,
	opts ...WriteOption,
) (res FileRef, err error) {
	defer s.metrics.methodScope(ctx, "WriteWithKey", &err).end()

	if len(records) == 0 {
		return FileRef{}, errors.New(
			"WriteWithKey: records is empty")
	}
	values, err := partitionKeyValues(partitionKey, s.resolved.PartitionKeyParts)
	if err != nil {
		return FileRef{}, err
	}
	o, err := s.resolveWriteOpts(opts...)
	if err != nil {
		return FileRef{}, err
	}
	return s.writePartition(ctx, partitionKey, values, records, o)
}

// writePartition is the inner write path. Concretely:
//
//  1. Resolve the per-partition idempotency token (if any) and
//     short-circuit on a hit against the partial UNIQUE index.
//  2. Resolve MV rows up front (shape errors fail fast).
//  3. Encode parquet OUTSIDE the wrapping tx — the PG connection
//     stays in the pool during compression. On the (rare) OCC
//     mismatch later, the encoded bytes are wasted; the trade is
//     deliberate (CLAUDE.md notes WithExpectedVersion writes are
//     uncommon, the happy path is the hot path).
//  4. Generate a fresh S3 key (UUIDv7 + dataKey).
//  5. INSERT pending_writes via insertPendingWriteDetachedTx —
//     independent commit on a fresh pool connection (DetachTx).
//     The pre-tx position is load-bearing for concurrency:
//     relocating this inside the wrapping tx would force two
//     simultaneous pool connections per writer and deadlock
//     under load.
//  6. Wrapping tx via Executor.RunInTx:
//     a. upsertPartition (returns version; ErrVersionConflict on
//     OCC mismatch fails fast here — encoded bytes wasted).
//     b. insertFile (token UNIQUE violation surfaces as
//     errTokenRaceLost).
//     c. BuildS3Metadata (needs `version` from upsertPartition).
//     d. S3 PUT — s3PutAttempted flips just before so the post-
//     tx cleanup logic can tell pre-PUT failures apart. body
//     reference is dropped right after to release the encoded
//     buffer for GC during the trailing SQL.
//     e. DELETE pending_writes on the wrapping-tx connection
//     (rollback restores the row → GC sees the orphan).
//     f. insertMVRows.
//     g. notifySequencer (queued NOTIFY, delivered at COMMIT).
//  7. Translate the wrapping-tx error: errTokenRaceLost →
//     re-lookup; ErrVersionConflict → propagate; else wrap.
//     Pre-PUT failures additionally trigger a best-effort
//     deletePendingWriteDetachedTx to reclaim the orphan row
//     that GC would otherwise reap.
//
// CLAUDE.md's invariants pin the ordering rules: the file row
// before the PUT, the pending_writes pre-tx INSERT before the
// wrapping tx, the MV inserts in the same tx as the file row,
// the NOTIFY queued for COMMIT.
func (s *Store[T]) writePartition(
	ctx context.Context, partitionKey string, partValues []string,
	records []T, o writeOpts,
) (FileRef, error) {
	// 1. Token resolution + idempotency short-circuit.
	token, err := s.resolveTokenForPartition(records, o)
	if err != nil {
		return FileRef{}, err
	}
	if token != "" {
		existing, ok, err := s.lookupTokenFileRef(
			ctx, partitionKey, token)
		if err != nil {
			return FileRef{}, fmt.Errorf(
				"lookup token: %w", err)
		}
		s.metrics.recordLookupByToken(ctx, ok)
		if ok {
			return existing, nil
		}
	}

	// 2. MV rows resolved before any DB / S3 work so shape
	//    errors fail fast.
	mvRows, err := s.resolveMVRows(records)
	if err != nil {
		return FileRef{}, err
	}

	// 3. Encode parquet outside the wrapping tx. CPU-bound under
	//    compression; running here keeps the PG connection pool
	//    free for siblings.
	body, uncompressedSize, err := s.encoder.encode(ctx, records)
	if err != nil {
		return FileRef{}, fmt.Errorf("encode parquet: %w", err)
	}
	fileSize := int64(len(body))
	recordCount := int64(len(records))

	// 4. Fresh S3 key.
	s3Key, err := s.newS3Key(partitionKey)
	if err != nil {
		return FileRef{}, err
	}

	// 5. Pre-tx pending_writes (independent commit; see
	//    CLAUDE.md's orphan-tracking invariant).
	if err := s.insertPendingWriteDetachedTx(ctx, s3Key); err != nil {
		return FileRef{}, fmt.Errorf(
			"insert pending_writes: %w", err)
	}

	// 6. Wrapping tx. s3PutAttempted is the pivot the post-tx
	//    cleanup uses to tell pre-PUT failures (safe to clean
	//    up the orphan row) from PUT-or-later failures (PUT may
	//    have committed; leave for GC).
	var (
		version        int64
		fileID         int64
		writtenAt      time.Time
		s3PutAttempted bool
	)
	err = s.cfg.Executor.RunInTx(ctx, func(d DBTX) error {
		var err error
		version, err = s.upsertPartition(ctx, d, partitionUpsertOpts{
			PartitionKey:       partitionKey,
			PartValues:         partValues,
			ExpectedVersionSet: o.expectedVersionSet,
			ExpectedVersion:    o.expectedVersion,
		})
		if err != nil {
			return err
		}

		fileID, writtenAt, err = s.insertFile(ctx, d, fileInsertOpts{
			PartitionKey:     partitionKey,
			S3Key:            s3Key,
			Version:          version,
			FileSize:         fileSize,
			UncompressedSize: uncompressedSize,
			RecordCount:      recordCount,
			Token:            token,
			PartValues:       partValues,
			Metadata:         o.metadata,
		})
		if err != nil {
			return err
		}

		// Build the S3 user-metadata bag now that all values are
		// resolved (token, sizes, version, ext columns). Building
		// inside the wrapping tx — before any S3 work — means
		// validation errors (header-unsafe ext values, oversized
		// metadata) abort cleanly with no S3 side-effects.
		s3Metadata, err := BuildS3Metadata(
			token, recordCount, uncompressedSize, version,
			s.resolved.ExtensionColumns, o.metadata,
		)
		if err != nil {
			return err
		}

		s3PutAttempted = true
		if err := s.target.put(ctx, s3Key, body,
			"application/vnd.apache.parquet", s3Metadata); err != nil {
			return fmt.Errorf("S3 PUT: %w", err)
		}
		// Drop the body reference now that the SDK has consumed
		// it — under load this releases the encoded buffer for
		// GC during the trailing DELETE / MV / NOTIFY round
		// trips instead of holding it until function exit.
		body = nil

		if _, err := d.Exec(ctx,
			s.sql.pendingWriteDelete, s3Key); err != nil {
			return fmt.Errorf("delete pending_writes: %w", err)
		}

		if err := s.insertMVRows(ctx, d, mvRows); err != nil {
			return fmt.Errorf("MV inserts: %w", err)
		}
		return s.notifySequencer(ctx, d)
	})

	// 7. Error handling: optional pre-tx cleanup + translate.
	if err != nil {
		if !s3PutAttempted {
			if dErr := s.deletePendingWriteDetachedTx(ctx, s3Key); dErr != nil {
				slog.WarnContext(ctx,
					"s3pgstore: pre-tx pending_writes cleanup failed",
					"s3_key", s3Key, "err", dErr)
			}
		}
		return s.translateWriteTxErr(ctx, err, partitionKey, token)
	}

	s.metrics.recordWriteVolume(ctx, fileSize, recordCount)
	// Copy the user-supplied metadata into a fresh map so the
	// returned FileRef.Extensions has no aliasing with the
	// caller's input AND matches Read/Poll's "non-nil empty when
	// no values" shape (consistent iteration semantics).
	exts := make(map[string]any, len(o.metadata))
	for k, v := range o.metadata {
		exts[k] = v
	}
	return FileRef{
		PartitionKey:     partitionKey,
		S3Key:            s3Key,
		FileID:           fileID,
		Version:          version,
		WrittenAt:        writtenAt,
		RecordCount:      recordCount,
		FileSize:         fileSize,
		UncompressedSize: uncompressedSize,
		Extensions:       exts,
	}, nil
}

// newS3Key returns a fresh hive-style S3 key for partitionKey,
// using a UUIDv7 as the per-write suffix. Pure name derivation
// — no DB or S3 access.
func (s *Store[T]) newS3Key(partitionKey string) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate UUIDv7: %w", err)
	}
	return s.dataKey(partitionKey, id.String()), nil
}

// partitionUpsertOpts is the parameter bag for upsertPartition.
// Replaces the parallel positional-args + flag combinations the
// inline code grew over time.
type partitionUpsertOpts struct {
	PartitionKey       string
	PartValues         []string
	ExpectedVersionSet bool
	ExpectedVersion    int64
}

// upsertPartition bumps s3pgstore_partitions.version for the
// configured partition and returns the new value. Two shapes:
//
//   - ExpectedVersionSet && ExpectedVersion > 0: pure UPDATE
//     WHERE partition_key = $ AND version = $expected. A missing
//     partition or stale version returns ErrVersionConflict.
//   - else: UPSERT (INSERT new at version=1, or UPDATE conflict
//     branch bumping by 1). ExpectedVersionSet with value 0
//     gates the conflict branch on version=0; absence allows
//     any prior version.
//
// Must run inside a wrapping tx (caller passes the tx's DBTX).
func (s *Store[T]) upsertPartition(
	ctx context.Context, d DBTX, opts partitionUpsertOpts,
) (int64, error) {
	var version int64
	if opts.ExpectedVersionSet && opts.ExpectedVersion > 0 {
		err := d.QueryRow(ctx,
			s.sql.partitionUpdateOCC,
			opts.PartitionKey, opts.ExpectedVersion,
		).Scan(&version)
		if err != nil {
			if isNoRowsErr(err) {
				return 0, ErrVersionConflict
			}
			return 0, fmt.Errorf("update partitions (OCC): %w", err)
		}
		return version, nil
	}
	args := make([]any, 0, 1+len(opts.PartValues))
	args = append(args, opts.PartitionKey)
	for _, v := range opts.PartValues {
		args = append(args, v)
	}
	upsertSQL := s.sql.partitionUpsertNoExpect
	if opts.ExpectedVersionSet {
		upsertSQL = s.sql.partitionUpsertExpect
	}
	err := d.QueryRow(ctx, upsertSQL, args...).Scan(&version)
	if err != nil {
		if isNoRowsErr(err) && opts.ExpectedVersionSet {
			return 0, ErrVersionConflict
		}
		return 0, fmt.Errorf("upsert partitions: %w", err)
	}
	return version, nil
}

// fileInsertOpts is the parameter bag for insertFile. The named fields
// replace the previous positional []any plumbing and align with
// the column ordering documented on FilesInsertSQL.
type fileInsertOpts struct {
	PartitionKey     string
	S3Key            string
	Version          int64
	FileSize         int64
	UncompressedSize int64
	RecordCount      int64
	Token            string         // "" → SQL NULL
	PartValues       []string       // matches PartitionKeyParts order
	Metadata         map[string]any // sourced from WithMetadata
}

// insertFile writes one s3pgstore_files row inside the wrapping
// tx and returns the assigned file_id and the server-side
// written_at timestamp. A unique-violation on (partition_key,
// idempotency_token) — the concurrent-token race window — is
// translated to errTokenRaceLost so the caller can re-lookup the
// canonical row after rollback.
//
// Positional args mirror s.sql.filesInsert's parameter list:
// $1=partition_key, $2=s3_key, $3=written_at_version,
// $4=file_size, $5=uncompressed_size, $6=record_count,
// $7=idempotency_token, $8…=part_<n>, then ext_<n>.
func (s *Store[T]) insertFile(
	ctx context.Context, d DBTX, row fileInsertOpts,
) (int64, time.Time, error) {
	hasToken := row.Token != ""
	args := make([]any, 0,
		7+len(row.PartValues)+len(s.resolved.ExtensionColumns))
	args = append(args,
		row.PartitionKey, row.S3Key, row.Version,
		row.FileSize, row.UncompressedSize, row.RecordCount,
		nullableString(row.Token))
	for _, v := range row.PartValues {
		args = append(args, v)
	}
	for _, e := range s.resolved.ExtensionColumns {
		args = append(args, metadataValueFor(row.Metadata, e))
	}
	var (
		fileID    int64
		writtenAt time.Time
	)
	err := d.QueryRow(ctx, s.sql.filesInsert, args...).
		Scan(&fileID, &writtenAt)
	if err != nil {
		if hasToken && isUniqueViolation(err) {
			return 0, time.Time{}, errTokenRaceLost
		}
		return 0, time.Time{}, fmt.Errorf("insert files: %w", err)
	}
	return fileID, writtenAt, nil
}

// nullableString maps the empty-string sentinel to a SQL NULL
// (pgx encodes a nil any as NULL), leaving non-empty values
// untouched. Used for columns whose "absent" state must hit a
// partial-index NULL branch — currently only
// s3pgstore_files.idempotency_token, whose partial UNIQUE gates
// on WHERE idempotency_token IS NOT NULL.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// notifySequencer issues the LISTEN/NOTIFY wake-up for the
// sequencer inside the wrapping tx. Empty NotifyChannel is a
// no-op (sequencer falls back to interval polling). NOTIFY is
// queued at the tx level and delivered at COMMIT, so a rolled-
// back catalog write produces no notification.
func (s *Store[T]) notifySequencer(ctx context.Context, d DBTX) error {
	if s.resolved.NotifyChannel == "" {
		return nil
	}
	if _, err := d.Exec(ctx,
		"SELECT pg_notify($1, $2)",
		s.resolved.NotifyChannel, "",
	); err != nil {
		return fmt.Errorf("pg_notify: %w", err)
	}
	return nil
}

// translateWriteTxErr maps a wrapping-tx error to the
// (FileRef, error) the caller of writePartition should see:
//
//   - errTokenRaceLost: re-lookup the canonical (partition_key,
//     idempotency_token) row and return it. A vanished row means
//     the winner also rolled back; surface a retryable error.
//   - ErrVersionConflict: propagate as-is (caller's WithExpected
//     Version contract).
//   - any other: wrap with "catalog tx".
func (s *Store[T]) translateWriteTxErr(
	ctx context.Context, err error, partitionKey, token string,
) (FileRef, error) {
	if errors.Is(err, errTokenRaceLost) {
		s.metrics.recordTokenRaceRetry(ctx)
		existing, ok, lookupErr := s.lookupTokenFileRef(
			ctx, partitionKey, token)
		if lookupErr != nil {
			return FileRef{}, fmt.Errorf(
				"token-race re-lookup: %w", lookupErr)
		}
		s.metrics.recordLookupByToken(ctx, ok)
		if !ok {
			return FileRef{}, errors.New(
				"token UNIQUE conflict but lookup " +
					"found no row (concurrent rollback)")
		}
		return existing, nil
	}
	if errors.Is(err, ErrVersionConflict) {
		s.metrics.recordOCCConflict(ctx)
		return FileRef{}, ErrVersionConflict
	}
	return FileRef{}, fmt.Errorf("catalog tx: %w", err)
}

// insertPendingWriteDetachedTx writes the orphan-tracking row
// on a fresh pool connection, bypassing any caller-supplied tx
// via Executor.DetachTx so it commits independently of the
// surrounding wrapping tx and of any caller WithTx. The row's
// purpose is to outlive a wrapping-tx rollback so GC can reap
// the corresponding S3 object — that requires an independent
// commit. A single-statement Exec auto-commits, so Run is
// sufficient (no RunInTx needed). The DetachedTx suffix is
// load-bearing in the name: callers MUST NOT reach for this
// helper from inside a tx that's expected to govern its
// visibility.
func (s *Store[T]) insertPendingWriteDetachedTx(
	ctx context.Context, s3Key string,
) error {
	indCtx := s.cfg.Executor.DetachTx(ctx)
	return s.cfg.Executor.Run(indCtx, func(d DBTX) error {
		_, err := d.Exec(indCtx,
			s.sql.pendingWriteInsert, s3Key)
		return err
	})
}

// deletePendingWriteDetachedTx removes a pending_writes row on
// a fresh pool connection (DetachTx, so independent of any
// caller-supplied tx). Used by the writePartition error path
// to proactively reclaim the orphan record on fail-fast cases
// where no S3 PUT was attempted; the success-path DELETE on
// the wrapping-tx connection lives inline so a rollback
// restores the row. On PUT-attempted error paths the row stays
// and GC handles it.
//
// Best-effort: the caller logs a warning and ignores any
// error here, since cmd/s3pgstore-gc remains the safety net.
func (s *Store[T]) deletePendingWriteDetachedTx(
	ctx context.Context, s3Key string,
) error {
	indCtx := s.cfg.Executor.DetachTx(ctx)
	return s.cfg.Executor.Run(indCtx, func(d DBTX) error {
		_, err := d.Exec(indCtx,
			s.sql.pendingWriteDelete, s3Key)
		return err
	})
}
