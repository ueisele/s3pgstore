package s3pgstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/google/uuid"
)

// WriteResult is returned per partition from Write and from
// WriteWithKey. FileID is the catalog row's BIGSERIAL primary
// key; Version is the partition row's version after the bump
// this Write applied.
//
// FileSize is the compressed parquet bytes (the size of the
// object on S3). UncompressedSize sums TotalUncompressedSize
// across every column chunk in the file — a useful denominator
// for compression-ratio dashboards and a planning aid for
// downstream consumers that materialize the decoded data.
type WriteResult struct {
	PartitionKey     string
	S3Key            string
	FileID           int64
	Version          int64
	RecordCount      int
	FileSize         int
	UncompressedSize int64
}

// WriteOption is the interface implemented by Write
// modifiers. Phase 5 ships no options — they're added in
// Phase 7 (idempotency, OCC) and Phase 8 (metadata). The
// type exists now so the API surface stays stable.
type WriteOption interface {
	applyWrite(*writeOpts)
}

// writeOpts is the resolved option bag passed down the write
// path. Fields are per-call; the zero value is the
// no-token / no-OCC / no-metadata default.
type writeOpts struct {
	idempotencyToken     *string
	idempotencyTokenOf   func() (string, error)
	idempotencyTokenOfFn func(records any) (string, error)
	expectedVersionSet   bool
	expectedVersion      int64
	metadata             map[string]any
}

func (s *Store[T]) resolveWriteOpts(opts ...WriteOption) (writeOpts, error) {
	var o writeOpts
	for _, opt := range opts {
		opt.applyWrite(&o)
	}
	if o.idempotencyToken != nil && o.idempotencyTokenOfFn != nil {
		return writeOpts{}, errors.New(
			"WithIdempotencyToken and " +
				"WithIdempotencyTokenOf are mutually exclusive")
	}
	if err := validateMetadata(o.metadata,
		s.resolved.ExtensionColumns); err != nil {
		return writeOpts{}, err
	}
	return o, nil
}

// Write groups records by PartitionKeyOf, encodes one parquet
// file per group, and inserts the catalog rows. Returns a
// WriteResult per partition in lex order of partition key.
//
// The empty input case (records=nil or empty) returns nil, nil
// — no S3 PUTs, no catalog INSERTs.
func (s *Store[T]) Write(
	ctx context.Context, records []T, opts ...WriteOption,
) (out []WriteResult, err error) {
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

	// Multi-partition fan-out: one worker per partition, slot-
	// indexed results preserve lex order regardless of completion
	// order. Concurrency caps at the s3target's effective
	// concurrency — extra partition workers would just queue on
	// the inner S3 semaphore.
	//
	// Partial-failure semantics: on first error fanOut cancels
	// in-flight siblings, but partitions whose catalog tx already
	// committed before cancel reaches them stay committed. The
	// returned slice has length len(keys); failed partitions
	// carry the zero WriteResult (FileID == 0). Callers that
	// retry should rely on WithIdempotencyToken — the
	// partial-UNIQUE short-circuit collapses retries to the
	// canonical row regardless of which partitions committed
	// first.
	out = make([]WriteResult, len(keys))
	if err := fanOut(ctx, keys, s.target.effectiveConcurrency(), nil,
		func(ctx context.Context, i int, key string) error {
			res, err := s.writePartition(ctx, key,
				keyValues[key], groups[key], o)
			if err != nil {
				return err
			}
			out[i] = res
			return nil
		},
	); err != nil {
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
) (WriteResult, error) {
	if len(records) == 0 {
		return WriteResult{}, errors.New(
			"WriteWithKey: records is empty")
	}
	values, err := partitionKeyValues(partitionKey, s.resolved.PartitionKeyParts)
	if err != nil {
		return WriteResult{}, err
	}
	o, err := s.resolveWriteOpts(opts...)
	if err != nil {
		return WriteResult{}, err
	}
	return s.writePartition(ctx, partitionKey, values, records, o)
}

// writePartition is the inner write path. Concretely:
//
//  1. Resolve the per-partition idempotency token (if any) and
//     short-circuit on a hit against the partial UNIQUE index.
//  2. Resolve MV rows up front (shape errors fail fast).
//  3. Generate a fresh S3 key (UUIDv7 + dataKey).
//  4. INSERT pending_writes via insertPendingWrite — independent
//     commit on a fresh pool connection (DetachTx). The pre-tx
//     position is load-bearing for concurrency: relocating this
//     inside the wrapping tx would force two simultaneous pool
//     connections per writer and deadlock under load.
//  5. Wrapping tx via Executor.RunInTx:
//     a. upsertPartition (returns version; ErrVersionConflict on
//     OCC mismatch fails fast here).
//     b. Encode parquet (file_size + uncompressed_size).
//     c. insertFile (token UNIQUE violation surfaces as
//     errTokenRaceLost).
//     d. S3 PUT — s3PutAttempted flips just before so the post-
//     tx cleanup logic can tell pre-PUT failures apart.
//     e. DELETE pending_writes on the wrapping-tx connection
//     (rollback restores the row → GC sees the orphan).
//     f. insertMVRows.
//     g. notifySequencer (queued NOTIFY, delivered at COMMIT).
//  6. Translate the wrapping-tx error: errTokenRaceLost →
//     re-lookup; ErrVersionConflict → propagate; else wrap.
//     Pre-PUT failures additionally trigger a best-effort
//     deletePendingWrite to reclaim the orphan row that GC
//     would otherwise reap.
//
// CLAUDE.md's invariants pin the ordering rules: the file row
// before the PUT, the pending_writes pre-tx INSERT before the
// wrapping tx, the MV inserts in the same tx as the file row,
// the NOTIFY queued for COMMIT.
func (s *Store[T]) writePartition(
	ctx context.Context, partitionKey string, partValues []string,
	records []T, o writeOpts,
) (WriteResult, error) {
	// 1. Token resolution + idempotency short-circuit.
	token, err := s.resolveTokenForPartition(records, o)
	if err != nil {
		return WriteResult{}, err
	}
	if token != "" {
		existing, ok, err := s.lookupTokenWriteResult(
			ctx, partitionKey, token)
		if err != nil {
			return WriteResult{}, fmt.Errorf(
				"lookup token: %w", err)
		}
		if ok {
			return existing, nil
		}
	}

	// 2. MV rows resolved before any DB / S3 work so shape
	//    errors fail fast.
	mvRows, err := s.resolveMVRows(records)
	if err != nil {
		return WriteResult{}, err
	}

	// 3. Fresh S3 key.
	s3Key, err := s.newS3Key(partitionKey)
	if err != nil {
		return WriteResult{}, err
	}

	// 4. Pre-tx pending_writes (independent commit; see
	//    CLAUDE.md's orphan-tracking invariant).
	if err := s.insertPendingWrite(ctx, s3Key); err != nil {
		return WriteResult{}, fmt.Errorf(
			"insert pending_writes: %w", err)
	}

	// 5. Wrapping tx. s3PutAttempted is the pivot the post-tx
	//    cleanup uses to tell pre-PUT failures (safe to clean
	//    up the orphan row) from PUT-or-later failures (PUT may
	//    have committed; leave for GC).
	var (
		version          int64
		fileID           int64
		body             []byte
		uncompressedSize int64
		s3PutAttempted   bool
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

		body, uncompressedSize, err = s.encoder.encode(ctx, records)
		if err != nil {
			return fmt.Errorf("encode parquet: %w", err)
		}

		fileID, err = s.insertFile(ctx, d, fileInsertOpts{
			PartitionKey:     partitionKey,
			S3Key:            s3Key,
			Version:          version,
			FileSize:         int64(len(body)),
			UncompressedSize: uncompressedSize,
			RecordCount:      int64(len(records)),
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
			token, int64(len(records)), uncompressedSize, version,
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

		if _, err := d.Exec(ctx,
			s.names.PendingWriteDeleteSQL(), s3Key); err != nil {
			return fmt.Errorf("delete pending_writes: %w", err)
		}

		if err := s.insertMVRows(ctx, d, mvRows); err != nil {
			return fmt.Errorf("MV inserts: %w", err)
		}
		return s.notifySequencer(ctx, d)
	})

	// 6. Error handling: optional pre-tx cleanup + translate.
	if err != nil {
		if !s3PutAttempted {
			if dErr := s.deletePendingWrite(ctx, s3Key); dErr != nil {
				slog.WarnContext(ctx,
					"s3pgstore: pre-tx pending_writes cleanup failed",
					"s3_key", s3Key, "err", dErr)
			}
		}
		return s.translateWriteTxErr(ctx, err, partitionKey, token)
	}

	return WriteResult{
		PartitionKey:     partitionKey,
		S3Key:            s3Key,
		FileID:           fileID,
		Version:          version,
		RecordCount:      len(records),
		FileSize:         len(body),
		UncompressedSize: uncompressedSize,
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
			s.names.PartitionUpdateOCCSQL(),
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
	err := d.QueryRow(ctx,
		s.names.PartitionUpsertSQL(s.resolved.PartitionKeyParts,
			opts.ExpectedVersionSet),
		args...,
	).Scan(&version)
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
// tx and returns the assigned file_id. A unique-violation on
// (partition_key, idempotency_token) — the concurrent-token
// race window — is translated to errTokenRaceLost so the
// caller can re-lookup the canonical row after rollback.
//
// Positional args are still required by the SQL but are built
// once, in order, mirroring FilesInsertSQL's documented param
// list ($1=partition_key, $2=s3_key, $3=written_at_version,
// $4=file_size, $5=uncompressed_size, $6=record_count,
// $7=idempotency_token, $8…=part_<n>, then ext_<n>).
func (s *Store[T]) insertFile(
	ctx context.Context, d DBTX, row fileInsertOpts,
) (int64, error) {
	var tokenArg any
	if row.Token != "" {
		tokenArg = row.Token
	}
	args := []any{
		row.PartitionKey, row.S3Key, row.Version,
		row.FileSize, row.UncompressedSize, row.RecordCount,
		tokenArg,
	}
	for _, v := range row.PartValues {
		args = append(args, v)
	}
	extNames := make([]string, len(s.resolved.ExtensionColumns))
	for i, e := range s.resolved.ExtensionColumns {
		extNames[i] = e.Name
		args = append(args, metadataValueFor(row.Metadata, e))
	}
	var fileID int64
	err := d.QueryRow(ctx,
		s.names.FilesInsertSQL(s.resolved.PartitionKeyParts, extNames),
		args...,
	).Scan(&fileID)
	if err != nil {
		if isUniqueViolation(err) && row.Token != "" {
			return 0, errTokenRaceLost
		}
		return 0, fmt.Errorf("insert files: %w", err)
	}
	return fileID, nil
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
// (WriteResult, error) the caller of writePartition should see:
//
//   - errTokenRaceLost: re-lookup the canonical (partition_key,
//     idempotency_token) row and return it. A vanished row means
//     the winner also rolled back; surface a retryable error.
//   - ErrVersionConflict: propagate as-is (caller's WithExpected
//     Version contract).
//   - any other: wrap with "catalog tx".
func (s *Store[T]) translateWriteTxErr(
	ctx context.Context, err error, partitionKey, token string,
) (WriteResult, error) {
	if errors.Is(err, errTokenRaceLost) {
		existing, ok, lookupErr := s.lookupTokenWriteResult(
			ctx, partitionKey, token)
		if lookupErr != nil {
			return WriteResult{}, fmt.Errorf(
				"token-race re-lookup: %w", lookupErr)
		}
		if !ok {
			return WriteResult{}, errors.New(
				"token UNIQUE conflict but lookup " +
					"found no row (concurrent rollback)")
		}
		return existing, nil
	}
	if errors.Is(err, ErrVersionConflict) {
		return WriteResult{}, ErrVersionConflict
	}
	return WriteResult{}, fmt.Errorf("catalog tx: %w", err)
}

// insertPendingWrite writes the orphan-tracking row on a fresh
// pool connection, bypassing any caller-supplied tx via
// Executor.DetachTx so it commits independently of the
// surrounding wrapping tx and of any caller WithTx. The row's
// purpose is to outlive a wrapping-tx rollback so GC can reap
// the corresponding S3 object — that requires an independent
// commit. A single-statement Exec auto-commits, so Run is
// sufficient (no RunInTx needed).
func (s *Store[T]) insertPendingWrite(
	ctx context.Context, s3Key string,
) error {
	indCtx := s.cfg.Executor.DetachTx(ctx)
	return s.cfg.Executor.Run(indCtx, func(d DBTX) error {
		_, err := d.Exec(indCtx,
			s.names.PendingWriteInsertSQL(), s3Key)
		return err
	})
}

// deletePendingWrite removes a pre-tx pending_writes row on a
// fresh pool connection (DetachTx, so independent of any
// caller-supplied tx). Used by the writePartition error path
// to proactively reclaim the orphan record on fail-fast cases
// where no S3 PUT was attempted; on PUT-attempted paths the
// row stays and GC handles it.
//
// Best-effort: the caller logs a warning and ignores any
// error here, since cmd/s3pgstore-gc remains the safety net.
func (s *Store[T]) deletePendingWrite(
	ctx context.Context, s3Key string,
) error {
	indCtx := s.cfg.Executor.DetachTx(ctx)
	return s.cfg.Executor.Run(indCtx, func(d DBTX) error {
		_, err := d.Exec(indCtx,
			s.names.PendingWriteDeleteSQL(), s3Key)
		return err
	})
}
