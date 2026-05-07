package s3pgstore

import (
	"context"
	"errors"
	"fmt"
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

// writePartition is the inner write path. Outline:
//
//  1. Idempotency short-circuit: SELECT on the (partition_key,
//     idempotency_token) partial UNIQUE skips every step below
//     when a prior write already committed for this token.
//  2. Resolve MV rows up front (shape errors fail fast).
//  3. Generate a UUIDv7 for the file's S3 key.
//  4. INSERT s3pgstore_pending_writes (separate tx, before the
//     wrapping tx so the orphan tracking row survives a
//     wrapping-tx rollback).
//  5. RunInTx wrapping {
//     a. UPSERT s3pgstore_partitions (returns version) — OCC
//     mismatch fails fast here, before any encode or PUT.
//     b. Encode parquet via the pooled encoder; the byte length
//     is file_size, the per-chunk metadata sums to
//     uncompressed_size.
//     c. INSERT s3pgstore_files with both sizes — UNIQUE
//     violation on (partition_key, idempotency_token)
//     surfaces the concurrent-token-race window as
//     errTokenRaceLost.
//     d. PUT the parquet body to S3.
//     e. DELETE the pending_writes row.
//     f. INSERT MV rows.
//     g. NOTIFY the sequencer (queued at COMMIT).
//     }
//
// The wrapping tx holds the DB connection across the S3 PUT.
// CLAUDE.md's invariants assume this is acceptable for the
// 1-writer-per-partition workload — partition row-lock
// contention is essentially zero, so the longer hold doesn't
// serialize unrelated work.
//
// Orphan tracking still works: the pending_writes row commits
// in its own tx before the wrapping tx begins, so a wrapping-
// tx rollback after the S3 PUT succeeded leaves an S3 object
// with no catalog reference and a pending_writes row pointing
// at it. cmd/s3pgstore-gc reaps it after a grace period.
//
// Idempotency: a retry of a previously-completed write hits
// step 1 and returns immediately — no encode, no PUT, no
// catalog tx. A concurrent-token-race (two writers with the
// same token, both past the step-1 SELECT) loses one of the
// step-5c INSERTs to UNIQUE violation; the loser rolls back
// and re-fetches the canonical row.
//
// OCC: when opts.expectedVersionSet, step 5a's UPDATE/UPSERT
// carries WHERE version = $expected. A non-match returns
// ErrVersionConflict and the wrapping tx rolls back BEFORE
// encode + PUT — fail-fast on a stale version costs only the
// pending_writes pre-tx + a tx round-trip.
func (s *Store[T]) writePartition(
	ctx context.Context, partitionKey string, partValues []string,
	records []T, o writeOpts,
) (WriteResult, error) {
	// Resolve the per-partition idempotency token (if any).
	token, err := s.resolveTokenForPartition(records, o)
	if err != nil {
		return WriteResult{}, err
	}

	// Idempotency short-circuit: if the token already maps to
	// a file row, return the existing WriteResult and skip
	// every other step.
	if token != "" {
		if existing, ok, err := s.lookupTokenWriteResult(
			ctx, partitionKey, token); err != nil {
			return WriteResult{}, fmt.Errorf(
				"lookup token: %w", err)
		} else if ok {
			return existing, nil
		}
	}

	// Resolve MV rows up front so shape errors surface before
	// any S3 PUT. The resolved rows are inserted inside the
	// wrapping tx so MV state tracks file state under both
	// commit and rollback.
	mvRows, err := s.resolveMVRows(records)
	if err != nil {
		return WriteResult{}, err
	}

	id, err := uuid.NewV7()
	if err != nil {
		return WriteResult{}, fmt.Errorf("generate UUIDv7: %w", err)
	}
	s3Key := s.dataKey(partitionKey, id.String())

	// INSERT pending_writes on its own pool connection, with
	// the caller's tx (if any) detached from the context. The
	// row MUST commit independently: orphan tracking depends on
	// it surviving a rollback of either the wrapping tx that
	// follows or — if the caller composed via WithTx — the
	// caller's outer tx. A single-statement Exec auto-commits,
	// so Run is sufficient (no need for RunInTx).
	indCtx := s.cfg.Executor.DetachTx(ctx)
	if err := s.cfg.Executor.Run(indCtx, func(d DBTX) error {
		_, err := d.Exec(indCtx,
			s.names.PendingWriteInsertSQL(), s3Key)
		return err
	}); err != nil {
		return WriteResult{}, fmt.Errorf(
			"insert pending_writes: %w", err)
	}

	// Wrapping tx. Holds the DB connection across encode + PUT.
	// On commit the row is durable + visible; on rollback the
	// pending_writes row from the pre-tx tracks any S3 orphan.
	var (
		version          int64
		fileID           int64
		body             []byte
		uncompressedSize int64
	)
	err = s.cfg.Executor.RunInTx(ctx, func(d DBTX) error {
		// Partition upsert: bumps version atomically and
		// returns the new value. WithExpectedVersion takes
		// two shapes:
		//   - expected==0: full upsert with CAS on the
		//     conflict branch (matches the "no writes yet"
		//     spec — INSERT or UPDATE WHERE version=0).
		//   - expected>0: pure UPDATE WHERE version=N. No
		//     INSERT branch — a missing partition fails the
		//     CAS like any stale-version write would.
		//
		// Either failure mode (ErrVersionConflict) trips here
		// before encode/PUT, so an OCC miss costs nothing more
		// than a tx round-trip.
		switch {
		case o.expectedVersionSet && o.expectedVersion > 0:
			row := d.QueryRow(ctx,
				s.names.PartitionUpdateOCCSQL(),
				partitionKey, o.expectedVersion)
			if err := row.Scan(&version); err != nil {
				if isNoRowsErr(err) {
					return ErrVersionConflict
				}
				return fmt.Errorf("update partitions (OCC): %w", err)
			}
		default:
			upsertArgs := make([]any, 0, 1+len(partValues))
			upsertArgs = append(upsertArgs, partitionKey)
			for _, v := range partValues {
				upsertArgs = append(upsertArgs, v)
			}
			row := d.QueryRow(ctx,
				s.names.PartitionUpsertSQL(s.resolved.PartitionKeyParts,
					o.expectedVersionSet),
				upsertArgs...)
			if err := row.Scan(&version); err != nil {
				if isNoRowsErr(err) && o.expectedVersionSet {
					return ErrVersionConflict
				}
				return fmt.Errorf("upsert partitions: %w", err)
			}
		}

		// Encode after the version bump so an OCC failure
		// short-circuits before encode. Ordering ensures every
		// committed file row has the correct uncompressed_size
		// and file_size set from the actual encoded bytes.
		body, uncompressedSize, err = s.encoder.encode(ctx, records)
		if err != nil {
			return fmt.Errorf("encode parquet: %w", err)
		}

		// File insert. The token (when present) goes into
		// idempotency_token; the partial UNIQUE index on
		// (partition_key, idempotency_token) WHERE
		// idempotency_token IS NOT NULL backstops races —
		// a concurrent writer that landed the same token
		// before us causes a unique-violation here, which
		// we translate into a re-fetch of the canonical row.
		var tokenArg any
		if token != "" {
			tokenArg = token
		}
		fileArgs := []any{
			partitionKey, s3Key, version,
			len(body), uncompressedSize, len(records),
			tokenArg,
		}
		for _, v := range partValues {
			fileArgs = append(fileArgs, v)
		}
		// ExtensionColumns: positional values mirror the
		// declaration order. Missing keys yield SQL NULL.
		for _, c := range s.resolved.ExtensionColumns {
			fileArgs = append(fileArgs, metadataValueFor(o.metadata, c))
		}
		extNames := make([]string, len(s.resolved.ExtensionColumns))
		for i, e := range s.resolved.ExtensionColumns {
			extNames[i] = e.Name
		}
		if err := d.QueryRow(ctx,
			s.names.FilesInsertSQL(s.resolved.PartitionKeyParts, extNames),
			fileArgs...,
		).Scan(&fileID); err != nil {
			if isUniqueViolation(err) && token != "" {
				// Concurrent writer landed the same token
				// after our pre-flight SELECT but before
				// our INSERT. Re-fetch and return their
				// row's WriteResult instead of failing. We
				// haven't done the S3 PUT yet, so no orphan
				// is created on this path beyond the
				// pending_writes row from the pre-tx.
				return errTokenRaceLost
			}
			return fmt.Errorf("insert files: %w", err)
		}

		// S3 PUT lands AFTER the file row INSERT — that way a
		// concurrent-token-race never wastes an S3 PUT.
		if err := s.target.put(ctx, s3Key, body,
			"application/vnd.apache.parquet"); err != nil {
			return fmt.Errorf("S3 PUT: %w", err)
		}

		if _, err := d.Exec(ctx,
			s.names.PendingWriteDeleteSQL(), s3Key); err != nil {
			return fmt.Errorf("delete pending_writes: %w", err)
		}

		// Materialized-view inserts. Same tx as the file row,
		// so MV consistency tracks file consistency under both
		// commit and rollback.
		if err := s.insertMVRows(ctx, d, mvRows); err != nil {
			return fmt.Errorf("MV inserts: %w", err)
		}

		// Sequencer wake-up. NOTIFY is queued by PostgreSQL
		// inside the tx and delivered at COMMIT, so a rolled-
		// back catalog write produces no notification — exactly
		// the right semantics for "tell the sequencer a new row
		// is ready to be assigned." Empty NotifyChannel disables
		// LISTEN/NOTIFY entirely; the sequencer falls back to
		// interval polling.
		if s.resolved.NotifyChannel != "" {
			if _, err := d.Exec(ctx,
				"SELECT pg_notify($1, $2)",
				s.resolved.NotifyChannel, "",
			); err != nil {
				return fmt.Errorf("pg_notify: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errTokenRaceLost) {
			// Re-resolve the canonical row outside the
			// rolled-back transaction. No S3 PUT happened on
			// this path, so the only orphan to clean up is the
			// pending_writes row from the pre-tx — GC reaps
			// it with a no-op DELETE on a non-existent S3 key.
			existing, ok, lookupErr := s.lookupTokenWriteResult(
				ctx, partitionKey, token)
			if lookupErr != nil {
				return WriteResult{}, fmt.Errorf(
					"token-race re-lookup: %w", lookupErr)
			}
			if !ok {
				// Lost the race but the row vanished — the
				// other writer must have rolled back too.
				// Surface the original error so the caller
				// can retry.
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
