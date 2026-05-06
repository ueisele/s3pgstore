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
type WriteResult struct {
	PartitionKey string
	S3Key        string
	FileID       int64
	Version      int64
	RecordCount  int
	FileSize     int
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
) ([]WriteResult, error) {
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
	out := make([]WriteResult, len(keys))
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
//  1. Encode records to parquet bytes via the pooled encoder.
//  2. Generate a UUIDv7 for the file's S3 key.
//  3. INSERT s3pgstore_pending_writes row (orphan tracking).
//  4. PUT the parquet body to S3.
//  5. RunInTx { UPSERT s3pgstore_partitions (returns version);
//     INSERT s3pgstore_files (returns file_id); DELETE the
//     pending_writes row }.
//
// The pending_writes row is INSERTed before the S3 PUT so a
// crash between PUT and the catalog tx leaves a tracked
// orphan; the row is DELETEd inside the catalog transaction so
// rollback also rolls back the DELETE — no false-positive GC.
//
// Idempotency: when opts.idempotencyToken is set (or
// idempotencyTokenOfFn yields one), an upfront SELECT looks
// for an existing (partition_key, token) row. A hit returns
// the original WriteResult — no S3 PUT, no catalog write.
//
// OCC: when opts.expectedVersionSet, the partition upsert's
// conflict branch carries WHERE version = $expected. A
// non-match returns ErrVersionConflict.
//
// Phase 5 left ExtensionColumns / MV inserts as TODOs;
// Phase 8 wires WithMetadata, Phase 13 wires MV row insert.
// Phase 7 (this phase) handles only the (partition_key,
// idempotency_token) UNIQUE conflict path: if the SQL hits
// the conflict at INSERT time (concurrent writer beat us to
// the same token), we re-run the SELECT and return the
// landed row. Idempotency is unbounded in time per CLAUDE.md.
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

	body, err := s.encoder.encode(ctx, records)
	if err != nil {
		return WriteResult{}, fmt.Errorf("encode parquet: %w", err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return WriteResult{}, fmt.Errorf("generate UUIDv7: %w", err)
	}
	s3Key := s.dataKey(partitionKey, id.String())

	// Step 3 — INSERT pending_writes (separate tx; the row
	// must be visible before the S3 PUT lands so a crash
	// between them is recoverable by GC).
	var pendingID uuid.UUID
	if err := s.cfg.Executor.RunInTx(ctx, func(d DBTX) error {
		return d.QueryRow(ctx,
			s.names.PendingWriteInsertSQL(), s3Key,
		).Scan(&pendingID)
	}); err != nil {
		return WriteResult{}, fmt.Errorf(
			"insert pending_writes: %w", err)
	}

	// Step 4 — S3 PUT.
	if err := s.target.put(ctx, s3Key, body, "application/vnd.apache.parquet"); err != nil {
		return WriteResult{}, fmt.Errorf("S3 PUT: %w", err)
	}

	// Step 5 — single transaction containing the partition
	// upsert + file insert + pending_writes delete. On commit
	// the row is durable + visible; on rollback, the S3 file
	// stays orphaned and the pending_writes row holds the
	// reference for cmd/s3pgstore-gc.
	var (
		version int64
		fileID  int64
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
			s.resolved.SchemaVersion,
			len(body), len(records),
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
				// row's WriteResult instead of failing.
				return errTokenRaceLost
			}
			return fmt.Errorf("insert files: %w", err)
		}

		if _, err := d.Exec(ctx,
			s.names.PendingWriteDeleteSQL(), pendingID); err != nil {
			return fmt.Errorf("delete pending_writes: %w", err)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errTokenRaceLost) {
			// Re-resolve the canonical row outside any
			// rolled-back transaction. The S3 PUT we
			// performed is now an orphan tracked by
			// pending_writes (which the rollback retained,
			// since the DELETE was inside the same tx).
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
		PartitionKey: partitionKey,
		S3Key:        s3Key,
		FileID:       fileID,
		Version:      version,
		RecordCount:  len(records),
		FileSize:     len(body),
	}, nil
}
