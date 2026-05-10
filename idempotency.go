package s3pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// errTokenRaceLost is the sentinel writePartition uses to
// signal "the partial UNIQUE on (partition_key,
// idempotency_token) bounced our INSERT — a concurrent writer
// landed first and we should re-fetch their canonical row."
//
//nolint:gochecknoglobals // sentinel
var errTokenRaceLost = errors.New(
	"token UNIQUE conflict — another writer landed first")

// resolveTokenForPartition returns the idempotency token for a
// partition's records, or "" when no idempotency option was
// passed.
//
// WithIdempotencyToken contributes a fixed token; on any
// partition the returned value is the same.
//
// WithIdempotencyTokenOf invokes the per-partition closure on
// the records. The closure returns ("", nil) to opt that
// partition out of token-based dedup; an error short-circuits
// the whole Write.
func (s *Store[T]) resolveTokenForPartition(
	records []T, o writeOpts,
) (string, error) {
	if o.idempotencyToken != nil {
		return *o.idempotencyToken, nil
	}
	if o.idempotencyTokenOfFn != nil {
		token, err := o.idempotencyTokenOfFn(records)
		if err != nil {
			return "", fmt.Errorf(
				"WithIdempotencyTokenOf: %w", err)
		}
		return token, nil
	}
	return "", nil
}

// lookupTokenFileRef resolves an existing
// (partition_key, idempotency_token) row to its FileRef.
// Returns (zero, false, nil) when no row matches.
func (s *Store[T]) lookupTokenFileRef(
	ctx context.Context, partitionKey, token string,
) (FileRef, bool, error) {
	e := FileRef{
		Extensions: make(map[string]any,
			len(s.resolved.ExtensionColumns)),
	}
	var (
		feedSeq   *int64
		extValues = make([]any, len(s.resolved.ExtensionColumns))
	)
	err := s.cfg.Executor.Run(ctx, func(d DBTX) error {
		scanArgs := []any{
			&e.FileID, &e.PartitionKey, &e.S3Key, &e.Version,
			&e.WrittenAt, &e.FileSize, &e.UncompressedSize,
			&e.RecordCount, &feedSeq,
		}
		for i := range extValues {
			scanArgs = append(scanArgs, &extValues[i])
		}
		row := d.QueryRow(ctx,
			s.sql.idempotencyLookup,
			partitionKey, token)
		return row.Scan(scanArgs...)
	})
	if err != nil {
		if isNoRowsErr(err) {
			return FileRef{}, false, nil
		}
		return FileRef{}, false, err
	}
	if feedSeq != nil {
		e.Offset = *feedSeq
	}
	for i, c := range s.resolved.ExtensionColumns {
		if extValues[i] != nil {
			e.Extensions[c.Name] = extValues[i]
		}
	}
	return e, true, nil
}

// LookupByToken probes the (partition_key, idempotency_token)
// partial UNIQUE index without writing. Useful for
// orchestrators that want to short-circuit before paying the
// encode + S3 PUT cost on a known-completed write.
//
// Returns (zero, false, nil) when no row matches; the second
// return distinguishes "missing" from "transport error."
func (s *Store[T]) LookupByToken(
	ctx context.Context, partitionKey, token string,
) (res FileRef, hit bool, err error) {
	defer s.metrics.methodScope(ctx, "LookupByToken", &err).end()

	if partitionKey == "" {
		return FileRef{}, false, errors.New(
			"LookupByToken: partitionKey is empty")
	}
	if token == "" {
		return FileRef{}, false, errors.New(
			"LookupByToken: token is empty")
	}
	if _, err := partitionKeyValues(partitionKey,
		s.resolved.PartitionKeyParts); err != nil {
		return FileRef{}, false, err
	}
	res, hit, err = s.lookupTokenFileRef(ctx, partitionKey, token)
	if err == nil {
		s.metrics.recordLookupByToken(ctx, hit)
	}
	return res, hit, err
}

// isNoRowsErr reports whether err is pgx's pgx.ErrNoRows.
// Wrapped errors are unwrapped via errors.Is.
func isNoRowsErr(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

// isUniqueViolation reports whether err is a PostgreSQL
// unique-constraint violation (SQLSTATE 23505). Used by the
// write path to detect the (partition_key, idempotency_token)
// partial UNIQUE bouncing a concurrent INSERT.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "23505"
}
