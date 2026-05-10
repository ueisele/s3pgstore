package s3pgstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Poll returns the FileRefs in the half-open offset range
// [since, until), ordered by feed_seq ascending. The second
// return is `next` — one past the highest offset seen, suitable
// for passing back as `since` on the next call. On an empty
// result `next` equals the input `since` (never moves
// backwards).
//
// Range conventions:
//   - since: inclusive lower bound. OffsetNone (0) is safe — it's
//     below every live offset (the sequencer assigns from 1).
//   - until: exclusive upper bound. OffsetLatest omits the
//     upper-bound clause from the SQL so the read sees every
//     committed sequenced row at SELECT time.
//
// Match s3pgstore_files.ResolveFileRefsInRange's [since, until)
// convention so users switching between the two ranges think
// the same way about bounds.
//
// Empty range (since == until) returns (nil, since, nil)
// without touching the database. Inverted range (since > until)
// returns an error.
//
// No upper-bound LIMIT — caller bounds the result via
// (until - since). For unbounded drains use PollRecordsIter
// instead, which streams files lazily and bounds memory via
// WithPollFetchAheadFiles / WithPollDecodeAheadBytes.
//
// Filtering on the catalog is offset-only; consumers wanting
// per-file filtering (by partition, by extension columns)
// should filter the returned entries client-side and pass the
// surviving entries to ReadFileRefsIter for decoding.
func (s *Store[T]) Poll(
	ctx context.Context, since, until Offset,
) (out []FileRef, next Offset, err error) {
	defer s.metrics.methodScope(ctx, "Poll", &err).end()
	if until != OffsetLatest && since > until {
		return nil, since, fmt.Errorf(
			"Poll: since (%d) > until (%d)", since, until)
	}
	if until != OffsetLatest && since == until {
		return nil, since, nil
	}

	cols := []string{
		"file_id", "feed_seq", "partition_key", "s3_key",
		"written_at_version", "written_at", "file_size",
		"uncompressed_size", "record_count",
	}
	for _, c := range s.resolved.ExtensionColumns {
		cols = append(cols, "ext_"+c.Name)
	}
	args := []any{since}
	where := "feed_seq IS NOT NULL AND feed_seq >= $1"
	if until != OffsetLatest {
		args = append(args, until)
		where += fmt.Sprintf(" AND feed_seq < $%d", len(args))
	}
	q := fmt.Sprintf(
		`SELECT %s FROM %s
		WHERE %s
		ORDER BY feed_seq`,
		strings.Join(cols, ", "), s.names.Files(), where)

	next = since
	err = s.cfg.Executor.Run(ctx, func(d DBTX) error {
		rows, err := d.Query(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			e := FileRef{
				Extensions: make(map[string]any,
					len(s.resolved.ExtensionColumns)),
			}
			extValues := make([]any, len(s.resolved.ExtensionColumns))
			scanArgs := []any{
				&e.FileID, &e.Offset, &e.PartitionKey, &e.S3Key,
				&e.Version, &e.WrittenAt, &e.FileSize,
				&e.UncompressedSize, &e.RecordCount,
			}
			for i := range extValues {
				scanArgs = append(scanArgs, &extValues[i])
			}
			if err := rows.Scan(scanArgs...); err != nil {
				return err
			}
			for i, c := range s.resolved.ExtensionColumns {
				if extValues[i] != nil {
					e.Extensions[c.Name] = extValues[i]
				}
			}
			out = append(out, e)
			if e.Offset+1 > next {
				next = e.Offset + 1
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, since, fmt.Errorf("Poll: %w", err)
	}
	return out, next, nil
}

// PollRecords is Poll + parallel S3 GET + decode. Returns one
// FileResult per file (in feed_seq input order, preserving
// commit-time ordering for stream consumers) plus `next` — the
// offset to pass as `since` on the next call.
//
// No dedup is applied — stream consumers see every observed
// version, in commit order. (Dedup is a per-partition Read-side
// concept; the stream feed is intentionally raw so consumers
// can build their own derived state from the full sequence.)
//
// Backed by the chan-based fetch+decode pipeline shared with
// PollRecordsIter, but auto-tunes for batch use: WithPollDecode
// Workers defaults to min(WorkerPool.MaxConcurrent(),
// GOMAXPROCS, lenFiles) and WithPollDecodeAheadFiles to
// ceil(lenFiles/W). Caller-supplied options always win.
//
// Caller bounds memory via the (until - since) range. For
// unbounded drains prefer PollRecordsIter, which streams files
// lazily and bounds memory via the WithPoll* knobs.
//
// Empty range (since == until) returns (nil, since, nil)
// without touching the database.
func (s *Store[T]) PollRecords(
	ctx context.Context, since, until Offset, opts ...PollOption,
) (out []FileResult[T], next Offset, err error) {
	defer s.metrics.methodScope(ctx, "PollRecords", &err).end()

	entries, next, err := s.Poll(ctx, since, until)
	if err != nil {
		return nil, since, err
	}
	if len(entries) == 0 {
		return nil, next, nil
	}

	o := resolvePollOpts(opts)
	var iterErr error
	s.pollFetchAndDecodeIter(ctx, "PollRecords", entries, &o,
		s.pollCollectDefaults, pollCollectEmit(&out, &iterErr))
	if iterErr != nil {
		return nil, since, iterErr
	}
	return out, next, nil
}

// OffsetAt returns the smallest feed_seq whose feed_seq_at is at
// or after t. Used by stream consumers to seek to a wall-clock
// time without scanning. Returns OffsetNone (0) (and no error)
// when no sequenced row matches — interpretable as "nothing yet
// committed at or after t" by callers.
//
// Sentinel: OffsetNone (0) is never a valid live offset (the
// sequencer assigns starting at 1), so a caller can safely
// treat OffsetNone as "no result" without a separate not-found
// signal.
func (s *Store[T]) OffsetAt(
	ctx context.Context, t time.Time,
) (out Offset, err error) {
	defer s.metrics.methodScope(ctx, "OffsetAt", &err).end()
	q := fmt.Sprintf(
		`SELECT MIN(feed_seq) FROM %s
		WHERE feed_seq IS NOT NULL AND feed_seq_at >= $1`,
		s.names.Files())
	var off *int64
	err = s.cfg.Executor.Run(ctx, func(d DBTX) error {
		row := d.QueryRow(ctx, q, t.UTC())
		return row.Scan(&off)
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return OffsetNone, nil
		}
		return OffsetNone, fmt.Errorf("OffsetAt: %w", err)
	}
	if off == nil {
		return OffsetNone, nil
	}
	return *off, nil
}
