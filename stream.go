package s3pgstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// PollOption is the interface implemented by Poll / PollRecords
// modifiers. The only option in v2.0 is WithUntilOffset.
type PollOption interface {
	applyPoll(*pollOpts)
}

type pollOpts struct {
	untilSet   bool
	untilValue Offset
}

type withUntilOffsetOpt struct{ until Offset }

func (o withUntilOffsetOpt) applyPoll(opts *pollOpts) {
	opts.untilSet = true
	opts.untilValue = o.until
}

// WithUntilOffset bounds the upper cursor of a Poll /
// PollRecords call to until inclusive. Useful for "drain
// everything committed up to point X and stop":
//
//	tip, _ := store.OffsetAt(ctx, time.Now())
//	for since := startOffset; since < tip; {
//	    rs, next, _ := store.PollRecords(ctx, since, 100,
//	        s3pgstore.WithUntilOffset(tip))
//	    if len(rs) == 0 { break }
//	    process(rs)
//	    since = next
//	}
//
// The bound is inclusive (`feed_seq <= until`) — a row whose
// feed_seq exactly matches `until` is included.
func WithUntilOffset(until Offset) PollOption {
	return withUntilOffsetOpt{until: until}
}

// Poll returns the next batch of FileRefs with
// `feed_seq > since AND feed_seq IS NOT NULL`, ordered by
// feed_seq, capped at n. Returns the refs and the highest
// observed offset (suitable for passing back as `since` next
// time). On an empty result the second return is `since`
// unchanged — never moves backwards.
//
// n <= 0 returns (nil, since, nil) without touching the
// database.
//
// Filtering on the catalog is offset-only; consumers wanting
// per-file filtering (by partition, by extension columns)
// should filter the returned entries client-side and pass the
// surviving entries to ReadFileRefsIter for decoding.
func (s *Store[T]) Poll(
	ctx context.Context, since Offset, n int, opts ...PollOption,
) (out []FileRef, next Offset, err error) {
	defer s.metrics.methodScope(ctx, "Poll", &err).end()
	if n <= 0 {
		return nil, since, nil
	}
	var o pollOpts
	for _, opt := range opts {
		opt.applyPoll(&o)
	}

	cols := []string{
		"file_id", "feed_seq", "partition_key", "s3_key",
		"written_at_version", "written_at", "file_size",
		"uncompressed_size", "record_count",
	}
	for _, c := range s.resolved.ExtensionColumns {
		cols = append(cols, "ext_"+c.Name)
	}
	args := []any{since, n}
	where := "feed_seq IS NOT NULL AND feed_seq > $1"
	if o.untilSet {
		args = append(args, o.untilValue)
		where += fmt.Sprintf(" AND feed_seq <= $%d", len(args))
	}
	q := fmt.Sprintf(
		`SELECT %s FROM %s
		WHERE %s
		ORDER BY feed_seq
		LIMIT $2`,
		strings.Join(cols, ", "), s.names.Files(), where)

	maxOffset := since
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
			if e.Offset > maxOffset {
				maxOffset = e.Offset
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, since, fmt.Errorf("Poll: %w", err)
	}
	return out, maxOffset, nil
}

// PollRecords is Poll + parallel S3 GET + decode. Returns the
// flattened slice of decoded records (in offset order) and the
// next offset.
//
// No dedup is applied — stream consumers see every observed
// version, in commit order. (Dedup is a per-partition Read-side
// concept; the stream feed is intentionally raw so consumers
// can build their own derived state from the full sequence.)
//
// n <= 0 returns (nil, since, nil) without touching the
// database.
func (s *Store[T]) PollRecords(
	ctx context.Context, since Offset, n int, opts ...PollOption,
) (out []T, next Offset, err error) {
	defer s.metrics.methodScope(ctx, "PollRecords", &err).end()

	entries, next, err := s.Poll(ctx, since, n, opts...)
	if err != nil {
		return nil, since, err
	}
	if len(entries) == 0 {
		return nil, next, nil
	}

	bodies := make([][]T, len(entries))
	if err := fanOutPool(ctx, s.resolved.WorkerPool, entries,
		s.metrics.fanOutObserverFor("PollRecords"),
		func(ctx context.Context, i int, e FileRef) error {
			data, err := s.target.get(ctx, e.S3Key)
			if err != nil {
				return fmt.Errorf("GET %s: %w", e.S3Key, err)
			}
			recs, err := decodeParquet[T](data)
			if err != nil {
				return fmt.Errorf("decode %s: %w", e.S3Key, err)
			}
			bodies[i] = recs
			return nil
		},
	); err != nil {
		return nil, since, err
	}

	total := 0
	for _, b := range bodies {
		total += len(b)
	}
	out = make([]T, 0, total)
	for _, b := range bodies {
		out = append(out, b...)
	}
	return out, next, nil
}

// OffsetAt returns the smallest feed_seq whose feed_seq_at is at
// or after t. Used by stream consumers to seek to a wall-clock
// time without scanning. Returns 0 (and no error) when no
// sequenced row matches — interpretable as "nothing yet
// committed at or after t" by callers.
//
// Sentinel: Offset(0) is never a valid live offset (the
// sequencer assigns starting at 1), so a caller can safely
// treat 0 as "no result" without a separate not-found signal.
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
			return 0, nil
		}
		return 0, fmt.Errorf("OffsetAt: %w", err)
	}
	if off == nil {
		return 0, nil
	}
	return *off, nil
}
