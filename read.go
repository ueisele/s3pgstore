package s3pgstore

import (
	"context"
	"fmt"
	"strings"
)

// PartitionResult is a per-partition read result. Records are
// the decoded rows (post-dedup if EntityKeyOf+VersionOf are
// configured); Version is the partition's version derived from
// MAX(written_at_version) over the returned files (equal to
// s3pgstore_partitions.version by the catalog construction
// invariant in CLAUDE.md).
//
// FileExtensions is populated in Phase 8 (ExtensionColumns).
// In Phase 6 the slice is non-nil (one entry per file in
// stable order) but every entry's Extensions map is empty.
type PartitionResult[T any] struct {
	PartitionKey   string
	Records        []T
	Version        int64
	FileExtensions []FileExtensions
}

// FileExtensions carries the typed ext_<n> columns for a
// single file. Phase 6 populates FileID; the map is empty
// until Phase 8 wires WithMetadata + ExtensionColumns.
type FileExtensions struct {
	FileID     int64
	Extensions map[string]any
}

// ReadOption is the interface implemented by Read modifiers.
type ReadOption interface {
	applyRead(*readOpts)
}

type readOpts struct {
	// includeHistory disables the per-partition latest-per-
	// entity dedup. Set via WithHistory.
	includeHistory bool

	// readAheadPartitions controls how many partitions ahead of
	// the current yield position the iter pipeline buffers in
	// the decoder→yield channel. nil (option not supplied)
	// resolves to the default of 1 — minimum useful lookahead so
	// decode of partition N+1 overlaps yield of partition N.
	// *p == 0 is the explicit-no-buffer mode: unbuffered handoff,
	// the decoder still works on N+1 concurrent with yield
	// emitting N but never holds two decoded partitions in
	// memory at once. *p > 0 buffers up to p partitions in the
	// channel, at O(p+1 partitions) memory.
	//
	// Pointer-typed so the zero value of WithReadAheadPartitions
	// (an explicit 0) stays distinguishable from "option not
	// supplied" (which falls back to the default of 1).
	readAheadPartitions *int

	// readAheadBytes caps the cumulative uncompressed parquet
	// bytes that may sit decoded in the iter pipeline ahead of
	// the current yield position. Zero (default) disables the
	// cap; only readAheadPartitions binds. Read from each
	// parquet file's footer (sum of row-group total_byte_size)
	// so the cap is exact, not a heuristic.
	readAheadBytes int64
}

// WithHistory disables the per-partition latest-per-entity
// dedup, returning every (entity, version) pair surviving
// replica collapse. No effect when EntityKeyOf or VersionOf
// are not configured.
func WithHistory() ReadOption { return withHistoryOpt{} }

type withHistoryOpt struct{}

func (withHistoryOpt) applyRead(o *readOpts) { o.includeHistory = true }

// WithReadAheadPartitions tells the iter pipeline how many
// partitions to buffer ahead of the current yield position.
// Default (option not supplied) is 1 — minimum useful lookahead
// so decode of partition N+1 overlaps yield of partition N.
// Pass a larger value for more aggressive prefetch on consumers
// that do non-trivial per-record work; combine with
// WithReadAheadBytes to bound stacking of skewed-size partitions.
//
// n=0 is the explicit-no-buffer mode: unbuffered handoff between
// decoder and yield loop. The decoder still works on partition
// N+1 concurrent with yield emitting N (the handoff just blocks
// the decoder briefly), but never two decoded partitions sit in
// memory at once. Useful when records are large and the
// consumer's per-record work is fast — the byte cap is then the
// only memory regulator.
//
// Negative values are floored to 0.
//
// Each buffered partition holds its decoded records in memory
// until the yield loop consumes them. Memory: O((n+1) partitions)
// — current + n prefetched.
//
// No effect on the buffered Read path (which materialises every
// partition concurrently by design).
func WithReadAheadPartitions(n int) ReadOption {
	if n < 0 {
		n = 0
	}
	return withReadAheadPartitionsOpt{n: n}
}

type withReadAheadPartitionsOpt struct{ n int }

func (o withReadAheadPartitionsOpt) applyRead(opts *readOpts) {
	n := o.n
	opts.readAheadPartitions = &n
}

// WithReadAheadBytes caps the cumulative uncompressed parquet
// bytes that may sit decoded in the iter pipeline ahead of the
// current yield position. Zero (default) disables the cap; only
// WithReadAheadPartitions binds.
//
// Composes with WithReadAheadPartitions — both are evaluated and
// whichever cap binds first holds the producer back. Useful when
// partition sizes are skewed: a tiny WithReadAheadPartitions(1)
// is too conservative for many small partitions but a larger
// value risks OOM on a few large ones; a byte cap auto-tunes
// across both.
//
// The byte total is read from each parquet file's footer
// (total_byte_size summed across row groups), so the cap is
// exact, not a heuristic. Decoded Go memory typically runs
// 1–2× the uncompressed size depending on data shape.
//
// Per-partition guarantee: if a single partition's uncompressed
// size exceeds the cap, that one partition still decodes (the
// cap can't be enforced below partition granularity without
// row-group-level streaming). The cap only prevents *additional*
// partitions from joining the buffer.
//
// No effect on the buffered Read path.
func WithReadAheadBytes(n int64) ReadOption {
	if n < 0 {
		n = 0
	}
	return withReadAheadBytesOpt{n: n}
}

type withReadAheadBytesOpt struct{ n int64 }

func (o withReadAheadBytesOpt) applyRead(opts *readOpts) {
	opts.readAheadBytes = o.n
}

// Read returns one PartitionResult[T] per partition matched by
// filters. Records are deduplicated by (EntityKeyOf,
// VersionOf) when both are configured (pass WithHistory to
// opt out).
//
// Single SQL query against s3pgstore_files (no per-file
// filtering, no LIMIT — every file row for every matched
// partition comes back, per CLAUDE.md). For each partition,
// files are fetched in parallel from S3 (capped by
// Config.S3MaxConcurrentOpsPerMethod inside this Read call,
// and globally by s3client.Options.MaxOpenConnections), decoded, and
// concatenated; dedup runs once per partition.
//
// Empty filters slice returns (nil, nil) — no partitions
// matched, no S3 traffic.
func (s *Store[T]) Read(
	ctx context.Context, filters []PartitionFilter, opts ...ReadOption,
) (parts []PartitionResult[T], err error) {
	defer s.metrics.methodScope(ctx, "Read", &err).end()

	if len(filters) == 0 {
		return nil, nil
	}
	var o readOpts
	for _, opt := range opts {
		opt.applyRead(&o)
	}

	rows, err := s.selectFileRows(ctx, filters)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}

	// Group rows by partition_key. Single pass; partitions
	// emit in lex order (ORDER BY partition_key in the SQL).
	type group struct {
		key   string
		files []fileRow
	}
	var groups []group
	current := group{}
	for _, r := range rows {
		if r.partitionKey != current.key {
			if current.key != "" {
				groups = append(groups, current)
			}
			current = group{key: r.partitionKey}
		}
		current.files = append(current.files, r)
	}
	groups = append(groups, current)

	// Outer fan-out across partitions — each worker decodes
	// one partition (and internally fan-outs across that
	// partition's files via fetchAndDecode). Slot-indexed
	// writes preserve lex order of partition keys regardless
	// of completion order. Concurrency caps at the s3target's
	// effective concurrency: extra partition workers would
	// just queue on the inner semaphore.
	//
	// Memory note: with N outer × N inner workers, N²
	// goroutines may exist concurrently. Most park on the
	// inner s3target semaphore so the actual S3 fan-out
	// stays bounded. Decode buffers are the real memory
	// pressure — Read is already a buffered API ("everything
	// in RAM"), so the trade is acceptable. Streaming
	// callers needing per-partition memory bounds should use
	// ReadPartitionIter (Phase 12).
	out := make([]PartitionResult[T], len(groups))
	if err := fanOut(ctx, groups, s.target.effectiveConcurrency(), nil,
		func(ctx context.Context, i int, g group) error {
			records, err := s.fetchAndDecode(ctx, g.files)
			if err != nil {
				return fmt.Errorf("read partition %q: %w", g.key, err)
			}

			var version int64
			exts := make([]FileExtensions, 0, len(g.files))
			for _, f := range g.files {
				if f.writtenAtVersion > version {
					version = f.writtenAtVersion
				}
				extMap := make(map[string]any,
					len(s.resolved.ExtensionColumns))
				for j, c := range s.resolved.ExtensionColumns {
					if j < len(f.extValues) && f.extValues[j] != nil {
						extMap[c.Name] = f.extValues[j]
					}
				}
				exts = append(exts, FileExtensions{
					FileID:     f.fileID,
					Extensions: extMap,
				})
			}

			records = sortAndDedup(records,
				s.resolved.EntityKeyOf, s.resolved.VersionOf,
				o.includeHistory)

			out[i] = PartitionResult[T]{
				PartitionKey:   g.key,
				Records:        records,
				Version:        version,
				FileExtensions: exts,
			}
			return nil
		},
	); err != nil {
		return nil, err
	}
	return out, nil
}

// fileRow is one row from the read-path SELECT. Field
// ordering mirrors the SELECT column list so Scan args stay
// in step with the SQL.
type fileRow struct {
	fileID           int64
	partitionKey     string
	s3Key            string
	writtenAtVersion int64
	// extValues holds the ext_<n> columns, in declaration
	// order matching s.resolved.ExtensionColumns. Pulled out
	// into the FileExtensions.Extensions map at read time.
	extValues []any
}

// selectFileRows builds and runs the read-path SELECT against
// s3pgstore_files. Every row for every matched partition comes
// back (no per-file filter, no LIMIT). Ordered by
// (partition_key, s3_key) so the caller can group rows in a
// single pass and per-partition file order is deterministic
// (lex by S3 key — required for the dedup tie-break per
// CLAUDE.md).
//
// SELECT column list:
//
//	file_id, partition_key, s3_key, written_at_version,
//	ext_<col1>, ext_<col2>, ...
func (s *Store[T]) selectFileRows(
	ctx context.Context, filters []PartitionFilter,
) ([]fileRow, error) {
	where, args, err := translateFilters(filters,
		partColResolver(s.resolved.PartitionKeyParts))
	if err != nil {
		return nil, err
	}

	cols := []string{
		"file_id", "partition_key", "s3_key", "written_at_version",
	}
	for _, c := range s.resolved.ExtensionColumns {
		cols = append(cols, "ext_"+c.Name)
	}
	q := fmt.Sprintf(
		`SELECT %s
		FROM %s
		WHERE %s
		ORDER BY partition_key, s3_key`,
		strings.Join(cols, ", "), s.names.Files(), where)

	var out []fileRow
	err = s.cfg.Executor.Run(ctx, func(d DBTX) error {
		rows, err := d.Query(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			r := fileRow{
				extValues: make([]any, len(s.resolved.ExtensionColumns)),
			}
			scanArgs := []any{
				&r.fileID, &r.partitionKey,
				&r.s3Key, &r.writtenAtVersion,
			}
			extPtrs := make([]any, len(s.resolved.ExtensionColumns))
			for i := range extPtrs {
				extPtrs[i] = &r.extValues[i]
			}
			scanArgs = append(scanArgs, extPtrs...)
			if err := rows.Scan(scanArgs...); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("SELECT files: %w", err)
	}
	return out, nil
}

// fetchAndDecode pulls every parquet file in files from S3 in
// parallel via a fanOut goroutine pool sized to
// Config.S3MaxConcurrentOpsPerMethod, decodes each into []T,
// and concatenates the partition's records in s3_key lex order.
//
// Lex ordering matters for dedup tie-break (last wins on
// equal max version, per CLAUDE.md). The caller pre-sorted
// files by s3_key in the SELECT; per-index result slots
// preserve that order while parallelising the GETs.
//
// The global TCP-connection cap (s3client.Options.MaxOpenConnections)
// is enforced one level down by the s3.Client's HTTP
// transport. fanOut's shared-cancel ctx propagates
// first-error-wins through the SDK's adaptive-retry loop, so a
// failing GET unwinds in-flight siblings instead of running
// them to completion.
func (s *Store[T]) fetchAndDecode(
	ctx context.Context, files []fileRow,
) ([]T, error) {
	if len(files) == 0 {
		return nil, nil
	}
	bodies := make([][]T, len(files))

	if err := fanOut(ctx, files, s.target.effectiveConcurrency(), nil,
		func(ctx context.Context, i int, f fileRow) error {
			data, err := s.target.get(ctx, f.s3Key)
			if err != nil {
				return fmt.Errorf("GET %s: %w", f.s3Key, err)
			}
			recs, err := decodeParquet[T](data)
			if err != nil {
				return fmt.Errorf("decode %s: %w", f.s3Key, err)
			}
			bodies[i] = recs
			return nil
		},
	); err != nil {
		return nil, err
	}

	total := 0
	for _, b := range bodies {
		total += len(b)
	}
	out := make([]T, 0, total)
	for _, b := range bodies {
		out = append(out, b...)
	}
	return out, nil
}
