package s3pgstore

// Phase 12 — ReadIter matrix.
//
// The implementation plan calls for vendoring s3store's chan-
// based producer/downloader/decoder pipeline along with the
// WithReadAheadPartitions / WithReadAheadBytes budget knobs and
// the stall watchdog. That vendor pass is deferred — the v2.0
// surface ships with a simpler per-partition-serial pipeline
// that satisfies the "memory-bounded reads for large result
// sets" milestone (one partition's records at a time, never the
// full union) without the chan-based scaffolding.
//
// API surface is forward-compatible: the readahead options are
// accepted today as no-ops so callers writing against the
// stable surface don't have to migrate when the vendored
// pipeline lands. See the implementation plan's "Phase 12"
// section for the upstream files we'll port.

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"time"
)

// The ReadIter family reuses the buffered Read path's
// ReadOption type and option bag — every option that applies to
// Read (currently just WithHistory) applies the same way to the
// streaming variants. New iter-only knobs (e.g.,
// WithReadAheadPartitions / WithReadAheadBytes per Phase 12's
// vendor pass) can be added as ReadOption implementations whose
// applyRead is a no-op when called from Read but meaningful in
// iter context, or via a dedicated marker interface — to be
// decided when the vendor pass lands.

// ReadIter returns an iter.Seq2[T, error] that yields every
// record matching filters, lazily one partition at a time.
// Memory bound: one partition's decoded record set (plus its
// per-file parquet bodies, briefly, during decode).
//
// Cancellation: yielding `false` from the loop body cancels
// in-flight S3 GETs through ctx propagation. Iteration also
// terminates immediately on the first per-partition error.
//
// Emission order: lex by partition key, then per-partition
// (entity, version) ascending after dedup (if configured) or
// decode/insertion order without it. Same input yields byte-
// identical sequences across runs per CLAUDE.md.
//
// Empty filters yields nothing without touching the database.
func (s *Store[T]) ReadIter(
	ctx context.Context, filters []PartitionFilter,
	opts ...ReadOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		o := resolveIterOpts(opts)
		s.iterPartitions(ctx, filters,
			func(_ string, records []T) bool {
				records = sortAndDedup(records,
					s.resolved.EntityKeyOf,
					s.resolved.VersionOf, o.includeHistory)
				for _, r := range records {
					if !yield(r, nil) {
						return false
					}
				}
				return true
			},
			func(err error) {
				var zero T
				yield(zero, err)
			})
	}
}

// ReadPartitionIter is the per-partition variant of ReadIter:
// each yield is one PartitionResult[T] with Records, Version,
// and FileExtensions populated. Same memory bound and
// cancellation semantics as ReadIter.
func (s *Store[T]) ReadPartitionIter(
	ctx context.Context, filters []PartitionFilter,
	opts ...ReadOption,
) iter.Seq2[PartitionResult[T], error] {
	return func(yield func(PartitionResult[T], error) bool) {
		o := resolveIterOpts(opts)
		stop := false
		s.iterPartitionsFull(ctx, filters,
			func(part PartitionResult[T]) bool {
				part.Records = sortAndDedup(part.Records,
					s.resolved.EntityKeyOf,
					s.resolved.VersionOf, o.includeHistory)
				if !yield(part, nil) {
					stop = true
					return false
				}
				return true
			},
			func(err error) {
				if !stop {
					yield(PartitionResult[T]{}, err)
				}
			})
	}
}

// ReadRangeIter walks every record whose feed_seq_at falls in
// [since, until). Bounds are resolved to feed_seq values at
// call entry (one OffsetAt-shape lookup each), so the upper
// bound stays stable under concurrent writes.
//
// Half-open semantics:
//   - since.IsZero() → start at offset 1 (stream head).
//   - until.IsZero() → stop at the live tip captured at call
//     entry (currently MAX(feed_seq)).
//   - Records at since are included; records at until are not.
//
// No partition filter is applied — every partition with any
// in-range row contributes records.
func (s *Store[T]) ReadRangeIter(
	ctx context.Context, since, until time.Time,
	opts ...ReadOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		o := resolveIterOpts(opts)
		s.iterRange(ctx, since, until,
			func(_ string, records []T) bool {
				records = sortAndDedup(records,
					s.resolved.EntityKeyOf,
					s.resolved.VersionOf, o.includeHistory)
				for _, r := range records {
					if !yield(r, nil) {
						return false
					}
				}
				return true
			},
			func(err error) {
				var zero T
				yield(zero, err)
			})
	}
}

// ReadPartitionRangeIter is the per-partition variant of
// ReadRangeIter — same time-bound resolution, per-partition
// output shape.
func (s *Store[T]) ReadPartitionRangeIter(
	ctx context.Context, since, until time.Time,
	opts ...ReadOption,
) iter.Seq2[PartitionResult[T], error] {
	return func(yield func(PartitionResult[T], error) bool) {
		o := resolveIterOpts(opts)
		stop := false
		s.iterRangeFull(ctx, since, until,
			func(part PartitionResult[T]) bool {
				part.Records = sortAndDedup(part.Records,
					s.resolved.EntityKeyOf,
					s.resolved.VersionOf, o.includeHistory)
				if !yield(part, nil) {
					stop = true
					return false
				}
				return true
			},
			func(err error) {
				if !stop {
					yield(PartitionResult[T]{}, err)
				}
			})
	}
}

// ReadEntriesIter decodes pre-resolved StreamEntry slices
// without re-querying the catalog. Each entry's S3 key is
// validated up front against this Store's bucket+prefix; an
// entry from a different Store fails with an error before any
// S3 traffic.
//
// Records emit in entries-input order, grouped by partition
// (Key field). Within a partition the records are sortAndDedup'd
// the same way ReadIter dedups.
func (s *Store[T]) ReadEntriesIter(
	ctx context.Context, entries []StreamEntry,
	opts ...ReadOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		o := resolveIterOpts(opts)
		if err := s.validateEntries(entries); err != nil {
			var zero T
			yield(zero, err)
			return
		}
		s.iterEntries(ctx, entries,
			func(_ string, records []T) bool {
				records = sortAndDedup(records,
					s.resolved.EntityKeyOf,
					s.resolved.VersionOf, o.includeHistory)
				for _, r := range records {
					if !yield(r, nil) {
						return false
					}
				}
				return true
			},
			func(err error) {
				var zero T
				yield(zero, err)
			})
	}
}

// ReadPartitionEntriesIter is the per-partition variant of
// ReadEntriesIter.
func (s *Store[T]) ReadPartitionEntriesIter(
	ctx context.Context, entries []StreamEntry,
	opts ...ReadOption,
) iter.Seq2[PartitionResult[T], error] {
	return func(yield func(PartitionResult[T], error) bool) {
		o := resolveIterOpts(opts)
		if err := s.validateEntries(entries); err != nil {
			yield(PartitionResult[T]{}, err)
			return
		}
		stop := false
		s.iterEntriesFull(ctx, entries,
			func(part PartitionResult[T]) bool {
				part.Records = sortAndDedup(part.Records,
					s.resolved.EntityKeyOf,
					s.resolved.VersionOf, o.includeHistory)
				if !yield(part, nil) {
					stop = true
					return false
				}
				return true
			},
			func(err error) {
				if !stop {
					yield(PartitionResult[T]{}, err)
				}
			})
	}
}

// resolveIterOpts collapses an options slice into the
// internal opts struct.
func resolveIterOpts(opts []ReadOption) readOpts {
	var o readOpts
	for _, opt := range opts {
		opt.applyRead(&o)
	}
	return o
}

// iterPartitions enumerates partitions matching filters and
// invokes onPartition with each partition's decoded record set
// in lex order. Returns early via onErr when an error occurs;
// onPartition returning false stops iteration (no further
// partitions decoded).
func (s *Store[T]) iterPartitions(
	ctx context.Context, filters []PartitionFilter,
	onPartition func(key string, records []T) bool,
	onErr func(err error),
) {
	if len(filters) == 0 {
		return
	}
	rows, err := s.selectFileRows(ctx, filters)
	if err != nil {
		onErr(err)
		return
	}
	s.dispatchGroups(ctx, rows,
		func(_ string, records []T, _ int64,
			_ []FileExtensions) bool {
			return onPartition("", records)
		}, onErr)
}

// iterPartitionsFull is the per-partition-result variant —
// onPartition receives the fully-populated PartitionResult.
func (s *Store[T]) iterPartitionsFull(
	ctx context.Context, filters []PartitionFilter,
	onPartition func(part PartitionResult[T]) bool,
	onErr func(err error),
) {
	if len(filters) == 0 {
		return
	}
	rows, err := s.selectFileRows(ctx, filters)
	if err != nil {
		onErr(err)
		return
	}
	s.dispatchGroups(ctx, rows,
		func(key string, records []T, version int64,
			exts []FileExtensions) bool {
			return onPartition(PartitionResult[T]{
				PartitionKey:   key,
				Records:        records,
				Version:        version,
				FileExtensions: exts,
			})
		}, onErr)
}

// iterRange resolves the time bounds, queries the catalog, and
// drives the per-partition pipeline. Same shape as
// iterPartitions but via a feed_seq predicate rather than a
// PartitionFilter.
func (s *Store[T]) iterRange(
	ctx context.Context, since, until time.Time,
	onPartition func(key string, records []T) bool,
	onErr func(err error),
) {
	rows, err := s.selectFileRowsByRange(ctx, since, until)
	if err != nil {
		onErr(err)
		return
	}
	s.dispatchGroups(ctx, rows,
		func(_ string, records []T, _ int64,
			_ []FileExtensions) bool {
			return onPartition("", records)
		}, onErr)
}

func (s *Store[T]) iterRangeFull(
	ctx context.Context, since, until time.Time,
	onPartition func(part PartitionResult[T]) bool,
	onErr func(err error),
) {
	rows, err := s.selectFileRowsByRange(ctx, since, until)
	if err != nil {
		onErr(err)
		return
	}
	s.dispatchGroups(ctx, rows,
		func(key string, records []T, version int64,
			exts []FileExtensions) bool {
			return onPartition(PartitionResult[T]{
				PartitionKey:   key,
				Records:        records,
				Version:        version,
				FileExtensions: exts,
			})
		}, onErr)
}

// iterEntries drives the pipeline directly off pre-resolved
// StreamEntry slices, bypassing the catalog SELECT entirely.
// Files are grouped by entry.Key (partition).
func (s *Store[T]) iterEntries(
	ctx context.Context, entries []StreamEntry,
	onPartition func(key string, records []T) bool,
	onErr func(err error),
) {
	rows := entriesToFileRows(entries, s.resolved.ExtensionColumns)
	s.dispatchGroups(ctx, rows,
		func(_ string, records []T, _ int64,
			_ []FileExtensions) bool {
			return onPartition("", records)
		}, onErr)
}

func (s *Store[T]) iterEntriesFull(
	ctx context.Context, entries []StreamEntry,
	onPartition func(part PartitionResult[T]) bool,
	onErr func(err error),
) {
	rows := entriesToFileRows(entries, s.resolved.ExtensionColumns)
	s.dispatchGroups(ctx, rows,
		func(key string, records []T, version int64,
			exts []FileExtensions) bool {
			return onPartition(PartitionResult[T]{
				PartitionKey:   key,
				Records:        records,
				Version:        version,
				FileExtensions: exts,
			})
		}, onErr)
}

// dispatchGroups groups pre-sorted file rows by partition_key
// (rows must arrive in lex partition_key, then s3_key order)
// and decodes each partition serially. Per-partition file
// fetches run in parallel via the existing fetchAndDecode
// helper. onGroup returns false to stop iteration; on error,
// onErr fires.
func (s *Store[T]) dispatchGroups(
	ctx context.Context, rows []fileRow,
	onGroup func(key string, records []T, version int64,
		exts []FileExtensions) bool,
	onErr func(err error),
) {
	if len(rows) == 0 {
		return
	}
	type group struct {
		key   string
		files []fileRow
	}
	var groups []group
	current := group{key: rows[0].partitionKey,
		files: []fileRow{rows[0]}}
	for _, r := range rows[1:] {
		if r.partitionKey != current.key {
			groups = append(groups, current)
			current = group{key: r.partitionKey,
				files: []fileRow{r}}
		} else {
			current.files = append(current.files, r)
		}
	}
	groups = append(groups, current)

	for _, g := range groups {
		records, err := s.fetchAndDecode(ctx, g.files)
		if err != nil {
			onErr(fmt.Errorf("read partition %q: %w",
				g.key, err))
			return
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
		if !onGroup(g.key, records, version, exts) {
			return
		}
		if ctx.Err() != nil {
			onErr(ctx.Err())
			return
		}
	}
}

// selectFileRowsByRange runs the time-range catalog SELECT,
// filtered by feed_seq_at (unbounded sides default to
// open-ended). Rows come back ordered by partition_key, s3_key
// for the same group-then-decode pattern as the partition-
// filter SELECT.
//
// Bound semantics:
//   - since: feed_seq_at >= since (zero → unbounded below).
//   - until: feed_seq_at < until (zero → unbounded above; the
//     "live tip captured at call entry" property holds only
//     because the catalog row count is monotonically
//     non-decreasing in v2.0 — files are never deleted).
//
// Records with feed_seq IS NULL (not yet sequenced) are
// excluded so the result is stable under sequencer races.
func (s *Store[T]) selectFileRowsByRange(
	ctx context.Context, since, until time.Time,
) ([]fileRow, error) {
	cols := []string{
		"file_id", "partition_key", "s3_key", "written_at_version",
	}
	for _, c := range s.resolved.ExtensionColumns {
		cols = append(cols, "ext_"+c.Name)
	}
	where := []string{"feed_seq IS NOT NULL"}
	args := []any{}
	if !since.IsZero() {
		args = append(args, since.UTC())
		where = append(where,
			fmt.Sprintf("feed_seq_at >= $%d", len(args)))
	}
	if !until.IsZero() {
		args = append(args, until.UTC())
		where = append(where,
			fmt.Sprintf("feed_seq_at < $%d", len(args)))
	}
	q := fmt.Sprintf(
		`SELECT %s FROM %s
		WHERE %s
		ORDER BY partition_key, s3_key`,
		strings.Join(cols, ", "), s.names.Files(),
		strings.Join(where, " AND "))

	var out []fileRow
	err := s.cfg.Executor.Run(ctx, func(d DBTX) error {
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
			for i := range r.extValues {
				scanArgs = append(scanArgs, &r.extValues[i])
			}
			if err := rows.Scan(scanArgs...); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("SELECT files (range): %w", err)
	}
	return out, nil
}

// validateEntries verifies every entry's DataPath lives under
// this Store's data prefix. Catches "entries from a different
// Store" before any S3 traffic — much cheaper to fail upfront
// than to discover via 404 mid-iteration.
//
// Cross-Store entries are an easy mistake to make (poll one
// Store, decode in another) and the error message points to
// the offending key.
func (s *Store[T]) validateEntries(entries []StreamEntry) error {
	dataPrefix := s.dataPath() + "/"
	for i, e := range entries {
		if !strings.HasPrefix(e.DataPath, dataPrefix) {
			return fmt.Errorf(
				"entries[%d].DataPath %q does not belong to "+
					"this Store (expected prefix %q)",
				i, e.DataPath, dataPrefix)
		}
	}
	return nil
}

// entriesToFileRows projects StreamEntry into the internal
// fileRow shape so the entries pipeline reuses the same
// decode/group machinery as the catalog-driven paths.
//
// Order is preserved: entries are emitted in the input slice's
// order; consecutive entries with the same Key form one group.
// (Callers wanting partition-grouped behavior should sort their
// slice by Key first; we do not re-sort to keep the contract
// "decode in order presented" — useful for caller-controlled
// playback.)
//
// Extensions are decoded by name into the same positional slot
// as the SELECT path, leaving missing keys as nil.
func entriesToFileRows(
	entries []StreamEntry, extCols []ExtensionColumn,
) []fileRow {
	out := make([]fileRow, len(entries))
	for i, e := range entries {
		extValues := make([]any, len(extCols))
		for j, c := range extCols {
			if v, ok := e.Extensions[c.Name]; ok {
				extValues[j] = v
			}
		}
		out[i] = fileRow{
			partitionKey: e.Key,
			s3Key:        e.DataPath,
			extValues:    extValues,
			// fileID and writtenAtVersion are not exposed via
			// StreamEntry; iter callers reading via the
			// entries path don't get a meaningful Version on
			// the per-partition variant. Documented behavior.
		}
	}
	return out
}
