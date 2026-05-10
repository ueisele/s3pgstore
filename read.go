package s3pgstore

// read.go is the read path for s3pgstore: the public entry
// points (Read / ReadPartition / ReadIter and its variants),
// the catalog SELECT helpers (ResolveFileRefs /
// ResolveFileRefsInRange / queryFileRows), and the chan-based
// fetch+decode pipeline (fetchAndDecodeIter and the stream/worker
// state machinery) that backs every entry point.
//
// The pipeline is vendored from
// https://github.com/ueisele/s3store/blob/da75ca9/reader_iter.go
// (post `860cf19` "Replace iter pipeline sync.Cond with chan-based
// primitives" + `da75ca9` "Fix iter pipeline deadlock under
// cond.Wait scheduler unfairness"). Copyright (c) 2024-2026 Uwe
// Eisele. MIT License.
//
// What we vendor: the producer → downloader → decoder topology,
// the body-slot semaphore (buffered chan, FIFO sendq),
// reserveBytes / releaseBytes around WithDecodeAheadBytes, the
// stall-watchdog (pure observer), recordEmit / partitionEmit.
//
// What we adapt:
//
//   - s3store.Reader[T] → s3pgstore.Store[T]; Target is reached
//     via s.target rather than s.cfg.Target.
//   - keyMeta → FileRef: the catalog gives us partition_key,
//     written_at_version, and ext_<n> values per file at
//     SELECT time, so partState carries FileRef directly. No
//     hiveKeyOfDataFile parsing — the partition is already in
//     the row. FileRef is also the public type returned by Poll
//     and consumed by ReadFileRefsIter, so the entries-input path
//     hands its slice straight to the pipeline (no projection).
//   - tolerantOfMissingData is gone: s3pgstore has no ref-stream
//     gate, every read path is strict. NoSuchKey from S3
//     surfaces as a wrapped error (operator-driven prune is
//     incident-class, not silently skipped).
//   - The `keys []keyMeta` upstream argument splits into three
//     input modes here (filters / time range / pre-resolved
//     entries); each method enumerates FileRef and hands the
//     slice to the same pipeline.
//   - Methods retain s3pgstore signatures: ReadIter takes
//     `[]PartitionFilter`, ReadPartitionIter yields
//     PartitionResult[T] (carrying Version + FileRefs),
//     not HivePartition[T].
//
// What we DO NOT vendor:
//
//   - The s3 missing-data counter / slog.Warn skip — strict
//     paths only here.
//   - methodScope addRecords/addPartitions/addBytes/addFiles —
//     s3pgstore methodScope keeps the surface to duration +
//     calls + in_flight; the iter saturation metrics
//     (body_slot, byte_budget, decode_duration, stall.count)
//     cover the operationally interesting iter signal.

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ueisele/s3pgstore/pool"
)

// PartitionResult is a per-partition read result. Records are
// the decoded rows (post-dedup if EntityKeyOf+VersionOf are
// configured); Version is the partition's version derived from
// MAX(written_at_version) over the returned files (equal to
// s3pgstore_partitions.version by the catalog construction
// invariant in CLAUDE.md).
//
// FileRefs is the catalog rows backing this partition's records,
// in deterministic S3Key order — same shape as the slice returned
// by Write or yielded by Poll. Each entry's Extensions carries the
// typed ext_<n> columns; the map is empty when no metadata was
// written.
type PartitionResult[T any] struct {
	PartitionKey string
	Records      []T
	Version      int64
	FileRefs     []FileRef
}

// ResolveFileRefs translates a partition-filter expression into
// the matching catalog rows (FileRef) without fetching or
// decoding the parquet bodies. Useful when the caller wants to
// inspect the catalog (sizes, record counts, tokens, extension
// columns) before committing to the S3 + decode cost — pass the
// returned slice straight to ReadFileRefsIter to round-trip into
// records.
//
// The returned slice carries every FileRef field the catalog
// stores (FileID, PartitionKey, S3Key, Version, WrittenAt,
// FileSize, UncompressedSize, RecordCount, Extensions). Offset
// is populated when the row has been sequenced (feed_seq IS NOT
// NULL); NoOffset otherwise.
//
// Filters compose with the same semantics as Read / ReadIter:
// each PartitionFilter narrows the partition-key space, every
// matched partition contributes every committed file (no
// per-file filter, no LIMIT — same construction invariant as
// Read; see CLAUDE.md "Read returns the complete file set per
// matched partition").
//
// Empty filters returns (nil, nil) — no partitions match, no
// query is issued.
func (s *Store[T]) ResolveFileRefs(
	ctx context.Context, filters []PartitionFilter,
) ([]FileRef, error) {
	if len(filters) == 0 {
		return nil, nil
	}
	where, args, err := translateFilters(filters,
		partColResolver(s.resolved.PartitionKeyParts))
	if err != nil {
		return nil, err
	}
	return s.queryFileRows(ctx, where, args)
}

// ResolveFileRefsInRange returns every catalog row whose
// written_at falls in [since, until) without fetching or
// decoding the parquet bodies. Useful for orchestration that
// scopes work by ingest time — incremental MV rebuilds, audit
// of a window, or pre-resolving refs to drive ReadFileRefsIter
// in batches.
//
// All committed rows in the window are returned regardless of
// sequencer state — atomic-visibility-on-commit per CLAUDE.md.
//
// Bound semantics:
//   - since: written_at >= since (zero → unbounded below).
//   - until: written_at < until (zero → unbounded above; the
//     "live tip captured at call entry" property holds because
//     the catalog row count is monotonically non-decreasing in
//     v2.0 — files are never deleted).
//
// Backed by the unconditional s3pgstore_files_written_at_idx
// BTREE on (written_at) — added in DDL alongside the partial
// _seq_scan_idx. The partial index can't serve this query because
// it only covers rows with feed_seq IS NULL (sequencer hot scan);
// the unconditional index covers both sequenced and unsequenced
// rows. Operators on schemas predating the index need to add it
// via their migration tool — SchemaManager.Validate doesn't
// enforce index presence (columns only).
func (s *Store[T]) ResolveFileRefsInRange(
	ctx context.Context, since, until time.Time,
) ([]FileRef, error) {
	parts := []string{}
	args := []any{}
	if !since.IsZero() {
		args = append(args, since.UTC())
		parts = append(parts,
			fmt.Sprintf("written_at >= $%d", len(args)))
	}
	if !until.IsZero() {
		args = append(args, until.UTC())
		parts = append(parts,
			fmt.Sprintf("written_at < $%d", len(args)))
	}
	if len(parts) == 0 {
		// Both bounds zero — match every row.
		parts = append(parts, "TRUE")
	}
	return s.queryFileRows(ctx, strings.Join(parts, " AND "), args)
}

// queryFileRows runs a SELECT against s3pgstore_files with the
// supplied WHERE clause + args. Every matching row comes back
// (no per-file filter, no LIMIT) in (partition_key, s3_key)
// order so the caller can group rows in a single pass and
// per-partition file order is deterministic (lex by S3 key —
// required for the dedup tie-break per CLAUDE.md).
//
// Projection covers every FileRef field the catalog stores:
//
//	file_id, partition_key, s3_key, written_at_version,
//	written_at, file_size, uncompressed_size, record_count,
//	feed_seq, ext_<col1>, ext_<col2>, ...
//
// feed_seq is nullable (NULL until the sequencer assigns) and
// scanned into FileRef.Offset as NoOffset on NULL.
func (s *Store[T]) queryFileRows(
	ctx context.Context, where string, args []any,
) ([]FileRef, error) {
	cols := []string{
		"file_id", "partition_key", "s3_key", "written_at_version",
		"written_at", "file_size", "uncompressed_size",
		"record_count", "feed_seq",
	}
	for _, c := range s.resolved.ExtensionColumns {
		cols = append(cols, "ext_"+c.Name)
	}
	q := fmt.Sprintf(
		`SELECT %s FROM %s
		WHERE %s
		ORDER BY partition_key, s3_key`,
		strings.Join(cols, ", "), s.names.Files(), where)

	var out []FileRef
	err := s.cfg.Executor.Run(ctx, func(d DBTX) error {
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
			var feedSeq *int64
			extValues := make([]any, len(s.resolved.ExtensionColumns))
			scanArgs := []any{
				&e.FileID, &e.PartitionKey, &e.S3Key, &e.Version,
				&e.WrittenAt, &e.FileSize, &e.UncompressedSize,
				&e.RecordCount, &feedSeq,
			}
			for i := range extValues {
				scanArgs = append(scanArgs, &extValues[i])
			}
			if err := rows.Scan(scanArgs...); err != nil {
				return err
			}
			if feedSeq != nil {
				e.Offset = *feedSeq
			}
			for i, c := range s.resolved.ExtensionColumns {
				if extValues[i] != nil {
					e.Extensions[c.Name] = extValues[i]
				}
			}
			out = append(out, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("SELECT files: %w", err)
	}
	return out, nil
}

// iterStreamDefaults fills the iter family's per-call defaults
// (W=1, K=1) into any field the user left unset. Designed for
// streaming consumers — single-decoder, minimum lookahead. Pass
// to fetchAndDecodeIter from every ReadIter / ReadPartitionIter /
// ReadRangeIter / ReadFileRefsIter call.
//
// User-supplied options always win: WithDecodeWorkers(N) /
// WithDecodeAheadPartitions(K) set the field before this runs;
// the helper only fills the unset case.
func iterStreamDefaults(o *readOpts, _ int) {
	if o.decodeWorkers == 0 {
		o.decodeWorkers = 1
	}
	if o.decodeAheadPartitions == nil {
		n := 1
		o.decodeAheadPartitions = &n
	}
}

// readBatchDefaults fills the Read / ReadPartition family's
// auto-tuned defaults: W = min(pool, GOMAXPROCS, lenParts),
// K = ceil(lenParts/W). Both bound by lenParts so we never spawn
// idle workers or oversize per-worker queues. The W formula
// caps at the smallest of:
//
//   - pool.MaxConcurrent — no point having more decoders than
//     the I/O pool can feed bodies to;
//   - runtime.GOMAXPROCS — decode is CPU-bound, oversubscribing
//     hurts latency more than it helps;
//   - lenParts — no work for surplus workers.
//
// K = ceil(lenParts/W) lets each worker buffer all of its
// round-robin assignments. Memory cost is absorbed by the
// already-large result slice (Read materialises everything),
// while the high K removes the "fast worker stalls during slow
// sibling's decode" hazard from skewed workloads.
//
// User-supplied options always win.
func (s *Store[T]) readBatchDefaults(o *readOpts, lenParts int) {
	if o.decodeWorkers == 0 {
		o.decodeWorkers = min(
			s.resolved.WorkerPool.MaxConcurrent(),
			runtime.GOMAXPROCS(0),
			lenParts,
		)
	}
	if o.decodeAheadPartitions == nil {
		wc := max(o.decodeWorkers, 1)
		n := (lenParts + wc - 1) / wc
		o.decodeAheadPartitions = &n
	}
}

// Read returns every record matching filters as a flat slice
// in lex partition order, deduplicated by (EntityKeyOf,
// VersionOf) when both are configured (pass WithHistory to
// opt out).
//
// Backed by the same chan-based fetch+decode pipeline as
// ReadIter, but auto-tunes for batch use: WithDecodeWorkers
// defaults to min(WorkerPool.MaxConcurrent(), GOMAXPROCS,
// lenParts) and WithDecodeAheadPartitions to ceil(lenParts/W),
// so decode runs at near-linear CPU parallelism out of the box
// for many-partition reads. Caller-supplied options always win.
//
// Materialises every record before returning — memory is O(all
// matched records). Use ReadIter for streaming consumption with
// bounded memory.
//
// Empty filters slice returns (nil, nil) — no partitions
// matched, no S3 traffic.
func (s *Store[T]) Read(
	ctx context.Context, filters []PartitionFilter, opts ...ReadOption,
) (out []T, err error) {
	defer s.metrics.methodScope(ctx, "Read", &err).end()
	if len(filters) == 0 {
		return nil, nil
	}
	rows, err := s.ResolveFileRefs(ctx, filters)
	if err != nil {
		return nil, err
	}
	o := resolveIterOpts(opts)
	var iterErr error
	s.fetchAndDecodeIter(ctx, "Read", rows, &o,
		s.readBatchDefaults, recordCollectEmit(&out, &iterErr))
	if iterErr != nil {
		return nil, iterErr
	}
	return out, nil
}

// ReadPartition returns one PartitionResult[T] per partition
// matched by filters, preserving per-partition Version and
// FileRefs alongside the records. Records within each
// partition are deduplicated by (EntityKeyOf, VersionOf) when
// both are configured (pass WithHistory to opt out).
//
// Same backend pipeline and auto-tuning as Read; same materialise-
// everything memory profile. Use ReadPartitionIter for streaming.
//
// Empty filters slice returns (nil, nil) — no partitions
// matched, no S3 traffic.
func (s *Store[T]) ReadPartition(
	ctx context.Context, filters []PartitionFilter, opts ...ReadOption,
) (out []PartitionResult[T], err error) {
	defer s.metrics.methodScope(ctx, "ReadPartition", &err).end()
	if len(filters) == 0 {
		return nil, nil
	}
	rows, err := s.ResolveFileRefs(ctx, filters)
	if err != nil {
		return nil, err
	}
	o := resolveIterOpts(opts)
	var iterErr error
	s.fetchAndDecodeIter(ctx, "ReadPartition", rows, &o,
		s.readBatchDefaults, partitionCollectEmit(&out, &iterErr))
	if iterErr != nil {
		return nil, iterErr
	}
	return out, nil
}

// ReadIter returns an iter.Seq2[T, error] that yields every
// record matching filters, lazily one partition at a time
// through the chan-based pipeline.
//
// Memory: O((WithDecodeAheadPartitions + 1) partitions' decoded
// records) by default; cap with WithDecodeAheadBytes to bound the
// uncompressed-bytes footprint. Without options, one partition's
// records sit in memory at any time.
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
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadIter", &iterErr).end()
		o := resolveIterOpts(opts)
		if len(filters) == 0 {
			return
		}
		rows, err := s.ResolveFileRefs(ctx, filters)
		if err != nil {
			iterErr = err
			yield(*new(T), err)
			return
		}
		s.fetchAndDecodeIter(ctx, "ReadIter", rows, &o,
			iterStreamDefaults, s.recordEmit(yield, &iterErr))
	}
}

// ReadPartitionIter is the per-partition variant of ReadIter:
// each yield is one PartitionResult[T] with Records, Version,
// and FileRefs populated. Same memory bound and
// cancellation semantics as ReadIter.
func (s *Store[T]) ReadPartitionIter(
	ctx context.Context, filters []PartitionFilter,
	opts ...ReadOption,
) iter.Seq2[PartitionResult[T], error] {
	return func(yield func(PartitionResult[T], error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadPartitionIter", &iterErr).end()
		o := resolveIterOpts(opts)
		if len(filters) == 0 {
			return
		}
		rows, err := s.ResolveFileRefs(ctx, filters)
		if err != nil {
			iterErr = err
			yield(PartitionResult[T]{}, err)
			return
		}
		s.fetchAndDecodeIter(ctx, "ReadPartitionIter", rows, &o,
			iterStreamDefaults, s.partitionEmit(yield, &iterErr))
	}
}

// ReadRangeIter walks every record whose written_at falls in
// [since, until). Bounds are resolved at call entry via the
// SELECT's WHERE clause, so the upper bound stays stable under
// concurrent writes — once the catalog SELECT runs, no new rows
// can appear in the result set.
//
// Filters by write commit time, not sequencer-assignment time:
// recently-written rows that the sequencer hasn't yet processed
// are still visible (consistent with the atomic-visibility-on-
// commit invariant in CLAUDE.md). Use Poll / PollRecords if you
// need a sequenced-rows-only view.
//
// Half-open semantics:
//   - since.IsZero() → start from the earliest write (unbounded
//     below).
//   - until.IsZero() → walk to the live tip (unbounded above —
//     captured by the SELECT; rows committed after the SELECT
//     don't contribute).
//   - Records at since are included; records at until are not.
//
// No partition filter is applied — every partition with any
// in-range row contributes records.
func (s *Store[T]) ReadRangeIter(
	ctx context.Context, since, until time.Time,
	opts ...ReadOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadRangeIter", &iterErr).end()
		o := resolveIterOpts(opts)
		rows, err := s.ResolveFileRefsInRange(ctx, since, until)
		if err != nil {
			iterErr = err
			yield(*new(T), err)
			return
		}
		s.fetchAndDecodeIter(ctx, "ReadRangeIter", rows, &o,
			iterStreamDefaults, s.recordEmit(yield, &iterErr))
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
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadPartitionRangeIter", &iterErr).end()
		o := resolveIterOpts(opts)
		rows, err := s.ResolveFileRefsInRange(ctx, since, until)
		if err != nil {
			iterErr = err
			yield(PartitionResult[T]{}, err)
			return
		}
		s.fetchAndDecodeIter(ctx, "ReadPartitionRangeIter", rows, &o,
			iterStreamDefaults, s.partitionEmit(yield, &iterErr))
	}
}

// ReadFileRefsIter decodes pre-resolved FileRef slices without
// re-querying the catalog. Each ref's S3Key is validated up
// front against this Store's bucket+prefix; an entry from a
// different Store fails with an error before any S3 traffic.
//
// Records emit in lex order of PartitionKey. The
// pipeline groups by partition first and sorts the partition
// keys lex before driving the producer — the input slice's
// order is not preserved across partitions (per the
// "Deterministic emission order" contract in CLAUDE.md).
func (s *Store[T]) ReadFileRefsIter(
	ctx context.Context, entries []FileRef,
	opts ...ReadOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadFileRefsIter", &iterErr).end()
		o := resolveIterOpts(opts)
		if err := s.validateFileRefs(entries); err != nil {
			iterErr = err
			yield(*new(T), err)
			return
		}
		s.fetchAndDecodeIter(ctx, "ReadFileRefsIter", entries, &o,
			iterStreamDefaults, s.recordEmit(yield, &iterErr))
	}
}

// ReadPartitionFileRefsIter is the per-partition variant of
// ReadFileRefsIter.
func (s *Store[T]) ReadPartitionFileRefsIter(
	ctx context.Context, entries []FileRef,
	opts ...ReadOption,
) iter.Seq2[PartitionResult[T], error] {
	return func(yield func(PartitionResult[T], error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadPartitionFileRefsIter", &iterErr).end()
		o := resolveIterOpts(opts)
		if err := s.validateFileRefs(entries); err != nil {
			iterErr = err
			yield(PartitionResult[T]{}, err)
			return
		}
		s.fetchAndDecodeIter(ctx, "ReadPartitionFileRefsIter", entries, &o,
			iterStreamDefaults, s.partitionEmit(yield, &iterErr))
	}
}

// validateFileRefs verifies every entry's S3Key lives under
// this Store's data prefix. Catches "entries from a different
// Store" before any S3 traffic — much cheaper to fail upfront
// than to discover via 404 mid-iteration.
//
// Cross-Store entries are an easy mistake to make (poll one
// Store, decode in another) and the error message points to
// the offending key.
func (s *Store[T]) validateFileRefs(entries []FileRef) error {
	dataPrefix := s.dataPath() + "/"
	for i, e := range entries {
		if !strings.HasPrefix(e.S3Key, dataPrefix) {
			return fmt.Errorf(
				"entries[%d].S3Key %q does not belong to "+
					"this Store (expected prefix %q)",
				i, e.S3Key, dataPrefix)
		}
	}
	return nil
}

// recordEmit returns the per-batch emit callback that flattens
// each partition's already-dedup'd records into the consumer's
// iter.Seq2[T, error] yield. Used by ReadIter / ReadRangeIter /
// ReadFileRefsIter — paths that surface records one at a time.
//
// On a hard pipeline error: sets *iterErr, yields (zero T, err)
// once, returns false so the emit loop terminates and the
// deferred methodScope.end picks up *iterErr for outcome
// classification.
func (s *Store[T]) recordEmit(
	yield func(T, error) bool, iterErr *error,
) func(decodedBatch[T]) bool {
	return func(b decodedBatch[T]) bool {
		if b.err != nil {
			*iterErr = b.err
			yield(*new(T), b.err)
			return false
		}
		for _, r := range b.records {
			if !yield(r, nil) {
				return false
			}
		}
		return true
	}
}

// partitionEmit returns the per-batch emit callback that yields
// one PartitionResult[T] per partition. Used by
// ReadPartitionIter / ReadPartitionRangeIter /
// ReadPartitionFileRefsIter — paths that surface records grouped
// by partition.
//
// On a hard pipeline error: sets *iterErr, yields a zero
// PartitionResult with the error, returns false so the emit
// loop terminates.
func (s *Store[T]) partitionEmit(
	yield func(PartitionResult[T], error) bool, iterErr *error,
) func(decodedBatch[T]) bool {
	return func(b decodedBatch[T]) bool {
		if b.err != nil {
			*iterErr = b.err
			yield(PartitionResult[T]{}, b.err)
			return false
		}
		return yield(PartitionResult[T]{
			PartitionKey: b.partitionKey,
			Records:      b.records,
			Version:      b.version,
			FileRefs:     b.files,
		}, nil)
	}
}

// recordCollectEmit returns the per-batch emit callback that
// appends each partition's records into the *out slice. Used by
// Read — collects every record across every partition into one
// flat result. On a hard pipeline error: sets *iterErr and
// returns false to terminate the emit loop.
func recordCollectEmit[T any](
	out *[]T, iterErr *error,
) func(decodedBatch[T]) bool {
	return func(b decodedBatch[T]) bool {
		if b.err != nil {
			*iterErr = b.err
			return false
		}
		*out = append(*out, b.records...)
		return true
	}
}

// partitionCollectEmit returns the per-batch emit callback that
// appends each PartitionResult into the *out slice. Used by
// ReadPartition — preserves per-partition Version and
// FileRefs alongside records. On a hard pipeline error:
// sets *iterErr and returns false.
func partitionCollectEmit[T any](
	out *[]PartitionResult[T], iterErr *error,
) func(decodedBatch[T]) bool {
	return func(b decodedBatch[T]) bool {
		if b.err != nil {
			*iterErr = b.err
			return false
		}
		*out = append(*out, PartitionResult[T]{
			PartitionKey: b.partitionKey,
			Records:      b.records,
			Version:      b.version,
			FileRefs:     b.files,
		})
		return true
	}
}

// fetchAndDecodeIter is the chan-based streaming pipeline
// backing every ReadIter / ReadPartitionIter / ReadRangeIter /
// ReadPartitionRangeIter / ReadFileRefsIter /
// ReadPartitionFileRefsIter call.
//
// Three concurrent stages plus the caller's emit loop:
//
//  1. Fetcher goroutine: walks partitions in lex order,
//     acquires one body-pool slot per file (fetcher-side
//     back-pressure — see CLAUDE.md "Shared-pool workers must
//     never block on per-call coordination"), then submits one
//     download task per file to the Store's shared *pool.Pool.
//     Cross-partition lookahead happens here — pool tasks are
//     not partition-bound, so partition P+1's downloads can run
//     in parallel with partition P being yielded.
//
//  2. Decoder goroutine: walks partitions in order; for each,
//     waits until all its files are downloaded, parses each
//     parquet footer to compute the partition's exact
//     uncompressed total, gates on (DecodeAheadPartitions,
//     DecodeAheadBytes), decodes records, sort+dedup's them
//     in-place, and pushes a decodedBatch to the emitter.
//
//  3. Emit loop (this goroutine): pulls decoded partitions in
//     order, hands each to emitOne (record-by-record yield or
//     PartitionResult yield), and frees the partition's
//     reserved bytes on completion so the decoder can proceed.
//
// On a hard pipeline error, decoder sends decodedBatch{err:err}
// and the emit callback receives a non-nil err — it should
// yield the error to the consumer, set iterErr, and return
// false. On success, emit returns true to keep going.
//
// Pool-worker shape: the submitted task does only the S3 GET
// and a markComplete / state.releaseBodySlots / recordHardErr
// update. It never blocks on per-call coordination — body-slot
// acquire is on the fetcher, decoder back-pressure is in the
// (non-pool) decoder goroutine. This satisfies the shared-pool
// rule that pool tasks must always make progress, so a slow
// consumer of one ReadIter cannot starve unrelated Stores
// sharing the pool.
func (s *Store[T]) fetchAndDecodeIter(
	ctx context.Context, method string, entries []FileRef,
	opts *readOpts, applyDefaults func(*readOpts, int),
	emitOne func(decodedBatch[T]) bool,
) {
	if len(entries) == 0 {
		return
	}
	parts := s.preparePartitions(entries)
	if len(parts) == 0 {
		return
	}

	// Apply per-call defaults — iterStreamDefaults for the iter
	// family (W=1, K=1) or s.readBatchDefaults for Read /
	// ReadPartition (auto-tuned from pool / GOMAXPROCS / lenParts).
	// Caller-supplied options always take precedence; the defaulter
	// only fills fields the user left unset.
	applyDefaults(opts, len(parts))

	// Universal clamp: surplus workers (W > len(parts)) would
	// spawn only to exit immediately because their loop's
	// `pi := workerIdx; pi < len(parts)` start is past the end.
	// emit's `pi % W` over pi < len(parts) never indexes them, so
	// this is purely allocation savings — same access pattern, no
	// behavior change. Applied here (not in the defaulters) so it
	// catches user-supplied W > len(parts) too.
	if opts.decodeWorkers > len(parts) {
		opts.decodeWorkers = len(parts)
	}

	// bodyCap bounds the per-call resident compressed bodies:
	// fetcher acquires a slot per file; decoder releases on
	// nil-out. Defaults to pool.MaxConcurrent so a single reader
	// saturates the pool's S3-op budget; WithFetchAheadFiles
	// dials it down for shared-pool deployments where each
	// reader holding the full budget inflates aggregate body
	// memory. Floor at max(filesPerPartition) keeps oversized
	// partitions live — without it the fetcher blocks on the cap
	// with the decoder waiting for the partition's remaining
	// files (deadlock).
	bodyCap := opts.fetchAheadFiles
	if bodyCap <= 0 {
		bodyCap = s.resolved.WorkerPool.MaxConcurrent()
	}
	for _, p := range parts {
		if n := len(p.files); n > bodyCap {
			bodyCap = n
		}
	}

	// state.ctx is the single cancellation source for every
	// stage. WithCancelCause propagates the parent's cause
	// automatically and lets us attach an abort reason
	// atomically with our own cancels — see streamState's
	// type comment for the rationale.
	stateCtx, cancel := context.WithCancelCause(ctx)
	state := &streamState{
		ctx:    stateCtx,
		cancel: cancel,
		parts:  parts,
		slotCh: make(chan struct{}, bodyCap),
		m:      s.metrics,
	}

	// One WaitGroup covers every helper goroutine so the deferred
	// cleanup below can cancel ctx and then wait for everything to
	// drain before returning — no orphaned goroutines, no leaked
	// state. cancel(context.Canceled) is a no-op if some other
	// path already set the cause; only the first cancel sticks.
	var wg sync.WaitGroup
	defer func() {
		cancel(context.Canceled)
		wg.Wait()
	}()

	// Stage 1: fetcher. Acquires body slots and submits per-file
	// download tasks to the shared pool. Calls g.Wait() before
	// exiting so all in-flight pool tasks drain before
	// fetchAndDecodeIter returns.
	//
	// On any pool task error, the task fn calls recordHardErr
	// (cancelling state.ctx with the wrapped err as cause)
	// before markComplete. gctx is derived from state.ctx, so
	// it auto-cancels too — fetcher sees gctx done in
	// acquireBodySlot, sibling tasks observe gctx done in
	// s.target.get. Files past the fetcher's exit point are
	// deliberately NOT markComplete'd; their partitions' done
	// channels would block waitForPartition forever, except
	// state.ctx is cancelled, so waitForPartition's ctx.Done
	// branch fires and decode workers forward hardErr
	// (= context.Cause(state.ctx)).
	//
	// g.Wait's return is ignored — for any non-nil result
	// state.ctx is already cancelled with the right cause (task
	// error path or parent propagation). The g.Wait call exists
	// only to drain in-flight tasks before this goroutine exits
	// so wg.Wait sees all pool work complete before
	// fetchAndDecodeIter returns and the streamState is dropped.
	g, gctx := s.resolved.WorkerPool.WithContext(state.ctx)
	wg.Go(func() {
		s.runFetcher(gctx, g, state)
		_ = g.Wait()
	})
	// No cancel-broadcast helper goroutine: every blocking
	// primitive (slotCh acquire, partState.done wait,
	// workerState.releaseSig bell) selects on state.ctx.Done()
	// natively. See CLAUDE.md "Concurrency invariants".

	// Stage 2: decode workers. Each worker handles partitions
	// where pi % decodeWorkers == workerIdx and writes results
	// to its own queue (cap = decodeAheadPartitions). Per-worker
	// queues replace a shared decodedCh so workers don't compete
	// on a single buffer; lex emit order is preserved by the
	// emit loop reading queue[(pi mod W)] sequentially.
	//
	// applyDefaults + the clamp above guarantee
	// decodeWorkers in [1, len(parts)] and
	// decodeAheadPartitions != nil — no fallbacks needed here.
	workers := make([]*workerState[T], opts.decodeWorkers)
	for w := range workers {
		workers[w] = newWorkerState[T](*opts.decodeAheadPartitions, s.metrics)
	}
	for w := range workers {
		wg.Go(func() {
			s.runDecodeWorker(state.ctx, state, workers[w],
				w, opts.decodeWorkers, opts)
		})
	}

	// Stall watchdog. Pure observer — surfaces deadlocks
	// (library regressions) and slow consumers via slog.Warn +
	// s3pgstore.read.iter.stall.count, never cancels. See
	// runDeadlockObserver for the rationale.
	wg.Go(func() {
		state.runDeadlockObserver(state.ctx, method,
			stallTickInterval, stallThreshold)
	})

	// Stage 3: emit loop. Walks partitions in lex order, reads
	// each from its assigned worker's queue, hands the batch to
	// the per-method emit callback, then releases that worker's
	// byte budget so it can claim its next partition.
	for pi := range state.parts {
		// Surface emit position to the observer so a stalled
		// pipeline's slog.Warn points at the right partition.
		state.decoderPi.Store(int64(pi))
		ws := workers[pi%opts.decodeWorkers]
		var batch decodedBatch[T]
		select {
		case batch = <-ws.queue:
		case <-state.ctx.Done():
			emitOne(decodedBatch[T]{err: context.Cause(state.ctx)})
			return
		}
		ok := emitOne(batch)
		ws.releaseBytes(batch.uncompBytes)
		if !ok {
			return
		}
	}
}

// preparePartitions groups entries by partition_key (entries
// must arrive in lex partition_key, then s3_key order — both
// the SQL ORDER BY and Poll's projection produce that order,
// EXCEPT a caller-supplied entries slice may be unordered.
// Defensive sort here regardless: cheap on already-sorted input,
// mandatory for out-of-order input.
//
// Each partition's files are sorted by S3Key (deterministic
// download order); per-file Version + Extensions are already on
// the FileRef, so the decoded batch can carry PartitionResult's
// Version + FileRefs without a second pass.
func (s *Store[T]) preparePartitions(
	entries []FileRef,
) []*partState {
	if len(entries) == 0 {
		return nil
	}
	byPartition := make(map[string][]FileRef)
	for _, e := range entries {
		byPartition[e.PartitionKey] = append(byPartition[e.PartitionKey], e)
	}
	partitionKeys := make([]string, 0, len(byPartition))
	for k := range byPartition {
		partitionKeys = append(partitionKeys, k)
	}
	// Public contract: partition emission is lex-ordered. Every
	// read path (Read / ReadIter / ReadPartitionIter / ... /
	// ReadFileRefsIter / ReadPartitionFileRefsIter) flows through
	// this sort. Removing it surfaces Go's randomized map
	// iteration order to the consumer and breaks byte-for-byte
	// stable output across calls — see "Deterministic emission
	// order across read and write paths" in CLAUDE.md.
	slices.Sort(partitionKeys)

	parts := make([]*partState, 0, len(partitionKeys))
	for _, k := range partitionKeys {
		files := byPartition[k]
		// Per-partition file ordering by S3Key — deterministic
		// decode order within a partition, also load-bearing for
		// dedup tie-break (lex-later filename wins on equal
		// max version, per CLAUDE.md).
		slices.SortFunc(files, func(a, b FileRef) int {
			return strings.Compare(a.S3Key, b.S3Key)
		})

		var version int64
		for _, f := range files {
			if f.Version > version {
				version = f.Version
			}
		}

		// byPartition only emits non-empty buckets (every insert
		// is append, so len(files) >= 1), so we can construct
		// partState directly without an empty-files defense.
		parts = append(parts, &partState{
			partitionKey: k,
			files:        files,
			version:      version,
			bodies:       make([][]byte, len(files)),
			done:         make(chan struct{}),
		})
	}
	return parts
}

// partState holds per-partition download progress. partitionKey,
// files, and version are fixed at preparePartitions time;
// bodies + completed are mutated by pool tasks under
// streamState.mu.
//
// done is a per-partition completion signal. markComplete closes
// it when the final file lands; waitForPartition selects on it
// (plus state.ctx.Done) so the decoder observes both completion
// and cancellation natively without a cond.Broadcast helper.
// Closed exactly once: the completed == len(files) edge is
// reached at most once because each downloader marks each file
// at most once and the increment is mutex-protected, so only
// one goroutine sees the final increment.
//
// No per-file errors slice — pool tasks call recordHardErr to
// cancel state.ctx with the wrapped err as cause; the decoder
// receives that cause via waitForPartition's return value.
type partState struct {
	partitionKey string
	files        []FileRef
	version      int64
	bodies       [][]byte
	completed    int
	done         chan struct{}
}

// streamState is the shared mutable state coordinating the
// fetcher, decode workers, and emit loop. The byte budget
// (WithDecodeAheadBytes) is per-decode-worker and lives on
// workerState — keeping it off the shared struct removes the
// multi-waiter coordination concern.
//
// ctx + cancel are the single cancellation source for every
// stage, built via context.WithCancelCause from the parent.
// recordHardErr cancels with the abort reason as cause,
// atomically with the close of ctx.Done — any observer that
// sees ctx.Done and reads context.Cause(ctx) is guaranteed the
// cause (no race between cancel and err-read). Parent cancel
// auto-propagates with its cause intact. First cancel wins;
// subsequent cancels are no-ops, preserving "first hard err
// wins" semantics. Replaces an earlier two-channel design
// (separate firstHardErr field + outer ctx) where a parent-ctx
// cancel could race the recordHardErr write.
//
// slotCh is the body-slot semaphore, a buffered channel with
// cap = bodyCap. Go's runtime drains a channel's sendq FIFO on
// every receive, so releaseBodySlots wakes the earliest-parked
// sender. The chan-based shape removes a scheduler-biased
// starvation window in the prior cond+counter design — see
// CLAUDE.md "Concurrency invariants" for the deadlock trace.
//
// m is the metrics handle. acquireBodySlot reports wait duration
// when the call blocked and succeeded; cancel-during-wait is
// not recorded (shutdown noise would drown out the saturation
// signal).
type streamState struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	mu     sync.Mutex
	parts  []*partState
	slotCh chan struct{}
	m      *metrics

	// lastProgressNs is the wall-clock timestamp (UnixNano) of
	// the most recent forward-progress event in the pipeline:
	// markComplete (a download landed) or releaseBodySlots (the
	// decoder advanced through a file). Read by
	// runDeadlockObserver to surface pipelines that have parked
	// indefinitely. Atomic so the observer reads lock-free;
	// staleness from a recent progress event between the load
	// and the threshold check is fine — the observer fires at
	// most every tick anyway.
	lastProgressNs atomic.Int64
	// decoderPi is the partition index the emit loop is currently
	// waiting for. Surfaced by the observer to point operators at
	// the stuck partition. With multiple decode workers, this
	// reflects emit's position (the gating waiter), not any
	// individual worker's progress.
	decoderPi atomic.Int64
}

// recordHardErr cancels state.ctx with cause = err. First call
// sets the cause; subsequent calls are no-ops (cancel is
// idempotent and only the first cause sticks). Used by:
//
//   - pool task error path: surfaces the wrapped GET err to
//     the consumer.
//   - submitter cancel-detected path: defensive — state.ctx is
//     already cancelled by whoever cancelled it (parent or a
//     sibling task), so this is a no-op but documents intent.
//   - submitter goroutine after g.Wait: covers the case where
//     g.Wait returned an err that no in-tree path recorded
//     (e.g., parent cancel during in-flight tasks).
//
// Cancel is immediate. Listeners on state.ctx (decoder,
// watchdog, in-flight pool tasks via gctx, submitter via gctx)
// observe ctx.Done as soon as the first hard err is recorded.
//
// This is load-bearing for partial-body protection: ordering
// recordHardErr BEFORE markComplete in the pool task fn means
// any observer that sees p.done close (markComplete's tail
// effect) is guaranteed to subsequently observe state.ctx done
// and read the cause via hardErr — no decoder can silently
// emit a partition with mixed present/nil bodies.
//
// Trade-off: a decoder that's mid-decode of partition K-1
// when recordHardErr fires for partition K may have its
// sendBatch racing the cancel. If decodedCh is full (slow
// consumer), K-1's batch may be dropped instead of emitted.
// In practice decodedCh is usually drained promptly by the
// emit loop, so K-1 lands. The trade is intentional —
// surfacing the abort reason quickly and reliably is more
// important than guaranteeing one extra batch on slow
// consumers.
func (s *streamState) recordHardErr(err error) {
	if err == nil {
		return
	}
	s.cancel(err)
}

// acquireBodySlot reserves one slot in the compressed-body
// pool. Blocks while the pool is full and ctx is alive. Returns
// nil on successful acquire; non-nil error (= context.Cause(ctx))
// when ctx is cancelled while waiting — caller can use the
// returned error directly without a separate hardErr lookup.
//
// The pool bounds the worst-case compressed-byte footprint of
// the pipeline to roughly cap × largest_compressed_size.
// Implemented via streamState.slotCh; see its type comment for
// the FIFO + deadlock-fix rationale.
//
// Records to metrics.recordIterBodySlotWait only when the slot
// wasn't immediately available AND the acquire eventually
// succeeded — cancel-during-wait is intentionally not recorded
// (shutdown noise, near-zero duration would drown out the
// saturation signal).
func (s *streamState) acquireBodySlot(ctx context.Context) error {
	if s.slotCh == nil {
		return context.Cause(ctx)
	}
	// Non-blocking fast path: slot available, no wait
	// observation.
	select {
	case s.slotCh <- struct{}{}:
		return nil
	default:
	}
	waitStart := time.Now()
	select {
	case s.slotCh <- struct{}{}:
		s.m.recordIterBodySlotWait(ctx, time.Since(waitStart))
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// releaseBodySlots returns n slots to the pool. Each receive on
// slotCh wakes the FIFO-earliest blocked downloader (per Go's
// channel sendq). Also bumps lastProgressNs so
// runDeadlockObserver sees decoder-side progress (slots being
// freed = the decoder is making its way through partitions).
func (s *streamState) releaseBodySlots(n int) {
	if n <= 0 || s.slotCh == nil {
		return
	}
	for range n {
		<-s.slotCh
	}
	s.lastProgressNs.Store(time.Now().UnixNano())
}

// markComplete is the pool-task-side update: store the body in
// the partition's slot, increment completed, and close the
// partition's done channel when the final file lands. body is
// nil for hard errors — the decoder will see waitForPartition
// return non-nil (because the failing task called recordHardErr
// before this markComplete, cancelling state.ctx) and forward
// that cause to the consumer.
//
// The completed == len(files) edge is reached at most once
// across concurrent downloaders because each file is marked
// exactly once and the increment is mutex-protected — only one
// goroutine sees the counter hit len(files), so close(p.done)
// runs exactly once. The close itself is intentionally outside
// the critical section so we don't hold mu across the
// channel-close runtime call.
//
// Bumps lastProgressNs after the mutex is released so
// runDeadlockObserver observes a fresh timestamp on every
// downloaded file, lock-free.
func (s *streamState) markComplete(
	partIdx, fileIdx int, body []byte,
) {
	p := s.parts[partIdx]
	s.mu.Lock()
	p.bodies[fileIdx] = body
	p.completed++
	finalFile := p.completed == len(p.files)
	s.mu.Unlock()
	if finalFile {
		close(p.done)
	}
	s.lastProgressNs.Store(time.Now().UnixNano())
}

// waitForPartition blocks until every file in partition pi has
// been downloaded OR ctx is cancelled. Returns nil iff the
// partition completed AND ctx is still alive at return time;
// non-nil error (= context.Cause(ctx)) otherwise.
//
// The post-condition (nil ⇒ ctx alive) is load-bearing: it
// folds in the "did a sibling task error during our wait?"
// check the decoder used to do separately. recordHardErr
// always precedes markComplete (CLAUDE.md "Multi-goroutine
// pipelines unify cancel + abort reason"), so when p.done
// closes for a partition that has any nil body, state.ctx is
// already cancelled. Returning the cause here lets the decoder
// forward it without an extra hardErr lookup; without it, the
// decoder could silently emit a partial PartitionResult (some
// bodies present, some marked nil by failed tasks).
func (s *streamState) waitForPartition(
	ctx context.Context, pi int,
) error {
	p := s.parts[pi]
	select {
	case <-p.done:
	case <-ctx.Done():
	}
	return context.Cause(ctx)
}

// workerState is the per-decode-worker state: an output queue
// the worker fills with decoded batches and a private byte
// budget. Each worker is the sole reserver against its own
// budget; emit is the sole releaser. Single-waiter semantics on
// releaseSig (this worker), so the same chan(1) edge-trigger
// pattern as the prior shared byteWake works without multi-
// waiter coordination.
//
// Workers self-assign partitions round-robin (worker w handles
// pi where pi % W == w); emit drains queues in lex pi order so
// the deterministic-emission contract holds.
type workerState[T any] struct {
	queue chan decodedBatch[T]

	mu            sync.Mutex
	bufferedBytes int64
	releaseSig    chan struct{}

	m *metrics
}

func newWorkerState[T any](queueCap int, m *metrics) *workerState[T] {
	return &workerState[T]{
		queue:      make(chan decodedBatch[T], queueCap),
		releaseSig: make(chan struct{}, 1),
		m:          m,
	}
}

// reserveBytes accounts uncomp bytes against the per-worker cap.
// Blocks while bufferedBytes + uncomp would exceed cap AND the
// buffer is non-empty; the empty-buffer escape lets a single
// oversized partition through (otherwise the worker would block
// on the cap with emit waiting for the result). Returns nil on
// successful reservation; non-nil error (= context.Cause(ctx))
// when ctx is cancelled while waiting.
//
// Single-waiter discipline: only this worker reserves against
// ws; only emit releases. While the worker is parked,
// bufferedBytes can only stay the same or decrease (no other
// worker writes to ws). Stale releaseSig signals are therefore
// harmless — the loop always re-checks the predicate.
//
// Records to metrics.recordIterByteBudgetWait only when the
// wait fired AND the reservation succeeded — cancel path is
// not recorded.
func (ws *workerState[T]) reserveBytes(
	ctx context.Context, uncomp, cap int64,
) error {
	if cap <= 0 || uncomp <= 0 {
		return context.Cause(ctx)
	}
	var waitStart time.Time
	waited := false
	for {
		ws.mu.Lock()
		fits := ws.bufferedBytes <= 0 || ws.bufferedBytes+uncomp <= cap
		if fits {
			ws.bufferedBytes += uncomp
			ws.mu.Unlock()
			if waited {
				ws.m.recordIterByteBudgetWait(ctx, time.Since(waitStart))
			}
			return nil
		}
		ws.mu.Unlock()
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if !waited {
			waitStart = time.Now()
			waited = true
		}
		select {
		case <-ws.releaseSig:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// releaseBytes is called by the emit loop after a partition's
// records have been forwarded; frees the reservation so the
// owning worker can pick the next partition. The non-blocking
// send on releaseSig is the bell-ring: a coalesced wake costs
// at most one extra trip through the predicate loop.
func (ws *workerState[T]) releaseBytes(uncomp int64) {
	if uncomp <= 0 {
		return
	}
	ws.mu.Lock()
	ws.bufferedBytes -= uncomp
	ws.mu.Unlock()
	select {
	case ws.releaseSig <- struct{}{}:
	default:
	}
}

// runFetcher walks partitions in order and submits
// one download task per file to the shared pool. The body-slot
// acquire happens here on the fetcher side rather than inside
// the pool task — pool workers must never block on per-call
// coordination (CLAUDE.md "Shared-pool workers must never block
// on per-call coordination"), or N waiting workers could starve
// every other Group sharing the pool, including unrelated
// Stores.
//
// Concurrency model:
//
//   - Fetcher is one goroutine; acquireBodySlot blocks here
//     when the per-call slot pool is full, so the fetcher
//     itself paces the pipeline.
//   - g.Submit blocks when the shared pool has no free slot.
//     Both back-pressure points run on the fetcher, never
//     on the pool worker.
//   - In flight: at most min(bodyCap, pool.MaxConcurrent) S3
//     GETs at any time across this call's submitted tasks.
//
// All errors are strict: a NoSuchKey from S3 is treated as a
// hard error (the catalog points at it; missing means
// operator-driven prune or a torn write — incident-class). The
// task records hardErr and returns the wrapped error so the
// pool's errgroup cancels gctx — sibling tasks observe ctx.Done
// in s.target.get and bail.
//
// Body-slot leak on submit-skip is harmless. If gctx is
// cancelled between acquireBodySlot and g.Submit, the task fn
// never runs and the slot stays held — but slotCh is per-call
// state torn down by fetchAndDecodeIter's deferred wg.Wait, and
// waitForPartition observes ctx.Done. No deadlock, no observable
// leak.
func (s *Store[T]) runFetcher(
	ctx context.Context, g *pool.Group, state *streamState,
) {
	for pi := range state.parts {
		for fi := range state.parts[pi].files {
			if state.acquireBodySlot(ctx) != nil {
				// state.ctx is already cancelled with a cause
				// set (either parent propagation or a sibling
				// task's recordHardErr — the failing task
				// always cancels before returning to errgroup,
				// so by the time gctx is done state.ctx is
				// done too). The decoder will reach this
				// partition's waitForPartition, observe ctx
				// done, and forward hardErr — no need for us
				// to markComplete or recordHardErr here.
				return
			}
			key := state.parts[pi].files[fi].S3Key
			g.Submit(ctx, func(ctx context.Context) error {
				body, err := s.target.get(ctx, key)
				if err != nil {
					// No body materialised — return the slot.
					state.releaseBodySlots(1)
					wrapped := fmt.Errorf("get %s: %w", key, err)
					state.recordHardErr(wrapped)
					state.markComplete(pi, fi, nil)
					return wrapped
				}
				// Slot stays held; decoder releases it when
				// bodies are nil'd in decodePartition.
				state.markComplete(pi, fi, body)
				return nil
			})
		}
	}
}

// runDecodeWorker handles partitions assigned to worker w via
// round-robin: pi where pi % numWorkers == w. Each iteration
// waits for downloads, parses the footer, reserves byte budget
// against this worker's private cap, decodes, and publishes the
// result to ws.queue. Emit drains ws.queue in lex pi order.
//
// On any error (download, footer, decode), the failing path
// publishes a decodedBatch{err} to ws.queue so emit forwards the
// cause to the consumer; the worker then exits without claiming
// further partitions. ctx cancellation is observed at every
// blocking site (waitForPartition, reserveBytes, queue send).
//
// Error precedence: context.Cause(state.ctx) is the single
// source of truth for hard download errors, and the
// wait/reserve helpers return that cause directly when they
// return non-nil. waitForPartition's post-condition (nil ⇒ ctx
// alive) folds in the "did a sibling task error?" check, so no
// extra pre/post checks around the wait.
func (s *Store[T]) runDecodeWorker(
	ctx context.Context, state *streamState,
	ws *workerState[T], workerIdx, numWorkers int, opts *readOpts,
) {
	for pi := workerIdx; pi < len(state.parts); pi += numWorkers {
		if err := state.waitForPartition(ctx, pi); err != nil {
			sendBatch(ctx, ws.queue, decodedBatch[T]{err: err})
			return
		}
		ps := state.parts[pi]

		// Parse footers once: exact uncompressed total for the
		// byte budget AND total row count for pre-allocating
		// the decoded slice. Missing files (nil body) contribute
		// zero — but in s3pgstore the strict-error policy means
		// any nil body is paired with a non-nil hardErr, and
		// waitForPartition would have returned non-nil above.
		// Defensive nil-skip here is for the cancel path where
		// the producer never reached some files.
		uncomp, totalRows, err := footerStats(ps)
		if err != nil {
			sendBatch(ctx, ws.queue, decodedBatch[T]{err: err})
			return
		}

		// Gate on this worker's byte budget if configured. A
		// single oversized partition still flows once the
		// worker's buffer is empty — otherwise the worker would
		// block on the cap with emit waiting for the result.
		if err := ws.reserveBytes(ctx, uncomp, opts.decodeAheadBytes); err != nil {
			sendBatch(ctx, ws.queue, decodedBatch[T]{err: err})
			return
		}

		decodeStart := time.Now()
		recs, err := s.decodePartition(state, ps, totalRows,
			opts.includeHistory)
		state.m.recordIterDecodeDuration(ctx, time.Since(decodeStart))
		// decodePartition nils each body + releases its
		// body-pool slot per-file; nothing else to clean up at
		// the partition level.
		if err != nil {
			ws.releaseBytes(uncomp)
			sendBatch(ctx, ws.queue, decodedBatch[T]{err: err})
			return
		}

		if !sendBatch(ctx, ws.queue, decodedBatch[T]{
			partitionKey: ps.partitionKey,
			records:      recs,
			version:      ps.version,
			files:        ps.files,
			uncompBytes:  uncomp,
		}) {
			ws.releaseBytes(uncomp)
			return
		}
	}
}

// decodePartition parses every successfully-downloaded body in
// ps, sort+dedup's the concatenated records, and returns the
// final slice. Each body is nil'd and its body-pool slot
// released as soon as the file is decoded, so the
// compressed-byte footprint inside a single partition's decode
// shrinks to ~one body instead of holding every file's
// compressed bytes for the full loop.
//
// The pre-dedup slice is pre-sized to totalRows (summed from
// row-group metadata in footerStats) so growth-doubling doesn't
// inflate the transient allocation peak. sortAndDedup compacts
// in-place and returns out[:n] — same backing array, length
// truncated to the survivor count.
func (s *Store[T]) decodePartition(
	state *streamState, ps *partState, totalRows int64,
	includeHistory bool,
) ([]T, error) {
	out := make([]T, 0, totalRows)
	for fi, body := range ps.bodies {
		if body == nil {
			continue
		}
		recs, err := decodeParquet[T](body)
		// Free the body and return its slot regardless of
		// decode outcome — we're done with it either way.
		ps.bodies[fi] = nil
		state.releaseBodySlots(1)
		if err != nil {
			return nil, fmt.Errorf(
				"decode %s: %w", ps.files[fi].S3Key, err)
		}
		out = append(out, recs...)
	}
	return sortAndDedup(out,
		s.resolved.EntityKeyOf,
		s.resolved.VersionOf, includeHistory), nil
}

// decodedBatch is one partition's decoded records (or a single
// hard error) flowing from the decoder to the emit loop.
//
// partitionKey is the partition the records came from (carried
// so partition-emitting public methods can surface it).
// records is already sort+dedup'd by decodePartition.
// version + files are populated at preparePartitions time
// (catalog-derived, no parquet decode required).
// uncompBytes is what the decoder reserved; the emit loop
// returns it via releaseBytes after the records are forwarded.
type decodedBatch[T any] struct {
	partitionKey string
	records      []T
	version      int64
	files        []FileRef
	uncompBytes  int64
	err          error
}

// sendBatch pushes a batch onto decodedCh, returning false on
// ctx cancellation so the caller can clean up the byte
// reservation it might have just made.
//
// Best-effort delivery: try the non-blocking send first.
// Without this the select below would race ctx.Done against a
// ready send, and Go's non-deterministic select could drop an
// error batch when the buffer has capacity AND ctx is already
// cancelled — a silent-drop hole on the strict-error path
// where the downloader's cancel() runs before the decoder's
// send arrives. Only fall back to the racing select when the
// buffer is full (consumer abandoned the iter and the deferred
// cancel keeps the pipeline from deadlocking).
func sendBatch[T any](
	ctx context.Context, decodedCh chan<- decodedBatch[T],
	b decodedBatch[T],
) bool {
	select {
	case decodedCh <- b:
		return true
	default:
	}
	select {
	case decodedCh <- b:
		return true
	case <-ctx.Done():
		return false
	}
}

// footerStats opens each non-nil body via parquet-go's footer
// parser and returns the partition's totals: uncompressed bytes
// (per-row-group total_byte_size, which the parquet spec
// defines as the total uncompressed size of all column data in
// the row group) and total row count. Metadata is parsed once
// per file (~10–100 KB of footer bytes); the body is already in
// memory so this is essentially free.
//
// The uncompressed total drives the byte-budget gate; the row
// count drives pre-sizing of the decoded slice so its growth
// doesn't double-allocate at decode time.
func footerStats(p *partState) (uncomp, totalRows int64, err error) {
	for fi, body := range p.bodies {
		if body == nil {
			continue
		}
		f, openErr := parquet.OpenFile(
			bytes.NewReader(body), int64(len(body)))
		if openErr != nil {
			return 0, 0, fmt.Errorf(
				"open %s: %w", p.files[fi].S3Key, openErr)
		}
		for _, rg := range f.Metadata().RowGroups {
			uncomp += rg.TotalByteSize
			totalRows += rg.NumRows
		}
	}
	return uncomp, totalRows, nil
}

// Stall watchdog defaults. Production fetchAndDecodeIter
// wires these via runDeadlockObserver; tests can override with
// shorter intervals so a stall signal fires within milliseconds
// rather than minutes (currently no test does so — left here
// as the upstream-vendored knob).
//
// stallTickInterval is generous: the observer is a background
// safety net, not a hot-path metric. A 30s tick on a stuck
// pipeline means at most 30s of latency between the stall
// starting and the first slog.Warn — operators see it long
// before SIGQUIT-debugging is needed.
//
// stallThreshold is double the tick: a single missed tick is
// not a stall (that would just be timing noise on a slow
// consumer that's about to pull the next batch). Two missed
// ticks signals a real lack of forward progress.
const (
	stallTickInterval = 30 * time.Second
	stallThreshold    = 60 * time.Second
)

// runDeadlockObserver is the iter pipeline's stall watchdog —
// pure observer, never cancels. Periodically wakes and emits a
// slog.Warn + s3pgstore.read.iter.stall.count increment if the
// pipeline made no forward progress (markComplete or
// releaseBodySlots) within the threshold window.
//
// The observer is a regression detector: the FIFO channel-based
// slot semaphore makes the cond+counter starvation deadlock
// unreachable, but a future change introducing a different
// stall would otherwise be silent. Operators set an alert on a
// non-zero rate of s3pgstore.read.iter.stall.count to surface
// deadlocks (real bugs) and slow consumers (heavy yield-side
// processing) — both worth seeing.
//
// Pure-observer rationale: auto-canceling on stall would mask
// information needed to diagnose the underlying issue
// (goroutine state for SIGQUIT, channel occupancy, decoder
// partition) and risk false-positive aborts of legitimately
// slow consumers. Users who want a hard ceiling pass
// ctx.WithTimeout at the call site — that propagates through
// every layer uniformly. A circuit-breaker at the iter layer is
// straightforward to add in caller code on top of this metric
// if a specific operational scenario needs it.
func (s *streamState) runDeadlockObserver(
	ctx context.Context, method string,
	tickInterval, threshold time.Duration,
) {
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	thresholdNs := threshold.Nanoseconds()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			last := s.lastProgressNs.Load()
			if last == 0 {
				// Pipeline hasn't logged any progress yet;
				// either the very-first download is still in
				// flight (a single GET that exceeds the
				// threshold is a legitimate reason to skip the
				// alert) or the pipeline is empty and about to
				// exit.
				continue
			}
			staleNs := now.UnixNano() - last
			if staleNs < thresholdNs {
				continue
			}
			slog.Warn(
				"s3pgstore: iter pipeline made no forward progress within watchdog window",
				"method", method,
				"stale_seconds", time.Duration(staleNs).Seconds(),
				"decoder_partition", s.decoderPi.Load(),
				"slot_occupancy", len(s.slotCh),
				"slot_capacity", cap(s.slotCh))
			s.m.recordIterStall(ctx, method)
		}
	}
}
