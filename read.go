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

	// fetchAheadFiles caps the number of compressed parquet
	// bodies the fetcher may keep resident (in-flight + landed-
	// but-not-yet-decoded) at once. Zero or negative (default)
	// resolves to Config.WorkerPool.MaxConcurrent() so a single
	// reader can saturate the pool's S3-op budget. Set explicitly
	// to a smaller value in shared-pool deployments where each
	// concurrent reader holding the full budget would inflate
	// total resident body memory.
	//
	// Internally floored to max(filesPerPartition) so that one
	// oversized partition still fits — without that floor the
	// fetcher would block on the cap with the decoder waiting
	// for the partition's remaining files (see read_iter.go for
	// the deadlock trace).
	fetchAheadFiles int

	// decodeWorkers controls the number of parallel decode
	// goroutines. Zero or negative (default) resolves to 1, the
	// single-decoder behavior. Workers self-assign partitions
	// round-robin (worker w handles pi where pi % W == w); emit
	// drains worker queues in lex order to preserve the
	// deterministic emission contract.
	decodeWorkers int

	// decodeAheadPartitions controls how many decoded partitions
	// each worker may buffer in its output queue before blocking
	// on send. nil (option not supplied) resolves to the default
	// of 1. *p == 0 is the explicit-unbuffered mode (worker
	// blocks immediately after each decode until emit drains).
	// With W > 1 workers the per-call total is W × n.
	//
	// Pointer-typed so the zero value of WithDecodeAheadPartitions
	// (an explicit 0) stays distinguishable from "option not
	// supplied" (which falls back to the default of 1).
	decodeAheadPartitions *int

	// decodeAheadBytes caps the cumulative uncompressed parquet
	// bytes EACH decode worker may hold (currently-decoding +
	// queued for emit). Zero (default) disables the cap. Read
	// from each parquet file's footer (sum of row-group
	// total_byte_size) so the cap is exact, not a heuristic.
	// With W > 1 workers the per-call total is W × n.
	decodeAheadBytes int64
}

// WithHistory disables the per-partition latest-per-entity
// dedup, returning every (entity, version) pair surviving
// replica collapse. No effect when EntityKeyOf or VersionOf
// are not configured.
func WithHistory() ReadOption { return withHistoryOpt{} }

type withHistoryOpt struct{}

func (withHistoryOpt) applyRead(o *readOpts) { o.includeHistory = true }

// WithFetchAheadFiles caps the number of compressed parquet
// bodies the fetcher may keep resident at once (downloads
// in-flight + landed-but-not-yet-decoded). Default (option not
// supplied or n <= 0) is Config.WorkerPool.MaxConcurrent() — a
// single reader can saturate the pool's S3-op budget.
//
// Lower values reduce per-call body memory at the cost of S3
// concurrency: the fetcher cannot dispatch beyond this cap into
// the pool, so pool slots above n sit idle for this reader. Use
// in shared-pool deployments where each concurrent reader
// independently consuming the full budget inflates aggregate
// resident body memory beyond what the operator wants.
//
// Internally floored to max(filesPerPartition) so a single
// oversized partition still flows in full — without that floor
// the fetcher would block on the cap with the decoder waiting
// on the partition's remaining files.
func WithFetchAheadFiles(n int) ReadOption {
	if n < 0 {
		n = 0
	}
	return withFetchAheadFilesOpt{n: n}
}

type withFetchAheadFilesOpt struct{ n int }

func (o withFetchAheadFilesOpt) applyRead(opts *readOpts) {
	opts.fetchAheadFiles = o.n
}

// WithDecodeWorkers sets the number of parallel decoder
// goroutines for the iter pipeline. Default for ReadIter / its
// variants is 1 (single-decoder, deterministic CPU footprint).
// Default for Read / ReadPartition is auto-tuned to
// min(WorkerPool.MaxConcurrent(), GOMAXPROCS, lenParts) so batch
// reads parallelize across cores out of the box.
//
// n > 1 fans out decode across n goroutines. Workers self-assign
// partitions round-robin: worker w handles partitions where
// pi % n == w. Emit drains worker queues in lex partition order
// so the deterministic-emission contract is preserved.
//
// Use on the iter family for batch-analytics workloads where
// decode dominates per-call wall-time (many partitions,
// reasonably uniform sizes, throughput-bound consumer).
// Streaming workloads with small records and a slow consumer
// see no benefit — decode time is already small relative to
// consumer yield.
//
// Pair with WithDecodeAheadPartitions on skewed workloads.
// With K=1 (the iter default), fast workers stall after one
// batch ahead while emit waits on a slow sibling. A larger K
// lets fast workers race ahead during slow-partition decodes.
//
// Round-robin assignment can imbalance load when partition sizes
// are strongly correlated with pi mod n; for typical workloads
// (uniform sizes, or sparsely-distributed outliers) the
// imbalance is small.
//
// Memory implications: WithDecodeAheadPartitions and
// WithDecodeAheadBytes are PER WORKER — total in-flight decoded
// memory is multiplied by n. Account for this when tuning.
func WithDecodeWorkers(n int) ReadOption {
	if n < 1 {
		n = 1
	}
	return withDecodeWorkersOpt{n: n}
}

type withDecodeWorkersOpt struct{ n int }

func (o withDecodeWorkersOpt) applyRead(opts *readOpts) {
	opts.decodeWorkers = o.n
}

// WithDecodeAheadPartitions tells each decode worker how many
// decoded partitions it may buffer in its output queue ahead of
// the emit loop. Default (option not supplied) is 1 — minimum
// useful lookahead so decode of the worker's next partition
// overlaps yield of its previous one.
//
// n=0 is the explicit-no-buffer mode: unbuffered handoff. The
// worker still decodes its next partition concurrent with emit
// draining the previous (the handoff just blocks the worker
// briefly), but never two decoded partitions per worker sit in
// memory at once.
//
// Negative values are floored to 0.
//
// Per-worker semantics: with WithDecodeWorkers(W), the per-call
// total of buffered decoded partitions is W × n. Memory:
// O((W × n + 1) partitions) — workers' queues + the one being
// yielded.
func WithDecodeAheadPartitions(n int) ReadOption {
	if n < 0 {
		n = 0
	}
	return withDecodeAheadPartitionsOpt{n: n}
}

type withDecodeAheadPartitionsOpt struct{ n int }

func (o withDecodeAheadPartitionsOpt) applyRead(opts *readOpts) {
	n := o.n
	opts.decodeAheadPartitions = &n
}

// WithDecodeAheadBytes caps the uncompressed parquet bytes EACH
// decode worker may hold (currently-decoding + queued for emit).
// Zero (default) disables the cap; only decodeAheadPartitions
// binds.
//
// Composes with WithDecodeAheadPartitions — both are evaluated
// and whichever cap binds first holds the worker back. Useful
// when partition sizes are skewed: a tiny
// WithDecodeAheadPartitions(1) is too conservative for many
// small partitions but a larger value risks OOM on a few large
// ones; a byte cap auto-tunes across both.
//
// The byte total is read from each parquet file's footer
// (total_byte_size summed across row groups), so the cap is
// exact, not a heuristic. Decoded Go memory typically runs
// 1–2× the uncompressed size depending on data shape.
//
// Per-partition guarantee: if a single partition's uncompressed
// size exceeds the cap, that one partition still decodes (the
// cap can't be enforced below partition granularity without
// row-group-level streaming).
//
// Per-worker semantics: with WithDecodeWorkers(W), the per-call
// total of decoded uncompressed bytes is W × n.
func WithDecodeAheadBytes(n int64) ReadOption {
	if n < 0 {
		n = 0
	}
	return withDecodeAheadBytesOpt{n: n}
}

type withDecodeAheadBytesOpt struct{ n int64 }

func (o withDecodeAheadBytesOpt) applyRead(opts *readOpts) {
	opts.decodeAheadBytes = o.n
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
	rows, err := s.selectFileRows(ctx, filters)
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
// FileExtensions alongside the records. Records within each
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
	rows, err := s.selectFileRows(ctx, filters)
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
