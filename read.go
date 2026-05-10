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
//
// No effect on the buffered Read path.
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
// goroutines for the iter pipeline. Default (option not supplied
// or n <= 0) is 1 — single-decoder behavior, deterministic
// CPU footprint at one core.
//
// n > 1 fans out decode across n goroutines. Workers self-assign
// partitions round-robin: worker w handles partitions where
// pi % n == w. Emit drains worker queues in lex partition order
// so the deterministic-emission contract is preserved.
//
// Use for batch-analytics workloads where decode dominates the
// per-call wall-time (many partitions, reasonably uniform sizes,
// throughput-bound consumer). Streaming workloads with small
// records and a slow consumer see no benefit — decode time is
// already small relative to consumer yield.
//
// Round-robin assignment can imbalance load when partition sizes
// are strongly correlated with pi mod n; for typical workloads
// (uniform sizes, or sparsely-distributed outliers) the
// imbalance is small.
//
// Memory implications: WithDecodeAheadPartitions and
// WithDecodeAheadBytes are PER WORKER — total in-flight decoded
// memory is multiplied by n. Account for this when tuning.
//
// No effect on the buffered Read path.
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
//
// No effect on the buffered Read path (which materialises every
// partition concurrently by design).
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
//
// No effect on the buffered Read path.
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

// Read returns one PartitionResult[T] per partition matched by
// filters. Records are deduplicated by (EntityKeyOf,
// VersionOf) when both are configured (pass WithHistory to
// opt out).
//
// Single SQL query against s3pgstore_files (no per-file
// filtering, no LIMIT — every file row for every matched
// partition comes back, per CLAUDE.md). Every (partition,
// file) pair across the result set is fetched + decoded in
// parallel through the shared Config.WorkerPool (defaulted to
// 64 slots; bounded globally by s3client.Options.MaxOpenConnections
// at the HTTP transport). Records are then concatenated per
// partition in s3_key order and deduplicated once per
// partition on the calling goroutine.
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

	// Flat fan-out across every (partition, file) pair. The
	// shared pool's deadlock detector forbids same-pool
	// reentrancy (a pool task that calls g.Submit on the same
	// pool would hold a slot while waiting for another), so we
	// can't nest "fan-out partitions × fan-out files" — flatten
	// to a single fan-out and group bodies back together
	// afterward.
	//
	// Slot-indexed writes preserve per-group + per-file order:
	// each task writes to bodies[gi][fi] without coordination.
	// Concatenation + dedup runs once on the main goroutine
	// after all GETs complete; decode is done in-task to
	// parallelise the parquet decode across pool workers.
	bodies := make([][][]T, len(groups))
	type job struct {
		gi, fi int
		f      fileRow
	}
	totalFiles := 0
	for gi := range groups {
		bodies[gi] = make([][]T, len(groups[gi].files))
		totalFiles += len(groups[gi].files)
	}
	jobs := make([]job, 0, totalFiles)
	for gi, g := range groups {
		for fi, f := range g.files {
			jobs = append(jobs, job{gi: gi, fi: fi, f: f})
		}
	}
	if err := fanOutPool(ctx, s.resolved.WorkerPool, jobs, nil,
		func(ctx context.Context, _ int, j job) error {
			data, err := s.target.get(ctx, j.f.s3Key)
			if err != nil {
				return fmt.Errorf("GET %s: %w", j.f.s3Key, err)
			}
			recs, err := decodeParquet[T](data)
			if err != nil {
				return fmt.Errorf("decode %s: %w", j.f.s3Key, err)
			}
			bodies[j.gi][j.fi] = recs
			return nil
		},
	); err != nil {
		return nil, err
	}

	out := make([]PartitionResult[T], len(groups))
	for gi, g := range groups {
		total := 0
		for _, b := range bodies[gi] {
			total += len(b)
		}
		records := make([]T, 0, total)
		for _, b := range bodies[gi] {
			records = append(records, b...)
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

		out[gi] = PartitionResult[T]{
			PartitionKey:   g.key,
			Records:        records,
			Version:        version,
			FileExtensions: exts,
		}
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
