package s3pgstore

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
	// for the partition's remaining files (see fetchAndDecodeIter
	// for the deadlock trace).
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

// resolveIterOpts collapses an options slice into the
// internal opts struct.
func resolveIterOpts(opts []ReadOption) readOpts {
	var o readOpts
	for _, opt := range opts {
		opt.applyRead(&o)
	}
	return o
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
