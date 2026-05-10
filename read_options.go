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
	// for the partition's remaining files (see readFetchAndDecodeIter
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
