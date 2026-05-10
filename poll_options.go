package s3pgstore

// PollOption is the interface implemented by PollRecords /
// PollRecordsIter modifiers. Distinct from ReadOption so the
// surfaces don't cross-contaminate at the call site (a
// Poll-specific WithDecodeAheadFiles passed to ReadIter is a
// compile error). Knobs that work for both pipelines live in
// iter_options.go and return IterOption (which satisfies both
// ReadOption and PollOption).
type PollOption interface {
	applyPoll(*pollOpts)
}

type pollOpts struct {
	// fetchAheadFiles caps the number of compressed parquet
	// bodies the fetcher may keep resident (in-flight + landed-
	// but-not-yet-decoded) at once. Zero or negative (default)
	// resolves to Config.WorkerPool.MaxConcurrent() so a single
	// PollRecords/PollRecordsIter call saturates the pool's
	// S3-op budget. Set explicitly to a smaller value in
	// shared-pool deployments where each concurrent reader
	// holding the full budget would inflate total resident body
	// memory.
	fetchAheadFiles int

	// decodeWorkers controls the number of parallel decode
	// goroutines. Zero or negative (default) resolves to 1 for
	// PollRecordsIter (single-decoder, deterministic CPU
	// footprint) and to min(WorkerPool.MaxConcurrent(),
	// GOMAXPROCS, lenFiles) for PollRecords. Workers self-assign
	// files round-robin (worker w handles fi where fi % W == w);
	// emit drains worker queues in feed_seq order to preserve
	// the input-order contract.
	decodeWorkers int

	// decodeAheadFiles controls how many decoded files each
	// worker may buffer in its output queue before blocking on
	// send. nil (option not supplied) resolves to 1 for
	// PollRecordsIter and ceil(lenFiles/W) for PollRecords. *p
	// == 0 is the explicit-unbuffered mode (worker blocks
	// immediately after each decode until emit drains). With
	// W > 1 workers the per-call total is W × n.
	//
	// Pointer-typed so an explicit 0 from
	// WithDecodeAheadFiles stays distinguishable from
	// "option not supplied."
	decodeAheadFiles *int

	// decodeAheadBytes caps the cumulative uncompressed parquet
	// bytes EACH decode worker may hold (currently-decoding +
	// queued for emit). Zero (default) disables the cap. Read
	// from each parquet file's footer (sum of row-group
	// total_byte_size) so the cap is exact, not a heuristic.
	// With W > 1 workers the per-call total is W × n.
	decodeAheadBytes int64
}

// resolvePollOpts collapses the variadic option list into a
// flat pollOpts struct. Defaults applied later by the
// pipeline-mode-specific defaulter (collect vs. iter).
func resolvePollOpts(opts []PollOption) pollOpts {
	var o pollOpts
	for _, opt := range opts {
		opt.applyPoll(&o)
	}
	return o
}

// WithDecodeAheadFiles tells each decode worker how many
// decoded files to buffer ahead of the consumer. The worker's
// queue is bounded at this depth; a slow consumer back-pressures
// the worker via the queue's blocking send, which in turn
// back-pressures fetch via the body-slot semaphore.
//
// Defaults to ceil(lenFiles/W) for PollRecords (collect — fast
// workers don't stall) and 1 for PollRecordsIter (minimum
// lookahead, matches the read iter family's defaults).
//
// Poll-only (the read pipeline groups by partition, so its
// queue-depth knob is WithDecodeAheadPartitions). The Poll-
// specific name keeps the partition-vs-file grain explicit at
// the call site — type system separates them anyway, but the
// name reinforces the conceptual difference.
func WithDecodeAheadFiles(n int) PollOption {
	if n < 0 {
		n = 0
	}
	return withDecodeAheadFilesOpt{n: n}
}

type withDecodeAheadFilesOpt struct{ n int }

func (o withDecodeAheadFilesOpt) applyPoll(opts *pollOpts) {
	v := o.n
	opts.decodeAheadFiles = &v
}
