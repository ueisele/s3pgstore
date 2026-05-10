package s3pgstore

// PollOption is the interface implemented by PollRecords /
// PollRecordsIter modifiers. Distinct from ReadOption so the
// surfaces don't cross-contaminate at the call site (a
// WithPollFetchAheadFiles passed to ReadIter is a compile error).
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
	// WithPollDecodeAheadFiles stays distinguishable from
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

// WithPollFetchAheadFiles caps the number of compressed parquet
// bodies that can be downloaded ahead of the consumer. Bounds
// the per-call resident compressed-body memory to roughly
// n * avg_file_size.
//
// Defaults to WorkerPool.MaxConcurrent() so a single PollRecords
// call saturates the pool's S3-op budget. Dial it down for
// shared-pool deployments where each reader holding the full
// budget inflates aggregate body memory across concurrent
// readers.
func WithPollFetchAheadFiles(n int) PollOption {
	if n < 0 {
		n = 0
	}
	return withPollFetchAheadFilesOpt{n: n}
}

type withPollFetchAheadFilesOpt struct{ n int }

func (o withPollFetchAheadFilesOpt) applyPoll(opts *pollOpts) {
	opts.fetchAheadFiles = o.n
}

// WithPollDecodeWorkers sets the number of parallel decoder
// goroutines. Each worker handles files where
// `fileIdx % W == workerIdx`; emit drains worker queues in
// round-robin so input order is preserved.
//
// Defaults to min(WorkerPool.MaxConcurrent(), GOMAXPROCS,
// lenFiles) for PollRecords (collect), and 1 for PollRecordsIter
// (stream — single-decoder minimum-lookahead, matches the read
// iter family's defaults).
//
// Floored at 1: WithPollDecodeWorkers(0) becomes 1; the
// per-call default is only used when the option is not supplied.
func WithPollDecodeWorkers(n int) PollOption {
	if n < 1 {
		n = 1
	}
	return withPollDecodeWorkersOpt{n: n}
}

type withPollDecodeWorkersOpt struct{ n int }

func (o withPollDecodeWorkersOpt) applyPoll(opts *pollOpts) {
	opts.decodeWorkers = o.n
}

// WithPollDecodeAheadFiles tells each decode worker how many
// decoded files to buffer ahead of the consumer. The worker's
// queue is bounded at this depth; a slow consumer back-pressures
// the worker via the queue's blocking send, which in turn
// back-pressures fetch via the body-slot semaphore.
//
// Defaults to ceil(lenFiles/W) for PollRecords (collect — fast
// workers don't stall) and 1 for PollRecordsIter (minimum
// lookahead, matches the read iter family's defaults).
func WithPollDecodeAheadFiles(n int) PollOption {
	if n < 0 {
		n = 0
	}
	return withPollDecodeAheadFilesOpt{n: n}
}

type withPollDecodeAheadFilesOpt struct{ n int }

func (o withPollDecodeAheadFilesOpt) applyPoll(opts *pollOpts) {
	v := o.n
	opts.decodeAheadFiles = &v
}

// WithPollDecodeAheadBytes caps the uncompressed parquet bytes
// EACH decode worker holds in flight. When a decoded file's
// uncompressed size would push the worker over cap AND the
// worker's buffer is non-empty, the worker blocks until emit
// drains a previous file. The empty-buffer escape lets a single
// oversized file flow even if it exceeds cap.
//
// Zero (the default) disables the byte budget; the queue depth
// (WithPollDecodeAheadFiles) is the only memory bound.
func WithPollDecodeAheadBytes(n int64) PollOption {
	if n < 0 {
		n = 0
	}
	return withPollDecodeAheadBytesOpt{n: n}
}

type withPollDecodeAheadBytesOpt struct{ n int64 }

func (o withPollDecodeAheadBytesOpt) applyPoll(opts *pollOpts) {
	opts.decodeAheadBytes = o.n
}
