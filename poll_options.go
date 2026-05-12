package s3pgstore

import "time"

// PollOption is the interface implemented by PollRecords /
// PollRecordsIter / TailRecordsIter modifiers. Distinct from
// ReadOption so the surfaces don't cross-contaminate at the call
// site (a Poll-specific WithDecodeAheadFiles passed to ReadIter
// is a compile error). Knobs that work for both pipelines live
// in iter_options.go and return IterOption (which satisfies both
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

	// tailBaseInterval is the wait after the first empty poll
	// in TailRecordsIter. Subsequent consecutive empty polls
	// double the wait up to tailMaxInterval. Reset to base on
	// any non-empty poll. Zero (default) resolves to
	// defaultTailBaseInterval (100ms). Tail-only — has no
	// effect on PollRecords or PollRecordsIter.
	tailBaseInterval time.Duration

	// tailMaxInterval caps the exponential backoff between
	// empty polls in TailRecordsIter. Zero (default) resolves
	// to defaultTailMaxInterval (2s). When max < base, max is
	// promoted to base by tailIntervals (no degenerate
	// shrinking-cap configuration). Tail-only.
	tailMaxInterval time.Duration

	// pollPageSize is the per-page row count used by PollIter
	// and TailIter for paged SELECTs. Zero or negative
	// (default) resolves to defaultPollPageSize (10000). Each
	// page is a self-contained Poll call against the catalog;
	// smaller pages reduce first-row latency and per-iter
	// memory but increase round-trip count. Larger pages
	// amortise per-query overhead but delay the first emit and
	// inflate the producer's materialisation buffer. Has no
	// effect on Poll itself (single-shot) or on PollRecords
	// (which materialises before invoking the pipeline).
	pollPageSize int
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

// WithTailIdleBackoff bounds TailRecordsIter's wait between
// empty polls. base is the wait after the first empty poll;
// each subsequent consecutive empty poll doubles the wait up
// to max. Reset to base on any non-empty poll.
//
// Defaults (option not supplied): base=100ms, max=2s. The
// defaults balance "react quickly to new data" against "don't
// hammer the catalog when truly idle" — at the cap the
// catalog sees one query per 2s per tail consumer.
//
// Non-positive values are treated as "use default" (per
// dimension); max < base is promoted to max=base so the cap
// is never below the floor. Effective range:
// [max(base, 1ns), max(max, base)].
//
// Tail-only — has no effect on PollRecords or PollRecordsIter
// (which return on EOS rather than wait for new data).
func WithTailIdleBackoff(base, max time.Duration) PollOption {
	return withTailIdleBackoffOpt{base: base, max: max}
}

type withTailIdleBackoffOpt struct{ base, max time.Duration }

func (o withTailIdleBackoffOpt) applyPoll(opts *pollOpts) {
	opts.tailBaseInterval = o.base
	opts.tailMaxInterval = o.max
}

// WithPollPageSize sets the per-page row count used by PollIter
// and TailIter for paged SELECTs against the catalog. n <= 0
// resolves to the default (10000).
//
// Each page is a self-contained Poll call. Smaller pages
// (1K-5K) reduce first-row latency and per-iter memory for
// backfill-style consumers, at the cost of more SQL round
// trips. Larger pages (50K+) amortise per-query overhead and
// reduce round-trip count, at the cost of higher first-row
// latency and larger producer materialisation buffer per
// iter call.
//
// Has no effect on Poll (single-shot, no paging) or on
// PollRecords (which materialises the full range before
// invoking the pipeline). Tail-mode steady-state cycles are
// typically much smaller than any reasonable page size, so the
// option mainly affects first-poll behavior (tail startup catch-
// up) and PollIter / PollRecordsIter on bounded ranges.
func WithPollPageSize(n int) PollOption {
	return withPollPageSizeOpt{n: n}
}

type withPollPageSizeOpt struct{ n int }

func (o withPollPageSizeOpt) applyPoll(opts *pollOpts) {
	opts.pollPageSize = o.n
}
