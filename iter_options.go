package s3pgstore

// iter_options.go holds option functions accepted by both the
// read and poll iter pipelines. Each returns an IterOption — a
// value that satisfies both ReadOption and PollOption — so the
// same With* call works for either family. The type system
// catches misuse: passing a Read-only option (WithHistory,
// WithDecodeAheadPartitions) to PollRecords is a compile error,
// and passing a Poll-only option (WithDecodeAheadFiles) to
// ReadIter is a compile error.
//
// readOpts and pollOpts both store these knobs under identical
// field names (decodeWorkers, fetchAheadFiles, decodeAheadBytes),
// so each shared option type carries one apply method per family
// with byte-identical bodies that just touch the relevant struct.

// IterOption is accepted by both Read* and Poll* methods. It is
// a value satisfying both ReadOption and PollOption. Use the
// With* functions in this file to construct values; the
// underlying types are unexported.
type IterOption interface {
	ReadOption
	PollOption
}

// WithDecodeWorkers sets the number of parallel decoder
// goroutines for the iter pipeline.
//
// Defaults differ per entry point:
//   - ReadIter / ReadPartitionIter / ReadRangeIter /
//     ReadFileRefsIter / PollRecordsIter: 1 (single decoder,
//     deterministic CPU footprint).
//   - Read / ReadPartition: auto-tuned to
//     min(WorkerPool.MaxConcurrent(), GOMAXPROCS, lenParts).
//   - PollRecords: auto-tuned to
//     min(WorkerPool.MaxConcurrent(), GOMAXPROCS, lenFiles).
//
// n > 1 fans out decode across n goroutines. Workers self-assign
// work units round-robin (worker w handles units where i % n == w).
// Emit drains worker queues in input order so the deterministic-
// emission contract is preserved.
//
// Use on the iter family for batch-analytics workloads where
// decode dominates per-call wall-time. Streaming workloads with
// small records and a slow consumer see no benefit — decode time
// is already small relative to consumer yield.
//
// Memory implications: WithDecodeAheadPartitions /
// WithDecodeAheadFiles and WithDecodeAheadBytes are PER WORKER —
// total in-flight decoded memory is multiplied by n. Account for
// this when tuning.
//
// Floored at 1: WithDecodeWorkers(0) becomes 1; the per-call
// default is only used when the option is not supplied.
func WithDecodeWorkers(n int) IterOption {
	if n < 1 {
		n = 1
	}
	return decodeWorkersOpt{n: n}
}

type decodeWorkersOpt struct{ n int }

func (o decodeWorkersOpt) applyRead(opts *readOpts) { opts.decodeWorkers = o.n }
func (o decodeWorkersOpt) applyPoll(opts *pollOpts) { opts.decodeWorkers = o.n }

// WithFetchAheadFiles caps the number of compressed parquet
// bodies the fetcher may keep resident at once (downloads
// in-flight + landed-but-not-yet-decoded). Bounds per-call
// resident compressed-body memory to roughly n * avg_file_size.
//
// Default (option not supplied or n <= 0) is
// Config.WorkerPool.MaxConcurrent() — a single call can
// saturate the pool's S3-op budget. Lower values reduce per-call
// body memory at the cost of S3 concurrency: the fetcher cannot
// dispatch beyond this cap into the pool, so pool slots above n
// sit idle for this call. Use in shared-pool deployments where
// each concurrent reader independently consuming the full budget
// inflates aggregate resident body memory beyond what the
// operator wants.
//
// Read pipeline only: internally floored to
// max(filesPerPartition) so a single oversized partition still
// flows in full — without that floor the fetcher would block on
// the cap with the decoder waiting on the partition's remaining
// files. Poll has no floor; each file is independent.
func WithFetchAheadFiles(n int) IterOption {
	if n < 0 {
		n = 0
	}
	return fetchAheadFilesOpt{n: n}
}

type fetchAheadFilesOpt struct{ n int }

func (o fetchAheadFilesOpt) applyRead(opts *readOpts) { opts.fetchAheadFiles = o.n }
func (o fetchAheadFilesOpt) applyPoll(opts *pollOpts) { opts.fetchAheadFiles = o.n }

// WithDecodeAheadBytes caps the cumulative uncompressed parquet
// bytes EACH decode worker may hold (currently-decoding + queued
// for emit). Zero (default) disables the cap; only the queue-depth
// option (WithDecodeAheadPartitions for read, WithDecodeAheadFiles
// for poll) binds.
//
// Composes with the queue-depth option — both are evaluated and
// whichever cap binds first holds the worker back. Useful when
// work-unit sizes are skewed: a tiny queue depth is too
// conservative for many small units but a larger value risks OOM
// on a few large ones; a byte cap auto-tunes across both.
//
// The byte total is read from each parquet file's footer
// (total_byte_size summed across row groups), so the cap is
// exact, not a heuristic. Decoded Go memory typically runs 1-2x
// the uncompressed size depending on data shape.
//
// Per-unit guarantee: if a single work unit's uncompressed size
// exceeds the cap, that unit still decodes (the cap can't be
// enforced below file granularity without row-group-level
// streaming).
//
// Per-worker semantics: with WithDecodeWorkers(W), the per-call
// total of decoded uncompressed bytes is W * n.
func WithDecodeAheadBytes(n int64) IterOption {
	if n < 0 {
		n = 0
	}
	return decodeAheadBytesOpt{n: n}
}

type decodeAheadBytesOpt struct{ n int64 }

func (o decodeAheadBytesOpt) applyRead(opts *readOpts) { opts.decodeAheadBytes = o.n }
func (o decodeAheadBytesOpt) applyPoll(opts *pollOpts) { opts.decodeAheadBytes = o.n }
