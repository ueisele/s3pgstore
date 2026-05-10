package s3pgstore

// poll.go is the read path for the sequenced feed: the public
// entry points (Poll / PollRecords / PollRecordsIter /
// TailRecordsIter / OffsetAt) and the chan-based file-grain
// fetch+decode pipeline (pollFetchAndDecodeIter and the per-file
// state machinery) that backs PollRecords, PollRecordsIter, and
// TailRecordsIter.
//
// File layout mirrors read.go's outside-in shape:
//
//   1. Public methods (Poll, PollRecords, PollRecordsIter,
//      TailRecordsIter, OffsetAt)
//   2. Per-call defaulters (pollIterDefaults, pollCollectDefaults,
//      tailIntervals)
//   3. Emit callbacks (pollIterEmit, pollCollectEmit)
//   4. Pipeline orchestration (pollFetchAndDecodeIter)
//   5. State types (fileState, pollState, pollWorker) + state
//      methods
//   6. Feeders (runOneShotFeeder, runTailFeeder)
//   7. Pipeline stages (runPollFetcher, runPollDecodeWorker)
//   8. Internal types (decodedPollBatch) and helpers
//      (pollFooterUncomp, nextTailBackoff)
//
// Pipeline design diverges from read.go's partition-grain
// readFetchAndDecodeIter:
//
//   - One *fileState per FileRef (no partition grouping). Stream
//     consumers want feed_seq input order, not lex-by-partition
//     order; partition machinery is dead weight here.
//   - No sortAndDedup. The stream feed is intentionally raw —
//     consumers see every observed version in commit order.
//   - decode is per-file; no "wait for all partition files"
//     coordination, no per-partition decoded slice pre-sizing,
//     no in-memory sort.
//   - File set is a stream (chan *fileState), not a slice. A
//     feeder goroutine produces files into the channel and closes
//     it on EOS; the pipeline drains. Three feeders share one
//     pipeline shape: PollRecords/PollRecordsIter use the one-shot
//     feeder (push entries, close); TailRecordsIter uses the tail
//     feeder (loop Poll + sleep, close only on ctx/error). The
//     pipeline is unaware of which feeder it has.
//
// The load-bearing primitives (body-slot semaphore, per-worker
// byte budget, stall observer, race-free batch send) live in
// iter_pipeline_shared.go and are reused verbatim from the read
// pipeline — see that file for the FIFO sendq + WithCancelCause
// rationale.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/parquet-go/parquet-go"

	"github.com/ueisele/s3pgstore/pool"
)

// Tail-feeder backoff defaults. Applied by tailIntervals when the
// caller doesn't supply WithTailIdleBackoff. Base is the wait
// after the first empty poll; the wait doubles per consecutive
// empty poll up to max. Reset to base after any non-empty poll.
//
//   - 100ms base balances "react quickly to new data" against
//     "don't hammer the catalog when truly idle." A consumer at
//     the head of a busy stream sees ~100ms add-on latency.
//   - 2s max bounds DB load on long quiet periods. Five
//     consecutive empty polls is enough to reach the cap, after
//     which polling cadence is one query per 2s — negligible
//     load on any production catalog.
const (
	defaultTailBaseInterval = 100 * time.Millisecond
	defaultTailMaxInterval  = 2 * time.Second
)

// Poll returns the FileRefs in the half-open offset range
// [since, until), ordered by feed_seq ascending. The second
// return is `next` — one past the highest offset seen, suitable
// for passing back as `since` on the next call. On an empty
// result `next` equals the input `since` (never moves
// backwards).
//
// Range conventions:
//   - since: inclusive lower bound. OffsetNone (0) is safe — it's
//     below every live offset (the sequencer assigns from 1).
//   - until: exclusive upper bound. OffsetLatest omits the
//     upper-bound clause from the SQL so the read sees every
//     committed sequenced row at SELECT time.
//
// Match s3pgstore_files.ResolveFileRefsInRange's [since, until)
// convention so users switching between the two ranges think
// the same way about bounds.
//
// Empty range (since == until) returns (nil, since, nil)
// without touching the database. Inverted range (since > until)
// returns an error.
//
// No upper-bound LIMIT — caller bounds the result via
// (until - since). For unbounded drains use PollRecordsIter
// instead, which streams files lazily and bounds memory via
// WithFetchAheadFiles / WithDecodeAheadBytes.
//
// Filtering on the catalog is offset-only; consumers wanting
// per-file filtering (by partition, by extension columns)
// should filter the returned entries client-side and pass the
// surviving entries to ReadFileRefsIter for decoding.
func (s *Store[T]) Poll(
	ctx context.Context, since, until Offset,
) (out []FileRef, next Offset, err error) {
	defer s.metrics.methodScope(ctx, "Poll", &err).end()
	if until != OffsetLatest && since > until {
		return nil, since, fmt.Errorf(
			"Poll: since (%d) > until (%d)", since, until)
	}
	if until != OffsetLatest && since == until {
		return nil, since, nil
	}

	cols := []string{
		"file_id", "feed_seq", "partition_key", "s3_key",
		"written_at_version", "written_at", "file_size",
		"uncompressed_size", "record_count",
	}
	for _, c := range s.resolved.ExtensionColumns {
		cols = append(cols, "ext_"+c.Name)
	}
	args := []any{since}
	where := "feed_seq IS NOT NULL AND feed_seq >= $1"
	if until != OffsetLatest {
		args = append(args, until)
		where += fmt.Sprintf(" AND feed_seq < $%d", len(args))
	}
	q := fmt.Sprintf(
		`SELECT %s FROM %s
		WHERE %s
		ORDER BY feed_seq`,
		strings.Join(cols, ", "), s.names.Files(), where)

	next = since
	err = s.cfg.Executor.Run(ctx, func(d DBTX) error {
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
			extValues := make([]any, len(s.resolved.ExtensionColumns))
			scanArgs := []any{
				&e.FileID, &e.Offset, &e.PartitionKey, &e.S3Key,
				&e.Version, &e.WrittenAt, &e.FileSize,
				&e.UncompressedSize, &e.RecordCount,
			}
			for i := range extValues {
				scanArgs = append(scanArgs, &extValues[i])
			}
			if err := rows.Scan(scanArgs...); err != nil {
				return err
			}
			for i, c := range s.resolved.ExtensionColumns {
				if extValues[i] != nil {
					e.Extensions[c.Name] = extValues[i]
				}
			}
			out = append(out, e)
			if e.Offset+1 > next {
				next = e.Offset + 1
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, since, fmt.Errorf("Poll: %w", err)
	}
	return out, next, nil
}

// PollRecords is Poll + parallel S3 GET + decode. Returns one
// FileResult per file (in feed_seq input order, preserving
// commit-time ordering for stream consumers) plus `next` — the
// offset to pass as `since` on the next call.
//
// No dedup is applied — stream consumers see every observed
// version, in commit order. (Dedup is a per-partition Read-side
// concept; the stream feed is intentionally raw so consumers
// can build their own derived state from the full sequence.)
//
// Backed by the chan-based fetch+decode pipeline shared with
// PollRecordsIter and TailRecordsIter, but auto-tunes for batch
// use: WithDecodeWorkers defaults to min(WorkerPool.MaxConcurrent(),
// GOMAXPROCS, lenFiles) and WithDecodeAheadFiles to
// ceil(lenFiles/W). Caller-supplied options always win.
//
// Caller bounds memory via the (until - since) range. For
// unbounded drains prefer PollRecordsIter, which streams files
// lazily and bounds memory via the iter-pipeline knobs.
//
// Empty range (since == until) returns (nil, since, nil)
// without touching the database.
func (s *Store[T]) PollRecords(
	ctx context.Context, since, until Offset, opts ...PollOption,
) (out []FileResult[T], next Offset, err error) {
	defer s.metrics.methodScope(ctx, "PollRecords", &err).end()

	entries, next, err := s.Poll(ctx, since, until)
	if err != nil {
		return nil, since, err
	}
	if len(entries) == 0 {
		return nil, next, nil
	}

	o := resolvePollOpts(opts)
	var iterErr error
	s.pollFetchAndDecodeIter(ctx, "PollRecords", &o,
		s.pollCollectDefaults(len(entries)),
		s.runOneShotFeeder(entries),
		pollCollectEmit(&out, &iterErr))
	if iterErr != nil {
		return nil, since, iterErr
	}
	return out, next, nil
}

// PollRecordsIter walks the half-open offset range
// [since, until) and yields one FileResult per file in feed_seq
// (commit-time) order. Each yield is the file's catalog row
// (FileRef, including Offset for checkpointing) plus its decoded
// records. No dedup — stream consumers see every observed
// version.
//
// Memory is bounded by the pipeline knobs:
//   - WithFetchAheadFiles caps resident compressed bodies.
//   - WithDecodeAheadFiles caps the per-worker decode queue.
//   - WithDecodeAheadBytes caps decoded uncompressed bytes
//     per worker.
//
// Suitable for bulk replay / drain workloads — the pipelined
// fetch + decode keeps network and CPU saturated while the
// consumer iterates.
//
// `until = OffsetLatest` walks to the live tip captured at iter
// start; `until` is exclusive (offset == until is not yielded).
// Empty range (since == until) yields nothing without touching
// the database. Inverted range (since > until) yields a single
// error.
//
// Resume idiom: track `since = fr.File.Offset + 1` after each
// yield. On failure, the last successfully-yielded FileResult
// gives the checkpoint.
//
// For continuous follow-the-stream consumption (no upper bound,
// runs until ctx cancellation) use TailRecordsIter — it loops
// Poll internally and emits new files as they're committed.
func (s *Store[T]) PollRecordsIter(
	ctx context.Context, since, until Offset, opts ...PollOption,
) iter.Seq2[FileResult[T], error] {
	return func(yield func(FileResult[T], error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "PollRecordsIter", &iterErr).end()

		entries, _, err := s.Poll(ctx, since, until)
		if err != nil {
			iterErr = err
			yield(FileResult[T]{}, err)
			return
		}
		if len(entries) == 0 {
			return
		}

		o := resolvePollOpts(opts)
		s.pollFetchAndDecodeIter(ctx, "PollRecordsIter", &o,
			pollIterDefaults,
			s.runOneShotFeeder(entries),
			s.pollIterEmit(yield, &iterErr))
	}
}

// TailRecordsIter walks the feed forward from `since` and keeps
// going — it never returns on its own. Each yield is one decoded
// FileResult in feed_seq (commit-time) order, exactly like
// PollRecordsIter. When the catalog has no new files, the
// iterator blocks (with exponential backoff between empty polls,
// see WithTailIdleBackoff) until a new file commits or ctx is
// cancelled.
//
// Termination conditions:
//   - ctx cancellation: the iterator returns. ctx cancelled while
//     idle returns silently (no error yield, matches range-over-
//     channel semantics for clean shutdown). ctx cancelled mid-
//     round propagates as an error yield (matches PollRecordsIter).
//   - Poll DB error: yielded as an error and the iterator returns
//     (matches PollRecordsIter — caller wraps in retry logic if
//     resilience is needed).
//   - Pipeline error (S3 GET, decode): yielded as an error and the
//     iterator returns.
//   - Caller breaks the range: iterator returns cleanly; ctx is
//     cancelled internally so all pipeline goroutines drain.
//
// Memory is bounded the same way as PollRecordsIter — by the
// fetchAheadFiles body-slot semaphore and the per-worker decode
// queues. Unlike a slice-based design, the file set is streamed
// through a channel so memory stays O(W × buffer_depth) regardless
// of how many files have been emitted across the lifetime.
//
// Resume idiom: after a clean break or error, restart with
// `since = fr.File.Offset + 1` (where fr is the last successfully-
// yielded FileResult). The internal cursor is not exposed.
//
// Suitable for stream consumers, change-data-capture, and any
// "watch the feed forever" use case. For bounded replay use
// PollRecordsIter; for batch drains use PollRecords.
func (s *Store[T]) TailRecordsIter(
	ctx context.Context, since Offset, opts ...PollOption,
) iter.Seq2[FileResult[T], error] {
	return func(yield func(FileResult[T], error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "TailRecordsIter", &iterErr).end()

		o := resolvePollOpts(opts)
		base, maxInterval := tailIntervals(&o)
		s.pollFetchAndDecodeIter(ctx, "TailRecordsIter", &o,
			pollIterDefaults,
			s.runTailFeeder(since, base, maxInterval),
			s.pollIterEmit(yield, &iterErr))
	}
}

// OffsetAt returns the smallest feed_seq whose feed_seq_at is at
// or after t. Used by stream consumers to seek to a wall-clock
// time without scanning. Returns OffsetNone (0) (and no error)
// when no sequenced row matches — interpretable as "nothing yet
// committed at or after t" by callers.
//
// Sentinel: OffsetNone (0) is never a valid live offset (the
// sequencer assigns starting at 1), so a caller can safely
// treat OffsetNone as "no result" without a separate not-found
// signal.
func (s *Store[T]) OffsetAt(
	ctx context.Context, t time.Time,
) (out Offset, err error) {
	defer s.metrics.methodScope(ctx, "OffsetAt", &err).end()
	q := fmt.Sprintf(
		`SELECT MIN(feed_seq) FROM %s
		WHERE feed_seq IS NOT NULL AND feed_seq_at >= $1`,
		s.names.Files())
	var off *int64
	err = s.cfg.Executor.Run(ctx, func(d DBTX) error {
		row := d.QueryRow(ctx, q, t.UTC())
		return row.Scan(&off)
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return OffsetNone, nil
		}
		return OffsetNone, fmt.Errorf("OffsetAt: %w", err)
	}
	if off == nil {
		return OffsetNone, nil
	}
	return *off, nil
}

// pollIterDefaults fills the iter family's per-call defaults
// (W=1, K=1) into any field the user left unset. Designed for
// streaming consumers — single-decoder, minimum lookahead. Used
// by both PollRecordsIter (bounded) and TailRecordsIter
// (unbounded); both have the same "stream consumer drives the
// pace" shape and don't know lenFiles at pipeline-start time
// (the file set is a stream from the feeder, not a slice), so
// defaults are file-count-independent.
func pollIterDefaults(o *pollOpts) {
	if o.decodeWorkers == 0 {
		o.decodeWorkers = 1
	}
	if o.decodeAheadFiles == nil {
		n := 1
		o.decodeAheadFiles = &n
	}
}

// pollCollectDefaults returns a defaulter closed over the
// known total file count. For the PollRecords (collect) family
// the file count is fixed at call entry, so defaults can be
// auto-tuned: W = min(pool, GOMAXPROCS, lenFiles), K = ceil(
// lenFiles/W). Same logic as Read's readBatchDefaults but per-
// file.
func (s *Store[T]) pollCollectDefaults(lenFiles int) func(*pollOpts) {
	return func(o *pollOpts) {
		if o.decodeWorkers == 0 {
			o.decodeWorkers = min(
				s.resolved.WorkerPool.MaxConcurrent(),
				runtime.GOMAXPROCS(0),
				lenFiles,
			)
		}
		if o.decodeAheadFiles == nil {
			wc := max(o.decodeWorkers, 1)
			n := (lenFiles + wc - 1) / wc
			o.decodeAheadFiles = &n
		}
	}
}

// tailIntervals resolves the tail-feeder's idle-backoff
// boundaries from the option struct. Both base and max default
// when unset; explicit values are used verbatim. Returns the
// resolved (base, max) pair the runTailFeeder uses to compute
// nextTailBackoff. The max return is named maxInterval to
// avoid shadowing the builtin max() at the call site.
func tailIntervals(o *pollOpts) (base, maxInterval time.Duration) {
	base = o.tailBaseInterval
	if base <= 0 {
		base = defaultTailBaseInterval
	}
	maxInterval = o.tailMaxInterval
	if maxInterval <= 0 {
		maxInterval = defaultTailMaxInterval
	}
	if maxInterval < base {
		maxInterval = base
	}
	return base, maxInterval
}

// pollIterEmit returns the per-batch emit callback that yields
// one FileResult[T] per file. Used by PollRecordsIter and
// TailRecordsIter — both surface results via iter.Seq2 yield.
//
// On a hard pipeline error: sets *iterErr, yields a zero
// FileResult with the error, returns false so the emit loop
// terminates.
//
// On a ctx-derived error (caller cancelled or deadline
// expired): suppresses both the yield AND iterErr. Caller-driven
// cancellation is the stop signal for iter mode, not an error
// condition — yielding it would force every range loop to
// filter context errors, and recording it as iterErr would
// taint the methodScope outcome metric for what is normal
// shutdown behavior. The caller can check ctx.Err() themselves
// if they need to distinguish "iter exhausted" from "iter
// cancelled."
func (s *Store[T]) pollIterEmit(
	yield func(FileResult[T], error) bool, iterErr *error,
) func(decodedPollBatch[T]) bool {
	return func(b decodedPollBatch[T]) bool {
		if b.err != nil {
			if errors.Is(b.err, context.Canceled) ||
				errors.Is(b.err, context.DeadlineExceeded) {
				return false
			}
			*iterErr = b.err
			yield(FileResult[T]{}, b.err)
			return false
		}
		return yield(FileResult[T]{
			File:    b.file,
			Records: b.records,
		}, nil)
	}
}

// pollCollectEmit returns the per-batch emit callback that
// appends each FileResult into the *out slice. Used by
// PollRecords (collect). On a hard pipeline error: sets *iterErr
// and returns false to terminate the emit loop.
func pollCollectEmit[T any](
	out *[]FileResult[T], iterErr *error,
) func(decodedPollBatch[T]) bool {
	return func(b decodedPollBatch[T]) bool {
		if b.err != nil {
			*iterErr = b.err
			return false
		}
		*out = append(*out, FileResult[T]{
			File:    b.file,
			Records: b.records,
		})
		return true
	}
}

// pollFetchAndDecodeIter is the chan-based streaming pipeline
// backing PollRecords (collect via callback), PollRecordsIter
// (bounded stream via callback), and TailRecordsIter (unbounded
// stream via callback). Four concurrent stages plus the caller's
// emit loop:
//
//  1. Feeder goroutine: produces *fileState into state.files in
//     fi order. The one-shot feeder pushes pre-resolved entries
//     and closes; the tail feeder loops s.Poll + sleep and closes
//     only on ctx/error. The pipeline doesn't know which it is.
//
//  2. Fetcher goroutine: reads from state.files, acquires one
//     body-pool slot per file, submits one download task per
//     file to the Store's shared *pool.Pool, then fans the
//     *fileState out to the assigned worker's input channel
//     (workers[fi%W].input). Same-pool reentrancy isn't an issue
//     here — pool tasks do GET + close(done) only.
//
//  3. Decode workers (W goroutines): each ranges over its own
//     input channel; for each *fileState waits for the file's
//     download to complete (fs.done), parses the footer for
//     uncompressed bytes, gates on per-worker (decodeAheadFiles,
//     decodeAheadBytes), decodes into T, and sends a
//     decodedPollBatch to its queue. On clean input-channel
//     close, the worker drains and closes its own queue.
//
//  4. Emit loop (this goroutine): walks a running counter,
//     reads from workers[counter%W].queue, hands each batch to
//     emitOne (record-list collect or FileResult yield), and
//     frees the worker's reserved bytes on completion. Exits
//     when the assigned worker's queue closes (clean EOS) or
//     state.ctx fires (error/cancel).
//
// On a hard pipeline error, the failing stage calls
// state.recordHardErr(err) which cancels state.ctx with err as
// cause. All ctx-aware blocking primitives unblock, errored
// workers send a decodedPollBatch{err:err} to their queue, and
// emit forwards the cause via emitOne. Defer cancel(context.
// Canceled) + wg.Wait() on exit drains every goroutine before
// returning.
//
// Shutdown cascade (one-shot feeder, clean EOS):
//
//	feeder closes state.files
//	  → fetcher's range exits, defer closes all workers[w].input
//	    → each worker's range exits, defer closes its own queue
//	      → emit reads ok=false from the assigned worker, returns
//
// Shutdown cascade (ctx cancel mid-round): feeder's send
// select hits ctx.Done and exits via defer; the same chain
// follows. Emit's state.ctx.Done branch fires first if a worker
// is still mid-decode, yielding the cause to the caller.
func (s *Store[T]) pollFetchAndDecodeIter(
	ctx context.Context, method string,
	opts *pollOpts, applyDefaults func(*pollOpts),
	feeder feederFunc,
	emitOne func(decodedPollBatch[T]) bool,
) {
	// Apply per-call defaults — pollIterDefaults for stream
	// consumers (PollRecordsIter, TailRecordsIter; W=1, K=1) or
	// pollCollectDefaults for PollRecords (auto-tuned from
	// pre-known lenFiles). Caller-supplied options always win;
	// the defaulter only fills fields the user left unset.
	applyDefaults(opts)

	// bodyCap bounds resident compressed bodies. Default to
	// pool.MaxConcurrent so a single call saturates the pool's
	// S3-op budget. No floor needed: each file is independent,
	// so no "must hold all of partition's files at once"
	// deadlock risk like the read pipeline has.
	bodyCap := opts.fetchAheadFiles
	if bodyCap <= 0 {
		bodyCap = s.resolved.WorkerPool.MaxConcurrent()
	}

	stateCtx, cancel := context.WithCancelCause(ctx)
	state := &pollState{
		ctx:    stateCtx,
		cancel: cancel,
		// state.files cap = max(W, 1) — small buffer absorbs
		// feeder/fetcher rate mismatches; *fileState is a
		// pointer so memory cost is trivial.
		files: make(chan *fileState, max(opts.decodeWorkers, 1)),
		slots: newBodySlots(bodyCap, s.metrics),
	}

	// Per-worker structures: each owns an input channel
	// (fetcher fans out to workers[fi%W].input) and the shared
	// byte-budget machinery (queue + reserveBytes/releaseBytes).
	//
	// pollWorker.input cap = max(*opts.decodeAheadFiles, 1) —
	// matches the output queue depth; a slow worker
	// backpressures the fetcher symmetrically through both
	// queues.
	workers := make([]*pollWorker[T], opts.decodeWorkers)
	inputCap := max(*opts.decodeAheadFiles, 1)
	for w := range workers {
		workers[w] = &pollWorker[T]{
			input:       make(chan *fileState, inputCap),
			workerState: newWorkerState[decodedPollBatch[T]](*opts.decodeAheadFiles, s.metrics),
		}
	}

	var wg sync.WaitGroup
	defer func() {
		cancel(context.Canceled)
		wg.Wait()
	}()

	// Stage 1: feeder. Produces *fileState into state.files in
	// fi order; closes state.files on EOS or ctx exit (its own
	// defer). The pipeline drains whatever the feeder produces.
	// feederFunc takes only the dependencies it actually needs
	// (ctx, the channel, an err callback) so feeders can't reach
	// into consumer-side state — see feederFunc's doc.
	wg.Go(func() {
		feeder(state.ctx, state.files, state.recordHardErr)
	})

	// Stage 2: fetcher. Reads from state.files, acquires body
	// slots, submits per-file downloads to the shared pool, and
	// fans *fileState out to workers[fi%W].input. Closes all
	// worker inputs on exit (defer) so workers see EOS cleanly.
	g, gctx := s.resolved.WorkerPool.WithContext(state.ctx)
	wg.Go(func() {
		s.runPollFetcher(gctx, g, state, workers)
		_ = g.Wait()
	})

	// Stage 3: decode workers. Each ranges over its own input;
	// drains, decodes, sends to its own queue. Closes its queue
	// on exit (defer).
	for w := range workers {
		wg.Go(func() {
			s.runPollDecodeWorker(state.ctx, state, workers[w], opts)
		})
	}

	// Stall watchdog — pure observer; never cancels. Suppressed
	// when slots.occupancy() == 0 (idle/shutdown), so tail-mode
	// quiet periods don't generate false-positive warnings.
	wg.Go(func() {
		runDeadlockObserver(state.ctx, s.metrics, method, "poll",
			state.slots, "decoder_file", state.decoderFi.Load,
			stallTickInterval, stallThreshold)
	})

	// Stage 4: emit loop. Walks a running counter; for each
	// counter value, reads from workers[counter%W].queue.
	// Termination conditions:
	//   - emitOne returns false: exit. The callback returns
	//     false on caller-stopped-range, hard error forwarded
	//     to the caller, or — for iter-mode emits — ctx-cancel
	//     suppression (callback returns false without yielding).
	//   - assigned worker's queue closes (clean EOS): if
	//     state.ctx has a non-nil cause (recordHardErr fired
	//     OR caller cancelled), forward the cause via emitOne
	//     so collect-mode callbacks (PollRecords) can surface
	//     it as iterErr. Iter-mode callbacks (PollRecordsIter,
	//     TailRecordsIter) decide themselves whether to yield
	//     it to the caller.
	//   - state.ctx fires before the worker queue: same.
	//
	// Suppression of ctx-derived errors lives in the per-mode
	// emit callback (pollIterEmit suppresses; pollCollectEmit
	// surfaces) so the pipeline's "always forward the cause"
	// behavior stays consistent across modes. The pipeline can't
	// suppress universally — PollRecords needs the err to
	// distinguish "completed normally" from "interrupted at
	// offset N" (the returned `next` reflects the Poll
	// boundary, not the emit boundary, so a silent partial
	// return would mislead the caller into skipping unprocessed
	// entries on retry).
	for counter := 0; ; counter++ {
		state.decoderFi.Store(int64(counter))
		ws := workers[counter%opts.decodeWorkers]
		select {
		case batch, ok := <-ws.queue:
			if !ok {
				if err := context.Cause(state.ctx); err != nil {
					emitOne(decodedPollBatch[T]{err: err})
				}
				return
			}
			cont := emitOne(batch)
			ws.releaseBytes(batch.uncompBytes)
			if !cont {
				return
			}
		case <-state.ctx.Done():
			emitOne(decodedPollBatch[T]{err: context.Cause(state.ctx)})
			return
		}
	}
}

// fileState holds per-file download progress. file and fi are
// fixed at feeder-construction time; body is mutated by the pool
// task before close(done) and read by the worker after.
//
// fi is the running file index assigned by the feeder. Drives
// the round-robin worker assignment (fi % W) and the emit
// loop's counter — must be monotonically increasing across the
// pipeline lifetime so emit's per-worker queue reads land in fi
// order.
//
// done is a per-file completion signal. The download task closes
// it after writing body or recording an error; the decode worker
// selects on it (plus state.ctx.Done) so it observes both
// completion and cancellation natively. Closed exactly once per
// file because a file has exactly one downloader.
type fileState struct {
	fi   int
	file FileRef
	body []byte
	done chan struct{}
}

// pollState coordinates the feeder, fetcher, decode workers, and
// emit loop. ctx + cancel mirror read.go's readState — single
// cancellation source via WithCancelCause; recordHardErr cancels
// with the abort reason as cause atomically with ctx.Done close.
//
// files is the feeder→fetcher channel. The feeder produces
// *fileState in fi order and closes the channel on EOS; the
// fetcher drains it. Cap is small (max(W,1)); *fileState is a
// pointer so the channel itself is cheap.
//
// slots is the body-slot semaphore — see iter_pipeline_shared.go
// for FIFO sendq + watchdog progress-bump rationale.
type pollState struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	files chan *fileState
	slots *bodySlots

	// decoderFi: emit's current counter — atomic so the stall
	// observer can read it without locking. Logged in slog
	// alongside the stall warn for context. Unbounded across
	// the pipeline lifetime (suitable for tail mode where the
	// emit counter grows without limit).
	decoderFi atomic.Int64
}

// pollWorker bundles the per-decode-worker resources: an input
// channel of *fileState (fed by the fetcher's fi%W fan-out) and
// the shared byte-budget machinery (queue + reserveBytes /
// releaseBytes via the embedded *workerState).
//
// The input channel is poll-specific (read pipeline doesn't
// fan out by file index) so it lives on this poll-local struct
// rather than on the shared workerState[B]. Embedding
// *workerState keeps the byte-budget call sites unchanged.
type pollWorker[T any] struct {
	input chan *fileState
	*workerState[decodedPollBatch[T]]
}

// feederFunc is the contract pollFetchAndDecodeIter requires
// from any file-set producer. Both runOneShotFeeder (PollRecords,
// PollRecordsIter) and runTailFeeder (TailRecordsIter) satisfy
// it.
//
// Parameters carry exactly what a feeder needs — no access to
// the rest of pollState (slots, workers, decoderFi are consumer
// concerns):
//
//   - ctx: shutdown signal. Feeder must select on ctx.Done in
//     every blocking operation and return when it fires.
//   - files: the channel to produce into. Feeder must close it
//     on exit (defer close(files) at the top of the function).
//     Pipeline drains whatever the feeder produced before close.
//   - recordErr: callback for unrecoverable errors (e.g., a Poll
//     DB failure mid-tail). Setting an err here cancels the
//     pipeline's ctx with the err as cause; emit forwards it to
//     the caller. Feeders MUST NOT call this for ctx-derived
//     errors (Canceled, DeadlineExceeded) — those are normal
//     shutdown, not pipeline failures, and routing them through
//     here would taint the outcome metric.
type feederFunc func(
	ctx context.Context,
	files chan<- *fileState,
	recordErr func(error),
)

// recordHardErr cancels state.ctx with err as the cause. First
// call wins; subsequent calls are no-ops (the pool's errgroup
// preserves first-error semantics). Any goroutine reading
// context.Cause(state.ctx) after a select on state.ctx.Done()
// is guaranteed to see the cause set atomically with the close.
func (state *pollState) recordHardErr(err error) {
	state.cancel(err)
}

// waitForFile blocks until fs's download has completed (done
// closed) or ctx is cancelled. Returns nil on completion AND ctx
// still alive, context.Cause(ctx) otherwise — so the "download
// recorded a hard err and closed done" case (which always
// cancels state.ctx via recordHardErr first) is folded into the
// same return value the worker forwards.
func waitForFile(ctx context.Context, fs *fileState) error {
	return waitOrCancel(ctx, fs.done)
}

// runOneShotFeeder returns a feeder that pushes the supplied
// entries into the files channel in input order then closes it.
// Used by PollRecords and PollRecordsIter — the file set is
// bounded and known at call entry. fi is assigned 0..N-1 in
// input order so the round-robin (fi%W) worker assignment
// matches the input slice's order.
//
// On ctx cancel mid-push, the select returns and the deferred
// close still fires — fetcher and workers see EOS cleanly.
//
// recordErr is unused — one-shot has no DB or Poll dependency
// that could fail asynchronously. The parameter exists to
// satisfy feederFunc; the orchestrator threads it through
// uniformly across both feeders.
func (s *Store[T]) runOneShotFeeder(entries []FileRef) feederFunc {
	return func(
		ctx context.Context,
		files chan<- *fileState,
		_ func(error),
	) {
		defer close(files)
		for i, e := range entries {
			fs := &fileState{
				fi:   i,
				file: e,
				done: make(chan struct{}),
			}
			select {
			case files <- fs:
			case <-ctx.Done():
				return
			}
		}
	}
}

// runTailFeeder returns a feeder that loops s.Poll forward from
// `since`, pushing newly-discovered files into the channel. On
// an empty poll, sleeps with exponential backoff (base..max,
// doubling per consecutive empty poll, reset to base on any
// non-empty poll). On a Poll DB error, recordErr cancels the
// pipeline's ctx with the err as cause and the feeder exits —
// the deferred close still fires, so fetcher/workers/emit
// cascade-shutdown cleanly via the ctx cancel + the EOS signal.
//
// Never returns of its own accord — only via ctx cancel or hard
// error. fi is a running counter assigned across all pushed
// files (not reset between polls), so worker assignment stays
// consistent across the pipeline's lifetime.
//
// cursor advances to e.Offset+1 after each push so resume after
// any partial-progress exit is gap-free (the caller sees the
// same offset they yielded last + 1; matches PollRecordsIter's
// resume idiom).
func (s *Store[T]) runTailFeeder(
	since Offset, base, maxInterval time.Duration,
) feederFunc {
	return func(
		ctx context.Context,
		files chan<- *fileState,
		recordErr func(error),
	) {
		defer close(files)
		cursor := since
		fi := 0
		consecEmpty := 0
		for {
			entries, _, err := s.Poll(ctx, cursor, OffsetLatest)
			if err != nil {
				// Don't recordErr on ctx-derived errors —
				// they're the expected shutdown path. Cancel
				// (caller stopped) and DeadlineExceeded
				// (caller's WithTimeout fired) both belong here;
				// recording them as a hard cause would taint
				// the outcome metric and surface as a noisy
				// yield via emit. Real DB errors (connection
				// loss, query failure) fall through to recordErr
				// and emit cleanly.
				if !errors.Is(err, context.Canceled) &&
					!errors.Is(err, context.DeadlineExceeded) {
					recordErr(err)
				}
				return
			}
			if len(entries) == 0 {
				consecEmpty++
				wait := nextTailBackoff(consecEmpty, base, maxInterval)
				select {
				case <-ctx.Done():
					return
				case <-time.After(wait):
				}
				continue
			}
			consecEmpty = 0
			for _, e := range entries {
				fs := &fileState{
					fi:   fi,
					file: e,
					done: make(chan struct{}),
				}
				select {
				case files <- fs:
					fi++
					cursor = e.Offset + 1
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// runPollFetcher reads *fileState from state.files in fi order,
// acquires a body slot per file, submits one download task per
// file to the shared pool, then hands fs to its assigned worker
// via workers[fi%W].input. Body-slot acquire happens on the
// fetcher (not the pool worker) — see CLAUDE.md "Shared-pool
// workers must never block on per-call coordination."
//
// On clean EOS (state.files closed), the deferred loop closes
// every worker's input so workers see EOS and shut down. On
// ctx cancel mid-loop, returns immediately; the deferred close
// still fires.
//
// Download task: GET → set fs.body, close(fs.done), bump
// progress. On GET failure: release slot, recordHardErr (cancels
// state.ctx with the wrapped err as cause), close(fs.done).
// Order matters — cause is set before done closes so the
// decoder's waitForFile observes both atomically.
func (s *Store[T]) runPollFetcher(
	ctx context.Context, g *pool.Group, state *pollState,
	workers []*pollWorker[T],
) {
	defer func() {
		for _, w := range workers {
			close(w.input)
		}
	}()
	for {
		var fs *fileState
		select {
		case f, ok := <-state.files:
			if !ok {
				return
			}
			fs = f
		case <-ctx.Done():
			return
		}
		if state.slots.acquire(ctx) != nil {
			return
		}
		key := fs.file.S3Key
		g.Submit(ctx, func(ctx context.Context) error {
			body, err := s.target.get(ctx, key)
			if err != nil {
				state.slots.release()
				wrapped := fmt.Errorf("get %s: %w", key, err)
				state.recordHardErr(wrapped)
				close(fs.done)
				return wrapped
			}
			// Slot stays held; decoder releases after nil-out.
			fs.body = body
			close(fs.done)
			state.slots.bumpProgress()
			return nil
		})
		select {
		case workers[fs.fi%len(workers)].input <- fs:
		case <-ctx.Done():
			return
		}
	}
}

// runPollDecodeWorker drains its own input channel in fi order
// (the fetcher pushes round-robin so the worker sees only its
// own assignments). For each *fileState: waits for the download,
// parses the footer for uncompressed bytes, reserves byte
// budget, decodes, and publishes the result to ws.queue. On
// input-channel close (clean EOS) OR ctx cancel, the deferred
// close on ws.queue propagates EOS to emit.
//
// The outer loop is a select on ctx.Done + input rather than
// `for fs := range w.input` so the worker observes ctx
// natively, per CLAUDE.md "Every blocking primitive must
// observe ctx.Done() natively." Ranging over the channel would
// require the fetcher's deferred input-close to act as a
// cancel-broadcast helper — the antipattern the invariant
// calls out. Files in flight when ctx fires are discarded
// cleanly: the fetcher's defer still closes worker inputs (no
// panic on second close because only the fetcher closes), and
// any *fileState left in the input buffer is unreferenced and
// GC-eligible.
func (s *Store[T]) runPollDecodeWorker(
	ctx context.Context, state *pollState,
	w *pollWorker[T], opts *pollOpts,
) {
	defer close(w.queue)
	for {
		var fs *fileState
		select {
		case f, ok := <-w.input:
			if !ok {
				return
			}
			fs = f
		case <-ctx.Done():
			return
		}
		if err := waitForFile(ctx, fs); err != nil {
			sendBatch(ctx, w.queue, decodedPollBatch[T]{err: err})
			return
		}

		// Parse footer once: exact uncompressed bytes for the
		// byte budget. Per-file, no row-count pre-allocation
		// (decodeParquet allocates as needed for one file).
		uncomp, err := pollFooterUncomp(fs.body)
		if err != nil {
			state.slots.release()
			fs.body = nil
			sendBatch(ctx, w.queue, decodedPollBatch[T]{
				err: fmt.Errorf("footer %s: %w", fs.file.S3Key, err),
			})
			return
		}

		// Gate on this worker's byte budget if configured.
		if err := w.reserveBytes(ctx, uncomp, opts.decodeAheadBytes); err != nil {
			state.slots.release()
			fs.body = nil
			sendBatch(ctx, w.queue, decodedPollBatch[T]{err: err})
			return
		}

		decodeStart := time.Now()
		recs, err := decodeParquet[T](fs.body)
		s.metrics.recordIterDecodeDuration(ctx, time.Since(decodeStart))
		// Free the body and slot regardless of decode outcome.
		fs.body = nil
		state.slots.release()
		if err != nil {
			w.releaseBytes(uncomp)
			sendBatch(ctx, w.queue, decodedPollBatch[T]{
				err: fmt.Errorf("decode %s: %w", fs.file.S3Key, err),
			})
			return
		}

		if !sendBatch(ctx, w.queue, decodedPollBatch[T]{
			file:        fs.file,
			records:     recs,
			uncompBytes: uncomp,
		}) {
			w.releaseBytes(uncomp)
			return
		}
	}
}

// decodedPollBatch is one file's decoded records (or a single
// hard error) flowing from the decoder to the emit loop. file
// is the catalog row (carried so emit can populate
// FileResult.File); records is the parquet body decoded into T;
// uncompBytes is what the decoder reserved (emit returns it via
// releaseBytes after forwarding).
type decodedPollBatch[T any] struct {
	file        FileRef
	records     []T
	uncompBytes int64
	err         error
}

// pollFooterUncomp parses the parquet footer and returns the
// total uncompressed bytes across every row group. Same
// computation as read.go's footerStats but for a single file
// (and skipping the row-count return — decodeParquet allocates
// per-file as needed).
func pollFooterUncomp(body []byte) (int64, error) {
	pf, err := parquet.OpenFile(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return 0, err
	}
	var uncomp int64
	for _, rg := range pf.Metadata().RowGroups {
		uncomp += rg.TotalByteSize
	}
	return uncomp, nil
}

// nextTailBackoff returns the wait duration after the n-th
// consecutive empty poll. n=1 returns base; each subsequent
// empty poll doubles the wait, capped at max. Resets to base
// after any non-empty poll (caller passes n=0 then increments).
//
// Caps the shift at 30 to avoid int64 overflow at extreme n —
// far past the max cap on any realistic base/max pair, so the
// behavior change at n=30 is invisible.
func nextTailBackoff(n int, base, maxInterval time.Duration) time.Duration {
	if n <= 1 {
		return base
	}
	if n > 30 {
		return maxInterval
	}
	d := base << (n - 1)
	if d > maxInterval || d < 0 {
		return maxInterval
	}
	return d
}
