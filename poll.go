package s3pgstore

// poll.go is the read path for the sequenced feed: the public
// entry points (Poll / PollRecords / PollRecordsIter / OffsetAt)
// and the chan-based file-grain fetch+decode pipeline
// (pollFetchAndDecodeIter and the per-file state machinery) that
// backs PollRecords and PollRecordsIter.
//
// File layout mirrors read.go's outside-in shape:
//
//   1. Public methods (Poll, PollRecords, PollRecordsIter, OffsetAt)
//   2. Per-call defaulters (pollIterDefaults, pollCollectDefaults)
//   3. Emit callbacks (pollIterEmit, pollCollectEmit)
//   4. Pipeline orchestration (pollFetchAndDecodeIter)
//   5. State types (fileState, pollState) + state methods
//   6. Pipeline stages (runPollFetcher, runPollDecodeWorker)
//   7. Internal types (decodedPollBatch) and helpers (pollFooterUncomp)
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
// PollRecordsIter, but auto-tunes for batch use: WithDecode
// Workers defaults to min(WorkerPool.MaxConcurrent(),
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
	s.pollFetchAndDecodeIter(ctx, "PollRecords", entries, &o,
		s.pollCollectDefaults, pollCollectEmit(&out, &iterErr))
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
		s.pollFetchAndDecodeIter(ctx, "PollRecordsIter", entries, &o,
			pollIterDefaults, s.pollIterEmit(yield, &iterErr))
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
// streaming consumers — single-decoder, minimum lookahead.
func pollIterDefaults(o *pollOpts, _ int) {
	if o.decodeWorkers == 0 {
		o.decodeWorkers = 1
	}
	if o.decodeAheadFiles == nil {
		n := 1
		o.decodeAheadFiles = &n
	}
}

// pollCollectDefaults fills the PollRecords (collect) family's
// auto-tuned defaults: W = min(pool, GOMAXPROCS, lenFiles),
// K = ceil(lenFiles/W). Same logic as Read's readBatchDefaults
// but per-file.
func (s *Store[T]) pollCollectDefaults(o *pollOpts, lenFiles int) {
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

// pollIterEmit returns the per-batch emit callback that yields
// one FileResult[T] per file. On a hard pipeline error: sets
// *iterErr, yields a zero FileResult with the error, returns
// false so the emit loop terminates.
func (s *Store[T]) pollIterEmit(
	yield func(FileResult[T], error) bool, iterErr *error,
) func(decodedPollBatch[T]) bool {
	return func(b decodedPollBatch[T]) bool {
		if b.err != nil {
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
// backing PollRecords (collect via callback) and PollRecordsIter
// (stream via callback). Three concurrent stages plus the
// caller's emit loop:
//
//  1. Fetcher goroutine: walks files in input order, acquires
//     one body-pool slot per file, submits one download task
//     per file to the Store's shared *pool.Pool. Same-pool
//     reentrancy isn't an issue here — pool tasks do GET +
//     close(done) only.
//
//  2. Decode workers (W goroutines): each handles files where
//     fileIdx % W == workerIdx; waits for the file's download
//     to complete, parses the footer for uncompressed bytes,
//     gates on per-worker (decodeAheadFiles, decodeAheadBytes),
//     decodes into T, and sends a decodedPollBatch to its queue.
//
//  3. Emit loop (this goroutine): pulls decoded files in input
//     order from worker[fi % W].queue, hands each to emitOne
//     (record-list collect or FileResult yield), and frees the
//     worker's reserved bytes on completion so the worker can
//     proceed.
//
// On a hard pipeline error, the decoder sends
// decodedPollBatch{err:err} and the emit callback receives it —
// it should yield/record the error and return false. On success,
// emit returns true to keep going.
func (s *Store[T]) pollFetchAndDecodeIter(
	ctx context.Context, method string, entries []FileRef,
	opts *pollOpts, applyDefaults func(*pollOpts, int),
	emitOne func(decodedPollBatch[T]) bool,
) {
	if len(entries) == 0 {
		return
	}
	files := make([]*fileState, len(entries))
	for i, e := range entries {
		files[i] = &fileState{file: e, done: make(chan struct{})}
	}

	// Apply per-call defaults — pollIterDefaults for the iter
	// path (W=1, K=1) or pollCollectDefaults for PollRecords
	// (auto-tuned). Caller-supplied options always win; the
	// defaulter only fills fields the user left unset.
	applyDefaults(opts, len(files))

	// Universal clamp: surplus workers (W > len(files)) waste
	// allocations; clamp here so caller-supplied W > len(files)
	// is also handled.
	if opts.decodeWorkers > len(files) {
		opts.decodeWorkers = len(files)
	}

	// bodyCap bounds resident compressed bodies. Default to
	// pool.MaxConcurrent so a single call saturates the pool's
	// S3-op budget.
	bodyCap := opts.fetchAheadFiles
	if bodyCap <= 0 {
		bodyCap = s.resolved.WorkerPool.MaxConcurrent()
	}
	// No floor needed: each file is independent, so no
	// "must hold all of partition's files at once" deadlock
	// risk like the read pipeline has.

	stateCtx, cancel := context.WithCancelCause(ctx)
	state := &pollState{
		ctx:    stateCtx,
		cancel: cancel,
		files:  files,
		slots:  newBodySlots(bodyCap, s.metrics),
	}

	var wg sync.WaitGroup
	defer func() {
		cancel(context.Canceled)
		wg.Wait()
	}()

	// Stage 1: fetcher. Acquires body slots and submits per-file
	// download tasks to the shared pool. g.Wait drains in-flight
	// tasks before this goroutine exits.
	g, gctx := s.resolved.WorkerPool.WithContext(state.ctx)
	wg.Go(func() {
		s.runPollFetcher(gctx, g, state)
		_ = g.Wait()
	})

	// Stage 2: decode workers. Each handles files where
	// fi % decodeWorkers == workerIdx and writes results to its
	// own queue (cap = decodeAheadFiles).
	workers := make([]*workerState[decodedPollBatch[T]], opts.decodeWorkers)
	for w := range workers {
		workers[w] = newWorkerState[decodedPollBatch[T]](*opts.decodeAheadFiles, s.metrics)
	}
	for w := range workers {
		wg.Go(func() {
			s.runPollDecodeWorker(state.ctx, state, workers[w],
				w, opts.decodeWorkers, opts)
		})
	}

	// Stall watchdog — pure observer; never cancels.
	wg.Go(func() {
		runDeadlockObserver(state.ctx, s.metrics, method, "poll",
			state.slots, "decoder_file", state.decoderFi.Load,
			stallTickInterval, stallThreshold)
	})

	// Stage 3: emit loop. Walks files in input order, reads
	// from each file's assigned worker's queue, hands the batch
	// to the per-method emit callback, then releases that
	// worker's byte budget so it can claim the next file.
	for fi := range state.files {
		state.decoderFi.Store(int64(fi))
		ws := workers[fi%opts.decodeWorkers]
		var batch decodedPollBatch[T]
		select {
		case batch = <-ws.queue:
		case <-state.ctx.Done():
			emitOne(decodedPollBatch[T]{err: context.Cause(state.ctx)})
			return
		}
		ok := emitOne(batch)
		ws.releaseBytes(batch.uncompBytes)
		if !ok {
			return
		}
	}
}

// fileState holds per-file download progress. file is fixed at
// construction; body and err are mutated by the pool task under
// pollState.mu.
//
// done is a per-file completion signal. The download task closes
// it after writing body or recording an error; the decode worker
// selects on it (plus state.ctx.Done) so it observes both
// completion and cancellation natively. Closed exactly once per
// file because a file has exactly one downloader.
type fileState struct {
	file FileRef
	body []byte
	done chan struct{}
}

// pollState coordinates the fetcher, decode workers, and emit
// loop. ctx + cancel mirror read.go's readState — single
// cancellation source via WithCancelCause; recordHardErr cancels
// with the abort reason as cause atomically with ctx.Done close.
//
// slots is the body-slot semaphore — see iter_pipeline_shared.go
// for FIFO sendq + watchdog progress-bump rationale.
type pollState struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	files []*fileState
	slots *bodySlots

	// decoderFi: emit's current file index — atomic so the stall
	// observer can read it without locking. Logged in slog
	// alongside the stall warn for context.
	decoderFi atomic.Int64
}

// recordHardErr cancels state.ctx with err as the cause. First
// call wins; subsequent calls are no-ops (the pool's errgroup
// preserves first-error semantics). Any goroutine reading
// context.Cause(state.ctx) after a select on state.ctx.Done()
// is guaranteed to see the cause set atomically with the close.
func (state *pollState) recordHardErr(err error) {
	state.cancel(err)
}

// waitForFile blocks until file fi's download has completed
// (done closed) or ctx is cancelled. Returns nil on completion
// AND ctx still alive, context.Cause(ctx) otherwise — so the
// "download recorded a hard err and closed done" case (which
// always cancels state.ctx via recordHardErr first) is folded
// into the same return value the worker forwards.
func (state *pollState) waitForFile(ctx context.Context, fi int) error {
	return waitOrCancel(ctx, state.files[fi].done)
}

// runPollFetcher walks files in input order and submits one
// download task per file to the shared pool. Body-slot acquire
// happens here (fetcher-side back-pressure) so pool workers
// never block on per-call coordination — see CLAUDE.md.
//
// On download success the task sets fs.body, closes fs.done,
// and bumps the body-slot watchdog timestamp (the watchdog's
// progress signal). On download failure: release the slot,
// recordHardErr (cancels state.ctx with the wrapped err as
// cause), then close(fs.done) — order matters: cause is set
// before done closes so the decoder's waitForFile observes
// both atomically.
func (s *Store[T]) runPollFetcher(
	ctx context.Context, g *pool.Group, state *pollState,
) {
	for fi := range state.files {
		if state.slots.acquire(ctx) != nil {
			return
		}
		fs := state.files[fi]
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
	}
}

// runPollDecodeWorker handles files assigned to worker w via
// round-robin: fi where fi % numWorkers == w. Each iteration
// waits for the download, parses the footer for uncompressed
// bytes, reserves byte budget, decodes, and publishes the
// result to ws.queue. Emit drains ws.queue in input order.
func (s *Store[T]) runPollDecodeWorker(
	ctx context.Context, state *pollState,
	ws *workerState[decodedPollBatch[T]], workerIdx, numWorkers int,
	opts *pollOpts,
) {
	for fi := workerIdx; fi < len(state.files); fi += numWorkers {
		if err := state.waitForFile(ctx, fi); err != nil {
			sendBatch(ctx, ws.queue, decodedPollBatch[T]{err: err})
			return
		}
		fs := state.files[fi]

		// Parse footer once: exact uncompressed bytes for the
		// byte budget. Per-file, no row-count pre-allocation
		// (decodeParquet allocates as needed for one file).
		uncomp, err := pollFooterUncomp(fs.body)
		if err != nil {
			state.slots.release()
			fs.body = nil
			sendBatch(ctx, ws.queue, decodedPollBatch[T]{
				err: fmt.Errorf("footer %s: %w", fs.file.S3Key, err),
			})
			return
		}

		// Gate on this worker's byte budget if configured.
		if err := ws.reserveBytes(ctx, uncomp, opts.decodeAheadBytes); err != nil {
			state.slots.release()
			fs.body = nil
			sendBatch(ctx, ws.queue, decodedPollBatch[T]{err: err})
			return
		}

		decodeStart := time.Now()
		recs, err := decodeParquet[T](fs.body)
		s.metrics.recordIterDecodeDuration(ctx, time.Since(decodeStart))
		// Free the body and slot regardless of decode outcome.
		fs.body = nil
		state.slots.release()
		if err != nil {
			ws.releaseBytes(uncomp)
			sendBatch(ctx, ws.queue, decodedPollBatch[T]{
				err: fmt.Errorf("decode %s: %w", fs.file.S3Key, err),
			})
			return
		}

		if !sendBatch(ctx, ws.queue, decodedPollBatch[T]{
			file:        fs.file,
			records:     recs,
			uncompBytes: uncomp,
		}) {
			ws.releaseBytes(uncomp)
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
