package s3pgstore

// poll_iter.go is the file-grain fetch+decode pipeline backing
// PollRecords and PollRecordsIter. Its design diverges from
// read.go's partition-grain fetchAndDecodeIter:
//
//   - One *fileState per FileRef (no partition grouping). Stream
//     consumers want feed_seq input order, not lex-by-partition
//     order; partition machinery is dead weight here.
//   - No sortAndDedup. The stream feed is intentionally raw —
//     consumers see every observed version in commit order.
//   - decodePollFile is per-file; no "wait for all partition
//     files" coordination, no per-partition decoded slice
//     pre-sizing, no in-memory sort.
//
// The shape that DOES carry over from read.go (because both
// pipelines need the same correctness properties):
//
//   - Body-slot semaphore (bounded compressed-body memory; FIFO
//     wake order via Go's sendq).
//   - context.WithCancelCause + recordHardErr for unified abort
//     reason across goroutines.
//   - Per-decode-worker byte budget for emit-side back-pressure.
//   - Stall observer (pure observer; never cancels).
//
// We keep these as parallel implementations rather than shared
// helpers in this first cut. After both pipelines ship and the
// boundaries are stable we can revisit factoring out the
// primitives that are textually identical.

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ueisele/s3pgstore/pool"
)

// PollOption is the interface implemented by PollRecords /
// PollRecordsIter modifiers. Distinct from ReadOption so the
// surfaces don't cross-contaminate at the call site (a
// WithPollFetchAheadFiles passed to ReadIter is a compile error).
type PollOption interface {
	applyPoll(*pollOpts)
}

// pollOpts is the resolved per-call tuning for the poll pipeline.
// Pointer-typed defaultable fields distinguish "user said zero"
// from "unset" so iter defaulters never overwrite a deliberate
// zero.
type pollOpts struct {
	fetchAheadFiles  int
	decodeWorkers    int
	decodeAheadFiles *int
	decodeAheadBytes int64
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
// loop. ctx + cancel mirror read.go's streamState — single
// cancellation source via WithCancelCause; recordHardErr cancels
// with the abort reason as cause atomically with ctx.Done close.
type pollState struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	files []*fileState

	// slotCh: body-slot semaphore — buffered chan, FIFO sendq.
	// Acquire on the fetcher side before submitting download
	// tasks; release in the decode worker after the body is
	// nil'd (post-decode).
	slotCh chan struct{}

	// lastProgressNs: wall-clock timestamp (UnixNano) of the most
	// recent decoder progress event. Bumped on every
	// releaseBodySlot. Read by runPollDeadlockObserver to surface
	// pipelines that have parked on body-slot acquire / byte-
	// budget reservation / queue send for too long. Zero means
	// "no decoder progress yet" — legitimate startup state, not
	// a stall.
	lastProgressNs atomic.Int64

	// decoderFi: emit's current file index — atomic so the stall
	// observer can read it without locking. Logged in slog
	// alongside the stall warn for context.
	decoderFi atomic.Int64

	m *metrics
}

// recordHardErr cancels state.ctx with err as the cause. First
// call wins; subsequent calls are no-ops (the pool's errgroup
// preserves first-error semantics). Any goroutine reading
// context.Cause(state.ctx) after a select on state.ctx.Done()
// is guaranteed to see the cause set atomically with the close.
func (state *pollState) recordHardErr(err error) {
	state.cancel(err)
}

// acquireBodySlot blocks until a body slot is available or ctx
// is cancelled. Returns context.Cause(ctx) on cancellation, nil
// on success.
//
// Records to metrics.recordIterBodySlotWait only when the slot
// acquire fell through to the blocking select AND succeeded —
// the cancel path is not recorded. The fast path (slot available
// immediately) costs no metric observation.
func (state *pollState) acquireBodySlot(ctx context.Context) error {
	if state.slotCh == nil {
		return context.Cause(ctx)
	}
	// Best-effort fast path before paying for a select.
	select {
	case state.slotCh <- struct{}{}:
		return nil
	default:
	}
	waitStart := time.Now()
	select {
	case state.slotCh <- struct{}{}:
		state.m.recordIterBodySlotWait(ctx, time.Since(waitStart))
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// releaseBodySlot returns one slot to the semaphore. The receive
// wakes the FIFO-earliest blocked downloader (per Go's runtime
// sendq guarantee). Also bumps lastProgressNs so
// runPollDeadlockObserver sees decoder-side progress (slot freed
// = the decoder is making its way through files).
func (state *pollState) releaseBodySlot() {
	if state.slotCh == nil {
		return
	}
	<-state.slotCh
	state.lastProgressNs.Store(time.Now().UnixNano())
}

// pollWorkerState is the per-worker state for the poll pipeline.
// Mirrors read.go's workerState but on FileRef-grain decoded
// batches.
type pollWorkerState[T any] struct {
	queue chan decodedPollBatch[T]

	mu            sync.Mutex
	bufferedBytes int64
	releaseSig    chan struct{}

	m *metrics
}

func newPollWorkerState[T any](queueCap int, m *metrics) *pollWorkerState[T] {
	return &pollWorkerState[T]{
		queue:      make(chan decodedPollBatch[T], queueCap),
		releaseSig: make(chan struct{}, 1),
		m:          m,
	}
}

// reserveBytes accounts uncomp bytes against the per-worker cap
// (same single-waiter discipline as read.go's workerState).
// Empty-buffer escape lets a single oversized file flow.
func (ws *pollWorkerState[T]) reserveBytes(
	ctx context.Context, uncomp, cap int64,
) error {
	if cap <= 0 || uncomp <= 0 {
		return context.Cause(ctx)
	}
	var waitStart time.Time
	waited := false
	for {
		ws.mu.Lock()
		fits := ws.bufferedBytes <= 0 || ws.bufferedBytes+uncomp <= cap
		if fits {
			ws.bufferedBytes += uncomp
			ws.mu.Unlock()
			if waited {
				ws.m.recordIterByteBudgetWait(ctx, time.Since(waitStart))
			}
			return nil
		}
		ws.mu.Unlock()
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if !waited {
			waitStart = time.Now()
			waited = true
		}
		select {
		case <-ws.releaseSig:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// releaseBytes is called by the emit loop after a file's records
// have been forwarded; frees the reservation so the owning
// worker can pick the next file.
func (ws *pollWorkerState[T]) releaseBytes(uncomp int64) {
	if uncomp <= 0 {
		return
	}
	ws.mu.Lock()
	ws.bufferedBytes -= uncomp
	ws.mu.Unlock()
	select {
	case ws.releaseSig <- struct{}{}:
	default:
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
		slotCh: make(chan struct{}, bodyCap),
		m:      s.metrics,
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
	workers := make([]*pollWorkerState[T], opts.decodeWorkers)
	for w := range workers {
		workers[w] = newPollWorkerState[T](*opts.decodeAheadFiles, s.metrics)
	}
	for w := range workers {
		wg.Go(func() {
			s.runPollDecodeWorker(state.ctx, state, workers[w],
				w, opts.decodeWorkers, opts)
		})
	}

	// Stall watchdog — pure observer; never cancels.
	wg.Go(func() {
		state.runPollDeadlockObserver(state.ctx, method,
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

// runPollFetcher walks files in input order and submits one
// download task per file to the shared pool. Body-slot acquire
// happens here (fetcher-side back-pressure) so pool workers
// never block on per-call coordination — see CLAUDE.md.
//
// On download success the task sets fs.body, closes fs.done,
// and bumps lastProgressNs (the watchdog's progress signal).
// On download failure: releaseBodySlot, recordHardErr (cancels
// state.ctx with the wrapped err as cause), then close(fs.done)
// — order matters: cause is set before done closes so the
// decoder's waitForFile observes both atomically.
func (s *Store[T]) runPollFetcher(
	ctx context.Context, g *pool.Group, state *pollState,
) {
	for fi := range state.files {
		if state.acquireBodySlot(ctx) != nil {
			return
		}
		fs := state.files[fi]
		key := fs.file.S3Key
		g.Submit(ctx, func(ctx context.Context) error {
			body, err := s.target.get(ctx, key)
			if err != nil {
				state.releaseBodySlot()
				wrapped := fmt.Errorf("get %s: %w", key, err)
				state.recordHardErr(wrapped)
				close(fs.done)
				return wrapped
			}
			// Slot stays held; decoder releases after nil-out.
			fs.body = body
			close(fs.done)
			state.lastProgressNs.Store(time.Now().UnixNano())
			return nil
		})
	}
}

// waitForFile blocks until file fi's download has completed
// (done closed) or ctx is cancelled. Returns nil on completion,
// context.Cause(ctx) on cancellation. The download task either
// sets fs.body (success) or calls recordHardErr (failure) before
// closing done; on success the caller must check fs.body for nil
// against the ctx-cause to detect "downloader recorded a hard
// err" — but the standard pattern is: if waitForFile returns
// nil AND fs.body is non-nil, decode proceeds.
func (state *pollState) waitForFile(ctx context.Context, fi int) error {
	fs := state.files[fi]
	select {
	case <-fs.done:
		// Distinguish "download succeeded" from "download recorded
		// a hard err and closed done." The latter sets ctx-cause;
		// returning the cause lets the worker forward it directly.
		if err := context.Cause(ctx); err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// runPollDecodeWorker handles files assigned to worker w via
// round-robin: fi where fi % numWorkers == w. Each iteration
// waits for the download, parses the footer for uncompressed
// bytes, reserves byte budget, decodes, and publishes the
// result to ws.queue. Emit drains ws.queue in input order.
func (s *Store[T]) runPollDecodeWorker(
	ctx context.Context, state *pollState,
	ws *pollWorkerState[T], workerIdx, numWorkers int,
	opts *pollOpts,
) {
	for fi := workerIdx; fi < len(state.files); fi += numWorkers {
		if err := state.waitForFile(ctx, fi); err != nil {
			sendPollBatch(ctx, ws.queue, decodedPollBatch[T]{err: err})
			return
		}
		fs := state.files[fi]

		// Parse footer once: exact uncompressed bytes for the
		// byte budget. Per-file, no row-count pre-allocation
		// (decodeParquet allocates as needed for one file).
		uncomp, err := pollFooterUncomp(fs.body)
		if err != nil {
			state.releaseBodySlot()
			fs.body = nil
			sendPollBatch(ctx, ws.queue, decodedPollBatch[T]{
				err: fmt.Errorf("footer %s: %w", fs.file.S3Key, err),
			})
			return
		}

		// Gate on this worker's byte budget if configured.
		if err := ws.reserveBytes(ctx, uncomp, opts.decodeAheadBytes); err != nil {
			state.releaseBodySlot()
			fs.body = nil
			sendPollBatch(ctx, ws.queue, decodedPollBatch[T]{err: err})
			return
		}

		decodeStart := time.Now()
		recs, err := decodeParquet[T](fs.body)
		state.m.recordIterDecodeDuration(ctx, time.Since(decodeStart))
		// Free the body and slot regardless of decode outcome.
		fs.body = nil
		state.releaseBodySlot()
		if err != nil {
			ws.releaseBytes(uncomp)
			sendPollBatch(ctx, ws.queue, decodedPollBatch[T]{
				err: fmt.Errorf("decode %s: %w", fs.file.S3Key, err),
			})
			return
		}

		if !sendPollBatch(ctx, ws.queue, decodedPollBatch[T]{
			file:        fs.file,
			records:     recs,
			uncompBytes: uncomp,
		}) {
			ws.releaseBytes(uncomp)
			return
		}
	}
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

// sendPollBatch pushes a batch onto ws.queue, returning false on
// ctx cancellation so the caller can clean up the byte
// reservation it might have just made.
//
// Best-effort delivery — same rationale as read.go's sendBatch:
// race-free non-blocking send first to avoid Go's select
// non-determinism dropping an error batch when the buffer has
// capacity AND ctx is cancelled.
func sendPollBatch[T any](
	ctx context.Context, q chan<- decodedPollBatch[T],
	b decodedPollBatch[T],
) bool {
	select {
	case q <- b:
		return true
	default:
	}
	select {
	case q <- b:
		return true
	case <-ctx.Done():
		return false
	}
}

// runPollDeadlockObserver surfaces stalls (no decoder progress
// within stallThreshold) via slog.Warn + the
// s3pgstore.read.iter.stall.count counter. Pure observer — never
// cancels. Mirrors read.go's runDeadlockObserver: tracks
// decoder-side progress (lastProgressNs bumped by releaseBodySlot)
// and logs the emit position (decoderFi) for context.
//
// Pure-observer rationale matches read.go: auto-cancelling on
// stall would mask information needed to diagnose the underlying
// issue and risk false-positive aborts of legitimately slow
// consumers. Hard ceilings belong at the call site via
// ctx.WithTimeout.
func (state *pollState) runPollDeadlockObserver(
	ctx context.Context, method string,
	tick, threshold time.Duration,
) {
	t := time.NewTicker(tick)
	defer t.Stop()
	thresholdNs := threshold.Nanoseconds()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			last := state.lastProgressNs.Load()
			if last == 0 {
				// Pipeline hasn't logged any decoder progress
				// yet; either the very-first download is still
				// in flight (a single GET that exceeds the
				// threshold is a legitimate reason to skip the
				// alert) or the pipeline is empty and about to
				// exit.
				continue
			}
			staleNs := now.UnixNano() - last
			if staleNs < thresholdNs {
				continue
			}
			slog.Warn(
				"s3pgstore: poll pipeline made no forward progress within watchdog window",
				"method", method,
				"stale_seconds", time.Duration(staleNs).Seconds(),
				"decoder_file", state.decoderFi.Load(),
				"slot_occupancy", len(state.slotCh),
				"slot_capacity", cap(state.slotCh),
			)
			state.m.recordIterStall(ctx, method)
		}
	}
}

// PollRecordsIter walks the half-open offset range
// [since, until) and yields one FileResult per file in feed_seq
// (commit-time) order. Each yield is the file's catalog row
// (FileRef, including Offset for checkpointing) plus its decoded
// records. No dedup — stream consumers see every observed
// version.
//
// Memory is bounded by the pipeline knobs:
//   - WithPollFetchAheadFiles caps resident compressed bodies.
//   - WithPollDecodeAheadFiles caps the per-worker decode queue.
//   - WithPollDecodeAheadBytes caps decoded uncompressed bytes
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
