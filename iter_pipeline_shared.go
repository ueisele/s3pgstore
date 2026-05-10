package s3pgstore

// iter_pipeline_shared.go holds the primitives that read.go's
// readFetchAndDecodeIter and poll.go's pollFetchAndDecodeIter
// both rely on. It is NOT the pipeline itself — each pipeline lives
// in its own file with its own grain (partition vs file). What
// lives here is the load-bearing machinery that must stay
// byte-identical across both: the body-slot semaphore, the
// per-decode-worker byte budget, the deadlock observer, and the
// race-free batch send. Centralising removes the silent-drift
// risk these primitives carry — the cond+counter starvation
// deadlock that motivated the chan-based redesign (CLAUDE.md
// "Concurrency invariants") was a single-pipeline bug, but the
// same shape exists in two pipelines now and any fix must apply
// to both.
//
// Pipeline-shape pieces (fetcher loop, decoder loop, state
// structs, defaulters) deliberately stay parallel in read.go /
// poll.go: they're short, grain-specific, and easier to read
// in isolation than as a generic abstraction.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Stall watchdog defaults. Wired by both pipelines via
// runDeadlockObserver. The observer is a background safety net,
// not a hot-path metric:
//
//   - tick at 30s — at most 30s of latency between a stall
//     starting and the first slog.Warn; operators see it long
//     before SIGQUIT-debugging is needed.
//   - threshold at 60s (2× tick) — a single missed tick is
//     timing noise on a slow consumer about to pull the next
//     batch. Two missed ticks is real lack of forward progress.
const (
	stallTickInterval = 30 * time.Second
	stallThreshold    = 60 * time.Second
)

// bodySlots is the per-call compressed-body semaphore. Buffered
// channel because Go's runtime drains a channel's sendq strictly
// FIFO on every receive, so the earliest-parked downloader wakes
// next when a slot is freed — no scheduler-biased starvation
// (the failure mode that motivated the chan-based redesign;
// see CLAUDE.md "No cond.Broadcast with per-actor predicates").
//
// lastProgressNs is bumped on every release AND by the fetcher
// (markComplete in read; the download task fn in poll) so the
// watchdog observes any forward-progress event in the pipeline,
// not just decoder consumption.
type bodySlots struct {
	slotCh         chan struct{}
	lastProgressNs atomic.Int64
	m              *metrics
}

// newBodySlots constructs a body-slot semaphore. cap must be
// positive — both pipelines floor bodyCap to max(1, pool budget,
// max files per partition) before constructing, so a non-positive
// value is a programming error. Panicking here keeps the
// acquire/release fast paths free of nil-channel guards (no dead
// code that could mask a future wiring bug).
func newBodySlots(cap int, m *metrics) *bodySlots {
	if cap <= 0 {
		panic(fmt.Sprintf("newBodySlots: cap must be > 0, got %d", cap))
	}
	return &bodySlots{slotCh: make(chan struct{}, cap), m: m}
}

// acquire reserves one slot. Fast path is a non-blocking send
// to skip metric instrumentation when the pool isn't saturated;
// the racing select records to recordIterBodySlotWait only when
// the wait fired AND succeeded — cancel-during-wait is not
// recorded (shutdown noise would drown out the saturation
// signal). Returns context.Cause(ctx) on cancellation.
func (b *bodySlots) acquire(ctx context.Context) error {
	select {
	case b.slotCh <- struct{}{}:
		return nil
	default:
	}
	waitStart := time.Now()
	select {
	case b.slotCh <- struct{}{}:
		b.m.recordIterBodySlotWait(ctx, time.Since(waitStart))
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// release returns one slot to the pool and bumps lastProgressNs
// so the watchdog observes decoder-side forward progress (a slot
// freed = the decoder advanced through a file). The receive on
// slotCh wakes the FIFO-earliest blocked downloader.
func (b *bodySlots) release() {
	<-b.slotCh
	b.lastProgressNs.Store(time.Now().UnixNano())
}

// bumpProgress records a forward-progress event without releasing
// a slot. Used by fetchers to report download completion (the
// slot stays held — the decoder releases it after nil-out).
func (b *bodySlots) bumpProgress() {
	b.lastProgressNs.Store(time.Now().UnixNano())
}

func (b *bodySlots) lastProgress() int64 { return b.lastProgressNs.Load() }
func (b *bodySlots) occupancy() int      { return len(b.slotCh) }
func (b *bodySlots) capacity() int       { return cap(b.slotCh) }

// waitOrCancel blocks until done is closed or ctx is cancelled,
// then returns context.Cause(ctx) — nil iff ctx is still alive
// at return time. The post-condition (nil ⇒ ctx alive) folds in
// the "did a sibling task error?" check that the read pipeline's
// waitForPartition relied on: recordHardErr always precedes the
// download-side close (markComplete in read, close(fs.done) in
// poll), so when done closes for a partition/file with any nil
// body, state.ctx is already cancelled and the cause is set.
func waitOrCancel(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
	case <-ctx.Done():
	}
	return context.Cause(ctx)
}

// workerState is the per-decode-worker queue + private byte
// budget. Generic over the batch type B so read uses
// workerState[decodedBatch[T]] and poll uses
// workerState[decodedPollBatch[T]].
//
// Workers self-assign work units round-robin (worker w handles
// idx where idx % W == w); emit drains queues in idx order so
// the deterministic-emission contract holds.
//
// Single-waiter discipline on releaseSig: each worker is the
// sole reserver against its own ws; emit is the sole releaser.
// While the worker is parked, bufferedBytes can only stay the
// same or decrease (no other worker writes to ws), so stale
// releaseSig signals are harmless — the loop always re-checks
// the predicate.
type workerState[B any] struct {
	queue chan B

	mu            sync.Mutex
	bufferedBytes int64
	releaseSig    chan struct{}

	m *metrics
}

func newWorkerState[B any](queueCap int, m *metrics) *workerState[B] {
	return &workerState[B]{
		queue:      make(chan B, queueCap),
		releaseSig: make(chan struct{}, 1),
		m:          m,
	}
}

// reserveBytes accounts uncomp bytes against the per-worker cap.
// Blocks while bufferedBytes + uncomp would exceed cap AND the
// buffer is non-empty; the empty-buffer escape lets a single
// oversized work unit through (otherwise the worker would block
// on the cap with emit waiting for the result). Returns nil on
// successful reservation; non-nil error (= context.Cause(ctx))
// when ctx is cancelled while waiting.
//
// Records to recordIterByteBudgetWait only when the wait fired
// AND the reservation succeeded — cancel path is not recorded.
func (ws *workerState[B]) reserveBytes(
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

// releaseBytes is called by the emit loop after a work unit's
// records have been forwarded; frees the reservation so the
// owning worker can pick the next unit. The non-blocking send
// on releaseSig is the bell-ring: a coalesced wake costs at most
// one extra trip through the predicate loop.
func (ws *workerState[B]) releaseBytes(uncomp int64) {
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

// sendBatch pushes b onto q, returning false on ctx cancellation
// so the caller can clean up any byte reservation it just made.
//
// Best-effort delivery: try the non-blocking send first. Without
// this the select below would race ctx.Done against a ready
// send, and Go's non-deterministic select could drop an error
// batch when the buffer has capacity AND ctx is already cancelled
// — a silent-drop hole on the strict-error path where the
// downloader's cancel() runs before the decoder's send arrives.
// Only fall back to the racing select when the buffer is full.
func sendBatch[B any](
	ctx context.Context, q chan<- B, b B,
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

// runDeadlockObserver is the iter pipeline's stall watchdog —
// pure observer, never cancels. Periodically wakes and emits a
// slog.Warn + recordIterStall increment if the pipeline made no
// forward progress (slots.lastProgress) within the threshold
// window.
//
// kind tags the slog message ("read" or "poll" — operators
// dashboarding can route on this). posKey + posLoader carry the
// pipeline's emit position for the log record (decoder_partition
// + state.decoderPi.Load for read; decoder_file +
// state.decoderFi.Load for poll). method is the public entry
// point name (ReadIter, PollRecordsIter, …) for the metric
// attribute.
//
// Pure-observer rationale: auto-cancelling on stall would mask
// information needed to diagnose the underlying issue (goroutine
// state for SIGQUIT, channel occupancy, decoder position) and
// risk false-positive aborts of legitimately slow consumers.
// Users who want a hard ceiling pass ctx.WithTimeout at the call
// site — that propagates through every layer uniformly.
func runDeadlockObserver(
	ctx context.Context, m *metrics, method, kind string,
	slots *bodySlots, posKey string, posLoader func() int64,
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
			last := slots.lastProgress()
			if last == 0 {
				// Pipeline hasn't logged any progress yet;
				// either the very-first download is still in
				// flight (a single GET that exceeds the
				// threshold is a legitimate reason to skip the
				// alert) or the pipeline is empty and about to
				// exit.
				continue
			}
			if slots.occupancy() == 0 {
				// Nothing in flight — there's nothing to
				// deadlock on. Suppresses false positives
				// during idle periods (notably the tail
				// feeder's between-poll waits) and during
				// pipeline shutdown after the last body has
				// been released.
				continue
			}
			staleNs := now.UnixNano() - last
			if staleNs < thresholdNs {
				continue
			}
			slog.Warn(
				"s3pgstore: "+kind+" pipeline made no forward progress within watchdog window",
				"method", method,
				"stale_seconds", time.Duration(staleNs).Seconds(),
				posKey, posLoader(),
				"slot_occupancy", slots.occupancy(),
				"slot_capacity", slots.capacity())
			m.recordIterStall(ctx, method)
		}
	}
}
