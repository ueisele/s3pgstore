package s3pgstore

// Phase 12 — chan-based iter pipeline.
//
// Vendored from
// https://github.com/ueisele/s3store/blob/da75ca9/reader_iter.go
// (post `860cf19` "Replace iter pipeline sync.Cond with chan-based
// primitives" + `da75ca9` "Fix iter pipeline deadlock under
// cond.Wait scheduler unfairness"). Copyright (c) 2024-2026 Uwe
// Eisele. MIT License.
//
// What we vendor: the producer → downloader → decoder topology,
// the body-slot semaphore (buffered chan, FIFO sendq),
// reserveBytes / releaseBytes around WithReadAheadBytes, the
// stall-watchdog (pure observer), recordEmit / partitionEmit.
//
// What we adapt:
//
//   - s3store.Reader[T] → s3pgstore.Store[T]; Target is reached
//     via s.target rather than s.cfg.Target.
//   - keyMeta → fileRow: the catalog gives us partition_key,
//     written_at_version, and ext_<n> values per file at
//     SELECT time, so partState carries fileRow directly. No
//     hiveKeyOfDataFile parsing — the partition is already in
//     the row.
//   - tolerantOfMissingData is gone: s3pgstore has no ref-stream
//     gate, every read path is strict. NoSuchKey from S3
//     surfaces as a wrapped error (operator-driven prune is
//     incident-class, not silently skipped).
//   - The `keys []keyMeta` upstream argument splits into three
//     input modes here (filters / time range / pre-resolved
//     entries); each method enumerates fileRow and hands them
//     to the same pipeline.
//   - Methods retain s3pgstore signatures: ReadIter takes
//     `[]PartitionFilter`, ReadPartitionIter yields
//     PartitionResult[T] (carrying Version + FileExtensions),
//     not HivePartition[T].
//
// What we DO NOT vendor:
//
//   - The s3 missing-data counter / slog.Warn skip — strict
//     paths only here.
//   - methodScope addRecords/addPartitions/addBytes/addFiles —
//     s3pgstore methodScope keeps the surface to duration +
//     calls + in_flight; the iter saturation metrics
//     (body_slot, byte_budget, decode_duration, stall.count)
//     cover the operationally interesting iter signal.

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ueisele/s3pgstore/pool"
)

// ReadIter returns an iter.Seq2[T, error] that yields every
// record matching filters, lazily one partition at a time
// through the chan-based pipeline.
//
// Memory: O((WithReadAheadPartitions + 1) partitions' decoded
// records) by default; cap with WithReadAheadBytes to bound the
// uncompressed-bytes footprint. Without options, one partition's
// records sit in memory at any time.
//
// Cancellation: yielding `false` from the loop body cancels
// in-flight S3 GETs through ctx propagation. Iteration also
// terminates immediately on the first per-partition error.
//
// Emission order: lex by partition key, then per-partition
// (entity, version) ascending after dedup (if configured) or
// decode/insertion order without it. Same input yields byte-
// identical sequences across runs per CLAUDE.md.
//
// Empty filters yields nothing without touching the database.
func (s *Store[T]) ReadIter(
	ctx context.Context, filters []PartitionFilter,
	opts ...ReadOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadIter", &iterErr).end()
		o := resolveIterOpts(opts)
		if len(filters) == 0 {
			return
		}
		rows, err := s.selectFileRows(ctx, filters)
		if err != nil {
			iterErr = err
			yield(*new(T), err)
			return
		}
		s.downloadAndDecodeIter(ctx, "ReadIter", rows, &o,
			s.recordEmit(yield, &iterErr))
	}
}

// ReadPartitionIter is the per-partition variant of ReadIter:
// each yield is one PartitionResult[T] with Records, Version,
// and FileExtensions populated. Same memory bound and
// cancellation semantics as ReadIter.
func (s *Store[T]) ReadPartitionIter(
	ctx context.Context, filters []PartitionFilter,
	opts ...ReadOption,
) iter.Seq2[PartitionResult[T], error] {
	return func(yield func(PartitionResult[T], error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadPartitionIter", &iterErr).end()
		o := resolveIterOpts(opts)
		if len(filters) == 0 {
			return
		}
		rows, err := s.selectFileRows(ctx, filters)
		if err != nil {
			iterErr = err
			yield(PartitionResult[T]{}, err)
			return
		}
		s.downloadAndDecodeIter(ctx, "ReadPartitionIter", rows, &o,
			s.partitionEmit(yield, &iterErr))
	}
}

// ReadRangeIter walks every record whose feed_seq_at falls in
// [since, until). Bounds are resolved at call entry via the
// SELECT's WHERE clause, so the upper bound stays stable under
// concurrent writes — once the catalog SELECT runs, no new rows
// can appear in the result set.
//
// Half-open semantics:
//   - since.IsZero() → start at the stream head (unbounded
//     below).
//   - until.IsZero() → walk to the live tip (unbounded above —
//     captured by the SELECT; rows committed after the SELECT
//     don't contribute).
//   - Records at since are included; records at until are not.
//
// No partition filter is applied — every partition with any
// in-range row contributes records.
func (s *Store[T]) ReadRangeIter(
	ctx context.Context, since, until time.Time,
	opts ...ReadOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadRangeIter", &iterErr).end()
		o := resolveIterOpts(opts)
		rows, err := s.selectFileRowsByRange(ctx, since, until)
		if err != nil {
			iterErr = err
			yield(*new(T), err)
			return
		}
		s.downloadAndDecodeIter(ctx, "ReadRangeIter", rows, &o,
			s.recordEmit(yield, &iterErr))
	}
}

// ReadPartitionRangeIter is the per-partition variant of
// ReadRangeIter — same time-bound resolution, per-partition
// output shape.
func (s *Store[T]) ReadPartitionRangeIter(
	ctx context.Context, since, until time.Time,
	opts ...ReadOption,
) iter.Seq2[PartitionResult[T], error] {
	return func(yield func(PartitionResult[T], error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadPartitionRangeIter", &iterErr).end()
		o := resolveIterOpts(opts)
		rows, err := s.selectFileRowsByRange(ctx, since, until)
		if err != nil {
			iterErr = err
			yield(PartitionResult[T]{}, err)
			return
		}
		s.downloadAndDecodeIter(ctx, "ReadPartitionRangeIter", rows, &o,
			s.partitionEmit(yield, &iterErr))
	}
}

// ReadEntriesIter decodes pre-resolved StreamEntry slices
// without re-querying the catalog. Each entry's S3 key is
// validated up front against this Store's bucket+prefix; an
// entry from a different Store fails with an error before any
// S3 traffic.
//
// Records emit in lex order of partition key (Key field). The
// pipeline groups by partition first and sorts the partition
// keys lex before driving the producer — the input slice's
// order is not preserved across partitions (per the
// "Deterministic emission order" contract in CLAUDE.md).
func (s *Store[T]) ReadEntriesIter(
	ctx context.Context, entries []StreamEntry,
	opts ...ReadOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadEntriesIter", &iterErr).end()
		o := resolveIterOpts(opts)
		if err := s.validateEntries(entries); err != nil {
			iterErr = err
			yield(*new(T), err)
			return
		}
		rows := entriesToFileRows(entries, s.resolved.ExtensionColumns)
		s.downloadAndDecodeIter(ctx, "ReadEntriesIter", rows, &o,
			s.recordEmit(yield, &iterErr))
	}
}

// ReadPartitionEntriesIter is the per-partition variant of
// ReadEntriesIter.
func (s *Store[T]) ReadPartitionEntriesIter(
	ctx context.Context, entries []StreamEntry,
	opts ...ReadOption,
) iter.Seq2[PartitionResult[T], error] {
	return func(yield func(PartitionResult[T], error) bool) {
		var iterErr error
		defer s.metrics.methodScope(ctx, "ReadPartitionEntriesIter", &iterErr).end()
		o := resolveIterOpts(opts)
		if err := s.validateEntries(entries); err != nil {
			iterErr = err
			yield(PartitionResult[T]{}, err)
			return
		}
		rows := entriesToFileRows(entries, s.resolved.ExtensionColumns)
		s.downloadAndDecodeIter(ctx, "ReadPartitionEntriesIter", rows, &o,
			s.partitionEmit(yield, &iterErr))
	}
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

// recordEmit returns the per-batch emit callback that flattens
// each partition's already-dedup'd records into the consumer's
// iter.Seq2[T, error] yield. Used by ReadIter / ReadRangeIter /
// ReadEntriesIter — paths that surface records one at a time.
//
// On a hard pipeline error: sets *iterErr, yields (zero T, err)
// once, returns false so the emit loop terminates and the
// deferred methodScope.end picks up *iterErr for outcome
// classification.
func (s *Store[T]) recordEmit(
	yield func(T, error) bool, iterErr *error,
) func(decodedBatch[T]) bool {
	return func(b decodedBatch[T]) bool {
		if b.err != nil {
			*iterErr = b.err
			yield(*new(T), b.err)
			return false
		}
		for _, r := range b.records {
			if !yield(r, nil) {
				return false
			}
		}
		return true
	}
}

// partitionEmit returns the per-batch emit callback that yields
// one PartitionResult[T] per partition. Used by
// ReadPartitionIter / ReadPartitionRangeIter /
// ReadPartitionEntriesIter — paths that surface records grouped
// by partition.
//
// On a hard pipeline error: sets *iterErr, yields a zero
// PartitionResult with the error, returns false so the emit
// loop terminates.
func (s *Store[T]) partitionEmit(
	yield func(PartitionResult[T], error) bool, iterErr *error,
) func(decodedBatch[T]) bool {
	return func(b decodedBatch[T]) bool {
		if b.err != nil {
			*iterErr = b.err
			yield(PartitionResult[T]{}, b.err)
			return false
		}
		return yield(PartitionResult[T]{
			PartitionKey:   b.partitionKey,
			Records:        b.records,
			Version:        b.version,
			FileExtensions: b.exts,
		}, nil)
	}
}

// downloadAndDecodeIter is the chan-based streaming pipeline
// backing every ReadIter / ReadPartitionIter / ReadRangeIter /
// ReadPartitionRangeIter / ReadEntriesIter /
// ReadPartitionEntriesIter call.
//
// Three concurrent stages plus the caller's emit loop:
//
//  1. Submitter goroutine: walks partitions in lex order,
//     acquires one body-pool slot per file (submitter-side
//     back-pressure — see CLAUDE.md "Shared-pool workers must
//     never block on per-call coordination"), then submits one
//     download task per file to the Store's shared *pool.Pool.
//     Cross-partition lookahead happens here — pool tasks are
//     not partition-bound, so partition P+1's downloads can run
//     in parallel with partition P being yielded.
//
//  2. Decoder goroutine: walks partitions in order; for each,
//     waits until all its files are downloaded, parses each
//     parquet footer to compute the partition's exact
//     uncompressed total, gates on (ReadAheadPartitions,
//     ReadAheadBytes), decodes records, sort+dedup's them
//     in-place, and pushes a decodedBatch to the emitter.
//
//  3. Emit loop (this goroutine): pulls decoded partitions in
//     order, hands each to emitOne (record-by-record yield or
//     PartitionResult yield), and frees the partition's
//     reserved bytes on completion so the decoder can proceed.
//
// On a hard pipeline error, decoder sends decodedBatch{err:err}
// and the emit callback receives a non-nil err — it should
// yield the error to the consumer, set iterErr, and return
// false. On success, emit returns true to keep going.
//
// Pool-worker shape: the submitted task does only the S3 GET
// and a markComplete / state.releaseBodySlots / recordHardErr
// update. It never blocks on per-call coordination — body-slot
// acquire is on the submitter, decoder back-pressure is in the
// (non-pool) decoder goroutine. This satisfies the shared-pool
// rule that pool tasks must always make progress, so a slow
// consumer of one ReadIter cannot starve unrelated Stores
// sharing the pool.
func (s *Store[T]) downloadAndDecodeIter(
	ctx context.Context, method string, rows []fileRow,
	opts *readOpts, emitOne func(decodedBatch[T]) bool,
) {
	if len(rows) == 0 {
		return
	}
	parts := s.preparePartitions(rows)
	if len(parts) == 0 {
		return
	}

	ctx, cancel := context.WithCancel(ctx)

	// bodyCap bounds the per-call in-memory compressed-body
	// footprint: the submitter blocks on acquireBodySlot once
	// cap slots are held; the decoder releases slots as it nils
	// each body. The default ceiling is the per-method cap
	// (target.effectiveConcurrency) so per-call body memory
	// stays predictable regardless of pool size — the shared
	// pool's MaxConcurrent caps in-flight S3 ops globally; this
	// caps body memory locally. Floor at the largest partition's
	// file count so a single oversized partition still fits in
	// the pool — otherwise its last few files would block on
	// the cap and the decoder would block on those files,
	// producing a deadlock.
	bodyCap := s.target.effectiveConcurrency()
	for _, p := range parts {
		if n := len(p.files); n > bodyCap {
			bodyCap = n
		}
	}

	state := &streamState{
		parts:    parts,
		slotCh:   make(chan struct{}, bodyCap),
		byteWake: make(chan struct{}, 1),
		m:        s.metrics,
	}

	// One WaitGroup covers every helper goroutine so the deferred
	// cleanup below can cancel ctx and then wait for everything to
	// drain before returning — no orphaned goroutines, no leaked
	// state.
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	// Stage 1: submitter. Acquires body slots and submits per-file
	// download tasks to the shared pool. Calls g.Wait() before
	// exiting so all in-flight pool tasks drain before
	// downloadAndDecodeIter returns.
	//
	// On any pool task error, the pool's errgroup cancels gctx.
	// The submitter sees that on its next acquireBodySlot and
	// exits early; in-flight tasks observe gctx.Done in
	// s.target.get and bail. Files past the submitter's exit
	// point — including the rest of the current partition and
	// every later partition — are deliberately NOT
	// markComplete'd. Their partitions' done channels would
	// block waitForPartition forever, except we cancel the
	// outer ctx below so waitForPartition's ctx.Done branch
	// fires and the decoder forwards the hardErr that the
	// failing pool task recorded. Old design called the outer
	// cancel from inside the downloader on each hard err; the
	// new design routes the same signal via g.Wait()'s return
	// value (errgroup gives us the first task error; we just
	// translate it into an outer-ctx cancel).
	//
	// recordHardErr on g.Wait err covers the case where
	// every task succeeded but the parent ctx was cancelled
	// (Group.Wait returns parentCtx.Err() in that case): no
	// task recorded a hardErr, the submitter's cancel-detected
	// path may not have fired (if all submissions were
	// already in flight), so without this the decoder might
	// see an outer-ctx-cancel with hardErr nil and exit
	// silently. With this, the consumer always sees a
	// context.Canceled (or the wrapped task error) when the
	// pipeline exits abnormally.
	g, gctx := s.resolved.WorkerPool.WithContext(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runDownloadSubmitter(gctx, g, state)
		if err := g.Wait(); err != nil {
			state.recordHardErr(err)
			cancel()
		}
	}()
	// No cancel-broadcast helper goroutine: every blocking
	// primitive in streamState (slotCh acquire, partState.done
	// wait, byteWake bell) selects on ctx.Done() natively. See
	// CLAUDE.md "Concurrency invariants".

	// Stage 2: decoder. Channel cap = readAheadPartitions so the
	// pipeline buffers up to N decoded partitions ahead. The
	// pointer-typed option distinguishes "not supplied" (nil →
	// default 1, the minimum useful pipeline shape — decode of
	// partition N+1 overlaps yield of partition N) from "explicit
	// zero" (cap=0, unbuffered handoff). To bound stacking when
	// N>1, combine with WithReadAheadBytes.
	readAheadParts := 1
	if opts.readAheadPartitions != nil {
		readAheadParts = *opts.readAheadPartitions
	}
	decodedCh := make(chan decodedBatch[T], readAheadParts)
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runDecoder(ctx, state, opts, decodedCh)
	}()

	// Stall watchdog. Pure observer — surfaces deadlocks
	// (library regressions) and slow consumers via slog.Warn +
	// s3pgstore.read.iter.stall.count, never cancels. See
	// runDeadlockObserver for the rationale.
	wg.Add(1)
	go func() {
		defer wg.Done()
		state.runDeadlockObserver(ctx, method,
			stallTickInterval, stallThreshold)
	}()

	// Stage 3: emit loop. Drains decodedCh, hands each batch to
	// the per-method emit callback (record-by-record yield or
	// PartitionResult yield), signals on each completed
	// partition so the decoder can release the byte-budget
	// reservation.
	for batch := range decodedCh {
		ok := emitOne(batch)
		state.releaseBytes(batch.uncompBytes)
		if !ok {
			return
		}
	}
}

// preparePartitions groups rows by partition_key (rows must
// arrive in lex partition_key, then s3_key order — both the
// SQL ORDER BY and the entries-path projection produce that
// order, EXCEPT the entries path doesn't sort. Defensive sort
// here regardless: cheap on already-sorted input, mandatory for
// out-of-order input.
//
// Each partition's files are sorted by S3 key (deterministic
// download order); per-file writtenAtVersion + ext_<n> values
// are pre-aggregated so the decoded batch can carry Version +
// FileExtensions without a second pass over fileRow.
func (s *Store[T]) preparePartitions(
	rows []fileRow,
) []*partState {
	if len(rows) == 0 {
		return nil
	}
	byPartition := make(map[string][]fileRow)
	for _, r := range rows {
		byPartition[r.partitionKey] = append(byPartition[r.partitionKey], r)
	}
	partitionKeys := make([]string, 0, len(byPartition))
	for k := range byPartition {
		partitionKeys = append(partitionKeys, k)
	}
	// Public contract: partition emission is lex-ordered. Every
	// read path (Read / ReadIter / ReadPartitionIter / ... /
	// ReadEntriesIter / ReadPartitionEntriesIter) flows through
	// this sort. Removing it surfaces Go's randomized map
	// iteration order to the consumer and breaks byte-for-byte
	// stable output across calls — see "Deterministic emission
	// order across read and write paths" in CLAUDE.md.
	slices.Sort(partitionKeys)

	parts := make([]*partState, 0, len(partitionKeys))
	for _, k := range partitionKeys {
		files := byPartition[k]
		// Per-partition file ordering by s3_key — deterministic
		// decode order within a partition, also load-bearing for
		// dedup tie-break (lex-later filename wins on equal
		// max version, per CLAUDE.md).
		slices.SortFunc(files, func(a, b fileRow) int {
			return strings.Compare(a.s3Key, b.s3Key)
		})

		var version int64
		exts := make([]FileExtensions, 0, len(files))
		for _, f := range files {
			if f.writtenAtVersion > version {
				version = f.writtenAtVersion
			}
			extMap := make(map[string]any,
				len(s.resolved.ExtensionColumns))
			for j, c := range s.resolved.ExtensionColumns {
				if j < len(f.extValues) && f.extValues[j] != nil {
					extMap[c.Name] = f.extValues[j]
				}
			}
			exts = append(exts, FileExtensions{
				FileID:     f.fileID,
				Extensions: extMap,
			})
		}

		ps := &partState{
			partitionKey: k,
			files:        files,
			version:      version,
			exts:         exts,
			bodies:       make([][]byte, len(files)),
			done:         make(chan struct{}),
		}
		// Defensive: byPartition only emits non-empty buckets, so
		// len(files) == 0 should be unreachable here. Pre-close
		// for safety so a future caller path that constructs an
		// empty partition can't deadlock the decoder.
		if len(files) == 0 {
			close(ps.done)
		}
		parts = append(parts, ps)
	}
	return parts
}

// partState holds per-partition download progress. partitionKey,
// files, version, and exts are fixed at preparePartitions time;
// bodies + completed are mutated by downloaders under
// streamState.mu.
//
// done is a per-partition completion signal. markComplete closes
// it under streamState.mu when the final file lands;
// waitForPartition selects on it (plus ctx.Done) so the decoder
// observes both completion and cancellation natively without a
// cond.Broadcast helper. Closed exactly once: the
// completed == len(files) edge is reached at most once because
// each downloader marks each file at most once and the close
// happens inside the same critical section that increments
// completed.
//
// No per-file errors slice — runDownloader records hard errors
// on streamState.firstHardErr (single source of truth for the
// decoder); cascade ctx.Canceled from acquire-cancel is
// intentionally swallowed (it's not a "real" error, just the
// pipeline shutting down).
type partState struct {
	partitionKey string
	files        []fileRow
	version      int64
	exts         []FileExtensions
	bodies       [][]byte
	completed    int
	done         chan struct{}
}

// streamState carries the shared mutable state of the pipeline:
// per-partition download counters, the decoded-bytes
// reservation, the body-slot semaphore, and the per-partition /
// byte-budget signal channels used to coordinate across stages
// (download completion, decoded-byte release).
//
// slotCh is the body-slot semaphore, a buffered channel with
// cap = bodyCap. Senders that find the buffer full park in the
// channel's sendq, which the Go runtime drains FIFO on every
// receive — so a release always wakes the longest-waiting
// downloader. Replaces an earlier cond + counter design that
// allowed scheduler-biased starvation: with cond.Broadcast all
// waiters race for the mutex after wake, and whichever the
// scheduler picked first incremented the counter, with the rest
// re-Waiting. On a busy pipeline the same worker could
// consistently win the race, leaving one specific worker's
// pending pull permanently unmarked — and once the decoder
// reached that pull's partition, the pipeline deadlocked.
// Channel-based acquire is strictly FIFO and removes the
// fairness window. See CLAUDE.md "Concurrency invariants".
//
// byteWake is the byte-budget wake bell, a chan(1) edge-trigger
// signal. releaseBytes does a non-blocking send; reserveBytes
// selects on byteWake / ctx.Done in the predicate loop.
// Coalesces multiple releases between checks (single waiter —
// the decoder). A stale signal from a previous wait round is
// harmless: the loop always re-checks the predicate after the
// receive, and only the decoder itself reserves, so
// bufferedBytes can only stay the same or decrease while the
// decoder is parked.
//
// firstHardErr holds the first non-cancellation error a
// downloader hit (NoSuchKey, hard transport). Set once via
// recordHardErr just before cancel() fires so the decoder can
// surface it on its way out — without it, the decoder's
// waitForPartition / reserveBytes returning false on ctx.Done
// would terminate the pipeline silently and the caller would
// observe (partial records, nil error).
//
// m is the optional metrics handle. acquireBodySlot and
// reserveBytes report wait duration via
// metrics.recordIterBodySlotWait /
// recordIterByteBudgetWait when the call blocked and ended in
// success, so operators can see body-slot pool / byte-budget
// contention. Cancel-during-wait is not recorded (shutdown
// noise).
type streamState struct {
	mu            sync.Mutex
	parts         []*partState
	bufferedBytes int64
	slotCh        chan struct{}
	byteWake      chan struct{}
	firstHardErr  error
	m             *metrics

	// lastProgressNs is the wall-clock timestamp (UnixNano) of
	// the most recent forward-progress event in the pipeline:
	// markComplete (a download landed) or releaseBodySlots (the
	// decoder advanced through a file). Read by
	// runDeadlockObserver to surface pipelines that have parked
	// indefinitely. Atomic so the observer reads lock-free;
	// staleness from a recent progress event between the load
	// and the threshold check is fine — the observer fires at
	// most every tick anyway.
	lastProgressNs atomic.Int64
	// decoderPi is the partition index the decoder is currently
	// processing (waiting for / decoding / sending). Surfaced
	// by the observer to point operators at the stuck partition.
	decoderPi atomic.Int64
}

// recordHardErr stores the first non-cancellation download
// error the pipeline hit so the decoder can forward it before
// exiting on ctx.Done. Subsequent calls are no-ops — the first
// error wins. Caller is responsible for invoking cancel()
// afterwards to halt the rest of the pipeline.
func (s *streamState) recordHardErr(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.firstHardErr == nil {
		s.firstHardErr = err
	}
	s.mu.Unlock()
}

// hardErr returns the first hard download error recorded via
// recordHardErr, or nil if none. Read by the decoder on cancel
// paths so a NoSuchKey or hard transport error surfaces even
// when ctx fired before the decoder reached its partition.
func (s *streamState) hardErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstHardErr
}

// acquireBodySlot reserves one slot in the compressed-body
// pool. Blocks while the pool is full and ctx is alive. Returns
// false if ctx is cancelled while waiting.
//
// The pool counts compressed parquet bodies that downloaders
// have stored into per-partition slots and the decoder has not
// yet cleared. It bounds the worst-case compressed-byte
// footprint of the pipeline to roughly cap × largest_compressed_size.
//
// Implemented as a buffered channel `send`. The Go runtime
// drains blocked senders FIFO on every receive, so
// releaseBodySlots always wakes the earliest-parked downloader.
// This is the load-bearing piece for the deterministic-deadlock
// fix: an earlier cond + counter shape allowed scheduler-biased
// starvation (Broadcast wakes everyone, the scheduler picks an
// arbitrary winner, the rest re-Wait), which on busy pipelines
// could leave one specific worker's pending pull permanently
// unmarked.
//
// Records to metrics.recordIterBodySlotWait only when the slot
// wasn't immediately available AND the acquire eventually
// succeeded — cancel-during-wait is intentionally not recorded
// (shutdown noise, near-zero duration would drown out the
// saturation signal).
func (s *streamState) acquireBodySlot(ctx context.Context) bool {
	if s.slotCh == nil {
		return ctx.Err() == nil
	}
	// Non-blocking fast path: slot available, no wait
	// observation.
	select {
	case s.slotCh <- struct{}{}:
		return true
	default:
	}
	waitStart := time.Now()
	select {
	case s.slotCh <- struct{}{}:
		s.m.recordIterBodySlotWait(ctx, time.Since(waitStart))
		return true
	case <-ctx.Done():
		return false
	}
}

// releaseBodySlots returns n slots to the pool. Each receive on
// slotCh wakes the FIFO-earliest blocked downloader (per Go's
// channel sendq). Also bumps lastProgressNs so
// runDeadlockObserver sees decoder-side progress (slots being
// freed = the decoder is making its way through partitions).
func (s *streamState) releaseBodySlots(n int) {
	if n <= 0 || s.slotCh == nil {
		return
	}
	for range n {
		<-s.slotCh
	}
	s.lastProgressNs.Store(time.Now().UnixNano())
}

// markComplete is the downloader-side update: store the body in
// the partition's slot, increment completed, and close the
// partition's done channel when the final file lands. body is
// nil for hard errors and acquire-cancel — the decoder skips
// nil bodies in decodePartition and surfaces hard errors via
// streamState.firstHardErr.
//
// The close happens inside the same critical section that
// increments completed, so the "completed == len(files)" edge
// is reached at most once even under concurrent downloaders —
// each file is marked exactly once by exactly one downloader.
//
// Bumps lastProgressNs after the mutex is released so
// runDeadlockObserver observes a fresh timestamp on every
// downloaded file, lock-free.
func (s *streamState) markComplete(
	partIdx, fileIdx int, body []byte,
) {
	p := s.parts[partIdx]
	s.mu.Lock()
	p.bodies[fileIdx] = body
	p.completed++
	finalFile := p.completed == len(p.files)
	s.mu.Unlock()
	if finalFile {
		close(p.done)
	}
	s.lastProgressNs.Store(time.Now().UnixNano())
}

// waitForPartition blocks until every file in partition pi has
// been downloaded (success or error) or ctx is cancelled.
// Returns true if the partition fully completed; false only
// when ctx fired before completion.
//
// Note the asymmetry with ctx: a partition that finishes at
// roughly the same instant ctx fires still returns true so the
// decoder can inspect streamState.firstHardErr and surface a
// hard error that triggered our own cancel(). Without this, a
// NoSuchKey race could close decodedCh without forwarding the
// error. The non-blocking precheck on p.done preserves this
// asymmetry — Go's select among ready cases is uniform-random,
// so the precheck pattern (same shape as sendBatch) prefers
// completion over cancellation when both are observable.
func (s *streamState) waitForPartition(
	ctx context.Context, pi int,
) bool {
	p := s.parts[pi]
	select {
	case <-p.done:
		return true
	default:
	}
	select {
	case <-p.done:
		return true
	case <-ctx.Done():
		return false
	}
}

// reserveBytes accounts uncomp bytes against the cap. Blocks
// while bufferedBytes + uncomp would exceed cap AND the buffer
// is non-empty; the empty-buffer escape lets a single oversized
// partition through (otherwise the pipeline would deadlock).
// Returns false if ctx is cancelled while waiting.
//
// Predicate-loop shape: lock, check, unlock, wait on byteWake
// or ctx.Done, retry. Stale signals are harmless — only the
// decoder reserves and the loop always re-checks the predicate
// after the receive, so bufferedBytes can only stay the same
// or decrease while parked. The bell coalesces multiple
// releases between checks because the chan has cap 1 and
// releaseBytes uses a non-blocking send.
//
// Records to metrics.recordIterByteBudgetWait only when the
// wait fired AND the reservation succeeded — same shape as
// acquireBodySlot, cancel path is not recorded.
func (s *streamState) reserveBytes(
	ctx context.Context, uncomp, cap int64,
) bool {
	if cap <= 0 || uncomp <= 0 {
		return ctx.Err() == nil
	}
	var waitStart time.Time
	waited := false
	for {
		s.mu.Lock()
		// "Fits" is the inverse of the blocking predicate
		// "buffer non-empty AND would exceed cap": empty buffer
		// → oversized partition escape; cap not exceeded → just
		// fits. Either case admits the reservation.
		fits := s.bufferedBytes <= 0 || s.bufferedBytes+uncomp <= cap
		if fits {
			s.bufferedBytes += uncomp
			s.mu.Unlock()
			if waited {
				s.m.recordIterByteBudgetWait(ctx, time.Since(waitStart))
			}
			return true
		}
		s.mu.Unlock()
		if ctx.Err() != nil {
			// Cancel path: do not record — only successful
			// reservations are observed.
			return false
		}
		if !waited {
			waitStart = time.Now()
			waited = true
		}
		select {
		case <-s.byteWake:
		case <-ctx.Done():
			return false
		}
	}
}

// releaseBytes is called by the emit loop after a partition's
// records have been forwarded; frees the reservation so the
// decoder can pick the next partition. The non-blocking send
// on byteWake is the bell-ring: if a previous signal is still
// pending (decoder hasn't consumed it), this drop is fine
// because the decoder always re-checks the predicate after a
// receive — a coalesced wake costs at most one extra trip
// through the predicate loop.
func (s *streamState) releaseBytes(uncomp int64) {
	if uncomp <= 0 {
		return
	}
	s.mu.Lock()
	s.bufferedBytes -= uncomp
	s.mu.Unlock()
	select {
	case s.byteWake <- struct{}{}:
	default:
	}
}

// runDownloadSubmitter walks partitions in order and submits
// one download task per file to the shared pool. The body-slot
// acquire happens here on the submitter side rather than inside
// the pool task — pool workers must never block on per-call
// coordination (CLAUDE.md "Shared-pool workers must never block
// on per-call coordination"), or N waiting workers could starve
// every other Group sharing the pool, including unrelated
// Stores.
//
// Concurrency model:
//
//   - Submitter is one goroutine; acquireBodySlot blocks here
//     when the per-call slot pool is full, so the submitter
//     itself paces the pipeline.
//   - g.Submit blocks when the shared pool has no free slot.
//     Both back-pressure points run on the submitter, never
//     on the pool worker.
//   - In flight: at most min(bodyCap, pool.MaxConcurrent) S3
//     GETs at any time across this call's submitted tasks.
//
// All errors are strict: a NoSuchKey from S3 is treated as a
// hard error (the catalog points at it; missing means
// operator-driven prune or a torn write — incident-class). The
// task records hardErr and returns the wrapped error so the
// pool's errgroup cancels gctx — sibling tasks observe ctx.Done
// in s.target.get and bail.
//
// Body-slot leak on submit-skip is harmless. If gctx is
// cancelled between our acquireBodySlot and pool.Group.Submit's
// internal Acquire, the task fn never runs and the body slot
// stays held — but slotCh is per-call state torn down by
// downloadAndDecodeIter's deferred wg.Wait, and waitForPartition
// for files we never markComplete'd observes ctx.Done and
// returns false. No deadlock, no observable leak.
func (s *Store[T]) runDownloadSubmitter(
	ctx context.Context, g *pool.Group, state *streamState,
) {
	for pi := range state.parts {
		for fi := range state.parts[pi].files {
			if !state.acquireBodySlot(ctx) {
				// Set hardErr BEFORE markComplete so the
				// decoder, on observing this partition's done
				// channel close, sees the cancel signal and
				// bails before decoding a partition that may
				// have a mix of present and nil bodies (would
				// otherwise silently emit a partial
				// PartitionResult). Both calls go through the
				// same mu in streamState; close(p.done) happens
				// after the markComplete unlock, so a decoder
				// that observes p.done closed and then
				// mu.Locks for hardErr is guaranteed to see
				// the value we wrote here.
				//
				// recordHardErr is no-op if hardErr is already
				// set — preserving "first task error wins"
				// when an in-flight task errored before us.
				state.recordHardErr(ctx.Err())
				state.markComplete(pi, fi, nil)
				return
			}
			key := state.parts[pi].files[fi].s3Key
			g.Submit(ctx, func(ctx context.Context) error {
				body, err := s.target.get(ctx, key)
				if err != nil {
					// No body materialised — return the slot.
					state.releaseBodySlots(1)
					wrapped := fmt.Errorf("get %s: %w", key, err)
					state.recordHardErr(wrapped)
					state.markComplete(pi, fi, nil)
					return wrapped
				}
				// Slot stays held; decoder releases it when
				// bodies are nil'd in decodePartition.
				state.markComplete(pi, fi, body)
				return nil
			})
		}
	}
}

// runDecoder walks partitions in order; for each it waits on
// download completion, parses footers for the exact
// uncompressed total, gates on the byte budget, and decodes.
// Sends each completed partition's records — or any hard error
// captured anywhere in the pipeline — to decodedCh.
//
// Error precedence: streamState.hardErr() is the single source
// of truth for hard download errors. The decoder checks it at
// every partition boundary and on every cancel-aware exit
// (waitForPartition / reserveBytes returning false). Per-file
// errs in partState are not stored — the only signal a worker
// records is hardErr.
func (s *Store[T]) runDecoder(
	ctx context.Context, state *streamState,
	opts *readOpts, decodedCh chan<- decodedBatch[T],
) {
	defer close(decodedCh)
	for pi := range state.parts {
		// Surface decoder position to the observer so a stalled
		// pipeline's slog.Warn points at the right partition.
		state.decoderPi.Store(int64(pi))
		// Check before each partition: a worker on an already-
		// completed partition may have recorded a hard error
		// while we were decoding the previous one. Surface it
		// before touching pi.
		if err := state.hardErr(); err != nil {
			sendBatch(ctx, decodedCh, decodedBatch[T]{err: err})
			return
		}

		if !state.waitForPartition(ctx, pi) {
			// ctx fired before pi finished — either a worker
			// recorded a hard error and called cancel (forward
			// it) or the consumer cancelled (exit cleanly).
			if err := state.hardErr(); err != nil {
				sendBatch(ctx, decodedCh, decodedBatch[T]{err: err})
			}
			return
		}
		ps := state.parts[pi]

		// Re-check: a concurrent worker may have hit a hard
		// error during the wait. Without this check the decoder
		// would proceed to footerStats / decode of a partition
		// whose firstHardErr is set, and the consumer would
		// never see the real error.
		if err := state.hardErr(); err != nil {
			sendBatch(ctx, decodedCh, decodedBatch[T]{err: err})
			return
		}

		// Parse footers once: exact uncompressed total for the
		// byte budget AND total row count for pre-allocating
		// the decoded slice. Missing files (nil body) contribute
		// zero — but in s3pgstore the strict-error policy means
		// any nil body is paired with a non-nil hardErr, which
		// we already returned above. Defensive nil-skip here is
		// for the cancel path where the producer never reached
		// some files.
		uncomp, totalRows, err := footerStats(ps)
		if err != nil {
			sendBatch(ctx, decodedCh, decodedBatch[T]{err: err})
			return
		}

		// Gate on byte budget if configured. A single oversized
		// partition still flows once the buffer is empty —
		// otherwise the pipeline would deadlock.
		if !state.reserveBytes(ctx, uncomp, opts.readAheadBytes) {
			if err := state.hardErr(); err != nil {
				sendBatch(ctx, decodedCh, decodedBatch[T]{err: err})
			}
			return
		}

		decodeStart := time.Now()
		recs, err := s.decodePartition(state, ps, totalRows,
			opts.includeHistory)
		state.m.recordIterDecodeDuration(ctx, time.Since(decodeStart))
		// decodePartition nils each body + releases its
		// body-pool slot per-file; nothing else to clean up at
		// the partition level.
		if err != nil {
			state.releaseBytes(uncomp)
			sendBatch(ctx, decodedCh, decodedBatch[T]{err: err})
			return
		}

		if !sendBatch(ctx, decodedCh, decodedBatch[T]{
			partitionKey: ps.partitionKey,
			records:      recs,
			version:      ps.version,
			exts:         ps.exts,
			uncompBytes:  uncomp,
		}) {
			state.releaseBytes(uncomp)
			return
		}
	}
}

// decodePartition parses every successfully-downloaded body in
// ps, sort+dedup's the concatenated records, and returns the
// final slice. Each body is nil'd and its body-pool slot
// released as soon as the file is decoded, so the
// compressed-byte footprint inside a single partition's decode
// shrinks to ~one body instead of holding every file's
// compressed bytes for the full loop.
//
// The pre-dedup slice is pre-sized to totalRows (summed from
// row-group metadata in footerStats) so growth-doubling doesn't
// inflate the transient allocation peak. sortAndDedup compacts
// in-place and returns out[:n] — same backing array, length
// truncated to the survivor count.
func (s *Store[T]) decodePartition(
	state *streamState, ps *partState, totalRows int64,
	includeHistory bool,
) ([]T, error) {
	out := make([]T, 0, totalRows)
	for fi, body := range ps.bodies {
		if body == nil {
			continue
		}
		recs, err := decodeParquet[T](body)
		// Free the body and return its slot regardless of
		// decode outcome — we're done with it either way.
		ps.bodies[fi] = nil
		state.releaseBodySlots(1)
		if err != nil {
			return nil, fmt.Errorf(
				"decode %s: %w", ps.files[fi].s3Key, err)
		}
		out = append(out, recs...)
	}
	return sortAndDedup(out,
		s.resolved.EntityKeyOf,
		s.resolved.VersionOf, includeHistory), nil
}

// decodedBatch is one partition's decoded records (or a single
// hard error) flowing from the decoder to the emit loop.
//
// partitionKey is the partition the records came from (carried
// so partition-emitting public methods can surface it).
// records is already sort+dedup'd by decodePartition.
// version + exts are populated at preparePartitions time
// (catalog-derived, no parquet decode required).
// uncompBytes is what the decoder reserved; the emit loop
// returns it via releaseBytes after the records are forwarded.
type decodedBatch[T any] struct {
	partitionKey string
	records      []T
	version      int64
	exts         []FileExtensions
	uncompBytes  int64
	err          error
}

// sendBatch pushes a batch onto decodedCh, returning false on
// ctx cancellation so the caller can clean up the byte
// reservation it might have just made.
//
// Best-effort delivery: try the non-blocking send first.
// Without this the select below would race ctx.Done against a
// ready send, and Go's non-deterministic select could drop an
// error batch when the buffer has capacity AND ctx is already
// cancelled — a silent-drop hole on the strict-error path
// where the downloader's cancel() runs before the decoder's
// send arrives. Only fall back to the racing select when the
// buffer is full (consumer abandoned the iter and the deferred
// cancel keeps the pipeline from deadlocking).
func sendBatch[T any](
	ctx context.Context, decodedCh chan<- decodedBatch[T],
	b decodedBatch[T],
) bool {
	select {
	case decodedCh <- b:
		return true
	default:
	}
	select {
	case decodedCh <- b:
		return true
	case <-ctx.Done():
		return false
	}
}

// footerStats opens each non-nil body via parquet-go's footer
// parser and returns the partition's totals: uncompressed bytes
// (per-row-group total_byte_size, which the parquet spec
// defines as the total uncompressed size of all column data in
// the row group) and total row count. Metadata is parsed once
// per file (~10–100 KB of footer bytes); the body is already in
// memory so this is essentially free.
//
// The uncompressed total drives the byte-budget gate; the row
// count drives pre-sizing of the decoded slice so its growth
// doesn't double-allocate at decode time.
func footerStats(p *partState) (uncomp, totalRows int64, err error) {
	for fi, body := range p.bodies {
		if body == nil {
			continue
		}
		f, openErr := parquet.OpenFile(
			bytes.NewReader(body), int64(len(body)))
		if openErr != nil {
			return 0, 0, fmt.Errorf(
				"open %s: %w", p.files[fi].s3Key, openErr)
		}
		for _, rg := range f.Metadata().RowGroups {
			uncomp += rg.TotalByteSize
			totalRows += rg.NumRows
		}
	}
	return uncomp, totalRows, nil
}

// Stall watchdog defaults. Production downloadAndDecodeIter
// wires these via runDeadlockObserver; tests can override with
// shorter intervals so a stall signal fires within milliseconds
// rather than minutes (currently no test does so — left here
// as the upstream-vendored knob).
//
// stallTickInterval is generous: the observer is a background
// safety net, not a hot-path metric. A 30s tick on a stuck
// pipeline means at most 30s of latency between the stall
// starting and the first slog.Warn — operators see it long
// before SIGQUIT-debugging is needed.
//
// stallThreshold is double the tick: a single missed tick is
// not a stall (that would just be timing noise on a slow
// consumer that's about to pull the next batch). Two missed
// ticks signals a real lack of forward progress.
const (
	stallTickInterval = 30 * time.Second
	stallThreshold    = 60 * time.Second
)

// runDeadlockObserver is the iter pipeline's stall watchdog —
// pure observer, never cancels. Periodically wakes and emits a
// slog.Warn + s3pgstore.read.iter.stall.count increment if the
// pipeline made no forward progress (markComplete or
// releaseBodySlots) within the threshold window.
//
// The observer is a regression detector: the FIFO channel-based
// slot semaphore makes the cond+counter starvation deadlock
// unreachable, but a future change introducing a different
// stall would otherwise be silent. Operators set an alert on a
// non-zero rate of s3pgstore.read.iter.stall.count to surface
// deadlocks (real bugs) and slow consumers (heavy yield-side
// processing) — both worth seeing.
//
// Pure-observer rationale: auto-canceling on stall would mask
// information needed to diagnose the underlying issue
// (goroutine state for SIGQUIT, channel occupancy, decoder
// partition) and risk false-positive aborts of legitimately
// slow consumers. Users who want a hard ceiling pass
// ctx.WithTimeout at the call site — that propagates through
// every layer uniformly. A circuit-breaker at the iter layer is
// straightforward to add in caller code on top of this metric
// if a specific operational scenario needs it.
func (s *streamState) runDeadlockObserver(
	ctx context.Context, method string,
	tickInterval, threshold time.Duration,
) {
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	thresholdNs := threshold.Nanoseconds()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			last := s.lastProgressNs.Load()
			if last == 0 {
				// Pipeline hasn't logged any progress yet;
				// either the very-first download is still in
				// flight (a single GET that exceeds the
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
				"s3pgstore: iter pipeline made no forward progress within watchdog window",
				"method", method,
				"stale_seconds", time.Duration(staleNs).Seconds(),
				"decoder_partition", s.decoderPi.Load(),
				"slot_occupancy", len(s.slotCh),
				"slot_capacity", cap(s.slotCh))
			s.m.recordIterStall(ctx, method)
		}
	}
}

// selectFileRowsByRange runs the time-range catalog SELECT,
// filtered by feed_seq_at (unbounded sides default to
// open-ended). Rows come back ordered by partition_key, s3_key
// for the same group-then-decode pattern as the partition-
// filter SELECT.
//
// Bound semantics:
//   - since: feed_seq_at >= since (zero → unbounded below).
//   - until: feed_seq_at < until (zero → unbounded above; the
//     "live tip captured at call entry" property holds because
//     the catalog row count is monotonically non-decreasing in
//     v2.0 — files are never deleted).
//
// Rows with feed_seq IS NULL (not yet sequenced) are excluded
// so the result is stable under sequencer races.
func (s *Store[T]) selectFileRowsByRange(
	ctx context.Context, since, until time.Time,
) ([]fileRow, error) {
	cols := []string{
		"file_id", "partition_key", "s3_key", "written_at_version",
	}
	for _, c := range s.resolved.ExtensionColumns {
		cols = append(cols, "ext_"+c.Name)
	}
	where := []string{"feed_seq IS NOT NULL"}
	args := []any{}
	if !since.IsZero() {
		args = append(args, since.UTC())
		where = append(where,
			fmt.Sprintf("feed_seq_at >= $%d", len(args)))
	}
	if !until.IsZero() {
		args = append(args, until.UTC())
		where = append(where,
			fmt.Sprintf("feed_seq_at < $%d", len(args)))
	}
	q := fmt.Sprintf(
		`SELECT %s FROM %s
		WHERE %s
		ORDER BY partition_key, s3_key`,
		strings.Join(cols, ", "), s.names.Files(),
		strings.Join(where, " AND "))

	var out []fileRow
	err := s.cfg.Executor.Run(ctx, func(d DBTX) error {
		rows, err := d.Query(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			r := fileRow{
				extValues: make([]any, len(s.resolved.ExtensionColumns)),
			}
			scanArgs := []any{
				&r.fileID, &r.partitionKey,
				&r.s3Key, &r.writtenAtVersion,
			}
			for i := range r.extValues {
				scanArgs = append(scanArgs, &r.extValues[i])
			}
			if err := rows.Scan(scanArgs...); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("SELECT files (range): %w", err)
	}
	return out, nil
}

// validateEntries verifies every entry's DataPath lives under
// this Store's data prefix. Catches "entries from a different
// Store" before any S3 traffic — much cheaper to fail upfront
// than to discover via 404 mid-iteration.
//
// Cross-Store entries are an easy mistake to make (poll one
// Store, decode in another) and the error message points to
// the offending key.
func (s *Store[T]) validateEntries(entries []StreamEntry) error {
	dataPrefix := s.dataPath() + "/"
	for i, e := range entries {
		if !strings.HasPrefix(e.DataPath, dataPrefix) {
			return fmt.Errorf(
				"entries[%d].DataPath %q does not belong to "+
					"this Store (expected prefix %q)",
				i, e.DataPath, dataPrefix)
		}
	}
	return nil
}

// entriesToFileRows projects StreamEntry into the internal
// fileRow shape so the entries pipeline reuses the same
// preparePartitions / decode machinery as the catalog-driven
// paths.
//
// Extensions are decoded by name into the same positional slot
// as the SELECT path, leaving missing keys as nil. fileID and
// writtenAtVersion are not exposed via StreamEntry; iter
// callers reading via the entries path get a meaningful
// PartitionResult.Version only when the entries' s3_key lex
// order yields it (currently always 0 since StreamEntry
// doesn't carry written_at_version). Documented behavior.
func entriesToFileRows(
	entries []StreamEntry, extCols []ExtensionColumn,
) []fileRow {
	out := make([]fileRow, len(entries))
	for i, e := range entries {
		extValues := make([]any, len(extCols))
		for j, c := range extCols {
			if v, ok := e.Extensions[c.Name]; ok {
				extValues[j] = v
			}
		}
		out[i] = fileRow{
			partitionKey: e.Key,
			s3Key:        e.DataPath,
			extValues:    extValues,
		}
	}
	return out
}
