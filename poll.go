package s3pgstore

// poll.go is the read path for the sequenced feed: the public
// entry points (Poll / PollIter / PollRecords / PollRecordsIter
// / TailIter / TailRecordsIter / OffsetLatest / OffsetAt) and the
// chan-based file-grain fetch+decode pipeline
// (pollFetchAndDecodeIter and the per-file state machinery) that
// backs PollRecords / PollRecordsIter / TailRecordsIter.
//
// File layout mirrors read.go's outside-in shape:
//
//   1. Public methods (Poll, PollIter, PollRecords,
//      PollRecordsIter, TailIter, TailRecordsIter, OffsetLatest,
//      OffsetAt)
//   2. Per-call defaulters (pollIterDefaults, pollCollectDefaults,
//      tailIntervals)
//   3. Source adapters (bridgeBufferSize, sliceToChan, seq2ToChan,
//      isCtxErr)
//   4. Emit callbacks (pollIterEmit, pollCollectEmit)
//   5. Pipeline orchestration (pollFetchAndDecodeIter)
//   6. State types (fileState, pollState, pollWorker) + state
//      methods
//   7. Pipeline stages (runPollFetcher, runPollDecodeWorker)
//   8. Helpers (nextTailBackoff)
//
// Public iter shape:
//
//   - PollIter / TailIter yield FileRef and are the public
//     streaming-row APIs. PollIter walks a bounded half-open
//     [since, until) range with paged SQL underneath; TailIter
//     follows the feed forever from `since`, polling repeatedly
//     with exponential backoff between empty cycles. Both are
//     iter.Seq2[FileRef, error] — idiomatic Go stream APIs that
//     don't spawn extra goroutines for the basic case (the iter
//     body executes in the consumer's goroutine, the only blocking
//     work is the synchronous Poll call).
//
//   - PollRecords / PollRecordsIter / TailRecordsIter are the
//     decode-and-emit-records wrappers. They share one channel-
//     based pipeline (pollFetchAndDecodeIter). PollRecords feeds
//     it via sliceToChan (pre-filled channel from its resolved
//     entries slice; no goroutine). PollRecordsIter and
//     TailRecordsIter feed it via seq2ToChan (bridge goroutine
//     consuming PollIter / TailIter into a bounded channel; the
//     helper's wait func surfaces non-ctx source errors). Each
//     iter-mode wrapper derives innerCtx via WithCancelCause —
//     cancelled on pipeline error so the pipeline's stateCtx
//     observes the cause, and again in a defer so the bridge
//     unblocks before drain.
//
// The load-bearing primitives (body-slot semaphore, per-worker
// byte budget, stall observer, race-free batch send) live in
// iter_pipeline_shared.go and are reused verbatim from the read
// pipeline — see that file for the FIFO sendq + WithCancelCause
// rationale.

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

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

// defaultPollPageSize is the per-page row limit used by PollIter
// and TailIter when WithPollPageSize is not supplied.
//
// At ~120 bytes serialized per FileRef row, 10K rows is ~1 MB on
// the wire and ~3 MB in-memory after FileRef materialization —
// per-page query time is under 200ms on any reasonable PG +
// network, and per-page memory is well within any production
// host's budget. Each page is short enough that
// statement_timeout is not a practical concern and PgBouncer
// transaction-mode pooling can multiplex between pages.
//
// Smaller defaults (1K-5K) would lower first-row latency further
// for backfill-style consumers; larger (50K+) would amortize
// query overhead more but increase first-row latency and per-iter
// memory. 10K is a balanced middle ground; users with specific
// latency or memory budgets can tune via WithPollPageSize.
const defaultPollPageSize = 10000

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
//   - until: exclusive upper bound. Pass a concrete offset, or
//     use Store.OffsetLatest(ctx) to snapshot the current tip and
//     pass that. There is no "OffsetLatest sentinel" anymore —
//     the explicit snapshot keeps the snapshot point inspectable
//     by the caller and removes the "did this method capture the
//     tip implicitly?" question.
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
// (until - since). Poll is the page primitive used by PollIter /
// TailIter internally (each loops Poll over page-sized ranges)
// as well as the snapshot-then-decode entry point for users who
// want everything materialised in one slice.
//
// Filtering on the catalog is offset-only; consumers wanting
// per-file filtering (by partition, by extension columns)
// should filter the returned entries client-side and pass the
// surviving entries to ReadFileRefsIter for decoding.
func (s *Store[T]) Poll(
	ctx context.Context, since, until Offset,
) (out []FileRef, next Offset, err error) {
	defer s.metrics.methodScope(ctx, "Poll", &err).end()
	if since > until {
		return nil, since, fmt.Errorf(
			"Poll: since (%d) > until (%d)", since, until)
	}
	if since == until {
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
	q := fmt.Sprintf(
		`SELECT %s FROM %s
		WHERE feed_seq IS NOT NULL
		  AND feed_seq >= $1 AND feed_seq < $2
		ORDER BY feed_seq`,
		strings.Join(cols, ", "), s.names.Files())

	next = since
	err = s.cfg.Executor.Run(ctx, func(d DBTX) error {
		rows, err := d.Query(ctx, q, since, until)
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

// PollIter walks the half-open offset range [since, until) and
// yields each FileRef as it arrives. Internally paged — each
// iteration runs Poll over a window of WithPollPageSize rows,
// so connection-hold is short per page and PgBouncer transaction-
// mode + statement_timeout stay safe. Subsequent pages are
// fetched serially (no prefetch); the consumer drains the
// current page before the next SELECT.
//
// Compared to Poll (which materialises the entire range upfront),
// PollIter:
//   - Streams: the consumer sees the first row within ~one page
//     of latency, not after the whole range is materialised.
//   - Bounded-memory: O(pageSize) FileRefs resident at a time,
//     not O(until-since).
//
// Trade-off: PollIter does one SELECT per page (default 10K rows
// per page → 10 SELECTs for a 100K-row range). Per-query overhead
// is sub-millisecond on prepared-statement-cached connections, so
// this is invisible against the data scan cost.
//
// For unbounded follow consumption use TailIter — it keeps polling
// past the end of the available offsets, with exponential backoff
// between empty cycles. PollIter terminates as soon as the range
// is exhausted or an empty page is returned (no rows in
// [cursor, cursor+pageSize) means we've reached the catalog tip
// within [since, until)).
//
// Resume idiom: track `since = fr.Offset + 1` after each yield;
// restart with the new since on the next call.
func (s *Store[T]) PollIter(
	ctx context.Context, since, until Offset, opts ...PollOption,
) iter.Seq2[FileRef, error] {
	return func(yield func(FileRef, error) bool) {
		o := resolvePollOpts(opts)
		pageSize := o.pollPageSize
		if pageSize <= 0 {
			pageSize = defaultPollPageSize
		}

		cursor := since
		for cursor < until {
			pageEnd := cursor + Offset(pageSize)
			if pageEnd > until {
				pageEnd = until
			}
			page, next, err := s.Poll(ctx, cursor, pageEnd)
			if err != nil {
				yield(FileRef{}, err)
				return
			}
			if len(page) == 0 {
				// No rows in [cursor, pageEnd) — we've reached
				// the tip within the bounded range. Exit.
				return
			}
			for _, e := range page {
				if !yield(e, nil) {
					return
				}
			}
			cursor = next
		}
	}
}

// TailIter walks the feed forward from `since` and keeps going —
// it never returns on its own. Each yield is one FileRef in
// feed_seq order. When the catalog has no new files at the
// current cursor, the iterator blocks (with exponential backoff
// between empty polls, see WithTailIdleBackoff) until a new file
// commits or ctx is cancelled.
//
// Internally each polling cycle runs Poll over a window of
// WithPollPageSize rows. If a cycle returns a full page, the
// iterator immediately advances to the next page (catch-up mode,
// no backoff). If a cycle returns fewer than pageSize rows, the
// iterator yields them and then sleeps before polling again
// (steady-state tail mode).
//
// Termination conditions:
//   - ctx cancellation: the iterator returns silently. Caller-
//     driven shutdown matches range-over-channel semantics; no
//     error is yielded.
//   - Poll DB error: yielded as a (zero FileRef, err) pair, then
//     the iterator returns. Caller can retry from `since`.
//   - Caller breaks the range: iterator returns cleanly.
//
// Resume idiom: track `since = fr.Offset + 1` after each yield.
// On any exit, restart with the new since on the next call.
//
// Suitable for stream consumers, change-data-capture, monitoring,
// and any "watch the feed forever" use case. For bounded replay
// use PollIter; for one-shot snapshots use Poll.
func (s *Store[T]) TailIter(
	ctx context.Context, since Offset, opts ...PollOption,
) iter.Seq2[FileRef, error] {
	return func(yield func(FileRef, error) bool) {
		o := resolvePollOpts(opts)
		pageSize := o.pollPageSize
		if pageSize <= 0 {
			pageSize = defaultPollPageSize
		}
		base, maxInterval := tailIntervals(&o)

		cursor := since
		consecEmpty := 0
		for {
			page, _, err := s.Poll(ctx, cursor, cursor+Offset(pageSize))
			if err != nil {
				if !errors.Is(err, context.Canceled) &&
					!errors.Is(err, context.DeadlineExceeded) {
					yield(FileRef{}, err)
				}
				return
			}
			if len(page) == 0 {
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
			for _, e := range page {
				if !yield(e, nil) {
					return
				}
				cursor = e.Offset + 1
			}
			// If we got a full page, loop immediately (likely more
			// rows available). If partial, the next iteration will
			// observe an empty page and sleep.
		}
	}
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
	if perr := s.pollFetchAndDecodeIter(ctx, "PollRecords", &o,
		s.pollCollectDefaults(len(entries)),
		sliceToChan(entries),
		pollCollectEmit(&out)); perr != nil {
		return nil, since, perr
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
//   - WithPollPageSize controls the per-Poll-page row count.
//
// Suitable for bulk replay / drain workloads — the pipelined
// fetch + decode keeps network and CPU saturated while the
// consumer iterates.
//
// `until` is the exclusive upper bound. Pass a concrete offset,
// or use Store.OffsetLatest(ctx) to snapshot the current tip and
// pass that. Empty range (since == until) yields nothing
// without touching the database. Inverted range (since > until)
// yields a single error.
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

		o := resolvePollOpts(opts)
		innerCtx, cancel := context.WithCancelCause(ctx)
		fileRefCh, bridgeWait := seq2ToChan(innerCtx,
			s.PollIter(innerCtx, since, until, opts...),
			bridgeBufferSize(&o))
		defer func() {
			cancel(context.Canceled) // unblock bridge regardless of how we exit
			if err := bridgeWait(); err != nil && !isCtxErr(err) {
				iterErr = err
				yield(FileResult[T]{}, err)
			}
		}()

		if err := s.pollFetchAndDecodeIter(innerCtx, "PollRecordsIter", &o,
			pollIterDefaults, fileRefCh,
			pollIterEmit(yield)); err != nil && !isCtxErr(err) {
			iterErr = err
			yield(FileResult[T]{}, err)
		}
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
//   - ctx cancellation: the iterator returns silently. Caller-
//     driven shutdown is the stop signal for the iter, not an
//     error condition. Matches range-over-channel semantics.
//   - Pipeline error (S3 GET, decode): yielded as an error and
//     the iterator returns.
//   - Caller breaks the range: iterator returns cleanly; ctx is
//     cancelled internally so all pipeline goroutines drain.
//
// Memory is bounded the same way as PollRecordsIter — by the
// fetchAheadFiles body-slot semaphore, the per-worker decode
// queues, and WithPollPageSize for the per-cycle row count.
//
// Resume idiom: after a clean break or error, restart with
// `since = fr.File.Offset + 1` (where fr is the last successfully-
// yielded FileResult).
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
		innerCtx, cancel := context.WithCancelCause(ctx)
		fileRefCh, bridgeWait := seq2ToChan(innerCtx,
			s.TailIter(innerCtx, since, opts...),
			bridgeBufferSize(&o))
		defer func() {
			cancel(context.Canceled) // unblock bridge regardless of how we exit
			if err := bridgeWait(); err != nil && !isCtxErr(err) {
				iterErr = err
				yield(FileResult[T]{}, err)
			}
		}()

		if err := s.pollFetchAndDecodeIter(innerCtx, "TailRecordsIter", &o,
			pollIterDefaults, fileRefCh,
			pollIterEmit(yield)); err != nil && !isCtxErr(err) {
			iterErr = err
			yield(FileResult[T]{}, err)
		}
	}
}

// OffsetLatest returns one past the highest feed_seq currently
// assigned by the sequencer — the next offset that would be
// assigned. Use as the exclusive `until` bound to read everything
// committed at this exact moment:
//
//	tip, err := store.OffsetLatest(ctx)
//	if err != nil { return err }
//	for fr, err := range store.PollRecordsIter(ctx, since, tip) {
//	    …
//	}
//
// Returns OffsetEarliest (1) when no rows are sequenced yet —
// the lowest possible exclusive upper bound (no rows match in
// [any, 1)). This is the "empty catalog" sentinel.
//
// Snapshot at call time: the returned offset reflects what was
// committed when this query ran. Concurrent writers may commit
// new rows after the return; those are not visible via the
// returned bound. For continuous follow use TailRecordsIter /
// TailIter, which have no upper bound.
func (s *Store[T]) OffsetLatest(
	ctx context.Context,
) (out Offset, err error) {
	defer s.metrics.methodScope(ctx, "OffsetLatest", &err).end()
	q := fmt.Sprintf(
		`SELECT COALESCE(MAX(feed_seq), 0) FROM %s
		WHERE feed_seq IS NOT NULL`,
		s.names.Files())
	var maxSeq int64
	err = s.cfg.Executor.Run(ctx, func(d DBTX) error {
		return d.QueryRow(ctx, q).Scan(&maxSeq)
	})
	if err != nil {
		return OffsetEarliest, fmt.Errorf("OffsetLatest: %w", err)
	}
	return maxSeq + 1, nil
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
// (the file set is a stream from the bridge goroutine, not a
// slice), so defaults are file-count-independent.
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
// resolved (base, max) pair the tail loop uses to compute
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

// bridgeBufferSize returns the channel buffer between seq2ToChan's
// bridge goroutine and the pipeline's fetcher. Default: 2 ×
// fetchAheadFiles — enough rows queued for the fetcher to saturate
// every body slot without waiting on the bridge, plus a small slack.
//
// Memory cost is trivial: 2 × 64 × ~120 bytes ≈ 15 KB of channel
// storage at defaults. FileRef values flow by value here but the
// dominant memory cost (compressed bodies) is bounded separately
// via the body-slot semaphore.
func bridgeBufferSize(o *pollOpts) int {
	bufCap := 2 * o.fetchAheadFiles
	if bufCap <= 0 {
		bufCap = 2 * defaultWorkerPoolSize
	}
	if bufCap < 1 {
		bufCap = 1
	}
	return bufCap
}

// sliceToChan returns a buffered channel pre-filled with items
// then closed. No goroutine — the channel buffer holds the entire
// slice. Use for finite, already-materialised sources like
// PollRecords' resolved entries; for stream-shaped sources use
// seq2ToChan instead.
//
// Memory: cap(ch) == len(items), so the channel buffer doubles
// the FileRef footprint until the receiver drains. Acceptable
// for collect-mode callers that have already committed to
// len(items) of resident memory.
func sliceToChan[T any](items []T) <-chan T {
	ch := make(chan T, len(items))
	for _, item := range items {
		ch <- item
	}
	close(ch)
	return ch
}

// seq2ToChan spawns a bridge goroutine that consumes seq and
// pushes values into a buffered channel. Returns:
//
//   - ch: the buffered channel the bridge writes to. Closed when
//     the bridge exits (clean exhaustion, ctx cancel, or source
//     error).
//   - bridgeWait: blocks until the bridge goroutine has exited,
//     then returns whatever error the source iter yielded (if
//     any), or nil.
//
// No internal filtering — the helper is a pure adapter from
// iter.Seq2[T, error] to (<-chan T, errFn). Callers filter
// ctx-derived errors via isCtxErr at the yield site, keeping
// error policy in one place (the wrapper) rather than split
// across helpers.
//
// IMPORTANT: bridgeWait only blocks; it does NOT cancel ctx.
// Callers MUST cancel ctx (or otherwise ensure seq exhausts
// naturally) before calling bridgeWait — otherwise the bridge
// can deadlock on a buffer-full send if the consumer has stopped
// reading. The typical pattern is one deferred func doing
// cancel-then-wait-then-yield:
//
//	defer func() {
//	    cancel(context.Canceled)
//	    if err := bridgeWait(); err != nil && !isCtxErr(err) {
//	        yield(zero, err)
//	    }
//	}()
func seq2ToChan[T any](
	ctx context.Context, seq iter.Seq2[T, error], bufSize int,
) (ch <-chan T, bridgeWait func() error) {
	out := make(chan T, bufSize)
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		defer close(out)
		for v, itErr := range seq {
			if itErr != nil {
				err = itErr
				return
			}
			select {
			case out <- v:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, func() error {
		<-done
		return err
	}
}

// recoverInto recovers a panic from the deferred goroutine and
// surfaces it as a hard pipeline error so shutdown flows through
// the same path as any other error. Stack capture aids
// post-mortem; first-cancel-wins on stateCtx keeps recurring
// panics from clobbering the root cause.
//
// MUST be used as `defer recoverInto(state, name)` — recover()
// only works when called directly by a deferred function, not
// from a function called by one.
func recoverInto(state *pollState, name string) {
	if r := recover(); r != nil {
		state.recordHardErr(fmt.Errorf("%s panic: %v\n%s", name, r, debug.Stack()))
	}
}

// isCtxErr reports whether err is (or wraps) a context
// cancellation or deadline. Used uniformly at every error-
// surfacing decision in the poll pipeline:
//
//   - At recordHardErr call sites (decoder closure, fetcher pool
//     task): skip the call when err is ctx-derived. recordHardErr
//     is first-cancel-wins, so the call would be a no-op when ctx
//     is already cancelled, but the explicit filter expresses
//     intent ("ctx cancellation isn't a hard pipeline error").
//   - At iter wrapper yield sites (bridge defer, pipeline call):
//     suppress the yield. Ctx cancellation is the iter consumer's
//     stop signal, not an error value worth surfacing through
//     range-over-iter.
//
// PollRecords (collect mode) deliberately does NOT use this
// filter on its pipeline-return path — it surfaces ctx errors
// directly, matching Go's sync-API convention.
func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// pollIterEmit returns the per-batch emit callback that yields
// one FileResult[T] per file. Used by PollRecordsIter and
// TailRecordsIter — both surface results via iter.Seq2 yield.
//
// Success-only: errors flow via pollFetchAndDecodeIter's return
// value, not through this callback. The wrapper handles error
// surfacing (yield + iterErr) explicitly. Returning false from
// yield (consumer broke the for-range) propagates back as false
// here, terminating the emit loop cleanly.
func pollIterEmit[T any](
	yield func(FileResult[T], error) bool,
) func(FileResult[T]) bool {
	return func(fr FileResult[T]) bool {
		return yield(fr, nil)
	}
}

// pollCollectEmit returns the per-batch emit callback that
// appends each FileResult into the *out slice. Used by
// PollRecords (collect). Always returns true — collect mode
// has no early-exit signal; errors flow via the pipeline's
// return value.
func pollCollectEmit[T any](
	out *[]FileResult[T],
) func(FileResult[T]) bool {
	return func(fr FileResult[T]) bool {
		*out = append(*out, fr)
		return true
	}
}

// pollFetchAndDecodeIter is the chan-based streaming pipeline
// backing PollRecords (collect via callback), PollRecordsIter
// (bounded stream via callback), and TailRecordsIter (unbounded
// stream via callback). Three concurrent stages plus the caller's
// emit loop:
//
//  1. Fetcher goroutine: reads FileRefs from fileRefCh in input
//     order, acquires one body-pool slot per file, submits one
//     download task per file to the Store's shared *pool.Pool,
//     then fans the *fileState out to the assigned worker's
//     input channel (workers[fi%W].input). Same-pool reentrancy
//     isn't an issue here — pool tasks do GET + close(done) only.
//     The wg.Go closure wrapping the fetcher owns close(w.input)
//     for every worker (deferred so it runs after g.Wait drains
//     in-flight downloads).
//
//  2. Decode workers (W goroutines): each ranges over its own
//     input channel; for each *fileState waits for the file's
//     download to complete (fs.done), gates on per-worker
//     budget (decodeAheadFiles, decodeAheadBytes) using the
//     catalog's UncompressedSize, decodes into T, and sends a
//     success FileResult to its queue. On hard decode error the
//     worker returns the error; the wg.Go closure wrapping the
//     worker calls state.recordHardErr (so stateCtx's cause is
//     set BEFORE the deferred close(queue) — pollFetchAndDecodeIter's
//     defer reads context.Cause and surfaces it via the return).
//
//  3. Emit loop (this goroutine): walks a running counter,
//     reads from workers[counter%W].queue, hands each success
//     batch to emitOne (slice append for collect; yield for
//     iter), and frees the worker's reserved bytes on completion.
//     Exits when emitOne returns false (consumer broke), when
//     the assigned worker's queue closes (EOS), or when
//     state.ctx fires (cancel/hard error).
//
// Input shape: <-chan FileRef. The caller (PollRecords /
// PollRecordsIter / TailRecordsIter) is responsible for
// producing FileRefs into the channel and closing it on EOS or
// ctx cancellation. Each wrapper has its own bridge (sliceToChan
// for the slice case, seq2ToChan for the iter cases) for this;
// the pipeline doesn't know which source it has.
//
// Error reporting: single error path via the return value.
// emitOne is success-only — never called with errors. The
// returned error is one of:
//
//   - nil: clean shutdown (EOS reached, consumer broke).
//   - real err: a hard pipeline failure recorded via
//     recordHardErr.
//   - context.Canceled / context.DeadlineExceeded: parent ctx
//     cancellation propagated via context.Cause.
//
// The wrapper decides what to do with the err — PollRecords
// returns it directly; iter wrappers filter ctx-derived errors
// before yielding (consumer's stop signal isn't a yield-worthy
// error).
//
// Internal coordination: derives stateCtx from ctx via
// WithCancelCause. recordHardErr (called by the fetcher's pool
// task on GET failure, and by the decoder's wg.Go wrapper on
// any decode failure) cancels stateCtx with the wrapped err as
// cause. The defer reads context.Cause BEFORE its own cleanup
// cancel (otherwise the cleanup cancel would set cause to
// context.Canceled and shadow real errors). wg.Wait drains all
// internal goroutines before returning. Errors that fire
// strictly during wg.Wait drain (after the cause snapshot) are
// not surfaced — acceptable trade-off for the simpler defer.
func (s *Store[T]) pollFetchAndDecodeIter(
	ctx context.Context, method string,
	opts *pollOpts, applyDefaults func(*pollOpts),
	fileRefCh <-chan FileRef,
	emitOne func(FileResult[T]) bool,
) (retErr error) {
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
		slots:  newBodySlots(bodyCap, s.metrics),
	}

	// Per-worker structures: each owns an input channel
	// (fetcher fans out to workers[fi%W].input) and the shared
	// byte-budget machinery (queue + reserveBytes/releaseBytes).
	workers := make([]*pollWorker[T], opts.decodeWorkers)
	inputCap := max(*opts.decodeAheadFiles, 1)
	for w := range workers {
		workers[w] = &pollWorker[T]{
			input:       make(chan *fileState, inputCap),
			workerState: newWorkerState[FileResult[T]](*opts.decodeAheadFiles, s.metrics),
		}
	}

	var wg sync.WaitGroup
	defer func() {
		// Snapshot the cause BEFORE our cleanup cancel — otherwise
		// cancel(context.Canceled) sets cause=context.Canceled on
		// clean shutdown and shadows real errors. Errors that fire
		// strictly during wg.Wait drain are not surfaced (the
		// snapshot has already been taken); accepted as a trade-off
		// for the simpler defer.
		retErr = context.Cause(state.ctx)
		cancel(context.Canceled)
		wg.Wait()
	}()

	// Stage 1: fetcher. Reads from fileRefCh, acquires body
	// slots, submits per-file downloads to the shared pool, and
	// fans *fileState out to workers[fi%W].input. The closure
	// owns close(w.input) for every worker (deferred after
	// g.Wait so in-flight pool tasks complete first).
	g, gctx := s.resolved.WorkerPool.WithContext(state.ctx)
	wg.Go(func() {
		defer func() {
			for _, w := range workers {
				close(w.input)
			}
		}()
		s.runPollFetcher(gctx, g, state, workers, fileRefCh)
		_ = g.Wait()
	})

	// Stage 2: decode workers. Each drains its own input,
	// decodes, sends success batches to its queue. On error the
	// worker returns the error; the closure calls recordHardErr
	// (filtered via isCtxErr) BEFORE the deferred close on the
	// queue — so the defer's context.Cause snapshot sees the
	// cause if a real error occurred. recoverInto guards against
	// decodeParquet panicking on malformed parquet bytes — a
	// single bad file shouldn't crash the process.
	for w := range workers {
		wg.Go(func() {
			defer recoverInto(state, "decoder")
			defer close(workers[w].queue)
			if err := s.runPollDecodeWorker(state.ctx, state, workers[w], opts); err != nil && !isCtxErr(err) {
				state.recordHardErr(err)
			}
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

	// Stage 3: emit loop. Walks a running counter; for each
	// counter value, reads from workers[counter%W].queue. Exits
	// when emitOne returns false (consumer broke), the assigned
	// worker's queue closes (clean EOS or hard error), or
	// state.ctx fires (cancel/error). Errors are not passed to
	// emitOne — they flow back to the caller via the deferred
	// context.Cause read on the return value.
	for counter := 0; ; counter++ {
		state.decoderFi.Store(int64(counter))
		ws := workers[counter%opts.decodeWorkers]
		select {
		case batch, ok := <-ws.queue:
			if !ok {
				return
			}
			cont := emitOne(batch)
			ws.releaseBytes(batch.File.UncompressedSize)
			if !cont {
				return
			}
		case <-state.ctx.Done():
			return
		}
	}
}

// fileState holds per-file download progress. file and fi are
// fixed at construction time; body is mutated by the pool task
// before close(done) and read by the worker after.
//
// fi is the running file index assigned by the fetcher as it
// reads from fileRefCh. Drives the round-robin worker assignment
// (fi % W) — must be monotonically increasing across the
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

// pollState coordinates the fetcher, decode workers, and emit
// loop. ctx + cancel mirror read.go's readState — single
// cancellation source via WithCancelCause; recordHardErr cancels
// with the abort reason as cause atomically with ctx.Done close.
//
// No state.files channel anymore — the input file stream is
// supplied by the caller as a fileRefCh parameter; the fetcher
// reads from it directly.
//
// slots is the body-slot semaphore — see iter_pipeline_shared.go
// for FIFO sendq + watchdog progress-bump rationale.
type pollState struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

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
	*workerState[FileResult[T]]
}

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

// runPollFetcher reads FileRefs from fileRefCh in fi order,
// acquires a body slot per file, submits one download task per
// file to the shared pool, then hands the *fileState to its
// assigned worker via workers[fi%W].input. Body-slot acquire
// happens on the fetcher (not the pool worker) — see CLAUDE.md
// "Shared-pool workers must never block on per-call coordination."
//
// Returns on clean EOS (fileRefCh closed) or ctx cancel. Channel
// cleanup (close(w.input) for every worker) is owned by the
// wg.Go closure wrapping this function so the fetcher itself
// stays a pure router.
//
// Download task: GET → set fs.body, close(fs.done), bump
// progress. On GET failure: release slot, recordHardErr (cancels
// state.ctx with the wrapped err as cause), close(fs.done).
// Order matters — cause is set before done closes so the
// decoder's waitForFile observes both atomically. recordHardErr
// fires inline on the pool task (not via g.Wait) so decoders
// see the cancel mid-flight rather than waiting for in-flight
// GETs to finish.
func (s *Store[T]) runPollFetcher(
	ctx context.Context, g *pool.Group, state *pollState,
	workers []*pollWorker[T], fileRefCh <-chan FileRef,
) {
	fi := 0
	for {
		var fr FileRef
		select {
		case f, ok := <-fileRefCh:
			if !ok {
				return
			}
			fr = f
		case <-ctx.Done():
			return
		}
		if state.slots.acquire(ctx) != nil {
			return
		}
		fs := &fileState{
			fi:   fi,
			file: fr,
			done: make(chan struct{}),
		}
		fi++
		key := fr.S3Key
		g.Submit(ctx, func(ctx context.Context) (retErr error) {
			// Unified cleanup for success, error, AND panic.
			// recover() must be called directly in this deferred
			// func — a helper can't substitute. On panic OR err
			// return: release the slot, record the cause, then
			// close(fs.done) last so the decoder unblocking from
			// waitForFile observes context.Cause already set. On
			// success: slot stays held (decoder releases after
			// nil-out), no record, just close.
			defer func() {
				if r := recover(); r != nil {
					retErr = fmt.Errorf("get %s panic: %v\n%s", key, r, debug.Stack())
				}
				if retErr != nil {
					state.slots.release()
					if !isCtxErr(retErr) {
						state.recordHardErr(retErr)
					}
				}
				close(fs.done)
			}()
			body, err := s.target.get(ctx, key)
			if err != nil {
				return fmt.Errorf("get %s: %w", key, err)
			}
			// Slot stays held; decoder releases after nil-out.
			fs.body = body
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
// reserves byte budget against the catalog's recorded
// uncompressed size, decodes, and publishes the success
// FileResult to ws.queue.
//
// Returns:
//   - nil on clean EOS (input channel closed) or ctx cancel.
//   - A wrapped error on hard decode failure (byte budget,
//     decode). The wg.Go closure wrapping this function calls
//     state.recordHardErr on non-ctx errors BEFORE the deferred
//     close(queue), so the pipeline's defer-snapshot of
//     context.Cause observes the cause.
//
// Byte budget source: fs.file.UncompressedSize from the catalog
// (loaded by Poll's SELECT). This is the same metric the writer
// stored (sum of column-chunk TotalUncompressedSize), equal by
// parquet spec to the row-group TotalByteSize sum. Avoids a
// per-file footer parse and keeps the budget exact.
//
// The outer loop is a select on ctx.Done + input rather than
// `for fs := range w.input` so the worker observes ctx
// natively, per CLAUDE.md "Every blocking primitive must
// observe ctx.Done() natively." Files in flight when ctx fires
// are discarded cleanly: the wrapping wg.Go closure's deferred
// close(w.input) still fires (no panic — only that closure
// closes), and any *fileState left in the input buffer is
// unreferenced and GC-eligible.
func (s *Store[T]) runPollDecodeWorker(
	ctx context.Context, state *pollState,
	w *pollWorker[T], opts *pollOpts,
) error {
	for {
		var fs *fileState
		select {
		case f, ok := <-w.input:
			if !ok {
				return nil
			}
			fs = f
		case <-ctx.Done():
			return nil
		}
		if err := waitForFile(ctx, fs); err != nil {
			// Ctx-derived (parent cancel) or the fetcher's
			// wrapped GET err — both are already routed via
			// state.ctx's cause, so just propagate.
			return err
		}

		// Use the catalog's uncompressed_size for the byte
		// budget (already loaded into fs.file.UncompressedSize
		// by Poll's SELECT). Identical to the footer's row-
		// group TotalByteSize sum; no footer parse needed.
		uncomp := fs.file.UncompressedSize
		if err := w.reserveBytes(ctx, uncomp, opts.decodeAheadBytes); err != nil {
			state.slots.release()
			fs.body = nil
			return err
		}

		decodeStart := time.Now()
		recs, err := decodeParquet[T](fs.body)
		s.metrics.recordIterDecodeDuration(ctx, time.Since(decodeStart))
		// Free the body and slot regardless of decode outcome.
		fs.body = nil
		state.slots.release()
		if err != nil {
			w.releaseBytes(uncomp)
			return fmt.Errorf("decode %s: %w", fs.file.S3Key, err)
		}

		if !sendBatch(ctx, w.queue, FileResult[T]{
			File:    fs.file,
			Records: recs,
		}) {
			// Ctx cancelled mid-send; emit has already exited
			// (or is about to). Release the reservation we just
			// took and return without surfacing — the cancel
			// cause flows through ctx, not through here.
			w.releaseBytes(uncomp)
			return nil
		}
	}
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
