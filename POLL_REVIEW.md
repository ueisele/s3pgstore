# Poll pipeline review

Review of `poll.go` and supporting files (`iter_pipeline_shared.go`,
`poll_options.go`) after the recent refactor cycle that:

- Replaced `decodedPollBatch` with `FileResult`.
- Unified context-error filtering via `isCtxErr`.
- Routed all error surfacing through `pollFetchAndDecodeIter`'s
  return value (emit callbacks are success-only).
- Added `recoverInto` panic safety to the decoder closure and the
  pool task's GET callback.
- Added `seq2ToChan` / `sliceToChan` as input adapters and
  `pollIterDefaults` / `pollCollectDefaults` as per-mode defaulters.

## CLAUDE.md invariants — all preserved

- **Read stability / no library deletion** — Poll only SELECTs +
  decodes. No DELETE/UPDATE on catalog tables.
- **Stream replay stability** — `ORDER BY feed_seq` over half-open
  `[since, until)` is byte-identical across calls.
- **Half-open range convention** — `since` inclusive, `until`
  exclusive, `since == until` no-ops. PollIter's
  `pageEnd = min(cursor + pageSize, until)` correctly clips the
  last page.
- **Poll emits in feed_seq order, not lex partition** — Pipeline
  emit reads `workers[counter%W].queue` with monotonic counter;
  fetcher hands files to `workers[fi%W]`. Round-robin assignment
  + counter-mod-W reads = emit order equals fi order = feed_seq
  order from the catalog SELECT.
- **No DDL on runtime path** — Pure SELECTs throughout.
- **Shared-pool workers must never block on per-call coordination**
  — Fetcher acquires the body slot BEFORE `g.Submit`. Pool tasks
  only do GET + close(done) + bumpProgress; no slot/byte-budget
  waits inside the worker.
- **Same-pool reentrancy panics** — `ctx` passed to `g.Submit` is
  `gctx` (carries the marker). Pool tasks don't re-submit.
- **WithCancelCause for abort reason** — `state.ctx` is constructed
  via `WithCancelCause`. Both the fetcher's pool task and the
  decoder closure call `state.recordHardErr` BEFORE
  `close(fs.done)` / `close(queue)`, satisfying the load-bearing
  ordering rule (cause set atomically with the close, observable
  by waiters that select on ctx.Done).
- **Stall watchdog is observer-only** — `runDeadlockObserver` only
  emits `slog.Warn` + `recordIterStall`; never cancels.
- **No `cond.Broadcast` per-actor** — Body slot is a buffered
  channel (FIFO sendq); `releaseSig` is a single-waiter wake bell.

## Concurrency — termination in every path

The `pollFetchAndDecodeIter` shape (W+2 goroutines: 1 fetcher, W
decoders, 1 watchdog) terminates correctly:

| Trigger | Path |
|---|---|
| Clean EOS (fileRefCh closed) | fetcher loop exits → `g.Wait` drains pool tasks → defer closes worker inputs → decoders exit → defer closes queues → emit hits closed queue → return → deferred `wg.Wait` drains watchdog via cancel |
| Hard error (decode/GET) | `recordHardErr` cancels `state.ctx` → all stages observe `ctx.Done` natively (acquire, waitForFile, sendBatch, watchdog, emit's select) → drain via `wg.Wait` |
| Parent ctx cancel | propagates to `state.ctx` → same as hard error path; `context.Cause` returns the parent's error |
| Consumer break (emitOne returns false) | emit returns → defer cancels `state.ctx` → producers unblock and drain |

Defer ordering at the orchestration site is correct:
`retErr = context.Cause(state.ctx)` is sampled BEFORE
`cancel(context.Canceled)`, so cleanup-cancel doesn't shadow a
real abort cause. The documented trade-off ("errors that fire
strictly during wg.Wait drain are not surfaced") is acceptable.

Pool task cleanup (named `retErr` + recover idiom) correctly
handles the three terminal states (success / fn-returned-error /
panic). Slot release on success is correctly deferred to the
decoder (after `fs.body = nil`).

## Bugs

### 1. `decodeWorkers` defaulter accepts negative values

**Severity:** Low (not user-reachable today).

`WithDecodeWorkers` floors `n < 1` to 1, but `pollOpts.decodeWorkers`
is a plain `int` field and the defaulter only fills when `== 0`.
A negative value would survive and panic at
`make([]*pollWorker[T], n)`. Not exploitable via the public
options API today; harden defensively with `max(1, ...)` at the
make site if/when new internal callers appear.

### 2. Asymmetric panic safety vs read.go

**Severity:** Design observation, not a bug.

`recoverInto` guards the decoder closure and the pool task's GET
callback. The **fetcher closure** and **watchdog closure** have
no recover — a panic in either is fatal (`sync.WaitGroup.Go`
doesn't recover; the inner defer fires `wg.Done` but the panic
then propagates uncaught).

`read.go` doesn't recover anywhere either, so this is consistent
with the project pattern of "only recover where third-party code
can plausibly panic" (decodeParquet, target.get). Worth being
explicit that fetcher/watchdog panics are fatal, since the
`recoverInto` helper now exists and reads as a missing bookend.

## Memory / leaks — none found

- All goroutines accounted for via `wg`; deferred `wg.Wait` ensures
  no escape.
- Bridge goroutine in `seq2ToChan` is bounded by `<-ctx.Done()` in
  its send-select; wrapper's defer cancels innerCtx before
  `bridgeWait`, so the bridge always exits.
- `fs.body = nil` after decode (and on byte-budget reservation
  failure) — compressed bodies don't leak past their slot release.
- Body slots: pool task's deferred unified cleanup releases on
  err/panic; decoder releases on success after nil-out.
  `reserveBytes` release on error path before returning.
- Worker input channels: fetcher closure's defer closes them all
  (LIFO after `g.Wait`), so decoders always see a clean EOS even
  on partial drain.

## Smaller observations

- `inputCap := max(*opts.decodeAheadFiles, 1)` is computed and used
  on the next `make(chan *fileState, inputCap)` line.
- `state.slots.acquire(ctx) != nil` discards the error. That's the
  documented contract (state.ctx is already cancelled by whoever
  closed it).
- `reserveBytes` with `uncomp = fs.file.UncompressedSize` is correct
  per the catalog/parquet equivalence noted in the doc comment, and
  matches read.go's source.

## Summary

The pipeline is structurally sound; the recent panic-safety +
error-flow refactors hold up under tracing. CLAUDE.md invariants
are preserved. Remaining items are defensive nits; none block
landing.
