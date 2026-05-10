# Correctness invariants

These are contracts the library makes to its users. Refactors
must preserve them — even when the change appears unrelated.

- **Atomic visibility on commit** — A successful Write is visible
  to every subsequent Read / ReadIter / MaterializedView.Lookup the
  moment its PostgreSQL transaction commits. There is no settle
  window, no propagation delay, no marker-gate. Refactors must not
  introduce visibility gates outside the catalog transaction or
  read paths that bypass it.
- **Orphan tracking via `s3pgstore_pending_writes`** — The S3 PUT
  side-effect cannot be undone by a transaction rollback. The
  pending-writes row is INSERTed via the `insertPendingWrite`
  helper, which calls `Executor.Run(Executor.DetachTx(ctx), …)`
  on a fresh pool connection, so it commits independently of both
  the wrapping catalog tx and any caller-supplied tx (`WithTx`).
  The matching DELETE runs inside the catalog tx, so a rollback
  of that tx restores the orphan record.

  The pre-tx INSERT lands *before* the wrapping tx is begun —
  not inside its callback. This is load-bearing for concurrency:
  each writer holds at most one pool connection at a time
  (pre-tx connection released before the wrapping-tx connection
  is acquired). Relocating the INSERT inside the wrapping tx
  would make every writer hold two connections simultaneously,
  deadlocking the pool whenever concurrency approaches its size
  (verified empirically on `TestSequencer_GapFreeUnderConcurrentWriters`).

  The catalog tx wraps the S3 PUT: UPSERT partitions → encode →
  INSERT files → PUT → DELETE pending_writes → INSERT MV →
  NOTIFY → COMMIT. The file row INSERT precedes the PUT so a
  token-race unique-violation aborts before any S3 write. On
  rollback after a successful PUT (PUT succeeded but COMMIT
  crashed, or any later step failed), the S3 file exists with
  no catalog reference and the pre-tx pending_writes row points
  at it; `cmd/s3pgstore-gc` reclaims the orphan after a grace
  period. Fail-fast paths (OCC, token race, encode error) leave
  a pending_writes row pointing at an s3_key that was never PUT;
  GC's DELETE on that key is a no-op, so the cost is one GC
  cycle per fail-fast event.

  Refactors must not move the pending-writes DELETE outside the
  catalog tx, must not commit the catalog row before the PUT
  completes, must not let the pending-writes INSERT participate
  in a caller-supplied tx (always go through `Executor.DetachTx`),
  and must not relocate the pending-writes INSERT inside the
  wrapping tx — see the pool-deadlock note above.
- **Read stability — no library-driven deletion** — Two consecutive
  reads with no intervening writes return the same records. The
  library never deletes catalog rows, MV rows, or S3 objects on
  its own. Cleanup is operator-driven (`cmd/s3pgstore-gc` for
  orphans only; S3 lifecycle for cold storage). Refactors must
  not introduce automatic GC, age-based pruning, or in-Write
  cleanup of failed-Write artifacts.
- **Stream replay stability via the sequencer** — Once a record
  has been observed at offset N by Poll / PollRecords, that record
  at offset N never changes; replay from offset 0 produces the
  same sequence every time. Two properties combine to back this:

  1. The sequencer holds `pg_advisory_xact_lock` over a fixed
     key; only one sequencer instance assigns `feed_seq` at a
     time.
  2. The sequencer only touches rows whose transaction has
     already committed (`feed_seq IS NULL` after commit), so no
     writer can commit a row "behind" an already-assigned offset.

  Refactors must not weaken the sequencer's serialization (no
  parallel sequencers, no skip-and-fill heuristics), must not
  allow `feed_seq` to be reassigned or made nullable after assignment,
  and must not advance `feed_seq` from inside a writer's transaction.
- **Idempotency is unbounded in time** — A retry with the same
  `(partition_key, idempotency_token)` returns the original
  WriteResult, regardless of how much time has passed. Implemented
  via partial UNIQUE index `(partition_key, idempotency_token)
  WHERE idempotency_token IS NOT NULL`. Refactors must not
  reintroduce a `maxRetryAge`-style bound — that would silently
  fall through to a duplicate write after expiry.
- **OCC at the partition grain via row-level CAS** —
  `WithExpectedVersion` translates to a single
  `UPDATE s3pgstore_partitions SET version = version + 1
  WHERE partition_key = $ AND version = $expected`. The CAS is
  serialized by PostgreSQL row-level locking; the version check
  and increment happen atomically. Refactors must not split the
  check from the increment, or read the version and compare in
  application code.
- **Read returns the complete file set per matched partition** —
  The Read query has no per-file filtering and no LIMIT. For every
  matched partition, every file row comes back. This is
  load-bearing for the version-derivation invariant below: `MAX`
  over the returned files is meaningful only if no files are
  missing. Refactors must not introduce per-file filters,
  pagination, or sampling on the Read path. (`ReadIter` may stream
  rows lazily, but it still emits every file.)
- **`MAX(written_at_version)` per partition equals the partition's
  current version** — Every Write atomically bumps
  `s3pgstore_partitions.version` from N to N+1 *and* inserts a
  file row with `written_at_version = N+1`, in one transaction.
  No path bumps the version without inserting a file. No path
  inserts a file without bumping the version. No file is ever
  deleted in v2.0. Therefore `MAX(written_at_version)` over a
  partition's files equals `s3pgstore_partitions.version` exactly,
  by construction. The Read path derives `PartitionResult.Version`
  from this `MAX` rather than querying the partitions table —
  fewer queries, no REPEATABLE READ needed. Refactors that
  introduce a write path bumping version without a file (or
  inserting a file without a version bump) break this invariant
  and the Read path's version-derivation. v2.1 compaction will
  preserve the invariant: every catalog write that changes a
  partition's live state must bump version and insert a file row
  carrying that version.
- **Read runs under READ COMMITTED, no explicit transaction** —
  The Read path issues a single SELECT and lets PostgreSQL apply a
  statement-level snapshot. OCC's `WHERE version = $expected` on
  the Write CAS is the source of truth for write-write race
  detection; the (Version, Records) pair returned to the caller is
  consistent under the construction invariants above. Refactors
  must not silently introduce a transaction wrapper on the Read
  path (callers needing snapshot semantics for the pair wrap their
  own `Executor.RunInTx`).
- **Transactional composition via Executor** — The library calls
  `Config.Executor.RunInTx(ctx, fn)` for every multi-statement
  catalog write. If ctx carries an active transaction, fn
  participates in it (no nested BEGIN). If not, the executor opens
  its own. A successful Write commits if and only if the host
  transaction commits. The orphan-tracking pending_writes INSERT
  is the one deliberate exception: it goes through
  `Executor.Run(Executor.DetachTx(ctx), …)` so it always commits
  on its own pool connection, even when the caller composed via
  `WithTx` — see the orphan-tracking invariant for why. Refactors
  must not bypass the Executor for catalog writes (no direct
  `pool.Begin`, no `pgxpool.Pool.Exec` on a catalog table), must
  not assume the executor always opens its own tx, and must not
  remove `DetachTx` from the Executor interface — every adapter
  needs an escape hatch for orphan-tracking-class writes.
- **MaterializedView is a set-membership index, no FK to
  s3pgstore_files** — MV rows hold the column tuple declared as
  `Config.MaterializedViews[i].Columns`; the primary key is the
  full tuple. Conflict policy is uniform: `ON CONFLICT (...) DO
  NOTHING` — writing the same tuple twice is idempotent; writing
  a tuple that differs in any column is a new row. MV row
  insertion happens in the same transaction as the catalog row,
  so consistency is automatic.

  Two semantic consequences operators rely on:

  1. **No false negatives.** Once a record is written that emits
     a tuple, that tuple is in the MV until an operator deletes
     it. The MV faithfully records every column-tuple ever
     observed.
  2. **False positives possible.** A tuple logically superseded
     by a later record (per `EntityKeyOf+VersionOf` dedup on the
     Read path) still has its MV row. Lookup returns the
     historical set, not the current state.

  Operators wanting "current value per key" go through the Read
  path, which dedups records via `VersionOf`. The MV is the
  existence index; Read is the authoritative current state.

  Refactors must not add a FK to `file_id`, must not change the
  conflict policy from `DO NOTHING` (last-write-wins semantics
  silently lose data under concurrent writers without OCC, and
  reintroduce the MVCC-churn / out-of-order overwrite hazards
  the all-PK design eliminates), and must not insert MV rows in
  a separate transaction from the catalog write.
- **ExtensionColumns are typed and declared up-front** —
  `Config.ExtensionColumns` declares each column's name and SQL
  type. `WithMetadata` validates that every key is declared and
  that the value's Go type matches the declared SQL type. Unknown
  keys are an error at call time, not a silent fallback. Refactors
  must not reintroduce JSONB metadata storage or untyped-key
  tolerance, and must not promote arbitrary `WithMetadata` keys to
  catalog columns at runtime.
- **No DDL on the runtime path** — The library validates schema
  at `New()` via read-only `information_schema` queries. It never
  creates, alters, or drops tables outside `SchemaManager.{Create,
  Drop}`, which exist explicitly for tests and small deployments.
  Production schema changes go through the operator's migration
  tool. Refactors must not introduce `CREATE TABLE IF NOT EXISTS`,
  `ALTER TABLE`, or similar self-healing DDL into `New()` or any
  read/write path.
- **VersionOf is record-only; never derived from storage timing**
  — The signature is `func(T) int64`. The closure must derive its
  value from the record itself (business timestamp, monotonic
  counter assigned before the write, etc.), not from any
  storage-side stamp. Storage timing doesn't correspond to logical
  update order — a worker that started later but completed faster
  gets a "later" stamp than one that started earlier, silently
  losing the earlier worker's update on dedup. Refactors must not
  expose storage time to VersionOf or compute version from
  storage time anywhere on the write or read path.
- **Per-partition dedup, not global** — Read-time dedup via
  `EntityKeyOf + VersionOf` runs within one partition at a time.
  `EntityKeyOf` must be fully determined by the partition key so
  no entity ever spans partitions; otherwise an entity surfaces
  with a separate "latest" pick per partition. Refactors must not
  introduce a global-dedup escape hatch (the union-slice memory
  cost is real, and per-partition dedup is correctness-equivalent
  under the precondition).
- **`PartitionKeyOf` is deterministic and stable** — Same record
  in equals same partition key out, every time, across processes
  and library versions. The library writes the materialized
  partition_key into the catalog and the per-part columns
  (`part_<n>`) derived from it; subsequent reads filter on those
  columns. A non-deterministic `PartitionKeyOf` produces records
  that land in different partitions on different runs, with no
  visible duplicate-detection at write time. Refactors must not
  introduce time, randomness, or process-local state into the
  `PartitionKeyOf` derivation path.
- **Deterministic parquet encoding** — Same records + same
  compression codec produce byte-identical parquet bytes.
  `WithIdempotencyToken` retries depend on this for the
  unlikely-but-possible case where a retry's S3 PUT lands before
  the original transaction commits or rolls back, leaving two
  per-attempt parquets that the catalog row points at one of.
  Refactors must not introduce non-determinism into parquet
  encoding (random row ordering, timestamp injection beyond
  `InsertedAtField`). The pooled-encoder path
  (`pqWriterPool` + `encodeBufPool`) must produce
  byte-identical bytes to a fresh non-pooled encode — this is a
  load-bearing test, not a perf optimization (see Verification §
  Benchmarks).
- **Deterministic emission order across read and write paths**
  — every read path (`Read` / `ReadIter` / `ReadPartitionIter` /
  `ReadRangeIter` / `ReadPartitionRangeIter` / `ReadEntriesIter`
  / `ReadPartitionEntriesIter` / `PollRecords`) and the
  write-side partition grouping emit partitions in **lex order
  of the partition-key string**, with per-partition records in
  **`(entity, version)` ascending order** when dedup is
  configured (decode/insertion order without it). Same input on
  the same data produces byte-identical output every time.
  Consumers may rely on this for diffing, hashing, replay
  equality, and golden-file tests.

  The load-bearing pieces (named once they exist):

  1. The catalog SELECT on the read path uses `ORDER BY
     partition_key, feed_seq` (or the equivalent for non-feed
     queries) — collapses Go's randomized map iteration to
     deterministic lex order before files are grouped.
  2. Per-partition file ordering by `s3_key` — deterministic
     decode order within a partition.
  3. `sortAndDedup` (vendored from s3store) runs a stable
     `(entity, version)` sort followed by in-place dedup —
     deterministic record order within a partition's output.
  4. The write-side `GroupByPartition` sorts partition keys
     before emitting — same map-iteration-collapse pattern.

  Refactors must not introduce non-deterministic partition
  iteration, parallel-decode pipelines that race batch sends
  out of order, or non-stable record sorts inside a partition.
  `PollRecords` consumers needing wall-clock ordering across
  partitions must re-sort by their own timestamp field — its
  next-offset advancement (the "don't miss records" property)
  is unaffected.

# Concurrency invariants

These bound how the iter pipeline (Phase 12; vendored from
s3store) and any future multi-goroutine pipeline must
coordinate. Properties recent regressions in s3store taught us
— preserve them on every refactor.

- **No `cond.Broadcast` with per-actor predicates.** When N
  actors wait on a condition that depends on each actor's
  individual progress (e.g., "did *my* slot land"), use a
  buffered channel as a FIFO semaphore — Go's runtime drains a
  channel's sendq strictly FIFO on every receive. The earlier
  cond+counter design in s3store's iter pipeline allowed
  scheduler-biased starvation: `Broadcast` wakes everyone, all
  waiters race for the mutex, and the scheduler can consistently
  pick the same winner, leaving one specific actor's predicate
  permanently unsatisfied and deadlocking the pipeline. This is
  not theoretical — it was reproduced deterministically on a
  1417-partition cost read (s3store commit `da75ca9`).
  `sync.Cond` is acceptable only when there is exactly one
  waiter AND the predicate is global (not per-actor) AND a
  chan-based wake bell would be measurably worse; in practice
  that combination is rare enough that channels should be the
  default.

- **Every blocking primitive must observe `ctx.Done()`
  natively.** A primitive that requires a sibling
  broadcast-on-cancel goroutine to unblock is a code smell — the
  workaround adds goroutine count, lifecycle complexity, and a
  separate synchronisation site that can drift out of sync with
  the primitive it protects. Channel-based primitives (`<-ch`,
  buffered semaphores, wake bells) `select` on `ctx.Done()` for
  free. Refactors must not reintroduce primitives that need a
  cancel-broadcast helper goroutine.

- **Stall watchdogs are observers, never cancellers.** The iter
  pipeline's stall observer (vendored from s3store) surfaces
  stalls via `slog.Warn` + a `s3pgstore.read.iter.stall.count`
  counter without aborting. Auto-cancelling on stall would mask
  the goroutine state needed for SIGQUIT diagnosis and risk
  false-positive aborts of legitimately slow consumers. Hard
  ceilings belong at the call site via `ctx.WithTimeout`.
  Refactors must not promote the observer into a circuit
  breaker.

- **Shared-pool workers must never block on per-call
  coordination.** Every fan-out call site routes through the
  shared `pool.Pool` (defaulted to 64 slots when
  `Config.WorkerPool` is nil; explicitly shared across Stores
  in multi-Store deployments). A worker function that waits
  on a per-call resource (body slot, byte budget, decoder
  backpressure signal) holds a pool slot while waiting — and
  N waiting workers can starve every other Group sharing the
  pool, including unrelated Stores. Fix: hoist the wait to
  the *submitter* side (acquire-then-Submit), so pool workers
  are guaranteed to make progress without blocking. All four
  fan-out sites comply: Write multi-partition, Read
  fetch+decode, PollRecords body fetch, and
  `downloadAndDecodeIter` (whose `runDownloadSubmitter`
  acquires the per-call body slot before calling `g.Submit`,
  leaving the pool task to do only the S3 GET +
  markComplete). Refactors must not introduce a worker
  function that calls `<-state.slotCh`, `<-state.byteWake`,
  or any other per-call channel inside a pool-submitted task.

- **Same-pool reentrancy panics; cross-pool nesting is fine.**
  A worker that calls `g.Submit(...)` on the same pool would
  hold a slot while waiting for another slot it can't get,
  wedging the pool. The `pool.Group.Submit` path injects a
  per-pool ctx marker before invoking fn; a nested submit
  with that marker present panics at submit time with a clear
  message. Cross-pool nesting (Pool A worker submitting to
  Pool B) is safe and does not panic. Spawning a fresh
  goroutine that submits to the same pool is also safe (the
  goroutine isn't a worker, doesn't carry the marker).
  Refactors must not weaken the marker check or strip the
  ctx marker on the worker path.

- **Multi-goroutine pipelines unify cancel + abort reason via
  `context.WithCancelCause`.** When a pipeline has multiple
  goroutines that need to coordinate shutdown AND the consumer
  needs to know WHY shutdown happened (a recorded hard error,
  a parent ctx cancel, a deadline), construct one per-call ctx
  via `context.WithCancelCause(parent)`, store it on the
  pipeline's state struct, and have every stage listen on it.
  The "record an error" helper (`recordHardErr` in the iter
  pipeline) calls `cancel(err)` so the abort reason is set
  atomically with the close of `ctx.Done()`. Reads happen via
  `context.Cause(ctx)` — guaranteed non-nil at any site
  reached because a ctx-aware select returned false. First
  cancel wins; subsequent cancels are no-ops, preserving
  errgroup-style "first error wins" semantics.

  The alternative (a separate mutex-protected err field plus
  an unrelated cancel signal) introduces a race: the cancel
  propagates through Go's runtime via channel close, racing in
  parallel with the err-field write, so an observer of
  `ctx.Done` may read a still-nil err and surface a silent
  termination instead of the real reason. Parent cancellation
  is the case where this race fires reliably — there's no
  in-tree code path between the parent's cancel and the
  derived ctx's done firing.

  Load-bearing ordering: when `recordHardErr` precedes a
  state-mutation that another goroutine waits on (e.g.,
  closing a per-partition `done` channel after `markComplete`),
  the cancel-plus-cause set by `recordHardErr` becomes visible
  to that other goroutine at the same time it observes the
  state mutation. The iter pipeline relies on this to prevent
  a decoder from silently emitting a partial PartitionResult
  whose mid-flight files were marked nil by the failing task.

  `streamState.ctx` (in `read_iter.go`) is the canonical
  example. Refactors that introduce a similar multi-goroutine
  pipeline must use this shape rather than reintroduce a
  parallel err field + cancel design.

# Backend assumptions

Properties of PostgreSQL and S3 that the library's correctness
reasoning depends on. The library *assumes* them — it does not
detect or test for them at runtime.

- **PostgreSQL is the only supported database.** The library uses
  partial indexes, `INSERT ... ON CONFLICT DO UPDATE`, `RETURNING`,
  advisory locks (`pg_advisory_xact_lock`, used by both the
  sequencer and `LockPartition`), and `LISTEN/NOTIFY` (optional).
  Each is load-bearing. PostgreSQL-protocol-compatible databases
  (Aurora PostgreSQL, AlloyDB, CockroachDB, YugabyteDB) are likely
  to work but aren't tested.
- **Statement-level snapshots under READ COMMITTED.** Each SELECT
  observes a snapshot taken when the statement begins; sequential
  statements on the same connection see all earlier commits. The
  library's Read path relies on this temporal-ordering guarantee
  (no "fresh-version, stale-records" race possible).
- **`pg_advisory_xact_lock` is transaction-scoped exclusive on
  the configured key.** Only one holder at a time across the
  entire database; release on transaction end (commit or
  rollback). Advisory locks don't block plain SQL operations on
  tables — only other holders of the same key block. The
  sequencer uses a fixed key; `LockPartition` uses a key derived
  from a hash of the partition key (cooperative serialization
  between participants who all call `LockPartition`; non-callers
  proceed without blocking).
- **S3 GET-after-PUT on new keys is consistent.** The runtime
  path PUTs new UUID keys (never overwrites) and GETs them by key
  (never LISTs). The catalog tells the reader exactly which key
  to fetch. AWS S3 (since 2020) and MinIO are strongly consistent
  natively. StorageGRID needs `ConsistencyControl: read-after-new-write`
  — a multi-site grid uses a consistency leader on a local-index
  miss, so a fresh GET still finds the write. `strong-site` and
  `strong-global` are not required (they exist to handle
  cross-site overwrite propagation and LIST consistency, neither
  of which s3pgstore depends on).
- **S3 LIST is used only by `cmd/s3pgstore-rebuild`.** That is an
  offline disaster-recovery tool and tolerates eventual
  consistency on LIST. The runtime read/write path never LISTs.
  Refactors must not introduce LIST on any runtime path.
- **S3 user-metadata is self-describing for catalog rebuild.**
  Every PUT carries a fixed user-metadata bag built by
  `BuildS3Metadata` and emitted via the wrapping tx's
  `s.target.put` call: `s3pgstore-token`, `s3pgstore-records`,
  `s3pgstore-uncompressed`, `s3pgstore-version`, plus one
  `s3pgstore-ext-<name>` per non-NULL ExtensionColumn.
  `cmd/s3pgstore-rebuild` reads the bag from a HEAD response,
  reconstructs the catalog row, and skips the body GET entirely;
  for legacy files written before the metadata convention (no
  bag), rebuild falls back to GET + parquet-footer parse so DR
  still completes. The recovery path is HEAD-only, no LIST-only
  variant — S3 LIST returns key + size + LastModified + ETag,
  not user-metadata. Refactors must not drop fields from the
  bag (would break recovery for files written under the new
  convention) and must keep the bag under `S3MetadataMaxBytes`
  (2 KiB; AWS rejects oversized metadata at PUT time, so we
  validate at write time for a clean error).
- **S3 PUT is atomic per object.** No partial-write states are
  observable to GET. The library never relies on multi-object
  atomicity from S3 — the catalog transaction provides that.
- **Writer wall-clock is not in the protocol.** Unlike s3store,
  s3pgstore has no `refMicroTs`, `SettleWindow`, or `MaxClockSkew`.
  All ordering and visibility decisions are made by PostgreSQL's
  transaction machinery using its own internal MVCC. Refactors
  must not introduce wall-clock-derived ordering decisions on the
  write, read, or feed path.

# Verification

Before considering a change done, run all four:

```sh
go vet -tags=integration ./...
go test -count=1 ./...
go test -tags=integration -count=1 -timeout=10m ./...
golangci-lint run ./...
```

Only required when the change touches Go code (`.go` files,
`go.mod`, `go.sum`, build tags, generated code). Pure
documentation, comment, or asset changes (e.g. `README.md`,
`CLAUDE.md`, image files) don't need them — none of these gates
exercise non-Go content.

`go vet -tags=integration` is the cheapest way to catch
non-compiling integration test files — `go test` without the tag
silently skips them.

## Build tags

Integration tests live behind `//go:build integration` and need a
Docker daemon — the fixture spins up pinned PostgreSQL and MinIO
containers via testcontainers, one of each shared across the
invocation. Always pass `-tags=integration` when verifying any
read-, write-, sequencer-, or schema-path change; plain
`go test ./...` quietly omits them.

## Lint discipline

`golangci-lint run` covers gofmt, govet, and the project's
configured linters in one shot. Pre-existing lint issues count —
fix them in the same PR rather than carrying them forward.

## Benchmarks (perf-sensitive changes only)

The four-gate suite above does not run benchmarks — they are not
a hard gate, intentionally. Bench numbers are noisy without
benchstat, slow at the size tiers we care about, and most
changes don't touch perf-sensitive paths. Making it a global
gate would burn contributor time on every PR for value most PRs
don't deliver.

But benchmarks ARE the right verification gate for changes that
touch:

- `encodeParquet` (the `Writer[T]` method) or its pool
  (`pqWriterPool`, `encodeBufPool`).
- `EncodeBufPoolMaxBytes` defaults, the cap-discard logic, or
  the surrounding `bytes.Buffer` reset/copy semantics.
- `decodeParquet` or any caller's per-file allocation pattern in
  the iter-pipeline decoder.
- The S3 client wrapper's body-buffer allocation strategy.

The 16 MiB → 48 MiB cap regression caught upstream in s3store's
`parquet_bench_test.go` — pooled encode silently went **+27%
B/op vs no-pool** at sizes above the cap — is the kind of issue
only benchmarks surface, and it would have shipped if the writer
pool had been merged without bench data. We inherit the same
pool machinery, so we inherit the same bench requirement.

For these changes, capture before/after numbers via benchstat
and attach them to the PR description:

```sh
git stash                                     # save WIP
go test -bench=. -benchmem -count=10 -cpu=1 \
  -run=^$ ./... > /tmp/bench-base.txt
git stash pop                                 # apply WIP
go test -bench=. -benchmem -count=10 -cpu=1 \
  -run=^$ ./... > /tmp/bench-pr.txt
benchstat /tmp/bench-base.txt /tmp/bench-pr.txt
```

`-count=10` over `-count=3` because benchstat's confidence
intervals widen sharply on small samples; 10 samples per config
matches the upstream Go performance-tracking convention and
gives meaningful p-values.

For changes that don't touch the paths above, the four-gate
suite is enough — don't bother running benchmarks.

## Metrics ↔ dashboard sync

Every new OTel instrument registered in the metrics file must
also appear as a panel in `dashboards/s3pgstore.json` in the
same PR. Drift is silent — a metric that emits but isn't
visualized is operationally invisible, and metric/dashboard PRs
that land separately tend never to land at all.

When adding a metric:

1. Register it in the metrics constructor.
2. If it's a rare-event single-series counter, add it to the
   prewarm list so `rate()` catches the first non-zero sample.
3. Add a panel to `dashboards/s3pgstore.json` matching an
   existing instrument of the same shape — copy the label
   filters (`cluster`, `stage`, `k8s_namespace_name`,
   `s3pgstore_bucket`, `s3pgstore_prefix`) verbatim from a
   sibling panel. Incident counters use the
   yellow-at-`0.001/s`, red-at-`0.1/s` threshold pattern.

The same rule runs in reverse: removing or renaming a metric
requires removing or updating the corresponding dashboard panel
in the same PR. A panel querying a metric that no longer exists
shows `No data` and erodes trust in the dashboard.
