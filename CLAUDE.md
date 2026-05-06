# Correctness invariants

These are contracts the library makes to its users. Refactors
must preserve them — even when the change appears unrelated.

- **Atomic visibility on commit** — A successful Write is visible
  to every subsequent Read / ReadIter / MaterializedView.Lookup the
  moment its PostgreSQL transaction commits. There is no settle
  window, no propagation delay, no marker-gate. Refactors must not
  introduce visibility gates outside the catalog transaction or
  read paths that bypass it.
- **Orphan tracking via `s3pgstore_pending_writes`** — S3 PUT and
  the catalog INSERT cannot be transactionally coupled. The
  pending-writes row is INSERTed before the S3 PUT and DELETEd
  inside the same transaction as the catalog INSERT. On rollback,
  the S3 file exists with no catalog reference; the pending-writes
  row stays and `cmd/s3pgstore-gc` reclaims the orphan after a
  grace period. Refactors must not move the pending-writes DELETE
  outside the catalog transaction, or skip the INSERT-before-PUT.
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
  transaction commits. Refactors must not bypass the Executor for
  catalog writes (no direct `pool.Begin`, no `pgxpool.Pool.Exec`
  on a catalog table) and must not assume the executor always
  opens its own tx.
- **MaterializedView is self-contained; no FK to s3pgstore_files**
  — MV rows hold `(KeyColumns..., ValueColumns...)` with a primary
  key on `KeyColumns`. Conflict policy is determined by shape:
  `ON CONFLICT DO NOTHING` when `ValueColumns` is empty
  (key-only MV; first-write-wins, idempotent),
  `ON CONFLICT DO UPDATE` when `ValueColumns` is non-empty
  (last-write-wins). MV row insertion happens in the same
  transaction as the catalog row, so consistency is automatic.
  Refactors must not add a FK to `file_id`, must not change the
  conflict policy without changing the MV shape semantics, and
  must not insert MV rows in a separate transaction from the
  catalog write.
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
  `InsertedAtField`).

# Backend assumptions

Properties of PostgreSQL and S3 that the library's correctness
reasoning depends on. The library *assumes* them — it does not
detect or test for them at runtime.

- **PostgreSQL is the only supported database.** The library uses
  partial indexes, `INSERT ... ON CONFLICT DO UPDATE`, `RETURNING`,
  advisory locks (`pg_advisory_xact_lock`), row-level locks
  (`SELECT ... FOR UPDATE`), and `LISTEN/NOTIFY` (optional). Each
  is load-bearing. PostgreSQL-protocol-compatible databases
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
  rollback). The sequencer uses a fixed key; if another process
  or operator holds the same key, the sequencer blocks rather
  than proceeding in parallel.
- **`SELECT ... FOR UPDATE` blocks UPDATE/DELETE/SELECT FOR UPDATE
  on the same row, not plain SELECT.** `LockPartition` uses this
  to serialize concurrent writers to the same partition while
  letting read-only callers continue to see committed state
  without blocking.
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
