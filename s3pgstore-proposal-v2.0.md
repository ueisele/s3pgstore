# s3pgstore — Design and Specification

*The v2 of the s3store family: PostgreSQL-coordinated Parquet on S3.*

s3store: https://github.com/ueisele/s3store

## Executive summary

s3pgstore keeps the data plane that works in s3store — Parquet files
in S3, Hive-style partitioning, typed Go records, idempotent writes,
change streams — and replaces the metadata plane with a PostgreSQL
catalog. S3 stores bytes; PostgreSQL coordinates.

The motivation is correctness, not performance. s3store achieves
strong correctness properties (atomic per-file visibility via the
token-commit marker, at-least-once durability, exactly-once
consumption with reader-side dedup) by carefully layering
coordination protocols on top of S3's limited primitives. The result
works under its operational contract, but the contract itself is
fragile in distributed environments. s3pgstore inherits PostgreSQL's
transactional correctness — a 40-year-old, formally-verified,
exhaustively-tested contract — instead of building
distributed-coordination correctness manually.

For workloads where correctness is critical (billing, financial,
regulatory data) and PostgreSQL is operationally available,
s3pgstore is the appropriate choice. For workloads where pure-S3
deployment is required, s3store remains the right choice.

**Scope: v2.0.**

v2.0 ships the catalog-backed write/read/feed/materialized-view
path, plus correct (if not yet fast) time-travel snapshot queries
against partition and global coordinates. It does **not** include
compaction, supersession tracking, or data deletion. Files
accumulate monotonically; replay from the beginning of the feed
always returns the complete history because nothing is ever removed
or hidden. This is the simplest correct model.

v2.1 will add compaction, supersession, retention, and the
replication tooling that depends on them. v2.1 may carry breaking
schema changes — we do not commit to in-place migration from v2.0
to v2.1. Deployments that want compaction will re-create their
catalog at v2.1.

---

## Why s3pgstore: the correctness argument

### What we need

For correctness-critical workloads — billing, financial records,
regulatory compliance data — the storage layer must guarantee:

1. **Atomic visibility.** A write either commits and is visible to
   all readers, or doesn't and is invisible to all readers. No
   intermediate state where some readers see the write and others
   don't. This is fundamental for any workflow that computes deltas
   between snapshots and stream-processes the changes — a
   visibility split between snapshot and stream produces silently
   incorrect downstream results.

2. **At-least-once durability.** A successful write is durable
   forever. Crashes, network partitions, and server restarts never
   lose data that was acknowledged.

3. **Exactly-once at the consumer.** Retries don't produce
   duplicates that make their way to downstream processing. Either
   the storage layer prevents duplicates, or the consumer can
   deduplicate them deterministically.

4. **Strong read-after-write consistency for snapshot readers.** A
   successful write is immediately visible to all snapshot reads
   everywhere. Snapshot reads compute "current state" — any write
   missing from the snapshot is wrong.

5. **Bounded-delay durable visibility plus replay stability for
   stream readers.** Stream readers process incremental changes.
   They don't need instant visibility — a settle window of a few
   seconds is acceptable. They DO need:

   - Once a write succeeds, it eventually becomes visible within a
     bounded delay.
   - Once a record has been observed at offset N, that record at
     offset N never changes.
   - Replaying from offset 0 produces the same sequence every time.

6. **Detection of single-writer-per-partition violations.** For
   workloads where the new version is a delta against a prior
   version (cost calculations, transactional state, accumulating
   state), correctness requires that only one writer produces the
   next version of a partition at a time. The threat model is
   *zombie writers*: an orchestrator believes a job is dead and
   dispatches a replacement, but the original writer is still
   running and proceeds to write its result. Without a
   storage-layer backstop, the zombie's write silently overwrites
   the replacement's. With OCC, the zombie's write fails (its
   expected version has been superseded), and the application can
   react.

7. **Transactional composition.** A storage write can be made
   atomic with respect to writes to other systems (typically the
   application database). All-or-nothing semantics across systems.

### How s3store achieves these (and what it costs)

s3store achieves a meaningful subset through a coordination protocol
on S3:

**Atomic visibility** via **token-commit markers**. Every write
lands a `<token>.commit` zero-byte object as the last PUT in the
sequence. Both reader paths gate on its presence; a crashed writer
leaves an orphan parquet/ref pair invisible to every reader.

**At-least-once durability** is inherent in S3's PUT semantics.

**Exactly-once at the consumer** via two complementary mechanisms:

- **Token-based replica collapse.** Sequential retries short-circuit
  at the upfront HEAD on `<token>.commit` (or via conditional PUT
  under `WithOptimisticCommit`, on backends that support it).
  Out-of-contract concurrent retries are absorbed at read time —
  both read paths filter LIST/refs down to the canonical attempt
  named in the marker's metadata.
- **Version-based latest-per-entity selection.** Reader-side dedup
  via `EntityKeyOf + VersionOf` collapses multiple versions of the
  same entity to the latest.

**Strong read-after-write consistency for snapshot readers** depends
on the backend. AWS S3 / MinIO are native; StorageGRID requires
`ConsistencyControl = strong-global` for multi-site.

**Bounded-delay durable visibility for stream readers** via the
settle window. Refs are written to per-attempt paths and never
rewritten; the writer's `refMicroTs` is encoded in the ref filename
for stable lex-ordering; `SettleWindow = CommitTimeout +
MaxClockSkew` bounds how late a ref can become visible relative to
its stamped time.

**Detection of single-writer-per-partition violations** is
workload-dependent in s3store:

- For **append-only workloads** with independent rows (usage
  records, event logs), s3store handles concurrent writes
  correctly: marker arbitration (under stable tokens) or reader
  dedup (without tokens) collapses them.
- For **delta-computation workloads** (cost calculations against
  prior versions), s3store's contract is *not enough*. The marker
  mechanism handles byte-identical retries correctly. The problem
  is two writers computing v(n+1) from v(n) without seeing each
  other — they produce *different* v(n+1) values, not
  byte-identical ones. The application has no way to detect that
  a zombie writer produced a divergent computation.

**Transactional composition with PostgreSQL** is achievable via the
outbox pattern but requires the application to build it. s3store
doesn't provide it as a library feature.

The price s3store pays:

- **Many round-trips per write.** Every write is `M+3` PUTs
  (projection markers + data + ref + token-commit) plus an upfront
  HEAD on the idempotent path. `WithOptimisticCommit` skips the
  HEAD via conditional PUT but trades an orphan-on-collision cost.
  For 12k writes/day on a billing pipeline this is acceptable; for
  higher rates it becomes notable.
- **Operational fragility.** Two persisted timing knobs
  (`commit-timeout`, `max-clock-skew`) must be sized correctly,
  agreed on by writer and reader, and stable for the lifetime of
  the dataset.
- **Manual cleanup of orphans.** Failed writes leave per-attempt
  data files and refs on S3. The reader-side gate filters them
  out, but the bytes remain. Cleanup requires an operator-driven
  sweeper or S3 lifecycle policies.
- **Complex correctness reasoning.** Several interacting parts
  (per-attempt-paths, server-side visibility timing,
  writer-stamped `refMicroTs`, two-knob skew model, deterministic
  encoding requirement, marker metadata referencing one attempt)
  all need to be understood together.

### Why s3store's correctness is fragile

s3store's correctness rests on a chain of operational assumptions
that all need to hold simultaneously:

1. **`CommitTimeout` is large enough.** A storage-node GC pause,
   network blip, or noisy-neighbor effect can push real elapsed
   past the configured budget.
2. **`MaxClockSkew` correctly bounds writer↔reader divergence.** If
   the reader's clock drifts beyond it, the cutoff overtakes refs
   whose markers might still be valid server-side. Reader skips
   them silently.
3. **StorageGRID's LIST is consistent under load.** s3store's
   settle window assumes ref files appear in LIST results within
   `SettleWindow` of being written. The StorageGRID documentation
   describes consistency for HEAD/GET but is less explicit about
   LIST under load.
4. **HEAD on `<token>.commit` returns 200/404 reliably.** No false
   negatives, no transient errors that look like 404s under load.
5. **Per-attempt-paths sidestep overwrite consistency correctly
   across all backends.** True for the documented backends today.
6. **Reader-side dedup is configured and works under all retry
   overlap scenarios.**

When all hold, the design is correct. When any one breaks — even
briefly — you get **visibility splits between snapshot and stream
readers** that violate the contract silently. The system doesn't
fail loudly; it produces wrong answers, and you find out when
reconciliation reports differ days or weeks later.

The most insidious failure mode: a write becomes visible to
snapshot readers (which gate on the marker being present at LIST
time) but not to stream readers (whose `refCutoff` advanced past the
write's `refMicroTs` while the marker was still propagating). A
workflow that snapshots state and stream-processes deltas gets
inconsistent inputs.

In a distributed system, you cannot guarantee the underlying
assumptions hold simultaneously at all times. Storage GC pauses,
network partitions delaying StorageGRID metadata propagation, NTP
clock steps, noisy-neighbor throttling — any of these can push
real-world behaviour past the configured envelope. s3store provides
metrics and recovery primitives (`ErrCommitAfterTimeout`,
`RestampRef`) to detect such cases, but the underlying assumptions
are not contractual.

### Could conditional PUT help?

A natural question: would S3-side conditional writes (`If-None-Match:
*`, similar to AWS's conditional write support since 2024) enable a
more reliable s3store design?

The honest answer: *yes, somewhat* — and s3store's
`WithOptimisticCommit` already uses it on backends that support it
(AWS S3, recent MinIO, StorageGRID 12.0+; older StorageGRID falls
back to a bucket-policy `s3:PutOverwriteObject` deny). But
conditional PUT doesn't give you:

- **Atomic multi-object commits.** A single conditional PUT is
  atomic; a sequence of writes spanning multiple objects is not.
- **Transactional composition.** Conditional PUT is S3-internal; it
  cannot be made atomic with a write to PostgreSQL or any other
  system.
- **Strong consistency on LIST.** Conditional PUT affects PutObject;
  LIST behavior is independent.
- **Coordination state with rich semantics.** A conditional PUT
  creates one object atomically. Building OCC, locks, or versioning
  on top requires the same coordination protocol
  manual-construction problem.

The deeper issue: **object stores were not designed to be
coordinators.** Coordination primitives that distributed databases
provide as table stakes (atomic transactions, MVCC, foreign keys,
well-defined isolation levels) are deliberately absent from object
stores because implementing them efficiently across object-store
scale-out is a different (and harder) engineering problem than
building a database.

When you try to bolt coordination onto an object store, you're
fighting the architecture. The result is fragile in exactly the ways
s3store demonstrates.

### The logical conclusion: use a coordinator designed for it

PostgreSQL provides every coordination primitive S3 lacks:

- **Atomic transactions** with guaranteed all-or-nothing semantics.
- **MVCC** with consistent reads at any point in time and no
  reader/writer blocking.
- **Row-level CAS** via `UPDATE ... WHERE version = ?`.
- **UNIQUE constraints** for atomic uniqueness enforcement.
- **Advisory locks** (transaction-scoped or session-scoped).
- **Foreign keys** for referential integrity.

These primitives are 40 years old, formally verified, exhaustively
tested by adversarial test suites (Jepsen and others), and validated
in production at every scale. Failure modes are loudly visible
(transaction abort, deadlock, serialization failure with clear error
codes).

Using PostgreSQL for coordination collapses s3store's correctness
story from "six operational assumptions all hold simultaneously" to
"PostgreSQL's transaction semantics work as documented." This is a
qualitative simplification.

### What PostgreSQL provides for our library

For each correctness need, PostgreSQL provides an established
pattern:

- **Atomic visibility** — `INSERT INTO s3pgstore_files (...) VALUES
  (...)` inside a transaction. The row is visible atomically when
  the transaction commits. No marker files, no settle windows, no
  timing knobs.
- **At-least-once durability** — PostgreSQL's WAL plus S3's PUT
  durability.
- **Exactly-once at the consumer** — UNIQUE constraint on
  `(partition_key, idempotency_token)`. Retries with the same token
  return the prior `WriteResult`. No reader-side dedup required
  (though it remains useful for entity-level semantics).
- **Strong read-after-write consistency for snapshot readers** —
  PostgreSQL's default isolation guarantees this.
- **Bounded-delay durable visibility plus replay stability for
  stream readers** — The sequencer assigns a global sequence number
  to every committed write. Stream readers query by sequence number
  range. Once a write commits and the sequencer has assigned its
  `feed_seq`, the offset is fixed; replay from sequence 0 is
  deterministic.
- **Detection of single-writer-per-partition violations** —
  `LockPartition` acquires an advisory lock for the duration of the
  transaction; `WithExpectedVersion` provides OCC. Both detect
  zombie-writer violations rather than silently allowing them.
- **Transactional composition** — `Config.Executor` makes the
  catalog write part of the caller's transaction.

These aren't novel patterns. They're textbook database design,
applied to coordinating Parquet files in object storage.

### When s3pgstore isn't the answer

- **Lakehouse-style features.** Named immutable snapshots ("the
  end-of-Q2 baseline"), efficient O(changes) diffs between
  snapshots, atomic multi-partition commits with consistent
  snapshot isolation throughout. These are what Iceberg/Delta exist
  to provide.
- **Pure-S3 deployments.** If the use case forbids a database,
  s3store is the right choice.

### When to choose s3store vs s3pgstore

| Concern | s3store | s3pgstore |
|---|---|---|
| PostgreSQL available | not required | required |
| Correctness assumptions in distributed systems | operationally fragile | inherits PostgreSQL's contract |
| Snapshot read consistency | via marker design (depends on backend strong-global) | via PostgreSQL transactions (unconditional) |
| Stream read stability | via filename-encoded coordination + settle window | via sequencer assigning monotonic offsets in committed transaction |
| Concurrent writes (independent rows, byte-identical retries) | absorbed via marker arbitration | absorbed via UNIQUE constraint |
| Zombie-writer fencing (delta-computation) | not detected — silent acceptance of arbitrary winner | OCC + LockPartition detect violations and fail loudly |
| Cross-system transactional composition | via outbox pattern (caller builds) | via Executor (library feature) |
| Operational complexity per write | many round-trips, persisted config, sweeper | catalog INSERT in a transaction |
| Cleanup of failed writes | manual sweeper required | automatic (transaction abort) |
| SQL queries against catalog | not available | native |
| Schema evolution | filename-encoded, manual migration | catalog tables, standard migration tools |

---

## Concurrent writers and `VersionOf`

`VersionOf`'s signature is `func(T) int64` — derived from the record
itself, never from storage timing.

Storage timing doesn't correspond to logical update order. A worker
that started later but completed faster gets a *later* write
timestamp than a worker that started earlier — even though the
earlier worker's result might be the one you want preserved.
Storage-timing versions silently lose updates in concurrent
scenarios.

`VersionOf` must derive its value from the record itself — a
business timestamp, a job ID, a monotonic counter assigned before
the write, anything that captures the *logical* moment the data
represents.

Read-time deduplication uses `VersionOf`: when multiple files in a
partition contain the same `EntityKeyOf`, readers keep the row with
the highest `VersionOf`. v2.0 does this dedup at read time, on the
fly. v2.1's compaction will use the same `VersionOf` to consolidate
files at rest with the same deterministic result.

For callers who genuinely need write-time information on a record
(e.g., per-record audit), `Config.InsertedAtField` populates a
`time.Time` field on `T` with the writer's wall-clock at write-start
(persisted as a real parquet column). Read it from the record if
needed; never compute `VersionOf` from it.

### Read-modify-write: OCC vs pessimistic locking

The library offers two coordination strategies for read-modify-write
cycles. Both compose with `WithTx` for transactional composition.

**Optimistic concurrency control (OCC).** Suits low-contention
workloads where conflicts are rare and re-running the modify is
cheap. The caller reads, computes, writes with
`WithExpectedVersion`; on conflict the partition's CAS fails and
the caller retries from a fresh Read.

```go
for {
    result, err := store.Read(ctx, filters)
    if err != nil { return err }
    
    var version int64
    if len(result) > 0 {
        version = result[0].Version
    }  // version stays 0 for brand-new partitions
    
    modified := compute(result)
    
    _, err = store.Write(ctx, modified,
        s3pgstore.WithExpectedVersion(version))
    if errors.Is(err, s3pgstore.ErrVersionConflict) {
        continue  // someone else wrote; retry against fresh state
    }
    return err
}
```

No transaction needed at the caller level — Read runs under READ
COMMITTED, Write opens its own transaction internally for the CAS.

**Pessimistic locking.** Suits high-contention workloads, or
read-modify-write cycles where re-running the modify is expensive
(slow compute, external API calls, large payloads).
`LockPartition(ctx, partitionKey)` acquires a transaction-scoped
PostgreSQL advisory lock (`pg_advisory_xact_lock`) keyed by a
hash of `partitionKey`. Other callers of `LockPartition` on the
same partition block until the lock-holding transaction commits
or rolls back. Cooperative — writers that skip `LockPartition`
proceed without blocking on a holder; the pattern is "all
participants take the lock, or none."

```go
tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
if err != nil { return err }
defer tx.Rollback(context.Background())

ctx = s3pgstore.WithTx(ctx, tx)

if err := store.LockPartition(ctx, partitionKey,
    s3pgstore.WithLockTimeout(5*time.Second)); err != nil {
    return err
}

result, err := store.Read(ctx, filters)  // protected by the lock
if err != nil { return err }

modified := compute(result)

if _, err := store.Write(ctx, modified); err != nil {  // no WithExpectedVersion needed
    return err
}
return tx.Commit(ctx)
```

The lock guarantees no concurrent `LockPartition` holder can
interleave a Write between our `Read` and `Write`, so
`WithExpectedVersion` is redundant under the cooperative protocol
(passing it is harmless — the CAS will always succeed). Read-only
callers without a lock continue to see committed state
concurrently; advisory locks don't block plain SQL operations on
tables, only other holders of the same advisory key.

`LockPartition` requires an active transaction in `ctx` —
advisory-transaction locks release on autocommit, defeating the
purpose. The lock is taken on a hash of `partitionKey`, so it
works uniformly for first-write and subsequent-write scenarios
without touching the partitions table.

**Lock ordering caveat.** Transactions that lock multiple
partitions must lock them in a deterministic order (e.g., sorted by
partition key) across all callers; otherwise two transactions
locking A→B and B→A deadlock. PostgreSQL detects and aborts one
with a deadlock error, but it's avoidable.

**Cooperative-protocol caveat — pick one per partition.** OCC and
`LockPartition` are alternative concurrency strategies, not
composable per-call. Pick one for a given partition's write
protocol and apply it to every writer of that partition. If
writer A holds `LockPartition` and writer B uses
`WithExpectedVersion`, A's lock doesn't block B (advisory locks
serialize only between holders) and B's CAS doesn't observe A's
in-flight prep. Both writes land in the catalog (no data loss —
files are never deleted in v2.0), but the writer doing
read-modify-write commits a delta against a stale read. This is
the same family of mistake as a writer that forgets OCC entirely;
the cooperative protocol is no weaker than what a row-level lock
would provide against a writer that simply skips the read step.
Mixing OCC and `LockPartition` for the same partition silently
weakens both — like mixing two mutex disciplines on the same data
in Go.

| Pattern | When |
|---|---|
| **OCC** (`Read` + `Write` with `WithExpectedVersion`) | Low contention, retries are cheap. Compute is fast, conflicts are rare. |
| **Pessimistic** (`LockPartition` + `Read` + `Write`) | High contention, retries are expensive. Fewer wasted attempts; more serialization. |
| **Read only** | Read-only callers; no Lock, no OCC needed. |

---

## Architecture

### Storage layout

```
S3 bucket:
  <root>/
    data/
      <partition_key>/<uuid>.parquet       # immutable data files

PostgreSQL:
  s3pgstore_files              # catalog: one row per data file
  s3pgstore_partitions         # partition snapshot + OCC primitive
  s3pgstore_pending_writes     # orphan GC tracking
  s3pgstore_mv_<n>             # per-materialized-view tables
```

S3 keys carry no semantic content beyond the partition prefix — no
timestamps, no token suffix, no ordering. Metadata lives in the
catalog. Catalog rows reference S3 keys; S3 objects do not reference
each other.

### S3 access pattern

The runtime path uses three S3 operations:

- **PUT** a new UUID key (data file). Never overwrites by
  construction.
- **GET** a UUID key the catalog has named. Never blind GETs.
- **DELETE** a UUID key on orphan reclaim, via `cmd/s3pgstore-gc`.

No LIST, no HEAD, no overwrite. The catalog is the source of truth
for file membership; the library never asks S3 "what files exist?"
on the read or write path. (`cmd/s3pgstore-rebuild` does walk
`data/` via LIST to reconstruct the catalog from S3 alone, but
that is an offline disaster-recovery tool, not a runtime path.)

That access pattern needs only **GET-after-PUT-on-a-new-key**
consistency from S3:

- **AWS S3** (since 2020) and **MinIO** are strongly consistent
  natively; no header needed.
- **NetApp StorageGRID**: `ConsistencyControl: read-after-new-write`
  is sufficient. On a multi-site grid, when a node misses a
  freshly-PUT object on its local index it queries the consistency
  leader — adding a small latency penalty on the miss but
  guaranteeing the GET finds the write. `strong-site` and
  `strong-global` are not required. Those exist to handle
  cross-site overwrite propagation and LIST consistency under
  load — neither of which s3pgstore depends on.

This is materially weaker than s3store's StorageGRID requirement
(`strong-global` for multi-site) and removes the operational
complexity of running on the strongest tier.

### Schema management

The library does not run DDL. It reads from and writes to the
catalog tables, but never creates, modifies, or verifies schema
structure autonomously. Operators apply schema changes through their
migration tool (Flyway, golang-migrate, atlas, sqitch, or
equivalent).

Reasons:

- DDL requires elevated privileges that application credentials
  shouldn't have.
- DDL conflicts with versioned migration tooling — two pathways to
  the same schema produce drift.
- `CREATE TABLE IF NOT EXISTS` and similar look idempotent but
  interact poorly with concurrent deployments and rollbacks.
- Schema should be explicit, versioned, and reviewable. Hidden
  library DDL breaks this.

The library provides two schema-related helpers:

- **`RenderDDL[T any](cfg Config[T]) (string, error)`** generates
  the SQL for the catalog. Operators apply it through their
  migration tool. Idempotent (uses `IF NOT EXISTS`).
- **`SchemaManager`** wraps `RenderDDL` for tests and small
  deployments. `Create(ctx)` runs the DDL, `Drop(ctx)` removes
  everything, `Validate(ctx)` checks the schema matches
  expectations.

```go
cfg := s3pgstore.Config[Record]{
    Executor:          executor,
    TablePrefix:       "test_",
    PartitionKeyParts: []string{"charge_period", "customer"},
    ExtensionColumns: []s3pgstore.ExtensionColumn{
        {Name: "job_id", Type: "TEXT"},
    },
    MaterializedViews: []s3pgstore.MaterializedViewDef[Record]{...},
    PartitionKeyOf:    func(r Record) string { ... },
}

mgr := s3pgstore.NewSchemaManager(cfg)
mgr.Create(ctx)                           // tests/small deployments
store, err := s3pgstore.New[Record](ctx, cfg)  // production: schema applied via migration tool
```

`SchemaManager.Create` is implemented as `db.Exec(RenderDDL(cfg))` —
production DDL path and test-setup path share the same SQL
generation. No drift.

At `New()`, the library validates the schema via read-only
`information_schema` queries and returns a clear error if columns or
tables are missing. It never repairs.

**Table prefix.** `Config.TablePrefix` (default `"s3pgstore_"`) is
prepended to every library-managed table. With `"cost_"`, the
catalog table is `cost_files`. Validated:
`^[a-z][a-z0-9_]*$`, max 20 characters. Empty string is valid.

### Write path

```
Caller             Library                   Executor              S3
------             -------                   --------              --
Write(ctx, records, opts...)
  │
  │                Serialize Parquet
  │                Generate UUID key
  │                Executor.Run(INSERT pending_writes)
  │                ──────────────────────────►
  │                PUT data file
  │                ──────────────────────────────────────►
  │                Executor.RunInTx(
  │                  UPSERT s3pgstore_partitions  (CAS if WithExpectedVersion)
  │                  INSERT s3pgstore_files
  │                  INSERT s3pgstore_mv_*
  │                  DELETE s3pgstore_pending_writes
  │                )
  ◄──────────────── WriteResult
```

S3 PUTs are outside any transaction (they can't be transactional).
Catalog operations are one PostgreSQL transaction. `Executor.RunInTx`
joins a transaction in `ctx` if present, otherwise opens its own.

On commit, the write is atomic, visible, and indexed. On rollback,
the data file exists in S3 but is orphaned; GC reclaims it after a
grace period via `s3pgstore_pending_writes`.

#### Write options

```go
Write(ctx, records,
    WithIdempotencyToken(token),       // retry-safe write
    WithIdempotencyTokenOf(fn),        // per-partition variant
    WithExpectedVersion(version),      // OCC
    WithMetadata(map[string]any{...}), // typed extension columns
)
```

Options compose. A single `Write` call may combine
`WithIdempotencyToken` (or `WithIdempotencyTokenOf`),
`WithExpectedVersion`, and `WithMetadata`.

**Idempotency.** Partial UNIQUE constraint on `(partition_key,
idempotency_token) WHERE idempotency_token IS NOT NULL`. Retries
absorbed by the constraint; original `WriteResult` returned. No
`maxRetryAge` — the constraint is unbounded in time. Per-partition
scope (the same token in different partitions doesn't conflict).

`WithIdempotencyTokenOf` is the per-partition variant: a `func([]T)
(string, error)` invoked once per partition. Each partition's
catalog row uses its own token. Mutually exclusive with
`WithIdempotencyToken`.

**Optimistic concurrency.** `WithExpectedVersion(v)` adds `AND
version = $expected` to the partition UPDATE. On mismatch, returns
`ErrVersionConflict`.

**The partition row.** `s3pgstore_partitions` has one row per
partition, holding `version` and bookkeeping. Write's interaction
depends on the caller's options:

| Option | SQL form | First write | Existing partition |
|---|---|---|---|
| No `WithExpectedVersion` | `INSERT ... ON CONFLICT DO UPDATE` | INSERT row at version 1 | UPDATE; version becomes N+1 |
| `WithExpectedVersion(0)` | upsert with `WHERE version = 0` | INSERT row at version 1 | UPDATE if version=0; else `ErrVersionConflict` |
| `WithExpectedVersion(N>0)` | `UPDATE WHERE version=N` | Zero rows updated → `ErrVersionConflict` | UPDATE succeeds if version=N; else `ErrVersionConflict` |

The semantics of `WithExpectedVersion(0)`: "I expect this partition
to have had no writes yet." In v2.0 the only state matching is
"partition row absent" — `LockPartition` uses
`pg_advisory_xact_lock` and never touches the row. The conflict
branch retains a `WHERE version = 0` filter for forward
compatibility with hypothetical future paths that might leave
version=0 rows; in v2.0 it's defensive code that never fires.

All three SQL shapes are single round-trips. No version is read
before the write; the UPDATE/INSERT increments and `RETURNING` gives
the new version.

**Composition with idempotency.** Both options together produce
retry-safe optimistic concurrency. Token dedup runs first (UNIQUE
check on INSERT); OCC runs only on genuinely new writes. Atomic
within one transaction.

#### Probing a token without writing

```go
result, exists, err := store.LookupByToken(ctx, partitionKey, token)
```

Returns the prior `WriteResult` if `(partition_key, token)` already
exists, or `(WriteResult{}, false, nil)` otherwise. A single SELECT
against the partial UNIQUE index. Useful for orchestrators that want
to short-circuit before encoding parquet.

#### Transactional composition

Callers compose `Write` with their own database work via the
configured `Executor`. The pattern depends on the caller's stack.

For pgx-native callers using s3pgstore's default pool executor:

```go
tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)

ctx = s3pgstore.WithTx(ctx, tx)

store.Write(ctx, records)
tx.Exec(ctx, "UPDATE jobs SET status = 'done' WHERE id = $1", jobID)

tx.Commit(ctx)
```

For callers using a host transaction manager (GORM, custom
`database/sql` wrappers), the host's existing tx primitives work
directly — no s3pgstore-specific tx wiring at the call site:

```go
err := txMgr.WithTransaction(ctx, func(ctx context.Context) error {
    db := txMgr.GetOrFallback(ctx)
    db.Create(&jobRow)                          // GORM
    _, err := store.Write(ctx, records)         // s3pgstore — same tx
    return err
})
```

In both cases, the s3pgstore write commits if and only if the
caller's other writes commit. If the host transaction rolls back,
the catalog row is never created; the S3 file is orphaned and GC
reclaims it.

The library calls `Config.Executor.RunInTx(ctx, fn)` for every
write. The executor's contract: if `ctx` already carries a
transaction, run `fn` in it; otherwise, open a new transaction.

**Read-after-write within the same transaction.** Standard
PostgreSQL MVCC: a `Read` after a `Write` in the same transaction
sees the just-written records. The S3 PUT happened before the
catalog row was created (durable); the catalog SELECT inside the
same transaction sees the row; the Parquet GET succeeds.

### Read path

```go
results, err := store.Read(ctx, []s3pgstore.PartitionFilter{
    s3pgstore.Eq("charge_period", "2026-03-17"),
    s3pgstore.Eq("customer", "abc"),
})
// results: []PartitionResult[T] — one entry per partition matched
```

The library does:

1. Translate `[]PartitionFilter` to a SQL WHERE clause. Each filter
   maps to a predicate on the corresponding `part_<n>` column;
   `Or`/`And` compose them.
2. SELECT every file row for the matched partitions from
   `s3pgstore_files`. One indexed query.
3. Group rows by `partition_key`. For each partition, derive
   `Version = MAX(written_at_version)` over its file rows.
4. Fetch Parquet files from S3 in parallel.
5. Apply dedup if `EntityKeyOf` + `VersionOf` are configured.
6. Return `[]PartitionResult[T]`, one entry per matched partition.

#### `PartitionResult[T]`

```go
type PartitionResult[T any] struct {
    PartitionKey   string
    Records        []T
    Version        int64
    FileExtensions []FileExtensions  // typed extension columns per file
}
```

`Read` returns only partitions that have at least one file row.
`LockPartition` doesn't materialize a partition row (it takes an
advisory lock keyed by hash), so a held lock with no subsequent
writes is invisible to `Read` — by design; callers already hold
the partition key. Callers who want OCC on a brand-new partition
use `WithExpectedVersion(0)` on the Write — no Read needed first.

#### Single-query design

```sql
SELECT partition_key, file_id, s3_key, written_at_version,
       <ext_* columns>, ...
FROM s3pgstore_files
WHERE <part_<n> predicates>
ORDER BY partition_key, feed_seq;
```

One indexed query. The Read path returns the complete set of files
for each matched partition (no LIMIT, no per-file filtering). The
returned `Version` per partition is `MAX(written_at_version)` over
that partition's files.

#### Why READ COMMITTED is enough

This Read does not open an explicit transaction. PostgreSQL runs
the SELECT under READ COMMITTED, against a statement-level
snapshot taken when the SELECT begins.

That is sufficient because of two invariants the write path
upholds:

1. Every Write atomically bumps `s3pgstore_partitions.version` from
   N to N+1 *and* inserts a file row with `written_at_version =
   N+1`, in one transaction. There is no path that bumps the
   version without inserting a file, and no path that inserts a
   file without bumping the version. No file is ever deleted in
   v2.0.

   Therefore, for any partition, the file rows carry
   `written_at_version` values `{1, 2, ..., K}` and
   `s3pgstore_partitions.version = K` exactly. **`MAX(written_at_version)`
   over a partition's files equals the partition's current version
   by construction.** No race window can produce a discrepancy.

2. Read returns the complete set of files for each matched
   partition (no per-file filtering). So `MAX` over the returned
   files equals `MAX` over the partition's files, equals the
   partition's version.

The OCC CAS on Write (`UPDATE s3pgstore_partitions SET version =
version + 1 WHERE partition_key = $key AND version = $expected`)
is the source of truth for write-write race detection. If a
concurrent writer commits between this Read and a subsequent Write
that uses `WithExpectedVersion(K)`, the CAS finds `version > K`
and fails with `ErrVersionConflict`. The caller retries. Read does
not need a transaction to make this work.

(There is no reverse race — "fresh version, stale records" — that
could slip past the CAS, because PostgreSQL processes one statement
at a time per connection, and a later statement's snapshot
includes every commit visible to an earlier statement on the same
connection.)

For callers who want a stronger snapshot guarantee for the
`(Version, Records)` pair (e.g., to export a partition state at a
specific version for audit), wrap the Read in your own
`Executor.RunInTx` — the executor's transaction can use REPEATABLE
READ if you need it. The default Read path stays cheap.

#### `PartitionFilter`

Typed predicates against the `part_<n>` columns:

```go
type PartitionFilter interface { /* ... */ }

func Eq(part, value string) PartitionFilter
func Prefix(part, prefix string) PartitionFilter
func Between(part, from, to string) PartitionFilter   // half-open [from, to)
func GE(part, value string) PartitionFilter
func LT(part, value string) PartitionFilter
func In(part string, values ...string) PartitionFilter

func And(filters ...PartitionFilter) PartitionFilter
func Or(filters ...PartitionFilter) PartitionFilter
```

Translation table:

| Filter | Generated WHERE |
|---|---|
| `Eq("charge_period", "2026-03-17")` | `part_charge_period = '2026-03-17'` |
| `Prefix("charge_period", "2026-03-")` | `part_charge_period LIKE '2026-03-%'` |
| `Between("charge_period", "2026-03-01", "2026-04-01")` | `part_charge_period >= '2026-03-01' AND part_charge_period < '2026-04-01'` |
| `In("customer", "a", "b", "c")` | `part_customer IN ('a', 'b', 'c')` |

Omitting a part from the filter list means "any value." There is no
explicit wildcard constructor; absence is the wildcard.

Validation at the library boundary: part names must exist in
`Config.PartitionKeyParts`, `Between` endpoints must satisfy `from <
to`, values are plain strings (lex comparison; zero-pad numerics if
you compare them as numbers).

**Multi-partition reads** use `Or` to express disjunctive selection:

```go
results, _ := store.Read(ctx, []s3pgstore.PartitionFilter{
    s3pgstore.Or(
        s3pgstore.And(
            s3pgstore.Eq("charge_period", "2026-03-17"),
            s3pgstore.Eq("customer", "abc"),
        ),
        s3pgstore.And(
            s3pgstore.Eq("charge_period", "2026-03-18"),
            s3pgstore.Eq("customer", "def"),
        ),
    ),
})
```

The single `Read` handles single- and multi-partition cases
uniformly.

#### Iter-based reads

`Read` buffers the full result set in memory before returning. For
large result sets, the library offers a 3×2 matrix of streaming
read methods — three input modes × two output shapes.

| Input | Records output (`iter.Seq2[T, error]`) | Per-partition output (`iter.Seq2[PartitionResult[T], error]`) |
|---|---|---|
| Filters (snapshot) | `ReadIter` | `ReadPartitionIter` |
| Time range `[since, until)` | `ReadRangeIter` | `ReadPartitionRangeIter` |
| Pre-resolved `[]StreamEntry` | `ReadEntriesIter` | `ReadPartitionEntriesIter` |

```go
// Filters → records
for r, err := range store.ReadIter(ctx, filters) {
    if err != nil { return err }
    process(r)
}

// Filters → per-partition results (when the partition boundary matters)
for part, err := range store.ReadPartitionIter(ctx, filters) {
    if err != nil { return err }
    aggregate(part.PartitionKey, part.Records)
}

// Time range → records (everything written between two wall-clock points)
for r, err := range store.ReadRangeIter(ctx, since, until) {
    if err != nil { return err }
    process(r)
}

// Pre-resolved entries → records (decode without re-querying the catalog)
entries, _, _ := store.Poll(ctx, lastOffset, 1000)
filtered := filterByExtensions(entries)
for r, err := range store.ReadEntriesIter(ctx, filtered) {
    if err != nil { return err }
    process(r)
}
```

**Time-range methods** (`ReadRangeIter`, `ReadPartitionRangeIter`)
take `time.Time` bounds. The library resolves both bounds to
`feed_seq` values at call entry (one indexed lookup each on
`feed_seq_at`), then walks the resolved offset range. Snapshotting
the bounds at call entry — not at first iteration — keeps the
upper bound stable under concurrent writes. Half-open semantics:
records at `since` are included, records at `until` are not. A
zero `time.Time` means unbounded (`since=zero` → stream head;
`until=zero` → live tip captured at call entry).

**Entries-based methods** (`ReadEntriesIter`,
`ReadPartitionEntriesIter`) take `[]StreamEntry` from a previous
`Poll` and decode the corresponding parquet files. Useful for
replicators, inspection tools, and any caller that wants to filter
or coordinate on entry metadata before paying for the GETs. The
library validates that every entry's `S3Key` lives under this
Store's `Bucket`/`Prefix` — passing entries from a different
Store fails before any S3 traffic.

**Per-partition output** yields one `PartitionResult[T]` per
partition (in lex order of partition key). All `PartitionResult`
fields are populated — `Records`, `Version`, `FileExtensions` —
the same shape as `Read` returns, but one at a time.

All iter methods share the same per-partition pipeline as
`ReadIter`: dedup via `EntityKeyOf` + `VersionOf` (or
`WithHistory()` to disable), cancel-on-break, lex-stable emission
order. Memory bound is one partition's records (or
`WithReadAheadBytes` if set; see `ReadIter` notes).

**Caveat: range and entries methods don't expose offset
checkpoints.** A consumer that aborts mid-iteration cannot resume
from where it left off. For checkpointable consumption use
`Poll` / `PollRecords` with `WithUntilOffset`.

### Stream path

The naive design — `BIGSERIAL` column polled with `WHERE id > last`
— is broken. Sequences allocate values at INSERT time, not COMMIT
time. Under concurrent writers, transaction A may acquire sequence
value 100 while transaction B acquires 101, B commits first, a
consumer sees 101 and advances past 100, A commits later, row 100
becomes visible but the consumer has already moved past it. Silent
gap.

Every serious database-as-log system has to address this. v2.0 uses
the **sequencer pattern**: writers INSERT with `feed_seq = NULL`. A
single-threaded sequencer job (serialized via
`pg_advisory_xact_lock`) scans committed rows where `feed_seq IS
NULL` and assigns monotonic offsets in commit order. Consumers poll
`WHERE feed_seq > last`. Gap-free by construction because the
sequencer only touches already-committed rows.

The sequencer assigns `feed_seq = MAX(feed_seq) + 1` under the
advisory lock. No PostgreSQL sequence is involved — the lock makes
MAX safe.

The sequencer ships as `cmd/s3pgstore-sequencer`, deployed as a
sidecar or dedicated service. It needs only a database connection;
no `Store[T]` instance.

```go
package sequencer

func Run(ctx context.Context, cfg Config) error
func RunOnce(ctx context.Context, cfg Config) (rowsAssigned int, err error)
```

Trade-off: `feed_seq` assignment lags writes by the sequencer
interval (default 1s). Between commit and `feed_seq` assignment, the
row is visible to direct partition reads but not to `Poll`. This is
the only eventual-consistency window in v2.0, and it applies only to
the feed.

For low-latency wake-ups, the Store can be configured to emit
`NOTIFY <channel>` after each write commit; the sequencer's `LISTEN`
reduces typical assignment lag to milliseconds. Falls back to
interval polling if NOTIFY is disabled or temporarily unavailable.

#### Reading the feed

Consumers poll with an opaque `Offset` (the last `feed_seq` they
processed):

```sql
SELECT ... FROM s3pgstore_files
WHERE feed_seq IS NOT NULL
  AND feed_seq > $cursor
ORDER BY feed_seq
LIMIT $batch_size;
```

```go
entries, newOffset, err := store.Poll(ctx, lastOffset, 100)
records, newOffset, err := store.PollRecords(ctx, lastOffset, 100)
offset, err := store.OffsetAt(ctx, t)
```

`Poll` returns `[]StreamEntry` (file-level entries; metadata only,
no GETs). `PollRecords` returns `[]T` (decoded records).
`OffsetAt(ctx, t)` returns the first `feed_seq` whose `feed_seq_at`
is at or after `t`, letting consumers seek to a wall-clock time.

**Bounded poll via `WithUntilOffset`.** Pass `WithUntilOffset(end
Offset)` to bound the upper cursor. Useful for "drain everything
committed up to point X and stop":

```go
tip, _ := store.OffsetAt(ctx, time.Now())
for since := startOffset; since < tip; {
    records, next, _ := store.PollRecords(ctx, since, 100,
        s3pgstore.WithUntilOffset(tip))
    if len(records) == 0 { break }
    process(records)
    since = next
}
```

Both `Poll` and `PollRecords` accept the option.

**Replay from offset 0 is always correct.** Because v2.0 never
removes or hides any catalog row, a consumer reading from `cursor =
0` sees the complete history of every write, in order. A consumer
that has been offline for a year resumes from its last-known cursor
and catches up.

**Storage grows monotonically.** Files accumulate; nothing is ever
deleted by the library. For workloads with bounded retention needs:

- For cost management, use S3 lifecycle policies to move older
  Parquet files to colder storage tiers.
- For partition-level retention, applications can choose to
  partition by time and stop writing to old partitions; queries
  that don't touch those partitions don't pay their cost.

Compaction, supersession, and built-in retention are deferred to
v2.1 (see Future work).

### Snapshot queries

v2.0 supports time-travel queries against partition and global
coordinates. Without supersession, every write stays "live forever,"
so the query is just "all writes up to coordinate N":

```sql
-- Partition P at version N:
SELECT s3_key FROM s3pgstore_files
WHERE partition_key = $1 AND written_at_version <= $N;

-- Full dataset at feed_seq N:
SELECT s3_key FROM s3pgstore_files
WHERE feed_seq IS NOT NULL AND feed_seq <= $N;
```

Read-time `EntityKeyOf` + `VersionOf` dedup collapses returned files
to the right answer. A query at "version 1000" returns the keys of
all files written up to that version; the reader keeps the highest
`VersionOf` per entity.

The cost grows linearly with history depth: a query at version 1000
reads more keys than a query at version 10. For partitions with low
version counts, this is fine. For high-write partitions where
snapshot reads become slow, v2.1's compaction will reduce the file
count and v2.1's `superseded_at_*` columns will cut the snapshot
query to "live as of N" rather than "all up to N."

### MaterializedView

For lookups by non-partition keys, declare a materialized view. Each
MV is a PostgreSQL table holding the user-projected columns; rows
are inserted in the same transaction as the file row.

```go
// Write side — declared in Config.MaterializedViews
type MaterializedViewDef[T any] struct {
    Name         string
    KeyColumns   []string
    ValueColumns []string  // optional; data carried alongside the key

    // Of emits zero or more rows per record. Each row's Key length
    // must equal len(KeyColumns) and Value length must equal
    // len(ValueColumns).
    Of func(T) ([]MVRow, error)
}

type MVRow struct {
    Key   []string
    Value []string  // empty when ValueColumns is empty
}

// Read side — bound at runtime via NewMaterializedView
type MaterializedViewLookupDef[K comparable] struct {
    Name         string
    KeyColumns   []string
    ValueColumns []string

    // From projects (KeyColumns + ValueColumns) values onto K.
    // Optional; nil → reflection-based mapping.
    From func(values []string) (K, error)
}

func NewMaterializedView[T, K any](
    store *Store[T],
    def MaterializedViewLookupDef[K],
) (*MaterializedView[K], error)

func (m *MaterializedView[K]) Lookup(
    ctx context.Context,
    filters []PartitionFilter,
) ([]K, error)
```

Schema:

```sql
CREATE TABLE s3pgstore_mv_<name> (
    key_col1   TEXT NOT NULL,
    key_col2   TEXT NOT NULL,
    val_col1   TEXT,           -- only when ValueColumns non-empty
    val_col2   TEXT,
    PRIMARY KEY (key_col1, key_col2)
);
```

Conflict policy is chosen by the MV's shape:

- **No `ValueColumns`** (key-only MV) → `ON CONFLICT (key_cols) DO
  NOTHING`. First write wins; idempotent.
- **Has `ValueColumns`** → `ON CONFLICT (key_cols) DO UPDATE SET
  val_col1 = EXCLUDED.val_col1, ...`. Last write wins.

MV rows are self-contained: no foreign-key reference to
`s3pgstore_files`. Queries against the MV table answer directly
without reading parquet. Use the MV when you need fast lookups by
non-partition keys; use the regular `Read` path when you need the
full record.

The MV must be declared up-front in `Config.MaterializedViews` so
the catalog DDL can include its table. `NewMaterializedView` returns
an error if the name doesn't match a declared MV, or if the
declared schema doesn't match the actual table.

### ExtensionColumns

Per-file metadata that callers query as typed SQL columns:

```go
type ExtensionColumn struct {
    Name string  // e.g. "job_id"
    Type string  // PostgreSQL type, e.g. "TEXT", "TIMESTAMPTZ", "UUID"
}

cfg := Config[Record]{
    ExtensionColumns: []ExtensionColumn{
        {Name: "job_id", Type: "TEXT"},
        {Name: "tenant_id", Type: "UUID"},
        {Name: "calculated_at", Type: "TIMESTAMPTZ"},
    },
    // ...
}

_, err := store.Write(ctx, records,
    s3pgstore.WithMetadata(map[string]any{
        "job_id":        jobID,
        "tenant_id":     tenantID,
        "calculated_at": time.Now().UTC(),
    }))
```

Each column lands as `ext_<name>` on `s3pgstore_files`:

```sql
CREATE TABLE s3pgstore_files (
    -- core columns ...
    ext_job_id        TEXT,
    ext_tenant_id     UUID,
    ext_calculated_at TIMESTAMPTZ
);
```

Querying via plain SQL:

```sql
SELECT * FROM s3pgstore_files WHERE ext_job_id = 'job-abc';
```

For hot query patterns, add an expression index on the `ext_<name>`
column. Indexes are operator-managed (same as the rest of the
schema).

`WithMetadata` validates that every key in the map is declared in
`Config.ExtensionColumns` and that the value's type matches the
declared SQL type. Unknown keys are an error at call time.

The typed Read path doesn't filter on ExtensionColumns; it filters
on partition columns. ExtensionColumns are surfaced on
`PartitionResult[T].FileExtensions` and `StreamEntry.Extensions` for
callers that want them.

**Composition with idempotency: first writer wins.** A retry with
the same token but different metadata is absorbed by the UNIQUE
constraint; the existing row's metadata stays.

ExtensionColumns are intentionally typed and declared up-front
rather than stored as JSONB. This trades a small amount of schema
rigidity for type safety, indexable columns, and migrations that
look like normal `ALTER TABLE` rather than JSONB-key archaeology.

### Schema evolution

Adding a column to `T` works without library changes: parquet-go
matches by tag, missing columns decode to Go's zero value. Old files
have the new column as zero-valued; new files carry the data.

Renames, type changes, splits, and computed derivations require
rewriting affected files. Out of scope for v2.0 — applications
that need a queryable per-file schema-version label can declare
an `ExtensionColumn{Name: "schema_version", Type: "INT"}` and
pass it via `WithMetadata` per write. A first-class
`SchemaVersion` field plus migration scaffolding is a v2.1
candidate.

### Disaster recovery and replication

The catalog is recoverable from S3 alone. A `cmd/s3pgstore-rebuild`
tool walks `data/`, reads each Parquet file's footer for record
counts and sizes, and rebuilds `s3pgstore_files` from scratch. Slow
for large datasets but guaranteed: every fact in the catalog is
either present in S3 or derivable from it.

`feed_seq` cannot be reconstructed from S3 (it's a coordination
artifact, not data). Rebuilt catalogs reassign `feed_seq` from 1
onward. Consumers must reset their offsets after a rebuild.

For active replication, the feed itself is the replication
mechanism. A secondary catalog tails `feed_seq`, copies S3 objects,
and replays catalog rows on the secondary. In v2.0 every feed event
is a write, so replay is straightforward. v2.1 will add compaction
events to the feed and a reference replicator implementation.

### Garbage collection

**Orphan data files.** S3 PUT succeeded; PostgreSQL transaction
rolled back. The data file exists with no catalog row referencing
it. `s3pgstore_pending_writes` tracks these — INSERT before the PUT,
DELETE after the COMMIT. A periodic GC job reads pending rows older
than a grace period (e.g., 24 hours) and DELETEs the corresponding
S3 objects.

A `cmd/s3pgstore-gc` tool implements this. Run periodically (e.g.,
hourly via cron) against any deployment.

v2.0 does not delete files for any other reason. Retention,
supersession cleanup, and time-based deletion all wait for v2.1 once
the supersession-tracking primitives are in place.

### Why PostgreSQL only

v2 targets PostgreSQL exclusively. PostgreSQL-protocol-compatible
databases (Aurora PostgreSQL, AlloyDB, CockroachDB, YugabyteDB) are
likely to work but aren't tested.

The choice is deliberate, not incidental. The library uses several
PostgreSQL-specific features:

- **Partial indexes** (`UNIQUE INDEX ... WHERE`) for the nullable
  idempotency-token uniqueness constraint.
- **`INSERT ... ON CONFLICT DO UPDATE`** for the atomic
  partition-row upsert.
- **`RETURNING` clauses** for single-round-trip writes.
- **Advisory locks** (`pg_advisory_xact_lock`) for both the
  sequencer's single-instance serialization (fixed key) and
  `LockPartition`'s per-partition pessimistic lock (key derived
  from a hash of `partitionKey`). Advisory locks don't block
  plain SQL operations on tables — read-only callers keep
  reading concurrently with a held `LockPartition`, and
  cooperative writers that don't take the lock proceed without
  blocking on a holder.
- **`LISTEN/NOTIFY`** for low-latency feed wake-ups (optional).

Each is load-bearing — switching to a database that lacks them would
force less efficient or less correct implementations.

### Database library: pgx

v2 uses pgx directly (not GORM, ent, or other ORMs).

- All SQL is library-internal and known at compile time. ORM
  benefits (query builder, schema migration, model-row mapping)
  don't apply.
- The `Executor` pattern requires a uniform handle that's both a
  pool and a transaction. pgx's `pgxpool.Pool` and `pgx.Tx` share
  the necessary `DBTX` interface.
- Reflection overhead in ORMs is a nonzero cost on every query.
- Migration coordination: ORMs that own schema migration would
  conflict with the no-DDL contract.

Callers using GORM or other ORMs in their application code are
unaffected. They route through the `Executor` interface — see "GORM
integration example" below.

---

## Schema

The schema below shows the structure the library expects. The
library does not create or modify it — operators apply it through
their migration tooling (or `SchemaManager.Create` for tests). Table
names use the default `TablePrefix: "s3pgstore_"`. Partition-part
columns are generated from `Config.PartitionKeyParts`;
ExtensionColumns from `Config.ExtensionColumns`. The example below
assumes `PartitionKeyParts: [charge_period, customer]` and
`ExtensionColumns: [{job_id, TEXT}, {tenant_id, UUID}]`.

```sql
CREATE TABLE s3pgstore_files (
    -- Identity
    file_id                 BIGSERIAL PRIMARY KEY,
    s3_key                  TEXT NOT NULL UNIQUE,
    -- Partition membership
    partition_key           TEXT NOT NULL,                  -- materialized "part1=v1/..."
    part_charge_period      TEXT NOT NULL,                  -- one column per PartitionKeyParts entry
    part_customer           TEXT NOT NULL,
    -- File metadata
    file_size               BIGINT NOT NULL,
    record_count            BIGINT,
    -- Versioning
    written_at_version      BIGINT,                         -- partition version at write time
    written_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Idempotency
    idempotency_token       TEXT,
    -- Sequencer / stream
    feed_seq                BIGINT UNIQUE,                  -- assigned by sequencer; NULL until then
    feed_seq_at             TIMESTAMPTZ,
    -- User extensions
    ext_job_id              TEXT,                           -- one column per ExtensionColumns entry
    ext_tenant_id           UUID
);

CREATE INDEX ON s3pgstore_files (partition_key, written_at);
CREATE INDEX ON s3pgstore_files (feed_seq) WHERE feed_seq IS NOT NULL;
CREATE INDEX ON s3pgstore_files (written_at, file_id) WHERE feed_seq IS NULL;  -- sequencer scan
CREATE INDEX ON s3pgstore_files (part_charge_period, part_customer);

CREATE UNIQUE INDEX s3pgstore_files_token_idx
    ON s3pgstore_files (partition_key, idempotency_token)
    WHERE idempotency_token IS NOT NULL;

CREATE TABLE s3pgstore_partitions (
    partition_key       TEXT PRIMARY KEY,
    part_charge_period  TEXT NOT NULL,                      -- mirrors columns on s3pgstore_files
    part_customer       TEXT NOT NULL,
    version             BIGINT NOT NULL DEFAULT 0,
    file_count          INT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON s3pgstore_partitions (part_charge_period, part_customer);

CREATE TABLE s3pgstore_pending_writes (
    s3_key       TEXT PRIMARY KEY,
    intended_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-MV tables (one per MaterializedViewDef in Config.MaterializedViews)
-- Example: KeyColumns = [sku_id, period_start], ValueColumns = [region]
CREATE TABLE s3pgstore_mv_sku_period (
    sku_id        TEXT NOT NULL,
    period_start  TEXT NOT NULL,
    region        TEXT,
    PRIMARY KEY (sku_id, period_start)
);
```

Scale envelope: comfortable up to ~50M rows on modest hardware
without partitioning. Beyond that, consider partitioning
`s3pgstore_files` by `written_at` (range, yearly or quarterly). The
library's queries are partition-compatible — every query that
benefits from time-pruning includes a relevant predicate.

---

## API

```go
package s3pgstore

// Executor is the database access abstraction. Callers provide an
// implementation that routes SQL operations through their own
// connection management (pgx pool, GORM tx manager, etc.).
type Executor interface {
    // Run fn against the database. If ctx carries an active
    // transaction, fn participates in it. Otherwise fn runs on a
    // pool-acquired connection without an explicit transaction.
    // Used for read paths and single-statement writes.
    Run(ctx context.Context, fn func(DBTX) error) error

    // RunInTx is like Run but guarantees fn runs inside a
    // transaction. If ctx already carries one, fn reuses it.
    // Otherwise opens a new transaction for the duration of fn,
    // commits on nil return, rolls back on error. Used for the
    // multi-statement Write path.
    RunInTx(ctx context.Context, fn func(DBTX) error) error
}

// DBTX is the pgx-native interface s3pgstore needs.
// *pgx.Conn, *pgxpool.Pool, and pgx.Tx all satisfy it.
type DBTX interface {
    Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
    Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
    QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// NewPoolExecutor returns the default pgx-native Executor that
// reads pgx.Tx from context (via WithTx) when present, otherwise
// uses the pool directly.
func NewPoolExecutor(pool *pgxpool.Pool) Executor

// WithTx injects a pgx.Tx into ctx. The default pool executor uses
// this to participate in caller-managed transactions. Callers using
// a host tx manager (GORM, custom database/sql wrapper) don't need
// this — their tx manager populates the context.
func WithTx(ctx context.Context, tx pgx.Tx) context.Context

type Config[T any] struct {
    Executor Executor

    // S3 wiring
    Bucket   string
    Prefix   string
    S3Client *s3.Client

    // Schema layout
    SchemaName  string  // default "public"
    TablePrefix string  // default "s3pgstore_"

    // Partitioning
    PartitionKeyParts []string                // ["charge_period", "customer"]
    PartitionKeyOf    func(T) string          // derive key from record

    // Typed extension columns on s3pgstore_files; queryable as plain SQL.
    ExtensionColumns []ExtensionColumn

    // Materialized views — declared up-front; typed query handles via NewMaterializedView at runtime.
    MaterializedViews []MaterializedViewDef[T]

    // Read-side dedup. Both fields together or neither.
    EntityKeyOf func(T) string
    VersionOf   func(T) int64

    // Optional: populates a time.Time field on T from the writer's
    // wall-clock at write-start (persisted as a real parquet column).
    InsertedAtField string

    Compression CompressionCodec  // default snappy

    // Feed wake-up notification. Empty disables NOTIFY (sequencer
    // falls back to interval polling).
    NotifyChannel string  // default "s3pgstore_writes"
}

type ExtensionColumn struct {
    Name string  // e.g. "job_id"
    Type string  // PostgreSQL type, e.g. "TEXT", "TIMESTAMPTZ", "UUID"
}

func New[T any](ctx context.Context, cfg Config[T]) (*Store[T], error)

// Schema management
type SchemaManager struct { /* ... */ }
func NewSchemaManager[T any](cfg Config[T]) *SchemaManager
func (m *SchemaManager) Create(ctx context.Context) error
func (m *SchemaManager) Drop(ctx context.Context) error
func (m *SchemaManager) Validate(ctx context.Context) error

func RenderDDL[T any](cfg Config[T]) (string, error)

// PartitionFilter constructors
type PartitionFilter interface { /* ... */ }
func Eq(part, value string) PartitionFilter
func Prefix(part, prefix string) PartitionFilter
func Between(part, from, to string) PartitionFilter
func GE(part, value string) PartitionFilter
func LT(part, value string) PartitionFilter
func In(part string, values ...string) PartitionFilter
func And(filters ...PartitionFilter) PartitionFilter
func Or(filters ...PartitionFilter) PartitionFilter

// Write
type WriteOption interface { /* ... */ }
func WithIdempotencyToken(token string) WriteOption
func WithIdempotencyTokenOf[T any](fn func([]T) (string, error)) WriteOption
func WithExpectedVersion(expected int64) WriteOption
func WithMetadata(m map[string]any) WriteOption

func (s *Store[T]) Write(ctx context.Context, records []T, opts ...WriteOption) ([]WriteResult, error)
func (s *Store[T]) WriteWithKey(ctx context.Context, key string, records []T, opts ...WriteOption) (WriteResult, error)
func (s *Store[T]) LookupByToken(ctx context.Context, partitionKey, token string) (WriteResult, bool, error)

// Read
type PartitionResult[T any] struct {
    PartitionKey   string
    Records        []T
    Version        int64
    FileExtensions []FileExtensions
}

type FileExtensions struct {
    FileID     int64
    Extensions map[string]any  // ext_* values keyed by ExtensionColumn.Name
}

type ReadOption interface { /* ... */ }
func WithHistory() ReadOption  // disable per-partition dedup

// Snapshot reads (filter-based)
func (s *Store[T]) Read(ctx context.Context, filters []PartitionFilter, opts ...ReadOption) ([]PartitionResult[T], error)
func (s *Store[T]) ReadIter(ctx context.Context, filters []PartitionFilter, opts ...ReadOption) iter.Seq2[T, error]
func (s *Store[T]) ReadPartitionIter(ctx context.Context, filters []PartitionFilter, opts ...ReadOption) iter.Seq2[PartitionResult[T], error]

// Time-range reads (records or per-partition output)
func (s *Store[T]) ReadRangeIter(ctx context.Context, since, until time.Time, opts ...ReadOption) iter.Seq2[T, error]
func (s *Store[T]) ReadPartitionRangeIter(ctx context.Context, since, until time.Time, opts ...ReadOption) iter.Seq2[PartitionResult[T], error]

// Decode pre-resolved entries (typically the output of Poll)
func (s *Store[T]) ReadEntriesIter(ctx context.Context, entries []StreamEntry, opts ...ReadOption) iter.Seq2[T, error]
func (s *Store[T]) ReadPartitionEntriesIter(ctx context.Context, entries []StreamEntry, opts ...ReadOption) iter.Seq2[PartitionResult[T], error]

// Stream
type Offset = int64

type StreamEntry struct {
    Offset     Offset
    Key        string
    DataPath   string
    Extensions map[string]any
}

type PollOption interface { /* ... */ }
func WithUntilOffset(until Offset) PollOption  // bound the upper cursor

func (s *Store[T]) Poll(ctx context.Context, since Offset, n int, opts ...PollOption) ([]StreamEntry, Offset, error)
func (s *Store[T]) PollRecords(ctx context.Context, since Offset, n int, opts ...PollOption) ([]T, Offset, error)
func (s *Store[T]) OffsetAt(ctx context.Context, t time.Time) (Offset, error)

// OCC
var ErrVersionConflict = errors.New("s3pgstore: version conflict")

// Pessimistic locking
type LockOption interface { /* ... */ }
func WithLockTimeout(d time.Duration) LockOption

func (s *Store[T]) LockPartition(ctx context.Context, partitionKey string, opts ...LockOption) error

// MaterializedView
type MVRow struct {
    Key   []string
    Value []string
}

type MaterializedViewDef[T any] struct {
    Name         string
    KeyColumns   []string
    ValueColumns []string
    Of           func(T) ([]MVRow, error)
}

type MaterializedViewLookupDef[K comparable] struct {
    Name         string
    KeyColumns   []string
    ValueColumns []string
    From         func(values []string) (K, error)
}

func NewMaterializedView[T, K any](store *Store[T], def MaterializedViewLookupDef[K]) (*MaterializedView[K], error)
func (m *MaterializedView[K]) Lookup(ctx context.Context, filters []PartitionFilter) ([]K, error)
```

### Sequencer

```go
package sequencer

type Config struct {
    Pool          *pgxpool.Pool  // required (LISTEN needs a dedicated connection,
                                 //  which the Executor abstraction can't expose)
    SchemaName    string         // default "public"
    TablePrefix   string         // default "s3pgstore_"
    PollInterval  time.Duration  // default 1s
    BatchSize     int            // default 1000
    NotifyChannel string         // default "s3pgstore_writes"; empty disables LISTEN
}

// Run blocks until ctx is cancelled, assigning feed_seq values to
// new rows as they commit. Uses LISTEN/NOTIFY for low-latency
// wake-ups when configured; falls back to polling otherwise.
func Run(ctx context.Context, cfg Config) error

// RunOnce assigns feed_seq values to all currently-eligible rows
// and returns. Used by tests and ad-hoc invocations.
func RunOnce(ctx context.Context, cfg Config) (rowsAssigned int, err error)
```

The sequencer ships as `cmd/s3pgstore-sequencer` for operators who
want a ready-to-deploy binary. It reads `Config` from environment
variables and runs `Run` until killed.

The `Store[T]` write path emits a `NOTIFY <NotifyChannel>` after
commit (when configured) so the sequencer wakes up immediately.

### Package structure

```
github.com/ueisele/s3pgstore/
├── *.go                         // root package: typed reads/writes, MV, OCC, etc.
├── sequencer/                   // gap-free feed_seq assignment; standalone
├── gc/                          // orphan GC
└── cmd/
    ├── s3pgstore-sequencer/     // sequencer daemon
    ├── s3pgstore-gc/            // orphan cleanup
    └── s3pgstore-rebuild/       // rebuild catalog from S3 alone
```

Single root package. No DuckDB. No multi-package split.

---

## GORM integration example

For callers using a host transaction manager (GORM, custom
`database/sql` wrappers), the `Executor` interface is implemented in
the caller's own package — typically a few lines wrapping the host's
existing connection-management primitives.

The example below assumes a host tx manager with the shape:

- `WithTransaction(ctx, fn)` — runs `fn` inside a transaction; if a
  transaction already exists in `ctx`, reuses it.
- `WithSQLConn(ctx, fn)` — runs `fn` with a `*sql.Conn`; inside a
  transaction, gives the transaction's connection; outside, takes
  one from the pool.

Adapter (in the caller's package, e.g. `dbtx`):

```go
package dbtx

import (
    "context"
    "database/sql"

    "github.com/jackc/pgx/v5/stdlib"
    "github.com/ueisele/s3pgstore"
)

type s3pgstoreExecutor struct {
    txMgr *SQLConnTransactionManager
    pgx   *PgxExecutor
}

func NewExecutorFromPgxExecutor(
    txMgr *SQLConnTransactionManager,
    pgx *PgxExecutor,
) s3pgstore.Executor {
    return &s3pgstoreExecutor{txMgr: txMgr, pgx: pgx}
}

func (e *s3pgstoreExecutor) Run(
    ctx context.Context,
    fn func(s3pgstore.DBTX) error,
) error {
    return e.txMgr.WithSQLConn(ctx, func(conn *sql.Conn) error {
        return conn.Raw(func(driverConn any) error {
            return fn(driverConn.(*stdlib.Conn).Conn())
        })
    })
}

func (e *s3pgstoreExecutor) RunInTx(
    ctx context.Context,
    fn func(s3pgstore.DBTX) error,
) error {
    return e.txMgr.WithTransaction(ctx, func(ctx context.Context) error {
        return e.Run(ctx, fn)
    })
}
```

Wiring:

```go
txMgr := dbtx.NewTransactionManager(gormDB)
pgxExec := dbtx.NewPgxExecutor(txMgr)

store, err := s3pgstore.New[Record](ctx, s3pgstore.Config[Record]{
    Executor: dbtx.NewExecutorFromPgxExecutor(txMgr, pgxExec),
    Bucket:   "warehouse",
    Prefix:   "billing",
    // ...
})
```

Call site — composes naturally with GORM transactions:

```go
err := txMgr.WithTransaction(ctx, func(ctx context.Context) error {
    db := txMgr.GetOrFallback(ctx)
    db.Create(&jobRow)                          // GORM
    _, err := store.Write(ctx, records)         // s3pgstore — same tx
    return err
})
```

If the host transaction rolls back, both the GORM row and the
s3pgstore catalog row are rolled back together. The S3 file is
orphaned and reclaimed by GC.

**Connection cost.** Outside a transaction, each
`Executor.Run`/`RunInTx` call does a single pool acquire + release
on the underlying connection pool — same cost as
`pgxpool.Pool.Acquire`/`Release` (microseconds, no new TCP
connection). Inside a transaction, all calls reuse the transaction's
connection. There is no per-operation TCP-connection overhead.

---

## Open design questions

The genuinely-undecided items, where implementation experience will
inform the choice.

### Stream consumer checkpoint storage

v2.0 callers track their own offsets externally. The library could
provide an optional helper for the common "checkpoint in PostgreSQL
alongside the catalog" pattern:

```go
checkpoint := s3pgstore.NewCheckpoint(executor, "consumer-name")
offset, _ := checkpoint.Load(ctx)
// ... process ...
checkpoint.Save(ctx, newOffset)
```

Decision: ship the helper if it fits naturally; skip if it forces
schema design choices on callers (e.g., requiring a specific
checkpoint table layout). Lean toward shipping.

---

## Future work (v2.1)

v2.1 adds compaction, supersession tracking, faster snapshot
queries, two-phase Poll offsets, retention, and replication. It
may introduce breaking schema changes; there is no committed
migration path from v2.0 (deployments adopting v2.1 re-create
their catalog from S3 via `cmd/s3pgstore-rebuild`).

Design sketch in [s3pgstore-proposal-v2.1.md](s3pgstore-proposal-v2.1.md).
