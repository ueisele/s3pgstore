# s3pgstore

PostgreSQL-coordinated Parquet on S3. The v2 of the
[s3store](https://github.com/ueisele/s3store) family.

> **Status: v2.0 implementation complete.** All 16 phases of the
> [implementation plan](implementation-plan-v2.0.md) have shipped.
> Specification: [s3pgstore-proposal-v2.0.md](s3pgstore-proposal-v2.0.md).
> Refactoring contracts: [CLAUDE.md](CLAUDE.md).

## What it does (planned)

- **Write** typed records as Parquet, grouped into Hive-style
  partitions; metadata lives in PostgreSQL.
- **Read** with strong consistency — a successful Write is visible
  to subsequent Reads the moment its PostgreSQL transaction
  commits. No marker-gates, no settle windows, no timing knobs.
- **Stream** changes via a gap-free `feed_seq` assigned by a
  separate sequencer process. Replay from offset 0 is
  deterministic.
- **Look up** records by non-partition keys via declared
  materialized views, maintained transactionally with the catalog.
- **Compose transactionally** with the caller's own PostgreSQL
  work — a write commits if and only if the host transaction
  commits, via a small `Executor` interface.
- **Detect zombie writers** via OCC (`WithExpectedVersion`) or
  pessimistic locking (`LockPartition`). Concurrent writes to the
  same partition fail loudly instead of silently overwriting.
- **Idempotent retries** of the same `(partition_key,
  idempotency_token)` return the original `WriteResult`,
  unbounded in time.

## Why s3pgstore

s3store achieves strong correctness on pure S3 by carefully
layering coordination protocols on top of S3's limited primitives.
The result works under its operational contract, but that contract
is fragile in distributed environments — visibility splits, clock
skew, propagation delays, multi-knob timing config — and produces
silent wrong answers when the assumptions don't hold.

For correctness-critical workloads (billing, financial,
regulatory data) where PostgreSQL is operationally available,
s3pgstore replaces s3store's distributed-coordination correctness
with PostgreSQL's transactional contract — 40 years old, formally
verified, exhaustively tested.

For pure-S3 deployments, [s3store](https://github.com/ueisele/s3store)
remains the right choice.

## When to choose s3store vs s3pgstore

| Concern | s3store | s3pgstore |
|---|---|---|
| PostgreSQL available | not required | required |
| Snapshot read consistency | via marker design (depends on backend strong-global) | via PostgreSQL transactions (unconditional) |
| Stream read stability | via filename-encoded coordination + settle window | via sequencer assigning monotonic offsets in committed transaction |
| Zombie-writer fencing (delta-computation) | not detected — silent acceptance of arbitrary winner | OCC + LockPartition detect violations and fail loudly |
| Cross-system transactional composition | via outbox pattern (caller builds) | via Executor (library feature) |
| Operational complexity per write | many round-trips, persisted timing config, sweeper | catalog INSERT in a transaction |
| Cleanup of failed writes | manual sweeper required | automatic (transaction abort + grace-period GC) |
| SQL queries against catalog | not available | native |
| StorageGRID consistency level | `strong-global` for multi-site | `read-after-new-write` is sufficient |

Full rationale and design discussion in
[s3pgstore-proposal-v2.0.md](s3pgstore-proposal-v2.0.md#why-s3pgstore-the-correctness-argument).

## Quick start

End-to-end example: spin up local PostgreSQL + MinIO via
`docker compose`, apply the schema, write records, read them
back, and stream them via `Poll`.

### 1. Local infrastructure

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:17.5-alpine
    environment:
      POSTGRES_USER: s3pgstore
      POSTGRES_PASSWORD: s3pgstore
      POSTGRES_DB: s3pgstore
    ports: ["5432:5432"]

  minio:
    image: pgsty/minio:RELEASE.2026-04-17T00-00-00Z
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address :9001
    ports: ["9000:9000", "9001:9001"]
```

```sh
docker compose up -d
```

### 2. Construct a Store and write records

```go
type CostRecord struct {
    CustomerID   string    `parquet:"customer_id"`
    ChargePeriod string    `parquet:"charge_period"`
    SKU          string    `parquet:"sku"`
    NetCost      float64   `parquet:"net_cost"`
    CalculatedAt time.Time `parquet:"calculated_at,timestamp(millisecond)"`
}

store, err := s3pgstore.New[CostRecord](ctx, s3pgstore.Config[CostRecord]{
    Executor: s3pgstore.NewPoolExecutor(pgPool),
    Bucket:   "warehouse",
    Prefix:   "billing",
    S3Client: s3Client,

    PartitionKeyParts: []string{"charge_period", "customer"},
    PartitionKeyOf: func(r CostRecord) string {
        return fmt.Sprintf("charge_period=%s/customer=%s",
            r.ChargePeriod, r.CustomerID)
    },

    EntityKeyOf: func(r CostRecord) string { return r.CustomerID + "|" + r.SKU },
    VersionOf:   func(r CostRecord) int64  { return r.CalculatedAt.UnixMicro() },
})

// Write: groups by PartitionKeyOf, one Parquet file per group, one catalog row.
_, err = store.Write(ctx, records)

// Read: strong consistency via PostgreSQL.
results, err := store.Read(ctx, []s3pgstore.PartitionFilter{
    s3pgstore.Eq("charge_period", "2026-03-17"),
    s3pgstore.Eq("customer", "abc"),
})

// Stream: gap-free, sequencer-assigned offsets (after running
// cmd/s3pgstore-sequencer).
entries, next, err := store.Poll(ctx, lastOffset, 100)
```

### 3. Apply schema

```go
mgr := s3pgstore.NewSchemaManager(cfg)
if err := mgr.Create(ctx); err != nil { /* ... */ }
```

For production, render the SQL via `s3pgstore.RenderDDL(cfg)`
and feed it to your migration tool — `SchemaManager.Create` is
intended for tests and small deployments.

### 4. Run the sequencer (optional, for stream consumption)

```sh
S3PGSTORE_DATABASE_URL=postgres://s3pgstore:s3pgstore@localhost/s3pgstore?sslmode=disable \
  go run github.com/ueisele/s3pgstore/cmd/s3pgstore-sequencer
```

The sequencer assigns gap-free `feed_seq` to committed catalog
rows. Wakes on NOTIFY (the writer emits one inside the catalog
tx) or on `S3PGSTORE_POLL_INTERVAL` ticks (default 1s).

### 5. Run garbage collection (recommended, for orphan reclaim)

```sh
S3PGSTORE_DATABASE_URL=postgres://s3pgstore:s3pgstore@localhost/s3pgstore?sslmode=disable \
S3PGSTORE_BUCKET=warehouse \
S3PGSTORE_S3_ENDPOINT=http://localhost:9000 \
  go run github.com/ueisele/s3pgstore/cmd/s3pgstore-gc
```

Reclaims S3 objects whose write transactions rolled back.
`S3PGSTORE_GRACE` controls the minimum age before reclaim
(default 24h); `S3PGSTORE_ONESHOT=1` runs once and exits
(useful for cron-style scheduling).

## Concurrency strategies

| Pattern | When |
|---|---|
| **OCC** (`Read` + `Write` with `WithExpectedVersion`) | Low contention, retries are cheap. |
| **Pessimistic** (`LockPartition` + `Read` + `Write`) | High contention, retries are expensive. |
| **Read only** | No concurrency control needed. |

`LockPartition` requires an active transaction in `ctx` (inject
via `s3pgstore.WithTx`) since `pg_advisory_xact_lock` releases
on autocommit. Don't mix OCC and `LockPartition` for the same
partition — see the
[don't-mix caveat](s3pgstore-proposal-v2.0.md#cooperative-protocol-caveat)
in the proposal.

## Disaster recovery

The catalog is fully recoverable from S3. After total catalog
loss:

```sh
S3PGSTORE_DATABASE_URL=postgres://... \
S3PGSTORE_BUCKET=warehouse \
S3PGSTORE_S3_PREFIX=billing \
S3PGSTORE_PARTITION_KEY_PARTS=charge_period,customer \
  go run github.com/ueisele/s3pgstore/cmd/s3pgstore-rebuild
```

`PartitionKeyParts` must match the writer's configuration. The
rebuild tool re-creates `s3pgstore_files` and
`s3pgstore_partitions` from the discovered parquet objects;
`feed_seq` is reassigned the next time the sequencer runs.
Materialized views are NOT rebuilt — operators re-run their
own MV-population pipelines after rebuild.

## Observability

Pass `cfg.Meter` (an
[`go.opentelemetry.io/otel/metric.Meter`](https://pkg.go.dev/go.opentelemetry.io/otel/metric))
to enable telemetry:

```go
cfg.Meter = otelProvider.Meter("s3pgstore")
```

The Store registers `s3pgstore.method.duration` (histogram) and
`s3pgstore.method.calls` (counter), both labeled by `method` and
`outcome` (success / error / canceled). A starter Grafana
dashboard ships at
[dashboards/s3pgstore.json](dashboards/s3pgstore.json) with
panels for the registered instruments. More metrics + panels
land in v2.0.x as instrumentation hooks expand.

## Architecture (one-line summary)

S3 stores bytes; PostgreSQL coordinates. Parquet files in
`<root>/data/<partition>/<uuid>.parquet`; catalog in
`s3pgstore_files`, `s3pgstore_partitions`, `s3pgstore_pending_writes`,
and per-MV `s3pgstore_mv_<name>` tables. A separate sequencer
process assigns gap-free `feed_seq` to committed catalog rows.

Detailed schema and write/read flows in
[s3pgstore-proposal-v2.0.md](s3pgstore-proposal-v2.0.md#architecture).

## Roadmap

**v2.0 (in design):**
- Catalog-backed write/read/feed/MV path.
- OCC via `WithExpectedVersion` and pessimistic `LockPartition`.
- Transactional composition via `Executor` interface (works with
  pgxpool directly, or with GORM/`database/sql` via a small adapter).
- Typed `ExtensionColumns` for per-file metadata.
- Sequencer (`cmd/s3pgstore-sequencer`) and orphan GC
  (`cmd/s3pgstore-gc`).
- Catalog rebuild from S3 (`cmd/s3pgstore-rebuild`).

**v2.1 (deferred, design sketch in
[s3pgstore-proposal-v2.1.md](s3pgstore-proposal-v2.1.md)):**
- Compaction with supersession tracking.
- Faster "live as of N" snapshot queries.
- Two-phase Poll offset for compaction-aware historical replay.
- Retention (`cmd/s3pgstore-retain`).
- Replication (`cmd/s3pgstore-replicate`).

v2.1 may carry breaking schema changes; there is no committed
migration path from v2.0 (deployments rebuild their catalog from
S3).

## Documentation

- [s3pgstore-proposal-v2.0.md](s3pgstore-proposal-v2.0.md) — v2.0 design
  specification: rationale, schema, API, write/read/stream
  semantics, executor abstraction, GORM integration example.
- [s3pgstore-proposal-v2.1.md](s3pgstore-proposal-v2.1.md) — v2.1
  design sketch: compaction, supersession, two-phase Poll offset,
  retention, replication. Provisional; informed by v2.0
  implementation experience.
- [implementation-plan-v2.0.md](implementation-plan-v2.0.md) —
  phased plan for shipping v2.0: 16 milestones with dependencies,
  test strategy, and done criteria.
- [CLAUDE.md](CLAUDE.md) — correctness invariants, backend
  assumptions, verification commands. Read this before refactoring.

## License

[MIT](LICENSE).
