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

`SchemaManager.Create` is intended for tests and small
deployments. For production, see the next section.

### 3a. Production schema management

Production deployments should drive their existing migration
tool (Atlas, golang-migrate, sqlc-generated migrations, etc.)
off `s3pgstore.RenderDDL(cfg)` rather than calling
`SchemaManager.Create` from the application. The pattern: a
shared package that defines the `Config[T]` once, two binaries
that consume it.

**Shared package.** Schema-shaping fields are defined once;
runtime-only fields (`Executor` / `S3Client` / `Bucket`) are
filled in by the constructor.

```go
// internal/billing/store.go
package billing

import (
    "context"

    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/ueisele/s3pgstore"
)

type Record struct {
    CustomerID   string  `parquet:"customer_id"`
    ChargePeriod string  `parquet:"charge_period"`
    SKU          string  `parquet:"sku"`
    NetCost      float64 `parquet:"net_cost"`
}

// schemaCfg returns the schema-shaping fields shared by the
// runtime Store and the DDL generator.
func schemaCfg() s3pgstore.Config[Record] {
    return s3pgstore.Config[Record]{
        SchemaName:        "billing",
        TablePrefix:       "billing_",
        PartitionKeyParts: []string{"charge_period", "customer"},
        PartitionKeyOf: func(r Record) string {
            return "charge_period=" + r.ChargePeriod +
                "/customer=" + r.CustomerID
        },
        ExtensionColumns: []s3pgstore.ExtensionColumn{
            {Name: "job_id", Type: "TEXT"},
        },
        MaterializedViews: []s3pgstore.MaterializedViewDef[Record]{{
            Name:    "customer_sku_period",
            Columns: []string{"customer_id", "sku", "charge_period"},
            Of: func(r Record) ([][]string, error) {
                return [][]string{{
                    r.CustomerID, r.SKU, r.ChargePeriod,
                }}, nil
            },
        }},
    }
}

// NewStore wires runtime fields onto schemaCfg.
func NewStore(
    ctx context.Context,
    pool *pgxpool.Pool, s3c *s3.Client, bucket string,
) (*s3pgstore.Store[Record], error) {
    cfg := schemaCfg()
    cfg.Executor = s3pgstore.NewPoolExecutor(pool)
    cfg.S3Client = s3c
    cfg.Bucket = bucket
    cfg.Prefix = "billing"
    return s3pgstore.New(ctx, cfg)
}

// RenderSchema returns the DDL for this store. Stubs for
// runtime-only fields satisfy Config.validate(); they are
// never invoked on the rendering path.
func RenderSchema() (string, error) {
    cfg := schemaCfg()
    cfg.Executor = s3pgstore.NewPoolExecutor(nil) // never invoked
    cfg.S3Client = &s3.Client{}                   // never invoked
    cfg.Bucket = "schema-only"
    return s3pgstore.RenderDDL(cfg)
}
```

**DDL generator binary.** A small `main.go` that emits the SQL
to a known location your migration tool watches.

```go
// cmd/render-ddl/main.go
package main

import (
    "flag"
    "fmt"
    "os"

    "yourorg/yourrepo/internal/billing"
)

func main() {
    out := flag.String("out", "schema/billing.sql",
        "output path; '-' for stdout")
    flag.Parse()

    sql, err := billing.RenderSchema()
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    if *out == "-" {
        fmt.Print(sql)
        return
    }
    if err := os.WriteFile(*out, []byte(sql), 0o644); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

**Workflow.** Edit `internal/billing/store.go`, regenerate, let
the migration tool diff against the live database:

```sh
go run ./cmd/render-ddl
# then run your migration tool's diff command, e.g.
#   atlas migrate diff add_job_id_ext --env billing
#   golang-migrate create -dir migrations -ext sql add_job_id_ext
```

The runtime `Store` and the DDL generator share the same
`schemaCfg` definition, so schema drift between them is
impossible by construction.

### 4. Run the sequencer (optional, for stream consumption)

```sh
S3PGSTORE_DATABASE_URL=postgres://s3pgstore:s3pgstore@localhost/s3pgstore?sslmode=disable \
  go run github.com/ueisele/s3pgstore/cmd/s3pgstore-sequencer
```

The sequencer assigns gap-free `feed_seq` to committed catalog
rows. Wakes on NOTIFY (the writer emits one inside the catalog
tx) or on `S3PGSTORE_POLL_INTERVAL` ticks (default 1s). Full
configuration and operational notes:
[`cmd/s3pgstore-sequencer/README.md`](cmd/s3pgstore-sequencer/README.md).

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
(useful for cron-style scheduling). Full configuration and
operational notes:
[`cmd/s3pgstore-gc/README.md`](cmd/s3pgstore-gc/README.md).

## Container images

The three operator binaries ship as multi-stage, multi-arch
distroless images. Dockerfiles live next to each binary; a
per-cmd README documents configuration, env vars, and
`docker run` examples.

| Binary                | Image                                    | Purpose                                                       | README                                                              |
| --------------------- | ---------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------- |
| `s3pgstore-sequencer` | `ueisele/s3pgstore-sequencer:<tag>`      | Assigns gap-free `feed_seq` to committed rows.                | [`cmd/s3pgstore-sequencer/`](cmd/s3pgstore-sequencer/README.md)     |
| `s3pgstore-gc`        | `ueisele/s3pgstore-gc:<tag>`             | Reclaims S3 orphans from rolled-back writes.                  | [`cmd/s3pgstore-gc/`](cmd/s3pgstore-gc/README.md)                   |
| `s3pgstore-rebuild`   | `ueisele/s3pgstore-rebuild:<tag>`        | Reconstructs the catalog from S3 alone (disaster recovery).   | [`cmd/s3pgstore-rebuild/`](cmd/s3pgstore-rebuild/README.md)         |

Build a single-arch image locally (e.g. for testing):

```sh
docker buildx build \
  -f cmd/s3pgstore-sequencer/Dockerfile \
  -t ueisele/s3pgstore-sequencer:dev \
  --load .
```

Build and publish a multi-arch (amd64 + arm64) manifest to
Docker Hub — replace the `--push` recipe with your release
pipeline of choice:

```sh
docker login
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f cmd/s3pgstore-sequencer/Dockerfile \
  -t ueisele/s3pgstore-sequencer:latest \
  -t ueisele/s3pgstore-sequencer:$(git describe --tags --always) \
  --push .
```

Repeat for `s3pgstore-gc` and `s3pgstore-rebuild`.

Telemetry on every binary is opt-in via the standard OTel
environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_METRICS_EXPORTER`, etc.). With none set, the binaries
emit no metrics. See
[`cmd/internal/otelinit/`](cmd/internal/otelinit/otelinit.go)
for the full env-var contract.

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

### Library metrics

Pass `cfg.Meter` (an
[`go.opentelemetry.io/otel/metric.Meter`](https://pkg.go.dev/go.opentelemetry.io/otel/metric))
to enable telemetry on a Store:

```go
cfg.Meter = otelProvider.Meter("s3pgstore")
```

The Store registers ~20 instruments covering public-method
timing, write volumes, S3 ops, target saturation, fan-out
shape, and orphan tracking. Highlights:

| Instrument | Type | What it tells you |
|---|---|---|
| `s3pgstore.method.{duration,calls,in_flight}` | hist+ctr+gauge | Per-method P50/P95/P99 + call rate + real-time concurrency. |
| `s3pgstore.write.{bytes,records}` | histograms | P100 on bytes drives `EncodeBufPoolMaxBytes` tuning. |
| `s3pgstore.write.encode_buf_dropped` | counter | Incident — encoder dropped an oversized buffer; cap is undersized. |
| `s3pgstore.write.token_race.retry.count` | counter | Incident — concurrent writers contending on the same idempotency token. |
| `s3pgstore.s3.{request.count,request.duration,body_size}` | ctr+hist+hist | Library's view of S3 ops; one increment per logical op (acquire + retry + release). |
| `s3pgstore.s3.transient_error.count` | counter | Fires *once per failed attempt* (every retry, even masked ones). Pair with `s3.request.count` for the percentage-error panel. Labels include `error.type` ∈ {slowdown, server, client, transport, ...}. |
| `s3pgstore.target.{sem_wait_duration,sem_inflight,sem_waiting}` | hist+gauge+gauge | `Config.MaxInflightS3Requests` saturation (default 32); `waiting > 0` sustained = bump the cap (and size the `*s3.Client`'s `*http.Client` pool to match). |
| `s3pgstore.fanout.{partitions,items}` | histograms | Per-call fan-out width — capacity planning. |
| `s3pgstore.occ.version_conflict.count` | counter | Incident — OCC writes colliding. |
| `s3pgstore.lookup_by_token.count{result}` | counter | Idempotency hit-rate. |
| `s3pgstore.poll.lag` | observable gauge | End-to-end stream lag (`now - latest feed_seq_at`). |
| `s3pgstore.pending_writes.depth` | observable gauge | Orphan-tracking backlog; sustained growth = stuck GC or write-side bug. |

The sequencer registers its own instruments
(`s3pgstore.sequencer.{assigned.count, unsequenced, lock_wait}`)
under `sequencer.Config.Meter`; the gc binary registers
`s3pgstore.gc.reclaimed.count` under `gc.Config.Meter`. Both
default to a no-op meter so telemetry is opt-in.

The Grafana dashboard at
[dashboards/s3pgstore.json](dashboards/s3pgstore.json) ships
panels for every registered instrument, organised into 8
section rows (Library methods, Write volumes, S3 ops +
transient-error ratio, Target saturation, Fan-out shape,
Sequencer & feed, Catalog & locking, GC). The S3 transient-error
ratio panel uses the same `rate(transient) / rate(request{attempts!=0})`
shape as s3store's dashboard — values can exceed 100% when calls
retry multiple times per call (every retry is one transient
event against one call), which is the sustained-pressure signal.

The iter pipeline saturation instruments (body-slot wait,
byte-budget wait, decode duration, stall counter) are reserved
for the vendored chan-based pipeline; v2.0 ships a serial
pipeline so those instruments do not yet emit. They land
alongside the pipeline vendor pass.

### AWS SDK middleware metrics (request-level retry diagnostics)

The library's `s3pgstore.s3.*` instruments measure logical
operations: one increment per call, regardless of how many
times the SDK retried internally. For wire-level diagnostics
(per-attempt latencies, SDK retry-budget exhaustion, per-
endpoint cardinality) enable AWS SDK v2's Smithy middleware
metrics on your `aws.Config`:

```go
import (
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/config"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
)

awsCfg, err := config.LoadDefaultConfig(ctx,
	config.WithAPIOptions([]func(*middleware.Stack) error{
		awsmiddleware.AddRecordResponseTiming,
	}),
	// ... other options ...
)
```

Configure the SDK's metric provider via the
`AWS_SDK_GO_USE_OTEL_METRIC` environment variable (see the
[SDK metrics docs](https://aws.github.io/aws-sdk-go-v2/docs/sdk-utilities/metrics/)
for the current wiring). Smithy emits e.g.
`smithy.client.call.duration`, `smithy.client.call.attempts`,
`smithy.client.http.response.status` — these complement, not
duplicate, our `s3pgstore.s3.*` surface:

- "Did the library issue 5 PUTs or 1 PUT that the SDK retried 4
  times?" → ours says 1, theirs says 5. Both true, both useful.
- "Is S3 throttling us?" → ours via
  `s3.transient_error.count{error.type=slowdown}`; theirs via
  retry-count-per-call.
- "Is latency in our code or in the SDK retry chain?" →
  compare `s3pgstore.s3.request.duration` against
  `smithy.client.call.duration`.

Dashboards for the SDK metrics live with the SDK's own
documentation; the s3pgstore dashboard does not bundle them
because their cardinality and naming are SDK-version-specific.

### PostgreSQL pool metrics (pgxpool)

s3pgstore composes against any `Executor` adapter; the most
common is the bundled `pgxpool` adapter
([executor.go]). pgx exposes its own pool metrics via
`*pgxpool.Pool.Stat()`, which returns a
[`pgxpool.Stat`](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool#Stat)
snapshot — operators wire these into OTel by registering
observable gauges that call `Stat()` once per collection cycle:

```go
import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"
)

func registerPoolMetrics(meter metric.Meter, pool *pgxpool.Pool) error {
	acquired, err := meter.Int64ObservableGauge(
		"pgxpool.acquired",
		metric.WithDescription("Connections currently in use."))
	if err != nil {
		return err
	}
	idle, err := meter.Int64ObservableGauge(
		"pgxpool.idle",
		metric.WithDescription("Idle pool connections."))
	if err != nil {
		return err
	}
	total, err := meter.Int64ObservableGauge(
		"pgxpool.total",
		metric.WithDescription("Total open pool connections."))
	if err != nil {
		return err
	}
	max, err := meter.Int64ObservableGauge(
		"pgxpool.max",
		metric.WithDescription("Pool capacity (MaxConns)."))
	if err != nil {
		return err
	}
	acquireDur, err := meter.Float64ObservableGauge(
		"pgxpool.acquire_duration_total_ms",
		metric.WithDescription("Cumulative wait time across all Acquire calls (ms)."))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			s := pool.Stat()
			o.ObserveInt64(acquired, int64(s.AcquiredConns()))
			o.ObserveInt64(idle, int64(s.IdleConns()))
			o.ObserveInt64(total, int64(s.TotalConns()))
			o.ObserveInt64(max, int64(s.MaxConns()))
			o.ObserveFloat64(acquireDur,
				float64(s.AcquireDuration().Milliseconds()))
			return nil
		},
		acquired, idle, total, max, acquireDur)
	return err
}
```

Sustained `acquired ≈ max` with non-zero growth in
`acquire_duration_total_ms` indicates the pool is undersized for
the offered concurrency — bump `pgxpool.Config.MaxConns`. The
library's own write-path concurrency is bounded by
`Config.MaxInflightRequests` (the S3 semaphore) plus the pool
size; CLAUDE.md § Orphan tracking notes that each writer holds
**at most one** pool connection at a time, so the pool needs to
size for `peak concurrent writers + readers + sequencer
listeners + your own application`.

The library does **not** register these metrics itself — the
adapter (pgx, GORM, custom `database/sql` wrappers) is caller-
supplied and not always backed by a concrete pool. Wire them
once at startup in your application's main function.

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
