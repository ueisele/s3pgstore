# s3pgstore v2.0 — Implementation Plan

*Phased plan for shipping v2.0. The spec is in
[s3pgstore-proposal-v2.0.md](s3pgstore-proposal-v2.0.md); this
document only describes how we get there.*

## Goals

By the end of v2.0 we have:

- A working `Store[T]` that writes typed records to Parquet in S3
  with PostgreSQL catalog rows, with strong-consistency reads,
  OCC, idempotency, transactional composition, and materialized
  views.
- A standalone sequencer process assigning gap-free `feed_seq`.
- A garbage collector reclaiming orphaned S3 files.
- A DR tool that rebuilds the catalog from S3 alone.
- Integration tests covering every read/write path against pinned
  PostgreSQL + MinIO containers.
- README quick-start that a new user can run end-to-end against
  local containers.

## Non-goals (v2.0)

- Compaction, supersession, retention, replication — see
  [s3pgstore-proposal-v2.1.md](s3pgstore-proposal-v2.1.md).
- DuckDB / SQL Query path.
- Multi-database support beyond PostgreSQL.
- Performance optimization beyond "correct and not embarrassingly
  slow." Hot-path tuning lands once we have real workloads to
  measure.

## Module layout

```
github.com/ueisele/s3pgstore
├── go.mod
├── *.go                          // root package: Store, Config, Read/Write, MV, OCC, etc.
├── *_test.go                     // unit tests (no Docker)
├── *_integration_test.go         // build tag: integration; uses PG + MinIO containers
├── fixture_test.go               // testcontainers setup
├── internal/
│   └── catalog/                  // shared schema constants, DDL templates, query helpers
│       ├── catalog.go
│       └── catalog_test.go
├── sequencer/                    // gap-free feed_seq assignment
│   ├── sequencer.go
│   └── sequencer_integration_test.go
├── gc/                           // orphan reclaim
│   ├── gc.go
│   └── gc_integration_test.go
└── cmd/
    ├── s3pgstore-sequencer/      // env-configured Run loop
    ├── s3pgstore-gc/             // env-configured one-shot or loop
    └── s3pgstore-rebuild/        // catalog rebuild from S3
```

The root package, `sequencer/`, `gc/`, and every `cmd/*` binary
import `internal/catalog/` for table-name resolution (with
`TablePrefix` substitution), DDL fragments, and shared SQL
strings. Keeping these in one internal package prevents drift
between the read/write path and operator tooling that operates on
the same tables.

## Dependencies

```
github.com/jackc/pgx/v5            // PostgreSQL driver + pool + tx
github.com/jackc/pgx/v5/pgxpool
github.com/jackc/pgx/v5/pgconn

github.com/aws/aws-sdk-go-v2/service/s3   // S3 client
github.com/aws/aws-sdk-go-v2/config

github.com/parquet-go/parquet-go   // typed read/write of Parquet

github.com/google/uuid             // UUIDv7 for file keys

go.opentelemetry.io/otel           // metrics
go.opentelemetry.io/otel/metric

// Test-only
github.com/testcontainers/testcontainers-go
github.com/testcontainers/testcontainers-go/modules/postgres
github.com/testcontainers/testcontainers-go/modules/minio
github.com/stretchr/testify        // (optional) assertions
```

Pin minor versions in `go.mod`. Go 1.23+ for `iter.Seq2`.

---

## Phased plan

Each phase ends with a concrete milestone testable in isolation.
Phases are sequenced by dependency. A phase produces a PR (or a
small series of PRs).

### Phase 0 — Project bootstrap

- `go.mod` with the dependencies above.
- Empty package files for the layout above.
- `fixture_test.go`: testcontainers fixtures spinning up pinned
  PostgreSQL and MinIO containers, one of each shared across the
  test invocation. `SetupT` helper that returns a fresh
  `*pgxpool.Pool` + `*s3.Client` + bucket name per test.
- CI pipeline running:
  - `go vet -tags=integration ./...`
  - `go test -count=1 ./...`
  - `go test -tags=integration -count=1 -timeout=10m ./...`
  - `golangci-lint run ./...`
- `.golangci.yml` with the project's linter set.

**Milestone:** an empty-but-CI-green project that other phases can
build on.

### Phase 1 — Executor abstraction + Config skeleton

- `DBTX` interface (Exec, Query, QueryRow with pgx types).
- `Executor` interface (Run, RunInTx).
- `NewPoolExecutor(*pgxpool.Pool) Executor` — default
  implementation. Reads `pgx.Tx` from context (via `WithTx`) when
  present; otherwise pool-acquires for `Run` or pool-begins for
  `RunInTx`.
- `WithTx(ctx, pgx.Tx) context.Context` + `txFromContext` helper.
- `Config[T]` struct (just the fields; no behavior yet).
- Validation: TablePrefix regex, PartitionKeyParts non-empty,
  PartitionKeyOf required.
- `ExtensionColumn` type with allowed PostgreSQL types
  (TEXT, UUID, TIMESTAMPTZ, INT, BIGINT, BOOLEAN, NUMERIC).
- Unit tests for validation paths.

**Milestone:** can construct an `Executor`, validate a `Config[T]`,
inject a `pgx.Tx` and observe it's reused inside `Executor.RunInTx`.

### Phase 2 — Schema management

- **`internal/catalog/`** — shared schema package. Holds:
  - Bare table-name constants (`"files"`, `"partitions"`,
    `"pending_writes"`, `"mv_"`).
  - `Names(prefix string) Names` — returns a struct with prefixed
    table names (`Files`, `Partitions`, `PendingWrites`, `MV(name)`).
  - DDL fragment templates for each table.
  - Common SQL query strings (`PartitionUpsertSQL`, `FilesSelectSQL`,
    `PendingWritesGCSQL`, etc.) parameterized by `Names`.
  - Used by the root package's `RenderDDL` / `SchemaManager`,
    `sequencer/`, `gc/`, and every `cmd/*` binary.
- `RenderDDL[T any](cfg Config[T]) (string, error)` (root package)
  — generates the SQL for `<prefix>_files`,
  `<prefix>_partitions`, `<prefix>_pending_writes`, all
  `<prefix>_mv_<name>` tables, and all required indexes. Composes
  fragments from `internal/catalog`. Idempotent (`IF NOT EXISTS`).
  Includes `part_<n>` columns and `ext_<n>` columns derived from
  `Config.PartitionKeyParts` and `Config.ExtensionColumns`.
- `SchemaManager` (root package) with `Create`, `Drop`, `Validate`
  methods. `Validate` queries `information_schema` and returns a
  structured error listing missing tables/columns.
- Integration tests: round-trip Create → Validate → Drop with
  several Config shapes (different prefixes, different ext
  columns, with/without MVs); `internal/catalog` unit tests for
  `Names` substitution and DDL-fragment rendering.

**Milestone:** can apply DDL to a fresh PG and validate the schema
matches the Config; `internal/catalog` is the single source of
truth for table names and shared SQL.

### Phase 3 — Parquet encode/decode

- Reflection-based parquet schema derivation from `T`'s struct
  tags via parquet-go.
- Encoder: `[]T` → parquet bytes; deterministic ordering (no
  randomization, no goroutine races).
- Decoder: parquet bytes → `[]T`; missing columns decode to
  zero value.
- `InsertedAtField` support: at encode time, populate the
  named `time.Time` field with `time.Now().UTC()`.
- Compression: snappy (default), zstd, gzip, uncompressed.
- Unit tests: round-trip, deterministic-bytes assertion,
  missing-column tolerance, InsertedAtField round-trip.

**Milestone:** stable encode/decode for arbitrary `T`.

### Phase 4 — S3 wiring

- Internal `s3target` struct holding `*s3.Client`, bucket, prefix,
  bounded parallelism semaphore (default 32).
- `put(ctx, key, body) error`, `get(ctx, key) (io.ReadCloser, error)`,
  `delete(ctx, key) error` with retry on transient errors.
- UUID key generation (UUIDv7 via `google/uuid`) under the
  partition prefix.
- Integration tests against MinIO: PUT, GET round-trip; concurrent
  PUTs respect the parallelism cap.

**Milestone:** library can read and write S3 objects keyed by
UUID under the configured prefix.

### Phase 5 — Write path (no idempotency, no OCC, no MV)

- `PartitionKeyOf` invocation; group records by partition key.
- For each partition:
  - Generate UUID key.
  - Encode parquet.
  - INSERT `s3pgstore_pending_writes` (via `Executor.Run`).
  - PUT to S3.
  - `Executor.RunInTx`: UPSERT `s3pgstore_partitions` (version
    bump, no CAS yet), INSERT `s3pgstore_files`, DELETE
    `s3pgstore_pending_writes`.
- `WriteResult` struct returned per partition.
- `Write(ctx, records []T) ([]WriteResult, error)` and
  `WriteWithKey(ctx, key string, records []T) (WriteResult, error)`.
- Multi-partition fan-out (sequential first; parallelize later).
- Integration tests: single-partition, multi-partition, host-tx
  rollback leaves S3 file + pending_writes row, no catalog row.

**Milestone:** can write records and find them in catalog + S3.

### Phase 6 — Read path

- `PartitionFilter` constructors: `Eq`, `Prefix`, `Between`, `GE`,
  `LT`, `In`. `And`, `Or` for composition.
- Filter → SQL WHERE clause translation (with parameter binding,
  no string concatenation).
- Single-query SELECT against `s3pgstore_files`.
- Group by `partition_key`; derive `Version =
  MAX(written_at_version)` per group.
- Parallel S3 GETs (capped by the parallelism semaphore).
- Decode parquet → `[]T` per file.
- Optional dedup via `EntityKeyOf` + `VersionOf` (per-partition).
- `PartitionResult[T]` returned.
- Integration tests: read-after-write within transaction;
  read-after-write across transactions; dedup correctness; complete
  file set returned per partition; lex-stable emission order.

**Milestone:** can read written records back with the right Version.

### Phase 7 — Idempotency, OCC, LookupByToken

- `WithIdempotencyToken(token)` — INSERT uses `ON CONFLICT
  (partition_key, idempotency_token) DO NOTHING RETURNING …`;
  on conflict, SELECT the existing row and return its `WriteResult`.
- `WithIdempotencyTokenOf(fn)` — per-partition variant. Mutually
  exclusive with `WithIdempotencyToken`.
- `WithExpectedVersion(v)` — partition UPSERT uses `WHERE version
  = $expected`; zero rows updated → `ErrVersionConflict`.
- `ErrVersionConflict` sentinel.
- `LookupByToken(ctx, partitionKey, token)` — single SELECT against
  the partial UNIQUE index.
- Composition: token dedup + OCC in one transaction.
- Integration tests: retry returns same WriteResult; OCC conflict;
  composition correctness; LookupByToken on existing/missing token.

**Milestone:** idempotent retries and OCC work end-to-end.

### Phase 8 — ExtensionColumns + WithMetadata

- `ext_<name>` columns generated in DDL from
  `Config.ExtensionColumns`.
- `WithMetadata(map[string]any)` validates: every key is declared,
  Go-type matches declared SQL type. Returns a clear error on
  unknown key or mismatch.
- INSERT path passes ext values as bound parameters.
- Read path decodes ext_* columns into `FileExtensions` on
  `PartitionResult[T]`.
- Integration tests: typed metadata round-trip; unknown-key
  rejected; type-mismatch rejected; first-writer-wins under
  idempotent retry with conflicting metadata.

**Milestone:** typed per-file metadata works end-to-end.

### Phase 9 — LockPartition

- `LockPartition(ctx, partitionKey, opts...)`:
  - Requires active transaction in `ctx` (errors otherwise).
  - INSERT `s3pgstore_partitions` row at version 0 if missing
    (idempotent: `ON CONFLICT DO NOTHING`).
  - `SELECT … FOR UPDATE` on the partition row.
- `WithLockTimeout(d)` — emits `SET LOCAL lock_timeout = '<d>ms'`
  before the SELECT.
- Integration tests: blocks concurrent writer; allows concurrent
  reader; deadlock detection (two transactions locking A→B and
  B→A); lock timeout returns clean error.

**Milestone:** pessimistic locking pattern verified end-to-end.

### Phase 10 — Sequencer

- `sequencer/` package.
- `Config` struct (Executor, SchemaName, TablePrefix, PollInterval,
  NotifyChannel).
- `RunOnce(ctx, cfg) (rowsAssigned int, err error)`:
  - Acquire `pg_advisory_xact_lock` on a fixed key.
  - SELECT rows WHERE `feed_seq IS NULL` ORDER BY `written_at`,
    `file_id` LIMIT batch.
  - For each row: assign `feed_seq = MAX(feed_seq) + 1`,
    `feed_seq_at = now()`. Single UPDATE per batch using
    `WITH numbered AS (SELECT file_id, ROW_NUMBER() OVER … + (SELECT
    MAX(feed_seq) FROM …))`.
- `Run(ctx, cfg)` loop: LISTEN on the configured channel; on
  NOTIFY or `PollInterval` tick, call `RunOnce`.
- Write path emits `NOTIFY <channel>` after commit (when
  `NotifyChannel` is configured on `Config[T]`).
- `cmd/s3pgstore-sequencer` binary: reads env vars, builds
  pgxpool, calls `Run`.
- Integration tests: gap-free assignment under concurrent writers;
  NOTIFY wakes the sequencer faster than polling; advisory-lock
  exclusion (a second sequencer instance blocks).

**Milestone:** committed rows pick up monotonic gap-free
`feed_seq` within seconds (or milliseconds with NOTIFY).

### Phase 11 — Stream consumption

- `Offset = int64`.
- `Poll(ctx, since, n, opts...)` — SELECT file rows WHERE
  `feed_seq > since AND feed_seq IS NOT NULL` ORDER BY `feed_seq`
  LIMIT `n`. Return `[]StreamEntry` (Offset, Key, DataPath,
  Extensions) and the next offset.
- `PollRecords(ctx, since, n, opts...)` — Poll + parallel S3 GETs
  + decode.
- `WithUntilOffset(until Offset) PollOption` — adds
  `AND feed_seq <= $until` to the SELECT. Applies to both Poll
  and PollRecords.
- `OffsetAt(ctx, t)` — SELECT MIN(`feed_seq`) WHERE `feed_seq_at
  >= t`.
- Integration tests: gap-free delivery; replay from offset 0
  produces same sequence; OffsetAt seek correctness;
  WithUntilOffset bounds the walk inclusively / exclusively per
  spec; "drain to tip" pattern terminates.

**Milestone:** stream consumers can poll, replay, and bound the
walk by offset.

### Phase 12 — ReadIter (streaming reads)

The internal pipeline is shared across all six iter methods —
filter resolution → file enumeration → parallel S3 downloads →
per-partition decode + dedup → emit. The methods differ only in
input mode (filters / time range / pre-resolved entries) and
output shape (records / per-partition).

**Pipeline (shared):**

- `ReadIter` and friends use a producer → downloader → decoder
  pipeline.
- Producer enumerates file rows for the input mode, groups by
  partition, emits in lex order of partition key.
- Downloader runs N parallel S3 GETs (capped by the parallelism
  semaphore from Phase 4).
- Decoder processes one partition at a time: waits until all its
  files are downloaded, decodes, runs `EntityKeyOf+VersionOf`
  dedup if configured, drops the batch before starting the next
  partition.
- Cancel-on-break: yielding an error from the iter cancels
  in-flight downloads.

**Six methods:**

- `ReadIter(ctx, filters, opts...) iter.Seq2[T, error]` — filters
  in, records out.
- `ReadPartitionIter(ctx, filters, opts...) iter.Seq2[PartitionResult[T], error]`
  — filters in, per-partition results out (with Version,
  FileExtensions populated).
- `ReadRangeIter(ctx, since, until time.Time, opts...) iter.Seq2[T, error]`
  — time range in, records out. Resolves both bounds to
  `feed_seq` values at call entry via two SELECTs against
  `feed_seq_at` (snapshot the upper bound so it stays stable
  under concurrent writes).
- `ReadPartitionRangeIter(ctx, since, until time.Time, opts...) iter.Seq2[PartitionResult[T], error]`
  — same input as ReadRangeIter, per-partition output.
- `ReadEntriesIter(ctx, entries []StreamEntry, opts...) iter.Seq2[T, error]`
  — pre-resolved entries in, records out. Validates upfront that
  every entry's S3 key lives under this Store's Bucket+Prefix
  (cross-store entry catches before any S3 traffic).
- `ReadPartitionEntriesIter(ctx, entries []StreamEntry, opts...) iter.Seq2[PartitionResult[T], error]`
  — same input as ReadEntriesIter, per-partition output.

`WithHistory()` opt-out for dedup applies to all six.

**Integration tests:**

- Large-result iteration with bounded memory.
- Early break cancels in-flight downloads.
- Same emission order as `Read`.
- ReadPartitionIter yields one result per partition with correct
  Version and FileExtensions.
- ReadRangeIter time bounds are half-open; zero `time.Time`
  unbounded; live tip stable under concurrent writes.
- ReadEntriesIter rejects entries whose S3 key doesn't belong to
  this Store's prefix.
- All six methods produce the same records as the corresponding
  `Read`/`Poll`+`PollRecords` for matching input.

**Milestone:** memory-bounded reads for large result sets via the
full 3×2 iter matrix (filters / time range / pre-resolved entries
× records / per-partition).

### Phase 13 — MaterializedView

- `MaterializedViewDef[T]` in `Config.MaterializedViews`.
- Schema generation: each MV becomes `s3pgstore_mv_<name>` with
  `KeyColumns` + `ValueColumns`, primary key on `KeyColumns`.
- Write path: after the catalog INSERT, INSERT MV rows in the
  same transaction. `ON CONFLICT (key_cols) DO NOTHING` when no
  ValueColumns; `ON CONFLICT (key_cols) DO UPDATE` when there are.
- `MaterializedViewLookupDef[K]` + `NewMaterializedView[T, K]` —
  validates the lookup def matches the actual table schema.
- `MaterializedView[K].Lookup(ctx, filters)` — same
  `PartitionFilter` translation against the MV table's columns.
  Returns `[]K`.
- Integration tests: insert+lookup; conflict semantics for both
  shapes; NewMaterializedView mismatch detection; MV consistency
  under transaction rollback.

**Milestone:** MV-backed lookups work; transaction rollback
preserves MV consistency.

### Phase 14 — Garbage collection

- `gc/` package.
- `RunOnce(ctx, cfg) (reclaimed int, err error)`:
  - SELECT `pending_id`, `s3_key` FROM `s3pgstore_pending_writes`
    WHERE `intended_at < now() - $grace`.
  - For each: S3 DELETE; on success, DELETE catalog row; on S3
    DELETE failure, log and skip (retry next run).
- `Run(ctx, cfg)` loop with `Interval`.
- `cmd/s3pgstore-gc` binary.
- Integration tests: orphan reclaim after rolled-back write;
  grace period respected; DELETE failure leaves row for next run.

**Milestone:** orphans cleaned up automatically.

### Phase 15 — Catalog rebuild from S3 (DR tool)

- `cmd/s3pgstore-rebuild` binary.
- Walks `<prefix>/data/` via S3 LIST.
- For each parquet: HEAD for size + LastModified; read footer for
  record_count; parse partition key from object key.
- INSERT `s3pgstore_files` rows with deterministic ordering
  (by partition_key, then UUID). `feed_seq` left NULL — sequencer
  re-assigns. Reconstruct `s3pgstore_partitions` rows from the
  per-partition file count + max(written_at_version) — but
  written_at_version isn't recoverable from S3 alone, so we set
  written_at_version = row index per partition (1, 2, 3, …) by
  S3-key lex order.
- Integration test: write a corpus via the library, drop the
  catalog, run rebuild, verify the rebuilt catalog supports
  Read with the same records.

**Milestone:** catalog can be reconstructed from S3 after a
total catalog loss.

### Phase 16 — Observability + polish

- OTel metrics:
  - `s3pgstore.write.duration` (histogram)
  - `s3pgstore.write.errors` (counter, by error type)
  - `s3pgstore.read.duration`
  - `s3pgstore.read.partition_count`
  - `s3pgstore.poll.lag` (gauge: now − latest feed_seq_at)
  - `s3pgstore.sequencer.assigned` (counter)
  - `s3pgstore.gc.reclaimed` (counter)
- slog structured logging at key boundaries (write commit, read
  start, GC reclaim, sequencer batch).
- Error-classification helpers (sentinel + wrapped).
- README expansion: full Quick Start with executable code, GORM
  integration example, schema-management workflow.
- Godoc on every exported symbol.

**Milestone:** production-ready instrumentation; README that a new
user can follow without reading the proposal.

---

## Test strategy

- **Unit tests** (no Docker) for: filter translation, DDL
  rendering, parquet encoding determinism, validation paths,
  context-tx propagation, error classification.
- **Integration tests** (`-tags=integration`, requires Docker) for
  every read/write/sequencer/MV/GC path. One PG container + one
  MinIO container shared across the invocation; each test gets
  a fresh schema and bucket prefix.
- **Property tests** where they're cheap to write — partition
  filter round-trips, parquet round-trips with arbitrary records.
- **Race tests** — `go test -race` on the unit suite.
- **Concurrency integration tests** for: OCC under contention,
  LockPartition serialization, sequencer's gap-free guarantee
  under N-writer pressure, idempotent-retry races.

CI runs all four `go vet -tags=integration` / `go test` / `go test
-tags=integration` / `golangci-lint` gates on every PR.

---

## Open questions to resolve during implementation

These are real undecided items where implementation experience
will inform the choice. Document the answer in the proposal
when each is decided.

- **Multi-partition Write fan-out concurrency.** Sequential is
  simplest; parallel improves throughput at the cost of more
  complex error-aggregation. Start sequential; add parallelism in
  Phase 5 if benchmarks justify it.
- **Sequencer batch size.** Too small → high overhead; too large
  → long-held advisory lock blocks the next batch's wakeup.
  Default 1000 rows per batch; expose as Config knob.
- **Stream consumer checkpoint helper.** Listed as an open
  question in the v2.0 proposal. Decision: ship it if the API
  fits naturally; skip otherwise. Defer the call to Phase 11.
- **Rebuild's `written_at_version` reconstruction.** Plan above
  uses S3-key lex order. If users rely on a specific value, this
  may need a more careful order. Confirm with a real DR rehearsal.
- **MV consistency check at `New()`.** Validate that all
  declared MVs exist in the schema with matching columns? Cheap
  but adds a round-trip per MV. Probably yes.

---

## Done criteria for v2.0

- All 16 phases shipped, with passing CI.
- README walks a new user through:
  1. `go get` the library.
  2. Spin up local PG + MinIO via `docker compose`.
  3. Apply schema via `SchemaManager.Create`.
  4. Write records.
  5. Read them back.
  6. Stream them via `Poll`.
  7. Look up via a MaterializedView.
  8. Run the sequencer and GC binaries.
- `go test -tags=integration` passes against pinned PG + MinIO.
- All correctness invariants in [CLAUDE.md](CLAUDE.md) have at
  least one integration test exercising them.
- API surface frozen — no breaking changes between v2.0.0 and
  v2.0.x without a clear deprecation cycle.
- Tagged release `v2.0.0` on GitHub.

After v2.0 ships and is in production for a meaningful period
(weeks, not days), v2.1 design starts based on the open questions
in [s3pgstore-proposal-v2.1.md](s3pgstore-proposal-v2.1.md).
