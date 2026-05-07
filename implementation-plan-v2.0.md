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

## Source attribution and licensing

A meaningful subset of the implementation is **vendored from
[s3store](https://github.com/ueisele/s3store)** with attribution.
Both projects are MIT-licensed under the same author, so license
compatibility is straightforward — but provenance still needs to
be explicit so future maintainers know where to look upstream
when something needs porting back.

### What we vendor

Substrate-independent code that solves the same problem in both
libraries: parquet bytes are parquet bytes, the chan-based iter
pipeline is hard-won concurrency, dedup is dedup. Specifically:

- `parquet.go` — Phase 3 (encode/decode).
- Selected pieces of `target.go` — Phase 4 (retry + semaphore;
  not consistency-control or timing-config seeding).
- `reader_dedup.go` — Phases 6 and 12 (per-partition dedup).
- `reader_iter.go` post-`860cf19` — Phase 12 (chan-based
  pipeline, read-ahead knobs, stall watchdog).
- `metrics.go` patterns — Phase 16 (`methodScope`, attribute
  conventions; metric names become `s3pgstore.*`).

### What we do NOT vendor

Substrate-dependent code where the catalog replaces s3store's
S3-only coordination machinery: token-commit markers, `refMicroTs` /
`SettleWindow` / `MaxClockSkew`, ref filename encoding, glob
grammar (replaced by `PartitionFilter`), `ConsistencyControl`
header routing, optimistic-commit / `RestampRef` /
`LookupCommit` / `ErrCommitAfterTimeout`, idempotency-token
storage paths.

### Attribution header

Every vendored file carries this header at the top, after the
package clause:

```go
// Adapted from https://github.com/ueisele/s3store/blob/<sha>/<path>
// Copyright (c) 2024-2026 Uwe Eisele. MIT License.
//
// <one-line note on what changed for s3pgstore, e.g.:
//  "Stripped commit-marker handling; renamed package from
//  s3store to s3pgstore.">
```

The `<sha>` pins the exact upstream version we vendored from so
maintainers can diff against it later.

### Sync policy

s3store and s3pgstore evolve independently after the initial
vendor. We don't auto-sync. When a meaningful improvement lands
in s3store (e.g., a deadlock fix in the iter pipeline), we make
a deliberate decision per file: port the change forward,
diverge, or backport our version upstream. The same applies in
reverse — improvements in s3pgstore can be hand-ported to
s3store. The shared author makes this practical without tooling.

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

**Source: vendored from s3store** (post-v0.25.0). Note: the
`parquet.go` file no longer exists in s3store — `52c34e7` moved
the parquet-tag walker into `materialized_view.go` (and
unexported it as `parquetFields`) since materialized views are
the only caller. The encode/decode work for s3pgstore lives in
several upstream files.

Vendor scope (with attribution):

- **Encoder** — `encodeParquet` from `writer_write.go`, plus
  the pool machinery from `writer.go`:
  - `pqWriterPool` (`*parquet.GenericWriter[T]` reuse)
  - `encodeBufPool` (`*bytes.Buffer` reuse with cap-discard)
  - `encodeBufPoolMaxBytes` (resolved from
    `WriterConfig.EncodeBufPoolMaxBytes`)
  - `defaultEncodeBufPoolMaxBytes = 48 << 20` (48 MiB; commit
    `9ef2075`)
- **Encoder config knob** — surface `EncodeBufPoolMaxBytes`
  on s3pgstore's `Config[T]` with the same semantics: cap above
  largest typical produced parquet; non-zero rate of
  `s3pgstore.write.encode_buf_dropped` (added in Phase 16)
  signals undersized cap.
- **Decoder** — the `decodeParquet` helper from
  `reader_iter.go`'s `runDecoder` (extracted; substrate-
  independent).
- **Read-value lifetime contract** — `2a2e7f9` pins the
  parquet-go `GenericReader` read-value lifetime guarantee
  (records valid until the next Read or Close). Vendor
  `parquet_lifetime_test.go` to keep the contract pinned.
- **Determinism test** — `f29e77f` verifies that pooled encode
  produces byte-identical output to a fresh non-pooled encode.
  Vendor this test; it's load-bearing per CLAUDE.md (the
  pooled-encoder path must be byte-equivalent for
  `WithIdempotencyToken` retries to remain correct).
- **Benchmarks** — vendor `parquet_bench_test.go` (encode +
  decode benchmarks across workload size tiers; commit
  `3b3b4b5`). Required by CLAUDE.md § Benchmarks for any future
  change that touches the encoder pool.

Adjust:

- Package from `s3store` to `s3pgstore`.
- Metric/log attribute names from `s3store.*` to `s3pgstore.*`.
- Drop any s3store-specific knobs (e.g., `ConsistencyControl`)
  from the `WriterConfig` projection.

Unit tests, benchmarks, and the lifetime test come over with
the files. Integration tests not needed for this phase (no S3
or PG involvement).

**Milestone:** stable encode/decode for arbitrary `T`, with the
hard-won determinism guarantees inherited from s3store, the
pooled-encoder byte-equivalence test passing, and benchmarks
green at expected size tiers.

### Phase 4 — S3 wiring

**Source: partial vendor from s3store** (`target.go`). Lift the
retry policy and `MaxInflightRequests` semaphore wrapper. Drop
the consistency-control routing (we only need
`read-after-new-write`), the timing-config seeding from
`<prefix>/_config/*`, and any code that reads/writes ref or
commit-marker paths.

What we lift verbatim (with attribution):

- `retry()` with jittered backoff + transient-error
  classification (`isTransientS3Error`).
- The `MaxInflightRequests` semaphore primitive (acquire/release
  around every PUT/GET/HEAD/LIST).
- `verifyPutObjectETag` for 0-byte-on-retry detection
  (commit `d3829cd`).

What we redo for s3pgstore:

- `s3target` struct: holds `*s3.Client`, bucket, prefix,
  semaphore. No `CommitTimeout`/`MaxClockSkew`/`SettleWindow`
  fields, no timing-config GETs at construction.
- `put`, `get`, `delete` operations with the lifted retry
  wrapping. No `head` on the runtime path (only `cmd/s3pgstore-rebuild`
  uses HEAD, and only for parquet footer reads via GET).
- UUID key generation (UUIDv7 via `google/uuid`).

Integration tests against MinIO: PUT, GET round-trip; concurrent
PUTs respect the parallelism cap; transient-error retry escalates
correctly.

**Milestone:** library can read and write S3 objects keyed by
UUID under the configured prefix, with s3store's
hard-tuned retry behavior.

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

**Source: vendored dedup, fresh enumeration.** Port
`reader_dedup.go` from s3store (`sortAndDedup`,
`sortKeyMetasByKey`) verbatim with attribution; the per-partition
dedup logic is identical regardless of how files are enumerated.
Reimplement the file-enumeration step against the catalog instead
of LIST + ref filtering.

Fresh implementation (no s3store equivalent):

- `PartitionFilter` constructors: `Eq`, `Prefix`, `Between`, `GE`,
  `LT`, `In`. `And`, `Or` for composition.
- Filter → SQL WHERE clause translation (with parameter binding,
  no string concatenation).
- Single-query SELECT against `s3pgstore_files`.
- Group by `partition_key`; derive `Version =
  MAX(written_at_version)` per group.

Vendored from s3store (with attribution):

- `sortKeyMetasByKey` — stable sort within a partition.
- `sortAndDedup` — `(EntityKeyOf, VersionOf)` stable sort + in-place
  dedup, last-version-wins semantics, `WithHistory()` opt-out.

Wired together:

- After the catalog SELECT and partition grouping, hand each
  partition's file list to the vendored decode+dedup pipeline.
- Parallel S3 GETs capped by the Phase 4 semaphore.
- Decode parquet → `[]T` per file using the Phase 3 decoder.
- Apply dedup if `EntityKeyOf` + `VersionOf` are configured.
- Return `[]PartitionResult[T]`.

Integration tests: read-after-write within transaction;
read-after-write across transactions; dedup correctness;
complete file set returned per partition; lex-stable emission
order; per-partition dedup edge cases (matches s3store's existing
test corpus where applicable).

**Milestone:** can read written records back with the right
Version; s3store's dedup correctness inherited verbatim.

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

`LockPartition` uses `pg_advisory_xact_lock` keyed by a stable
hash of the partition key — not a row-level `SELECT … FOR UPDATE`
on `s3pgstore_partitions`. Trade-offs: no version=0 stub rows in
the partitions table, no schema clutter for partitions that are
locked but never written, lock auto-releases on tx end (commit or
rollback). Cooperative semantics — a writer that doesn't call
`LockPartition` doesn't block on a holder; document this.

- `LockPartition(ctx, partitionKey, opts...)`:
  - Requires active transaction in `ctx` (errors otherwise —
    advisory-xact locks release on autocommit, defeating the
    purpose).
  - Computes a deterministic int64 key from `partitionKey`
    (e.g. `hash/fnv` 64-bit). Same key, same lock, every time.
  - Issues `SELECT pg_advisory_xact_lock($1)` with the hashed
    key. Blocks until other holders of the same key release
    their tx, or until `lock_timeout` fires.
- `WithLockTimeout(d)` — emits `SET LOCAL lock_timeout = '<d>ms'`
  before the lock acquisition. Applies to advisory locks the
  same way it applies to row locks.
- Integration tests: blocks concurrent locker on the same key;
  allows concurrent locker on a different key; allows concurrent
  reader (advisory locks don't block plain SELECT); deadlock
  detection (two transactions locking A→B and B→A); lock timeout
  returns clean error; cooperative-skip path (a Write without
  LockPartition is not blocked by a holder, by design).

**Milestone:** pessimistic-lock pattern verified end-to-end via
`pg_advisory_xact_lock`.

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

**Source: vendored pipeline from s3store** (`reader_iter.go` after
`860cf19` "Replace iter pipeline sync.Cond with chan-based
primitives" and `da75ca9` "Fix iter pipeline deadlock under
cond.Wait scheduler unfairness"). The producer → downloader →
decoder topology, the `WithReadAheadPartitions` /
`WithReadAheadBytes` budget knobs, the cancel-on-break semantics,
and the stall-watchdog are all hard-won correctness work that we
should inherit verbatim. Keep the test corpus too — the
concurrency tests are exactly what catches deadlocks.

**Concurrency contracts inherited:** the three rules in
[CLAUDE.md § Concurrency invariants](CLAUDE.md#concurrency-invariants)
(buffered-channel FIFO over `cond.Broadcast`, every blocking
primitive observes `ctx.Done()` natively, stall watchdogs are
observers never cancellers) come along with the vendor. They
must not be relaxed when adapting the pipeline.

What we vendor (with attribution):

- The chan-based producer/downloader/decoder pipeline structure.
- `WithReadAheadPartitions`, `WithReadAheadBytes` options.
- `runProducer`, `runDownloader`, `runDecoder` and their
  back-pressure wiring.
- The stall watchdog (`dd83417`).
- Per-partition emit helpers (`recordEmit`, `partitionEmit`).

What we adapt:

- Replace s3store's `resolvePatterns` (LIST + glob filter +
  commit-gate) with our `PartitionFilter`-to-SQL enumerator from
  Phase 6.
- Replace s3store's `walkRangeKeys` (`_ref/` LIST over a time
  window) with a SQL SELECT bounded by `feed_seq_at`.
- Replace `validateEntriesBelongHere` to check bucket+prefix
  ownership (s3store's check is the same shape; just point at
  our paths).
- Drop the upfront commit-gate HEAD per ref (catalog rows are
  the gate now).

The methods differ only in input mode (filters / time range /
pre-resolved entries) and output shape (records / per-partition);
all six share the same vendored pipeline.

**Pipeline (shared, vendored):**

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

**Source: vendored helpers from s3store**'s `materialized_view.go`.
Specifically the `parquetFields` tag walker (now unexported
upstream after `52c34e7`) and the auto-binding/auto-projection
helpers that map between `T`'s parquet-tagged fields and the MV
table's columns. These are the same helpers our MV
write/read paths need; vendoring keeps the parquet-tag
convention interpreted in one place.

Adapt the upstream's S3-marker-key encoding to PostgreSQL row
inserts: instead of building marker paths under
`<prefix>/_matview/`, insert rows into `s3pgstore_mv_<name>`
with the same column-derivation logic.

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
  - SELECT `s3_key` FROM `s3pgstore_pending_writes`
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

**Source: pattern lifted from s3store** (`metrics.go`). Follow
s3store's OTel attribute conventions and `methodScope` helper for
consistent metric/log boundaries; the metric names themselves
become `s3pgstore.*` (different metric names, same shape).

- `methodScope(ctx, methodName)` helper: starts the
  duration histogram, captures attributes, returns a `scope` whose
  `end(*err)` records duration + outcome (success / error type).
  Same pattern as s3store, renamed metrics.
- OTel metrics:
  - `s3pgstore.method.duration` (histogram, labeled by `method`)
  - `s3pgstore.method.calls` (counter, labeled by `method`,
    `outcome`)
  - `s3pgstore.method.in_flight` (gauge, labeled by `method`)
  - `s3pgstore.write.bytes` (histogram)
  - `s3pgstore.write.records` (histogram)
  - `s3pgstore.write.encode_buf_dropped` (counter; non-zero rate
    indicates `EncodeBufPoolMaxBytes` is undersized for the
    workload — see Phase 3)
  - `s3pgstore.poll.lag` (gauge: now − latest `feed_seq_at`)
  - `s3pgstore.sequencer.assigned` (counter)
  - `s3pgstore.gc.reclaimed` (counter)
  - `s3pgstore.read.iter.stall.count` (counter; emitted by the
    iter pipeline's stall observer per CLAUDE.md § Concurrency
    invariants)
  - `s3pgstore.s3.transient_error.count` (counter, labeled by
    `error.type` — same as s3store after `bcabdb8`).
- slog structured logging at key boundaries (write commit, read
  start, GC reclaim, sequencer batch). Match s3store's log key
  conventions (`partition_key`, `file_id`, `error.type`).
- Error-classification helpers (sentinel + wrapped).
- **Grafana dashboard** at `dashboards/s3pgstore.json`. Mirrors
  s3store's dashboard structure: a single dashboard JSON, no
  rows-as-`row` collapse blocks (Grafana's deprecated rows
  attribute) — instead, panels are organized into **section
  rows** (Grafana `row` panel type) so operators can collapse
  concerns visually. Ships in this phase; the
  [CLAUDE.md § Metrics ↔ dashboard sync](CLAUDE.md#metrics--dashboard-sync)
  rule is in force from v2.0 onward.

  **Templating variables** (mirrors s3store):
  - `datasource_metrics` — Prometheus datasource selector.
  - `cluster`, `stage` — deployment-level filters.
  - `namespace` (queries `k8s_namespace_name` label).
  - `bucket` (queries `s3pgstore_bucket` label).
  - `prefix` (queries `s3pgstore_prefix` label).

  **Section rows** (adapted from s3store; one row per concern):

  1. **Overview — all stores** (independent of bucket/prefix).
     Headline stat panels: writes/sec, reads/sec, write P95,
     read P95, current feed lag.
  2. **Library methods — selected bucket/prefix**. Per-method
     duration P50+P95+P99, in-flight gauge, call rate,
     outcome breakdown.
  3. **Write volumes**. Bytes/Write (P50/P95/P99/P100 — the
     P100 is the `EncodeBufPoolMaxBytes` tuning loop; panel
     description must explain this), records/Write,
     files written/sec, encode-buffer dropped rate (the
     incident counter signalling cap is undersized).
  4. **Read volumes**. Bytes/Read, records/Read, partitions
     read, files fetched per call.
  5. **S3 operations**. PUT/GET/DELETE rates, request body
     sizes, request duration, transient-error rate by error
     type (the `s3pgstore.s3.transient_error.count` panel),
     SDK retry quota exhaustion if available.
  6. **Target saturation**. `MaxInflightRequests` semaphore
     wait time and current depth — operators tune the cap
     against this.
  7. **Fan-out**. Per-method partition counts, parallel-worker
     counts, fan-out item counts.
  8. **Iter pipeline saturation**. Body-slot wait, byte-budget
     wait, partition decode duration, stall counter (incident
     gauge). Panel descriptions reference the
     [CLAUDE.md Concurrency invariants](CLAUDE.md#concurrency-invariants).
  9. **Sequencer & feed**. `feed_seq` assignment rate, advisory-
     lock acquisition wait, NOTIFY round-trip latency, current
     unsequenced-row count, sequencer-iteration duration.
  10. **Catalog & locking**. Transaction commit duration,
      `LockPartition` acquisition wait, OCC version-conflict
      rate, `LookupByToken` hit rate, partition-row UPSERT
      duration.

  **Per-panel conventions** (every panel obeys these):

  - Quantile panels show **P50 + P95 + P99** always, plus
    **P100 on size and count histograms** (write/read records,
    partitions, bytes, files, fan-out items/workers, S3 body
    sizes) — `histogram_quantile(1.0, ...)` returns the upper
    bound of the highest non-empty bucket, which is the right
    signal for sizing `EncodeBufPoolMaxBytes` against actual
    peak parquet write size. Duration panels keep P50+P95+P99
    only (P100 on duration is one outlier dominating the panel).
  - Every `rate(...)` and `histogram_quantile(...)` query sets
    **`Min step 1m`** in Grafana's panel options — Prometheus's
    default step is the dashboard auto-step (~10s at typical
    refresh), which makes spiky rate panels at high resolution.
    1m matches the workload's natural granularity (per s3store
    commit `33b9b0d`).
  - Label filters in every panel mirror the templating
    variables verbatim — copy from a sibling panel rather than
    rewriting from scratch.
  - **Incident counters** (`encode_buf_dropped`,
    `iter.stall.count`, `s3.transient_error.count`,
    `commit_after_timeout` not applicable but use the same
    pattern, OCC `version_conflict.count`, GC failure rate, etc.)
    use the yellow-at-`0.001/s`, red-at-`0.1/s` threshold
    pattern with stat-panel red overlay so a non-zero rate is
    visually obvious.
  - Incident counters with single-series shape go into the
    `prewarm` slice in the metrics constructor so Prometheus
    `rate()` returns a value on the first non-zero sample
    (otherwise the chart shows `No data` until the second
    sample lands — confusing during incident triage).

  **Dashboard PR convention** (per CLAUDE.md sync rule): any
  metric added to the codebase lands with its panel in the
  same PR. Conversely, removing/renaming a metric requires
  removing/updating the corresponding panel. A panel querying
  a non-existent metric shows `No data` and erodes trust.
- **Histogram bucket boundaries** mirror s3store's `byteBuckets`
  (vendored from upstream's `metrics.go`), specifically including
  **8 MiB and 64 MiB** boundaries so the bytes/Write P100 panel
  resolves cap-tuning recommendations to "bump
  `EncodeBufPoolMaxBytes` to ~10 MiB vs ~16 MiB" and
  "~80 MiB vs ~150 MiB" rather than collapsing both to a single
  bucket. The 32 MiB extra boundary is also kept (2× resolution
  point at typical workload upper end). Two extra buckets across
  ~5 metrics × ~3 method labels = ~30 extra series per pod —
  modest cardinality cost for materially better cap tuning.
- **README section: "Tuning the Go runtime
  (`GOMEMLIMIT` / `GOGC`)"** mirroring s3store's `299b290`. The
  decode path we vendor allocates ~32× input file size per
  parquet body (most inside parquet-go, not reachable from this
  library), so the writer pool reduces encode-side floor but not
  the decode-side. For GC-bound services, `GOMEMLIMIT` and
  `GOGC` are the highest-leverage knobs. Section covers what
  each does, suggested starting values keyed to common pod
  memory limits (1Gi → `800MiB`, 2Gi → `1600MiB`, etc.; `GOGC`
  100–400 by tier), how to set them in K8s vs programmatically,
  three verification signals (GC CPU fraction / p99 / heap
  headroom) with PromQL queries against
  `go_runtime_metrics_total`, and gotchas (Go does NOT auto-detect
  cgroup memory limits the way Go 1.25+ does for `GOMAXPROCS`;
  pointer to `KimMachineGun/automemlimit` for auto-detection,
  with a "verify against current release notes" caveat).
- README expansion: full Quick Start with executable code, GORM
  integration example, schema-management workflow.
- Godoc on every exported symbol.

**Milestone:** production-ready instrumentation aligned with
s3store's conventions (operators running both libraries see
consistent attribute keys); a Grafana dashboard with one panel
per registered metric; README that a new user can follow without
reading the proposal.

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
