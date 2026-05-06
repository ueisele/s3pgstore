# s3pgstore v2.1 — Design Sketch

*Builds on v2.0. Read [s3pgstore-proposal-v2.0.md](s3pgstore-proposal-v2.0.md)
first — this document only describes what changes.*

> **Status: design sketch.** v2.1 will not ship until v2.0 is in
> production. The shape below is intentionally provisional;
> implementation experience on v2.0 will inform the final design.

## Scope

What v2.1 adds:

- **Compaction** — consolidate many small parquet files into one
  larger file per partition.
- **Supersession tracking** — catalog records which files have been
  consolidated away; readers and snapshot queries filter on it.
- **Faster snapshot queries** — "live as of N" via supersession
  predicate, instead of v2.0's "all writes up to N".
- **Two-phase Poll offset** — compaction-aware historical replay.
- **Retention** — bounded-time deletion of superseded files (and
  optionally entire partitions).
- **Replication** — `cmd/s3pgstore-replicate` for one-way
  catalog+data replication to a secondary site.

What v2.1 keeps from v2.0 unchanged:

- Write API, OCC, `LockPartition`, transactional composition via
  `Executor`.
- MaterializedView (self-contained; not affected by compaction).
- ExtensionColumns.
- Sequencer pattern.
- Single-query Read with `MAX(written_at_version)`-derived Version
  — extended with a supersession filter (see below).

## Migration from v2.0

**None.** v2.1 may break the v2.0 schema. Deployments adopting
v2.1 re-create their catalog from scratch:

1. Stop writes on v2.0.
2. Drop v2.0 catalog tables.
3. Apply v2.1 DDL via the operator's migration tool.
4. Run `cmd/s3pgstore-rebuild` to walk `data/` in S3 and
   reconstruct catalog rows for every existing parquet file. Files
   land as `event_type='write'`, `superseded_at=NULL` — i.e.,
   "all writes are live, no compaction has happened yet."
5. Reset stream-consumer offsets (feed_seq starts at 1 again).
6. Resume writes on v2.1.

This is acceptable because v2.0 is targeted at workloads where the
catalog is small enough that rebuilding from S3 is feasible (single
to double-digit minutes for typical billing-pipeline scale).

---

## Schema additions

### `s3pgstore_files`

```sql
ALTER TABLE s3pgstore_files
    ADD COLUMN event_type             TEXT NOT NULL DEFAULT 'write',
    ADD COLUMN supersedes              BIGINT[],
    ADD COLUMN superseded_by           BIGINT REFERENCES s3pgstore_files(file_id),
    ADD COLUMN superseded_at           TIMESTAMPTZ,
    ADD COLUMN superseded_at_version   BIGINT,
    ADD COLUMN superseded_at_feed_seq  BIGINT;

-- Live-files lookup (read path; default partial index keeps it small)
CREATE INDEX ON s3pgstore_files (partition_key, written_at_version)
    WHERE superseded_at IS NULL;

-- Snapshot-as-of queries (partition-scoped)
CREATE INDEX ON s3pgstore_files (partition_key, written_at_version, superseded_at_version);

-- Snapshot-as-of queries (global)
CREATE INDEX ON s3pgstore_files (feed_seq, superseded_at_feed_seq)
    WHERE feed_seq IS NOT NULL;
```

Per-row semantics:

| Column | For 'write' rows | For 'compaction' rows |
|---|---|---|
| `event_type` | `'write'` | `'compaction'` |
| `supersedes` | NULL | array of file_ids being consolidated |
| `superseded_by` | NULL while live; set when compacted away | NULL (a compaction output is itself live) |
| `superseded_at`, `superseded_at_version`, `superseded_at_feed_seq` | NULL while live; set when compacted away | NULL |

A v2.0 row read by v2.1 (during rebuild) defaults to
`event_type='write'`, supersession columns NULL — semantically
"live and never compacted."

### `s3pgstore_partitions`

```sql
ALTER TABLE s3pgstore_partitions
    ADD COLUMN last_compact_at TIMESTAMPTZ;
```

Bookkeeping only; not on the correctness path.

---

## Compaction

```go
type CompactOpts struct {
    MinInputFiles  int     // skip if fewer live files than this; default 10
    MaxInputFiles  int     // upper bound per partition per call; default 100
    OutputMaxBytes int64   // soft cap on consolidated parquet size; default 256 MiB
}

type CompactResult struct {
    PartitionKey   string
    OutputFileID   int64
    InputFileIDs   []int64
    BytesReclaimed int64   // sum of input file_size − output file_size
}

func (s *Store[T]) Compact(
    ctx context.Context,
    filters []PartitionFilter,
    opts CompactOpts,
) ([]CompactResult, error)
```

### Algorithm

For each partition matched by `filters`:

1. Acquire a partition-level **compaction advisory lock** (a
   distinct `pg_advisory_xact_lock` key from `LockPartition`'s —
   compaction must not block regular writers, only other
   compactions on the same partition).
2. SELECT live files: `WHERE partition_key = $key AND superseded_at
   IS NULL ORDER BY feed_seq LIMIT MaxInputFiles`. Stop earlier if
   accumulated `file_size` exceeds `OutputMaxBytes`.
3. If count `< MinInputFiles`, skip this partition; release lock.
4. GET input parquet files from S3 in parallel.
5. Decode, dedup via `EntityKeyOf` + `VersionOf` (latest-version
   per entity), encode into a single consolidated parquet.
6. PUT the consolidated parquet to a fresh UUID key. INSERT into
   `s3pgstore_pending_writes` for orphan tracking.
7. Catalog transaction (via `Executor.RunInTx`):
   - INSERT `s3pgstore_files` row with `event_type='compaction'`,
     `supersedes=<input_file_ids>`, `written_at_version=current+1`.
   - UPDATE input rows: `superseded_by=<new_id>`,
     `superseded_at=now()`,
     `superseded_at_version=current+1`,
     `superseded_at_feed_seq=NULL` (sequencer fills it later
     atomically with `feed_seq` of the compaction event).
   - UPDATE `s3pgstore_partitions`: `version=version+1`,
     `last_compact_at=now()`.
   - DELETE pending-writes row.
8. Release compaction advisory lock.

### Concurrency

| Combination | Behavior |
|---|---|
| Compaction × compaction (same partition) | Serialized by compaction advisory lock. |
| Compaction × regular write | Brief contention on partition row UPDATE in step 7; PostgreSQL row-locking serializes. Both bump version. |
| Compaction × `LockPartition` | Cooperative — compaction must opt in to `LockPartition`'s advisory key before step 2 to serialize with held locks. Open design question for v2.1 implementation: see TODO below. |
| Compaction × Read | Atomic from reader's perspective: snapshot before compaction commits sees inputs as live; after, sees output as live. Never partial. |

### What compaction does and doesn't

**Does:** consolidate input rows into one output parquet, reduce
read fan-out, mark inputs as superseded.

**Doesn't:**

- Delete input parquets from S3. That's retention's job.
- Change records — output contains the same logical records as
  inputs (after dedup).
- Touch MaterializedViews — they're self-contained, hold no
  reference to file_ids, and don't need maintenance during
  compaction.
- Change ExtensionColumns of input rows. The compaction output
  row gets its own `WithMetadata` (passed via `CompactOpts`?
  open question).

### Open question: metadata on the compaction output row

A compaction output row needs `ExtensionColumns` populated. Two
options:

- **Inherit from inputs** with a deterministic merge rule (e.g.,
  most-recent value per column). Simplest; no operator burden.
- **Caller-supplied via `CompactOpts.Metadata`** — explicit, but
  the operator running `cmd/s3pgstore-compact` may not have
  domain context to populate them.

Likely answer: inherit by default (most-recent value); allow
`CompactOpts.Metadata` to override per call.

---

## Read path changes

The single-query Read picks up a supersession filter:

```sql
SELECT partition_key, file_id, s3_key, written_at_version, ext_*, ...
FROM s3pgstore_files
WHERE <part_<n> predicates>
  AND superseded_at IS NULL  -- only live files
ORDER BY partition_key, feed_seq;
```

The invariant `MAX(written_at_version) per partition equals
partition.version` still holds — compaction bumps version too,
and the compaction output row is live with the bumped version. So
`MAX(written_at_version)` over live files still equals current
version.

Records returned: same logical state as v2.0; vastly fewer files
to fetch on hot partitions.

---

## Snapshot queries

```sql
-- Partition P at version N (v2.1):
SELECT s3_key FROM s3pgstore_files
WHERE partition_key = $1
  AND written_at_version <= $N
  AND (superseded_at_version IS NULL OR superseded_at_version > $N);

-- Full dataset at feed_seq N (v2.1):
SELECT s3_key FROM s3pgstore_files
WHERE feed_seq IS NOT NULL
  AND feed_seq <= $N
  AND (superseded_at_feed_seq IS NULL OR superseded_at_feed_seq > $N);
```

Returns "live as of N" — files that existed and weren't yet
superseded at coordinate N. Far fewer rows than v2.0's "all up to
N", and the result decodes to the partition's logical state at N
without further dedup work.

API:

```go
func (s *Store[T]) ReadAtVersion(ctx, partitionKey string, version int64) (PartitionResult[T], error)
func (s *Store[T]) ReadAtFeedSeq(ctx, filters []PartitionFilter, feedSeq int64) ([]PartitionResult[T], error)
```

---

## Two-phase Poll offset

The offset becomes a tuple:

```go
type Offset struct {
    Cursor       int64  // consumer's current position
    StartFeedSeq int64  // feed tip when the consumer started
}

func (s *Store[T]) NewOffset(ctx context.Context) (Offset, error)
```

Two phases handled automatically based on the offset:

**Historical replay** (`Cursor < StartFeedSeq`) — consumer is
catching up from an old position. Files superseded *before* the
consumer started are filtered out (the consumer should see the
original write events instead of the compaction event that hides
them). Predicate:

```sql
WHERE feed_seq > $cursor
  AND feed_seq <= $start_feed_seq
  AND (superseded_at_feed_seq IS NULL OR superseded_at_feed_seq > $start_feed_seq)
```

**Live tailing** (`Cursor >= StartFeedSeq`) — consumer is caught
up. Compaction events are skipped (they carry no new logical
information; the records they consolidated were already delivered
in their original write events):

```sql
WHERE feed_seq > $cursor AND event_type = 'write'
```

The phase transition is automatic when `Cursor` reaches
`StartFeedSeq`.

### Why this works

- **No record is missed.** A consumer at offset 0 with
  `StartFeedSeq=N` sees: every write up to N (filtered by "not
  yet superseded by N", i.e., the consolidated representation of
  what was live at N) + every write after N (live tailing).
  Compaction events that consolidated inputs from before N into
  outputs after N are filtered out of historical because their
  inputs are still live-at-N; they're filtered out of live
  tailing because event_type='write'. Either way, no
  duplication.
- **No record is duplicated.** A record observed during
  historical replay (in its original write event) is not seen
  again during live tailing of the compaction event that
  consolidated it (filtered by event_type='write').
- **Replay stability.** Offset (cursor, start) deterministically
  produces the same sequence on every replay — the predicate is
  parameterized on `start_feed_seq`, which the consumer pins on
  first call.

### Migration from v2.0 offsets

v2.0 offsets are bare `int64` cursors. v2.1 offsets are tuples.
The catalog rebuild on v2.0→v2.1 reassigns `feed_seq` from 1
anyway, so consumers reset their offsets (they were going to
have to anyway). Use `NewOffset(ctx)` to get a fresh tuple
positioned at the current tip.

---

## Retention

```go
type RetentionPolicy struct {
    SupersededOlderThan time.Duration  // delete superseded files older than this
}

func (s *Store[T]) Retain(ctx context.Context, policy RetentionPolicy) (RetainResult, error)
```

Two-phase delete to preserve replay correctness for in-flight
consumers:

1. **Mark phase:** SELECT files where `superseded_at < now() -
   SupersededOlderThan`. Add `marked_for_deletion_at = now()`
   column or move to a separate marking table.
2. **Delete phase:** wait an additional grace period (e.g., 24h)
   to let any active `Poll`/`PollRecords` finish. Then DELETE
   catalog rows and the corresponding S3 objects.

A `cmd/s3pgstore-retain` tool runs this periodically.

Open question: should marked-but-not-yet-deleted files stay
visible to historical Poll, or be filtered out at marking time?
Answer probably "visible until delete-phase, then both gone" —
keep marking lightweight.

Future extensions (not in initial v2.1): partition-age retention
("drop entire partitions older than X"), MV-row retention, etc.
Each has its own semantics and cleanup sequencing.

---

## Replication

`cmd/s3pgstore-replicate` is a one-way replicator: primary catalog
+ S3 bucket → secondary catalog + S3 bucket.

### Algorithm

1. Connect to primary's catalog. Connect to secondary's catalog.
   Connect to both S3 endpoints.
2. Load secondary's last-replicated `feed_seq` from a checkpoint
   table the replicator owns on the secondary.
3. Poll primary:
   ```sql
   SELECT * FROM s3pgstore_files
   WHERE feed_seq > $checkpoint
   ORDER BY feed_seq
   LIMIT $batch;
   ```
4. For each row, in feed_seq order:
   - Copy S3 object from primary's bucket to secondary's bucket
     (server-side copy if same provider, else stream through the
     replicator).
   - Run all of the following in one transaction on the secondary:
     - INSERT row into secondary's `s3pgstore_files`. Preserve
       primary's `file_id`, `feed_seq`, `feed_seq_at`,
       `written_at_version`, `idempotency_token`, etc. ON
       CONFLICT (file_id) DO NOTHING for restartability.
     - For event_type='compaction': the row's `supersedes`
       references file_ids that already exist on secondary
       (replicated earlier in feed_seq order). UPDATE those
       rows' `superseded_*` columns from the row's data.
     - INSERT/UPDATE the partition row: bump version, update
       last_compact_at.
   - Update checkpoint after the transaction commits.
5. Loop.

### Caveats

- **Order is load-bearing.** Replicate strictly in `feed_seq`
  order so compaction events arrive after the writes they
  supersede.
- **MV rows are NOT replicated.** They're self-contained and
  computed from records via the writer's `Of` closure. The
  secondary either:
  - Re-computes MV rows during replication using a
    config-supplied set of `MaterializedViewDef[T]` (matching
    primary's), OR
  - Skips MVs entirely (read-only secondary that doesn't expose
    Lookup).
  Configuration choice.
- **ExtensionColumns must match.** Replicator validates at
  startup that secondary's `s3pgstore_files` schema declares the
  same `ext_*` columns as primary's; aborts on mismatch.
- **Idempotence.** Every step uses `ON CONFLICT` semantics so a
  crashed replicator can resume from the checkpoint without
  double-applying.
- **Schema management.** Replicator assumes secondary's catalog
  DDL has already been applied. It does not run DDL.

### What this enables

- **Read scaling:** secondaries serve read traffic at remote
  sites; primary handles writes.
- **DR posture:** if primary is lost, promote secondary, point
  writers at it, replay any in-flight S3 PUTs.
- **Cross-region cost reduction:** keep cold S3 data on the
  primary site; replicate hot partitions to nearby secondaries.

---

## API additions (sketch)

```go
package s3pgstore

// Compaction
type CompactOpts struct {
    MinInputFiles  int
    MaxInputFiles  int
    OutputMaxBytes int64
    Metadata       map[string]any  // override inherit-from-inputs
}

type CompactResult struct {
    PartitionKey   string
    OutputFileID   int64
    InputFileIDs   []int64
    BytesReclaimed int64
}

func (s *Store[T]) Compact(ctx context.Context, filters []PartitionFilter, opts CompactOpts) ([]CompactResult, error)

// Snapshot queries
func (s *Store[T]) ReadAtVersion(ctx context.Context, partitionKey string, version int64) (PartitionResult[T], error)
func (s *Store[T]) ReadAtFeedSeq(ctx context.Context, filters []PartitionFilter, feedSeq int64) ([]PartitionResult[T], error)

// Two-phase offset
type Offset struct {
    Cursor       int64
    StartFeedSeq int64
}

func (s *Store[T]) NewOffset(ctx context.Context) (Offset, error)
// Poll signature unchanged at the call site; internally reads the tuple.
func (s *Store[T]) Poll(ctx context.Context, since Offset, n int) ([]StreamEntry, Offset, error)

// Retention
type RetentionPolicy struct {
    SupersededOlderThan time.Duration
}

type RetainResult struct {
    MarkedRows  int
    DeletedRows int
    DeletedS3   int
}

func (s *Store[T]) Retain(ctx context.Context, policy RetentionPolicy) (RetainResult, error)
```

New binaries:

- `cmd/s3pgstore-compact` — runs `Compact` against partitions
  matching configured filters on a schedule.
- `cmd/s3pgstore-retain` — runs the two-phase retention sweep.
- `cmd/s3pgstore-replicate` — feed-based replicator described above.

---

## Open questions

These land as we implement.

- **Compaction trigger policy.** Manual via `Compact()` only, or
  an automatic threshold (e.g., "auto-compact when partition has >
  N live files")? Manual is simpler; automatic needs a daemon and
  policy config.
- **Compaction output metadata.** Inherit from inputs by default,
  or require explicit `CompactOpts.Metadata`?
- **Retention granularity.** Just superseded-older-than for v2.1,
  or also age-based and entity-based ("drop oldest version of every
  entity older than X")?
- **Replicator MV story.** Re-compute on secondary, or always
  skip? Configuration knob, or one or the other?
- **Replicator backpressure.** What happens when secondary falls
  behind enough that primary's `cmd/s3pgstore-retain` deletes a
  file the replicator hasn't copied yet? Likely: replicator must
  hold a "replication horizon" that retain respects.

These are real open questions, not just naming bikesheds; we'll
answer them with implementation experience on v2.0.
