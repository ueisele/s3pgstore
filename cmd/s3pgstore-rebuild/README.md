# s3pgstore-rebuild

Reconstructs the s3pgstore catalog from S3 alone. Disaster
recovery tool: walks `<prefix>/data/` via S3 LIST, reads each
parquet file's user-metadata bag (or footer for legacy files),
and INSERTs the matching `s3pgstore_files` and
`s3pgstore_partitions` rows.

This is the only place the library uses S3 LIST. The runtime
read/write path never LISTs.

## What it recovers, what it doesn't

| Field                | Source                                                      |
| -------------------- | ----------------------------------------------------------- |
| `partition_key`      | Derived from the S3 key path against `S3PGSTORE_PARTITION_KEY_PARTS`. |
| `s3_key`             | The discovered S3 object key.                               |
| `written_at_version` | S3 user-metadata `s3pgstore-version`.                       |
| `record_count`       | S3 user-metadata `s3pgstore-records`.                       |
| `uncompressed_size`  | S3 user-metadata `s3pgstore-uncompressed`.                  |
| `file_size`          | S3 HEAD `ContentLength`.                                    |
| `written_at`         | S3 `LastModified` (best-available proxy).                   |
| `idempotency_token`  | S3 user-metadata `s3pgstore-token` (when present).          |
| `ext_<name>`         | S3 user-metadata `s3pgstore-ext-<name>` (when present).     |
| `feed_seq`           | Left `NULL` — the sequencer reassigns on its next run.      |

**Materialized views are NOT rebuilt.** MVs depend on per-record
data, which can only be reconstructed by re-reading every
parquet body. Operators needing MVs after DR re-run their MV
population pipeline.

**Files lacking the s3pgstore user-metadata bag fail rebuild.**
They were uploaded outside the library or by a pre-metadata
version. Remove or re-upload them before re-running.

## Configuration

### Required

| Variable                        | Description                                                              |
| ------------------------------- | ------------------------------------------------------------------------ |
| `S3PGSTORE_DATABASE_URL`        | PostgreSQL connection string. Catalog DDL must already exist.            |
| `S3PGSTORE_BUCKET`              | S3 bucket containing the parquet objects.                                |
| `S3PGSTORE_PARTITION_KEY_PARTS` | Comma-separated part names — must match the writer's `PartitionKeyParts`. |

### Optional

| Variable                 | Default       | Description                                              |
| ------------------------ | ------------- | -------------------------------------------------------- |
| `S3PGSTORE_S3_PREFIX`    | _(empty)_     | S3 prefix matching the writer's `Config.Prefix`.         |
| `S3PGSTORE_S3_ENDPOINT`  | _(AWS)_       | Override for non-AWS S3 (e.g. `http://minio:9000`).      |
| `S3PGSTORE_S3_REGION`    | `us-east-1`   | AWS region.                                              |
| `S3PGSTORE_S3_MAX_INFLIGHT_REQUESTS` | `32` | Caps simultaneous S3 requests; sized in lockstep with the HTTP transport's connection pool. Higher values speed up multi-million-file rebuilds at the cost of socket pressure. |
| `S3PGSTORE_SCHEMA`       | `public`      | Schema containing the s3pgstore tables.                  |
| `S3PGSTORE_TABLE_PREFIX` | `s3pgstore_`  | Table-name prefix (must match the writer).               |

### Telemetry (opt-in)

The MeterProvider is installed as the OTel global so any future
internal instrumentation flows through automatically. The tool
itself does not currently register instruments — at 2M files a
rebuild is a multi-hour operation and the scaffolding is in
place for that future work.

| Variable                              | Description                                          |
| ------------------------------------- | ---------------------------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT`         | OTLP collector endpoint.                             |
| `OTEL_METRICS_EXPORTER`               | `otlp` (default), `prometheus`, `console`, `none`.   |
| `OTEL_METRIC_EXPORT_INTERVAL`         | Push interval in milliseconds (default `60000`).     |
| `OTEL_SERVICE_NAME`                   | Overrides the default `s3pgstore-rebuild`.           |

## Run

### Locally (Go)

```sh
export S3PGSTORE_DATABASE_URL="postgres://s3pgstore:s3pgstore@localhost:5432/s3pgstore?sslmode=disable"
export S3PGSTORE_BUCKET=my-bucket
export S3PGSTORE_PARTITION_KEY_PARTS="tenant,date"
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

go run ./cmd/s3pgstore-rebuild
```

### Docker

```sh
docker pull ueisele/s3pgstore-rebuild:latest

docker run --rm \
  -e S3PGSTORE_DATABASE_URL="postgres://s3pgstore:s3pgstore@host.docker.internal:5432/s3pgstore?sslmode=disable" \
  -e S3PGSTORE_BUCKET=my-bucket \
  -e S3PGSTORE_PARTITION_KEY_PARTS="tenant,date" \
  -e AWS_ACCESS_KEY_ID="..." \
  -e AWS_SECRET_ACCESS_KEY="..." \
  -e AWS_REGION=eu-central-1 \
  ueisele/s3pgstore-rebuild:latest
```

### Build the image locally

From the repository root:

```sh
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f cmd/s3pgstore-rebuild/Dockerfile \
  -t ueisele/s3pgstore-rebuild:dev \
  --load .
```

## Operational notes

- **Apply schema first.** This tool only INSERTs rows; it does
  not create tables. Run `SchemaManager.Create` (or your
  migration tool) before invoking it.
- **Idempotent on re-run.** `ON CONFLICT (s3_key) DO NOTHING`
  for files; partitions UPSERT to the recomputed values. Safe
  to re-invoke after a partial failure.
- **Run the sequencer afterward.** Recovered rows have
  `feed_seq = NULL`; consumers need it assigned to replay the
  stream. Start `s3pgstore-sequencer` after rebuild completes.
- **Time budget at scale.** With ~2M files, expect a multi-hour
  run dominated by S3 HEAD throughput. Plan rebuild against the
  per-second HEAD ceiling of your bucket and the parallelism
  the tool exercises. `S3PGSTORE_S3_MAX_INFLIGHT_REQUESTS` is
  the dial for that parallelism — raise it to push more HEADs
  at the bucket; lower if S3 starts returning `SlowDown`.
- **StorageGRID compatibility.** The S3 client uses
  `ResponseChecksumValidation = WhenRequired` rather than the
  SDK default `WhenSupported` — StorageGRID logs a warning on
  every GET otherwise.
- **Exit codes.** `0` on success; `1` on any error (logged via
  `slog`). The output line `s3pgstore-rebuild complete` reports
  `files_inserted` and `partitions_inserted` counts.
