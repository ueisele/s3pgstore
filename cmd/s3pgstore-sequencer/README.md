# s3pgstore-sequencer

Assigns gap-free `feed_seq` values to committed s3pgstore
catalog rows so consumers can replay the stream by offset. Runs
until `SIGTERM` / `SIGINT`. Serializes assignments via
`pg_advisory_xact_lock` over a fixed key — multiple instances
are safe to deploy (only one holds the lock at a time), but only
one will be productive.

## Configuration

All configuration is environment-variable based.

### Required

| Variable                   | Description                               |
| -------------------------- | ----------------------------------------- |
| `S3PGSTORE_DATABASE_URL`   | PostgreSQL connection string (`postgres://user:pass@host:5432/db?sslmode=…`). |

### Optional

| Variable                   | Default              | Description                                                                 |
| -------------------------- | -------------------- | --------------------------------------------------------------------------- |
| `S3PGSTORE_SCHEMA`         | `public`             | Schema containing the s3pgstore tables.                                     |
| `S3PGSTORE_TABLE_PREFIX`   | `s3pgstore_`         | Table-name prefix (must match the writer's configuration).                  |
| `S3PGSTORE_NOTIFY_CHANNEL` | `s3pgstore_writes`   | `LISTEN` channel for write notifications. Set empty to disable LISTEN and rely entirely on polling. |
| `S3PGSTORE_POLL_INTERVAL`  | `1s`                 | Fallback poll cadence when LISTEN is disabled or hasn't fired.              |
| `S3PGSTORE_BATCH_SIZE`     | `1000`               | Rows assigned per advisory-lock cycle.                                      |

### Telemetry (opt-in)

Setting any one of the OTel exporter env vars enables metric push.
With none set, telemetry is silent (no-op meter). See the
[`otelinit` package doc](../internal/otelinit/otelinit.go) for the
full env-var list.

| Variable                              | Description                                          |
| ------------------------------------- | ---------------------------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT`         | OTLP collector endpoint (gRPC default port `4317`).  |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | Metrics-specific endpoint override.                  |
| `OTEL_METRICS_EXPORTER`               | `otlp` (default), `prometheus`, `console`, `none`.   |
| `OTEL_EXPORTER_OTLP_PROTOCOL`         | `grpc` (default) or `http/protobuf`.                 |
| `OTEL_METRIC_EXPORT_INTERVAL`         | Push interval in milliseconds (default `60000`).     |
| `OTEL_SERVICE_NAME`                   | Overrides the default `s3pgstore-sequencer`.         |
| `OTEL_RESOURCE_ATTRIBUTES`            | Comma-separated extra resource attributes.           |

## Run

### Locally (Go)

```sh
export S3PGSTORE_DATABASE_URL="postgres://s3pgstore:s3pgstore@localhost:5432/s3pgstore?sslmode=disable"
go run ./cmd/s3pgstore-sequencer
```

### Locally (built binary)

```sh
go build -o s3pgstore-sequencer ./cmd/s3pgstore-sequencer
./s3pgstore-sequencer
```

### Docker

```sh
# Pull
docker pull ueisele/s3pgstore-sequencer:latest

# Run
docker run --rm \
  -e S3PGSTORE_DATABASE_URL="postgres://s3pgstore:s3pgstore@host.docker.internal:5432/s3pgstore?sslmode=disable" \
  -e S3PGSTORE_NOTIFY_CHANNEL=s3pgstore_writes \
  -e OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4317" \
  ueisele/s3pgstore-sequencer:latest
```

### Build the image locally

From the repository root:

```sh
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f cmd/s3pgstore-sequencer/Dockerfile \
  -t ueisele/s3pgstore-sequencer:dev \
  --load .
```

(`--load` only works for a single platform at a time; drop it
and add `--push` to publish a multi-arch manifest.)

## Operational notes

- **Run continuously.** The sequencer is a long-running daemon,
  not a cron job. It blocks on `LISTEN` and ticks the poll
  interval as a fallback.
- **Multiple replicas are safe but only one is active.** All
  replicas attempt `pg_advisory_xact_lock`; only the holder
  assigns rows. This is the canonical HA pattern — deploy two
  replicas in Kubernetes for failover, but expect one to log
  "waiting for lock" and the other to do the work.
- **Catalog DDL must already exist.** The sequencer queries
  `s3pgstore_files` directly; it does not create tables. Run the
  writer's `SchemaManager.Create` or your migration tool first.
- **Exit codes.** `0` on clean shutdown via signal; `1` on any
  other error (logged via `slog`).

## Health and metrics

When telemetry is enabled, the sequencer registers its own
instruments. Operationally important ones to watch:

- `s3pgstore.sequencer.assigned.count` — counter of rows
  assigned a `feed_seq` per cycle. Healthy non-zero rate means
  the sequencer is keeping up with writes.
- `s3pgstore.sequencer.unsequenced` — observable gauge of rows
  awaiting assignment. Sustained growth means writes outpace
  the sequencer (raise `S3PGSTORE_BATCH_SIZE`, lower
  `S3PGSTORE_POLL_INTERVAL`, or check PG load).
- `s3pgstore.sequencer.lock_wait` — histogram of advisory-lock
  acquisition latency. Bimodal (one mode near zero, one mode
  ~poll-interval) is expected when running multiple replicas
  for failover; one mode dominating ~poll-interval on a single
  replica indicates lock contention from another holder.
