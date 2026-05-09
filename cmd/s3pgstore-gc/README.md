# s3pgstore-gc

Reclaims S3 objects whose s3pgstore catalog transactions rolled
back (or never committed within a grace period). The library
inserts a `s3pgstore_pending_writes` row before every PUT and
deletes it inside the wrapping transaction; rows that survive
the grace window are orphans whose S3 keys this tool deletes,
then removes the pending_writes row.

Two modes:

- **Loop mode (default)** — runs until `SIGTERM` / `SIGINT`,
  sweeping every `S3PGSTORE_INTERVAL`.
- **One-shot mode** (`S3PGSTORE_ONESHOT=1`) — performs one
  sweep and exits. Useful for cron jobs or Kubernetes `Job`s.

## Configuration

### Required

| Variable                 | Description                                                        |
| ------------------------ | ------------------------------------------------------------------ |
| `S3PGSTORE_DATABASE_URL` | PostgreSQL connection string.                                      |
| `S3PGSTORE_S3_BUCKET`    | S3 bucket containing the orphans (must match the writer's bucket). |

### Optional

| Variable                 | Default       | Description                                                                                  |
| ------------------------ | ------------- | -------------------------------------------------------------------------------------------- |
| `S3PGSTORE_S3_ENDPOINT`  | _(AWS)_       | Override for non-AWS S3 (e.g. `http://minio:9000`).                                          |
| `S3PGSTORE_S3_REGION`    | `us-east-1`   | AWS region.                                                                                  |
| `S3PGSTORE_S3_MAX_OPEN_CONNECTIONS` | `64` | Caps concurrent TCP connections to S3 (drives `MaxConnsPerHost` / `MaxIdleConnsPerHost` / `MaxIdleConns`). This is the global concurrency ceiling — set to your share of the backend's per-client limit (e.g. STACKIT 500 ÷ N replicas with ~10% headroom). For "effectively unlimited", set a large value like 1000. |
| `S3PGSTORE_S3_MAX_RETRY_ATTEMPTS` | `5` | SDK retry budget per logical S3 op (1 initial + retries). Equal-jitter backoff windows 100ms..10s; worst-case wallclock for an exhausted budget at default ≈ 6 s. |
| `S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND` | _(unset = unlimited)_ | Pre-throttle outgoing S3 ops via a client-side token bucket. Set when the backend has a known per-second ceiling that concurrency alone can't enforce — STACKIT 2500 RPS, AWS S3 per-prefix 5500 GET / 3500 PUT. Pre-shaping avoids the adaptive-retry "discover via SlowDown" feedback loop. |
| `S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND_BURST` | `max(1, rate*0.1)` | Token bucket burst — caps the post-idle 1-second excursion at +10% over the sustained rate (worst-case 1s ops = `burst + rate`). Tighten on backends with strict per-second windows by setting this lower (e.g. `1`). |
| `S3PGSTORE_SCHEMA`       | `public`      | Schema containing the s3pgstore tables.                                                      |
| `S3PGSTORE_TABLE_PREFIX` | `s3pgstore_`  | Table-name prefix (must match the writer).                                                   |
| `S3PGSTORE_GRACE`        | `24h`         | Minimum age before a `pending_writes` row is reclaimed. Must exceed the longest writer's max retry duration plus clock skew. |
| `S3PGSTORE_INTERVAL`     | `1h`          | Loop period (ignored when `S3PGSTORE_ONESHOT=1`).                                            |
| `S3PGSTORE_BATCH_SIZE`   | `1000`        | Rows reclaimed per `RunOnce` call. Larger batches sweep faster but hold the lock longer.     |
| `S3PGSTORE_ONESHOT`      | _(unset)_     | `1` / `true` → run one sweep and exit. Anything else → loop.                                 |

AWS credentials follow the standard AWS SDK chain (env vars,
shared credentials file, IRSA, IMDS). For non-AWS S3 (MinIO,
StorageGRID), set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

The S3 client is built with `ResponseChecksumValidation =
WhenRequired` (vs the SDK default `WhenSupported`) so
StorageGRID doesn't log a warning on every GET — it returns no
body checksum on ordinary objects and the SDK's default policy
asks for one regardless.

Retry policy is `aws.RetryModeAdaptive` with a 5-attempt
budget. The SDK's adaptive token bucket pre-throttles outgoing
requests after SlowDown responses (HTTP 429 or HTTP 503 with
`SlowDown` code) and grows the rate back on successes.
Combined with the hard `MaxOpenConnections` cap, this keeps the
client well under most backend rate ceilings without ever
manually tuning a backoff schedule.

### Telemetry (opt-in)

Same OTel env vars as the other s3pgstore-* binaries. See the
[`otelinit` package doc](../internal/otelinit/otelinit.go).

| Variable                              | Description                                          |
| ------------------------------------- | ---------------------------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT`         | OTLP collector endpoint.                             |
| `OTEL_METRICS_EXPORTER`               | `otlp` (default), `prometheus`, `console`, `none`.   |
| `OTEL_METRIC_EXPORT_INTERVAL`         | Push interval in milliseconds (default `60000`).     |
| `OTEL_SERVICE_NAME`                   | Overrides the default `s3pgstore-gc`.                |

## Run

### Locally (Go)

```sh
export S3PGSTORE_DATABASE_URL="postgres://s3pgstore:s3pgstore@localhost:5432/s3pgstore?sslmode=disable"
export S3PGSTORE_S3_BUCKET=my-bucket
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Loop mode
go run ./cmd/s3pgstore-gc

# One-shot
S3PGSTORE_ONESHOT=1 go run ./cmd/s3pgstore-gc
```

### Docker

```sh
docker pull ueisele/s3pgstore-gc:latest

docker run --rm \
  -e S3PGSTORE_DATABASE_URL="postgres://s3pgstore:s3pgstore@host.docker.internal:5432/s3pgstore?sslmode=disable" \
  -e S3PGSTORE_S3_BUCKET=my-bucket \
  -e AWS_ACCESS_KEY_ID="..." \
  -e AWS_SECRET_ACCESS_KEY="..." \
  -e AWS_REGION=eu-central-1 \
  -e S3PGSTORE_GRACE=48h \
  -e S3PGSTORE_INTERVAL=15m \
  ueisele/s3pgstore-gc:latest

# One-shot for a Kubernetes CronJob:
docker run --rm \
  -e S3PGSTORE_DATABASE_URL=... \
  -e S3PGSTORE_S3_BUCKET=... \
  -e S3PGSTORE_ONESHOT=1 \
  ueisele/s3pgstore-gc:latest
```

### Build the image locally

From the repository root:

```sh
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f cmd/s3pgstore-gc/Dockerfile \
  -t ueisele/s3pgstore-gc:dev \
  --load .
```

## Operational notes

- **Sizing the grace period.** Set `S3PGSTORE_GRACE` greater
  than the longest possible time between a writer's PUT and its
  catalog COMMIT — typically dominated by retries on a slow PG
  primary. 24h is a generous default; tighten it once you know
  your writers' worst case.
- **Multiple replicas safe but redundant.** Like the sequencer,
  gc serializes via advisory lock. Run two for failover; expect
  one to be idle.
- **Doesn't drop catalog rows.** GC only touches
  `s3pgstore_pending_writes` and S3 objects it points at. The
  authoritative `s3pgstore_files` and `s3pgstore_partitions`
  rows are never modified — read-stability invariant.
- **Exit codes.** `0` on clean shutdown (signal in loop mode, or
  successful one-shot completion); `1` on any other error.

## Health and metrics

Recommended panels to watch when telemetry is enabled:

- `s3pgstore.gc.reclaimed.count` — rate of orphan deletions.
  Sustained high rate means writes are aborting often.
- `s3pgstore.pending_writes.depth` — should oscillate near zero
  in steady state. Sustained growth means the gc loop isn't
  keeping up (raise `S3PGSTORE_INTERVAL` cadence or
  `S3PGSTORE_BATCH_SIZE`).
- `s3pgstore.s3.request.duration` (`op="DeleteObject"`) — S3
  latency on the delete path.
