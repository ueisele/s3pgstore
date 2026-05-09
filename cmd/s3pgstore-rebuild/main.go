// Command s3pgstore-rebuild reconstructs the s3pgstore catalog
// from S3 alone — disaster-recovery tool. Walks <prefix>/data/
// via S3 LIST (the only place the library uses LIST), reads
// each Parquet file's footer for record_count, and INSERTs
// catalog rows.
//
// Environment variables:
//
//	S3PGSTORE_DATABASE_URL                       PostgreSQL DSN (required)
//	S3PGSTORE_S3_BUCKET                          S3 bucket (required)
//	S3PGSTORE_S3_PREFIX                          S3 prefix (default "")
//	S3PGSTORE_S3_ENDPOINT                        Optional non-AWS endpoint
//	S3PGSTORE_S3_REGION                          AWS region (default "us-east-1")
//	S3PGSTORE_S3_MAX_OPEN_CONNECTIONS            Cap concurrent TCP connections to S3 (default 64)
//	S3PGSTORE_S3_MAX_RETRY_ATTEMPTS              SDK retry budget per S3 op, 1 + retries (default 5)
//	S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND         Pre-throttle outgoing S3 ops to this rate (default 0 = unlimited)
//	S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND_BURST   Token bucket burst (default = max(1, rate*0.1))
//	S3PGSTORE_SCHEMA                             Schema name (default "public")
//	S3PGSTORE_TABLE_PREFIX                       Table prefix (default "s3pgstore_")
//	S3PGSTORE_PARTITION_KEY_PARTS                Comma-separated parts (required)
//
// Telemetry (opt-in; see cmd/internal/otelinit for details):
//
//	OTEL_METRICS_EXPORTER, OTEL_EXPORTER_OTLP_ENDPOINT,
//	OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, OTEL_SERVICE_NAME,
//	OTEL_RESOURCE_ATTRIBUTES, OTEL_METRIC_EXPORT_INTERVAL, ...
//
// The provider is installed as the OTel global so any future
// instrumentation added to the rebuild path emits without
// further wiring. The tool itself does not currently register
// instruments — at 2M files a rebuild is a multi-hour operation
// and the scaffolding is in place for that future work.
//
// Reads catalog DDL must already be applied (operators run
// SchemaManager.Create or their migration tool first); the
// tool is a row-level rebuild, not a schema-creating tool.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"

	"github.com/ueisele/s3pgstore/cmd/internal/otelinit"
	"github.com/ueisele/s3pgstore/internal/s3client"
)

func main() {
	if err := run(); err != nil {
		slog.Error("s3pgstore-rebuild exited with error",
			"err", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := os.Getenv("S3PGSTORE_DATABASE_URL")
	if dsn == "" {
		return errors.New("S3PGSTORE_DATABASE_URL is required")
	}
	bucket := os.Getenv("S3PGSTORE_S3_BUCKET")
	if bucket == "" {
		return errors.New("S3PGSTORE_S3_BUCKET is required")
	}
	partsRaw := os.Getenv("S3PGSTORE_PARTITION_KEY_PARTS")
	if partsRaw == "" {
		return errors.New(
			"S3PGSTORE_PARTITION_KEY_PARTS is required " +
				"(comma-separated, must match the writer's " +
				"PartitionKeyParts)")
	}
	parts := strings.Split(partsRaw, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}

	cfg := RebuildConfig{
		S3Bucket:          bucket,
		S3Prefix:          os.Getenv("S3PGSTORE_S3_PREFIX"),
		SchemaName:        getenvOr("S3PGSTORE_SCHEMA", "public"),
		TablePrefix:       getenvOr("S3PGSTORE_TABLE_PREFIX", "s3pgstore_"),
		PartitionKeyParts: parts,
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	meter, otelShutdown, err := otelinit.Setup(ctx,
		"s3pgstore-rebuild")
	if err != nil {
		return fmt.Errorf("otel setup: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(shutdownCtx); err != nil {
			slog.Warn("otel shutdown", "err", err)
		}
	}()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open pgxpool: %w", err)
	}
	defer pool.Close()
	cfg.Pool = pool

	// Threading meter through loadS3Client wires the
	// dashboard's s3pgstore.s3.{request,attempt.error,body_size,
	// ratelimit.wait,tcp.connections,connection.reuse}
	// instruments to fire over the multi-hour DR run.
	s3Client, err := loadS3Client(ctx, meter)
	if err != nil {
		return err
	}
	cfg.S3Client = s3Client

	slog.Info("s3pgstore-rebuild starting",
		"schema", cfg.SchemaName,
		"prefix", cfg.TablePrefix,
		"bucket", cfg.S3Bucket,
		"s3_prefix", cfg.S3Prefix,
		"partition_key_parts", cfg.PartitionKeyParts)

	res, err := Rebuild(ctx, cfg)
	if err != nil {
		return err
	}
	slog.Info("s3pgstore-rebuild complete",
		"files_inserted", res.FilesInserted,
		"partitions_inserted", res.PartitionsInserted)
	return nil
}

func loadS3Client(
	ctx context.Context, meter metric.Meter,
) (*s3.Client, error) {
	maxOpen, err := envIntNonNeg(
		"S3PGSTORE_S3_MAX_OPEN_CONNECTIONS")
	if err != nil {
		return nil, err
	}
	maxAttempts, err := envIntNonNeg(
		"S3PGSTORE_S3_MAX_RETRY_ATTEMPTS")
	if err != nil {
		return nil, err
	}
	maxRPS, err := envFloatNonNeg(
		"S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND")
	if err != nil {
		return nil, err
	}
	maxBurst, err := envIntNonNeg(
		"S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND_BURST")
	if err != nil {
		return nil, err
	}
	return s3client.BuildS3Client(ctx, s3client.Options{
		Region:                    os.Getenv("S3PGSTORE_S3_REGION"),
		Endpoint:                  os.Getenv("S3PGSTORE_S3_ENDPOINT"),
		MaxOpenConnections:        maxOpen,
		MaxRetryAttempts:          maxAttempts,
		MaxRequestsPerSecond:      maxRPS,
		MaxRequestsPerSecondBurst: maxBurst,
		Meter:                     meter,
	})
}

// envIntNonNeg parses a non-negative int env var. Empty → 0.
func envIntNonNeg(key string) (int, error) {
	n, err := envInt(key)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("%s %d: must be >= 0", key, n)
	}
	return n, nil
}

// envFloatNonNeg parses a non-negative float env var.
// Empty → 0.
func envFloatNonNeg(key string) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", key, v, err)
	}
	if f < 0 {
		return 0, fmt.Errorf("%s %g: must be >= 0", key, f)
	}
	return f, nil
}

// envInt parses an env var as int. Returns (0, nil) when unset.
func envInt(key string) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", key, v, err)
	}
	return n, nil
}

func getenvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
