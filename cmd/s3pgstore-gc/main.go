// Command s3pgstore-gc reclaims S3 objects whose s3pgstore
// catalog transactions rolled back (or never committed within a
// grace period). Reads its configuration from environment
// variables; supports one-shot and loop modes.
//
// Environment variables:
//
//	S3PGSTORE_DATABASE_URL                       PostgreSQL DSN (required)
//	S3PGSTORE_S3_BUCKET                          S3 bucket name (required)
//	S3PGSTORE_S3_USE_PATH_STYLE                  "1" / "true" → path-style URLs (https://endpoint/bucket/key); needed for local MinIO at localhost / IP-based endpoints; STACKIT, R2, StorageGRID with proper DNS use the SDK default (virtual-hosted-style)
//	S3PGSTORE_S3_MAX_OPEN_CONNECTIONS            Cap concurrent TCP connections to S3 (default 64)
//	S3PGSTORE_S3_MAX_RETRY_ATTEMPTS              SDK retry budget per S3 op, 1 + retries (default = AWS_MAX_ATTEMPTS or 5)
//	S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND         Pre-throttle outgoing S3 ops to this rate (default 0 = unlimited)
//	S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND_BURST   Token bucket burst (default = max(1, rate*0.1))
//	S3PGSTORE_SCHEMA                             Schema name (default "public")
//	S3PGSTORE_TABLE_PREFIX                       Table prefix (default "s3pgstore_")
//	S3PGSTORE_GRACE                              Orphan grace period (default "24h")
//	S3PGSTORE_INTERVAL                           Loop interval (default "1h")
//	S3PGSTORE_BATCH_SIZE                         Rows per RunOnce (default 1000)
//	S3PGSTORE_ONESHOT                            "1" / "true" → run RunOnce and exit
//
// AWS-side configuration (region, endpoint, credentials) follows
// the standard AWS SDK env-var chain — AWS_REGION,
// AWS_ENDPOINT_URL_S3 (or AWS_ENDPOINT_URL), AWS_ACCESS_KEY_ID,
// AWS_PROFILE, IRSA, IMDS. AWS_MAX_ATTEMPTS is honoured as the
// fallback for S3PGSTORE_S3_MAX_RETRY_ATTEMPTS when the
// s3pgstore-namespaced var is unset.
//
// Telemetry (opt-in; see cmd/internal/otelinit for details):
//
//	OTEL_METRICS_EXPORTER, OTEL_EXPORTER_OTLP_ENDPOINT,
//	OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, OTEL_SERVICE_NAME,
//	OTEL_RESOURCE_ATTRIBUTES, OTEL_METRIC_EXPORT_INTERVAL, ...
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ueisele/s3pgstore/cmd/internal/otelinit"
	"github.com/ueisele/s3pgstore/gc"
	"github.com/ueisele/s3pgstore/s3client"
)

func main() {
	if err := run(); err != nil {
		slog.Error("s3pgstore-gc exited with error", "err", err)
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

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	cfg.S3Bucket = bucket

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	meter, otelShutdown, err := otelinit.Setup(ctx, "s3pgstore-gc")
	if err != nil {
		return fmt.Errorf("otel setup: %w", err)
	}
	// Detached background ctx so ForceFlush/Shutdown still has
	// time to flush after SIGTERM cancels the run ctx.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(shutdownCtx); err != nil {
			slog.Warn("otel shutdown", "err", err)
		}
	}()
	cfg.Meter = meter

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open pgxpool: %w", err)
	}
	defer pool.Close()
	cfg.Pool = pool

	cfg.S3Client, err = s3client.NewClientFromEnv(ctx, "", meter)
	if err != nil {
		return err
	}

	oneshot, err := envBool("S3PGSTORE_ONESHOT")
	if err != nil {
		return err
	}
	slog.Info("s3pgstore-gc starting",
		"schema", cfg.SchemaName,
		"prefix", cfg.TablePrefix,
		"grace", cfg.Grace,
		"interval", cfg.Interval,
		"batch_size", cfg.BatchSize,
		"oneshot", oneshot)

	if oneshot {
		n, err := gc.RunOnce(ctx, cfg)
		if err != nil {
			return err
		}
		slog.Info("s3pgstore-gc one-shot complete", "reclaimed", n)
		return nil
	}

	if err := gc.Run(ctx, cfg); err != nil &&
		!errors.Is(err, context.Canceled) {
		return err
	}
	slog.Info("s3pgstore-gc stopped cleanly")
	return nil
}

func loadConfig() (gc.Config, error) {
	cfg := gc.Config{
		SchemaName:  os.Getenv("S3PGSTORE_SCHEMA"),
		TablePrefix: os.Getenv("S3PGSTORE_TABLE_PREFIX"),
	}
	if v := os.Getenv("S3PGSTORE_GRACE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf(
				"S3PGSTORE_GRACE %q: %w", v, err)
		}
		cfg.Grace = d
	}
	if v := os.Getenv("S3PGSTORE_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf(
				"S3PGSTORE_INTERVAL %q: %w", v, err)
		}
		cfg.Interval = d
	}
	if v := os.Getenv("S3PGSTORE_BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf(
				"S3PGSTORE_BATCH_SIZE %q: %w", v, err)
		}
		cfg.BatchSize = n
	}
	return cfg, nil
}

// envBool parses an optional bool env var via strconv.ParseBool.
// Empty → false (no error). Unrecognised values surface so a
// typo like "tru" fails at startup rather than silently flipping
// the switch off.
func envBool(key string) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s %q: %w", key, v, err)
	}
	return b, nil
}
