// Command s3pgstore-gc reclaims S3 objects whose s3pgstore
// catalog transactions rolled back (or never committed within a
// grace period). Reads its configuration from environment
// variables; supports one-shot and loop modes.
//
// Environment variables:
//
//	S3PGSTORE_DATABASE_URL              PostgreSQL DSN (required)
//	S3PGSTORE_BUCKET                    S3 bucket name (required)
//	S3PGSTORE_S3_ENDPOINT               Optional override for non-AWS S3 (e.g. MinIO)
//	S3PGSTORE_S3_REGION                 AWS region (default "us-east-1")
//	S3PGSTORE_S3_MAX_INFLIGHT_REQUESTS  Cap simultaneous S3 requests (default 32)
//	S3PGSTORE_SCHEMA                    Schema name (default "public")
//	S3PGSTORE_TABLE_PREFIX              Table prefix (default "s3pgstore_")
//	S3PGSTORE_GRACE                     Orphan grace period (default "24h")
//	S3PGSTORE_INTERVAL                  Loop interval (default "1h")
//	S3PGSTORE_BATCH_SIZE                Rows per RunOnce (default 1000)
//	S3PGSTORE_ONESHOT                   "1" / "true" → run RunOnce and exit
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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ueisele/s3pgstore/cmd/internal/otelinit"
	"github.com/ueisele/s3pgstore/cmd/internal/s3client"
	"github.com/ueisele/s3pgstore/gc"
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
	bucket := os.Getenv("S3PGSTORE_BUCKET")
	if bucket == "" {
		return errors.New("S3PGSTORE_BUCKET is required")
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	cfg.Bucket = bucket

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

	s3Client, err := loadS3Client(ctx)
	if err != nil {
		return err
	}
	cfg.S3Client = s3Client

	oneshot := isTruthy(os.Getenv("S3PGSTORE_ONESHOT"))
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

func loadS3Client(ctx context.Context) (*s3.Client, error) {
	maxInflight, err := envInt("S3PGSTORE_S3_MAX_INFLIGHT_REQUESTS")
	if err != nil {
		return nil, err
	}
	if maxInflight < 0 {
		return nil, fmt.Errorf(
			"S3PGSTORE_S3_MAX_INFLIGHT_REQUESTS %d: must be >= 0",
			maxInflight)
	}
	return s3client.BuildS3Client(ctx, s3client.Options{
		Region:              os.Getenv("S3PGSTORE_S3_REGION"),
		Endpoint:            os.Getenv("S3PGSTORE_S3_ENDPOINT"),
		MaxInflightRequests: maxInflight,
	})
}

// envInt parses an env var as int. Returns (0, nil) when unset.
// Negative-value validation is left to the caller so each
// binary can frame the error message in its own terms.
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

func isTruthy(s string) bool {
	switch s {
	case "1", "t", "T", "true", "TRUE", "True", "yes", "YES":
		return true
	}
	return false
}
