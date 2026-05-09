// Command s3pgstore-sequencer assigns gap-free feed_seq values
// to committed s3pgstore catalog rows. Reads its configuration
// from environment variables and runs until SIGTERM/SIGINT.
//
// Environment variables:
//
//	S3PGSTORE_DATABASE_URL    PostgreSQL DSN (required)
//	S3PGSTORE_SCHEMA          Schema name (default "public")
//	S3PGSTORE_TABLE_PREFIX    Table prefix (default "s3pgstore_")
//	S3PGSTORE_NOTIFY_CHANNEL  LISTEN channel (default "s3pgstore_writes";
//	                          set empty to disable LISTEN)
//	S3PGSTORE_POLL_INTERVAL   Fallback poll cadence (default "1s")
//	S3PGSTORE_BATCH_SIZE      Rows per RunOnce batch (default 1000)
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
	"github.com/ueisele/s3pgstore/sequencer"
)

func main() {
	if err := run(); err != nil {
		slog.Error("s3pgstore-sequencer exited with error",
			"err", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := os.Getenv("S3PGSTORE_DATABASE_URL")
	if dsn == "" {
		return errors.New("S3PGSTORE_DATABASE_URL is required")
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	meter, otelShutdown, err := otelinit.Setup(ctx,
		"s3pgstore-sequencer")
	if err != nil {
		return fmt.Errorf("otel setup: %w", err)
	}
	// Use a detached background context for shutdown — the signal
	// ctx is already canceled by the time we get here, which would
	// cause ForceFlush/Shutdown to bail before flushing.
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

	slog.Info("s3pgstore-sequencer starting",
		"schema", cfg.SchemaName,
		"prefix", cfg.TablePrefix,
		"poll_interval", cfg.PollInterval,
		"notify_channel", cfg.NotifyChannel,
		"batch_size", cfg.BatchSize)

	if err := sequencer.Run(ctx, cfg); err != nil &&
		!errors.Is(err, context.Canceled) {
		return err
	}
	slog.Info("s3pgstore-sequencer stopped cleanly")
	return nil
}

func loadConfig() (sequencer.Config, error) {
	cfg := sequencer.Config{
		SchemaName:    os.Getenv("S3PGSTORE_SCHEMA"),
		TablePrefix:   os.Getenv("S3PGSTORE_TABLE_PREFIX"),
		NotifyChannel: os.Getenv("S3PGSTORE_NOTIFY_CHANNEL"),
	}

	if v := os.Getenv("S3PGSTORE_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf(
				"S3PGSTORE_POLL_INTERVAL %q: %w", v, err)
		}
		cfg.PollInterval = d
	}
	if v := os.Getenv("S3PGSTORE_BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf(
				"S3PGSTORE_BATCH_SIZE %q: %w", v, err)
		}
		if n < 0 {
			return cfg, fmt.Errorf(
				"S3PGSTORE_BATCH_SIZE %d: must be >= 0", n)
		}
		cfg.BatchSize = n
	}
	return cfg, nil
}
