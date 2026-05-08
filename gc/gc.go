// Package gc reclaims orphaned S3 objects whose s3pgstore
// catalog transactions rolled back (or never committed within a
// grace period). Ships as a library used by cmd/s3pgstore-gc.
package gc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"

	"github.com/ueisele/s3pgstore/internal/catalog"
)

// Default values for Config fields.
const (
	DefaultSchemaName  = "public"
	DefaultTablePrefix = "s3pgstore_"
	// DefaultGrace is the minimum age a pending_writes row must
	// reach before GC reclaims it. Conservative default: 24h
	// covers most production retry timelines (idempotent writers
	// commonly retry for hours; few are still in-flight after a
	// day). Operators with shorter retry tails can tighten this.
	DefaultGrace = 24 * time.Hour
	// DefaultInterval is how often Run wakes for a sweep pass.
	// Default 1 hour — orphans are bounded (one row per failed
	// write, not a hot-path concern) so frequent sweeps add cost
	// without value.
	DefaultInterval = 1 * time.Hour
	// DefaultBatchSize bounds rows scanned per RunOnce call so a
	// single huge backlog can't hold a transaction open for
	// minutes; subsequent RunOnce calls drain the rest.
	DefaultBatchSize = 1000
)

// Config captures the operator-facing knobs. The zero value is
// not valid — Pool, S3Client, and Bucket are required.
type Config struct {
	Pool     *pgxpool.Pool
	S3Client *s3.Client
	Bucket   string

	// SchemaName / TablePrefix mirror the writer's
	// s3pgstore.Config and must match. Defaults are filled in
	// from DefaultSchemaName / DefaultTablePrefix.
	SchemaName  string
	TablePrefix string

	// Grace is the minimum age a pending_writes row must reach
	// before GC reclaims it. Tunes how long writers have to
	// commit (or roll back to ground state) before GC assumes
	// abandonment. Default DefaultGrace (24h).
	Grace time.Duration

	// Interval is the loop period for Run. Default
	// DefaultInterval (1h).
	Interval time.Duration

	// BatchSize bounds rows scanned per RunOnce call. Default
	// DefaultBatchSize (1000).
	BatchSize int

	// Meter is the OTel meter the gc binary registers its
	// instruments against. Nil resolves to a no-op meter so
	// telemetry is opt-in. See gc/metrics.go for the registered
	// instruments.
	Meter metric.Meter
}

func (c Config) resolved() Config {
	out := c
	if out.SchemaName == "" {
		out.SchemaName = DefaultSchemaName
	}
	if out.TablePrefix == "" {
		out.TablePrefix = DefaultTablePrefix
	}
	if out.Grace <= 0 {
		out.Grace = DefaultGrace
	}
	if out.Interval <= 0 {
		out.Interval = DefaultInterval
	}
	if out.BatchSize <= 0 {
		out.BatchSize = DefaultBatchSize
	}
	return out
}

func (c Config) validate() error {
	if c.Pool == nil {
		return errors.New("gc.Config: Pool is required")
	}
	if c.S3Client == nil {
		return errors.New("gc.Config: S3Client is required")
	}
	if c.Bucket == "" {
		return errors.New("gc.Config: Bucket is required")
	}
	return nil
}

// RunOnce scans s3pgstore_pending_writes for rows older than
// the configured grace period and reclaims each: S3 DELETE the
// object, then DELETE the catalog row. On S3 DELETE failure the
// row stays — the next RunOnce will retry.
//
// Returns the number of catalog rows successfully removed.
//
// Each (S3 DELETE, catalog DELETE) pair runs in its own short
// transaction so a single failure doesn't poison the whole
// batch. Workers do not run in parallel — orphans are bounded
// and the sequential path keeps the GC's blast radius small.
func RunOnce(ctx context.Context, cfg Config) (int, error) {
	if err := cfg.validate(); err != nil {
		return 0, err
	}
	r := cfg.resolved()
	m, err := newMetrics(r)
	if err != nil {
		return 0, fmt.Errorf("register gc metrics: %w", err)
	}
	return runOnceWithMetrics(ctx, r, m)
}

func runOnceWithMetrics(
	ctx context.Context, r Config, m *Metrics,
) (int, error) {
	names := catalog.NewNames(r.SchemaName, r.TablePrefix)

	cutoff := time.Now().Add(-r.Grace).UTC()

	rows, err := scanPending(ctx, r.Pool, names, cutoff, r.BatchSize)
	if err != nil {
		return 0, err
	}
	reclaimed := 0
	for _, key := range rows {
		if err := ctx.Err(); err != nil {
			m.recordReclaimed(ctx, reclaimed)
			return reclaimed, err
		}
		if err := reclaimOne(ctx, r, names, key); err != nil {
			// Log and continue — one bad object shouldn't
			// stall the rest of the batch. The row stays in
			// pending_writes for the next sweep.
			slog.Warn("s3pgstore.gc: reclaim failed",
				"s3_key", key, "err", err)
			continue
		}
		reclaimed++
	}
	m.recordReclaimed(ctx, reclaimed)
	return reclaimed, nil
}

// Run blocks until ctx is cancelled, calling RunOnce every
// Interval. Errors from RunOnce are logged and the loop
// continues so a transient PG hiccup doesn't shut down GC.
//
// On entry runs RunOnce immediately rather than waiting for
// the first tick — startup catches up backlogs quickly.
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	r := cfg.resolved()
	m, err := newMetrics(r)
	if err != nil {
		return fmt.Errorf("register gc metrics: %w", err)
	}

	for {
		n, err := runOnceWithMetrics(ctx, r, m)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			slog.Warn("s3pgstore.gc: RunOnce error",
				"err", err)
		} else if n > 0 {
			slog.Info("s3pgstore.gc: reclaimed orphans",
				"count", n)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.Interval):
		}
	}
}

// scanPending returns up to limit s3_key values whose
// intended_at is older than cutoff. Read-only — no DELETEs
// happen here; the per-row reclaim path takes care of those.
func scanPending(
	ctx context.Context, pool *pgxpool.Pool,
	names catalog.Names, cutoff time.Time, limit int,
) ([]string, error) {
	q := fmt.Sprintf(
		`SELECT s3_key FROM %s
		WHERE intended_at < $1
		ORDER BY intended_at
		LIMIT $2`,
		names.PendingWrites())
	rows, err := pool.Query(ctx, q, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("scan pending_writes: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// reclaimOne is the per-orphan reclamation path. Order is
// load-bearing:
//
//  1. S3 DELETE first. On success the object is gone; the
//     catalog row's purpose (tracking the orphan) is fulfilled.
//  2. Catalog DELETE second. Removes the now-stale tracker.
//
// If step 1 fails the catalog row stays — the next sweep
// retries. If step 1 succeeds but step 2 fails, the catalog
// row stays too; the next sweep tries step 1 against an
// already-deleted object (S3 DELETE on a missing key is
// idempotent — both AWS S3 and MinIO succeed) and proceeds to
// step 2 again. No leak.
func reclaimOne(
	ctx context.Context, cfg Config,
	names catalog.Names, s3Key string,
) error {
	if _, err := cfg.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(s3Key),
	}); err != nil {
		return fmt.Errorf("S3 DELETE %s: %w", s3Key, err)
	}
	q := fmt.Sprintf(
		`DELETE FROM %s WHERE s3_key = $1`,
		names.PendingWrites())
	if err := pgx.BeginFunc(ctx, cfg.Pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, q, s3Key)
		return err
	}); err != nil {
		return fmt.Errorf("delete pending_writes %s: %w",
			s3Key, err)
	}
	return nil
}
