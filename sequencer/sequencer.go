// Package sequencer assigns gap-free feed_seq values to
// committed catalog rows. Serializes via pg_advisory_xact_lock
// so only one sequencer instance writes at a time. Ships as a
// library used by cmd/s3pgstore-sequencer.
package sequencer

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"

	"github.com/ueisele/s3pgstore/internal/catalog"
)

// Default values for Config fields.
const (
	DefaultSchemaName    = "public"
	DefaultTablePrefix   = "s3pgstore_"
	DefaultPollInterval  = 1 * time.Second
	DefaultBatchSize     = 1000
	DefaultNotifyChannel = "s3pgstore_writes"
)

// sequencerLockMagic is the fixed int32 namespacing the
// sequencer's pg_advisory_xact_lock acquisitions. The two-key
// form (key1=magic, key2=hash(prefix@schema)) avoids collisions
// with LockPartition's single-key form — PostgreSQL maintains
// independent advisory-lock spaces for the two arities. The
// magic value is "spgs" interpreted as ASCII.
const sequencerLockMagic int32 = 0x73706773

// maxNotifyChannelLen mirrors PostgreSQL's NAMEDATALEN-1.
// LISTEN takes a literal identifier; pg_notify() takes a
// parameterized text but PostgreSQL still rejects channel names
// over 63 chars internally.
const maxNotifyChannelLen = 63

// listenReconnectBackoff is how long the LISTEN goroutine
// pauses after a connection loss before re-acquiring. Short
// enough to recover quickly, long enough to avoid hot-looping
// on a sustained pool failure (the polling fallback keeps
// running while we back off).
const listenReconnectBackoff = 2 * time.Second

// Config captures the operator-facing knobs. The zero value is
// not valid — at minimum, Pool is required.
type Config struct {
	// Pool is the pgxpool used for both the assignment tx and
	// (when NotifyChannel is non-empty) the dedicated LISTEN
	// connection. Required.
	Pool *pgxpool.Pool

	// SchemaName / TablePrefix mirror the corresponding s3pgstore.Config
	// fields and must match the values the writer uses; otherwise
	// the sequencer scans the wrong tables. Defaults are filled in
	// from DefaultSchemaName / DefaultTablePrefix.
	SchemaName  string
	TablePrefix string

	// PollInterval is the fallback wake cadence used when no
	// NOTIFY arrives. Default DefaultPollInterval (1s).
	PollInterval time.Duration

	// BatchSize bounds rows assigned per RunOnce call. Smaller
	// → lower advisory-lock hold time, more round-trips; larger
	// → fewer round-trips, longer holds. Default
	// DefaultBatchSize (1000) per the implementation plan.
	BatchSize int

	// NotifyChannel is the LISTEN channel name. Empty disables
	// LISTEN — the sequencer falls back to interval polling
	// only. Default DefaultNotifyChannel ("s3pgstore_writes").
	NotifyChannel string

	// Meter is the OTel meter the sequencer registers its
	// instruments against. Nil resolves to a no-op meter so
	// telemetry is opt-in. See sequencer/metrics.go for the
	// registered instruments.
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
	if out.PollInterval <= 0 {
		out.PollInterval = DefaultPollInterval
	}
	if out.BatchSize <= 0 {
		out.BatchSize = DefaultBatchSize
	}
	if out.NotifyChannel == "" {
		out.NotifyChannel = DefaultNotifyChannel
	}
	return out
}

func (c Config) validate() error {
	if c.Pool == nil {
		return errors.New("sequencer.Config: Pool is required")
	}
	if c.NotifyChannel != "" &&
		len(c.NotifyChannel) > maxNotifyChannelLen {
		return fmt.Errorf("sequencer.Config: NotifyChannel %q is "+
			"too long (max %d chars)", c.NotifyChannel,
			maxNotifyChannelLen)
	}
	if c.BatchSize < 0 {
		return errors.New("sequencer.Config: BatchSize must be >= 0")
	}
	return nil
}

// RunOnce assigns feed_seq values to up to BatchSize currently-
// eligible rows in a single transaction and returns the number
// of rows assigned. The transaction holds the sequencer's
// advisory lock for its duration, so concurrent RunOnce / Run
// invocations against the same (schema, prefix) serialize.
//
// Returns (0, nil) when no eligible rows are available.
//
// Used directly by tests and ad-hoc invocations; Run wraps
// RunOnce in a poll/NOTIFY loop with drain-to-empty semantics.
func RunOnce(ctx context.Context, cfg Config) (int, error) {
	if err := cfg.validate(); err != nil {
		return 0, err
	}
	r := cfg.resolved()
	m, err := newMetrics(r)
	if err != nil {
		return 0, fmt.Errorf("register sequencer metrics: %w", err)
	}
	return runOnceWithMetrics(ctx, r, m)
}

func runOnceWithMetrics(
	ctx context.Context, r Config, m *Metrics,
) (int, error) {
	names := catalog.NewNames(r.SchemaName, r.TablePrefix)

	var rowsAssigned int
	err := pgx.BeginFunc(ctx, r.Pool, func(tx pgx.Tx) error {
		// Sequencer advisory lock. Two-key form to namespace
		// against LockPartition (single-key form). key2 hashes
		// (schema + prefix) so multiple s3pgstore deployments
		// in the same database run independent sequencers
		// without blocking each other.
		key2 := scopeHash(r.SchemaName, r.TablePrefix)
		lockStart := time.Now()
		if _, err := tx.Exec(ctx,
			"SELECT pg_advisory_xact_lock($1, $2)",
			sequencerLockMagic, key2,
		); err != nil {
			return fmt.Errorf("acquire sequencer lock: %w", err)
		}
		m.recordLockWait(ctx, time.Since(lockStart).Seconds())
		ct, err := tx.Exec(ctx, assignSQL(names), r.BatchSize)
		if err != nil {
			return fmt.Errorf("assign feed_seq: %w", err)
		}
		rowsAssigned = int(ct.RowsAffected())
		return nil
	})
	if err == nil && rowsAssigned > 0 {
		m.recordAssigned(ctx, rowsAssigned)
	}
	return rowsAssigned, err
}

// Run blocks until ctx is cancelled, draining eligible rows
// whenever a NOTIFY arrives or the poll interval ticks. Drain
// is run-to-empty: after each wake, RunOnce is called in a tight
// loop until it reports fewer than BatchSize rows assigned, so
// a NOTIFY storm doesn't accumulate backlog.
//
// Returns ctx.Err() on cancel; LISTEN connection failures are
// logged and retried with backoff (the polling fallback keeps
// running through reconnects).
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	r := cfg.resolved()
	m, err := newMetrics(r)
	if err != nil {
		return fmt.Errorf("register sequencer metrics: %w", err)
	}
	// Observable gauge must register exactly once per process —
	// see registerUnsequencedGauge.
	if err := registerUnsequencedGauge(r); err != nil {
		return fmt.Errorf("register sequencer.unsequenced gauge: %w", err)
	}

	// Initial drain: catch up any rows that landed between
	// last shutdown and now. Failure here aborts startup —
	// operator should see the error early.
	if _, err := drainAll(ctx, r, m); err != nil {
		return fmt.Errorf("initial drain: %w", err)
	}

	// Wake-up channel. Buffered=1 so a burst of NOTIFY events
	// coalesces (sender does non-blocking send; receiver wakes
	// once and drains everything).
	notify := make(chan struct{}, 1)

	var listenWG sync.WaitGroup
	listenCtx, cancelListen := context.WithCancel(ctx)
	defer func() {
		cancelListen()
		listenWG.Wait()
	}()
	if r.NotifyChannel != "" {
		listenWG.Go(func() {
			runListen(listenCtx, r, notify)
		})
	}

	ticker := time.NewTicker(r.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		case <-ticker.C:
		}
		if _, err := drainAll(ctx, r, m); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			slog.Warn("s3pgstore.sequencer: drain error",
				"err", err)
		}
	}
}

// drainAll repeats RunOnce until a call assigns fewer than
// BatchSize rows. Returns the total assigned and the first
// error encountered (which short-circuits the drain). Reuses
// one *Metrics across calls so the assigned counter and
// lock_wait histogram aggregate across the whole drain.
func drainAll(ctx context.Context, cfg Config, m *Metrics) (int, error) {
	total := 0
	r := cfg.resolved()
	for {
		n, err := runOnceWithMetrics(ctx, r, m)
		total += n
		if err != nil {
			return total, err
		}
		if n < r.BatchSize {
			return total, nil
		}
	}
}

// runListen owns the LISTEN goroutine: it acquires a dedicated
// connection, issues LISTEN, and forwards every notification to
// the wake-up channel. On connection loss, sleeps
// listenReconnectBackoff and retries. Returns when ctx is
// cancelled.
func runListen(ctx context.Context, cfg Config, notify chan<- struct{}) {
	for ctx.Err() == nil {
		if err := listenOnce(ctx, cfg, notify); err != nil &&
			!errors.Is(err, context.Canceled) {
			slog.Warn("s3pgstore.sequencer: LISTEN dropped, reconnecting",
				"err", err)
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(listenReconnectBackoff):
		}
	}
}

// listenOnce acquires one dedicated connection, registers
// LISTEN, and forwards notifications until the connection
// errors or ctx cancels.
func listenOnce(ctx context.Context, cfg Config, notify chan<- struct{}) error {
	conn, err := cfg.Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire LISTEN conn: %w", err)
	}
	defer conn.Release()

	// LISTEN takes an unparameterized identifier, so we quote
	// the channel name via pgx.Identifier.Sanitize. Validated
	// length-bounded by Config.validate above.
	listenSQL := "LISTEN " +
		pgx.Identifier{cfg.NotifyChannel}.Sanitize()
	if _, err := conn.Exec(ctx, listenSQL); err != nil {
		return fmt.Errorf("LISTEN %s: %w", cfg.NotifyChannel, err)
	}
	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return err
		}
		// Coalesce: if the wake channel is already pending,
		// drop this one — the receiver will pick up everything
		// that's accumulated when it wakes.
		select {
		case notify <- struct{}{}:
		default:
		}
	}
}

// assignSQL renders the gap-free assignment query for the
// given table names. The CTE structure:
//
//   - `base` reads the current MAX(feed_seq) once, treating
//     missing-rows as 0 via COALESCE.
//   - `numbered` selects up to $1 unsequenced rows in
//     (written_at, file_id) order and assigns each a
//     ROW_NUMBER starting at 1.
//   - The UPDATE joins both CTEs into the files table and
//     writes feed_seq = base.max_seq + numbered.rn,
//     feed_seq_at = now().
//
// The advisory lock acquired by RunOnce serializes invocations,
// so MAX is read after every previous sequencer's commit; the
// "writers commit ahead of MAX" race the sequencer is built
// to prevent cannot occur within the locked window.
func assignSQL(n catalog.Names) string {
	files := n.Files()
	return fmt.Sprintf(`
WITH base AS (
    SELECT COALESCE(MAX(feed_seq), 0) AS max_seq
    FROM %s
),
numbered AS (
    SELECT file_id,
           ROW_NUMBER() OVER (ORDER BY written_at, file_id) AS rn
    FROM %s
    WHERE feed_seq IS NULL
    ORDER BY written_at, file_id
    LIMIT $1
)
UPDATE %s AS f
SET feed_seq = base.max_seq + numbered.rn,
    feed_seq_at = now()
FROM numbered, base
WHERE f.file_id = numbered.file_id
`, files, files, files)
}

// scopeHash returns the int32 used as the second arg to
// pg_advisory_xact_lock for the sequencer's per-deployment
// scope. FNV-32a of "<schema>@<prefix>" — different schemas or
// prefixes hash to different keys, so concurrent sequencers for
// distinct s3pgstore deployments don't serialize on each other.
func scopeHash(schema, prefix string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(schema))
	_, _ = h.Write([]byte("@"))
	_, _ = h.Write([]byte(prefix))
	return int32(h.Sum32()) //nolint:gosec // signed-cast intentional
}
