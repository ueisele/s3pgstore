package sequencer

// Sequencer-process metrics. The sequencer is its own binary with
// its own OTel surface — operators wiring telemetry on the
// sequencer pod don't need to wire anything in the writer pods
// to see these instruments. The sequencer.Config.Meter resolves
// to a no-op meter when nil so telemetry is opt-in.
//
// Three instruments cover the operationally interesting failure
// modes:
//
//   - sequencer.assigned.count — heartbeat counter; flat at zero
//     under load = sequencer is alive but not making progress.
//   - sequencer.unsequenced (observable gauge) — current rows
//     waiting for feed_seq assignment. Sustained growth = the
//     sequencer cannot keep up with write rate.
//   - sequencer.lock_wait — time spent acquiring the
//     pg_advisory_xact_lock before the assignment statement
//     runs. Non-zero P95 indicates two sequencers are racing
//     against each other (operator deployed a duplicate by
//     mistake) or another transaction is holding the same key.

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/ueisele/s3pgstore/internal/catalog"
)

// Metrics is the sequencer's per-Config instrumentation handle.
// Constructed inside RunOnce / Run via newMetrics(cfg).
// A nil receiver makes every record call a no-op.
type Metrics struct {
	assigned metric.Int64Counter
	lockWait metric.Float64Histogram
}

// newMetrics registers the counter + histogram instruments. The
// observable backlog gauge is NOT registered here so that
// callers invoking RunOnce in a loop (against the same meter)
// don't accumulate one observable callback per call.
// registerUnsequencedGauge handles that separately and is meant
// to be called exactly once per long-lived sequencer process —
// Run does it under the hood.
func newMetrics(cfg Config) (*Metrics, error) {
	meter := cfg.Meter
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("s3pgstore.sequencer")
	}

	assigned, err := meter.Int64Counter(
		"s3pgstore.sequencer.assigned.count",
		metric.WithDescription(
			"Total feed_seq values assigned by the sequencer "+
				"across every RunOnce call. Heartbeat — flat at "+
				"zero under sustained write load means the "+
				"sequencer is alive but not making progress."),
		metric.WithUnit("{row}"))
	if err != nil {
		return nil, err
	}
	lockWait, err := meter.Float64Histogram(
		"s3pgstore.sequencer.lock_wait",
		metric.WithDescription(
			"Time spent acquiring the sequencer's "+
				"pg_advisory_xact_lock before the assignment "+
				"statement runs. Non-zero P95 = a duplicate "+
				"sequencer is racing or another tx holds the "+
				"same key."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5))
	if err != nil {
		return nil, err
	}
	return &Metrics{
		assigned: assigned,
		lockWait: lockWait,
	}, nil
}

// registerUnsequencedGauge registers the
// s3pgstore.sequencer.unsequenced observable gauge against
// cfg.Meter. Call exactly once per long-lived sequencer
// process — Run does so before its main loop. Repeating it
// would attach multiple callbacks to the same instrument and
// inflate observations per collection cycle.
func registerUnsequencedGauge(cfg Config) error {
	if cfg.Meter == nil {
		return nil
	}
	r := cfg.resolved()
	names := catalog.NewNames(r.SchemaName, r.TablePrefix)
	depth, err := cfg.Meter.Int64ObservableGauge(
		"s3pgstore.sequencer.unsequenced",
		metric.WithDescription(
			"Current count of s3pgstore_files rows with feed_seq "+
				"IS NULL — the sequencer's pending backlog. "+
				"Sustained growth = sequencer not keeping up "+
				"with write rate."),
		metric.WithUnit("{row}"))
	if err != nil {
		return err
	}
	depthSQL := fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE feed_seq IS NULL`,
		names.Files())
	pool := r.Pool
	_, err = cfg.Meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			var n int64
			if err := pool.QueryRow(ctx, depthSQL).Scan(&n); err != nil {
				// Errors are silent: the gauge is best-effort
				// and a transient PG hiccup shouldn't spam logs
				// every collection cycle.
				return nil //nolint:nilerr
			}
			o.ObserveInt64(depth, n)
			return nil
		}, depth)
	return err
}

// recordAssigned fires after a successful RunOnce call with the
// row count assigned in that batch.
func (m *Metrics) recordAssigned(ctx context.Context, n int) {
	if m == nil {
		return
	}
	m.assigned.Add(ctx, int64(n))
}

// recordLockWait records the time spent inside the
// pg_advisory_xact_lock statement.
func (m *Metrics) recordLockWait(ctx context.Context, seconds float64) {
	if m == nil {
		return
	}
	m.lockWait.Record(ctx, seconds)
}

// pool is the local alias for *pgxpool.Pool — kept here to make
// the test stub pattern obvious if the sequencer ever needs to
// inject a fake.
var _ *pgxpool.Pool = (*pgxpool.Pool)(nil)
