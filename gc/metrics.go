package gc

// gc-process metrics. The gc binary owns the orphan reclamation
// loop and exposes one operationally interesting counter:
// gc.reclaimed.count. The pending_writes.depth gauge that tracks
// orphan backlog lives on the writer's Store metrics surface
// (s3pgstore.pending_writes.depth) — operators wire it on the
// writer pods because every writer touches pending_writes,
// whereas the gc binary is optional.

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// scopeName is the OTel meter scope this package's
// instruments register under when cfg.Meter is nil.
// Convention: the Go package import path (matches OTel's
// instrumentation-scope semantic).
const scopeName = "github.com/ueisele/s3pgstore/gc"

// metrics is the gc binary's per-Config instrumentation handle.
// A nil receiver makes every record call a no-op.
type metrics struct {
	reclaimed metric.Int64Counter
}

func newMetrics(cfg Config) (*metrics, error) {
	meter := cfg.Meter
	if meter == nil {
		meter = otel.GetMeterProvider().Meter(scopeName)
	}
	reclaimed, err := meter.Int64Counter(
		"s3pgstore.gc.reclaimed.count",
		metric.WithDescription(
			"Total orphan rows reclaimed across every RunOnce "+
				"call (S3 DELETE + catalog DELETE pair). Flat "+
				"at zero with sustained "+
				"s3pgstore.pending_writes.depth growth = GC "+
				"is not making progress."),
		metric.WithUnit("{row}"))
	if err != nil {
		return nil, err
	}
	return &metrics{
		reclaimed: reclaimed,
	}, nil
}

// recordReclaimed fires after a RunOnce call with the count of
// orphan rows successfully removed in that pass.
func (m *metrics) recordReclaimed(ctx context.Context, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.reclaimed.Add(ctx, int64(n))
}
