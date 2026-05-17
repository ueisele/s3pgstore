package s3pgstore

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// TestNextTailBackoff_Schedule verifies the exponential schedule
// the tail feeder uses for inter-poll waits: n=1 returns base,
// each subsequent step doubles, capped at max. This is the
// pure-function half of TailRecordsIter — exercised here so the
// integration test doesn't have to wait long enough to observe
// the cap.
func TestNextTailBackoff_Schedule(t *testing.T) {
	const (
		base = 100 * time.Millisecond
		max  = 2 * time.Second
	)
	cases := []struct {
		n    int
		want time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1600 * time.Millisecond},
		{6, 2 * time.Second},   // shift would yield 3.2s, cap at 2s
		{20, 2 * time.Second},  // far past the cap
		{100, 2 * time.Second}, // shift-overflow guard kicks in past 30
	}
	for _, c := range cases {
		if got := nextTailBackoff(c.n, base, max); got != c.want {
			t.Errorf("nextTailBackoff(%d, %v, %v): got %v, want %v",
				c.n, base, max, got, c.want)
		}
	}
}

// TestNextTailBackoff_NonPositiveN guards the n<=1 fast path:
// the first wait (n=1) returns base regardless of the doubling
// math; defensive n<=0 also returns base so a caller mistake
// doesn't cause a near-zero busy-loop wait.
func TestNextTailBackoff_NonPositiveN(t *testing.T) {
	const base = 50 * time.Millisecond
	for _, n := range []int{-5, -1, 0, 1} {
		if got := nextTailBackoff(n, base, time.Second); got != base {
			t.Errorf("nextTailBackoff(%d): got %v, want %v (= base)",
				n, got, base)
		}
	}
}

// TestTailIntervals_Defaults verifies the unset-field path
// resolves to the documented defaults — load-bearing for the
// PollOption-not-supplied case.
func TestTailIntervals_Defaults(t *testing.T) {
	o := pollOpts{}
	base, max := tailIntervals(&o)
	if base != defaultTailBaseInterval {
		t.Errorf("base: got %v, want %v",
			base, defaultTailBaseInterval)
	}
	if max != defaultTailMaxInterval {
		t.Errorf("max: got %v, want %v",
			max, defaultTailMaxInterval)
	}
}

// TestTailIntervals_MaxBelowBasePromoted guards the degenerate
// configuration where max < base would otherwise produce a
// shrinking-cap (the doubling loop computes base * 2^(n-1) and
// caps at max; if max<base the very first wait is already
// "above the cap" and would clamp to max immediately, defeating
// the purpose of base).
func TestTailIntervals_MaxBelowBasePromoted(t *testing.T) {
	o := pollOpts{
		tailBaseInterval: 500 * time.Millisecond,
		tailMaxInterval:  100 * time.Millisecond,
	}
	base, max := tailIntervals(&o)
	if base != 500*time.Millisecond {
		t.Errorf("base: got %v, want 500ms", base)
	}
	if max != 500*time.Millisecond {
		t.Errorf("max promoted to base: got %v, want 500ms",
			max)
	}
}

// TestWithTailIdleBackoff_StoresValues confirms the option lands
// the supplied base/max on the pollOpts fields verbatim — the
// tailIntervals defaulter then reads them.
func TestWithTailIdleBackoff_StoresValues(t *testing.T) {
	o := resolvePollOpts([]PollOption{
		WithTailIdleBackoff(50*time.Millisecond, 5*time.Second),
	})
	if o.tailBaseInterval != 50*time.Millisecond {
		t.Errorf("base: got %v, want 50ms", o.tailBaseInterval)
	}
	if o.tailMaxInterval != 5*time.Second {
		t.Errorf("max: got %v, want 5s", o.tailMaxInterval)
	}
}

// TestWithPollPageSize_StoresValue verifies the option lands the
// supplied value on pollOpts.pollPageSize verbatim. PollIter /
// TailIter then read the field and fall back to the default
// when it's <= 0.
func TestWithPollPageSize_StoresValue(t *testing.T) {
	o := resolvePollOpts([]PollOption{WithPollPageSize(2500)})
	if o.pollPageSize != 2500 {
		t.Errorf("pollPageSize: got %d, want 2500", o.pollPageSize)
	}
}

// TestWithPollPageSize_NonPositiveStoredAsIs verifies that a
// non-positive value lands on the field unmodified. PollIter /
// TailIter handle the default-fallback at use time, so the
// option doesn't need to massage the value at construction.
func TestWithPollPageSize_NonPositiveStoredAsIs(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		o := resolvePollOpts([]PollOption{WithPollPageSize(n)})
		if o.pollPageSize != n {
			t.Errorf("WithPollPageSize(%d): got pollPageSize=%d, want %d",
				n, o.pollPageSize, n)
		}
	}
}

// TestDeadlockObserver_OccupancyZeroSuppresses verifies the
// watchdog stays quiet during idle periods even when
// lastProgressNs is older than the threshold — required so
// TailRecordsIter's between-poll waits don't generate false-
// positive stall warnings. The semantic: a deadlock requires
// holding a body slot, so occupancy()==0 means nothing to
// deadlock on.
func TestDeadlockObserver_OccupancyZeroSuppresses(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader))
	m, err := newMetrics(metricsConfig{
		Meter: provider.Meter("test"),
	})
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}
	// cap=2 but no slots acquired — occupancy()==0 throughout.
	s := newTestReadState(t).withSlotCap(2)
	s.slots.m = m
	// Stale progress — would normally fire the watchdog.
	s.slots.lastProgressNs.Store(time.Now().Add(-time.Second).UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		// posLoader is grain-agnostic — the watchdog only reads
		// it for the slog log. Use the readState's decoderPi to
		// avoid wiring up a separate pollState here; the test
		// exercises the shared primitive's suppression branch.
		runDeadlockObserver(ctx, m, "PollRecordsIter", "poll",
			s.slots, "decoder_file", s.decoderPi.Load,
			5*time.Millisecond, 50*time.Millisecond)
	}()

	// Run for 200ms — the watchdog ticks ~40 times in that
	// window, every tick observes occupancy==0 and skips. Must
	// stay at zero throughout.
	time.Sleep(200 * time.Millisecond)

	got := stallCounterValue(t, reader, "PollRecordsIter")
	cancel()
	<-done

	if got > 0 {
		t.Errorf("stall counter = %d, want 0 "+
			"(occupancy==0 should suppress)", got)
	}
}
