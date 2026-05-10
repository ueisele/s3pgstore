package s3pgstore

// Adapted from
// https://github.com/ueisele/s3store/blob/da75ca9/reader_iter_test.go
// Copyright (c) 2024-2026 Uwe Eisele. MIT License.
//
// Adapted to s3pgstore: keyMeta → fileRow; methodKind →
// string method label; runDeadlockObserver takes a string
// method now (not a *methodScope); metric assertions use the
// in-package OTel SDK reader pattern from metrics_test.go.

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newTestStreamState() *streamState {
	return &streamState{}
}

// withSlotCap attaches a body-slot semaphore of the given
// capacity. Mirrors how fetchAndDecodeIter wires up slotCh
// in production. cap == 0 leaves slotCh nil — the
// no-back-pressure path the live pipeline never takes but that
// test cases exercise to confirm the disabled-cap branch
// returns immediately.
func (s *streamState) withSlotCap(cap int) *streamState {
	if cap > 0 {
		s.slotCh = make(chan struct{}, cap)
	}
	return s
}

// newTestWorkerState builds a per-decode-worker state for
// reserveBytes/releaseBytes unit tests. Mirrors
// fetchAndDecodeIter's setup but with a metrics-less worker so
// tests don't depend on the OTel SDK plumbing — the methods
// only use m for histogram recording, which the noop default
// handles.
func newTestWorkerState() *workerState[any] {
	m, err := newMetrics(metricsConfig{})
	if err != nil {
		panic(err)
	}
	return newWorkerState[any](1, m)
}

// TestReserveBytes_NoCap verifies the cap=0 fast path returns
// immediately and does not touch bufferedBytes.
func TestReserveBytes_NoCap(t *testing.T) {
	ws := newTestWorkerState()
	if err := ws.reserveBytes(context.Background(), 1<<30, 0); err != nil {
		t.Fatalf("reserveBytes with cap=0 should return nil, got %v", err)
	}
	if ws.bufferedBytes != 0 {
		t.Errorf("bufferedBytes = %d, want 0 (cap=0 should not reserve)",
			ws.bufferedBytes)
	}
}

// TestReserveBytes_FitsImmediately reserves a chunk inside the
// cap and verifies the running total is updated.
func TestReserveBytes_FitsImmediately(t *testing.T) {
	ws := newTestWorkerState()
	if err := ws.reserveBytes(context.Background(), 100, 1000); err != nil {
		t.Fatalf("reserveBytes should succeed when fits, got %v", err)
	}
	if ws.bufferedBytes != 100 {
		t.Errorf("bufferedBytes = %d, want 100", ws.bufferedBytes)
	}
}

// TestReserveBytes_BlocksUntilRelease verifies the gate: a
// second reservation that would exceed the cap blocks until
// the first one is released.
func TestReserveBytes_BlocksUntilRelease(t *testing.T) {
	ws := newTestWorkerState()
	if err := ws.reserveBytes(context.Background(), 700, 1000); err != nil {
		t.Fatalf("first reserve should succeed, got %v", err)
	}

	done := make(chan struct{})
	go func() {
		_ = ws.reserveBytes(context.Background(), 500, 1000)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("second reserveBytes should have blocked")
	case <-time.After(20 * time.Millisecond):
	}

	ws.releaseBytes(700)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second reserveBytes should have unblocked after release")
	}

	if ws.bufferedBytes != 500 {
		t.Errorf("bufferedBytes = %d, want 500 (only second still held)",
			ws.bufferedBytes)
	}
}

// TestReserveBytes_OversizedSinglePartitionFlows guards the
// escape clause: when the buffer is empty, an over-cap
// reservation still proceeds (otherwise a single oversized
// partition would deadlock the pipeline).
func TestReserveBytes_OversizedSinglePartitionFlows(t *testing.T) {
	ws := newTestWorkerState()
	if err := ws.reserveBytes(context.Background(), 2000, 1000); err != nil {
		t.Fatalf("oversized reserve into empty buffer should succeed, got %v", err)
	}
	if ws.bufferedBytes != 2000 {
		t.Errorf("bufferedBytes = %d, want 2000", ws.bufferedBytes)
	}
}

// TestReserveBytes_CtxCancellation guards that a blocked
// reservation returns the cancel cause when ctx is cancelled —
// needed so the pipeline can shut down cleanly when the caller
// breaks out of the iter loop.
func TestReserveBytes_CtxCancellation(t *testing.T) {
	ws := newTestWorkerState()
	if err := ws.reserveBytes(context.Background(), 700, 1000); err != nil {
		t.Fatalf("first reserve should succeed, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() {
		got <- ws.reserveBytes(ctx, 500, 1000)
	}()

	time.Sleep(10 * time.Millisecond)

	cancel()

	select {
	case err := <-got:
		if err == nil {
			t.Error("reserveBytes should have returned a cancel err")
		}
	case <-time.After(time.Second):
		t.Fatal("reserveBytes did not return after ctx cancel")
	}
}

// TestAcquireBodySlot_BlocksUntilRelease verifies the body-pool
// back-pressure: once cap slots are held, the next acquire
// blocks until releaseBodySlots returns one.
func TestAcquireBodySlot_BlocksUntilRelease(t *testing.T) {
	s := newTestStreamState().withSlotCap(2)

	if err := s.acquireBodySlot(context.Background()); err != nil {
		t.Fatalf("first acquire should succeed, got %v", err)
	}
	if err := s.acquireBodySlot(context.Background()); err != nil {
		t.Fatalf("second acquire should succeed, got %v", err)
	}

	done := make(chan struct{})
	go func() {
		_ = s.acquireBodySlot(context.Background())
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("third acquire should have blocked")
	case <-time.After(20 * time.Millisecond):
	}

	s.releaseBodySlots(1)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("third acquire should have unblocked after release")
	}

	if got := len(s.slotCh); got != 2 {
		t.Errorf("slotCh occupancy = %d, want 2", got)
	}
}

// TestAcquireBodySlot_NoCap returns nil immediately when
// slotCh is nil (semaphore disabled — the test-helper path).
func TestAcquireBodySlot_NoCap(t *testing.T) {
	s := newTestStreamState()
	if err := s.acquireBodySlot(context.Background()); err != nil {
		t.Fatalf("acquireBodySlot with nil slotCh should return nil, got %v", err)
	}
	if s.slotCh != nil {
		t.Errorf("slotCh = %v, want nil (no-cap path should not allocate)",
			s.slotCh)
	}
}

// TestAcquireBodySlot_CtxCancellation guards that a blocked
// acquire returns the cancel cause when ctx is cancelled.
func TestAcquireBodySlot_CtxCancellation(t *testing.T) {
	s := newTestStreamState().withSlotCap(1)
	if err := s.acquireBodySlot(context.Background()); err != nil {
		t.Fatalf("first acquire should succeed, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() {
		got <- s.acquireBodySlot(ctx)
	}()
	time.Sleep(10 * time.Millisecond)

	cancel()

	select {
	case err := <-got:
		if err == nil {
			t.Error("acquireBodySlot should return a cancel err")
		}
	case <-time.After(time.Second):
		t.Fatal("acquireBodySlot did not return after ctx cancel")
	}
}

// TestDeadlockObserver_FiresOnStall verifies the watchdog
// surfaces a stalled pipeline by incrementing the stall
// counter. lastProgressNs is seeded with a stale timestamp so
// the very first tick past the threshold trips the alert.
//
// Timing: the watchdog runs with a 5ms tick / 50ms threshold —
// in practice the first qualifying tick fires within ~10–60ms.
// Rather than sleep a fixed ceiling and assert at the end (the
// flake-prone shape: too short → false negative on a busy CI
// host, too long → tests crawl), we poll the counter until the
// observation lands or a generous 5s deadline expires. The
// watchdog goroutine is then drained via cancel.
func TestDeadlockObserver_FiresOnStall(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader))
	m, err := newMetrics(metricsConfig{
		Meter: provider.Meter("test"),
	})
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}
	s := newTestStreamState().withSlotCap(2)
	s.m = m
	s.lastProgressNs.Store(time.Now().Add(-time.Second).UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runDeadlockObserver(ctx, "ReadIter",
			5*time.Millisecond, 50*time.Millisecond)
	}()

	if !pollUntil(t, 5*time.Second, 5*time.Millisecond, func() bool {
		return stallCounterValue(t, reader, "ReadIter") >= 1
	}) {
		t.Fatalf("stall counter never fired within 5s deadline; "+
			"final value=%d",
			stallCounterValue(t, reader, "ReadIter"))
	}
	cancel()
	<-done
}

// TestDeadlockObserver_NoSignalWhenProgress verifies the
// watchdog stays quiet when the pipeline is making forward
// progress.
//
// Timing: with a 100ms threshold and bumps every 5ms, the
// watchdog sees a stale window only if 20 consecutive bumps
// fail to schedule — a 100x scheduler slip that would imply
// the host is broken anyway. The bumper runs in its own
// ctx-driven goroutine (not a fixed-iteration loop) so it
// keeps pace under any scheduling pressure for the test's
// duration. We sample the counter at the end after a window
// long enough that the watchdog has ticked many times under
// "progress made" conditions; counter must stay zero
// throughout.
func TestDeadlockObserver_NoSignalWhenProgress(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader))
	m, err := newMetrics(metricsConfig{
		Meter: provider.Meter("test"),
	})
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}
	s := newTestStreamState().withSlotCap(2)
	s.m = m
	// Seed lastProgressNs to "now" so the first tick (before
	// the bumper goroutine even starts) doesn't trip on the
	// initial window.
	s.lastProgressNs.Store(time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bumpDone := make(chan struct{})
	go func() {
		defer close(bumpDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.lastProgressNs.Store(time.Now().UnixNano())
			}
		}
	}()

	obsDone := make(chan struct{})
	go func() {
		defer close(obsDone)
		s.runDeadlockObserver(ctx, "ReadIter",
			5*time.Millisecond, 100*time.Millisecond)
	}()

	// Run for 300ms — the watchdog ticks ~60 times in that
	// window, all observing fresh progress, none qualifying.
	time.Sleep(300 * time.Millisecond)

	// Snapshot the counter while bumps are still active so a
	// post-cancel watchdog tick on stale state can't poison the
	// reading.
	got := stallCounterValue(t, reader, "ReadIter")
	cancel()
	<-bumpDone
	<-obsDone

	if got > 0 {
		t.Errorf("stall counter = %d, want 0 (pipeline was making progress)",
			got)
	}
}

// TestDeadlockObserver_ExitsOnCtxDone verifies the watchdog
// goroutine returns promptly when its ctx is cancelled.
// Required so fetchAndDecodeIter's deferred wg.Wait()
// doesn't leak the observer past the pipeline's lifetime.
func TestDeadlockObserver_ExitsOnCtxDone(t *testing.T) {
	s := newTestStreamState().withSlotCap(2)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runDeadlockObserver(ctx, "ReadIter", time.Hour, time.Hour)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runDeadlockObserver did not return after ctx cancel")
	}
}

// TestWaitForPartition_BlocksUntilComplete verifies that the
// decoder's wait actually unblocks when downloaders finish.
func TestWaitForPartition_BlocksUntilComplete(t *testing.T) {
	s := newTestStreamState()
	s.parts = []*partState{
		{
			files:  make([]fileRow, 3),
			bodies: make([][]byte, 3),
			done:   make(chan struct{}),
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- s.waitForPartition(context.Background(), 0)
	}()

	select {
	case <-done:
		t.Fatal("waitForPartition should have blocked on 0/3 complete")
	case <-time.After(20 * time.Millisecond):
	}

	for fi := range 3 {
		s.markComplete(0, fi, []byte("x"))
	}

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("waitForPartition should have returned nil on completion, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitForPartition did not return after completion")
	}
}

// TestWithDecodeAheadPartitions_ExplicitZero verifies that
// passing 0 stays distinguishable from "option not supplied"
// — the pointer-typed field carries the explicit-zero through.
func TestWithDecodeAheadPartitions_ExplicitZero(t *testing.T) {
	o := resolveIterOpts([]ReadOption{WithDecodeAheadPartitions(0)})
	if o.decodeAheadPartitions == nil {
		t.Fatal("explicit 0 should set decodeAheadPartitions, got nil")
	}
	if *o.decodeAheadPartitions != 0 {
		t.Errorf("decodeAheadPartitions = %d, want 0",
			*o.decodeAheadPartitions)
	}
}

// TestWithDecodeAheadPartitions_NegativeFloors floors negative
// values to 0 (instead of allocating a negative-cap channel,
// which would panic).
func TestWithDecodeAheadPartitions_NegativeFloors(t *testing.T) {
	o := resolveIterOpts([]ReadOption{WithDecodeAheadPartitions(-5)})
	if o.decodeAheadPartitions == nil || *o.decodeAheadPartitions != 0 {
		t.Errorf("negative decodeAheadPartitions did not floor to 0: %v",
			o.decodeAheadPartitions)
	}
}

// TestWithDecodeAheadBytes_NegativeFloors mirrors the partitions
// option: negative caps are floored to 0 (no cap).
func TestWithDecodeAheadBytes_NegativeFloors(t *testing.T) {
	o := resolveIterOpts([]ReadOption{WithDecodeAheadBytes(-1)})
	if o.decodeAheadBytes != 0 {
		t.Errorf("negative decodeAheadBytes = %d, want 0",
			o.decodeAheadBytes)
	}
}

// TestWithFetchAheadFiles_DefaultsToZero verifies that the
// option-not-supplied path leaves fetchAheadFiles at zero so the
// downstream bodyCap derivation falls back to MaxConcurrent.
func TestWithFetchAheadFiles_DefaultsToZero(t *testing.T) {
	o := resolveIterOpts(nil)
	if o.fetchAheadFiles != 0 {
		t.Errorf("default fetchAheadFiles = %d, want 0",
			o.fetchAheadFiles)
	}
}

// TestWithFetchAheadFiles_ExplicitValueHonored confirms positive
// values land on the readOpts field verbatim (the bodyCap floor
// at maxFilesPerPartition is enforced in fetchAndDecodeIter,
// not in option resolution).
func TestWithFetchAheadFiles_ExplicitValueHonored(t *testing.T) {
	o := resolveIterOpts([]ReadOption{WithFetchAheadFiles(8)})
	if o.fetchAheadFiles != 8 {
		t.Errorf("fetchAheadFiles = %d, want 8", o.fetchAheadFiles)
	}
}

// TestWithFetchAheadFiles_NegativeFloors mirrors the other
// options: negative values floor to 0 (= use default).
func TestWithFetchAheadFiles_NegativeFloors(t *testing.T) {
	o := resolveIterOpts([]ReadOption{WithFetchAheadFiles(-3)})
	if o.fetchAheadFiles != 0 {
		t.Errorf("negative fetchAheadFiles = %d, want 0",
			o.fetchAheadFiles)
	}
}

// TestWithDecodeWorkers_DefaultsToZero verifies the option-not-
// supplied path leaves decodeWorkers at zero so the downstream
// numWorkers derivation falls back to the single-decoder default.
func TestWithDecodeWorkers_DefaultsToZero(t *testing.T) {
	o := resolveIterOpts(nil)
	if o.decodeWorkers != 0 {
		t.Errorf("default decodeWorkers = %d, want 0", o.decodeWorkers)
	}
}

// TestWithDecodeWorkers_ExplicitValueHonored confirms a positive
// value lands on the readOpts field verbatim.
func TestWithDecodeWorkers_ExplicitValueHonored(t *testing.T) {
	o := resolveIterOpts([]ReadOption{WithDecodeWorkers(8)})
	if o.decodeWorkers != 8 {
		t.Errorf("decodeWorkers = %d, want 8", o.decodeWorkers)
	}
}

// TestWithDecodeWorkers_NonPositiveFloors asserts that 0 and
// negative values floor to 1 (single-decoder, current behavior).
// Floored at the option boundary so fetchAndDecodeIter never has
// to reason about a zero-worker config that would deadlock the
// pipeline.
func TestWithDecodeWorkers_NonPositiveFloors(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		o := resolveIterOpts([]ReadOption{WithDecodeWorkers(n)})
		if o.decodeWorkers != 1 {
			t.Errorf("WithDecodeWorkers(%d): got decodeWorkers=%d, want 1",
				n, o.decodeWorkers)
		}
	}
}

// pollUntil polls pred() at the given interval until it
// returns true or the deadline elapses. Returns the final
// pred() value. Used by timing-sensitive tests to convert
// "wait long enough" assertions into "wait until or fail at
// deadline" — flakes only when the system is truly broken,
// not on slow CI runs.
func pollUntil(
	t *testing.T, deadline, interval time.Duration,
	pred func() bool,
) bool {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if pred() {
			return true
		}
		time.Sleep(interval)
	}
	return pred()
}

// stallCounterValue extracts the stall counter's data point
// matching `method` from the manual reader. Returns 0 when no
// matching data point exists (counter never fired or label
// didn't match).
func stallCounterValue(
	t *testing.T, reader sdkmetric.Reader, method string,
) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name != "s3pgstore.read.iter.stall.count" {
				continue
			}
			sum, ok := mt.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				v, ok := dp.Attributes.Value("method")
				if !ok || v.AsString() != method {
					continue
				}
				return dp.Value
			}
		}
	}
	return 0
}
