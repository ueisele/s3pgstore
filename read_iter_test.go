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
	return &streamState{
		byteWake: make(chan struct{}, 1),
	}
}

// withSlotCap attaches a body-slot semaphore of the given
// capacity. Mirrors how downloadAndDecodeIter wires up slotCh
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

// TestReserveBytes_NoCap verifies the cap=0 fast path returns
// immediately and does not touch bufferedBytes.
func TestReserveBytes_NoCap(t *testing.T) {
	s := newTestStreamState()
	if !s.reserveBytes(context.Background(), 1<<30, 0) {
		t.Fatal("reserveBytes with cap=0 should return true")
	}
	if s.bufferedBytes != 0 {
		t.Errorf("bufferedBytes = %d, want 0 (cap=0 should not reserve)",
			s.bufferedBytes)
	}
}

// TestReserveBytes_FitsImmediately reserves a chunk inside the
// cap and verifies the running total is updated.
func TestReserveBytes_FitsImmediately(t *testing.T) {
	s := newTestStreamState()
	if !s.reserveBytes(context.Background(), 100, 1000) {
		t.Fatal("reserveBytes should succeed when fits")
	}
	if s.bufferedBytes != 100 {
		t.Errorf("bufferedBytes = %d, want 100", s.bufferedBytes)
	}
}

// TestReserveBytes_BlocksUntilRelease verifies the gate: a
// second reservation that would exceed the cap blocks until
// the first one is released.
func TestReserveBytes_BlocksUntilRelease(t *testing.T) {
	s := newTestStreamState()
	if !s.reserveBytes(context.Background(), 700, 1000) {
		t.Fatal("first reserve should succeed")
	}

	done := make(chan struct{})
	go func() {
		s.reserveBytes(context.Background(), 500, 1000)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("second reserveBytes should have blocked")
	case <-time.After(20 * time.Millisecond):
	}

	s.releaseBytes(700)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second reserveBytes should have unblocked after release")
	}

	if s.bufferedBytes != 500 {
		t.Errorf("bufferedBytes = %d, want 500 (only second still held)",
			s.bufferedBytes)
	}
}

// TestReserveBytes_OversizedSinglePartitionFlows guards the
// escape clause: when the buffer is empty, an over-cap
// reservation still proceeds (otherwise a single oversized
// partition would deadlock the pipeline).
func TestReserveBytes_OversizedSinglePartitionFlows(t *testing.T) {
	s := newTestStreamState()
	if !s.reserveBytes(context.Background(), 2000, 1000) {
		t.Fatal("oversized reserve into empty buffer should succeed")
	}
	if s.bufferedBytes != 2000 {
		t.Errorf("bufferedBytes = %d, want 2000", s.bufferedBytes)
	}
}

// TestReserveBytes_CtxCancellation guards that a blocked
// reservation returns false when ctx is cancelled — needed so
// the pipeline can shut down cleanly when the caller breaks
// out of the iter loop.
func TestReserveBytes_CtxCancellation(t *testing.T) {
	s := newTestStreamState()
	if !s.reserveBytes(context.Background(), 700, 1000) {
		t.Fatal("first reserve should succeed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan bool, 1)
	go func() {
		got <- s.reserveBytes(ctx, 500, 1000)
	}()

	time.Sleep(10 * time.Millisecond)

	cancel()

	select {
	case ok := <-got:
		if ok {
			t.Error("reserveBytes should have returned false on ctx cancel")
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

	if !s.acquireBodySlot(context.Background()) {
		t.Fatal("first acquire should succeed")
	}
	if !s.acquireBodySlot(context.Background()) {
		t.Fatal("second acquire should succeed")
	}

	done := make(chan struct{})
	go func() {
		s.acquireBodySlot(context.Background())
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

// TestAcquireBodySlot_NoCap returns true immediately when
// slotCh is nil (semaphore disabled — the test-helper path).
func TestAcquireBodySlot_NoCap(t *testing.T) {
	s := newTestStreamState()
	if !s.acquireBodySlot(context.Background()) {
		t.Fatal("acquireBodySlot with nil slotCh should return true")
	}
	if s.slotCh != nil {
		t.Errorf("slotCh = %v, want nil (no-cap path should not allocate)",
			s.slotCh)
	}
}

// TestAcquireBodySlot_CtxCancellation guards that a blocked
// acquire returns false when ctx is cancelled.
func TestAcquireBodySlot_CtxCancellation(t *testing.T) {
	s := newTestStreamState().withSlotCap(1)
	if !s.acquireBodySlot(context.Background()) {
		t.Fatal("first acquire should succeed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan bool, 1)
	go func() {
		got <- s.acquireBodySlot(ctx)
	}()
	time.Sleep(10 * time.Millisecond)

	cancel()

	select {
	case ok := <-got:
		if ok {
			t.Error("acquireBodySlot should return false on ctx cancel")
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
// Required so downloadAndDecodeIter's deferred wg.Wait()
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

	done := make(chan bool, 1)
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
	case ok := <-done:
		if !ok {
			t.Error("waitForPartition should have returned true on completion")
		}
	case <-time.After(time.Second):
		t.Fatal("waitForPartition did not return after completion")
	}
}

// TestWithReadAheadPartitions_ExplicitZero verifies that
// passing 0 stays distinguishable from "option not supplied"
// — the pointer-typed field carries the explicit-zero through.
func TestWithReadAheadPartitions_ExplicitZero(t *testing.T) {
	o := resolveIterOpts([]ReadOption{WithReadAheadPartitions(0)})
	if o.readAheadPartitions == nil {
		t.Fatal("explicit 0 should set readAheadPartitions, got nil")
	}
	if *o.readAheadPartitions != 0 {
		t.Errorf("readAheadPartitions = %d, want 0",
			*o.readAheadPartitions)
	}
}

// TestWithReadAheadPartitions_NegativeFloors floors negative
// values to 0 (instead of allocating a negative-cap channel,
// which would panic).
func TestWithReadAheadPartitions_NegativeFloors(t *testing.T) {
	o := resolveIterOpts([]ReadOption{WithReadAheadPartitions(-5)})
	if o.readAheadPartitions == nil || *o.readAheadPartitions != 0 {
		t.Errorf("negative readAheadPartitions did not floor to 0: %v",
			o.readAheadPartitions)
	}
}

// TestWithReadAheadBytes_NegativeFloors mirrors the partitions
// option: negative caps are floored to 0 (no cap).
func TestWithReadAheadBytes_NegativeFloors(t *testing.T) {
	o := resolveIterOpts([]ReadOption{WithReadAheadBytes(-1)})
	if o.readAheadBytes != 0 {
		t.Errorf("negative readAheadBytes = %d, want 0",
			o.readAheadBytes)
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
