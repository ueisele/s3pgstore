package s3client

import (
	"errors"
	"testing"
	"time"
)

// TestEqualJitterBackoff_NeverInstant locks in the load-bearing
// property: every attempt waits at least window/2 (no
// rand-from-0 floor). This is the whole reason for picking
// equal-jitter over the SDK's default full-jitter — operators
// rely on a non-zero retry delay after SlowDown.
func TestEqualJitterBackoff_NeverInstant(t *testing.T) {
	b := equalJitterBackoff{
		base: 200 * time.Millisecond,
		cap:  10 * time.Second,
	}
	for attempt := 1; attempt <= 5; attempt++ {
		for range 100 {
			d, err := b.BackoffDelay(attempt, errors.New("x"))
			if err != nil {
				t.Fatalf("BackoffDelay(%d): %v", attempt, err)
			}
			// Floor: half the (capped) exponential window.
			shift := attempt - 1
			window := b.base << shift
			if window > b.cap {
				window = b.cap
			}
			minWant := window / 2
			if d < minWant {
				t.Fatalf("attempt %d: delay %v < floor %v",
					attempt, d, minWant)
			}
			if d > window {
				t.Fatalf("attempt %d: delay %v > window %v",
					attempt, d, window)
			}
		}
	}
}

// TestEqualJitterBackoff_ExponentialGrowth verifies the window
// doubles per attempt up to the cap. Sample many runs per
// attempt and check the observed max approaches the expected
// window (not a tight bound — we want >= 90% of max to confirm
// the high end of the jitter window is reachable).
func TestEqualJitterBackoff_ExponentialGrowth(t *testing.T) {
	b := equalJitterBackoff{
		base: 200 * time.Millisecond,
		cap:  10 * time.Second,
	}
	for attempt := 1; attempt <= 4; attempt++ {
		shift := attempt - 1
		window := b.base << shift
		if window > b.cap {
			window = b.cap
		}
		var maxObserved time.Duration
		for range 200 {
			d, _ := b.BackoffDelay(attempt, nil)
			if d > maxObserved {
				maxObserved = d
			}
		}
		// At 200 samples the high end of the [window/2, window)
		// range should be reached with very high probability.
		threshold := window * 9 / 10
		if maxObserved < threshold {
			t.Errorf("attempt %d: max observed %v < %v "+
				"(window %v); jitter not reaching upper end?",
				attempt, maxObserved, threshold, window)
		}
	}
}

// TestEqualJitterBackoff_RespectsCap ensures the window never
// exceeds the cap regardless of attempt count — important so a
// pathological retry doesn't compound into a multi-minute wait.
func TestEqualJitterBackoff_RespectsCap(t *testing.T) {
	b := equalJitterBackoff{
		base: 200 * time.Millisecond,
		cap:  10 * time.Second,
	}
	// Attempt 100 would mathematically yield 200ms * 2^99 (way
	// beyond any reasonable cap) — verify we still floor at
	// cap/2 and ceiling at cap.
	for range 100 {
		d, _ := b.BackoffDelay(100, nil)
		if d < b.cap/2 || d > b.cap {
			t.Fatalf("attempt 100: delay %v out of [%v, %v]",
				d, b.cap/2, b.cap)
		}
	}
}
