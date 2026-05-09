package s3client

import (
	"math/rand/v2"
	"time"
)

// Backoff schedule constants. Equal-jitter (window/2 + jitter)
// rather than the SDK default's full-jitter (rand(0, window))
// so every retry waits at least window/2 — no instant-retry
// after a SlowDown.
//
// Window growth: backoffBase * 2^(attempt-1), capped at
// backoffCap. With base=200ms / cap=10s the schedule is:
//
//	attempt 1 → 100–200 ms
//	attempt 2 → 200–400 ms
//	attempt 3 → 400–800 ms
//	attempt 4 → 800–1600 ms
//	attempt 5 → 1600–3200 ms
//	            (… capped at 10 s on later attempts)
//
// Worst-case wallclock across 5 attempts: ~6 s of pure backoff,
// well under the SDK default's potential 30+ s.
const (
	backoffBase = 200 * time.Millisecond
	backoffCap  = 10 * time.Second
)

// equalJitterBackoff implements retry.BackoffDelayer with the
// equal-jitter algorithm: every attempt waits at least
// window/2, with the upper half jittered uniformly to prevent
// thundering-herd retries from N concurrent callers that all
// hit a coordinated SlowDown.
type equalJitterBackoff struct {
	base time.Duration
	cap  time.Duration
}

// BackoffDelay implements retry.BackoffDelayer.
func (b equalJitterBackoff) BackoffDelay(
	attempt int, _ error,
) (time.Duration, error) {
	if attempt < 1 {
		attempt = 1
	}
	// Exponential window: base * 2^(attempt-1), with overflow
	// guard for absurd attempt counts (cap will short-circuit
	// long before, but the shift itself must stay safe).
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}
	window := b.base << shift
	if window > b.cap || window <= 0 {
		window = b.cap
	}
	half := window / 2
	if half <= 0 {
		return window, nil
	}
	return half + time.Duration(rand.Int64N(int64(half))), nil
}
