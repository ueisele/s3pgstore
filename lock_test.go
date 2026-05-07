package s3pgstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestLockKey_Determinism verifies the FNV-64a hash is stable
// across calls — load-bearing for cross-process LockPartition
// coordination (two processes that compute different keys for
// the same partition would silently fail to serialize).
func TestLockKey_Determinism(t *testing.T) {
	cases := []string{
		"",
		"a",
		"customer=abc/charge_period=2026-03-17",
		// Multi-byte / non-ASCII to ensure byte-level hashing,
		// not rune-level.
		"日本語",
	}
	for _, k := range cases {
		got1 := lockKey(k)
		got2 := lockKey(k)
		if got1 != got2 {
			t.Errorf("lockKey(%q) not stable: %d vs %d", k, got1, got2)
		}
	}
}

// TestLockKey_Distinct verifies different inputs (in our usual
// shape) produce distinct lock keys. FNV-64a doesn't guarantee
// no collisions across the full input space — but for partition
// keys an operator might realistically use (a few thousand
// keys), the chance of collision is negligible. This test
// catches a degenerate hasher that maps everything to the same
// value.
func TestLockKey_Distinct(t *testing.T) {
	keys := []string{
		"customer=a", "customer=b", "customer=c",
		"customer=a/period=2026-01", "customer=a/period=2026-02",
	}
	seen := make(map[int64]string, len(keys))
	for _, k := range keys {
		v := lockKey(k)
		if other, dup := seen[v]; dup {
			t.Errorf("lockKey collision: %q and %q both map to %d",
				other, k, v)
		}
		seen[v] = k
	}
}

// TestLockPartition_RequiresTx verifies that calling
// LockPartition without WithTx returns ErrLockNotInTransaction
// without touching any database connection. The fakeTx is never
// used because the early-return short-circuits before
// Executor.Run is invoked — so we can use a nil pool and still
// observe the right error.
func TestLockPartition_RequiresTx(t *testing.T) {
	s := &Store[struct{}]{
		cfg: Config[struct{}]{
			Executor: NewPoolExecutor(nil),
		},
	}
	err := s.LockPartition(context.Background(), "p")
	if !errors.Is(err, ErrLockNotInTransaction) {
		t.Fatalf("LockPartition without tx: want %v, got %v",
			ErrLockNotInTransaction, err)
	}
}

// TestWithLockTimeout_ApplyOption verifies the option-bag plumbing.
// We don't exercise the SQL path here — that's the integration
// suite's job — just that applyLock writes the duration.
func TestWithLockTimeout_ApplyOption(t *testing.T) {
	var o lockOpts
	WithLockTimeout(250 * time.Millisecond).applyLock(&o)
	if o.timeout != 250*time.Millisecond {
		t.Errorf("timeout: want 250ms, got %v", o.timeout)
	}
}
