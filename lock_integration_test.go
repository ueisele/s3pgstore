//go:build integration

package s3pgstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ueisele/s3pgstore"
)

// lockRec is a minimal record type for LockPartition tests. We
// only need a Store[T] with a working catalog schema and a
// PartitionKeyOf — record content is irrelevant to advisory-lock
// behavior.
type lockRec struct {
	ID string `parquet:"id"`
}

func newLockCfg(f *fixture) s3pgstore.Config[lockRec] {
	return s3pgstore.Config[lockRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		S3Bucket:          f.Bucket,
		S3Prefix:          "lock",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"customer"},
		PartitionKeyOf:    func(r lockRec) string { return "customer=" + r.ID },
	}
}

func newLockStore(t *testing.T, f *fixture) *s3pgstore.Store[lockRec] {
	t.Helper()
	cfg := newLockCfg(f)
	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create schema: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })
	store, err := s3pgstore.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New store: %v", err)
	}
	return store
}

// TestLockPartition_BlocksConcurrentLocker verifies that two
// LockPartition calls on the same partition serialize: the
// second must wait until the first transaction releases (commit
// or rollback). Implemented as: tx A takes the lock and parks;
// tx B tries to take the same lock with a 2s timeout. While A
// holds the lock, B should still be blocked. After A rolls
// back, B should acquire promptly.
func TestLockPartition_BlocksConcurrentLocker(t *testing.T) {
	f := newFixture(t)
	store := newLockStore(t, f)

	const partitionKey = "customer=alice"

	txA, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	defer func() { _ = txA.Rollback(context.Background()) }()
	ctxA := s3pgstore.WithTx(t.Context(), txA)
	if err := store.LockPartition(ctxA, partitionKey); err != nil {
		t.Fatalf("LockPartition A: %v", err)
	}

	// B tries to acquire while A still holds. Use a tight
	// timeout so a buggy implementation surfaces fast — and
	// confirms the SET LOCAL lock_timeout path works.
	bResult := make(chan error, 1)
	go func() {
		txB, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
		if err != nil {
			bResult <- err
			return
		}
		defer func() { _ = txB.Rollback(context.Background()) }()
		ctxB := s3pgstore.WithTx(t.Context(), txB)
		bResult <- store.LockPartition(ctxB, partitionKey,
			s3pgstore.WithLockTimeout(500*time.Millisecond))
	}()

	// B should hit the timeout while A still holds the lock.
	select {
	case err := <-bResult:
		if !errors.Is(err, s3pgstore.ErrLockTimeout) {
			t.Fatalf("B (concurrent locker, timeout 500ms): "+
				"want ErrLockTimeout, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("B never returned within 3s")
	}

	// Now release A. A second tx C should acquire immediately.
	if err := txA.Rollback(t.Context()); err != nil {
		t.Fatalf("rollback A: %v", err)
	}

	txC, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx C: %v", err)
	}
	defer func() { _ = txC.Rollback(context.Background()) }()
	ctxC := s3pgstore.WithTx(t.Context(), txC)
	cStart := time.Now()
	if err := store.LockPartition(ctxC, partitionKey,
		s3pgstore.WithLockTimeout(2*time.Second)); err != nil {
		t.Fatalf("LockPartition C (after A released): %v", err)
	}
	if elapsed := time.Since(cStart); elapsed > 1*time.Second {
		t.Errorf("LockPartition C took %v after A released "+
			"(expected near-instant)", elapsed)
	}
}

// TestLockPartition_DifferentKeysDontBlock verifies that two
// concurrent LockPartition calls on different partition keys
// proceed independently — advisory locks are per-key, so
// unrelated partitions never serialize.
func TestLockPartition_DifferentKeysDontBlock(t *testing.T) {
	f := newFixture(t)
	store := newLockStore(t, f)

	txA, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	defer func() { _ = txA.Rollback(context.Background()) }()
	ctxA := s3pgstore.WithTx(t.Context(), txA)
	if err := store.LockPartition(ctxA, "customer=alice"); err != nil {
		t.Fatalf("LockPartition A: %v", err)
	}

	// B locks a different partition. Should proceed without
	// blocking even with a tight timeout.
	txB, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx B: %v", err)
	}
	defer func() { _ = txB.Rollback(context.Background()) }()
	ctxB := s3pgstore.WithTx(t.Context(), txB)
	if err := store.LockPartition(ctxB, "customer=bob",
		s3pgstore.WithLockTimeout(500*time.Millisecond)); err != nil {
		t.Fatalf("LockPartition B (different key): %v", err)
	}
}

// TestLockPartition_DoesNotBlockReader verifies that a held
// LockPartition does not block plain SQL operations on the
// catalog tables — only other LockPartition holders block. A
// concurrent Read continues to see committed state.
func TestLockPartition_DoesNotBlockReader(t *testing.T) {
	f := newFixture(t)
	store := newLockStore(t, f)

	// Seed one record so the read path has something to fetch.
	if _, err := store.Write(t.Context(),
		[]lockRec{{ID: "alice"}}); err != nil {
		t.Fatalf("seed Write: %v", err)
	}

	txA, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	defer func() { _ = txA.Rollback(context.Background()) }()
	ctxA := s3pgstore.WithTx(t.Context(), txA)
	if err := store.LockPartition(ctxA, "customer=alice"); err != nil {
		t.Fatalf("LockPartition A: %v", err)
	}

	// Concurrent Read on the same partition should not block.
	// Use a tight ctx deadline so a buggy implementation that
	// inadvertently blocks surfaces as a deadline-exceeded.
	readCtx, cancel := context.WithTimeout(t.Context(),
		3*time.Second)
	defer cancel()
	results, err := store.ReadPartition(readCtx,
		[]s3pgstore.PartitionFilter{s3pgstore.Eq("customer", "alice")})
	if err != nil {
		t.Fatalf("Read while LockPartition held: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Read results: want 1, got %d", len(results))
	}
}

// TestLockPartition_ReleaseOnRollback verifies the lock is
// released when the holding transaction rolls back — the
// pg_advisory_xact_lock contract. Sequence: A locks, A rolls
// back, B locks the same key (must succeed near-instantly).
func TestLockPartition_ReleaseOnRollback(t *testing.T) {
	f := newFixture(t)
	store := newLockStore(t, f)

	const partitionKey = "customer=alice"

	txA, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	ctxA := s3pgstore.WithTx(t.Context(), txA)
	if err := store.LockPartition(ctxA, partitionKey); err != nil {
		t.Fatalf("LockPartition A: %v", err)
	}
	if err := txA.Rollback(t.Context()); err != nil {
		t.Fatalf("rollback A: %v", err)
	}

	txB, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx B: %v", err)
	}
	defer func() { _ = txB.Rollback(context.Background()) }()
	ctxB := s3pgstore.WithTx(t.Context(), txB)
	if err := store.LockPartition(ctxB, partitionKey,
		s3pgstore.WithLockTimeout(500*time.Millisecond)); err != nil {
		t.Fatalf("LockPartition B (after A rolled back): %v", err)
	}
}

// TestLockPartition_ReleaseOnCommit verifies the lock is
// released when the holding transaction commits — same shape as
// the rollback test, just commits instead of rolls back.
func TestLockPartition_ReleaseOnCommit(t *testing.T) {
	f := newFixture(t)
	store := newLockStore(t, f)

	const partitionKey = "customer=alice"

	txA, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	ctxA := s3pgstore.WithTx(t.Context(), txA)
	if err := store.LockPartition(ctxA, partitionKey); err != nil {
		t.Fatalf("LockPartition A: %v", err)
	}
	if err := txA.Commit(t.Context()); err != nil {
		t.Fatalf("commit A: %v", err)
	}

	txB, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx B: %v", err)
	}
	defer func() { _ = txB.Rollback(context.Background()) }()
	ctxB := s3pgstore.WithTx(t.Context(), txB)
	if err := store.LockPartition(ctxB, partitionKey,
		s3pgstore.WithLockTimeout(500*time.Millisecond)); err != nil {
		t.Fatalf("LockPartition B (after A committed): %v", err)
	}
}

// TestLockPartition_CooperativeWriterNotBlocked verifies the
// cooperative-protocol semantics: a Write that does NOT call
// LockPartition is not blocked by a holder. Advisory locks
// serialize only between holders of the same key — non-holders
// proceed without blocking.
//
// This is a deliberate design choice (CLAUDE.md backend
// assumptions). Operators relying on cooperative locking must
// ensure all writers of a partition take the lock; this test
// codifies the "non-holder is not blocked" half of that
// contract.
func TestLockPartition_CooperativeWriterNotBlocked(t *testing.T) {
	f := newFixture(t)
	store := newLockStore(t, f)

	const partitionKey = "customer=alice"

	// A holds the lock.
	txA, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	defer func() { _ = txA.Rollback(context.Background()) }()
	ctxA := s3pgstore.WithTx(t.Context(), txA)
	if err := store.LockPartition(ctxA, partitionKey); err != nil {
		t.Fatalf("LockPartition A: %v", err)
	}

	// B writes without taking the lock. Should not block.
	writeCtx, cancel := context.WithTimeout(t.Context(),
		5*time.Second)
	defer cancel()
	if _, err := store.Write(writeCtx,
		[]lockRec{{ID: "alice"}}); err != nil {
		t.Fatalf("Write without LockPartition (cooperative): %v", err)
	}
}

// TestLockPartition_TimeoutErrIsClassifiable verifies that the
// lock_timeout path returns ErrLockTimeout (errors.Is-matchable)
// and not a wrapped pg error or a generic "acquire" error.
// Operators rely on this to retry with backoff vs. fail loudly.
func TestLockPartition_TimeoutErrIsClassifiable(t *testing.T) {
	f := newFixture(t)
	store := newLockStore(t, f)

	const partitionKey = "customer=alice"

	txA, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	defer func() { _ = txA.Rollback(context.Background()) }()
	ctxA := s3pgstore.WithTx(t.Context(), txA)
	if err := store.LockPartition(ctxA, partitionKey); err != nil {
		t.Fatalf("LockPartition A: %v", err)
	}

	txB, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx B: %v", err)
	}
	defer func() { _ = txB.Rollback(context.Background()) }()
	ctxB := s3pgstore.WithTx(t.Context(), txB)
	err = store.LockPartition(ctxB, partitionKey,
		s3pgstore.WithLockTimeout(100*time.Millisecond))
	if !errors.Is(err, s3pgstore.ErrLockTimeout) {
		t.Fatalf("want ErrLockTimeout, got %v", err)
	}
}

// TestLockPartition_DeadlockDetected verifies PostgreSQL's
// deadlock detector fires when two transactions take advisory
// locks on (A, B) vs (B, A). One transaction is aborted with
// SQLSTATE 40P01 (deadlock_detected); the library surfaces the
// underlying error so the caller can react.
//
// We don't expose a sentinel for deadlock errors — operators
// can match the SQLSTATE if they need to. The point of this
// test is to verify the library doesn't deadlock the pool or
// silently recover.
func TestLockPartition_DeadlockDetected(t *testing.T) {
	f := newFixture(t)
	store := newLockStore(t, f)

	const keyA = "customer=alice"
	const keyB = "customer=bob"

	type result struct {
		who string
		err error
	}
	results := make(chan result, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	// Tx 1: lock A then B.
	go func() {
		defer wg.Done()
		tx, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
		if err != nil {
			results <- result{"tx1", err}
			return
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		ctx := s3pgstore.WithTx(t.Context(), tx)
		if err := store.LockPartition(ctx, keyA); err != nil {
			results <- result{"tx1", err}
			return
		}
		// Sleep so tx2 reliably takes B before we try.
		time.Sleep(200 * time.Millisecond)
		err = store.LockPartition(ctx, keyB)
		results <- result{"tx1", err}
	}()

	// Tx 2: lock B then A.
	go func() {
		defer wg.Done()
		tx, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
		if err != nil {
			results <- result{"tx2", err}
			return
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		ctx := s3pgstore.WithTx(t.Context(), tx)
		if err := store.LockPartition(ctx, keyB); err != nil {
			results <- result{"tx2", err}
			return
		}
		time.Sleep(200 * time.Millisecond)
		err = store.LockPartition(ctx, keyA)
		results <- result{"tx2", err}
	}()

	wg.Wait()
	close(results)

	// Exactly one tx should fail (PG aborts the deadlock
	// loser); the other succeeds. We don't care which is
	// which.
	successes, failures := 0, 0
	for r := range results {
		if r.err == nil {
			successes++
		} else {
			failures++
		}
	}
	if successes != 1 || failures != 1 {
		t.Errorf("deadlock outcome: want 1 success + 1 failure, "+
			"got %d successes, %d failures", successes, failures)
	}
}
