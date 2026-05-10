//go:build integration

package s3pgstore_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/ueisele/s3pgstore"
)

func TestWrite_IdempotentRetryReturnsSameResult(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	rec := writeRec{CustomerID: "abc", ChargePeriod: "2026-03-17", NetCost: 1.5}
	first, err := store.Write(t.Context(), []writeRec{rec},
		s3pgstore.WithIdempotencyToken("retry-token-1"))
	if err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	second, err := store.Write(t.Context(), []writeRec{rec},
		s3pgstore.WithIdempotencyToken("retry-token-1"))
	if err != nil {
		t.Fatalf("Write 2 (retry): %v", err)
	}
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("len(first)=%d len(second)=%d", len(first), len(second))
	}
	if first[0].FileID != second[0].FileID {
		t.Errorf("FileID changed under retry: %d vs %d",
			first[0].FileID, second[0].FileID)
	}
	if first[0].S3Key != second[0].S3Key {
		t.Errorf("S3Key changed under retry: %q vs %q",
			first[0].S3Key, second[0].S3Key)
	}
	if first[0].Version != second[0].Version {
		t.Errorf("Version changed under retry: %d vs %d",
			first[0].Version, second[0].Version)
	}

	// Catalog has exactly one row for this partition (no
	// duplicate from the retry).
	var n int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_files" `+
			`WHERE partition_key = $1`,
		first[0].PartitionKey).Scan(&n); err != nil {
		t.Fatalf("count files: %v", err)
	}
	if n != 1 {
		t.Errorf("retry produced extra rows: count=%d", n)
	}
}

func TestWrite_IdempotentTokenScopedPerPartition(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	a := writeRec{CustomerID: "a", ChargePeriod: "2026-03-17"}
	b := writeRec{CustomerID: "b", ChargePeriod: "2026-03-17"}

	r1, err := store.Write(t.Context(), []writeRec{a},
		s3pgstore.WithIdempotencyToken("same-token"))
	if err != nil {
		t.Fatalf("Write a: %v", err)
	}
	r2, err := store.Write(t.Context(), []writeRec{b},
		s3pgstore.WithIdempotencyToken("same-token"))
	if err != nil {
		t.Fatalf("Write b: %v", err)
	}
	if r1[0].FileID == r2[0].FileID {
		t.Fatal("same token in different partitions collapsed — " +
			"per-partition scope broken")
	}
}

func TestWrite_IdempotentTokenOf(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	calls := 0
	tokenOf := func(records []writeRec) (string, error) {
		calls++
		return "tok-" + records[0].CustomerID, nil
	}

	rec := writeRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	first, err := store.Write(t.Context(), []writeRec{rec},
		s3pgstore.WithIdempotencyTokenOf(tokenOf))
	if err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	second, err := store.Write(t.Context(), []writeRec{rec},
		s3pgstore.WithIdempotencyTokenOf(tokenOf))
	if err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if first[0].FileID != second[0].FileID {
		t.Errorf("retry produced different FileID: %d vs %d",
			first[0].FileID, second[0].FileID)
	}
	if calls != 2 {
		t.Errorf("tokenOf calls: want 2 (one per Write), got %d", calls)
	}
}

func TestWrite_IdempotencyOptionsMutuallyExclusive(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	rec := writeRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	_, err := store.Write(t.Context(), []writeRec{rec},
		s3pgstore.WithIdempotencyToken("a"),
		s3pgstore.WithIdempotencyTokenOf(
			func([]writeRec) (string, error) { return "b", nil }),
	)
	if err == nil {
		t.Fatal("want mutual-exclusion error, got nil")
	}
}

func TestWrite_OCCFirstWriteAtVersionZero(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	rec := writeRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	r, err := store.Write(t.Context(), []writeRec{rec},
		s3pgstore.WithExpectedVersion(0))
	if err != nil {
		t.Fatalf("Write at version=0: %v", err)
	}
	if r[0].Version != 1 {
		t.Errorf("first write Version: want 1, got %d", r[0].Version)
	}
}

func TestWrite_OCCMatchingVersion(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	rec := writeRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	r1, err := store.Write(t.Context(), []writeRec{rec})
	if err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	r2, err := store.Write(t.Context(), []writeRec{rec},
		s3pgstore.WithExpectedVersion(r1[0].Version))
	if err != nil {
		t.Fatalf("Write 2 with matching expected version: %v", err)
	}
	if r2[0].Version != r1[0].Version+1 {
		t.Errorf("second write Version: want %d, got %d",
			r1[0].Version+1, r2[0].Version)
	}
}

func TestWrite_OCCStaleVersionConflicts(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	rec := writeRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	if _, err := store.Write(t.Context(), []writeRec{rec}); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if _, err := store.Write(t.Context(), []writeRec{rec}); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	// Now version=2; a write expecting 1 must fail.
	_, err := store.Write(t.Context(), []writeRec{rec},
		s3pgstore.WithExpectedVersion(1))
	if !errors.Is(err, s3pgstore.ErrVersionConflict) {
		t.Fatalf("OCC stale: want ErrVersionConflict, got %v", err)
	}

	// The OCC failure is a fail-fast path that never touched
	// S3, so the pre-tx pending_writes row must have been
	// proactively cleaned up — leaving GC nothing to do.
	var pending int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_pending_writes"`,
	).Scan(&pending); err != nil {
		t.Fatalf("count pending_writes: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending_writes after OCC fail-fast: want 0, got %d",
			pending)
	}
}

func TestWrite_OCCExpectedVersionOnNonExistent(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	// expected=5 on a partition that doesn't exist → conflict
	// (the upsert can't satisfy version=5 on the conflict
	// branch and the INSERT branch only matches version=0).
	rec := writeRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	_, err := store.Write(t.Context(), []writeRec{rec},
		s3pgstore.WithExpectedVersion(5))
	if !errors.Is(err, s3pgstore.ErrVersionConflict) {
		t.Fatalf("OCC on missing partition with expected>0: "+
			"want ErrVersionConflict, got %v", err)
	}
}

func TestLookupByToken_Existing(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	rec := writeRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	written, err := store.Write(t.Context(), []writeRec{rec},
		s3pgstore.WithIdempotencyToken("lookup-token"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, ok, err := store.LookupByToken(t.Context(),
		written[0].PartitionKey, "lookup-token")
	if err != nil {
		t.Fatalf("LookupByToken: %v", err)
	}
	if !ok {
		t.Fatal("LookupByToken: want exists=true, got false")
	}
	if got.FileID != written[0].FileID {
		t.Errorf("FileID: want %d, got %d", written[0].FileID, got.FileID)
	}
	if got.FileSize != written[0].FileSize {
		t.Errorf("FileSize via lookup: want %d, got %d",
			written[0].FileSize, got.FileSize)
	}
	if got.UncompressedSize != written[0].UncompressedSize {
		t.Errorf("UncompressedSize via lookup: want %d, got %d",
			written[0].UncompressedSize, got.UncompressedSize)
	}
}

func TestLookupByToken_Missing(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	_, ok, err := store.LookupByToken(t.Context(),
		"charge_period=2026-03-17/customer=ghost", "no-such-token")
	if err != nil {
		t.Fatalf("LookupByToken: %v", err)
	}
	if ok {
		t.Fatal("LookupByToken on missing: want exists=false, got true")
	}
}

func TestLookupByToken_RejectsEmptyArgs(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	if _, _, err := store.LookupByToken(t.Context(), "", "x"); err == nil {
		t.Error("empty partitionKey: want error")
	}
	if _, _, err := store.LookupByToken(t.Context(),
		"charge_period=p/customer=c", ""); err == nil {
		t.Error("empty token: want error")
	}
}

// TestWrite_OCCConcurrentWritersOneSurvives: launches N
// concurrent writers that all read the same starting version
// and try to write at that version; PostgreSQL's row-level
// locking on the partition row plus the OCC CAS guarantees
// exactly one succeeds and the rest get ErrVersionConflict.
func TestWrite_OCCConcurrentWritersOneSurvives(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	rec := writeRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	if _, err := store.Write(t.Context(), []writeRec{rec}); err != nil {
		t.Fatalf("seed Write: %v", err)
	}

	const N = 10
	var wg sync.WaitGroup
	wg.Add(N)
	successes := 0
	conflicts := 0
	var mu sync.Mutex

	for range N {
		go func() {
			defer wg.Done()
			_, err := store.Write(t.Context(), []writeRec{rec},
				s3pgstore.WithExpectedVersion(1))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successes++
			} else if errors.Is(err, s3pgstore.ErrVersionConflict) {
				conflicts++
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Errorf("expected exactly 1 success, got %d (conflicts=%d)",
			successes, conflicts)
	}
	if conflicts != N-1 {
		t.Errorf("expected %d conflicts, got %d", N-1, conflicts)
	}
}

// TestWrite_IdempotentTokenConcurrentRetries: N concurrent
// writers with the same token must converge on a single
// catalog row; the partial UNIQUE on (partition_key, token)
// catches the race and writePartition transparently
// re-fetches the canonical row.
func TestWrite_IdempotentTokenConcurrentRetries(t *testing.T) {
	f := newFixture(t)
	store := newWriteStore(t, f)

	rec := writeRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	const N = 10
	var wg sync.WaitGroup
	wg.Add(N)
	results := make([]s3pgstore.FileRef, N)
	errs := make([]error, N)

	for i := range N {
		go func(i int) {
			defer wg.Done()
			r, err := store.Write(t.Context(), []writeRec{rec},
				s3pgstore.WithIdempotencyToken("concurrent-token"))
			if err != nil {
				errs[i] = err
				return
			}
			results[i] = r[0]
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	// All workers must have observed the same FileID — the
	// canonical row.
	first := results[0].FileID
	for i, r := range results {
		if r.FileID != first {
			t.Errorf("worker %d FileID=%d != %d", i, r.FileID, first)
		}
	}

	// Catalog has exactly one row for this partition.
	var n int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_files" `+
			`WHERE partition_key = $1`,
		results[0].PartitionKey).Scan(&n); err != nil {
		t.Fatalf("count files: %v", err)
	}
	if n != 1 {
		t.Errorf("concurrent retries produced %d catalog rows, want 1", n)
	}
}
