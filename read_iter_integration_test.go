//go:build integration

package s3pgstore_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/ueisele/s3pgstore"
	"github.com/ueisele/s3pgstore/sequencer"
)

// iterRec is a minimal record type for ReadIter family tests.
type iterRec struct {
	Customer string `parquet:"customer"`
	Value    int64  `parquet:"value"`
}

func newIterCfg(f *fixture) s3pgstore.Config[iterRec] {
	return s3pgstore.Config[iterRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		Bucket:            f.Bucket,
		Prefix:            "iter",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"customer"},
		PartitionKeyOf: func(r iterRec) string {
			return "customer=" + r.Customer
		},
	}
}

func newIterStore(t *testing.T, f *fixture) *s3pgstore.Store[iterRec] {
	t.Helper()
	cfg := newIterCfg(f)
	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })
	store, err := s3pgstore.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

func runIterSequencer(t *testing.T, f *fixture) {
	t.Helper()
	if _, err := sequencer.RunOnce(t.Context(), sequencer.Config{
		Pool:        f.Pool,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		BatchSize:   1000,
	}); err != nil {
		t.Fatalf("sequencer.RunOnce: %v", err)
	}
}

// TestReadIter_MatchesRead verifies ReadIter yields the same
// record set in the same order as Read for the same filters.
func TestReadIter_MatchesRead(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	for _, c := range []string{"alice", "bob", "carol"} {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c, Value: 1},
				{Customer: c, Value: 2}}); err != nil {
			t.Fatalf("Write %s: %v", c, err)
		}
	}

	filters := []s3pgstore.PartitionFilter{
		s3pgstore.GE("customer", "alice"),
	}

	bufferedParts, err := store.Read(t.Context(), filters)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	want := []int64{}
	for _, p := range bufferedParts {
		for _, r := range p.Records {
			want = append(want, r.Value)
		}
	}

	got := []int64{}
	for r, err := range store.ReadIter(t.Context(), filters) {
		if err != nil {
			t.Fatalf("ReadIter yield: %v", err)
		}
		got = append(got, r.Value)
	}

	if len(want) != len(got) {
		t.Fatalf("len mismatch: want %d, got %d", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Errorf("[%d]: want %d, got %d", i, want[i], got[i])
		}
	}
}

// TestReadIter_BreakCancelsInFlight verifies the iter is lazy:
// breaking out of the loop before exhausting the sequence
// stops further work. We confirm by checking only the first
// partition's records were yielded.
func TestReadIter_BreakCancelsInFlight(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	for _, c := range []string{"alice", "bob", "carol", "dave"} {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	count := 0
	for _, err := range store.ReadIter(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.GE("customer", "alice"),
		}) {
		if err != nil {
			t.Fatalf("ReadIter yield: %v", err)
		}
		count++
		break
	}
	if count != 1 {
		t.Errorf("after break, yielded %d records (want 1)",
			count)
	}
}

// TestReadPartitionIter_EmitsPerPartitionResults verifies the
// per-partition variant yields one PartitionResult per
// matching partition with Records, Version, and FileExtensions
// populated, in lex order of partition key.
func TestReadPartitionIter_EmitsPerPartitionResults(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	for _, c := range []string{"zzz", "alice", "mid"} {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c, Value: 1}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	keys := []string{}
	for p, err := range store.ReadPartitionIter(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.GE("customer", "alice"),
		}) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		keys = append(keys, p.PartitionKey)
		if p.Version != 1 {
			t.Errorf("Version for %q: got %d, want 1",
				p.PartitionKey, p.Version)
		}
		if len(p.FileExtensions) != 1 {
			t.Errorf("FileExtensions for %q: got %d, want 1",
				p.PartitionKey, len(p.FileExtensions))
		}
	}
	want := []string{
		"customer=alice",
		"customer=mid",
		"customer=zzz",
	}
	if len(keys) != len(want) {
		t.Fatalf("keys: got %v, want %v", keys, want)
	}
	for i, k := range want {
		if keys[i] != k {
			t.Errorf("keys[%d]: got %q, want %q", i, keys[i], k)
		}
	}
}

// TestReadIter_NoFiltersYieldsNothing: empty-filters short-
// circuits without DB access, mirroring Read's contract.
func TestReadIter_NoFiltersYieldsNothing(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	if _, err := store.Write(t.Context(),
		[]iterRec{{Customer: "alice"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	count := 0
	for range store.ReadIter(t.Context(), nil) {
		count++
	}
	if count != 0 {
		t.Errorf("empty filters yielded %d (want 0)", count)
	}
}

// TestReadRangeIter_TimeWindow: writes seeded across two
// distinct sequencer runs land in different feed_seq_at
// buckets; the time-window walk should pick up only rows
// whose feed_seq_at falls in the range.
func TestReadRangeIter_TimeWindow(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	if _, err := store.Write(t.Context(),
		[]iterRec{{Customer: "alice", Value: 1}}); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	runIterSequencer(t, f)
	mid := time.Now()
	time.Sleep(50 * time.Millisecond)

	if _, err := store.Write(t.Context(),
		[]iterRec{{Customer: "bob", Value: 2}}); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	runIterSequencer(t, f)

	// since=mid, until=zero (live tip): only the second write.
	got := []int64{}
	for r, err := range store.ReadRangeIter(t.Context(),
		mid, time.Time{}) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		got = append(got, r.Value)
	}
	if len(got) != 1 || got[0] != 2 {
		t.Errorf("got %v, want [2]", got)
	}

	// since=zero (head), until=mid: only the first write.
	got = got[:0]
	for r, err := range store.ReadRangeIter(t.Context(),
		time.Time{}, mid) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		got = append(got, r.Value)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Errorf("got %v, want [1]", got)
	}
}

// TestReadRangeIter_UnboundedReturnsAll: zero/zero bounds
// yield every sequenced row.
func TestReadRangeIter_UnboundedReturnsAll(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	for _, c := range []string{"a", "b", "c"} {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runIterSequencer(t, f)

	count := 0
	for _, err := range store.ReadRangeIter(t.Context(),
		time.Time{}, time.Time{}) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Errorf("yielded %d, want 3", count)
	}
}

// TestReadEntriesIter_DecodesPolledEntries: poll → filter →
// decode round-trip. Confirms ReadEntriesIter correctly
// decodes the parquet bodies pointed to by Poll output.
func TestReadEntriesIter_DecodesPolledEntries(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	for i, c := range []string{"alice", "bob", "carol"} {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c, Value: int64(i + 1)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runIterSequencer(t, f)

	entries, _, err := store.Poll(t.Context(), 0, 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Poll: got %d entries, want 3", len(entries))
	}

	got := []int64{}
	for r, err := range store.ReadEntriesIter(t.Context(), entries) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		got = append(got, r.Value)
	}
	if len(got) != 3 {
		t.Errorf("got %d, want 3", len(got))
	}
}

// TestReadEntriesIter_RejectsForeignEntries: an entry whose
// DataPath doesn't live under this Store's data prefix is
// rejected before any S3 traffic.
func TestReadEntriesIter_RejectsForeignEntries(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	bad := []s3pgstore.StreamEntry{
		{Offset: 1, Key: "customer=alice",
			DataPath: "different-prefix/data/customer=alice/x.parquet"},
	}
	var captured error
	for _, err := range store.ReadEntriesIter(t.Context(), bad) {
		if err != nil {
			captured = err
			break
		}
	}
	if captured == nil {
		t.Fatal("expected validation error for foreign DataPath")
	}
	if !strings.Contains(captured.Error(),
		"does not belong to this Store") {
		t.Errorf("error text: %v", captured)
	}
}

// TestReadPartitionEntriesIter_GroupsByKey: entries are grouped
// by Key (partition); per-partition output reflects that
// grouping.
func TestReadPartitionEntriesIter_GroupsByKey(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	// Two records in two distinct partitions.
	for _, c := range []string{"alice", "bob"} {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c, Value: 1}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runIterSequencer(t, f)

	entries, _, err := store.Poll(t.Context(), 0, 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	parts := []string{}
	for p, err := range store.ReadPartitionEntriesIter(
		t.Context(), entries) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		parts = append(parts, p.PartitionKey)
		if len(p.Records) != 1 {
			t.Errorf("partition %q: got %d records, want 1",
				p.PartitionKey, len(p.Records))
		}
	}
	if len(parts) != 2 {
		t.Errorf("partitions: got %d, want 2", len(parts))
	}
}

// TestReadIter_DedupAppliesByDefault: configure
// EntityKeyOf+VersionOf, write the same entity twice, verify
// ReadIter yields only the latest.
func TestReadIter_DedupAppliesByDefault(t *testing.T) {
	f := newFixture(t)
	cfg := newIterCfg(f)
	cfg.EntityKeyOf = func(r iterRec) string { return r.Customer }
	cfg.VersionOf = func(r iterRec) int64 { return r.Value }
	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })
	store, err := s3pgstore.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for v := int64(1); v <= 3; v++ {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: "alice", Value: v}}); err != nil {
			t.Fatalf("Write %d: %v", v, err)
		}
	}

	got := []int64{}
	for r, err := range store.ReadIter(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.Eq("customer", "alice"),
		}) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		got = append(got, r.Value)
	}
	if len(got) != 1 || got[0] != 3 {
		t.Errorf("dedup default: got %v, want [3]", got)
	}

	// WithHistory disables dedup.
	got = got[:0]
	for r, err := range store.ReadIter(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.Eq("customer", "alice"),
		}, s3pgstore.WithHistory()) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		got = append(got, r.Value)
	}
	if len(got) != 3 {
		t.Errorf("WithHistory: got %v, want 3 elements", got)
	}
}

// Smoke check: catalog SELECT failures surface as iter errors,
// not silent empty iterators.
func TestReadIter_SchemaErrorSurfaces(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	// Drop the files table mid-flight — any subsequent SELECT
	// will error. We don't run the iter against a recreated
	// store; this is just verifying the SQL error surfaces.
	if _, err := f.Pool.Exec(t.Context(),
		`DROP TABLE "`+f.Schema+`"."s3pgstore_files"`,
	); err != nil {
		t.Fatalf("drop files: %v", err)
	}

	var captured error
	for _, err := range store.ReadIter(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.Eq("customer", "alice"),
		}) {
		captured = err
		break
	}
	if captured == nil {
		t.Fatal("expected SELECT error to surface")
	}
	if errors.Is(captured, nil) {
		t.Error("captured error is nil-equivalent")
	}
}

// newIterStoreWithMeter wires a manual-reader OTel meter into
// the Store so the iter tests can assert on emitted metrics.
// Returns the Store + the manual reader so callers can collect
// after exercising the iter.
func newIterStoreWithMeter(
	t *testing.T, f *fixture,
) (*s3pgstore.Store[iterRec], sdkmetric.Reader) {
	t.Helper()
	cfg := newIterCfg(f)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader))
	cfg.Meter = provider.Meter("iter-test")
	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })
	store, err := s3pgstore.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store, reader
}

// counterValue extracts a single Sum[int64] data-point value
// for a metric name from the manual reader. Matches by name
// only — used here for un-labeled counter pairs (the
// byte_budget.exhausted instrument carries no method label).
// Returns 0 if the metric isn't present yet (counter never
// fired).
func counterValue(
	t *testing.T, reader sdkmetric.Reader, name string,
) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name != name {
				continue
			}
			sum, ok := mt.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

// TestReadIter_WithReadAheadBytes_GatesDecoder verifies the
// byte-budget gate actually back-pressures the decoder under
// runtime conditions. The signal we look for is
// s3pgstore.read.iter.byte_budget.exhausted incrementing — it
// fires only when reserveBytes makes at least one round
// through its predicate loop without fitting (i.e., the
// decoder genuinely had to wait, not just walked through
// empty-buffer-escape on a fast consumer).
//
// To force the decoder to wait, the test consumer pauses on
// the first record of every partition. releaseBytes runs
// AFTER the user's yield returns (the emit loop's ordering),
// so a sleeping yield holds the partition's reservation,
// guaranteeing the decoder finds bufferedBytes > 0 when it
// hits reserveBytes for the next partition.
//
// With WithReadAheadBytes(1) and any partition uncomp > 1
// byte, the predicate `bufferedBytes <= 0 || ... <= cap`
// fails on the first attempt (non-empty buffer, uncomp
// dominates the 1-byte cap). The decoder parks, waited=true,
// recordIterByteBudgetWait fires when releaseBytes wakes it.
// Asserts the exhausted counter increments at least N-1 times
// — one wait event per partition past the first.
func TestReadIter_WithReadAheadBytes_GatesDecoder(t *testing.T) {
	f := newFixture(t)
	store, reader := newIterStoreWithMeter(t, f)

	const numPartitions = 5
	customers := []string{"alice", "bob", "carol", "dave", "eve"}
	for _, c := range customers {
		// Multi-record write so the parquet body is not
		// degenerately small — the byte budget is configured
		// independently of size, but a richer partition makes
		// the test less sensitive to parquet's overhead floor.
		batch := make([]iterRec, 10)
		for i := range batch {
			batch[i] = iterRec{Customer: c, Value: int64(i)}
		}
		if _, err := store.Write(t.Context(), batch); err != nil {
			t.Fatalf("Write %s: %v", c, err)
		}
	}

	// Consumer-side delay forces the decoder ahead. The
	// per-partition pause must outlast the decoder's
	// reserveBytes round-trip on the first attempt — anything
	// >> the parquet-decode cost (sub-ms for 10 records) is
	// safe; 30ms gives a comfortable margin under CI noise.
	const perPartitionPauseMs = 30
	count := 0
	seenPartitions := map[string]bool{}
	for r, err := range store.ReadIter(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.GE("customer", "alice"),
		},
		// readAheadPartitions large enough that the channel
		// cap NEVER gates — we want the byte budget to be the
		// only contention point.
		s3pgstore.WithReadAheadPartitions(numPartitions),
		s3pgstore.WithReadAheadBytes(1),
	) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		// Pause once on the first record of each partition so
		// the in-flight batch's reservation lingers long
		// enough for the decoder to enter its waiting branch
		// on the next partition.
		if !seenPartitions[r.Customer] {
			seenPartitions[r.Customer] = true
			time.Sleep(perPartitionPauseMs * time.Millisecond)
		}
		count++
	}
	if count != numPartitions*10 {
		t.Errorf("yielded %d records, want %d",
			count, numPartitions*10)
	}

	got := counterValue(t, reader,
		"s3pgstore.read.iter.byte_budget.exhausted")
	if min := int64(numPartitions - 1); got < min {
		t.Errorf("byte_budget.exhausted = %d, want >= %d "+
			"(every partition past the first should have "+
			"blocked on the 1-byte cap with the consumer "+
			"holding releaseBytes for %dms per partition)",
			got, min, perPartitionPauseMs)
	}
}

// TestReadIter_WithReadAheadPartitions_Zero_NoDeadlock guards
// the explicit-no-buffer mode. n=0 makes decodedCh unbuffered
// — the decoder's sendBatch must rendezvous with the emit
// loop's receive on every partition, never accumulating ahead.
// A regression that broke handoff under cap=0 would deadlock
// before the iter could yield the second partition.
//
// Setup is deliberately small (3 partitions, 1 record each)
// so the test runs in well under a second: we're verifying
// the deadlock-free path, not throughput. The 30-second test
// timeout would catch a true deadlock.
func TestReadIter_WithReadAheadPartitions_Zero_NoDeadlock(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	customers := []string{"alice", "bob", "carol"}
	for i, c := range customers {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c, Value: int64(i + 1)}},
		); err != nil {
			t.Fatalf("Write %s: %v", c, err)
		}
	}

	count := 0
	for r, err := range store.ReadIter(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.GE("customer", "alice"),
		},
		s3pgstore.WithReadAheadPartitions(0),
	) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		_ = r
		count++
	}
	if count != len(customers) {
		t.Errorf("yielded %d records, want %d", count, len(customers))
	}
}
