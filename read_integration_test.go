//go:build integration

package s3pgstore_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/ueisele/s3pgstore"
	"github.com/ueisele/s3pgstore/sequencer"
)

// readRec carries an entity key + version field so dedup
// behaviour can be exercised end-to-end.
type readRec struct {
	CustomerID   string  `parquet:"customer_id"`
	ChargePeriod string  `parquet:"charge_period"`
	SKU          string  `parquet:"sku"`
	NetCost      float64 `parquet:"net_cost"`
	Version      int64   `parquet:"version"`
}

func newReadCfg(f *fixture) s3pgstore.Config[readRec] {
	return s3pgstore.Config[readRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		S3Bucket:          f.Bucket,
		S3Prefix:          "billing",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"charge_period", "customer"},
		PartitionKeyOf: func(r readRec) string {
			return "charge_period=" + r.ChargePeriod +
				"/customer=" + r.CustomerID
		},
		EntityKeyOf: func(r readRec) string {
			return r.CustomerID + "|" + r.SKU
		},
		VersionOf: func(r readRec) int64 { return r.Version },
	}
}

func newReadStore(t *testing.T, f *fixture) *s3pgstore.Store[readRec] {
	t.Helper()
	cfg := newReadCfg(f)
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

// TestReadPartition_RoundTripSinglePartition: write a batch, read it
// back, verify records and Version match.
func TestReadPartition_RoundTripSinglePartition(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	want := []readRec{
		{CustomerID: "abc", ChargePeriod: "2026-03-17", SKU: "alpha",
			NetCost: 1.5, Version: 1},
		{CustomerID: "abc", ChargePeriod: "2026-03-17", SKU: "beta",
			NetCost: 2.5, Version: 1},
	}
	if _, err := store.Write(t.Context(), want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
		s3pgstore.Eq("customer", "abc"),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("partitions: want 1, got %d", len(got))
	}
	if got[0].PartitionKey != "charge_period=2026-03-17/customer=abc" {
		t.Errorf("PartitionKey: %q", got[0].PartitionKey)
	}
	if got[0].Version != 1 {
		t.Errorf("Version: want 1, got %d", got[0].Version)
	}
	if len(got[0].Records) != len(want) {
		t.Errorf("Records: want %d, got %d", len(want), len(got[0].Records))
	}
}

// TestReadPartition_DerivesVersionFromMaxFile: two writes to the same
// partition produce versions 1 and 2; the read must report
// Version=2.
func TestReadPartition_DerivesVersionFromMaxFile(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	rec1 := readRec{CustomerID: "abc", ChargePeriod: "2026-03-17",
		SKU: "x", NetCost: 1, Version: 1}
	rec2 := readRec{CustomerID: "abc", ChargePeriod: "2026-03-17",
		SKU: "y", NetCost: 2, Version: 2}

	if _, err := store.Write(t.Context(), []readRec{rec1}); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if _, err := store.Write(t.Context(), []readRec{rec2}); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	got, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
		s3pgstore.Eq("customer", "abc"),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("partitions: want 1, got %d", len(got))
	}
	if got[0].Version != 2 {
		t.Errorf("Version: want 2 (MAX(written_at_version)), got %d",
			got[0].Version)
	}
	if len(got[0].FileRefs) != 2 {
		t.Errorf("FileRefs: want 2 entries, got %d",
			len(got[0].FileRefs))
	}
}

// TestReadPartition_DedupLatestPerEntity: two writes of the same entity
// at different versions; default Read returns only the latest.
func TestReadPartition_DedupLatestPerEntity(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	old := readRec{CustomerID: "abc", ChargePeriod: "2026-03-17",
		SKU: "alpha", NetCost: 1, Version: 1}
	newRec := readRec{CustomerID: "abc", ChargePeriod: "2026-03-17",
		SKU: "alpha", NetCost: 99, Version: 2}

	if _, err := store.Write(t.Context(), []readRec{old}); err != nil {
		t.Fatalf("Write old: %v", err)
	}
	if _, err := store.Write(t.Context(), []readRec{newRec}); err != nil {
		t.Fatalf("Write new: %v", err)
	}

	got, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
		s3pgstore.Eq("customer", "abc"),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got[0].Records) != 1 {
		t.Fatalf("want 1 record after dedup, got %d", len(got[0].Records))
	}
	if got[0].Records[0].NetCost != 99 {
		t.Errorf("dedup picked wrong record: %+v", got[0].Records[0])
	}
}

// TestReadPartition_WithHistory: WithHistory disables dedup; both
// versions of the same entity come back.
func TestReadPartition_WithHistory(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	old := readRec{CustomerID: "abc", ChargePeriod: "2026-03-17",
		SKU: "alpha", NetCost: 1, Version: 1}
	newRec := readRec{CustomerID: "abc", ChargePeriod: "2026-03-17",
		SKU: "alpha", NetCost: 99, Version: 2}
	if _, err := store.Write(t.Context(), []readRec{old}); err != nil {
		t.Fatalf("Write old: %v", err)
	}
	if _, err := store.Write(t.Context(), []readRec{newRec}); err != nil {
		t.Fatalf("Write new: %v", err)
	}

	got, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
		s3pgstore.Eq("customer", "abc"),
	}, s3pgstore.WithHistory())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got[0].Records) != 2 {
		t.Fatalf("WithHistory: want 2 records, got %d", len(got[0].Records))
	}
}

// TestReadPartition_MultiPartition: filters across multiple partitions
// return them in lex order of partition key.
func TestReadPartition_MultiPartition(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	recs := []readRec{
		{CustomerID: "z", ChargePeriod: "2026-03-17", SKU: "x", Version: 1},
		{CustomerID: "a", ChargePeriod: "2026-03-17", SKU: "x", Version: 1},
		{CustomerID: "m", ChargePeriod: "2026-03-17", SKU: "x", Version: 1},
	}
	if _, err := store.Write(t.Context(), recs); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("partitions: want 3, got %d", len(got))
	}
	want := []string{
		"charge_period=2026-03-17/customer=a",
		"charge_period=2026-03-17/customer=m",
		"charge_period=2026-03-17/customer=z",
	}
	for i, w := range want {
		if got[i].PartitionKey != w {
			t.Errorf("got[%d]: %q want %q", i, got[i].PartitionKey, w)
		}
	}
}

// TestReadPartition_FilterCompositions: Or + And produce the expected
// row sets.
func TestReadPartition_FilterCompositions(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	recs := []readRec{
		{CustomerID: "a", ChargePeriod: "2026-03-17", SKU: "x", Version: 1},
		{CustomerID: "b", ChargePeriod: "2026-03-17", SKU: "x", Version: 1},
		{CustomerID: "c", ChargePeriod: "2026-03-18", SKU: "x", Version: 1},
		{CustomerID: "d", ChargePeriod: "2026-04-01", SKU: "x", Version: 1},
	}
	if _, err := store.Write(t.Context(), recs); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Or(
			s3pgstore.And(
				s3pgstore.Eq("charge_period", "2026-03-17"),
				s3pgstore.Eq("customer", "a"),
			),
			s3pgstore.And(
				s3pgstore.Eq("charge_period", "2026-03-18"),
				s3pgstore.Eq("customer", "c"),
			),
		),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("partitions: want 2, got %d", len(got))
	}
	gotKeys := []string{got[0].PartitionKey, got[1].PartitionKey}
	sort.Strings(gotKeys)
	want := []string{
		"charge_period=2026-03-17/customer=a",
		"charge_period=2026-03-18/customer=c",
	}
	for i, w := range want {
		if gotKeys[i] != w {
			t.Errorf("[%d] %q != %q", i, gotKeys[i], w)
		}
	}
}

// TestReadPartition_Between: half-open range filter selects the right
// partitions.
func TestReadPartition_Between(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	recs := []readRec{
		{CustomerID: "x", ChargePeriod: "2026-02-28", SKU: "a", Version: 1},
		{CustomerID: "x", ChargePeriod: "2026-03-01", SKU: "a", Version: 1},
		{CustomerID: "x", ChargePeriod: "2026-03-31", SKU: "a", Version: 1},
		{CustomerID: "x", ChargePeriod: "2026-04-01", SKU: "a", Version: 1},
	}
	if _, err := store.Write(t.Context(), recs); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Between("charge_period", "2026-03-01", "2026-04-01"),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Between [2026-03-01, 2026-04-01): want 2 partitions, got %d",
			len(got))
	}
}

// TestReadPartition_NoMatch: filters that match no rows return nil.
func TestReadPartition_NoMatch(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	got, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "1999-01-01"),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got != nil {
		t.Fatalf("no match: want nil, got %v", got)
	}
}

// TestReadPartition_EmptyFilters: empty filter slice → nil result, no
// SQL query, no S3 traffic.
func TestReadPartition_EmptyFilters(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	got, err := store.ReadPartition(t.Context(), nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got != nil {
		t.Fatalf("nil filters: want nil, got %v", got)
	}
}

// TestReadPartition_UnknownFilterPart: a filter referencing a part that
// isn't declared errors at translation, before any SQL executes.
func TestReadPartition_UnknownFilterPart(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)
	_, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("not_declared", "x"),
	})
	if err == nil {
		t.Fatal("want error for unknown filter part")
	}
}

// TestReadPartition_AfterRollback: a write that rolls back is invisible
// to subsequent Read calls — the atomic-visibility-on-commit
// invariant from CLAUDE.md.
func TestReadPartition_AfterRollback(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	rec := readRec{CustomerID: "abc", ChargePeriod: "2026-03-17",
		SKU: "x", Version: 1}

	tx, err := f.Pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()

	ctx := s3pgstore.WithTx(t.Context(), tx)
	if _, err := store.Write(ctx, []readRec{rec}); err != nil {
		t.Fatalf("Write inside tx: %v", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	got, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
		s3pgstore.Eq("customer", "abc"),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got != nil {
		t.Fatalf("rolled-back write visible: %v", got)
	}
}

// TestReadPartition_StableAcrossCalls: two consecutive Read calls with
// no intervening writes return the same records in the same
// order. CLAUDE.md's read-stability invariant.
func TestReadPartition_StableAcrossCalls(t *testing.T) {
	f := newFixture(t)
	store := newReadStore(t, f)

	recs := []readRec{
		{CustomerID: "a", ChargePeriod: "2026-03-17", SKU: "x", Version: 1},
		{CustomerID: "b", ChargePeriod: "2026-03-17", SKU: "y", Version: 1},
		{CustomerID: "c", ChargePeriod: "2026-03-17", SKU: "z", Version: 1},
	}
	if _, err := store.Write(t.Context(), recs); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got1, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
	})
	if err != nil {
		t.Fatalf("Read 1: %v", err)
	}
	got2, err := store.ReadPartition(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
	})
	if err != nil {
		t.Fatalf("Read 2: %v", err)
	}

	if len(got1) != len(got2) {
		t.Fatalf("len differs across calls")
	}
	for i := range got1 {
		if got1[i].PartitionKey != got2[i].PartitionKey {
			t.Errorf("[%d] partition order differs", i)
		}
		if got1[i].Version != got2[i].Version {
			t.Errorf("[%d] version differs", i)
		}
	}
}

// iterRec is a minimal record type for ReadIter family tests.
type iterRec struct {
	Customer string `parquet:"customer"`
	Value    int64  `parquet:"value"`
}

func newIterCfg(f *fixture) s3pgstore.Config[iterRec] {
	return s3pgstore.Config[iterRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		S3Bucket:          f.Bucket,
		S3Prefix:          "iter",
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

	bufferedRecs, err := store.Read(t.Context(), filters)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	want := []int64{}
	for _, r := range bufferedRecs {
		want = append(want, r.Value)
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
// matching partition with Records, Version, and FileRefs
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
		if len(p.FileRefs) != 1 {
			t.Errorf("FileRefs for %q: got %d, want 1",
				p.PartitionKey, len(p.FileRefs))
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

// TestReadRangeIter_TimeWindow: two writes seeded with a
// timestamp captured between them land in different written_at
// buckets; the time-window walk should pick up only rows whose
// written_at falls in the range.
func TestReadRangeIter_TimeWindow(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	if _, err := store.Write(t.Context(),
		[]iterRec{{Customer: "alice", Value: 1}}); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	mid := time.Now()
	time.Sleep(50 * time.Millisecond)

	if _, err := store.Write(t.Context(),
		[]iterRec{{Customer: "bob", Value: 2}}); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	// No sequencer call: ReadRangeIter filters by written_at and
	// is decoupled from feed_seq assignment per CLAUDE.md's
	// atomic-visibility-on-commit invariant.

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
// yield every committed row.
func TestReadRangeIter_UnboundedReturnsAll(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	for _, c := range []string{"a", "b", "c"} {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

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

// TestReadFileRefsIter_DecodesPolledEntries: poll → filter →
// decode round-trip. Confirms ReadFileRefsIter correctly
// decodes the parquet bodies pointed to by Poll output.
func TestReadFileRefsIter_DecodesPolledEntries(t *testing.T) {
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
	for r, err := range store.ReadFileRefsIter(t.Context(), entries) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		got = append(got, r.Value)
	}
	if len(got) != 3 {
		t.Errorf("got %d, want 3", len(got))
	}
}

// TestReadFileRefsIter_RejectsForeignEntries: an entry whose
// S3Key doesn't live under this Store's data prefix is
// rejected before any S3 traffic.
func TestReadFileRefsIter_RejectsForeignEntries(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	bad := []s3pgstore.FileRef{
		{Offset: 1, PartitionKey: "customer=alice",
			S3Key: "different-prefix/data/customer=alice/x.parquet"},
	}
	var captured error
	for _, err := range store.ReadFileRefsIter(t.Context(), bad) {
		if err != nil {
			captured = err
			break
		}
	}
	if captured == nil {
		t.Fatal("expected validation error for foreign S3Key")
	}
	if !strings.Contains(captured.Error(),
		"does not belong to this Store") {
		t.Errorf("error text: %v", captured)
	}
}

// TestReadPartitionFileRefsIter_GroupsByKey: entries are grouped
// by Key (partition); per-partition output reflects that
// grouping.
func TestReadPartitionFileRefsIter_GroupsByKey(t *testing.T) {
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
	for p, err := range store.ReadPartitionFileRefsIter(
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

// TestReadIter_WithDecodeAheadBytes_GatesDecoder verifies the
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
// With WithDecodeAheadBytes(1) and any partition uncomp > 1
// byte, the predicate `bufferedBytes <= 0 || ... <= cap`
// fails on the first attempt (non-empty buffer, uncomp
// dominates the 1-byte cap). The decoder parks, waited=true,
// recordIterByteBudgetWait fires when releaseBytes wakes it.
// Asserts the exhausted counter increments at least N-1 times
// — one wait event per partition past the first.
func TestReadIter_WithDecodeAheadBytes_GatesDecoder(t *testing.T) {
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
		// decodeAheadPartitions large enough that the channel
		// cap NEVER gates — we want the byte budget to be the
		// only contention point.
		s3pgstore.WithDecodeAheadPartitions(numPartitions),
		s3pgstore.WithDecodeAheadBytes(1),
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

// TestReadIter_WithDecodeAheadPartitions_Zero_NoDeadlock guards
// the explicit-no-buffer mode. n=0 makes the per-worker queue
// unbuffered — the worker's send must rendezvous with the emit
// loop's receive on every partition, never accumulating ahead.
// A regression that broke handoff under cap=0 would deadlock
// before the iter could yield the second partition.
//
// Setup is deliberately small (3 partitions, 1 record each)
// so the test runs in well under a second: we're verifying
// the deadlock-free path, not throughput. The 30-second test
// timeout would catch a true deadlock.
func TestReadIter_WithDecodeAheadPartitions_Zero_NoDeadlock(t *testing.T) {
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
		s3pgstore.WithDecodeAheadPartitions(0),
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

// TestReadIter_WithDecodeWorkers_PreservesOrderAndCount fans
// decode out across multiple workers and verifies the iter
// still yields every record AND the partition emit order stays
// lex-stable (CLAUDE.md "Deterministic emission order"). With
// W workers handling pi via round-robin (pi % W), workers
// complete partitions out of order; the emit loop's
// sequential walk reads queue[(pi mod W)] in pi order so the
// consumer sees lex order.
//
// Uses 12 partitions / W=4 — enough that each worker handles 3
// partitions, exercising the round-robin path without inflating
// runtime.
func TestReadIter_WithDecodeWorkers_PreservesOrderAndCount(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	const numPartitions = 12
	customers := make([]string, numPartitions)
	for i := range customers {
		// Lex-sortable single-letter prefix + index so
		// partition_key ordering is predictable: c00..c11.
		customers[i] = "c" + string('0'+rune(i/10)) + string('0'+rune(i%10))
	}
	for _, c := range customers {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c, Value: 1},
				{Customer: c, Value: 2}},
		); err != nil {
			t.Fatalf("Write %s: %v", c, err)
		}
	}

	gotCustomers := []string{}
	count := 0
	for r, err := range store.ReadIter(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.GE("customer", "c00"),
		},
		s3pgstore.WithDecodeWorkers(4),
	) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		count++
		// Track first occurrence of each customer to verify
		// per-partition emit order.
		if len(gotCustomers) == 0 || gotCustomers[len(gotCustomers)-1] != r.Customer {
			gotCustomers = append(gotCustomers, r.Customer)
		}
	}

	if count != numPartitions*2 {
		t.Errorf("yielded %d records, want %d", count, numPartitions*2)
	}
	if len(gotCustomers) != numPartitions {
		t.Errorf("saw %d distinct customers, want %d",
			len(gotCustomers), numPartitions)
	}
	// Lex order: c00, c01, ..., c11.
	for i, c := range gotCustomers {
		want := customers[i]
		if c != want {
			t.Errorf("partition emit order broken at index %d: got %q, want %q",
				i, c, want)
		}
	}
}

// TestReadIter_WithDecodeWorkers_PerWorkerByteBudget verifies
// the per-worker WithDecodeAheadBytes cap composes correctly
// with WithDecodeWorkers. With W=4 workers each capped at 1
// byte and partitions larger than 1 byte, the empty-buffer
// escape must let each partition through OR the pipeline would
// deadlock. Asserts every record still emits.
//
// This is the per-worker analog of
// TestReadIter_WithDecodeAheadBytes_GatesDecoder — proves the
// per-worker reservation honors the same correctness invariant
// (oversized partition flows on empty buffer).
func TestReadIter_WithDecodeWorkers_PerWorkerByteBudget(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	const numPartitions = 8
	for i := 0; i < numPartitions; i++ {
		c := "c" + string('0'+rune(i))
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c, Value: 1}}); err != nil {
			t.Fatalf("Write %s: %v", c, err)
		}
	}

	count := 0
	for r, err := range store.ReadIter(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.GE("customer", "c0"),
		},
		s3pgstore.WithDecodeWorkers(4),
		s3pgstore.WithDecodeAheadBytes(1),
	) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		_ = r
		count++
	}
	if count != numPartitions {
		t.Errorf("yielded %d records, want %d (per-worker byte cap "+
			"must permit oversized partitions via empty-buffer escape)",
			count, numPartitions)
	}
}

// TestRead_FlatRecordSlice verifies the new Read returns a flat
// []T across all matched partitions, with records in lex
// partition order. Auto-defaults the batch parallel-decode path
// — no explicit WithDecodeWorkers needed.
func TestRead_FlatRecordSlice(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	customers := []string{"alice", "bob", "carol", "dave"}
	for _, c := range customers {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c, Value: 1}, {Customer: c, Value: 2}},
		); err != nil {
			t.Fatalf("Write %s: %v", c, err)
		}
	}

	got, err := store.Read(t.Context(),
		[]s3pgstore.PartitionFilter{s3pgstore.GE("customer", "alice")})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != len(customers)*2 {
		t.Fatalf("len(got)=%d, want %d",
			len(got), len(customers)*2)
	}
	// Records arrive in lex partition-key order, then in write
	// order within a partition.
	for i, want := range customers {
		if got[i*2].Customer != want || got[i*2+1].Customer != want {
			t.Errorf("got[%d..%d] customer = %q,%q, want %q",
				i*2, i*2+1, got[i*2].Customer,
				got[i*2+1].Customer, want)
		}
	}
}

// TestReadPartition_PerPartitionResults verifies the per-partition
// variant returns []PartitionResult[T] with Records, Version, and
// FileRefs populated; same auto-default behavior as Read.
func TestReadPartition_PerPartitionResults(t *testing.T) {
	f := newFixture(t)
	store := newIterStore(t, f)

	customers := []string{"alice", "bob", "carol"}
	for _, c := range customers {
		if _, err := store.Write(t.Context(),
			[]iterRec{{Customer: c, Value: 7}}); err != nil {
			t.Fatalf("Write %s: %v", c, err)
		}
	}

	got, err := store.ReadPartition(t.Context(),
		[]s3pgstore.PartitionFilter{s3pgstore.GE("customer", "alice")})
	if err != nil {
		t.Fatalf("ReadPartition: %v", err)
	}
	if len(got) != len(customers) {
		t.Fatalf("len(got)=%d, want %d", len(got), len(customers))
	}
	for i, want := range customers {
		wantKey := "customer=" + want
		if got[i].PartitionKey != wantKey {
			t.Errorf("got[%d].PartitionKey = %q, want %q",
				i, got[i].PartitionKey, wantKey)
		}
		if got[i].Version != 1 {
			t.Errorf("got[%d].Version = %d, want 1",
				i, got[i].Version)
		}
		if len(got[i].Records) != 1 {
			t.Errorf("got[%d].Records: len=%d, want 1",
				i, len(got[i].Records))
		}
		if len(got[i].FileRefs) != 1 {
			t.Errorf("got[%d].FileRefs: len=%d, want 1",
				i, len(got[i].FileRefs))
		}
	}
}
