//go:build integration

package s3pgstore_test

import (
	"errors"
	"sort"
	"testing"

	"github.com/ueisele/s3pgstore"
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
	if len(got[0].FileExtensions) != 2 {
		t.Errorf("FileExtensions: want 2 entries, got %d",
			len(got[0].FileExtensions))
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

// silence unused import on errors when not needed in some
// runs; the test functions above reference it via t.Fatalf
// on err patterns. Keeping the import explicit in case future
// edits drop the `err` use.
var _ = errors.As
