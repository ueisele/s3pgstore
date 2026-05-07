//go:build integration

package s3pgstore_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/ueisele/s3pgstore"
)

type mvRec struct {
	Customer     string `parquet:"customer"`
	JobID        string `parquet:"job_id"`
	SKU          string `parquet:"sku"`
	ChargePeriod string `parquet:"charge_period"`
}

// newMVCfg builds a Config with two MVs that exercise the
// set-membership semantics:
//
//   - "customer_sku_period" — three-column tuple, the canonical
//     "find customers using sku at period" use case.
//   - "job_index" — single-column existence index.
func newMVCfg(f *fixture) s3pgstore.Config[mvRec] {
	return s3pgstore.Config[mvRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		Bucket:            f.Bucket,
		Prefix:            "mv",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"customer"},
		PartitionKeyOf: func(r mvRec) string {
			return "customer=" + r.Customer
		},
		MaterializedViews: []s3pgstore.MaterializedViewDef[mvRec]{
			{
				Name: "customer_sku_period",
				Columns: []string{
					"customer", "sku", "charge_period",
				},
				Of: func(r mvRec) ([][]string, error) {
					if r.SKU == "" || r.ChargePeriod == "" {
						return nil, nil
					}
					return [][]string{
						{r.Customer, r.SKU, r.ChargePeriod},
					}, nil
				},
			},
			{
				Name:    "job_index",
				Columns: []string{"job_id"},
				Of: func(r mvRec) ([][]string, error) {
					if r.JobID == "" {
						return nil, nil
					}
					return [][]string{{r.JobID}}, nil
				},
			},
		},
	}
}

func newMVStore(t *testing.T, cfg s3pgstore.Config[mvRec]) *s3pgstore.Store[mvRec] {
	t.Helper()
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

// TestMV_WriteInsertsTuplesInSameTx: one record produces one
// tuple per declared MV. Verifies the multi-column MV via
// Lookup and the single-column MV via direct count.
func TestMV_WriteInsertsTuplesInSameTx(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	rec := mvRec{
		Customer:     "alice",
		JobID:        "job-42",
		SKU:          "compute",
		ChargePeriod: "2026-03",
	}
	if _, err := store.Write(t.Context(), []mvRec{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Multi-column MV: lookup via the SKU index.
	mv, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:    "customer_sku_period",
			Columns: []string{"customer", "sku", "charge_period"},
			From: func(values []string) (string, error) {
				return values[0], nil // project customer
			},
		})
	if err != nil {
		t.Fatalf("NewMaterializedView: %v", err)
	}
	got, err := mv.Lookup(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.Eq("sku", "compute"),
			s3pgstore.Eq("charge_period", "2026-03"),
		})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(got) != 1 || got[0] != "alice" {
		t.Errorf("Lookup: got %v, want [alice]", got)
	}

	// Single-column MV via direct count.
	var n int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_mv_job_index" `+
			`WHERE "job_id" = $1`,
		rec.JobID).Scan(&n); err != nil {
		t.Fatalf("count job_index: %v", err)
	}
	if n != 1 {
		t.Errorf("job_index rows for %q: got %d, want 1",
			rec.JobID, n)
	}
}

// TestMV_DuplicateTupleIsNoOp: writing the same record twice
// produces one MV row per declared MV. ON CONFLICT DO NOTHING
// makes re-inserts of the same tuple idempotent — bounded
// table size, no MVCC churn.
func TestMV_DuplicateTupleIsNoOp(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	rec := mvRec{
		Customer:     "alice",
		JobID:        "job-stable",
		SKU:          "compute",
		ChargePeriod: "2026-03",
	}
	for range 3 {
		if _, err := store.Write(t.Context(),
			[]mvRec{rec}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	for _, mv := range []string{
		"s3pgstore_mv_customer_sku_period",
		"s3pgstore_mv_job_index",
	} {
		var n int
		if err := f.Pool.QueryRow(t.Context(),
			`SELECT count(*) FROM "`+f.Schema+`"."`+mv+`"`,
		).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", mv, n)
		}
		if n != 1 {
			t.Errorf("%s: got %d rows, want 1 (idempotent)",
				mv, n)
		}
	}
}

// TestMV_DistinctTuplesAccumulate: the same key value with
// different tail-column values produces multiple rows. This is
// the load-bearing semantic for the "false positives OK, false
// negatives never" contract — every distinct tuple ever
// observed is preserved.
func TestMV_DistinctTuplesAccumulate(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	// Same customer, different (sku, period) pairs across
	// writes.
	writes := []mvRec{
		{Customer: "alice", SKU: "compute", ChargePeriod: "2026-03"},
		{Customer: "alice", SKU: "storage", ChargePeriod: "2026-03"},
		{Customer: "alice", SKU: "compute", ChargePeriod: "2026-04"},
	}
	for _, r := range writes {
		if _, err := store.Write(t.Context(),
			[]mvRec{r}); err != nil {
			t.Fatalf("Write %+v: %v", r, err)
		}
	}

	mv, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[[3]string]{
			Name:    "customer_sku_period",
			Columns: []string{"customer", "sku", "charge_period"},
			From: func(v []string) ([3]string, error) {
				return [3]string{v[0], v[1], v[2]}, nil
			},
		})
	if err != nil {
		t.Fatalf("NewMaterializedView: %v", err)
	}
	got, err := mv.Lookup(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.Eq("customer", "alice"),
		})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("got %d tuples, want 3 (one per distinct "+
			"(sku, period) combination)", len(got))
	}
}

// TestMV_RollbackPreservesConsistency: when the host tx rolls
// back, both the file row AND the MV rows disappear together.
// Same tx → atomic visibility.
func TestMV_RollbackPreservesConsistency(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	tx, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	ctx := s3pgstore.WithTx(t.Context(), tx)

	if _, err := store.Write(ctx,
		[]mvRec{{
			Customer: "alice", SKU: "compute",
			ChargePeriod: "2026-03",
		}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var nFiles, nMV int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_files"`,
	).Scan(&nFiles); err != nil {
		t.Fatalf("count files: %v", err)
	}
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_mv_customer_sku_period"`,
	).Scan(&nMV); err != nil {
		t.Fatalf("count MV: %v", err)
	}
	if nFiles != 0 || nMV != 0 {
		t.Errorf("after rollback: files=%d mv=%d (want 0/0)",
			nFiles, nMV)
	}
}

// TestNewMaterializedView_RejectsUndeclaredName: lookup name
// must match a declared MV.
func TestNewMaterializedView_RejectsUndeclaredName(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	_, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:    "no_such_mv",
			Columns: []string{"x"},
			From: func([]string) (string, error) {
				return "", nil
			},
		})
	if err == nil ||
		!strings.Contains(err.Error(), "not declared") {
		t.Fatalf("expected 'not declared' error, got %v", err)
	}
}

// TestNewMaterializedView_RejectsColumnsMismatch: lookup def's
// Columns must match the declared MV's Columns verbatim.
func TestNewMaterializedView_RejectsColumnsMismatch(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	_, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:    "customer_sku_period",
			Columns: []string{"customer", "wrong_col", "charge_period"},
			From: func([]string) (string, error) {
				return "", nil
			},
		})
	if err == nil ||
		!strings.Contains(err.Error(), "Columns mismatch") {
		t.Fatalf("expected Columns mismatch error, got %v", err)
	}
}

// TestNewMaterializedView_RejectsNilFrom: From callback is
// required.
func TestNewMaterializedView_RejectsNilFrom(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	_, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:    "customer_sku_period",
			Columns: []string{"customer", "sku", "charge_period"},
		})
	if err == nil ||
		!strings.Contains(err.Error(), "From is required") {
		t.Fatalf("expected From error, got %v", err)
	}
}

// TestMV_LookupPropagatesFromError: an error from the From
// closure surfaces with the row index.
func TestMV_LookupPropagatesFromError(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	if _, err := store.Write(t.Context(),
		[]mvRec{{
			Customer: "alice", SKU: "compute",
			ChargePeriod: "2026-03",
		}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	sentinel := errors.New("from-failure")
	mv, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:    "customer_sku_period",
			Columns: []string{"customer", "sku", "charge_period"},
			From: func([]string) (string, error) {
				return "", sentinel
			},
		})
	if err != nil {
		t.Fatalf("NewMaterializedView: %v", err)
	}
	_, err = mv.Lookup(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.Eq("customer", "alice"),
		})
	if !errors.Is(err, sentinel) {
		t.Errorf("Lookup error: want sentinel, got %v", err)
	}
}

// TestMV_LookupFiltersByAnyColumn: filters reference any MV
// column. Verify the unknown-column path errors.
func TestMV_LookupFiltersByAnyColumn(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	for _, r := range []mvRec{
		{Customer: "alice", SKU: "compute", ChargePeriod: "2026-03"},
		{Customer: "bob", SKU: "storage", ChargePeriod: "2026-03"},
	} {
		if _, err := store.Write(t.Context(),
			[]mvRec{r}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	mv, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:    "customer_sku_period",
			Columns: []string{"customer", "sku", "charge_period"},
			From: func(values []string) (string, error) {
				return values[0], nil
			},
		})
	if err != nil {
		t.Fatalf("NewMaterializedView: %v", err)
	}

	// Filter by sku — non-leading column. Should work since the
	// resolver accepts any declared column.
	got, err := mv.Lookup(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.Eq("sku", "compute"),
		})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(got) != 1 || got[0] != "alice" {
		t.Errorf("got %v, want [alice]", got)
	}

	// Filter by an undeclared column → error.
	_, err = mv.Lookup(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.Eq("nonexistent", "x"),
		})
	if err == nil ||
		!strings.Contains(err.Error(), "unknown column") {
		t.Errorf("expected unknown-column error, got %v", err)
	}
}

// TestMV_EmptyFiltersReturnsAllRows: Lookup with empty filters
// returns every MV row.
func TestMV_EmptyFiltersReturnsAllRows(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	for _, c := range []string{"alice", "bob", "carol"} {
		if _, err := store.Write(t.Context(),
			[]mvRec{{
				Customer:     c,
				SKU:          "compute",
				ChargePeriod: "2026-03",
			}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	mv, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:    "customer_sku_period",
			Columns: []string{"customer", "sku", "charge_period"},
			From: func(values []string) (string, error) {
				return values[0], nil
			},
		})
	if err != nil {
		t.Fatalf("NewMaterializedView: %v", err)
	}

	got, err := mv.Lookup(t.Context(), nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("Lookup with no filters: got %d, want 3", len(got))
	}
}

// TestMV_OfErrorAbortsWrite: a failing Of closure aborts the
// Write before any catalog or MV row is inserted, preserving
// the all-or-nothing contract.
func TestMV_OfErrorAbortsWrite(t *testing.T) {
	f := newFixture(t)
	cfg := newMVCfg(f)
	sentinel := errors.New("of-failure")
	cfg.MaterializedViews[0].Of = func(mvRec) ([][]string, error) {
		return nil, sentinel
	}
	store := newMVStore(t, cfg)

	_, err := store.Write(t.Context(),
		[]mvRec{{
			Customer: "alice", SKU: "compute",
			ChargePeriod: "2026-03",
		}})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel from Of, got %v", err)
	}

	// No file row, no MV rows.
	var nFiles, nMV int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_files"`,
	).Scan(&nFiles); err != nil {
		t.Fatalf("count files: %v", err)
	}
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_mv_customer_sku_period"`,
	).Scan(&nMV); err != nil {
		t.Fatalf("count MV: %v", err)
	}
	if nFiles != 0 || nMV != 0 {
		t.Errorf("after Of failure: files=%d mv=%d (want 0/0)",
			nFiles, nMV)
	}
}

// TestMV_TupleShapeMismatchAbortsWrite: an Of closure that
// emits a tuple with the wrong column count fails the Write
// before the catalog tx opens.
func TestMV_TupleShapeMismatchAbortsWrite(t *testing.T) {
	f := newFixture(t)
	cfg := newMVCfg(f)
	cfg.MaterializedViews[0].Of = func(mvRec) ([][]string, error) {
		return [][]string{{"only-one-value"}}, nil // wants 3
	}
	store := newMVStore(t, cfg)

	_, err := store.Write(t.Context(),
		[]mvRec{{
			Customer: "alice", SKU: "compute",
			ChargePeriod: "2026-03",
		}})
	if err == nil ||
		!strings.Contains(err.Error(), "tuple len") {
		t.Errorf("got %v, want tuple-len mismatch error", err)
	}
}
