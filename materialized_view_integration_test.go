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
	Customer string `parquet:"customer"`
	JobID    string `parquet:"job_id"`
	Status   string `parquet:"status"`
}

// newMVCfg builds a Config with one shaped MV (customer →
// status) and one key-only MV (job_id index). The MV's Of
// closures emit one row per record from each.
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
				Name:         "customer_status",
				KeyColumns:   []string{"customer"},
				ValueColumns: []string{"status"},
				Of: func(r mvRec) ([]s3pgstore.MVRow, error) {
					return []s3pgstore.MVRow{
						{Key: []string{r.Customer},
							Value: []string{r.Status}},
					}, nil
				},
			},
			{
				Name:       "job_index",
				KeyColumns: []string{"job_id"},
				Of: func(r mvRec) ([]s3pgstore.MVRow, error) {
					if r.JobID == "" {
						return nil, nil
					}
					return []s3pgstore.MVRow{
						{Key: []string{r.JobID}},
					}, nil
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

// TestMV_WriteInsertsRowsInSameTx writes one record and
// verifies both MVs got the corresponding row. Reads back
// via Lookup for the shaped MV; via direct SQL for the
// key-only MV (the lookup constructor for empty K is
// ergonomically clunky, so we verify via SQL).
func TestMV_WriteInsertsRowsInSameTx(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	rec := mvRec{
		Customer: "alice",
		JobID:    "job-42",
		Status:   "active",
	}
	if _, err := store.Write(t.Context(), []mvRec{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Shaped MV: customer → status.
	mv, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:         "customer_status",
			KeyColumns:   []string{"customer"},
			ValueColumns: []string{"status"},
			From: func(values []string) (string, error) {
				return values[0] + "=" + values[1], nil
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
	if len(got) != 1 || got[0] != "alice=active" {
		t.Errorf("Lookup: got %v, want [alice=active]", got)
	}

	// Key-only MV: job_index.
	var jobIDCount int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_mv_job_index" `+
			`WHERE "job_id" = $1`,
		rec.JobID).Scan(&jobIDCount); err != nil {
		t.Fatalf("count job_index: %v", err)
	}
	if jobIDCount != 1 {
		t.Errorf("job_index rows for %q: got %d, want 1",
			rec.JobID, jobIDCount)
	}
}

// TestMV_ShapedConflictUpdates verifies the last-writer-wins
// semantic: a second Write to the same key updates the
// ValueColumns.
func TestMV_ShapedConflictUpdates(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	if _, err := store.Write(t.Context(),
		[]mvRec{{Customer: "alice", Status: "active"}}); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if _, err := store.Write(t.Context(),
		[]mvRec{{Customer: "alice", Status: "suspended"}}); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	mv, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:         "customer_status",
			KeyColumns:   []string{"customer"},
			ValueColumns: []string{"status"},
			From: func(values []string) (string, error) {
				return values[1], nil
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
	if len(got) != 1 || got[0] != "suspended" {
		t.Errorf("expected last-write-wins=suspended, got %v", got)
	}
}

// TestMV_KeyOnlyConflictDoesNothing verifies the first-writer-
// wins semantic: a second Write with the same key doesn't
// touch the row. Test exposes this by writing twice and
// counting rows in the key-only MV.
func TestMV_KeyOnlyConflictDoesNothing(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	for range 3 {
		if _, err := store.Write(t.Context(),
			[]mvRec{{Customer: "alice",
				JobID: "job-stable", Status: "x"}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	var n int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_mv_job_index"`,
	).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("key-only MV: got %d rows, want 1 (idempotent)", n)
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
		[]mvRec{{Customer: "alice", Status: "active"}}); err != nil {
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
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_mv_customer_status"`,
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
			Name:       "no_such_mv",
			KeyColumns: []string{"x"},
			From: func([]string) (string, error) {
				return "", nil
			},
		})
	if err == nil ||
		!strings.Contains(err.Error(), "not declared") {
		t.Fatalf("expected 'not declared' error, got %v", err)
	}
}

// TestNewMaterializedView_RejectsKeyColumnsMismatch: lookup
// def's KeyColumns must match the declared MV.
func TestNewMaterializedView_RejectsKeyColumnsMismatch(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	_, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:         "customer_status",
			KeyColumns:   []string{"unrelated"}, // wrong column
			ValueColumns: []string{"status"},
			From: func([]string) (string, error) {
				return "", nil
			},
		})
	if err == nil ||
		!strings.Contains(err.Error(), "KeyColumns mismatch") {
		t.Fatalf("expected KeyColumns error, got %v", err)
	}
}

// TestNewMaterializedView_RejectsNilFrom: From callback is
// required.
func TestNewMaterializedView_RejectsNilFrom(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	_, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:         "customer_status",
			KeyColumns:   []string{"customer"},
			ValueColumns: []string{"status"},
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
		[]mvRec{{Customer: "alice", Status: "x"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	sentinel := errors.New("from-failure")
	mv, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:         "customer_status",
			KeyColumns:   []string{"customer"},
			ValueColumns: []string{"status"},
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

// TestMV_LookupFiltersByMVColumn: filters reference MV columns
// (not part_<n>). Verify the unknown-column path errors.
func TestMV_LookupFiltersByMVColumn(t *testing.T) {
	f := newFixture(t)
	store := newMVStore(t, newMVCfg(f))

	if _, err := store.Write(t.Context(),
		[]mvRec{{Customer: "alice", Status: "x"},
			{Customer: "bob", Status: "y"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	mv, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:         "customer_status",
			KeyColumns:   []string{"customer"},
			ValueColumns: []string{"status"},
			From: func(values []string) (string, error) {
				return values[0], nil
			},
		})
	if err != nil {
		t.Fatalf("NewMaterializedView: %v", err)
	}

	// Filter by status (a value column) — should work since the
	// resolver accepts any column declared in KeyColumns +
	// ValueColumns.
	got, err := mv.Lookup(t.Context(),
		[]s3pgstore.PartitionFilter{
			s3pgstore.Eq("status", "x"),
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
			[]mvRec{{Customer: c, Status: "x"}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	mv, err := s3pgstore.NewMaterializedView(store,
		s3pgstore.MaterializedViewLookupDef[string]{
			Name:         "customer_status",
			KeyColumns:   []string{"customer"},
			ValueColumns: []string{"status"},
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
	cfg.MaterializedViews[0].Of = func(mvRec) ([]s3pgstore.MVRow, error) {
		return nil, sentinel
	}
	store := newMVStore(t, cfg)

	_, err := store.Write(t.Context(),
		[]mvRec{{Customer: "alice", Status: "x"}})
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
		`SELECT count(*) FROM "`+f.Schema+`"."s3pgstore_mv_customer_status"`,
	).Scan(&nMV); err != nil {
		t.Fatalf("count MV: %v", err)
	}
	if nFiles != 0 || nMV != 0 {
		t.Errorf("after Of failure: files=%d mv=%d (want 0/0)",
			nFiles, nMV)
	}
}
