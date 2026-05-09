//go:build integration

package s3pgstore_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ueisele/s3pgstore"
)

type metaRec struct {
	CustomerID   string  `parquet:"customer_id"`
	ChargePeriod string  `parquet:"charge_period"`
	NetCost      float64 `parquet:"net_cost"`
}

func newMetaCfg(f *fixture) s3pgstore.Config[metaRec] {
	return s3pgstore.Config[metaRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		S3Bucket:          f.Bucket,
		S3Prefix:          "billing",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"charge_period", "customer"},
		PartitionKeyOf: func(r metaRec) string {
			return "charge_period=" + r.ChargePeriod +
				"/customer=" + r.CustomerID
		},
		ExtensionColumns: []s3pgstore.ExtensionColumn{
			{Name: "job_id", Type: "TEXT"},
			{Name: "tenant_id", Type: "UUID"},
			{Name: "calculated_at", Type: "TIMESTAMPTZ"},
		},
	}
}

func newMetaStore(t *testing.T, f *fixture) *s3pgstore.Store[metaRec] {
	t.Helper()
	cfg := newMetaCfg(f)
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

func TestWithMetadata_RoundTrip(t *testing.T) {
	fix := newFixture(t)
	store := newMetaStore(t, fix)

	tenantID := uuid.New()
	calcAt := time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC)

	rec := metaRec{CustomerID: "abc", ChargePeriod: "2026-03-17", NetCost: 1.5}
	written, err := store.Write(t.Context(), []metaRec{rec},
		s3pgstore.WithMetadata(map[string]any{
			"job_id":        "job-007",
			"tenant_id":     tenantID,
			"calculated_at": calcAt,
		}),
	)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify the catalog row carries the typed values.
	var (
		gotJob  string
		gotTen  uuid.UUID
		gotCalc time.Time
	)
	if err := fix.Pool.QueryRow(t.Context(),
		`SELECT ext_job_id, ext_tenant_id, ext_calculated_at `+
			`FROM "`+fix.Schema+`"."s3pgstore_files" `+
			`WHERE file_id = $1`,
		written[0].FileID,
	).Scan(&gotJob, &gotTen, &gotCalc); err != nil {
		t.Fatalf("read ext columns: %v", err)
	}
	if gotJob != "job-007" {
		t.Errorf("ext_job_id: %q", gotJob)
	}
	if gotTen != tenantID {
		t.Errorf("ext_tenant_id: %v", gotTen)
	}
	if !gotCalc.Equal(calcAt) {
		t.Errorf("ext_calculated_at: %v want %v", gotCalc, calcAt)
	}

	// Read returns the same metadata.
	got, err := store.Read(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
		s3pgstore.Eq("customer", "abc"),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("partitions: %d", len(got))
	}
	if len(got[0].FileExtensions) != 1 {
		t.Fatalf("FileExtensions count: %d", len(got[0].FileExtensions))
	}
	ext := got[0].FileExtensions[0].Extensions
	if ext["job_id"] != "job-007" {
		t.Errorf("read ext.job_id: %v", ext["job_id"])
	}
}

func TestWithMetadata_UnknownKeyRejected(t *testing.T) {
	store := newMetaStore(t, newFixture(t))
	rec := metaRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	_, err := store.Write(t.Context(), []metaRec{rec},
		s3pgstore.WithMetadata(map[string]any{"unknown_key": "x"}),
	)
	if err == nil {
		t.Fatal("unknown key: want error, got nil")
	}
	if !strings.Contains(err.Error(), "unknown") {
		t.Errorf("error: %v", err)
	}
}

func TestWithMetadata_TypeMismatchRejected(t *testing.T) {
	store := newMetaStore(t, newFixture(t))
	rec := metaRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	_, err := store.Write(t.Context(), []metaRec{rec},
		s3pgstore.WithMetadata(map[string]any{
			"job_id": 12345, // declared TEXT
		}),
	)
	if err == nil {
		t.Fatal("type mismatch: want error, got nil")
	}
}

func TestWithMetadata_PartialPopulation(t *testing.T) {
	store := newMetaStore(t, newFixture(t))
	rec := metaRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	if _, err := store.Write(t.Context(), []metaRec{rec},
		s3pgstore.WithMetadata(map[string]any{
			"job_id": "j1", // only one of three columns
		}),
	); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Read(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
		s3pgstore.Eq("customer", "abc"),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	ext := got[0].FileExtensions[0].Extensions
	if ext["job_id"] != "j1" {
		t.Errorf("job_id: %v", ext["job_id"])
	}
	// Unset columns are absent (NULL → not in map).
	if _, ok := ext["tenant_id"]; ok {
		t.Errorf("tenant_id present despite no metadata: %v",
			ext["tenant_id"])
	}
}

// TestWithMetadata_FirstWriteWinsUnderIdempotency: a retry
// with the same token but different metadata returns the
// existing row's metadata (the retry's metadata is silently
// discarded).
func TestWithMetadata_FirstWriteWinsUnderIdempotency(t *testing.T) {
	store := newMetaStore(t, newFixture(t))

	rec := metaRec{CustomerID: "abc", ChargePeriod: "2026-03-17"}
	first, err := store.Write(t.Context(), []metaRec{rec},
		s3pgstore.WithIdempotencyToken("first-write-wins"),
		s3pgstore.WithMetadata(map[string]any{"job_id": "first"}),
	)
	if err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	second, err := store.Write(t.Context(), []metaRec{rec},
		s3pgstore.WithIdempotencyToken("first-write-wins"),
		s3pgstore.WithMetadata(map[string]any{"job_id": "second"}),
	)
	if err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if first[0].FileID != second[0].FileID {
		t.Fatal("retry created a new row")
	}

	// The catalog row's metadata must still be the FIRST write's.
	got, err := store.Read(t.Context(), []s3pgstore.PartitionFilter{
		s3pgstore.Eq("charge_period", "2026-03-17"),
		s3pgstore.Eq("customer", "abc"),
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got[0].FileExtensions[0].Extensions["job_id"] != "first" {
		t.Errorf("first-write-wins violated: %v",
			got[0].FileExtensions[0].Extensions["job_id"])
	}
}
