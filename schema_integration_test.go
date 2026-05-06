//go:build integration

package s3pgstore_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/ueisele/s3pgstore"
)

type smokeRecord struct {
	CustomerID   string  `parquet:"customer_id"`
	ChargePeriod string  `parquet:"charge_period"`
	NetCost      float64 `parquet:"net_cost"`
}

func newSchemaCfg(f *fixture) s3pgstore.Config[smokeRecord] {
	return s3pgstore.Config[smokeRecord]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		Bucket:            f.Bucket,
		Prefix:            "billing",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"charge_period", "customer"},
		PartitionKeyOf: func(r smokeRecord) string {
			return "charge_period=" + r.ChargePeriod +
				"/customer=" + r.CustomerID
		},
	}
}

// TestSchemaManager_CreateValidateDrop covers the standard
// round-trip: a fresh schema starts empty (Validate fails),
// Create renders + applies the DDL, Validate succeeds, Drop
// removes everything, Validate fails again. Mirrors the path
// operators take for tests and small deployments.
func TestSchemaManager_CreateValidateDrop(t *testing.T) {
	f := newFixture(t)
	cfg := newSchemaCfg(f)
	mgr := s3pgstore.NewSchemaManager(cfg)

	// Pre-create: the schema validation must fail because no
	// tables exist yet.
	var sve *s3pgstore.SchemaValidationError
	if err := mgr.Validate(t.Context()); err == nil ||
		!errors.As(err, &sve) {
		t.Fatalf("Validate on empty schema: want SchemaValidationError, "+
			"got %v", err)
	}

	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Post-create: validation must pass.
	if err := mgr.Validate(t.Context()); err != nil {
		t.Fatalf("Validate after Create: %v", err)
	}

	// Create is idempotent.
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create (second call): %v", err)
	}
	if err := mgr.Validate(t.Context()); err != nil {
		t.Fatalf("Validate after second Create: %v", err)
	}

	// Drop removes everything; Validate fails again.
	if err := mgr.Drop(t.Context()); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if err := mgr.Validate(t.Context()); err == nil ||
		!errors.As(err, &sve) {
		t.Fatalf("Validate after Drop: want SchemaValidationError, got %v",
			err)
	}
}

// TestSchemaManager_WithExtensionsAndMVs covers a richer
// configuration: ExtensionColumns plus two MaterializedView
// shapes (key-only and key+value). Validate must accept both.
func TestSchemaManager_WithExtensionsAndMVs(t *testing.T) {
	f := newFixture(t)
	cfg := newSchemaCfg(f)
	cfg.ExtensionColumns = []s3pgstore.ExtensionColumn{
		{Name: "job_id", Type: "TEXT"},
		{Name: "tenant_id", Type: "UUID"},
		{Name: "calculated_at", Type: "TIMESTAMPTZ"},
	}
	cfg.MaterializedViews = []s3pgstore.MaterializedViewDef[smokeRecord]{
		{
			Name:       "by_customer",
			KeyColumns: []string{"customer_id"},
			Of: func(smokeRecord) ([]s3pgstore.MVRow, error) {
				return nil, nil
			},
		},
		{
			Name:         "by_period_customer",
			KeyColumns:   []string{"period", "customer_id"},
			ValueColumns: []string{"region"},
			Of: func(smokeRecord) ([]s3pgstore.MVRow, error) {
				return nil, nil
			},
		},
	}

	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })

	if err := mgr.Validate(t.Context()); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Manually drop one MV table and confirm Validate flags
	// it. Operates on the qualified table name to bypass the
	// SchemaManager's bookkeeping.
	if _, err := f.Pool.Exec(t.Context(),
		`DROP TABLE "`+f.Schema+`"."s3pgstore_mv_by_customer"`); err != nil {
		t.Fatalf("manual drop of MV table: %v", err)
	}
	var sve *s3pgstore.SchemaValidationError
	err := mgr.Validate(t.Context())
	if !errors.As(err, &sve) {
		t.Fatalf("Validate after manual drop: want SchemaValidationError, "+
			"got %v", err)
	}
	if len(sve.Missing) == 0 ||
		!strings.Contains(sve.Missing[0], "s3pgstore_mv_by_customer") {
		t.Fatalf("Validate.Missing should mention the dropped MV table; "+
			"got %v", sve.Missing)
	}
}

// TestSchemaManager_DetectsMissingColumn verifies that a
// schema mutated outside the SchemaManager (e.g., a partial
// migration) is flagged at Validate time.
func TestSchemaManager_DetectsMissingColumn(t *testing.T) {
	f := newFixture(t)
	cfg := newSchemaCfg(f)
	cfg.ExtensionColumns = []s3pgstore.ExtensionColumn{
		{Name: "job_id", Type: "TEXT"},
	}

	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })

	if _, err := f.Pool.Exec(t.Context(),
		`ALTER TABLE "`+f.Schema+`"."s3pgstore_files" `+
			`DROP COLUMN ext_job_id`); err != nil {
		t.Fatalf("drop ext column: %v", err)
	}

	var sve *s3pgstore.SchemaValidationError
	err := mgr.Validate(t.Context())
	if !errors.As(err, &sve) {
		t.Fatalf("Validate after drop column: want SchemaValidationError, "+
			"got %v", err)
	}
	if len(sve.Missing) == 0 ||
		!strings.Contains(sve.Missing[0], "ext_job_id") {
		t.Fatalf("Validate.Missing should mention ext_job_id; got %v",
			sve.Missing)
	}
}
