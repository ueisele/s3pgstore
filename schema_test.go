package s3pgstore

import (
	"strings"
	"testing"
)

func TestRenderDDL_RejectsInvalidConfig(t *testing.T) {
	if _, err := RenderDDL(Config[map[string]any]{}); err == nil {
		t.Fatal("RenderDDL on empty Config: want error, got nil")
	}
}

func TestRenderDDL_DefaultPrefix(t *testing.T) {
	cfg := validConfig()
	out, err := RenderDDL(cfg)
	if err != nil {
		t.Fatalf("RenderDDL: %v", err)
	}
	mustContainStr(t, out, `"public"."s3pgstore_files"`)
	mustContainStr(t, out, `"public"."s3pgstore_partitions"`)
	mustContainStr(t, out, `"public"."s3pgstore_pending_writes"`)
}

func TestRenderDDL_CustomPrefix(t *testing.T) {
	cfg := validConfig()
	cfg.SchemaName = "billing"
	cfg.TablePrefix = "cost_"
	out, err := RenderDDL(cfg)
	if err != nil {
		t.Fatalf("RenderDDL: %v", err)
	}
	mustContainStr(t, out, `"billing"."cost_files"`)
	mustContainStr(t, out, `"billing"."cost_partitions"`)
	if strings.Contains(out, "s3pgstore_") {
		t.Fatal("custom prefix output contains default s3pgstore_ prefix")
	}
}

func TestRenderDDL_PartAndExtColumns(t *testing.T) {
	cfg := validConfig()
	cfg.ExtensionColumns = []ExtensionColumn{
		{Name: "job_id", Type: "TEXT"},
		{Name: "tenant_id", Type: "UUID"},
	}
	out, err := RenderDDL(cfg)
	if err != nil {
		t.Fatalf("RenderDDL: %v", err)
	}
	// part_<n> derived from PartitionKeyParts in validConfig().
	mustContainStr(t, out, "part_period")
	mustContainStr(t, out, "part_customer")
	mustContainStr(t, out, "ext_job_id")
	mustContainStr(t, out, "ext_tenant_id")
}

func TestRenderDDL_MaterializedViews(t *testing.T) {
	cfg := validConfig()
	cfg.MaterializedViews = []MaterializedViewDef[map[string]any]{
		{
			Name:       "by_sku",
			KeyColumns: []string{"sku_id"},
			Of:         func(map[string]any) ([]MVRow, error) { return nil, nil },
		},
	}
	out, err := RenderDDL(cfg)
	if err != nil {
		t.Fatalf("RenderDDL: %v", err)
	}
	mustContainStr(t, out, `"public"."s3pgstore_mv_by_sku"`)
}

func mustContainStr(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("output missing %q\n--- output ---\n%s", needle, haystack)
	}
}
