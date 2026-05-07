package catalog

import (
	"strings"
	"testing"
)

func TestNames_Substitution(t *testing.T) {
	n := NewNames("public", "s3pgstore_")
	cases := map[string]string{
		`"public"."s3pgstore_files"`:          n.Files(),
		`"public"."s3pgstore_partitions"`:     n.Partitions(),
		`"public"."s3pgstore_pending_writes"`: n.PendingWrites(),
		`"public"."s3pgstore_mv_sku"`:         n.MV("sku"),
	}
	for want, got := range cases {
		if got != want {
			t.Errorf("want %s, got %s", want, got)
		}
	}
}

func TestNames_CustomPrefix(t *testing.T) {
	n := NewNames("billing", "cost_")
	if got, want := n.Files(), `"billing"."cost_files"`; got != want {
		t.Errorf("custom prefix: want %q, got %q", want, got)
	}
}

func TestRenderAll_BasicRoundTrip(t *testing.T) {
	in := DDLInput{
		Names: NewNames("public", "s3pgstore_"),
		Parts: []DDLPart{{Name: "period"}, {Name: "customer"}},
	}
	out, err := RenderAll(in)
	if err != nil {
		t.Fatalf("RenderAll: %v", err)
	}

	mustContain(t, out, `CREATE TABLE IF NOT EXISTS "public"."s3pgstore_files"`)
	mustContain(t, out, `CREATE TABLE IF NOT EXISTS "public"."s3pgstore_partitions"`)
	mustContain(t, out, `CREATE TABLE IF NOT EXISTS "public"."s3pgstore_pending_writes"`)
	mustContain(t, out, "part_period            TEXT NOT NULL")
	mustContain(t, out, "part_customer            TEXT NOT NULL")
	mustContain(t, out, "feed_seq             BIGINT UNIQUE")
	mustContain(t, out, `s3pgstore_files_token_idx`)
	mustContain(t, out, `WHERE idempotency_token IS NOT NULL`)
}

func TestRenderAll_ExtensionColumns(t *testing.T) {
	in := DDLInput{
		Names: NewNames("public", "s3pgstore_"),
		Parts: []DDLPart{{Name: "period"}},
		Exts: []DDLExt{
			{Name: "job_id", Type: "TEXT"},
			{Name: "tenant_id", Type: "uuid"}, // case folded to UUID
		},
	}
	out, err := RenderAll(in)
	if err != nil {
		t.Fatalf("RenderAll: %v", err)
	}

	mustContain(t, out, "ext_job_id         TEXT")
	mustContain(t, out, "ext_tenant_id         UUID")
}

func TestRenderAll_MVSingleColumn(t *testing.T) {
	in := DDLInput{
		Names: NewNames("public", "s3pgstore_"),
		Parts: []DDLPart{{Name: "period"}},
		MVs: []DDLMV{
			{Name: "by_sku", Columns: []string{"sku_id"}},
		},
	}
	out, err := RenderAll(in)
	if err != nil {
		t.Fatalf("RenderAll: %v", err)
	}
	mustContain(t, out,
		`CREATE TABLE IF NOT EXISTS "public"."s3pgstore_mv_by_sku"`)
	mustContain(t, out, `"sku_id" TEXT NOT NULL,`)
	mustContain(t, out, `PRIMARY KEY ("sku_id")`)
}

func TestRenderAll_MVMultiColumn(t *testing.T) {
	in := DDLInput{
		Names: NewNames("public", "s3pgstore_"),
		Parts: []DDLPart{{Name: "period"}},
		MVs: []DDLMV{
			{
				Name:    "sku_period_region",
				Columns: []string{"sku_id", "period_start", "region"},
			},
		},
	}
	out, err := RenderAll(in)
	if err != nil {
		t.Fatalf("RenderAll: %v", err)
	}
	mustContain(t, out,
		`CREATE TABLE IF NOT EXISTS "public"."s3pgstore_mv_sku_period_region"`)
	mustContain(t, out, `"sku_id" TEXT NOT NULL,`)
	mustContain(t, out, `"period_start" TEXT NOT NULL,`)
	mustContain(t, out, `"region" TEXT NOT NULL,`)
	mustContain(t, out,
		`PRIMARY KEY ("sku_id", "period_start", "region")`)
}

func TestRenderAll_DeterministicOutput(t *testing.T) {
	in := DDLInput{
		Names: NewNames("public", "s3pgstore_"),
		Parts: []DDLPart{{Name: "period"}, {Name: "customer"}},
		Exts: []DDLExt{
			{Name: "job_id", Type: "TEXT"},
			{Name: "tenant_id", Type: "UUID"},
		},
		MVs: []DDLMV{
			{Name: "v1", Columns: []string{"a"}},
			{Name: "v2", Columns: []string{"b", "c"}},
		},
	}
	a, err := RenderAll(in)
	if err != nil {
		t.Fatalf("RenderAll a: %v", err)
	}
	b, err := RenderAll(in)
	if err != nil {
		t.Fatalf("RenderAll b: %v", err)
	}
	if a != b {
		t.Fatal("RenderAll output is not byte-identical across calls")
	}
}

func TestRenderAll_MultiplePrefixes(t *testing.T) {
	one, err := RenderAll(DDLInput{
		Names: NewNames("public", "billing_"),
		Parts: []DDLPart{{Name: "period"}},
	})
	if err != nil {
		t.Fatalf("RenderAll billing_: %v", err)
	}
	two, err := RenderAll(DDLInput{
		Names: NewNames("public", "events_"),
		Parts: []DDLPart{{Name: "period"}},
	})
	if err != nil {
		t.Fatalf("RenderAll events_: %v", err)
	}
	mustContain(t, one, `"public"."billing_files"`)
	mustContain(t, one, `billing_files_token_idx`)
	mustContain(t, two, `"public"."events_files"`)
	mustContain(t, two, `events_files_token_idx`)
	if strings.Contains(one, "events_") || strings.Contains(two, "billing_") {
		t.Fatal("prefixes leaked across renders")
	}
}

func TestRenderAll_RejectsEmpty(t *testing.T) {
	if _, err := RenderAll(DDLInput{}); err == nil {
		t.Fatal("RenderAll empty: want error, got nil")
	}
	if _, err := RenderAll(DDLInput{
		Names: NewNames("public", "s3pgstore_"),
	}); err == nil {
		t.Fatal("RenderAll missing parts: want error, got nil")
	}
}

func mustContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("output missing %q\n--- output ---\n%s", needle, haystack)
	}
}
