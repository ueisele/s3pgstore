package s3pgstore

import (
	"strings"
	"testing"
)

func translateOrFail(t *testing.T, parts []string, f PartitionFilter) (string, []any) {
	t.Helper()
	sql, args, err := translateFilters(
		[]PartitionFilter{f}, partColResolver(parts))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	return sql, args
}

func TestFilter_Eq(t *testing.T) {
	parts := []string{"period", "customer"}
	sql, args := translateOrFail(t, parts, Eq("period", "2026-03"))
	if sql != "part_period = $1" {
		t.Errorf("sql: %q", sql)
	}
	if len(args) != 1 || args[0] != "2026-03" {
		t.Errorf("args: %v", args)
	}
}

func TestFilter_Prefix(t *testing.T) {
	parts := []string{"period"}
	sql, args := translateOrFail(t, parts, Prefix("period", "2026-03-"))
	if sql != "part_period LIKE $1" {
		t.Errorf("sql: %q", sql)
	}
	if args[0] != "2026-03-%" {
		t.Errorf("prefix wildcard: %v", args[0])
	}
}

func TestFilter_Between(t *testing.T) {
	parts := []string{"period"}
	sql, args := translateOrFail(t, parts,
		Between("period", "2026-03-01", "2026-04-01"))
	if !strings.Contains(sql, "part_period >= $1 AND part_period < $2") {
		t.Errorf("sql: %q", sql)
	}
	if args[0] != "2026-03-01" || args[1] != "2026-04-01" {
		t.Errorf("args: %v", args)
	}
}

func TestFilter_BetweenInvalidOrder(t *testing.T) {
	parts := []string{"period"}
	_, _, err := translateFilters(
		[]PartitionFilter{Between("period", "z", "a")},
		partColResolver(parts))
	if err == nil {
		t.Fatal("Between with from>=to: want error")
	}
}

func TestFilter_GE_LT(t *testing.T) {
	parts := []string{"period"}
	sql, _ := translateOrFail(t, parts, GE("period", "2026-03-01"))
	if sql != "part_period >= $1" {
		t.Errorf("GE sql: %q", sql)
	}
	sql, _ = translateOrFail(t, parts, LT("period", "2026-04-01"))
	if sql != "part_period < $1" {
		t.Errorf("LT sql: %q", sql)
	}
}

func TestFilter_In(t *testing.T) {
	parts := []string{"customer"}
	sql, args := translateOrFail(t, parts,
		In("customer", "a", "b", "c"))
	if sql != "part_customer IN ($1, $2, $3)" {
		t.Errorf("sql: %q", sql)
	}
	if len(args) != 3 {
		t.Errorf("args: %v", args)
	}
}

func TestFilter_InEmpty(t *testing.T) {
	parts := []string{"customer"}
	sql, args := translateOrFail(t, parts, In("customer"))
	if sql != "FALSE" {
		t.Errorf("empty In: %q", sql)
	}
	if len(args) != 0 {
		t.Errorf("args: %v", args)
	}
}

func TestFilter_AndOr(t *testing.T) {
	parts := []string{"period", "customer"}
	sql, args := translateOrFail(t, parts,
		Or(
			And(
				Eq("period", "2026-03-01"),
				Eq("customer", "abc"),
			),
			And(
				Eq("period", "2026-03-02"),
				Eq("customer", "def"),
			),
		),
	)
	if !strings.Contains(sql, "OR") || !strings.Contains(sql, "AND") {
		t.Errorf("sql: %q", sql)
	}
	if len(args) != 4 {
		t.Errorf("args: %v", args)
	}
}

func TestFilter_TopLevelAndJoinsMultipleFilters(t *testing.T) {
	parts := []string{"period", "customer"}
	sql, args, err := translateFilters([]PartitionFilter{
		Eq("period", "2026-03"),
		Eq("customer", "abc"),
	}, partColResolver(parts))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(sql, " AND ") {
		t.Errorf("two-filter slice should join with AND: %q", sql)
	}
	if len(args) != 2 {
		t.Errorf("args: %v", args)
	}
}

func TestFilter_UnknownPart(t *testing.T) {
	parts := []string{"period"}
	_, _, err := translateFilters(
		[]PartitionFilter{Eq("not_declared", "x")},
		partColResolver(parts))
	if err == nil {
		t.Fatal("unknown part: want error")
	}
}

func TestFilter_EmptyAndOr(t *testing.T) {
	parts := []string{"period"}
	sql, _ := translateOrFail(t, parts, And())
	if sql != "TRUE" {
		t.Errorf("empty And: %q", sql)
	}
	sql, _ = translateOrFail(t, parts, Or())
	if sql != "FALSE" {
		t.Errorf("empty Or: %q", sql)
	}
}

func TestFilter_DeterministicOutput(t *testing.T) {
	parts := []string{"period", "customer"}
	f := Or(
		And(
			Eq("period", "2026-03-01"),
			Eq("customer", "a"),
		),
		Eq("customer", "b"),
	)
	a, _, _ := translateFilters([]PartitionFilter{f}, partColResolver(parts))
	b, _, _ := translateFilters([]PartitionFilter{f}, partColResolver(parts))
	if a != b {
		t.Fatalf("non-deterministic translation: %q vs %q", a, b)
	}
}

func TestTranslateFilters_Empty(t *testing.T) {
	sql, args, err := translateFilters(nil,
		partColResolver([]string{"period"}))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if sql != "" || args != nil {
		t.Fatalf("empty filters: sql=%q args=%v", sql, args)
	}
}
