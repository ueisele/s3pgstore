package s3pgstore

import (
	"slices"
	"testing"
)

func TestPartitionKeyValues_OK(t *testing.T) {
	got, err := partitionKeyValues(
		"charge_period=2026-03-17/customer=abc",
		[]string{"charge_period", "customer"},
	)
	if err != nil {
		t.Fatalf("partitionKeyValues: %v", err)
	}
	want := []string{"2026-03-17", "abc"}
	if !slices.Equal(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestPartitionKeyValues_SingleSegment(t *testing.T) {
	got, err := partitionKeyValues("a=1", []string{"a"})
	if err != nil {
		t.Fatalf("partitionKeyValues: %v", err)
	}
	if !slices.Equal(got, []string{"1"}) {
		t.Fatalf("got %v", got)
	}
}

func TestPartitionKeyValues_AllowsEqualsInValue(t *testing.T) {
	got, err := partitionKeyValues(
		"period=2026-03/raw=base64==",
		[]string{"period", "raw"},
	)
	if err != nil {
		t.Fatalf("partitionKeyValues: %v", err)
	}
	if got[1] != "base64==" {
		t.Fatalf("equals-in-value lost: %v", got)
	}
}

func TestPartitionKeyValues_Errors(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		parts []string
	}{
		{"empty key", "", []string{"a"}},
		{"wrong segment count",
			"a=1/b=2", []string{"a"}},
		{"missing equals", "a", []string{"a"}},
		{"name mismatch", "x=1/b=2", []string{"a", "b"}},
		{"empty value", "a=", []string{"a"}},
		{"leading equals", "=v", []string{"a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := partitionKeyValues(tc.key, tc.parts); err == nil {
				t.Fatalf("want error, got nil for %q", tc.key)
			}
		})
	}
}
