package s3pgstore

import (
	"slices"
	"testing"
)

type dedupRec struct {
	Entity  string
	Version int64
	// Tag distinguishes physical records that share
	// (entity, version). Tests use it to verify which physical
	// row survives ties.
	Tag string
}

func entOf(r dedupRec) string { return r.Entity }
func verOf(r dedupRec) int64  { return r.Version }

func TestSortAndDedup_Disabled(t *testing.T) {
	in := []dedupRec{
		{Entity: "b", Version: 1},
		{Entity: "a", Version: 2},
	}
	out := sortAndDedup(in, nil, nil, false)
	if !slices.Equal(out, in) {
		t.Fatalf("dedup disabled: input mutated")
	}
}

func TestSortAndDedup_LatestPerEntity(t *testing.T) {
	in := []dedupRec{
		{Entity: "a", Version: 1, Tag: "a1"},
		{Entity: "a", Version: 2, Tag: "a2"},
		{Entity: "b", Version: 5, Tag: "b5"},
		{Entity: "b", Version: 3, Tag: "b3"},
		{Entity: "a", Version: 3, Tag: "a3-latest"},
	}
	out := sortAndDedup(in, entOf, verOf, false)
	if len(out) != 2 {
		t.Fatalf("len: want 2, got %d", len(out))
	}
	// Output is sorted by entity ascending; within entity, the
	// latest-version record wins.
	if out[0].Entity != "a" || out[0].Version != 3 ||
		out[0].Tag != "a3-latest" {
		t.Errorf("a entry: %+v", out[0])
	}
	if out[1].Entity != "b" || out[1].Version != 5 ||
		out[1].Tag != "b5" {
		t.Errorf("b entry: %+v", out[1])
	}
}

func TestSortAndDedup_TieBreakLastWins(t *testing.T) {
	in := []dedupRec{
		{Entity: "x", Version: 1, Tag: "first"},
		{Entity: "x", Version: 1, Tag: "second"},
		{Entity: "x", Version: 1, Tag: "third"},
	}
	out := sortAndDedup(in, entOf, verOf, false)
	if len(out) != 1 {
		t.Fatalf("len: want 1, got %d", len(out))
	}
	if out[0].Tag != "third" {
		t.Errorf("tie-break: want last (Tag=third), got %q", out[0].Tag)
	}
}

func TestSortAndDedup_WithHistory(t *testing.T) {
	in := []dedupRec{
		{Entity: "a", Version: 1, Tag: "a1-replica1"},
		{Entity: "a", Version: 1, Tag: "a1-replica2"}, // duplicate
		{Entity: "a", Version: 2, Tag: "a2"},
		{Entity: "b", Version: 1, Tag: "b1"},
		{Entity: "b", Version: 1, Tag: "b1-replica"}, // duplicate
	}
	out := sortAndDedup(in, entOf, verOf, true)
	if len(out) != 3 {
		t.Fatalf("len: want 3, got %d", len(out))
	}
	// Replicas collapse but every distinct (entity, version)
	// survives.
	got := make(map[string]bool)
	for _, r := range out {
		got[r.Entity+"|"+itoa(r.Version)] = true
	}
	for _, k := range []string{"a|1", "a|2", "b|1"} {
		if !got[k] {
			t.Errorf("missing %s", k)
		}
	}
}

func TestSortAndDedup_Empty(t *testing.T) {
	if got := sortAndDedup[dedupRec](nil, entOf, verOf, false); got != nil {
		t.Fatalf("nil input: got %v", got)
	}
	in := []dedupRec{}
	if got := sortAndDedup(in, entOf, verOf, false); len(got) != 0 {
		t.Fatalf("empty input: got %v", got)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 20)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}
