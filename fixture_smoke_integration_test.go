//go:build integration

package s3pgstore_test

import (
	"testing"
)

// TestFixtureBoots is the smoke test for newFixture — confirms
// the shared PG + MinIO containers come up, a per-test schema
// and bucket are created, and the cleanup runs without error.
func TestFixtureBoots(t *testing.T) {
	f := newFixture(t)
	if f.Pool == nil || f.S3Client == nil {
		t.Fatal("fixture missing Pool or S3Client")
	}
	if f.Schema == "" || f.Bucket == "" {
		t.Fatalf("fixture missing schema/bucket: schema=%q bucket=%q",
			f.Schema, f.Bucket)
	}
	var got int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT 1`).Scan(&got); err != nil {
		t.Fatalf("PG ping: %v", err)
	}
	if got != 1 {
		t.Fatalf("PG ping: want 1, got %d", got)
	}
}
