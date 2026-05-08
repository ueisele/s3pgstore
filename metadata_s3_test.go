package s3pgstore

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestBuildS3Metadata_LibraryFields verifies the four
// always-present library fields land under the documented keys
// with deterministic encodings.
func TestBuildS3Metadata_LibraryFields(t *testing.T) {
	out, err := BuildS3Metadata("tok-123", 42, 999_999, 7, nil, nil)
	if err != nil {
		t.Fatalf("BuildS3Metadata: %v", err)
	}
	wants := map[string]string{
		S3MetaToken:            "tok-123",
		S3MetaRecordCount:      "42",
		S3MetaUncompressedSize: "999999",
		S3MetaWrittenAtVersion: "7",
	}
	for k, want := range wants {
		got, ok := out[k]
		if !ok {
			t.Errorf("missing key %q", k)
			continue
		}
		if got != want {
			t.Errorf("%s: want %q, got %q", k, want, got)
		}
	}
	if len(out) != 4 {
		t.Errorf("unexpected extra keys: %v", out)
	}
}

// TestBuildS3Metadata_TokenOmittedWhenEmpty: empty token must
// not appear in the bag (NULL idempotency_token in the catalog
// matches absence here).
func TestBuildS3Metadata_TokenOmittedWhenEmpty(t *testing.T) {
	out, err := BuildS3Metadata("", 1, 1, 1, nil, nil)
	if err != nil {
		t.Fatalf("BuildS3Metadata: %v", err)
	}
	if _, present := out[S3MetaToken]; present {
		t.Errorf("S3MetaToken set despite empty token: %v", out)
	}
}

// TestBuildS3Metadata_ExtRoundTrip exercises every ExtensionColumn
// SQL type at the format → parse boundary so an evolution of
// either function would fail loudly here.
func TestBuildS3Metadata_ExtRoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 34, 56, 789000000, time.UTC)
	uid := uuid.MustParse("11111111-2222-3333-4444-555555555555")

	exts := []ExtensionColumn{
		{Name: "tenant_id", Type: "TEXT"},
		{Name: "owner_uuid", Type: "UUID"},
		{Name: "created_at", Type: "TIMESTAMPTZ"},
		{Name: "shard", Type: "INT"},
		{Name: "rows", Type: "BIGINT"},
		{Name: "active", Type: "BOOLEAN"},
		{Name: "score", Type: "NUMERIC"},
	}
	values := map[string]any{
		"tenant_id":  "acme",
		"owner_uuid": uid,
		"created_at": now,
		"shard":      int(3),
		"rows":       int64(1_000_000),
		"active":     true,
		"score":      "1.234567890",
	}

	out, err := BuildS3Metadata("tok", 1, 1, 1, exts, values)
	if err != nil {
		t.Fatalf("BuildS3Metadata: %v", err)
	}

	cases := []struct {
		col  string
		want any
	}{
		{"tenant_id", "acme"},
		{"owner_uuid", uid.String()}, // ParseExtFromS3 returns canonical string
		{"created_at", now},
		{"shard", int64(3)},
		{"rows", int64(1_000_000)},
		{"active", true},
		{"score", "1.234567890"},
	}
	for _, tc := range cases {
		raw, ok := out[S3MetaExtPrefix+tc.col]
		if !ok {
			t.Errorf("ext %q: not present in metadata", tc.col)
			continue
		}
		var col ExtensionColumn
		for _, c := range exts {
			if c.Name == tc.col {
				col = c
				break
			}
		}
		got, err := ParseExtFromS3(col, raw)
		if err != nil {
			t.Errorf("ParseExtFromS3 %q: %v", tc.col, err)
			continue
		}
		if !equalParsedExt(got, tc.want) {
			t.Errorf("ext %q round-trip mismatch: got %v (%T), want %v (%T)",
				tc.col, got, got, tc.want, tc.want)
		}
	}
}

func equalParsedExt(a, b any) bool {
	if ta, ok := a.(time.Time); ok {
		tb, ok2 := b.(time.Time)
		return ok2 && ta.Equal(tb)
	}
	return a == b
}

// TestBuildS3Metadata_NilExtValueOmitted: a declared column
// without a corresponding entry in WithMetadata must not appear
// in the bag — mirrors the SQL NULL semantics.
func TestBuildS3Metadata_NilExtValueOmitted(t *testing.T) {
	exts := []ExtensionColumn{
		{Name: "tenant_id", Type: "TEXT"},
	}
	out, err := BuildS3Metadata("", 1, 1, 1, exts, nil)
	if err != nil {
		t.Fatalf("BuildS3Metadata: %v", err)
	}
	if _, present := out[S3MetaExtPrefix+"tenant_id"]; present {
		t.Errorf("ext appeared despite nil value: %v", out)
	}
}

// TestBuildS3Metadata_RejectsOversize: assembling a bag larger
// than the AWS 2 KiB cap must error with ErrS3MetadataTooLarge,
// not produce an oversized metadata map.
func TestBuildS3Metadata_RejectsOversize(t *testing.T) {
	exts := []ExtensionColumn{
		{Name: "huge", Type: "TEXT"},
	}
	values := map[string]any{
		"huge": strings.Repeat("a", S3MetadataMaxBytes+10),
	}
	_, err := BuildS3Metadata("", 1, 1, 1, exts, values)
	if err == nil {
		t.Fatal("BuildS3Metadata: want oversize error, got nil")
	}
	if !errors.Is(err, ErrS3MetadataTooLarge) {
		t.Errorf("want errors.Is(err, ErrS3MetadataTooLarge), got %v",
			err)
	}
}

// TestBuildS3Metadata_RejectsNonHeaderSafeBytes: ext column
// value containing CR/LF/control chars must be rejected at
// write time so the user sees a clear error rather than a
// surprise 400 from S3.
func TestBuildS3Metadata_RejectsNonHeaderSafeBytes(t *testing.T) {
	exts := []ExtensionColumn{
		{Name: "tenant", Type: "TEXT"},
	}
	values := map[string]any{
		"tenant": "acme\r\ninjected: header",
	}
	_, err := BuildS3Metadata("", 1, 1, 1, exts, values)
	if err == nil {
		t.Fatal("BuildS3Metadata: want header-validation error, got nil")
	}
	if !strings.Contains(err.Error(), "non-ASCII") &&
		!strings.Contains(err.Error(), "control") {
		t.Errorf("error should mention header-safety: %v", err)
	}
}

// TestParseExtFromS3_InvalidValuesError sanity-checks the
// parser rejects malformed inputs for typed columns.
func TestParseExtFromS3_InvalidValuesError(t *testing.T) {
	cases := []struct {
		col ExtensionColumn
		raw string
	}{
		{ExtensionColumn{Name: "u", Type: "UUID"}, "not-a-uuid"},
		{ExtensionColumn{Name: "t", Type: "TIMESTAMPTZ"}, "yesterday"},
		{ExtensionColumn{Name: "n", Type: "BIGINT"}, "abc"},
		{ExtensionColumn{Name: "b", Type: "BOOLEAN"}, "yes"},
	}
	for _, tc := range cases {
		t.Run(tc.col.Name, func(t *testing.T) {
			if _, err := ParseExtFromS3(tc.col, tc.raw); err == nil {
				t.Errorf("ParseExtFromS3(%q,%q): want error",
					tc.col.Type, tc.raw)
			}
		})
	}
}
