package s3pgstore

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestValidateMetadata_Empty(t *testing.T) {
	if err := validateMetadata(nil, nil); err != nil {
		t.Errorf("nil m, nil cols: %v", err)
	}
	if err := validateMetadata(map[string]any{}, nil); err != nil {
		t.Errorf("empty m, nil cols: %v", err)
	}
}

func TestValidateMetadata_TypeMatches(t *testing.T) {
	cols := []ExtensionColumn{
		{Name: "job_id", Type: "TEXT"},
		{Name: "tenant_id", Type: "UUID"},
		{Name: "calculated_at", Type: "TIMESTAMPTZ"},
		{Name: "shard", Type: "INT"},
		{Name: "total", Type: "BIGINT"},
		{Name: "active", Type: "BOOLEAN"},
		{Name: "amount", Type: "NUMERIC"},
	}
	uuidVal := uuid.New()
	good := map[string]any{
		"job_id":        "abc",
		"tenant_id":     uuidVal,
		"calculated_at": time.Now(),
		"shard":         5,
		"total":         int64(100),
		"active":        true,
		"amount":        "1.234",
	}
	if err := validateMetadata(good, cols); err != nil {
		t.Fatalf("good metadata rejected: %v", err)
	}
}

func TestValidateMetadata_UUIDStringForm(t *testing.T) {
	cols := []ExtensionColumn{{Name: "u", Type: "UUID"}}
	if err := validateMetadata(
		map[string]any{"u": uuid.New().String()}, cols); err != nil {
		t.Errorf("UUID-as-string: %v", err)
	}
}

func TestValidateMetadata_UnknownKey(t *testing.T) {
	cols := []ExtensionColumn{{Name: "job_id", Type: "TEXT"}}
	err := validateMetadata(map[string]any{"unknown": "x"}, cols)
	if err == nil {
		t.Fatal("unknown key: want error")
	}
	if !strings.Contains(err.Error(), "unknown key") {
		t.Errorf("error message: %v", err)
	}
}

func TestValidateMetadata_TypeMismatch(t *testing.T) {
	cases := []struct {
		name string
		col  ExtensionColumn
		val  any
	}{
		{"text expects string",
			ExtensionColumn{Name: "x", Type: "TEXT"}, 5},
		{"uuid expects uuid",
			ExtensionColumn{Name: "x", Type: "UUID"}, 5},
		{"timestamptz expects time.Time",
			ExtensionColumn{Name: "x", Type: "TIMESTAMPTZ"}, "x"},
		{"int expects int",
			ExtensionColumn{Name: "x", Type: "INT"}, "5"},
		{"bool expects bool",
			ExtensionColumn{Name: "x", Type: "BOOLEAN"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMetadata(map[string]any{"x": tc.val},
				[]ExtensionColumn{tc.col})
			if err == nil {
				t.Fatal("want error, got nil")
			}
		})
	}
}

func TestValidateMetadata_NilValueAllowed(t *testing.T) {
	cols := []ExtensionColumn{{Name: "x", Type: "TEXT"}}
	if err := validateMetadata(map[string]any{"x": nil}, cols); err != nil {
		t.Errorf("nil value: %v", err)
	}
}

func TestMetadataValueFor_MissingKeyReturnsNil(t *testing.T) {
	c := ExtensionColumn{Name: "x", Type: "TEXT"}
	if got := metadataValueFor(map[string]any{}, c); got != nil {
		t.Errorf("missing key: want nil, got %v", got)
	}
	if got := metadataValueFor(nil, c); got != nil {
		t.Errorf("nil map: want nil, got %v", got)
	}
}

func TestMetadataValueFor_PresentKey(t *testing.T) {
	c := ExtensionColumn{Name: "x", Type: "TEXT"}
	if got := metadataValueFor(map[string]any{"x": "v"}, c); got != "v" {
		t.Errorf("present key: got %v", got)
	}
}
