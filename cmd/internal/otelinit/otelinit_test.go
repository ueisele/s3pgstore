package otelinit

import (
	"context"
	"testing"
)

// TestSetup_NoEnv_NoOp verifies the opt-in behavior: with none
// of the OTel exporter env vars set, Setup returns a working
// no-op meter and a no-op shutdown that callers can defer
// unconditionally.
func TestSetup_NoEnv_NoOp(t *testing.T) {
	for _, k := range []string{
		"OTEL_METRICS_EXPORTER",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	} {
		t.Setenv(k, "")
	}

	meter, shutdown, err := Setup(context.Background(), "test-svc")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if meter == nil {
		t.Fatal("expected non-nil meter")
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown")
	}
	// Exercising the meter must not panic on the no-op path.
	c, err := meter.Int64Counter("test.counter")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	c.Add(context.Background(), 1)

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestSetup_ExporterNone_Enabled verifies that
// OTEL_METRICS_EXPORTER=none takes the enabled path (autoexport
// returns a no-op reader) and that shutdown completes cleanly.
// This also locks in that the enabled-path Resource construction
// doesn't fail under default env conditions.
func TestSetup_ExporterNone_Enabled(t *testing.T) {
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	meter, shutdown, err := Setup(context.Background(), "test-svc")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if meter == nil {
		t.Fatal("expected non-nil meter")
	}
	c, err := meter.Int64Counter("test.counter")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	c.Add(context.Background(), 1)

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestEnabled verifies the env-gate predicate matches the doc:
// any of the three exporter-config env vars enables, none
// disables. Locks the contract so a refactor doesn't silently
// drop the enable check.
func TestEnabled(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{
			name: "no env",
			env:  map[string]string{},
			want: false,
		},
		{
			name: "OTEL_METRICS_EXPORTER set",
			env:  map[string]string{"OTEL_METRICS_EXPORTER": "otlp"},
			want: true,
		},
		{
			name: "OTEL_EXPORTER_OTLP_ENDPOINT set",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://x:4317"},
			want: true,
		},
		{
			name: "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT set",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://x:4318/v1/metrics"},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear all three; t.Setenv restores on cleanup.
			for _, k := range []string{
				"OTEL_METRICS_EXPORTER",
				"OTEL_EXPORTER_OTLP_ENDPOINT",
				"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
			} {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := enabled(); got != tc.want {
				t.Errorf("enabled() = %v, want %v", got, tc.want)
			}
		})
	}
}
