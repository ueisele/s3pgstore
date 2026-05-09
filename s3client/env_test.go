package s3client

import (
	"testing"
)

// TestOptionsFromEnv_DefaultPrefix verifies an empty prefix
// resolves to "S3PGSTORE" and the standard env vars are read.
func TestOptionsFromEnv_DefaultPrefix(t *testing.T) {
	t.Setenv("S3PGSTORE_S3_MAX_OPEN_CONNECTIONS", "200")
	t.Setenv("S3PGSTORE_S3_MAX_RETRY_ATTEMPTS", "3")
	t.Setenv("S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND", "250")
	t.Setenv("S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND_BURST", "25")
	t.Setenv("S3PGSTORE_S3_USE_PATH_STYLE", "true")

	opts, err := OptionsFromEnv("")
	if err != nil {
		t.Fatalf("OptionsFromEnv: %v", err)
	}
	if opts.MaxOpenConnections != 200 {
		t.Errorf("MaxOpenConnections: want 200, got %d",
			opts.MaxOpenConnections)
	}
	if opts.MaxRetryAttempts != 3 {
		t.Errorf("MaxRetryAttempts: want 3, got %d",
			opts.MaxRetryAttempts)
	}
	if opts.MaxRequestsPerSecond != 250 {
		t.Errorf("MaxRequestsPerSecond: want 250, got %g",
			opts.MaxRequestsPerSecond)
	}
	if opts.MaxRequestsPerSecondBurst != 25 {
		t.Errorf("MaxRequestsPerSecondBurst: want 25, got %d",
			opts.MaxRequestsPerSecondBurst)
	}
	if !opts.UsePathStyle {
		t.Error("UsePathStyle: want true, got false")
	}
}

// TestOptionsFromEnv_CustomPrefix verifies a non-empty prefix
// is upper-cased and used to compose env-var lookups. Confirms
// the per-Store-config use case ("PRIMARY_S3_*" vs "ARCHIVE_S3_*").
func TestOptionsFromEnv_CustomPrefix(t *testing.T) {
	// Lowercase input — must be upper-cased before compose.
	t.Setenv("PRIMARY_S3_MAX_OPEN_CONNECTIONS", "100")
	// Verify scoping: the default-prefix var is unset, so a
	// stray DefaultEnvPrefix read would yield 0.
	t.Setenv("S3PGSTORE_S3_MAX_OPEN_CONNECTIONS", "")

	opts, err := OptionsFromEnv("primary")
	if err != nil {
		t.Fatalf("OptionsFromEnv: %v", err)
	}
	if opts.MaxOpenConnections != 100 {
		t.Errorf("custom-prefix lookup failed: want 100, got %d",
			opts.MaxOpenConnections)
	}
}

// TestOptionsFromEnv_AWSMaxAttemptsFallback verifies
// AWS_MAX_ATTEMPTS is honoured when the s3pgstore-namespaced
// var is unset. Important for callers wiring AWS-standard env
// vars via shared k8s base configs / IRSA pod templates.
func TestOptionsFromEnv_AWSMaxAttemptsFallback(t *testing.T) {
	t.Setenv("S3PGSTORE_S3_MAX_RETRY_ATTEMPTS", "")
	t.Setenv("AWS_MAX_ATTEMPTS", "7")

	opts, err := OptionsFromEnv("")
	if err != nil {
		t.Fatalf("OptionsFromEnv: %v", err)
	}
	if opts.MaxRetryAttempts != 7 {
		t.Errorf("AWS_MAX_ATTEMPTS fallback failed: want 7, got %d",
			opts.MaxRetryAttempts)
	}
}

// TestOptionsFromEnv_PrefixedAttemptsWinsOverAWS verifies the
// s3pgstore-namespaced var takes precedence over AWS_MAX_ATTEMPTS
// when both are set. Otherwise per-Store overrides would silently
// inherit from a shared AWS_MAX_ATTEMPTS the operator forgot
// about.
func TestOptionsFromEnv_PrefixedAttemptsWinsOverAWS(t *testing.T) {
	t.Setenv("S3PGSTORE_S3_MAX_RETRY_ATTEMPTS", "4")
	t.Setenv("AWS_MAX_ATTEMPTS", "9")

	opts, err := OptionsFromEnv("")
	if err != nil {
		t.Fatalf("OptionsFromEnv: %v", err)
	}
	if opts.MaxRetryAttempts != 4 {
		t.Errorf("prefix should win over AWS: want 4, got %d",
			opts.MaxRetryAttempts)
	}
}

// TestOptionsFromEnv_AllUnsetReturnsZeros verifies the empty
// case: every env var unset yields a zero-valued Options
// struct, which WithDefaults will resolve to library defaults.
func TestOptionsFromEnv_AllUnsetReturnsZeros(t *testing.T) {
	for _, k := range []string{
		"S3PGSTORE_S3_MAX_OPEN_CONNECTIONS",
		"S3PGSTORE_S3_MAX_RETRY_ATTEMPTS",
		"S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND",
		"S3PGSTORE_S3_MAX_REQUESTS_PER_SECOND_BURST",
		"S3PGSTORE_S3_USE_PATH_STYLE",
		"AWS_MAX_ATTEMPTS",
	} {
		t.Setenv(k, "")
	}
	opts, err := OptionsFromEnv("")
	if err != nil {
		t.Fatalf("OptionsFromEnv: %v", err)
	}
	if opts != (Options{}) {
		t.Errorf("all-unset should yield zero Options, got %+v", opts)
	}
}

// TestOptionsFromEnv_BadIntErrors verifies a malformed numeric
// value surfaces as an error rather than silently coercing to 0.
func TestOptionsFromEnv_BadIntErrors(t *testing.T) {
	t.Setenv("S3PGSTORE_S3_MAX_OPEN_CONNECTIONS", "not-a-number")
	_, err := OptionsFromEnv("")
	if err == nil {
		t.Fatal("expected parse error, got nil")
	}
}

// TestOptionsFromEnv_NegativeIntErrors verifies non-negative
// validation kicks in (matches WithDefaults's own zero-or-positive
// expectation).
func TestOptionsFromEnv_NegativeIntErrors(t *testing.T) {
	t.Setenv("S3PGSTORE_S3_MAX_OPEN_CONNECTIONS", "-5")
	_, err := OptionsFromEnv("")
	if err == nil {
		t.Fatal("expected error for negative value, got nil")
	}
}

// TestEnvBool covers the strconv.ParseBool contract: standard
// truthy/falsy spellings parse cleanly; empty is the documented
// "off" sentinel; unrecognised values surface as errors at
// startup rather than silently flipping a switch.
func TestEnvBool(t *testing.T) {
	cases := []struct {
		val     string
		want    bool
		wantErr bool
	}{
		// Truthy.
		{val: "1", want: true},
		{val: "t", want: true},
		{val: "T", want: true},
		{val: "true", want: true},
		{val: "TRUE", want: true},
		{val: "True", want: true},
		// Falsy.
		{val: "", want: false},
		{val: "0", want: false},
		{val: "f", want: false},
		{val: "false", want: false},
		{val: "FALSE", want: false},
		// Unrecognised → error.
		{val: "yes", wantErr: true},
		{val: "no", wantErr: true},
		{val: "tru", wantErr: true},
		{val: "garbage", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.val, func(t *testing.T) {
			t.Setenv("S3PGSTORE_PROBE", tc.val)
			got, err := envBool("S3PGSTORE_PROBE")
			if tc.wantErr {
				if err == nil {
					t.Errorf("envBool(%q): want error, got nil", tc.val)
				}
				return
			}
			if err != nil {
				t.Errorf("envBool(%q): unexpected error %v",
					tc.val, err)
			}
			if got != tc.want {
				t.Errorf("envBool(%q): want %v, got %v",
					tc.val, tc.want, got)
			}
		})
	}
}

// TestResolveEnvPrefix_Cases covers the three branches:
// empty → default, lowercase upper-cased, already-upper.
func TestResolveEnvPrefix_Cases(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", "S3PGSTORE"},
		{"primary", "PRIMARY"},
		{"ARCHIVE", "ARCHIVE"},
	}
	for _, tc := range cases {
		if got := resolveEnvPrefix(tc.in); got != tc.want {
			t.Errorf("resolveEnvPrefix(%q): want %q, got %q",
				tc.in, tc.want, got)
		}
	}
}
