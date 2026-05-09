package s3client

// env.go owns the environment-variable wiring for this package.
// Two entry points layered on top of WithDefaults:
//
//   - OptionsFromEnv: pure parser. Reads <PREFIX>_S3_* env vars
//     into an Options struct. No I/O beyond os.Getenv. Useful for
//     callers who want env-driven knobs but their own aws.Config.
//
//   - NewClientFromEnv: full constructor. LoadDefaultConfig +
//     OptionsFromEnv + WithDefaults in one call. The one-liner
//     for operator binaries and most library users.
//
// Region, endpoint, credentials, and other AWS-layer concerns
// are deliberately NOT in the <PREFIX>_S3_* schema — they're
// already covered by the AWS SDK's own env-var resolution
// (AWS_REGION, AWS_ENDPOINT_URL_S3, AWS_ACCESS_KEY_ID,
// AWS_PROFILE, IRSA, IMDS, ...). Shadowing them would create
// "which wins?" confusion. The only s3-client knob without an
// AWS-side equivalent is UsePathStyle, which is on Options.

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/otel/metric"
)

// DefaultEnvPrefix is the prefix used when callers pass an empty
// string to OptionsFromEnv / NewClientFromEnv. Multi-Store setups
// override it (e.g. "PRIMARY", "ARCHIVE") to scope env vars.
const DefaultEnvPrefix = "S3PGSTORE"

// OptionsFromEnv reads the <PREFIX>_S3_* env vars into an
// Options struct. Pure parser — no I/O beyond os.Getenv, no
// network calls, no awsConfig load. Returns an error on
// unparseable values (bad number / bool); missing values fall
// back to the documented defaults (zero on the struct, which
// WithDefaults resolves later).
//
// prefix == "" → DefaultEnvPrefix ("S3PGSTORE"). The supplied
// value is upper-cased before composing env-var names, so
// callers who pass "primary" get the expected "PRIMARY_S3_*"
// lookups.
//
// Env vars consumed:
//
//	<PREFIX>_S3_MAX_OPEN_CONNECTIONS            int
//	<PREFIX>_S3_MAX_RETRY_ATTEMPTS              int (falls back to AWS_MAX_ATTEMPTS)
//	<PREFIX>_S3_MAX_REQUESTS_PER_SECOND         float64
//	<PREFIX>_S3_MAX_REQUESTS_PER_SECOND_BURST   int
//	<PREFIX>_S3_USE_PATH_STYLE                  bool (strconv.ParseBool — accepts 1/t/T/TRUE/true/True; 0/f/F/FALSE/false/False; errors on anything else)
//
// Meter is left nil (no env var produces a metric.Meter); the
// caller plugs it in:
//
//	opts, err := s3client.OptionsFromEnv("")
//	opts.Meter = meter
//
// Or just calls NewClientFromEnv, which takes meter as an
// explicit parameter.
func OptionsFromEnv(prefix string) (Options, error) {
	p := resolveEnvPrefix(prefix)
	var opts Options
	var err error

	if opts.MaxOpenConnections, err = envIntNonNeg(
		p + "_S3_MAX_OPEN_CONNECTIONS"); err != nil {
		return Options{}, err
	}
	if opts.MaxRetryAttempts, err = envIntNonNeg(
		p + "_S3_MAX_RETRY_ATTEMPTS"); err != nil {
		return Options{}, err
	}
	// Honour AWS_MAX_ATTEMPTS as the fallback when the
	// s3pgstore-namespaced var is unset. Reason: callers
	// already wiring AWS's standard knobs (e.g. via IRSA pod
	// templates or shared k8s base configs) shouldn't need to
	// double-set it under our prefix. Our adaptive retryer
	// gets installed regardless of AWS_RETRY_MODE — we only
	// inherit the attempts count.
	if opts.MaxRetryAttempts == 0 {
		if opts.MaxRetryAttempts, err = envIntNonNeg(
			"AWS_MAX_ATTEMPTS"); err != nil {
			return Options{}, err
		}
	}
	if opts.MaxRequestsPerSecond, err = envFloatNonNeg(
		p + "_S3_MAX_REQUESTS_PER_SECOND"); err != nil {
		return Options{}, err
	}
	if opts.MaxRequestsPerSecondBurst, err = envIntNonNeg(
		p + "_S3_MAX_REQUESTS_PER_SECOND_BURST"); err != nil {
		return Options{}, err
	}
	if opts.UsePathStyle, err = envBool(
		p + "_S3_USE_PATH_STYLE"); err != nil {
		return Options{}, err
	}
	return opts, nil
}

// NewClientFromEnv constructs an s3.Client wired from
// environment variables: AWS standards via LoadDefaultConfig
// (AWS_REGION, AWS_ENDPOINT_URL_S3, credentials chain) plus the
// s3pgstore <PREFIX>_S3_* knobs via OptionsFromEnv.
//
// prefix == "" → DefaultEnvPrefix ("S3PGSTORE").
//
// optFns run LAST after WithDefaults, so callers can override
// anything (e.g. point at a non-default endpoint that
// AWS_ENDPOINT_URL_S3 doesn't cover):
//
//	client, err := s3client.NewClientFromEnv(ctx, "", meter,
//	    func(o *s3.Options) {
//	        o.BaseEndpoint = aws.String("https://my-internal/")
//	    })
//
// The region-override bug from the previous loadS3Client
// implementations is fixed here: we no longer pass
// awsconfig.WithRegion("us-east-1") as a default, so
// AWS_REGION (and shared-config region) take effect as the SDK
// intends.
func NewClientFromEnv(
	ctx context.Context,
	prefix string,
	meter metric.Meter,
	optFns ...func(*awss3.Options),
) (*awss3.Client, error) {
	opts, err := OptionsFromEnv(prefix)
	if err != nil {
		return nil, err
	}
	opts.Meter = meter
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	all := append([]func(*awss3.Options){WithDefaults(opts)}, optFns...)
	return awss3.NewFromConfig(awsCfg, all...), nil
}

// resolveEnvPrefix returns the upper-cased prefix to compose
// env-var names from. Empty input → DefaultEnvPrefix.
func resolveEnvPrefix(prefix string) string {
	if prefix == "" {
		return DefaultEnvPrefix
	}
	return strings.ToUpper(prefix)
}

// envIntNonNeg parses a non-negative int env var. Empty → 0.
// Negative or non-numeric values surface as a structured error.
func envIntNonNeg(key string) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", key, v, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("%s %d: must be >= 0", key, n)
	}
	return n, nil
}

// envFloatNonNeg parses a non-negative float env var. Empty → 0.
func envFloatNonNeg(key string) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", key, v, err)
	}
	if f < 0 {
		return 0, fmt.Errorf("%s %g: must be >= 0", key, f)
	}
	return f, nil
}

// envBool parses a bool env var via strconv.ParseBool — accepts
// 1/t/T/TRUE/true/True (truthy) and 0/f/F/FALSE/false/False
// (falsy). Empty → false (no error: unset is the documented
// "off" sentinel). Anything else → error, so a typo like
// "tru" surfaces at startup rather than silently flipping a
// switch off.
func envBool(key string) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s %q: %w", key, v, err)
	}
	return b, nil
}
