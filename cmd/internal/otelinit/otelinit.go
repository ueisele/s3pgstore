// Package otelinit wires an OpenTelemetry MeterProvider for the
// s3pgstore-* binaries from standard OTel SDK environment
// variables. Telemetry is opt-in: with no OTel env vars set,
// Setup returns a no-op meter and a no-op shutdown so the
// binaries behave exactly as they did before this package
// existed.
//
// Telemetry is enabled when any of these env vars is set:
//
//	OTEL_METRICS_EXPORTER             otlp / prometheus / console / none
//	OTEL_EXPORTER_OTLP_ENDPOINT       e.g. http://collector:4317
//	OTEL_EXPORTER_OTLP_METRICS_ENDPOINT  metrics-specific override
//
// Once enabled, the rest of the configuration follows the
// standard OTel SDK env-var conventions (autoexport reads them):
//
//	OTEL_EXPORTER_OTLP_PROTOCOL       grpc (default) / http/protobuf
//	OTEL_EXPORTER_OTLP_HEADERS        comma-separated key=value list
//	OTEL_METRIC_EXPORT_INTERVAL       periodic-reader interval (ms)
//	OTEL_SERVICE_NAME                 overrides the binary's default
//	OTEL_RESOURCE_ATTRIBUTES          extra resource attributes
package otelinit

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// ShutdownFn flushes pending telemetry and releases provider
// resources. Always non-nil — even in the no-op path it returns
// nil immediately so callers can defer it unconditionally.
type ShutdownFn func(context.Context) error

// Setup returns a Meter for the given binary name. When OTel env
// vars indicate telemetry is desired (see package doc),
// constructs a real autoexport-backed MeterProvider, sets it as
// the OTel global, and returns its Meter plus a shutdown closer.
// Otherwise returns a no-op meter and a no-op shutdown so the
// caller can defer it without nil-checking.
//
// defaultServiceName is used as service.name unless
// OTEL_SERVICE_NAME (or OTEL_RESOURCE_ATTRIBUTES) overrides it.
func Setup(
	ctx context.Context, defaultServiceName string,
) (metric.Meter, ShutdownFn, error) {
	if !enabled() {
		return noop.NewMeterProvider().Meter(defaultServiceName),
			func(context.Context) error { return nil }, nil
	}

	// Resource: env-derived attrs win over our default
	// service.name, matching the OTel SDK convention.
	defaultRes := resource.NewSchemaless(
		semconv.ServiceName(defaultServiceName))
	envRes, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithTelemetrySDK())
	if err != nil {
		return nil, nil, fmt.Errorf("otel resource: %w", err)
	}
	res, err := resource.Merge(defaultRes, envRes)
	if err != nil {
		return nil, nil, fmt.Errorf("otel resource merge: %w", err)
	}

	reader, err := autoexport.NewMetricReader(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("otel metric reader: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res))
	otel.SetMeterProvider(provider)

	shutdown := func(ctx context.Context) error {
		// Force-flush so the last batch lands before the
		// process exits, then tear down the reader.
		var errs []error
		if err := provider.ForceFlush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("force flush: %w", err))
		}
		if err := provider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown: %w", err))
		}
		return errors.Join(errs...)
	}
	return provider.Meter(defaultServiceName), shutdown, nil
}

func enabled() bool {
	for _, k := range []string{
		"OTEL_METRICS_EXPORTER",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	} {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return false
}
