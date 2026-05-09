package s3client

// storelabels.go owns the per-Store identity that the metrics
// middleware promotes to attributes on per-op metrics. Two
// Stores sharing one *s3.Client (the recommended setup) get
// distinguishable s3pgstore.s3.{request,attempt.error,body_size,
// ratelimit.wait}.* streams; per-client metrics
// (tcp.connections, connection.reuse, adaptive_retry.wait) stay
// unlabeled because they're inherently shared.
//
// Implementation choice: a private typed context key, not OTel
// baggage. Baggage is primarily designed for cross-service HTTP
// header propagation — installing a baggage propagator in the
// caller's app would emit a `baggage:` header on every S3
// request, leaking bucket/prefix to S3 and wasting bytes. A
// private context key is in-process only, typed, and zero-cost
// when no labels were set.

import "context"

// storeLabelsCtxKey is the unexported type used as the
// context.Value key for store labels. Unexported type → no other
// package can collide; nil-safe via valueless lookup.
//
//nolint:gochecknoglobals // canonical Go context-key pattern
type storeLabelsCtxKey struct{}

// storeLabels carries the (bucket, prefix) tuple the metrics
// middleware emits as labels. Both fields are independent — a
// caller can set just bucket if no prefix is meaningful.
type storeLabels struct {
	bucket string
	prefix string
}

// WithStoreLabels returns a derived ctx carrying bucket and
// prefix. The s3client metrics middleware reads these from ctx
// and emits them as s3pgstore.bucket / s3pgstore.prefix labels
// on per-op metrics (request.duration, request.count,
// attempt.error.count, body_size, ratelimit.wait.duration).
//
// Empty values are treated the same as "label not set" by the
// middleware — the attribute is omitted, keeping cardinality
// minimal for callers that don't differentiate per-Store.
//
// Per-client metrics (tcp.connections, connection.reuse,
// adaptive_retry.wait.duration) deliberately do not receive
// these labels. Those resources (connection pool, dialer
// tracking, adaptive token bucket) are owned by the *s3.Client
// itself and shared across every caller that holds it; per-Store
// attribution there would be misleading (Store A's adaptive-wait
// can be caused by Store B's prior throttling on the shared
// bucket).
//
// The s3pgstore library calls this from s3target before each
// PutObject / GetObject / DeleteObject; library callers using
// the s3.Client directly can call it themselves to opt into
// the same labeling.
func WithStoreLabels(
	ctx context.Context, bucket, prefix string,
) context.Context {
	return context.WithValue(ctx, storeLabelsCtxKey{},
		storeLabels{bucket: bucket, prefix: prefix})
}

// storeLabelsFromContext returns the (bucket, prefix) tuple
// that WithStoreLabels stashed on ctx, or zero values if none
// was set. Returning two strings (rather than the storeLabels
// struct) keeps the export surface minimal — only the metrics
// middleware needs this and it just unpacks them into
// attribute.String calls.
func storeLabelsFromContext(
	ctx context.Context,
) (bucket, prefix string) {
	if v, ok := ctx.Value(storeLabelsCtxKey{}).(storeLabels); ok {
		return v.bucket, v.prefix
	}
	return "", ""
}
