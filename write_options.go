package s3pgstore

import "errors"

// ErrVersionConflict is returned by Write when
// WithExpectedVersion is set and the partition's actual
// version doesn't match. Callers compose with errors.Is.
var ErrVersionConflict = errors.New("version conflict")

// WithIdempotencyToken returns a WriteOption that tags the
// write with token for retry-safe idempotency. Retries with
// the same (partition_key, token) pair return the original
// WriteResult without producing a second catalog row.
//
// Mutually exclusive with WithIdempotencyTokenOf — passing
// both fails Write at the call site (caught when options
// resolve, before any S3 PUT).
//
// Idempotency is unbounded in time per CLAUDE.md.
func WithIdempotencyToken(token string) WriteOption {
	return idempotencyTokenOpt{token: token}
}

// WithIdempotencyTokenOf returns a WriteOption that derives a
// per-partition idempotency token from the partition's records.
// Invoked once per partition (after grouping); each
// partition's catalog row uses its own token.
//
// Useful for fan-out writes whose partition key is composite
// and whose token must hash the partition's records (so
// concurrent fan-outs of the same input observe the same
// token even when they group records differently).
//
// Mutually exclusive with WithIdempotencyToken.
func WithIdempotencyTokenOf[T any](fn func([]T) (string, error)) WriteOption {
	return idempotencyTokenOfOpt[T]{fn: fn}
}

// WithExpectedVersion returns a WriteOption that asserts the
// partition's current version equals expected. The Write
// fails with ErrVersionConflict if the partition's actual
// version differs.
//
// expected=0 has special semantics: it asserts "this partition
// has had no writes yet" — satisfied when the partition row
// doesn't exist (the only version=0 state in v2.0; LockPartition
// uses pg_advisory_xact_lock and never touches the row). Upserts
// successfully into version=1.
//
// expected>0 requires the partition row to exist at that
// version; "no row yet" fails with ErrVersionConflict.
func WithExpectedVersion(expected int64) WriteOption {
	return expectedVersionOpt{version: expected}
}

type idempotencyTokenOpt struct {
	token string
}

func (o idempotencyTokenOpt) applyWrite(opts *writeOpts) {
	t := o.token
	opts.idempotencyToken = &t
}

type idempotencyTokenOfOpt[T any] struct {
	fn func([]T) (string, error)
}

func (o idempotencyTokenOfOpt[T]) applyWrite(opts *writeOpts) {
	// Cast through any since writeOpts is Type-erased; the
	// per-partition driver in writePartition uses the cast
	// to call fn against the typed records.
	opts.idempotencyTokenOf = func() (string, error) {
		// This direct call surface is unused — the per-partition
		// fn invocation goes through opts.idempotencyTokenOfFn,
		// resolved below.
		return "", nil
	}
	opts.idempotencyTokenOfFn = func(records any) (string, error) {
		typed, ok := records.([]T)
		if !ok {
			return "", errMixedTypeIdempotencyTokenOf
		}
		return o.fn(typed)
	}
}

type expectedVersionOpt struct {
	version int64
}

func (o expectedVersionOpt) applyWrite(opts *writeOpts) {
	opts.expectedVersionSet = true
	opts.expectedVersion = o.version
}

// errMixedTypeIdempotencyTokenOf would only fire when a
// caller manages to pass a WithIdempotencyTokenOf opt
// constructed for one T to a Store of another T — the type
// system makes that hard but not impossible (e.g. via
// reflection or build-tag tomfoolery), so the runtime check
// is here as a safety net.
//
//nolint:gochecknoglobals // sentinel
var errMixedTypeIdempotencyTokenOf = errors.New(
	"WithIdempotencyTokenOf type mismatch")
