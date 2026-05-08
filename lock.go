package s3pgstore

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrLockNotInTransaction is returned by LockPartition when ctx
// does not carry an active transaction. Advisory-xact locks
// release on autocommit, so taking one outside a transaction
// would silently make the lock useless — the library refuses up
// front. Callers compose with errors.Is.
var ErrLockNotInTransaction = errors.New(
	"LockPartition: ctx has no active transaction (use WithTx)")

// ErrLockTimeout is returned by LockPartition when the
// configured WithLockTimeout deadline expires before the
// advisory lock could be acquired. Callers compose with
// errors.Is.
var ErrLockTimeout = errors.New(
	"LockPartition: lock acquisition timed out")

// LockOption is the interface implemented by LockPartition
// modifiers. Currently the only option is WithLockTimeout; the
// type exists now so additional options can land later without
// breaking the API.
type LockOption interface {
	applyLock(*lockOpts)
}

// lockOpts is the resolved option bag passed to LockPartition.
// The zero value (timeout=0) means "wait indefinitely" — the
// PostgreSQL default for pg_advisory_xact_lock.
type lockOpts struct {
	timeout time.Duration
}

type withLockTimeoutOpt struct{ d time.Duration }

func (o withLockTimeoutOpt) applyLock(opts *lockOpts) {
	opts.timeout = o.d
}

// WithLockTimeout returns a LockOption that bounds the lock
// acquisition wait. The library issues `SET LOCAL lock_timeout
// = '<d>ms'` before the lock acquisition, so the timeout
// applies only to that one statement; the host transaction's
// own statement timing is unaffected.
//
// d <= 0 is treated as "no timeout" (wait indefinitely). Sub-
// millisecond durations are rounded up to 1ms — PostgreSQL's
// lock_timeout is millisecond-granular.
//
// On expiry the lock acquisition returns ErrLockTimeout.
func WithLockTimeout(d time.Duration) LockOption {
	return withLockTimeoutOpt{d: d}
}

// LockPartition acquires a transaction-scoped PostgreSQL
// advisory lock keyed by a stable hash of partitionKey. Other
// callers of LockPartition on the same partition block until
// the lock-holding transaction commits or rolls back; the lock
// is released automatically by PostgreSQL on tx end.
//
// Cooperative protocol: writers that don't call LockPartition
// proceed without blocking on a holder. Pick one strategy per
// partition (OCC or LockPartition) and apply it to every writer
// of that partition — see WithExpectedVersion for the don't-mix
// caveat.
//
// LockPartition requires an active transaction in ctx (injected
// via WithTx). Without one, the advisory lock would release on
// autocommit and the call would be useless; the library returns
// ErrLockNotInTransaction up front rather than silently
// degrading.
//
// Plain SQL operations on the partitions / files tables are
// unaffected — read-only callers (and writers that skip the
// lock) keep proceeding concurrently with a held lock.
//
// The lock key is derived from partitionKey via FNV-64a, which
// is deterministic across processes and runs. Hash collisions
// just over-serialize unrelated partitions; correctness is
// preserved because LockPartition's contract is "blocks other
// LockPartition holders," not "blocks only this exact
// partition's holders."
//
// Lock-ordering caveat: transactions that lock multiple
// partitions must lock them in a deterministic order (e.g.,
// sorted by partition key) across all callers. Two transactions
// locking A→B and B→A deadlock; PostgreSQL detects and aborts
// one with code 40P01, but the situation is avoidable.
func (s *Store[T]) LockPartition(
	ctx context.Context, partitionKey string, opts ...LockOption,
) (err error) {
	defer s.metrics.methodScope(ctx, "LockPartition", &err).end()

	if txFromContext(ctx) == nil {
		return ErrLockNotInTransaction
	}

	var o lockOpts
	for _, opt := range opts {
		opt.applyLock(&o)
	}

	key := lockKey(partitionKey)
	return s.cfg.Executor.Run(ctx, func(d DBTX) error {
		if o.timeout > 0 {
			ms := max(o.timeout.Milliseconds(), 1)
			if _, err := d.Exec(ctx,
				fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", ms),
			); err != nil {
				return fmt.Errorf("set lock_timeout: %w", err)
			}
		}
		if _, err := d.Exec(ctx,
			"SELECT pg_advisory_xact_lock($1)", key,
		); err != nil {
			if isLockTimeoutErr(err) {
				return ErrLockTimeout
			}
			return fmt.Errorf("acquire advisory lock: %w", err)
		}
		return nil
	})
}

// lockKey hashes partitionKey to a stable int64 suitable for
// pg_advisory_xact_lock. FNV-64a is deterministic, dependency-
// free, and well-distributed enough for our purposes —
// collisions just over-serialize unrelated partitions.
//
// The uint64→int64 cast is intentional: PostgreSQL's bigint is
// signed int64, so any int64 value is a valid argument.
func lockKey(partitionKey string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(partitionKey))
	return int64(h.Sum64()) //nolint:gosec // signed-cast intentional
}

// isLockTimeoutErr reports whether err is PostgreSQL's
// lock_not_available (SQLSTATE 55P03), the code returned when
// lock_timeout fires before the advisory lock could be taken.
func isLockTimeoutErr(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == "55P03"
	}
	return false
}
