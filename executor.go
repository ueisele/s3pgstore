package s3pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DBTX is the pgx-native interface s3pgstore uses for database
// access. *pgx.Conn, *pgxpool.Pool, *pgxpool.Conn, and pgx.Tx
// all satisfy it.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Executor abstracts the caller's connection-management
// strategy. The library calls Run for read paths and
// single-statement writes; RunInTx for the multi-statement
// catalog write path.
//
// The executor's contract:
//   - If ctx already carries an active transaction (via WithTx
//     or the caller's own tx manager), fn participates in it —
//     no nested BEGIN.
//   - Otherwise Run acquires a connection from the pool for
//     fn and releases it on return; RunInTx opens a new
//     transaction, commits on nil return, rolls back on error.
//
// This is the only seam between the library and the caller's
// PostgreSQL access stack. NewPoolExecutor returns the default
// implementation backed by pgxpool. Callers using GORM or a
// custom database/sql wrapper implement Executor in their own
// adapter package.
type Executor interface {
	Run(ctx context.Context, fn func(DBTX) error) error
	RunInTx(ctx context.Context, fn func(DBTX) error) error
}

// txCtxKey is the unexported context-key type for WithTx /
// txFromContext. Unexported so callers can't bypass the
// constructor and inject a typed value of their own.
type txCtxKey struct{}

// WithTx returns a child context carrying tx. The default pool
// executor reads it back and uses tx for both Run and RunInTx
// (instead of pool-acquiring or pool-beginning), letting
// callers compose s3pgstore writes with their own pgx
// transactions.
//
// Callers using a host transaction manager (GORM, custom
// database/sql wrapper) don't need this — their tx manager
// populates the context, and their Executor adapter reads from
// it directly.
func WithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txCtxKey{}, tx)
}

// txFromContext returns the pgx.Tx injected by WithTx, or nil
// if none is present. Used by the default pool executor to
// detect caller-managed transactions.
func txFromContext(ctx context.Context) pgx.Tx {
	tx, _ := ctx.Value(txCtxKey{}).(pgx.Tx)
	return tx
}

// poolExecutor is the default Executor implementation backed by
// a pgxpool.Pool. Reads pgx.Tx from context (via WithTx) when
// present; otherwise pool-acquires for Run or pool-begins for
// RunInTx.
type poolExecutor struct {
	pool *pgxpool.Pool
}

// NewPoolExecutor returns the default pgx-native Executor for
// pool. If pool is nil, the returned executor returns
// ErrNilPool from every method — useful for catching wiring
// mistakes early rather than at first query.
func NewPoolExecutor(pool *pgxpool.Pool) Executor {
	return &poolExecutor{pool: pool}
}

// ErrNilPool is returned by NewPoolExecutor's executor when it
// was constructed with a nil pool.
var ErrNilPool = errors.New("s3pgstore: pgxpool.Pool is nil")

func (e *poolExecutor) Run(ctx context.Context, fn func(DBTX) error) error {
	if tx := txFromContext(ctx); tx != nil {
		return fn(tx)
	}
	if e.pool == nil {
		return ErrNilPool
	}
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("s3pgstore: acquire pool conn: %w", err)
	}
	defer conn.Release()
	return fn(conn)
}

func (e *poolExecutor) RunInTx(ctx context.Context, fn func(DBTX) error) error {
	if tx := txFromContext(ctx); tx != nil {
		return fn(tx)
	}
	if e.pool == nil {
		return ErrNilPool
	}
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("s3pgstore: begin tx: %w", err)
	}
	defer func() {
		// Best-effort rollback on early return / panic. Rollback
		// after Commit returns ErrTxClosed which is harmless.
		_ = tx.Rollback(ctx)
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("s3pgstore: commit tx: %w", err)
	}
	return nil
}
