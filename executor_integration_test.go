//go:build integration

package s3pgstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/ueisele/s3pgstore"
)

// TestPoolExecutor_RunRoundTrip exercises the pool path of
// NewPoolExecutor: it acquires a connection, runs fn, releases.
// fn issues a trivial SELECT to confirm the DBTX it received is
// usable.
func TestPoolExecutor_RunRoundTrip(t *testing.T) {
	f := newFixture(t)
	e := s3pgstore.NewPoolExecutor(f.Pool)
	if err := e.Run(t.Context(), func(d s3pgstore.DBTX) error {
		var got int
		if err := d.QueryRow(t.Context(), `SELECT 42`).Scan(&got); err != nil {
			return err
		}
		if got != 42 {
			t.Fatalf("SELECT 42: got %d", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestPoolExecutor_RunInTxCommits verifies the pool-tx path:
// statements inside fn become visible after RunInTx returns.
func TestPoolExecutor_RunInTxCommits(t *testing.T) {
	f := newFixture(t)
	e := s3pgstore.NewPoolExecutor(f.Pool)

	tableQ := `CREATE TABLE ` + qualified(f.Schema, "executor_smoke") +
		` (id int PRIMARY KEY)`
	if _, err := f.Pool.Exec(t.Context(), tableQ); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if err := e.RunInTx(t.Context(), func(d s3pgstore.DBTX) error {
		_, err := d.Exec(t.Context(),
			`INSERT INTO `+qualified(f.Schema, "executor_smoke")+` VALUES (1)`)
		return err
	}); err != nil {
		t.Fatalf("RunInTx: %v", err)
	}

	var got int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT id FROM `+qualified(f.Schema, "executor_smoke")).
		Scan(&got); err != nil {
		t.Fatalf("post-commit SELECT: %v", err)
	}
	if got != 1 {
		t.Fatalf("post-commit SELECT: want 1, got %d", got)
	}
}

// TestPoolExecutor_RunInTxRollsBackOnError verifies fn errors
// roll back the transaction.
func TestPoolExecutor_RunInTxRollsBackOnError(t *testing.T) {
	f := newFixture(t)
	e := s3pgstore.NewPoolExecutor(f.Pool)

	if _, err := f.Pool.Exec(t.Context(),
		`CREATE TABLE `+qualified(f.Schema, "executor_rollback")+
			` (id int PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	wantErr := errors.New("rollback please")
	got := e.RunInTx(t.Context(), func(d s3pgstore.DBTX) error {
		if _, err := d.Exec(t.Context(),
			`INSERT INTO `+qualified(f.Schema, "executor_rollback")+
				` VALUES (1)`); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(got, wantErr) {
		t.Fatalf("RunInTx: want %v, got %v", wantErr, got)
	}

	var n int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM `+
			qualified(f.Schema, "executor_rollback")).Scan(&n); err != nil {
		t.Fatalf("post-rollback count: %v", err)
	}
	if n != 0 {
		t.Fatalf("post-rollback count: want 0, got %d", n)
	}
}

// TestPoolExecutor_ParticipatesInCallerTx verifies that when the
// caller injects a pgx.Tx via WithTx, both Run and RunInTx use
// that tx instead of pool-acquiring or pool-beginning a new one.
// Demonstrated via rollback: the caller rolls back; the row
// inserted via the executor disappears.
func TestPoolExecutor_ParticipatesInCallerTx(t *testing.T) {
	f := newFixture(t)
	e := s3pgstore.NewPoolExecutor(f.Pool)

	if _, err := f.Pool.Exec(t.Context(),
		`CREATE TABLE `+qualified(f.Schema, "executor_join")+
			` (id int PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tx, err := f.Pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin caller tx: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	ctx := s3pgstore.WithTx(t.Context(), tx)
	if err := e.Run(ctx, func(d s3pgstore.DBTX) error {
		_, err := d.Exec(t.Context(),
			`INSERT INTO `+qualified(f.Schema, "executor_join")+` VALUES (1)`)
		return err
	}); err != nil {
		t.Fatalf("Run participating: %v", err)
	}
	if err := e.RunInTx(ctx, func(d s3pgstore.DBTX) error {
		_, err := d.Exec(t.Context(),
			`INSERT INTO `+qualified(f.Schema, "executor_join")+` VALUES (2)`)
		return err
	}); err != nil {
		t.Fatalf("RunInTx participating: %v", err)
	}

	// Roll back the caller's tx — both rows must disappear.
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("caller rollback: %v", err)
	}

	var n int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM `+
			qualified(f.Schema, "executor_join")).Scan(&n); err != nil {
		t.Fatalf("post-rollback count: %v", err)
	}
	if n != 0 {
		t.Fatalf("post-rollback count: want 0 (executor honored caller tx), "+
			"got %d", n)
	}
}

// TestPoolExecutor_RunInTx_PanicRollsBack confirms that a
// panic inside fn rolls back resources before the panic
// propagates. Verified by checking that an INSERT inside the
// panicking fn never lands in the table.
func TestPoolExecutor_RunInTx_PanicRollsBack(t *testing.T) {
	f := newFixture(t)
	e := s3pgstore.NewPoolExecutor(f.Pool)

	if _, err := f.Pool.Exec(t.Context(),
		`CREATE TABLE `+qualified(f.Schema, "executor_panic")+
			` (id int PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic to propagate")
			}
		}()
		_ = e.RunInTx(t.Context(), func(d s3pgstore.DBTX) error {
			if _, err := d.Exec(t.Context(),
				`INSERT INTO `+qualified(f.Schema, "executor_panic")+
					` VALUES (1)`); err != nil {
				t.Fatalf("INSERT: %v", err)
			}
			panic("kaboom")
		})
	}()

	var n int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM `+
			qualified(f.Schema, "executor_panic")).Scan(&n); err != nil {
		t.Fatalf("post-panic count: %v", err)
	}
	if n != 0 {
		t.Fatalf("rows after panic: want 0 (rollback happened), got %d", n)
	}
}

// TestPoolExecutor_RunInTx_CancelledCtxStillRollsBack confirms
// that when fn returns an error AND the ctx is cancelled, the
// rollback still runs. Without context.WithoutCancel on the
// rollback, the rollback would be no-op'd by the cancelled
// ctx and the row would leak until PostgreSQL's
// idle-in-transaction timeout fires.
func TestPoolExecutor_RunInTx_CancelledCtxStillRollsBack(t *testing.T) {
	f := newFixture(t)
	e := s3pgstore.NewPoolExecutor(f.Pool)

	if _, err := f.Pool.Exec(t.Context(),
		`CREATE TABLE `+qualified(f.Schema, "executor_cancel")+
			` (id int PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	wantErr := errors.New("cancel-then-fail")
	got := e.RunInTx(ctx, func(d s3pgstore.DBTX) error {
		if _, err := d.Exec(ctx,
			`INSERT INTO `+qualified(f.Schema, "executor_cancel")+
				` VALUES (1)`); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		// Cancel the caller's ctx, then return an error so the
		// defer's rollback path runs against the cancelled ctx.
		cancel()
		return wantErr
	})
	if !errors.Is(got, wantErr) {
		t.Fatalf("RunInTx: want %v, got %v", wantErr, got)
	}

	var n int
	if err := f.Pool.QueryRow(t.Context(),
		`SELECT count(*) FROM `+
			qualified(f.Schema, "executor_cancel")).Scan(&n); err != nil {
		t.Fatalf("post-rollback count: %v", err)
	}
	if n != 0 {
		t.Fatalf("rows after cancelled-ctx rollback: want 0, got %d", n)
	}
}

func qualified(schema, table string) string {
	return `"` + schema + `"."` + table + `"`
}
