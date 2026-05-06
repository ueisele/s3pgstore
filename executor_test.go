package s3pgstore

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

// fakeTx is a stub pgx.Tx used to verify WithTx → txFromContext
// round-trips. It does NOT implement DBTX correctly; tests that
// touch the executor's pool path must use the integration
// fixture instead.
type fakeTx struct {
	pgx.Tx // embed for nil-method panics on accidental call
}

func TestWithTx_RoundTrip(t *testing.T) {
	tx := fakeTx{}
	ctx := WithTx(context.Background(), tx)
	got := txFromContext(ctx)
	if got == nil {
		t.Fatal("txFromContext returned nil after WithTx")
	}
	// The returned interface holds a fakeTx; verify by type
	// assertion. We don't compare values directly since pgx.Tx
	// is an interface and equality semantics vary.
	if _, ok := got.(fakeTx); !ok {
		t.Fatalf("txFromContext returned wrong type: %T", got)
	}
}

func TestTxFromContext_Empty(t *testing.T) {
	if got := txFromContext(context.Background()); got != nil {
		t.Fatalf("txFromContext on bare ctx: want nil, got %v", got)
	}
}

func TestPoolExecutor_NilPool_Run(t *testing.T) {
	e := NewPoolExecutor(nil)
	err := e.Run(context.Background(), func(DBTX) error { return nil })
	if !errors.Is(err, ErrNilPool) {
		t.Fatalf("Run on nil pool: want ErrNilPool, got %v", err)
	}
}

func TestPoolExecutor_NilPool_RunInTx(t *testing.T) {
	e := NewPoolExecutor(nil)
	err := e.RunInTx(context.Background(), func(DBTX) error { return nil })
	if !errors.Is(err, ErrNilPool) {
		t.Fatalf("RunInTx on nil pool: want ErrNilPool, got %v", err)
	}
}

// TestPoolExecutor_UsesContextTx verifies that when ctx carries
// a tx, Run and RunInTx both pass that tx through to fn instead
// of touching the (nil!) pool. The fake tx never has its
// methods invoked here — fn just inspects the DBTX it received.
func TestPoolExecutor_UsesContextTx(t *testing.T) {
	tx := fakeTx{}
	ctx := WithTx(context.Background(), tx)
	e := NewPoolExecutor(nil) // nil pool would normally fail

	var seen DBTX
	if err := e.Run(ctx, func(d DBTX) error { seen = d; return nil }); err != nil {
		t.Fatalf("Run with ctx-tx and nil pool: %v", err)
	}
	if _, ok := seen.(fakeTx); !ok {
		t.Fatalf("Run did not pass ctx tx to fn: got %T", seen)
	}

	seen = nil
	if err := e.RunInTx(ctx, func(d DBTX) error { seen = d; return nil }); err != nil {
		t.Fatalf("RunInTx with ctx-tx and nil pool: %v", err)
	}
	if _, ok := seen.(fakeTx); !ok {
		t.Fatalf("RunInTx did not pass ctx tx to fn: got %T", seen)
	}
}

// TestPoolExecutor_RunInTx_PropagatesError verifies fn errors
// propagate (and the outer code handles rollback). This uses the
// ctx-tx path so we don't need a live pool — the rollback path
// itself is exercised by the integration suite.
func TestPoolExecutor_RunInTx_PropagatesError(t *testing.T) {
	tx := fakeTx{}
	ctx := WithTx(context.Background(), tx)
	e := NewPoolExecutor(nil)

	want := errors.New("inner failure")
	got := e.RunInTx(ctx, func(DBTX) error { return want })
	if !errors.Is(got, want) {
		t.Fatalf("RunInTx: want %v, got %v", want, got)
	}
}

// TestPoolExecutor_RunInTx_PanicPropagatesViaCtxTx confirms
// that a panic inside fn propagates out of RunInTx (after the
// defer's recover + re-panic). The caller-injected tx path
// doesn't allocate a real tx, so we only verify the panic
// behavior here; the integration suite covers the
// rollback-on-panic path against a live pool.
func TestPoolExecutor_RunInTx_PanicPropagatesViaCtxTx(t *testing.T) {
	tx := fakeTx{}
	ctx := WithTx(context.Background(), tx)
	e := NewPoolExecutor(nil)

	defer func() {
		p := recover()
		if p == nil {
			t.Fatal("want panic to propagate, got nil")
		}
		if p != "kaboom" {
			t.Fatalf("panic value: want %q, got %v", "kaboom", p)
		}
	}()
	_ = e.RunInTx(ctx, func(DBTX) error {
		panic("kaboom")
	})
}
