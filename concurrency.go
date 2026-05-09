package s3pgstore

// Adapted from
// https://github.com/ueisele/s3store/blob/738a8bcbce870887833e158d4dc4e5116a29d4fc/concurrency.go
// Copyright (c) 2024-2026 Uwe Eisele. MIT License.
//
// Adapted to s3pgstore: dropped the *metrics parameter
// (Phase 16 wires the fanOutObserver callback below); added
// per-worker panic recovery so a panic in one work() invocation
// becomes an error rather than tearing down the process. The
// recovered panic is logged at WARN with the goroutine's stack
// so operators can still diagnose it.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// fanOutObserver is the optional metric/log hook fired exactly
// once per fanOut call (after the worker count is computed).
// nil is permitted — useful for tests and pre-Phase-16 callers.
type fanOutObserver func(ctx context.Context, items, workers int)

// fanOut runs work in parallel across items[0..len(items)) using
// a worker pool of at most min(concurrency, len(items)) goroutines.
// Each work invocation gets a per-call ctx that's cancelled when
// any sibling errors or the caller cancels, so blocked S3 calls
// bail promptly. Results land in the slot the caller chose by
// capturing i in the closure — slot indices are stable so callers
// can write into preallocated result slots without coordination.
//
// Workers share a single atomic counter to claim items, so each
// (i, item) pair is processed by exactly one worker and slot i
// is only written by that worker.
//
// Bounded goroutine count: spawned goroutines are capped at the
// worker count even when len(items) is large. Matters for
// nested fan-out (e.g. write partitions × MV inserts per
// partition) where the older one-goroutine-per-item shape could
// spawn N×K goroutines that mostly parked waiting for an HTTP
// transport socket (capped by Config.S3MaxOpenConnections).
//
// Error semantics: the first real (non-cancellation) error wins
// and cancels the rest. context.Canceled errors from siblings
// after that cancel are filtered out so callers see the
// root-cause failure, not the fallout. If no real error fired
// but the parent ctx is done, the parent ctx error is returned
// so a caller-triggered cancel surfaces as an error instead of
// an empty-success.
//
// Panic safety: a panic inside work() is recovered, logged at
// WARN with the goroutine's stack, and surfaced as a normal
// error (decorated with the panic value). Sibling workers are
// cancelled the same way as on a returned error. This trades a
// stack-trace-on-stderr crash for a graceful per-call error;
// operators still see the panic in logs.
//
// Fast path: len(items) == 1 calls work directly without
// spawning a goroutine, avoiding scheduler overhead for the
// sugar-wrapper single-item case. The fast path also recovers
// panics so callers can rely on uniform error semantics.
//
// concurrency <= 0 clamps to 1 worker (rather than deadlocking
// on a 0-sized pool).
func fanOut[I any](
	ctx context.Context,
	items []I,
	concurrency int,
	obs fanOutObserver,
	work func(ctx context.Context, i int, item I) error,
) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) == 1 {
		if obs != nil {
			obs(ctx, 1, 1)
		}
		return runOneRecover(ctx, 0, items[0], work)
	}

	workers := max(1, min(concurrency, len(items)))
	if obs != nil {
		obs(ctx, len(items), workers)
	}

	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make([]error, len(items))
	var next atomic.Int64
	var wg sync.WaitGroup

	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				i := int(next.Add(1)) - 1
				if i >= len(items) {
					return
				}
				if err := runOneRecover(ctx, i, items[i], work); err != nil {
					errs[i] = err
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()

	for _, err := range errs {
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		return err
	}
	return parentCtx.Err()
}

// runOneRecover invokes work(ctx, i, item) inside a recover
// guard. A panic is logged with stack and converted to an
// error so the fan-out's first-error-wins semantics apply
// uniformly.
func runOneRecover[I any](
	ctx context.Context,
	i int, item I,
	work func(ctx context.Context, i int, item I) error,
) (err error) {
	defer func() {
		if p := recover(); p != nil {
			stack := debug.Stack()
			slog.Warn("s3pgstore: fanOut worker panic recovered",
				"index", i,
				"panic", fmt.Sprint(p),
				"stack", string(stack))
			err = fmt.Errorf(
				"fanOut worker [%d] panicked: %v", i, p)
		}
	}()
	return work(ctx, i, item)
}

// fanOutMapReduce runs mapFn across items in parallel via fanOut,
// then folds the per-item slices through reduceFn. The map step
// reuses every guarantee of fanOut (worker pool, first-error
// wins, cancellation propagation, panic recovery, observer
// hook); the reduce step runs once on the main goroutine after
// all workers finish, so reduceFn does not need to be safe for
// concurrent use.
//
// Per-item results are stored in a preallocated [][]O slot so
// workers never share state. On any map error the reduce step
// is skipped and the zero value of R is returned alongside the
// error.
//
// Empty input returns reduceFn(nil) — reducers that need a
// different empty-case answer should check for it themselves.
func fanOutMapReduce[I, O, R any](
	ctx context.Context,
	items []I,
	concurrency int,
	obs fanOutObserver,
	mapFn func(ctx context.Context, item I) ([]O, error),
	reduceFn func([][]O) R,
) (R, error) {
	results := make([][]O, len(items))
	err := fanOut(ctx, items, concurrency, obs,
		func(ctx context.Context, i int, item I) error {
			r, err := mapFn(ctx, item)
			if err != nil {
				return err
			}
			results[i] = r
			return nil
		})
	if err != nil {
		var zero R
		return zero, err
	}
	return reduceFn(results), nil
}
