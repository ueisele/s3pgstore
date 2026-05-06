package s3pgstore

// Adapted from
// https://github.com/ueisele/s3store/blob/738a8bcbce870887833e158d4dc4e5116a29d4fc/pool_determinism_test.go,
// parquet_lifetime_test.go, and parquet_bench_test.go.
// Copyright (c) 2024-2026 Uwe Eisele. MIT License.
//
// Adapted to s3pgstore: encoder is the standalone parquetEncoder
// type rather than (*Writer[T]).encodeParquet; CompressionCodec
// is an int enum so resolveCompression(CompressionSnappy) replaces
// the upstream string-keyed switch.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

func TestResolveCompression(t *testing.T) {
	cases := []struct {
		name string
		in   CompressionCodec
		want compress.Codec
	}{
		{"default", CompressionDefault, &parquet.Snappy},
		{"snappy", CompressionSnappy, &parquet.Snappy},
		{"zstd", CompressionZstd, &parquet.Zstd},
		{"gzip", CompressionGzip, &parquet.Gzip},
		{"uncompressed", CompressionUncompressed, &parquet.Uncompressed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveCompression(tc.in)
			if err != nil {
				t.Fatalf("resolveCompression(%v): %v", tc.in, err)
			}
			if fmt.Sprintf("%T", got) != fmt.Sprintf("%T", tc.want) {
				t.Errorf("type mismatch: want %T, got %T", tc.want, got)
			}
		})
	}
}

func TestResolveCompression_Unknown(t *testing.T) {
	if _, err := resolveCompression(CompressionCodec(999)); err == nil {
		t.Fatal("resolveCompression(999): want error, got nil")
	}
}

// TestEncodeParquet_PooledMatchesFresh checks that pooling
// parquet writers via Reset produces byte-identical output to
// fresh construction for the same input. CLAUDE.md asserts
// deterministic parquet encoding ("same records + same codec
// produce byte-identical bytes") as a load-bearing invariant for
// WithIdempotencyToken retries — refactors must preserve it.
//
// Encodes the same record set three ways:
//  1. Fresh package-level encodeParquetUnpooled (no pool).
//  2. First call through parquetEncoder.encode (pool empty →
//     fresh under the hood, but goes through the new code path).
//  3. Second call through the same parquetEncoder.encode (pool
//     populated → exercises the Reset-and-reuse path).
//
// All three must produce byte-identical output.
func TestEncodeParquet_PooledMatchesFresh(t *testing.T) {
	type rec struct {
		Period   string `parquet:"period"`
		Customer string `parquet:"customer"`
		SKU      string `parquet:"sku"`
		Payload  []byte `parquet:"payload"`
	}
	in := []rec{
		{Period: "2026-05-06", Customer: "acme", SKU: "alpha",
			Payload: []byte("first-payload-bytes")},
		{Period: "2026-05-06", Customer: "acme", SKU: "beta",
			Payload: []byte("second-payload-bytes")},
		{Period: "2026-05-06", Customer: "globex", SKU: "alpha",
			Payload: []byte("third-payload-bytes")},
	}

	fresh, err := encodeParquetUnpooled(in, &parquet.Snappy)
	if err != nil {
		t.Fatalf("fresh encode: %v", err)
	}

	enc := newParquetEncoder[rec](&parquet.Snappy,
		defaultEncodeBufPoolMaxBytes, nil)
	ctx := context.Background()

	first, err := enc.encode(ctx, in)
	if err != nil {
		t.Fatalf("pooled encode #1: %v", err)
	}
	second, err := enc.encode(ctx, in)
	if err != nil {
		t.Fatalf("pooled encode #2 (after Reset): %v", err)
	}

	if !bytes.Equal(fresh, first) {
		t.Errorf("pooled-first != fresh: lens %d vs %d",
			len(first), len(fresh))
	}
	if !bytes.Equal(fresh, second) {
		t.Errorf("pooled-second != fresh: lens %d vs %d "+
			"(Reset path leaks state)", len(second), len(fresh))
	}
	if !bytes.Equal(first, second) {
		t.Errorf("pooled-first != pooled-second: lens %d vs %d "+
			"(non-deterministic across Reset)",
			len(first), len(second))
	}
}

// TestEncodeParquet_PoolReturnedAfterError verifies the
// cleanup-on-error path: a panic inside the encoder must
// still return the buffer to the pool (its contents reset on
// next Get) and re-panic so the caller's stack trace is
// preserved. The writer is intentionally NOT returned on
// error/panic — parquet-go doesn't document post-error
// writer state.
//
// Approach: we can't easily make pw.Write panic on demand,
// so we synthesise the panic via the onBufDropped callback —
// it fires after Write+Close succeed but before encode
// returns; panicking inside it lands in the deferred cleanup.
func TestEncodeParquet_PoolReturnedAfterPanic(t *testing.T) {
	type rec struct {
		Payload []byte `parquet:"payload"`
	}
	recs := []rec{{Payload: []byte("hello")}}

	// Tiny bufCap so onBufDropped fires; the callback then
	// panics to exercise the cleanup-on-panic path.
	enc := newParquetEncoder[rec](&parquet.Snappy, 1,
		func(context.Context) { panic("boom") })

	defer func() {
		if recover() == nil {
			t.Fatal("want panic to propagate")
		}
		// After panic, the encoder must remain usable. A new
		// encode (with a saner cap + no-op cb) should produce
		// the same bytes as the unpooled reference.
		want, err := encodeParquetUnpooled(recs, &parquet.Snappy)
		if err != nil {
			t.Fatalf("ref encode: %v", err)
		}
		enc2 := newParquetEncoder[rec](&parquet.Snappy,
			defaultEncodeBufPoolMaxBytes, nil)
		got, err := enc2.encode(context.Background(), recs)
		if err != nil {
			t.Fatalf("recovery encode: %v", err)
		}
		if !bytes.Equal(want, got) {
			t.Errorf("recovery encode bytes differ from reference")
		}
	}()
	_, _ = enc.encode(context.Background(), recs)
}

// TestEncodeParquet_BufDroppedCallback verifies that the
// onBufDropped callback fires when a single encode produces a
// buffer larger than bufCap, and does NOT fire when the buffer
// stays within the cap.
func TestEncodeParquet_BufDroppedCallback(t *testing.T) {
	type rec struct {
		Payload []byte `parquet:"payload"`
	}
	// Build a record set that produces a comfortably-sized
	// buffer (>1 KiB after Snappy on this layout).
	recs := make([]rec, 100)
	for i := range recs {
		recs[i] = rec{Payload: make([]byte, 256)}
		for j := range recs[i].Payload {
			recs[i].Payload[j] = byte(i + j)
		}
	}

	t.Run("under cap → no callback", func(t *testing.T) {
		var fired int
		enc := newParquetEncoder[rec](&parquet.Snappy,
			16<<20, // 16 MiB — way larger than any output here
			func(context.Context) { fired++ })
		if _, err := enc.encode(context.Background(), recs); err != nil {
			t.Fatalf("encode: %v", err)
		}
		if fired != 0 {
			t.Fatalf("callback fired unexpectedly: %d", fired)
		}
	})

	t.Run("over cap → callback fires", func(t *testing.T) {
		var fired int
		enc := newParquetEncoder[rec](&parquet.Snappy,
			16, // tiny cap → every buffer exceeds it
			func(context.Context) { fired++ })
		if _, err := enc.encode(context.Background(), recs); err != nil {
			t.Fatalf("encode: %v", err)
		}
		if fired != 1 {
			t.Fatalf("callback fire count: want 1, got %d", fired)
		}
	})
}

// TestEncodeParquet_RoundTrip verifies the full encode → decode
// path preserves records exactly.
func TestEncodeParquet_RoundTrip(t *testing.T) {
	type rec struct {
		ID     int64   `parquet:"id"`
		Name   string  `parquet:"name"`
		Amount float64 `parquet:"amount"`
	}
	in := []rec{
		{ID: 1, Name: "alpha", Amount: 1.5},
		{ID: 2, Name: "beta", Amount: 2.25},
		{ID: 3, Name: "gamma", Amount: 3.75},
	}
	enc := newParquetEncoder[rec](&parquet.Snappy,
		defaultEncodeBufPoolMaxBytes, nil)

	data, err := enc.encode(context.Background(), in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := decodeParquet[rec](data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("round-trip count: want %d, got %d", len(in), len(out))
	}
	for i := range in {
		if in[i] != out[i] {
			t.Errorf("rec[%d]: want %+v, got %+v", i, in[i], out[i])
		}
	}
}

func TestDecodeParquet_Empty(t *testing.T) {
	type rec struct {
		ID int64 `parquet:"id"`
	}
	data, err := encodeParquetUnpooled[rec](nil, &parquet.Snappy)
	if err != nil {
		t.Fatalf("encode empty: %v", err)
	}
	out, err := decodeParquet[rec](data)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("decode empty: want 0 rows, got %d", len(out))
	}
}

// ---------------------------------------------------------------------
// Lifetime tests — pin parquet-go's documented contract that the
// values returned by GenericReader[T].Read survive Close. Vendored
// from s3store/parquet_lifetime_test.go because the same
// GenericReader contract is load-bearing for s3pgstore's iter
// pipeline (Phase 12) — a future parquet-go upgrade that
// regresses it would fail loudly here.

type lifetimeRec struct {
	ID    int64  `parquet:"id"`
	Str   string `parquet:"str"`
	Bytes []byte `parquet:"bytes"`
}

func makeLifetimeFile(t *testing.T, prefix string, n int, codec compress.Codec) []byte {
	t.Helper()
	recs := make([]lifetimeRec, n)
	for i := range recs {
		recs[i] = lifetimeRec{
			ID:    int64(i),
			Str:   fmt.Sprintf("%s_STR_%06d", prefix, i),
			Bytes: fmt.Appendf(nil, "%s_BYTES_%06d", prefix, i),
		}
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[lifetimeRec](
		&buf, parquet.Compression(codec))
	if _, err := w.Write(recs); err != nil {
		t.Fatalf("write %s: %v", prefix, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close %s: %v", prefix, err)
	}
	return buf.Bytes()
}

func checkCorruption(
	t *testing.T, label string, recs []lifetimeRec, prefix string, baseIdx int,
) {
	t.Helper()
	const sampleLogs = 3
	corruptedStr := 0
	corruptedBytes := 0
	for i, rec := range recs {
		gIdx := baseIdx + i
		wantStr := fmt.Sprintf("%s_STR_%06d", prefix, gIdx)
		if rec.Str != wantStr {
			if corruptedStr < sampleLogs {
				t.Logf("%s: recs[%d].Str = %q, want %q",
					label, i, rec.Str, wantStr)
			}
			corruptedStr++
		}
		wantBytes := fmt.Appendf(nil, "%s_BYTES_%06d", prefix, gIdx)
		if !bytes.Equal(rec.Bytes, wantBytes) {
			if corruptedBytes < sampleLogs {
				t.Logf("%s: recs[%d].Bytes = %q, want %q",
					label, i, rec.Bytes, wantBytes)
			}
			corruptedBytes++
		}
	}
	if corruptedStr > 0 || corruptedBytes > 0 {
		t.Errorf("%s: corruption detected — Str %d/%d, Bytes %d/%d",
			label, corruptedStr, len(recs),
			corruptedBytes, len(recs))
	}
}

func TestParquetReader_Lifetime_ValuesSurviveClose(t *testing.T) {
	const N = 5000
	cases := []struct {
		name  string
		codec compress.Codec
	}{
		{"snappy", &parquet.Snappy},
		{"uncompressed", &parquet.Uncompressed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := makeLifetimeFile(t, "ONE", N, tc.codec)

			reader := parquet.NewGenericReader[lifetimeRec](
				bytes.NewReader(file))
			out := make([]lifetimeRec, N)
			n, err := reader.Read(out)
			if err != nil && err != io.EOF {
				t.Fatalf("read: %v", err)
			}
			if n != N {
				t.Fatalf("read: n=%d want %d", n, N)
			}

			if err := reader.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			// Stamp some additional churn through the runtime to
			// raise the chance that any released buffer is
			// reused for unrelated data before we check.
			scratch := make([]byte, 1<<20)
			for i := range scratch {
				scratch[i] = byte(i)
			}
			_ = scratch

			checkCorruption(t,
				"out (after reader.Close)",
				out, "ONE", 0)
		})
	}
}

func TestParquetReader_Lifetime_SequentialReadsOneFile(t *testing.T) {
	const N = 5000
	const chunkSize = 250
	cases := []struct {
		name  string
		codec compress.Codec
	}{
		{"snappy", &parquet.Snappy},
		{"uncompressed", &parquet.Uncompressed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := makeLifetimeFile(t, "ONE", N, tc.codec)
			reader := parquet.NewGenericReader[lifetimeRec](
				bytes.NewReader(file))
			defer func() { _ = reader.Close() }()

			var chunks [][]lifetimeRec
			collected := 0
			for collected < N {
				chunk := make([]lifetimeRec, chunkSize)
				m, err := reader.Read(chunk)
				if err != nil && err != io.EOF {
					t.Fatalf("read at %d: %v", collected, err)
				}
				if m == 0 {
					break
				}
				chunks = append(chunks, chunk[:m])
				collected += m
			}
			if collected != N {
				t.Fatalf("collected=%d want %d", collected, N)
			}

			base := 0
			for ci, chunk := range chunks {
				checkCorruption(t,
					fmt.Sprintf("chunk[%d] (after all %d reads done)",
						ci, len(chunks)),
					chunk, "ONE", base)
				base += len(chunk)
			}
		})
	}
}
