package s3pgstore

// Adapted from
// https://github.com/ueisele/s3store/blob/738a8bcbce870887833e158d4dc4e5116a29d4fc/parquet_bench_test.go.
// Copyright (c) 2024-2026 Uwe Eisele. MIT License.
//
// Adapted to s3pgstore: encoder is the standalone parquetEncoder
// type rather than (*Writer[T]).encodeParquet.
//
// Benchmarks for the encode + decode hot paths. The size buckets
// match the workload distribution captured in CLAUDE.md
// § Benchmarks. Run via:
//
//	go test -bench=. -benchmem -benchtime=2s -run=^$ -count=10 \
//	  -cpu=1 ./... | tee bench.txt
//
// -cpu=1 keeps sync.Pool measurements honest — at higher
// GOMAXPROCS the pool is per-P and a fixed b.N divides across
// multiple P slots, which understates per-P retention and
// confuses the allocs/op comparison between pool-empty and
// pool-warm paths. Real workloads with many concurrent writes
// still benefit; the bench just measures the baseline reuse
// pattern.

import (
	"context"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type benchRec struct {
	Period   string  `parquet:"period"`
	Customer string  `parquet:"customer"`
	SKU      string  `parquet:"sku"`
	Quantity int64   `parquet:"quantity"`
	Price    float64 `parquet:"price"`
	Notes    string  `parquet:"notes"`
}

// benchSizes targets the workload distribution captured in
// CLAUDE.md. Record counts chosen to roughly produce the named
// compressed-byte size after snappy + dictionary encoding.
//
//nolint:gochecknoglobals // bench fixture
var benchSizes = []struct {
	name string
	n    int
}{
	{"few_KB", 8},     // ~2 KB compressed
	{"100KB", 5000},   // ~115 KB
	{"1MB", 50000},    // ~1 MB
	{"10MB", 500000},  // ~10 MB
	{"20MB", 1000000}, // ~20 MB
}

func makeBenchRecs(n int) []benchRec {
	recs := make([]benchRec, n)
	for i := range recs {
		recs[i] = benchRec{
			Period:   fmt.Sprintf("2026-%02d-%02d", (i%12)+1, (i%28)+1),
			Customer: fmt.Sprintf("customer-%05d", i%1000),
			SKU:      fmt.Sprintf("sku-%06d", i%10000),
			Quantity: int64(i),
			Price:    float64(i) * 1.23,
			Notes:    fmt.Sprintf("benchmark record %d notes payload", i),
		}
	}
	return recs
}

// BenchmarkEncode_NoPool — what every encode looked like before
// the writer pool: fresh parquet.GenericWriter[T] + bytes.Buffer
// per call. Baseline for the pooled-vs-unpooled comparison.
func BenchmarkEncode_NoPool(b *testing.B) {
	for _, sc := range benchSizes {
		b.Run(sc.name, func(b *testing.B) {
			recs := makeBenchRecs(sc.n)
			data, _, err := encodeParquetUnpooled(recs, &parquet.Snappy)
			if err != nil {
				b.Fatalf("warmup encode: %v", err)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := encodeParquetUnpooled(
					recs, &parquet.Snappy); err != nil {
					b.Fatalf("encode: %v", err)
				}
			}
		})
	}
}

// BenchmarkEncode_Pooled — production path. Steady-state: pool
// is pre-warmed by the discarded warmup call, so every timed
// iteration hits the Reset-and-reuse branch.
func BenchmarkEncode_Pooled(b *testing.B) {
	for _, sc := range benchSizes {
		b.Run(sc.name, func(b *testing.B) {
			recs := makeBenchRecs(sc.n)
			enc := newParquetEncoder[benchRec](&parquet.Snappy,
				defaultEncodeBufPoolMaxBytes, nil)
			ctx := context.Background()
			data, _, err := enc.encode(ctx, recs)
			if err != nil {
				b.Fatalf("warmup encode: %v", err)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := enc.encode(ctx, recs); err != nil {
					b.Fatalf("encode: %v", err)
				}
			}
		})
	}
}

// BenchmarkDecode — read-side baseline. Reader pool is
// unimplemented (parquet-go GenericReader.Reset() doesn't take
// an io.Reader, see lifetime tests). This bench shows where
// decode-side time / allocs go.
func BenchmarkDecode(b *testing.B) {
	for _, sc := range benchSizes {
		b.Run(sc.name, func(b *testing.B) {
			recs := makeBenchRecs(sc.n)
			data, _, err := encodeParquetUnpooled(recs, &parquet.Snappy)
			if err != nil {
				b.Fatalf("encode fixture: %v", err)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := decodeParquet[benchRec](
					data); err != nil {
					b.Fatalf("decode: %v", err)
				}
			}
		})
	}
}
