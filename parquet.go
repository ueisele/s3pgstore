package s3pgstore

// Adapted from
// https://github.com/ueisele/s3store/blob/738a8bcbce870887833e158d4dc4e5116a29d4fc/writer_write.go
// (encodeParquet, the pooled (*Writer[T]).encodeParquet method)
// and reader_read.go (decodeParquet). Pool machinery follows
// writer.go's pqWriterPool / encodeBufPool / encodeBufPoolMaxBytes.
// Copyright (c) 2024-2026 Uwe Eisele. MIT License.
//
// Adapted to s3pgstore: package rename, CompressionCodec is an
// int enum here (vs string in upstream) so resolveCompression
// switches on iota constants, and the encoder is a standalone
// type (parquetEncoder) rather than a method on Writer[T] —
// s3pgstore's Store[T] composes it instead of inheriting it.
// The metric increment on dropped buffers is wired in Phase 16
// (currently a no-op closure injection point).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// defaultEncodeBufPoolMaxBytes is the fallback applied when
// Config.EncodeBufPoolMaxBytes is zero. 48 MiB covers parquet
// outputs from a few KB up to ~20 MiB with comfort, matching
// the common workload distribution. Workloads with regular
// ≥ 50 MiB writes should override the field — leaving it at
// the default makes those writes silently lose pool benefit.
//
// See CLAUDE.md § Benchmarks for the cap-tuning rationale.
const defaultEncodeBufPoolMaxBytes int = 48 << 20

// resolveCompression maps the user-facing CompressionCodec enum
// to the parquet-go codec instance used by the encode path.
// CompressionDefault → snappy, matching the ecosystem norm.
func resolveCompression(c CompressionCodec) (compress.Codec, error) {
	switch c {
	case CompressionDefault, CompressionSnappy:
		return &parquet.Snappy, nil
	case CompressionZstd:
		return &parquet.Zstd, nil
	case CompressionGzip:
		return &parquet.Gzip, nil
	case CompressionUncompressed:
		return &parquet.Uncompressed, nil
	}
	return nil, fmt.Errorf(
		"s3pgstore: unknown Compression %d (want default, snappy, "+
			"zstd, gzip, or uncompressed)", c)
}

// encodeParquetUnpooled writes records to a parquet byte stream
// using the given codec, with no pooling. Used by tests that
// construct parquet bytes outside of any encoder (varying codec
// or type per call) and as the byte-equivalence reference for
// the pooled encoder. The production write path goes through
// parquetEncoder.encode below.
func encodeParquetUnpooled[T any](
	records []T,
	codec compress.Codec,
) ([]byte, error) {
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[T](
		&buf, parquet.Compression(codec))
	if _, err := writer.Write(records); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// parquetEncoder owns the pooled parquet-go writer and output
// buffer for a single Store[T]. Codec is fixed at construction;
// the underlying *parquet.GenericWriter[T] is reused across
// encode calls via Reset.
//
// Lifetime: the returned []byte is a copy of the pooled
// buffer's bytes — nothing the caller holds points into pooled
// state. The pooled buffer is reset on next Get.
//
// Buffers grown beyond bufCap are dropped on return so a single
// huge encode doesn't balloon the pool permanently. The
// onBufDropped callback lets Phase 16 wire the
// s3pgstore.write.encode_buf_dropped counter without coupling
// this file to OTel; until then it's a no-op.
type parquetEncoder[T any] struct {
	codec  compress.Codec
	bufCap int

	pqWriterPool  sync.Pool
	encodeBufPool sync.Pool

	onBufDropped func(ctx context.Context)
}

// newParquetEncoder returns an encoder for T using codec. bufCap
// must be > 0; pass defaultEncodeBufPoolMaxBytes when no
// per-Config override applies. onBufDropped is optional (nil → no
// callback).
func newParquetEncoder[T any](
	codec compress.Codec,
	bufCap int,
	onBufDropped func(ctx context.Context),
) *parquetEncoder[T] {
	return &parquetEncoder[T]{
		codec:        codec,
		bufCap:       bufCap,
		onBufDropped: onBufDropped,
	}
}

// encode writes records to a parquet byte stream using the
// pooled writer + buffer. Both pools are sync.Pool-backed so a
// worker writing many files reuses the writer's column buffers
// / dictionary builders / compression scratch.
//
// The pooled-encoder byte-equivalence test (see
// parquet_pool_determinism_test.go) verifies that this method
// produces byte-identical output to encodeParquetUnpooled —
// load-bearing for WithIdempotencyToken correctness per
// CLAUDE.md.
func (e *parquetEncoder[T]) encode(
	ctx context.Context, records []T,
) ([]byte, error) {
	buf, _ := e.encodeBufPool.Get().(*bytes.Buffer)
	if buf == nil {
		buf = &bytes.Buffer{}
	} else {
		buf.Reset()
	}

	pw, _ := e.pqWriterPool.Get().(*parquet.GenericWriter[T])
	if pw == nil {
		pw = parquet.NewGenericWriter[T](
			buf, parquet.Compression(e.codec))
	} else {
		pw.Reset(buf)
	}

	if _, err := pw.Write(records); err != nil {
		return nil, err
	}
	if err := pw.Close(); err != nil {
		return nil, err
	}

	out := append([]byte(nil), buf.Bytes()...)

	e.pqWriterPool.Put(pw)
	if buf.Cap() <= e.bufCap {
		e.encodeBufPool.Put(buf)
	} else if e.onBufDropped != nil {
		e.onBufDropped(ctx)
	}
	return out, nil
}

// decodeParquet reads all rows of a parquet file into []T. T
// must be parquet-go-friendly (field-tagged, primitive-backed).
//
// Lifetime: the returned slice is a fresh allocation; no
// references into the input []byte remain after return. The
// parquet-go GenericReader's per-row buffers are released on
// Close.
func decodeParquet[T any](data []byte) ([]T, error) {
	reader := parquet.NewGenericReader[T](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

	total := reader.NumRows()
	if total == 0 {
		return nil, nil
	}

	out := make([]T, total)
	n, err := reader.Read(out)
	if err != nil && !errors.Is(err, io.EOF) {
		// parquet-go returns io.EOF at the end of the file;
		// treat that as a clean termination, not an error.
		return nil, err
	}
	return out[:n], nil
}
