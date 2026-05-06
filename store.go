package s3pgstore

import (
	"context"
	"fmt"

	"github.com/parquet-go/parquet-go/compress"

	"github.com/ueisele/s3pgstore/internal/catalog"
)

// Store is the typed entry point for writing and reading
// records. Construct via New[T]; New runs Config validation,
// constructs the parquet encoder, builds the S3 target, and
// validates the catalog schema via SchemaManager.Validate.
//
// All methods are safe for concurrent use.
type Store[T any] struct {
	cfg      Config[T]
	resolved Config[T]
	names    catalog.Names
	target   *s3target
	encoder  *parquetEncoder[T]
}

// New constructs a Store[T] for cfg. Validates cfg, resolves
// defaults, validates the catalog schema (via
// SchemaManager.Validate) so DDL drift is caught before the
// first write rather than at first INSERT, and constructs the
// parquet encoder + S3 target.
//
// Returns *SchemaValidationError if the catalog schema doesn't
// match cfg. Operators apply schema via their migration tool
// (or SchemaManager.Create for tests) before calling New.
func New[T any](ctx context.Context, cfg Config[T]) (*Store[T], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	r := cfg.resolved()

	codec, err := resolveCompression(r.Compression)
	if err != nil {
		return nil, err
	}
	bufCap := r.EncodeBufPoolMaxBytes
	if bufCap == 0 {
		bufCap = defaultEncodeBufPoolMaxBytes
	}

	target, err := newS3Target(s3TargetConfig{
		S3Client: r.S3Client,
		Bucket:   r.Bucket,
		Prefix:   r.Prefix,
	})
	if err != nil {
		return nil, err
	}

	if err := NewSchemaManager(cfg).Validate(ctx); err != nil {
		return nil, err
	}

	return &Store[T]{
		cfg:      cfg,
		resolved: r,
		names:    catalog.NewNames(r.SchemaName, r.TablePrefix),
		target:   target,
		encoder: newParquetEncoder[T](codec, bufCap, func(ctx context.Context) {
			// Phase 16 wires the encode_buf_dropped counter
			// here. Currently a no-op so the field is non-nil
			// for tests that exercise the cap-discard path.
		}),
	}, nil
}

// dataPath returns the S3-key prefix under which parquet data
// files live for this Store: "<Prefix>/data". A leading slash
// from cfg.Prefix is preserved verbatim — the library does not
// normalise prefixes.
func (s *Store[T]) dataPath() string {
	if s.resolved.Prefix == "" {
		return "data"
	}
	return s.resolved.Prefix + "/data"
}

// dataKey returns the S3 key for a single parquet file under
// partitionKey + uuid: "<Prefix>/data/<partitionKey>/<uuid>.parquet".
func (s *Store[T]) dataKey(partitionKey, uuid string) string {
	return s.dataPath() + "/" + partitionKey + "/" + uuid + ".parquet"
}

// codecForRead returns the compression codec we'd resolve given
// resolved.Compression. Used internally for the lifetime test
// path; kept private.
//
//nolint:unused // kept for future Phase 6 use
func (s *Store[T]) codecForRead() (compress.Codec, error) {
	return resolveCompression(s.resolved.Compression)
}

// validatePartitionKey runs the user's PartitionKeyOf and
// resolves it to per-part values via partitionKeyValues so the
// upstream contract failure ("PartitionKeyOf returned a key
// that doesn't match PartitionKeyParts") surfaces here, before
// any S3 PUT. Determinism / shape are caller responsibility per
// CLAUDE.md — the library validates the shape we observe on
// every call.
func (s *Store[T]) validatePartitionKey(rec T) (string, []string, error) {
	key := s.resolved.PartitionKeyOf(rec)
	values, err := partitionKeyValues(key, s.resolved.PartitionKeyParts)
	if err != nil {
		return "", nil, fmt.Errorf("PartitionKeyOf: %w", err)
	}
	return key, values, nil
}
