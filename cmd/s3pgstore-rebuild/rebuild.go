package main

// rebuild.go contains the catalog-rebuild logic. main.go is a
// thin env-config wrapper that calls Rebuild and reports the
// result. Splitting this way lets the integration test cover
// the rebuild path without invoking the binary itself.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ueisele/s3pgstore"
	"github.com/ueisele/s3pgstore/internal/catalog"
)

// RebuildConfig captures everything Rebuild needs to walk S3
// and reconstruct the catalog. SchemaName / TablePrefix mirror
// the writer's s3pgstore.Config and must match for the rebuilt
// rows to land in the operator's existing tables.
//
// PartitionKeyParts must match the writer's configuration —
// parsing the partition key out of an S3 object's path requires
// knowing how many segments to expect and what each is named.
//
// ExtensionColumns must match the writer's configuration so
// rebuild can recover ext_<name> values from S3 user-metadata
// (written by the writer's S3 PUT). Columns declared here that
// the original write didn't populate (no value in WithMetadata)
// stay NULL on the rebuilt row, matching the original catalog.
type RebuildConfig struct {
	Pool              *pgxpool.Pool
	S3Client          *s3.Client
	S3Bucket          string
	S3Prefix          string // matches the writer's Config.S3Prefix
	SchemaName        string
	TablePrefix       string
	PartitionKeyParts []string
	ExtensionColumns  []s3pgstore.ExtensionColumn
}

// RebuildResult captures the row counts produced by a Rebuild
// run, useful for operator-facing logging.
type RebuildResult struct {
	FilesInserted      int
	PartitionsInserted int
}

// Rebuild walks every parquet object under <S3Prefix>/data/
// and reconstructs s3pgstore_files + s3pgstore_partitions rows
// from the discovered objects.
//
// Behavior:
//
//   - feed_seq is left NULL — sequencer reassigns offsets when
//     it next runs.
//   - written_at is set from each object's S3 LastModified
//     timestamp (best available proxy; the original written_at
//     isn't recoverable from S3 alone).
//   - written_at_version is recovered from S3 user-metadata
//     (s3pgstore-version), preserving original version
//     continuity for callers using OCC across DR.
//   - record_count and uncompressed_size come from S3
//     user-metadata (s3pgstore-records /
//     s3pgstore-uncompressed). Missing values are a hard error
//     — every s3pgstore-written object carries them.
//   - file_size is the S3 object size (HEAD ContentLength).
//   - idempotency_token is recovered from s3pgstore-token
//     user-metadata when the original Write set one.
//   - ext_<n> values are recovered from s3pgstore-ext-<name>
//     user-metadata, parsed via s3pgstore.ParseExtFromS3 against
//     RebuildConfig.ExtensionColumns. Missing metadata leaves the
//     column NULL.
//
// Files lacking the s3pgstore user-metadata bag fail rebuild
// (likely uploaded outside the library or by a pre-metadata
// version). Operators must remove or re-upload such objects
// before re-running rebuild.
//
// Idempotent on re-run: ON CONFLICT (s3_key) DO NOTHING for
// files; partitions UPSERT to the recomputed (version,
// file_count). Operators can safely re-invoke after a partial
// failure.
//
// Materialized views are NOT rebuilt — MVs depend on T's
// record data and can't be reconstructed without re-reading
// every parquet body. Operators needing MVs after DR re-run
// their MV-population pipeline.
func Rebuild(
	ctx context.Context, cfg RebuildConfig,
) (RebuildResult, error) {
	if err := validateRebuildConfig(cfg); err != nil {
		return RebuildResult{}, err
	}
	names := catalog.NewNames(cfg.SchemaName, cfg.TablePrefix)

	dataPrefix := dataPrefixOf(cfg.S3Prefix)
	keys, err := listAllKeys(ctx, cfg.S3Client, cfg.S3Bucket, dataPrefix)
	if err != nil {
		return RebuildResult{}, fmt.Errorf("LIST: %w", err)
	}

	groups, err := groupByPartition(keys, dataPrefix,
		cfg.PartitionKeyParts)
	if err != nil {
		return RebuildResult{}, err
	}

	res := RebuildResult{}
	for _, g := range groups {
		// Per-partition lex order on the S3 key drives
		// written_at_version. Matches the deterministic
		// emission order CLAUDE.md asserts on read paths.
		sort.Strings(g.s3Keys)

		filesAdded := 0
		var maxVersion int64
		for i, s3Key := range g.s3Keys {
			fallback := int64(i + 1)
			version, ok, err := insertFileRow(ctx, cfg, names,
				s3Key, g.partitionKey, g.partValues, fallback)
			if err != nil {
				return res, fmt.Errorf("file %s: %w",
					s3Key, err)
			}
			if ok {
				filesAdded++
			}
			if version > maxVersion {
				maxVersion = version
			}
		}
		res.FilesInserted += filesAdded

		// Partitions row reflects the FULL set of files for
		// this partition, so a re-run after partial failure
		// converges to the right (version, file_count)
		// regardless of where the previous run stopped.
		// version = MAX(written_at_version) preserves
		// CLAUDE.md's MAX-equals-partition-version invariant
		// even when written_at_version values were recovered
		// from S3 metadata (which may not be 1..N if files
		// were ever deleted from the original catalog).
		ok, err := upsertPartitionRow(ctx, cfg, names,
			g.partitionKey, g.partValues,
			maxVersion, len(g.s3Keys))
		if err != nil {
			return res, fmt.Errorf("partition %s: %w",
				g.partitionKey, err)
		}
		if ok {
			res.PartitionsInserted++
		}
	}
	return res, nil
}

// validateRebuildConfig fails fast on the obvious mistakes —
// missing pool, missing S3, empty parts list — so a misconfigured
// run errors before we issue any LIST.
func validateRebuildConfig(cfg RebuildConfig) error {
	switch {
	case cfg.Pool == nil:
		return errors.New("rebuild: Pool is required")
	case cfg.S3Client == nil:
		return errors.New("rebuild: S3Client is required")
	case cfg.S3Bucket == "":
		return errors.New("rebuild: S3Bucket is required")
	case cfg.SchemaName == "":
		return errors.New("rebuild: SchemaName is required")
	case cfg.TablePrefix == "":
		return errors.New("rebuild: TablePrefix is required")
	case len(cfg.PartitionKeyParts) == 0:
		return errors.New("rebuild: PartitionKeyParts must be non-empty")
	}
	return nil
}

// dataPrefixOf returns the S3 prefix under which parquet bodies
// live for a Store with the given configured Prefix. Mirrors
// Store.dataPath.
func dataPrefixOf(s3Prefix string) string {
	if s3Prefix == "" {
		return "data/"
	}
	return s3Prefix + "/data/"
}

// listAllKeys pages through every object under prefix using
// ListObjectsV2 with the standard NextContinuationToken loop.
// Returns the full slice of *.parquet object keys; non-parquet
// suffixes are skipped silently (operator may have their own
// metadata files mixed in).
func listAllKeys(
	ctx context.Context, client *s3.Client,
	bucket, prefix string,
) ([]string, error) {
	var (
		out   []string
		token *string
	)
	for {
		resp, err := client.ListObjectsV2(ctx,
			&s3.ListObjectsV2Input{
				Bucket:            aws.String(bucket),
				Prefix:            aws.String(prefix),
				ContinuationToken: token,
			})
		if err != nil {
			return nil, err
		}
		for _, obj := range resp.Contents {
			if obj.Key == nil {
				continue
			}
			if !strings.HasSuffix(*obj.Key, ".parquet") {
				continue
			}
			out = append(out, *obj.Key)
		}
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		token = resp.NextContinuationToken
	}
	return out, nil
}

// partitionGroup holds the S3 keys belonging to a single
// reconstructed partition, plus the parsed part_<n> values.
type partitionGroup struct {
	partitionKey string
	partValues   []string
	s3Keys       []string
}

// groupByPartition extracts the partition_key suffix from each
// S3 object's path (strip dataPrefix; strip the trailing
// "<uuid>.parquet"), parses the part_<n> values, and groups by
// partition. Empty result is not an error — an empty bucket
// rebuilds an empty catalog.
func groupByPartition(
	keys []string, dataPrefix string, parts []string,
) ([]partitionGroup, error) {
	byKey := make(map[string]*partitionGroup)
	for _, k := range keys {
		if !strings.HasPrefix(k, dataPrefix) {
			continue
		}
		rest := k[len(dataPrefix):]
		// "<partition_key>/<uuid>.parquet" → split on last '/'.
		slash := strings.LastIndexByte(rest, '/')
		if slash <= 0 {
			continue
		}
		partKey := rest[:slash]
		values, err := catalog.PartitionKeyValues(partKey, parts)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", k, err)
		}
		g, ok := byKey[partKey]
		if !ok {
			g = &partitionGroup{
				partitionKey: partKey,
				partValues:   values,
			}
			byKey[partKey] = g
		}
		g.s3Keys = append(g.s3Keys, k)
	}

	out := make([]partitionGroup, 0, len(byKey))
	for _, g := range byKey {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].partitionKey < out[j].partitionKey
	})
	return out, nil
}

// insertFileRow builds the catalog file row for one S3 object.
// Recovery flow:
//
//  1. HEAD always — for ContentLength, LastModified, and the
//     user-metadata bag.
//  2. The metadata bag must carry the library's
//     s3pgstore-records / s3pgstore-uncompressed /
//     s3pgstore-version fields. If any are missing the file
//     wasn't written by s3pgstore (or was written by a version
//     predating the metadata convention) — return an error so
//     the operator can investigate.
//
// fallbackVersion is used as written_at_version when the S3
// metadata doesn't carry s3pgstore-version. Returns the
// version actually used (recovered or fallback) so the caller
// can compute MAX(version) for the partitions row.
//
// Returns (version, true, nil) if a new row was inserted;
// (version, false, nil) if the row already existed (re-run on
// partial-failure path).
func insertFileRow(
	ctx context.Context, cfg RebuildConfig,
	names catalog.Names,
	s3Key, partitionKey string, partValues []string,
	fallbackVersion int64,
) (int64, bool, error) {
	head, err := cfg.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return 0, false, fmt.Errorf("HEAD: %w", err)
	}

	var fileSize int64
	if head.ContentLength != nil {
		fileSize = *head.ContentLength
	}

	rec := newRecoveredFields(head.Metadata, cfg.ExtensionColumns,
		fallbackVersion)
	extValues, err := rec.parseExtValues(cfg.ExtensionColumns)
	if err != nil {
		return 0, false, err
	}

	if !rec.hasFooterFields() {
		return 0, false, fmt.Errorf(
			"s3 object %s: missing required user-metadata "+
				"(s3pgstore-records / s3pgstore-uncompressed) — "+
				"file was not written by s3pgstore or predates "+
				"the metadata convention", s3Key)
	}

	cols := []string{
		"partition_key", "s3_key", "written_at_version",
		"file_size", "uncompressed_size", "record_count",
	}
	args := []any{
		partitionKey, s3Key, rec.version,
		fileSize, rec.uncompressedSize, rec.recordCount,
	}
	if head.LastModified != nil {
		cols = append(cols, "written_at")
		args = append(args, head.LastModified.UTC())
	}
	if rec.token != "" {
		cols = append(cols, "idempotency_token")
		args = append(args, rec.token)
	}
	for i, p := range cfg.PartitionKeyParts {
		cols = append(cols, "part_"+p)
		args = append(args, partValues[i])
	}
	for _, c := range cfg.ExtensionColumns {
		v, ok := extValues[c.Name]
		if !ok {
			continue
		}
		cols = append(cols, "ext_"+c.Name)
		args = append(args, v)
	}

	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	q := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
		ON CONFLICT (s3_key) DO NOTHING`,
		names.Files(),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "))

	tag, err := cfg.Pool.Exec(ctx, q, args...)
	if err != nil {
		return 0, false, err
	}
	return rec.version, tag.RowsAffected() > 0, nil
}

// recoveredFields holds the library-controlled values pulled
// from a HEAD's user-metadata. token / extValues default to
// empty when the original write didn't set them; recordCount /
// uncompressedSize default to 0, signalling "metadata bag was
// missing required fields" via hasFooterFields below — the
// caller treats that as a hard error.
type recoveredFields struct {
	token            string
	recordCount      int64
	uncompressedSize int64
	version          int64
	rawExt           map[string]string
}

func newRecoveredFields(
	md map[string]string, exts []s3pgstore.ExtensionColumn,
	fallbackVersion int64,
) recoveredFields {
	rec := recoveredFields{
		version: fallbackVersion,
		rawExt:  make(map[string]string, len(exts)),
	}
	if v, ok := md[s3pgstore.S3MetaToken]; ok {
		rec.token = v
	}
	if v, ok := md[s3pgstore.S3MetaRecordCount]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			rec.recordCount = n
		}
	}
	if v, ok := md[s3pgstore.S3MetaUncompressedSize]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			rec.uncompressedSize = n
		}
	}
	if v, ok := md[s3pgstore.S3MetaWrittenAtVersion]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			rec.version = n
		}
	}
	for _, c := range exts {
		if v, ok := md[s3pgstore.S3MetaExtPrefix+c.Name]; ok {
			rec.rawExt[c.Name] = v
		}
	}
	return rec
}

// hasFooterFields reports whether the metadata bag carried the
// library's recordCount + uncompressedSize fields. Both are
// always non-zero on real s3pgstore writes, so a zero is the
// "metadata missing from this object" signal — the caller
// errors when this returns false.
func (r recoveredFields) hasFooterFields() bool {
	return r.recordCount > 0 && r.uncompressedSize > 0
}

// parseExtValues converts the raw S3-metadata strings to the
// Go-typed values pgx expects for each ExtensionColumn.
func (r recoveredFields) parseExtValues(
	exts []s3pgstore.ExtensionColumn,
) (map[string]any, error) {
	out := make(map[string]any, len(r.rawExt))
	for _, c := range exts {
		raw, ok := r.rawExt[c.Name]
		if !ok {
			continue
		}
		v, err := s3pgstore.ParseExtFromS3(c, raw)
		if err != nil {
			return nil, fmt.Errorf(
				"recover ext %q: %w", c.Name, err)
		}
		out[c.Name] = v
	}
	return out, nil
}

// upsertPartitionRow writes the s3pgstore_partitions row for a
// reconstructed partition. version = max(written_at_version)
// across the partition's recovered files, preserving CLAUDE.md's
// MAX(written_at_version) == partitions.version invariant. When
// every file's version was recovered from S3 metadata, this is
// the original partition.version; when the rebuild fell back to
// lex-order numbering, it equals the file_count.
//
// ON CONFLICT DO UPDATE so a re-run after partial failure
// recomputes (version, file_count) against the now-complete
// file set.
//
// Returns (true, nil) when a new row was inserted; (false, nil)
// when an existing row was updated. The caller treats these
// equivalently for the operator-facing summary count.
func upsertPartitionRow(
	ctx context.Context, cfg RebuildConfig,
	names catalog.Names,
	partitionKey string, partValues []string,
	maxVersion int64, fileCount int,
) (bool, error) {
	cols := []string{"partition_key"}
	args := []any{partitionKey}
	for i, p := range cfg.PartitionKeyParts {
		cols = append(cols, "part_"+p)
		args = append(args, partValues[i])
	}
	cols = append(cols, "version", "file_count")
	args = append(args, maxVersion, fileCount)

	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	q := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
		ON CONFLICT (partition_key) DO UPDATE SET
		    version    = EXCLUDED.version,
		    file_count = EXCLUDED.file_count,
		    updated_at = now()`,
		names.Partitions(),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "))

	tag, err := cfg.Pool.Exec(ctx, q, args...)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
