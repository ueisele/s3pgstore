package main

// rebuild.go contains the catalog-rebuild logic. main.go is a
// thin env-config wrapper that calls Rebuild and reports the
// result. Splitting this way lets the integration test cover
// the rebuild path without invoking the binary itself.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"

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
type RebuildConfig struct {
	Pool              *pgxpool.Pool
	S3Client          *s3.Client
	Bucket            string
	S3Prefix          string // matches the writer's Config.Prefix
	SchemaName        string
	TablePrefix       string
	PartitionKeyParts []string
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
//   - written_at_version is computed as the per-partition row
//     index in (s3_key lex) order — 1, 2, 3, ... This preserves
//     CLAUDE.md's MAX(written_at_version) ==
//     partitions.version invariant for the rebuilt catalog.
//   - record_count is read from the parquet footer.
//   - file_size is the S3 object size.
//   - ext_<n>, idempotency_token are NULL — not recoverable
//     from S3 alone.
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
	keys, err := listAllKeys(ctx, cfg.S3Client, cfg.Bucket, dataPrefix)
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
		for i, s3Key := range g.s3Keys {
			version := int64(i + 1)
			ok, err := insertFileRow(ctx, cfg, names,
				s3Key, g.partitionKey, g.partValues, version)
			if err != nil {
				return res, fmt.Errorf("file %s: %w",
					s3Key, err)
			}
			if ok {
				filesAdded++
			}
		}
		res.FilesInserted += filesAdded

		// Partitions row reflects the FULL set of files for
		// this partition, so a re-run after partial failure
		// converges to the right (version, file_count)
		// regardless of where the previous run stopped.
		ok, err := upsertPartitionRow(ctx, cfg, names,
			g.partitionKey, g.partValues, len(g.s3Keys))
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
	case cfg.Bucket == "":
		return errors.New("rebuild: Bucket is required")
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

// insertFileRow builds the catalog file row for one S3 object:
// HEAD for size + LastModified; GET for footer (record count);
// INSERT with ON CONFLICT (s3_key) DO NOTHING so a partial-
// failure re-run skips already-inserted rows.
//
// Returns (true, nil) if a new row was inserted; (false, nil)
// if the row already existed.
func insertFileRow(
	ctx context.Context, cfg RebuildConfig,
	names catalog.Names,
	s3Key, partitionKey string, partValues []string,
	writtenAtVersion int64,
) (bool, error) {
	head, err := cfg.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return false, fmt.Errorf("HEAD: %w", err)
	}

	body, err := getObjectBytes(ctx, cfg.S3Client,
		cfg.Bucket, s3Key)
	if err != nil {
		return false, fmt.Errorf("GET: %w", err)
	}

	pf, err := parquet.OpenFile(
		bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return false, fmt.Errorf(
			"OpenFile (footer parse): %w", err)
	}
	recordCount := pf.NumRows()

	var fileSize int64
	if head.ContentLength != nil {
		fileSize = *head.ContentLength
	} else {
		fileSize = int64(len(body))
	}

	cols := []string{
		"partition_key", "s3_key", "written_at_version",
		"file_size", "record_count",
	}
	args := []any{
		partitionKey, s3Key, writtenAtVersion,
		fileSize, recordCount,
	}
	if head.LastModified != nil {
		cols = append(cols, "written_at")
		args = append(args, head.LastModified.UTC())
	}
	for i, p := range cfg.PartitionKeyParts {
		cols = append(cols, "part_"+p)
		args = append(args, partValues[i])
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
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// upsertPartitionRow writes the s3pgstore_partitions row for a
// reconstructed partition. version = file_count and matches
// the highest written_at_version assigned by the file-row loop,
// preserving CLAUDE.md's MAX(written_at_version) ==
// partitions.version invariant for the rebuilt catalog.
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
	partitionKey string, partValues []string, fileCount int,
) (bool, error) {
	cols := []string{"partition_key"}
	args := []any{partitionKey}
	for i, p := range cfg.PartitionKeyParts {
		cols = append(cols, "part_"+p)
		args = append(args, partValues[i])
	}
	cols = append(cols, "version", "file_count")
	args = append(args, int64(fileCount), fileCount)

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

// getObjectBytes reads an entire S3 object into memory. Used by
// the rebuild path to feed parquet.OpenFile a seekable reader.
// Memory cost is one file at a time — the rebuild loop is
// sequential.
func getObjectBytes(
	ctx context.Context, client *s3.Client,
	bucket, key string,
) ([]byte, error) {
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
