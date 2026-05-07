// Command s3pgstore-rebuild reconstructs the s3pgstore catalog
// from S3 alone — disaster-recovery tool. Walks <prefix>/data/
// via S3 LIST (the only place the library uses LIST), reads
// each Parquet file's footer for record_count, and INSERTs
// catalog rows.
//
// Environment variables:
//
//	S3PGSTORE_DATABASE_URL          PostgreSQL DSN (required)
//	S3PGSTORE_BUCKET                S3 bucket (required)
//	S3PGSTORE_S3_PREFIX             S3 prefix (default "")
//	S3PGSTORE_S3_ENDPOINT           Optional non-AWS endpoint
//	S3PGSTORE_S3_REGION             AWS region (default "us-east-1")
//	S3PGSTORE_SCHEMA                Schema name (default "public")
//	S3PGSTORE_TABLE_PREFIX          Table prefix (default "s3pgstore_")
//	S3PGSTORE_PARTITION_KEY_PARTS   Comma-separated parts (required)
//
// Reads catalog DDL must already be applied (operators run
// SchemaManager.Create or their migration tool first); the
// tool is a row-level rebuild, not a schema-creating tool.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		slog.Error("s3pgstore-rebuild exited with error",
			"err", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := os.Getenv("S3PGSTORE_DATABASE_URL")
	if dsn == "" {
		return errors.New("S3PGSTORE_DATABASE_URL is required")
	}
	bucket := os.Getenv("S3PGSTORE_BUCKET")
	if bucket == "" {
		return errors.New("S3PGSTORE_BUCKET is required")
	}
	partsRaw := os.Getenv("S3PGSTORE_PARTITION_KEY_PARTS")
	if partsRaw == "" {
		return errors.New(
			"S3PGSTORE_PARTITION_KEY_PARTS is required " +
				"(comma-separated, must match the writer's " +
				"PartitionKeyParts)")
	}
	parts := strings.Split(partsRaw, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}

	cfg := RebuildConfig{
		Bucket:            bucket,
		S3Prefix:          os.Getenv("S3PGSTORE_S3_PREFIX"),
		SchemaName:        getenvOr("S3PGSTORE_SCHEMA", "public"),
		TablePrefix:       getenvOr("S3PGSTORE_TABLE_PREFIX", "s3pgstore_"),
		PartitionKeyParts: parts,
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open pgxpool: %w", err)
	}
	defer pool.Close()
	cfg.Pool = pool

	s3Client, err := loadS3Client(ctx)
	if err != nil {
		return err
	}
	cfg.S3Client = s3Client

	slog.Info("s3pgstore-rebuild starting",
		"schema", cfg.SchemaName,
		"prefix", cfg.TablePrefix,
		"bucket", cfg.Bucket,
		"s3_prefix", cfg.S3Prefix,
		"partition_key_parts", cfg.PartitionKeyParts)

	res, err := Rebuild(ctx, cfg)
	if err != nil {
		return err
	}
	slog.Info("s3pgstore-rebuild complete",
		"files_inserted", res.FilesInserted,
		"partitions_inserted", res.PartitionsInserted)
	return nil
}

func loadS3Client(ctx context.Context) (*s3.Client, error) {
	region := getenvOr("S3PGSTORE_S3_REGION", "us-east-1")
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	endpoint := os.Getenv("S3PGSTORE_S3_ENDPOINT")
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	}), nil
}

func getenvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
