package catalog

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// DDLPart describes the partition-key column set for DDL
// rendering. Each entry produces one part_<name> column on
// s3pgstore_files and s3pgstore_partitions.
type DDLPart struct {
	Name string
}

// DDLExt describes one extension column for DDL rendering.
// Each entry produces one ext_<name> column on
// s3pgstore_files. Type is one of the allowed PostgreSQL types
// validated by the root package's Config check.
type DDLExt struct {
	Name string
	Type string
}

// DDLMV describes one materialized-view table for DDL
// rendering. Each entry produces an s3pgstore_mv_<name> table.
// KeyColumns and ValueColumns hold simple identifiers; both
// land as TEXT columns (the library writes string-form values
// through MaterializedViewDef.Of).
type DDLMV struct {
	Name         string
	KeyColumns   []string
	ValueColumns []string
}

// DDLInput is everything RenderAll needs. Callers pass a
// validated Names + the project-derived part / ext / MV
// definitions.
type DDLInput struct {
	Names Names
	Parts []DDLPart
	Exts  []DDLExt
	MVs   []DDLMV
}

// RenderAll returns the complete DDL for the s3pgstore
// catalog. Output is a single string containing every CREATE
// TABLE / CREATE INDEX statement; statements are separated by
// blank lines and each is independently idempotent (`IF NOT
// EXISTS`).
//
// Apply via the operator's migration tool in production; the
// root-package SchemaManager.Create wraps a single Exec for
// tests and small deployments. Either way the output is
// deterministic — same input yields byte-identical SQL — so a
// caller can checksum it for migration tracking.
func RenderAll(in DDLInput) (string, error) {
	if err := validateDDLInput(in); err != nil {
		return "", fmt.Errorf("RenderAll: %w", err)
	}

	var sb strings.Builder
	if err := renderFiles(&sb, in); err != nil {
		return "", err
	}
	sb.WriteString("\n")
	if err := renderPartitions(&sb, in); err != nil {
		return "", err
	}
	sb.WriteString("\n")
	renderPendingWrites(&sb, in)
	for _, mv := range in.MVs {
		sb.WriteString("\n")
		renderMV(&sb, in.Names, mv)
	}
	return sb.String(), nil
}

func validateDDLInput(in DDLInput) error {
	if in.Names.Schema == "" {
		return errors.New("names.Schema is empty")
	}
	if in.Names.Prefix == "" {
		return errors.New("names.Prefix is empty")
	}
	if len(in.Parts) == 0 {
		return errors.New("parts must be non-empty")
	}
	return nil
}

func renderFiles(sb *strings.Builder, in DDLInput) error {
	files := in.Names.Files()
	fmt.Fprintf(sb, "CREATE TABLE IF NOT EXISTS %s (\n", files)
	// Identity
	sb.WriteString("    file_id              BIGSERIAL PRIMARY KEY,\n")
	sb.WriteString("    s3_key               TEXT NOT NULL UNIQUE,\n")
	// Partition membership
	sb.WriteString("    partition_key        TEXT NOT NULL,\n")
	for _, p := range in.Parts {
		fmt.Fprintf(sb, "    %s%s            TEXT NOT NULL,\n",
			PartColumnPrefix, p.Name)
	}
	// File metadata
	sb.WriteString("    file_size            BIGINT NOT NULL,\n")
	sb.WriteString("    record_count         BIGINT,\n")
	// Versioning
	sb.WriteString("    written_at_version   BIGINT,\n")
	sb.WriteString("    written_at           TIMESTAMPTZ NOT NULL DEFAULT now(),\n")
	// Idempotency
	sb.WriteString("    idempotency_token    TEXT,\n")
	// Sequencer / stream
	sb.WriteString("    feed_seq             BIGINT UNIQUE,\n")
	sb.WriteString("    feed_seq_at          TIMESTAMPTZ")
	for _, e := range in.Exts {
		fmt.Fprintf(sb, ",\n    %s%s         %s",
			ExtColumnPrefix, e.Name, strings.ToUpper(e.Type))
	}
	sb.WriteString("\n);\n")

	// Indexes. Names are mirror-suffixed off the bare table
	// name so multiple s3pgstore deployments in the same DB
	// (different prefixes) don't collide.
	bare := in.Names.FilesBare()
	fmt.Fprintf(sb,
		"CREATE INDEX IF NOT EXISTS %s_partition_written_idx "+
			"ON %s (partition_key, written_at);\n",
		bare, files)
	fmt.Fprintf(sb,
		"CREATE INDEX IF NOT EXISTS %s_feed_seq_idx "+
			"ON %s (feed_seq) WHERE feed_seq IS NOT NULL;\n",
		bare, files)
	fmt.Fprintf(sb,
		"CREATE INDEX IF NOT EXISTS %s_seq_scan_idx "+
			"ON %s (written_at, file_id) WHERE feed_seq IS NULL;\n",
		bare, files)

	// Per-part composite index for filter-translated reads.
	if len(in.Parts) > 0 {
		cols := make([]string, len(in.Parts))
		for i, p := range in.Parts {
			cols[i] = PartColumnPrefix + p.Name
		}
		fmt.Fprintf(sb,
			"CREATE INDEX IF NOT EXISTS %s_part_idx ON %s (%s);\n",
			bare, files, strings.Join(cols, ", "))
	}

	// Partial UNIQUE for idempotency-token dedup. Idempotency
	// is per-partition, so the index covers (partition_key,
	// idempotency_token).
	fmt.Fprintf(sb,
		"CREATE UNIQUE INDEX IF NOT EXISTS %s_token_idx "+
			"ON %s (partition_key, idempotency_token) "+
			"WHERE idempotency_token IS NOT NULL;\n",
		bare, files)

	return nil
}

func renderPartitions(sb *strings.Builder, in DDLInput) error {
	parts := in.Names.Partitions()
	fmt.Fprintf(sb, "CREATE TABLE IF NOT EXISTS %s (\n", parts)
	sb.WriteString("    partition_key   TEXT PRIMARY KEY,\n")
	for _, p := range in.Parts {
		fmt.Fprintf(sb, "    %s%s       TEXT NOT NULL,\n",
			PartColumnPrefix, p.Name)
	}
	sb.WriteString("    version         BIGINT NOT NULL DEFAULT 0,\n")
	sb.WriteString("    file_count      INT NOT NULL DEFAULT 0,\n")
	sb.WriteString("    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()\n")
	sb.WriteString(");\n")

	if len(in.Parts) > 0 {
		bare := in.Names.PartitionsBare()
		cols := make([]string, len(in.Parts))
		for i, p := range in.Parts {
			cols[i] = PartColumnPrefix + p.Name
		}
		fmt.Fprintf(sb,
			"CREATE INDEX IF NOT EXISTS %s_part_idx ON %s (%s);\n",
			bare, parts, strings.Join(cols, ", "))
	}
	return nil
}

func renderPendingWrites(sb *strings.Builder, in DDLInput) {
	pw := in.Names.PendingWrites()
	fmt.Fprintf(sb, "CREATE TABLE IF NOT EXISTS %s (\n", pw)
	sb.WriteString("    s3_key       TEXT PRIMARY KEY,\n")
	sb.WriteString("    intended_at  TIMESTAMPTZ NOT NULL DEFAULT now()\n")
	sb.WriteString(");\n")

	bare := in.Names.PendingWritesBare()
	fmt.Fprintf(sb,
		"CREATE INDEX IF NOT EXISTS %s_intended_idx "+
			"ON %s (intended_at);\n",
		bare, pw)
}

func renderMV(sb *strings.Builder, names Names, mv DDLMV) {
	tbl := names.MV(mv.Name)
	fmt.Fprintf(sb, "CREATE TABLE IF NOT EXISTS %s (\n", tbl)

	lines := make([]string, 0, len(mv.KeyColumns)+len(mv.ValueColumns)+1)
	for _, kc := range mv.KeyColumns {
		lines = append(lines, fmt.Sprintf("    %s TEXT NOT NULL",
			pgx.Identifier{kc}.Sanitize()))
	}
	for _, vc := range mv.ValueColumns {
		lines = append(lines, fmt.Sprintf("    %s TEXT",
			pgx.Identifier{vc}.Sanitize()))
	}
	quoted := make([]string, len(mv.KeyColumns))
	for i, kc := range mv.KeyColumns {
		quoted[i] = pgx.Identifier{kc}.Sanitize()
	}
	lines = append(lines,
		fmt.Sprintf("    PRIMARY KEY (%s)", strings.Join(quoted, ", ")))

	sb.WriteString(strings.Join(lines, ",\n"))
	sb.WriteString("\n);\n")
}
