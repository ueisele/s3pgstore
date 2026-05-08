package catalog

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// PendingWriteInsertSQL returns the INSERT statement for the
// orphan-tracking row that's INSERTed before the S3 PUT and
// DELETEd inside the catalog write transaction. The s3_key is
// the row's primary key; the caller already holds it (it was
// generated before this INSERT) and reuses it for the DELETE.
func (n Names) PendingWriteInsertSQL() string {
	return fmt.Sprintf(
		`INSERT INTO %s (s3_key) VALUES ($1)`,
		n.PendingWrites())
}

// PendingWriteDeleteSQL returns the DELETE statement for the
// orphan-tracking row, keyed by s3_key. Idempotent — DELETE on
// a missing row succeeds with rowcount=0.
func (n Names) PendingWriteDeleteSQL() string {
	return fmt.Sprintf(
		`DELETE FROM %s WHERE s3_key = $1`,
		n.PendingWrites())
}

// IdempotencyLookupSQL returns the SELECT statement that
// resolves an existing (partition_key, idempotency_token) pair
// to its WriteResult fields. Used by the write path's
// short-circuit check and by the public LookupByToken method.
//
// Only one row can match the partial UNIQUE index, so the
// caller's QueryRow is unambiguous. Returns the columns the
// public WriteResult struct cares about: file_id, s3_key,
// written_at_version, file_size, uncompressed_size,
// record_count.
func (n Names) IdempotencyLookupSQL() string {
	return fmt.Sprintf(
		`SELECT file_id, s3_key, written_at_version,
		        file_size, uncompressed_size, record_count
		FROM %s
		WHERE partition_key = $1 AND idempotency_token = $2`,
		n.Files())
}

// PartitionUpsertSQL returns the upsert SQL for the partition
// row. Keeps version monotonic by 1 per call:
// first INSERT lands at version=1, file_count=1; every
// subsequent call bumps version by 1, increments file_count,
// and refreshes updated_at.
//
// parts is the list of part_<n> column names ("part_charge_period",
// ...). They appear in the INSERT column list and the VALUES
// placeholder list; positional parameters are
// ($1=partition_key, $2=part_1, $3=part_2, ..., $N=part_N) and
// the returning new version comes back in the same statement.
//
// expectVersionZero=true gates the conflict branch on
// version=0 — used by WithExpectedVersion(0) which asserts
// "no writes yet." In v2.0 the only state matching is "row
// absent" (LockPartition uses pg_advisory_xact_lock and never
// touches the row), but the WHERE filter is kept for forward
// compatibility with future paths that might leave version=0
// rows.
//
// For WithExpectedVersion(N>0) callers go through
// PartitionUpdateOCCSQL instead — that's a pure UPDATE WHERE
// version=N, with no INSERT branch.
func (n Names) PartitionUpsertSQL(parts []string, expectVersionZero bool) string {
	cols := []string{"partition_key"}
	for _, p := range parts {
		cols = append(cols, PartColumnPrefix+p)
	}
	cols = append(cols, "version", "file_count")

	placeholders := make([]string, len(cols))
	for i := range len(cols) - 2 {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	// version=1 / file_count=1 are constants on the first-insert
	// path. ON CONFLICT swaps them in for the bump form below.
	placeholders[len(cols)-2] = "1"
	placeholders[len(cols)-1] = "1"

	conflict := `ON CONFLICT (partition_key) DO UPDATE SET
        version = ` + n.Partitions() + `.version + 1,
        file_count = ` + n.Partitions() + `.file_count + 1,
        updated_at = now()`

	if expectVersionZero {
		conflict += fmt.Sprintf(
			" WHERE %s.version = 0", n.Partitions())
	}

	return fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
%s
RETURNING version`,
		n.Partitions(),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		conflict,
	)
}

// PartitionUpdateOCCSQL is the pure-UPDATE form for
// WithExpectedVersion(N) where N > 0. There is no INSERT
// branch — a missing partition row fails with zero rows
// updated, which the caller translates to ErrVersionConflict.
//
// Positional parameters are ($1=partition_key, $2=expected
// version). part_<n> columns are NOT touched (they don't
// change across writes; the existing row already has them).
//
// Returns the new version (existing+1) so the caller can use
// it as written_at_version on the file row.
func (n Names) PartitionUpdateOCCSQL() string {
	return fmt.Sprintf(
		`UPDATE %s SET
        version = version + 1,
        file_count = file_count + 1,
        updated_at = now()
        WHERE partition_key = $1 AND version = $2
        RETURNING version`,
		n.Partitions())
}

// MVInsertSQL returns the INSERT ... ON CONFLICT DO NOTHING
// statement for a materialized-view table. Every column is part
// of the primary key (set-membership semantics), so a re-insert
// of the same tuple is a no-op; a tuple that differs in any
// column is a new row.
//
// Shape is UNNEST-based so one prepared statement covers an
// arbitrary number of rows without varying its SQL text — a
// single INSERT replaces the per-row round trips the writer
// would otherwise issue. Each column accepts a text[] array;
// PostgreSQL coerces text → declared type per column (all MV
// columns are TEXT NOT NULL per the catalog DDL, so the
// coercion is the identity).
//
// Parameters are ($1::text[]...$N::text[]) covering columns in
// declaration order; each array's length is the row count, and
// the i-th element of every array forms the i-th tuple.
// Identifiers are quoted via pgx.Identifier.Sanitize so
// reserved words / case-sensitive names round-trip correctly.
//
// Duplicate tuples within the input arrays are handled by ON
// CONFLICT DO NOTHING — the first occurrence inserts and any
// subsequent duplicates (whether vs an existing table row or a
// previously-inserted row in the same statement) are skipped.
func (n Names) MVInsertSQL(mvName string, columns []string) string {
	tbl := n.MV(mvName)

	cols := make([]string, len(columns))
	arrParams := make([]string, len(columns))
	for i, c := range columns {
		cols[i] = pgx.Identifier{c}.Sanitize()
		arrParams[i] = fmt.Sprintf("$%d::text[]", i+1)
	}
	colList := strings.Join(cols, ", ")

	return fmt.Sprintf(
		"INSERT INTO %s (%s) "+
			"SELECT * FROM unnest(%s) "+
			"ON CONFLICT (%s) DO NOTHING",
		tbl, colList, strings.Join(arrParams, ", "), colList)
}

// FilesInsertSQL returns the INSERT statement for the catalog
// row that anchors a written parquet file. The parameter list
// is positional in this order:
//
//	$1  partition_key
//	$2  s3_key
//	$3  written_at_version (the value returned by PartitionUpsertSQL)
//	$4  file_size (compressed parquet bytes on S3)
//	$5  uncompressed_size (sum of TotalUncompressedSize per column chunk)
//	$6  record_count
//	$7  idempotency_token (NULL when no token)
//	$8...  part_<n> columns (in PartitionKeyParts order)
//	then  ext_<n> columns (in ExtensionColumns order)
//
// Returns file_id so the caller can build a WriteResult with
// the assigned ID.
func (n Names) FilesInsertSQL(parts, exts []string) string {
	cols := []string{
		"partition_key", "s3_key", "written_at_version",
		"file_size", "uncompressed_size", "record_count",
		"idempotency_token",
	}
	for _, p := range parts {
		cols = append(cols, PartColumnPrefix+p)
	}
	for _, e := range exts {
		cols = append(cols, ExtColumnPrefix+e)
	}

	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	return fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) RETURNING file_id`,
		n.Files(),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
}
