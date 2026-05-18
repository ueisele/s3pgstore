// SQL builder helpers on the Names receiver. Each function
// returns the rendered statement (or SelectQuery) for a
// specific catalog operation; the Store / sequencer / GC
// packages cache the result in their own state and reuse it
// across calls — the rendering cost is paid once at
// construction time.
//
// File organization: helpers are grouped by the catalog table
// they touch (s3pgstore_files, s3pgstore_partitions,
// s3pgstore_pending_writes, s3pgstore_mv_<name>) and ordered
// within each section by lifecycle: INSERT → SELECT (read /
// observability) → UPDATE → DELETE. Cross-cutting helpers
// (SelectQuery) live at the top. New helpers should slot into
// their table's section in lifecycle order; don't append at
// the end.
//
// (Package doc lives in doc.go; this comment is intentionally
// a file note, not a package overview.)

package catalog

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// SelectQuery is a pre-rendered SELECT statement with a WHERE
// hole. The fixed parts (projection, table, ORDER BY) are baked
// once at construction time by catalog.Names helpers
// (FilesQuerySQL, MVLookupSQL); Render composes the final SQL
// with a caller-supplied WHERE expression.
//
// Using a typed builder rather than a fmt.Sprintf template
// avoids two hazards: a `string` template looks identical to a
// finished statement at the call site (easy to confuse), and a
// WHERE expression containing '%' would be interpreted as a
// format directive. Render is straight string concat — no
// format parsing, no '%' hazard.
type SelectQuery struct {
	head string // "SELECT <cols> FROM <table>"
	tail string // " ORDER BY ..." (leading space; may be empty)
}

// Render returns the final SELECT statement, splicing the
// WHERE expression between head and tail. Empty where → no
// WHERE clause emitted; non-empty where → " WHERE " + where is
// inserted between head and tail.
func (q SelectQuery) Render(where string) string {
	if where == "" {
		return q.head + q.tail
	}
	return q.head + " WHERE " + where + q.tail
}

// === s3pgstore_files ===

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
// Returns file_id and written_at so the caller can build a
// FileRef with the assigned ID and the server-side write
// timestamp (now() default).
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
		`INSERT INTO %s (%s) VALUES (%s) RETURNING file_id, written_at`,
		n.Files(),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
}

// FilesQuerySQL returns the SELECT template used by the read
// path's queryFileRows helper as a SelectQuery: projection
// (every FileRef column plus declared ext_<n> columns) and
// ORDER BY are fixed at New() time; the caller passes the per-
// call WHERE through SelectQuery.Render.
//
// Projection covers every FileRef field the catalog stores:
//
//	file_id, partition_key, s3_key, written_at_version,
//	written_at, file_size, uncompressed_size, record_count,
//	feed_seq, ext_<col1>, ext_<col2>, ...
//
// feed_seq is nullable (NULL until the sequencer assigns) and
// scanned into FileRef.Offset as OffsetNone on NULL. ORDER BY
// (partition_key, s3_key) is load-bearing for the read path's
// deterministic per-partition file order (see CLAUDE.md
// "Deterministic emission order").
func (n Names) FilesQuerySQL(exts []string) SelectQuery {
	cols := []string{
		"file_id", "partition_key", "s3_key", "written_at_version",
		"written_at", "file_size", "uncompressed_size",
		"record_count", "feed_seq",
	}
	for _, e := range exts {
		cols = append(cols, ExtColumnPrefix+e)
	}
	return SelectQuery{
		head: fmt.Sprintf("SELECT %s FROM %s",
			strings.Join(cols, ", "), n.Files()),
		tail: " ORDER BY partition_key, s3_key",
	}
}

// IdempotencyLookupSQL returns the SELECT statement that
// resolves an existing (partition_key, idempotency_token) pair
// to its FileRef fields. Used by the write path's
// short-circuit check and by the public LookupByToken method.
//
// Only one row can match the partial UNIQUE index, so the
// caller's QueryRow is unambiguous. Returns every column
// FileRef carries: file_id, partition_key, s3_key,
// written_at_version, written_at, file_size, uncompressed_size,
// record_count, feed_seq, and ext_<n> in declaration order.
// feed_seq is nullable; the caller scans into a *int64.
func (n Names) IdempotencyLookupSQL(exts []string) string {
	cols := []string{
		"file_id", "partition_key", "s3_key", "written_at_version",
		"written_at", "file_size", "uncompressed_size",
		"record_count", "feed_seq",
	}
	for _, e := range exts {
		cols = append(cols, ExtColumnPrefix+e)
	}
	return fmt.Sprintf(
		`SELECT %s
		FROM %s
		WHERE partition_key = $1 AND idempotency_token = $2`,
		strings.Join(cols, ", "), n.Files())
}

// PollFilesSQL returns the SELECT used by Poll / PollIter to
// fetch every sequenced FileRef in a half-open feed_seq window
// [$1, $2). ORDER BY feed_seq preserves commit-time emission
// order (load-bearing for Poll's "next-offset advancement"
// invariant per CLAUDE.md).
//
// Projection: file_id, feed_seq (positioned second so it scans
// into FileRef.Offset right after FileID), partition_key,
// s3_key, written_at_version, written_at, file_size,
// uncompressed_size, record_count, then each declared ext_<n>
// in ExtensionColumns order. feed_seq is NOT NULL on every
// returned row — the WHERE clause filters out unsequenced rows
// — so the caller scans into a plain int64.
func (n Names) PollFilesSQL(exts []string) string {
	cols := []string{
		"file_id", "feed_seq", "partition_key", "s3_key",
		"written_at_version", "written_at", "file_size",
		"uncompressed_size", "record_count",
	}
	for _, e := range exts {
		cols = append(cols, ExtColumnPrefix+e)
	}
	return fmt.Sprintf(
		`SELECT %s FROM %s
		WHERE feed_seq IS NOT NULL
		  AND feed_seq >= $1 AND feed_seq < $2
		ORDER BY feed_seq`,
		strings.Join(cols, ", "), n.Files())
}

// OffsetLatestSQL returns the SELECT used by Store.OffsetLatest:
// MAX(feed_seq) over sequenced rows, COALESCEd to 0 for the
// empty-catalog case so the caller can return OffsetEarliest
// (= MAX+1 = 1) without a NULL check.
func (n Names) OffsetLatestSQL() string {
	return fmt.Sprintf(
		`SELECT COALESCE(MAX(feed_seq), 0) FROM %s
		WHERE feed_seq IS NOT NULL`,
		n.Files())
}

// OffsetAtSQL returns the SELECT used by Store.OffsetAt: the
// smallest feed_seq whose feed_seq_at is at or after $1, or NULL
// when no sequenced row matches. The caller scans into a *int64
// and treats NULL as OffsetNone.
func (n Names) OffsetAtSQL() string {
	return fmt.Sprintf(
		`SELECT MIN(feed_seq) FROM %s
		WHERE feed_seq IS NOT NULL AND feed_seq_at >= $1`,
		n.Files())
}

// PollLagSQL returns the SELECT used by the
// s3pgstore.poll.lag observable gauge: seconds since the most
// recent feed_seq_at stamp on a sequenced row. Returns zero rows
// when no row is sequenced yet — the caller treats pgx.ErrNoRows
// as "no observation" rather than as a failure.
//
// ORDER BY feed_seq DESC LIMIT 1 hits the (feed_seq) WHERE
// feed_seq IS NOT NULL partial index for an O(1) backward index
// scan + one heap fetch, regardless of catalog size. The row
// with MAX(feed_seq) carries MAX(feed_seq_at) by the sequencer
// invariant: feed_seq is assigned monotonically under
// pg_advisory_xact_lock and feed_seq_at is set in the same
// statement, so the latest-numbered row also carries the latest
// timestamp. Refactors that weaken that serialization (parallel
// sequencers, deferred timestamping) would break this query.
func (n Names) PollLagSQL() string {
	return fmt.Sprintf(
		`SELECT EXTRACT(EPOCH FROM (now() - feed_seq_at))
		FROM %s WHERE feed_seq IS NOT NULL
		ORDER BY feed_seq DESC LIMIT 1`,
		n.Files())
}

// === s3pgstore_partitions ===

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

// === s3pgstore_pending_writes ===

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

// PendingWritesScanSQL returns the SELECT used by
// cmd/s3pgstore-gc to surface orphan candidates: every pending
// row whose intended_at is older than the cutoff ($1), up to
// LIMIT ($2), in ascending intended_at order so the oldest
// orphans reclaim first. Fully positional — no projection or
// ordering knobs.
func (n Names) PendingWritesScanSQL() string {
	return fmt.Sprintf(
		`SELECT s3_key FROM %s
		WHERE intended_at < $1
		ORDER BY intended_at
		LIMIT $2`,
		n.PendingWrites())
}

// PendingWritesDepthSQL returns the COUNT used by the
// s3pgstore.pending_writes.depth observable gauge.
// pending_writes is bounded by design (one row per failed
// write), so a full table scan is fine.
func (n Names) PendingWritesDepthSQL() string {
	return fmt.Sprintf(`SELECT COUNT(*) FROM %s`, n.PendingWrites())
}

// PendingWriteDeleteSQL returns the DELETE statement for the
// orphan-tracking row, keyed by s3_key. Idempotent — DELETE on
// a missing row succeeds with rowcount=0.
//
// Used both by the write path's catalog-tx cleanup (after a
// successful PUT) and by cmd/s3pgstore-gc's reclaimOne (after
// the S3 DELETE) — the byte-identical statement.
func (n Names) PendingWriteDeleteSQL() string {
	return fmt.Sprintf(
		`DELETE FROM %s WHERE s3_key = $1`,
		n.PendingWrites())
}

// === s3pgstore_mv_<name> ===

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

// MVLookupSQL returns the SELECT template used by
// MaterializedView.Lookup as a SelectQuery: projection
// (sanitized column list), table, and ORDER BY are fixed at the
// lookup handle's construction time; the caller passes the
// per-call WHERE through SelectQuery.Render.
//
// columns is the declared column tuple in MV-declaration order;
// callers should pass the same slice used by MVInsertSQL so the
// SELECT projection lines up with the INSERT column order
// (matters for MaterializedView.def.From, which receives values
// positionally).
//
// ORDER BY is unconditional because Config.validate rejects MVs
// with zero columns.
func (n Names) MVLookupSQL(mvName string, columns []string) SelectQuery {
	cols := make([]string, len(columns))
	for i, c := range columns {
		cols[i] = pgx.Identifier{c}.Sanitize()
	}
	colList := strings.Join(cols, ", ")
	return SelectQuery{
		head: fmt.Sprintf("SELECT %s FROM %s", colList, n.MV(mvName)),
		tail: " ORDER BY " + colList,
	}
}
