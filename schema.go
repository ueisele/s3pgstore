package s3pgstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/ueisele/s3pgstore/internal/catalog"
)

// RenderDDL returns the SQL needed to create every catalog
// table the configured Store[T] uses: s3pgstore_files,
// s3pgstore_partitions, s3pgstore_pending_writes, plus one
// table per declared MaterializedView. Output is a single
// string containing every CREATE TABLE / CREATE INDEX
// statement; each statement is independently idempotent (`IF
// NOT EXISTS`).
//
// Operators apply this through their migration tool in
// production. Tests and small deployments can use
// SchemaManager.Create which executes the same SQL via the
// configured Executor.
//
// Output is deterministic — same Config produces byte-identical
// SQL — so callers can checksum it for migration tracking.
func RenderDDL[T any](cfg Config[T]) (string, error) {
	if err := cfg.validate(); err != nil {
		return "", err
	}
	return catalog.RenderAll(ddlInputFromConfig(cfg))
}

// ddlInputFromConfig translates a validated Config[T] into the
// catalog package's DDLInput. The two representations are kept
// separate so the catalog package stays free of Go-generic
// types (it's imported by code that doesn't know T).
func ddlInputFromConfig[T any](cfg Config[T]) catalog.DDLInput {
	r := cfg.resolved()

	parts := make([]catalog.DDLPart, len(r.PartitionKeyParts))
	for i, p := range r.PartitionKeyParts {
		parts[i] = catalog.DDLPart{Name: p}
	}

	exts := make([]catalog.DDLExt, len(r.ExtensionColumns))
	for i, e := range r.ExtensionColumns {
		exts[i] = catalog.DDLExt{Name: e.Name, Type: e.Type}
	}

	mvs := make([]catalog.DDLMV, len(r.MaterializedViews))
	for i, mv := range r.MaterializedViews {
		mvs[i] = catalog.DDLMV{
			Name:         mv.Name,
			KeyColumns:   mv.KeyColumns,
			ValueColumns: mv.ValueColumns,
		}
	}

	return catalog.DDLInput{
		Names: catalog.NewNames(r.SchemaName, r.TablePrefix),
		Parts: parts,
		Exts:  exts,
		MVs:   mvs,
	}
}

// SchemaManager wraps RenderDDL and runs the SQL via the
// configured Executor. Designed for tests and small deployments
// — production should apply schema through the operator's
// migration tool.
//
// Every method is a separate transaction. Create is idempotent;
// Drop is destructive. Validate runs read-only
// information_schema queries.
type SchemaManager[T any] struct {
	cfg Config[T]
}

// NewSchemaManager returns a SchemaManager bound to cfg. The
// returned manager calls cfg.validate() on every method so
// configuration errors surface at the point of use, not at
// construction.
func NewSchemaManager[T any](cfg Config[T]) *SchemaManager[T] {
	return &SchemaManager[T]{cfg: cfg}
}

// Create applies the rendered DDL via cfg.Executor.Run. Every
// statement is idempotent (`IF NOT EXISTS`), so calling Create
// against a fully-populated database is a no-op.
func (m *SchemaManager[T]) Create(ctx context.Context) error {
	ddl, err := RenderDDL(m.cfg)
	if err != nil {
		return err
	}
	return m.cfg.Executor.Run(ctx, func(d DBTX) error {
		if _, err := d.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
		return nil
	})
}

// Drop removes every catalog table the configured Store[T]
// uses, plus all per-MV tables. Destructive. Intended for
// test cleanup; never run against production data.
//
// Each DROP is `IF EXISTS` so calling Drop against a
// half-populated database is safe.
func (m *SchemaManager[T]) Drop(ctx context.Context) error {
	if err := m.cfg.validate(); err != nil {
		return err
	}
	r := m.cfg.resolved()
	names := catalog.NewNames(r.SchemaName, r.TablePrefix)

	tables := make([]string, 0,
		3+len(m.cfg.MaterializedViews))
	for _, mv := range m.cfg.MaterializedViews {
		tables = append(tables, names.MV(mv.Name))
	}
	tables = append(tables,
		names.Files(),
		names.Partitions(),
		names.PendingWrites(),
	)

	return m.cfg.Executor.Run(ctx, func(d DBTX) error {
		for _, t := range tables {
			if _, err := d.Exec(ctx,
				fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", t),
			); err != nil {
				return fmt.Errorf("drop %s: %w", t, err)
			}
		}
		return nil
	})
}

// Validate queries information_schema and returns a structured
// error listing tables and columns that should exist (per the
// configured Config) but don't. Read-only — never modifies
// schema. Called by New() during Store[T] construction.
func (m *SchemaManager[T]) Validate(ctx context.Context) error {
	if err := m.cfg.validate(); err != nil {
		return err
	}
	r := m.cfg.resolved()

	expected := expectedSchema(r)

	var missing []string
	err := m.cfg.Executor.Run(ctx, func(d DBTX) error {
		for _, t := range expected.tables {
			ok, err := tableExists(ctx, d, r.SchemaName, t.name)
			if err != nil {
				return fmt.Errorf("check %s.%s: %w",
					r.SchemaName, t.name, err)
			}
			if !ok {
				missing = append(missing,
					fmt.Sprintf("table %q.%q", r.SchemaName, t.name))
				continue
			}
			cols, err := tableColumns(ctx, d, r.SchemaName, t.name)
			if err != nil {
				return fmt.Errorf("inspect columns of %s.%s: %w",
					r.SchemaName, t.name, err)
			}
			for _, want := range t.columns {
				if _, ok := cols[want]; !ok {
					missing = append(missing,
						fmt.Sprintf("column %q.%q.%q",
							r.SchemaName, t.name, want))
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return &SchemaValidationError{Missing: missing}
	}
	return nil
}

// SchemaValidationError lists tables and columns the runtime
// checks expected to find but didn't. Returned by
// SchemaManager.Validate and (transitively) New().
type SchemaValidationError struct {
	Missing []string
}

func (e *SchemaValidationError) Error() string {
	return fmt.Sprintf("schema validation failed: missing %s",
		strings.Join(e.Missing, ", "))
}

// expectedTable describes one table the Validate path expects:
// the bare table name and the unqualified columns each row
// must carry. Indexes are intentionally not validated — the
// library's correctness depends on column presence, not index
// shape (an operator may add or drop indexes for performance
// without breaking the runtime).
type expectedTable struct {
	name    string
	columns []string
}

type expectedSchemaSet struct {
	tables []expectedTable
}

func expectedSchema[T any](r Config[T]) expectedSchemaSet {
	names := catalog.NewNames(r.SchemaName, r.TablePrefix)

	filesCols := []string{
		"file_id", "partition_key", "s3_key", "feed_seq", "feed_seq_at",
		"written_at_version", "idempotency_token",
		"file_size", "record_count", "written_at",
	}
	for _, p := range r.PartitionKeyParts {
		filesCols = append(filesCols, catalog.PartColumnPrefix+p)
	}
	for _, e := range r.ExtensionColumns {
		filesCols = append(filesCols, catalog.ExtColumnPrefix+e.Name)
	}

	partitionsCols := []string{
		"partition_key", "version", "file_count", "updated_at",
	}
	for _, p := range r.PartitionKeyParts {
		partitionsCols = append(partitionsCols,
			catalog.PartColumnPrefix+p)
	}

	pwCols := []string{"s3_key", "intended_at"}

	out := expectedSchemaSet{
		tables: []expectedTable{
			{name: names.FilesBare(), columns: filesCols},
			{name: names.PartitionsBare(), columns: partitionsCols},
			{name: names.PendingWritesBare(), columns: pwCols},
		},
	}
	for _, mv := range r.MaterializedViews {
		mvCols := append(append([]string{}, mv.KeyColumns...),
			mv.ValueColumns...)
		out.tables = append(out.tables, expectedTable{
			name:    names.MVBare(mv.Name),
			columns: mvCols,
		})
	}
	return out
}

// tableExists returns true if the given table is present in
// the named schema. Uses information_schema.tables; no DDL.
func tableExists(ctx context.Context, d DBTX, schema, table string) (bool, error) {
	const q = `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = $1 AND table_name = $2
	)`
	var ok bool
	err := d.QueryRow(ctx, q, schema, table).Scan(&ok)
	return ok, err
}

// tableColumns returns the column-name set for the given
// table. Casefold is left to PostgreSQL — column names matched
// against expectations are produced by the same code that
// generated the DDL, so they share a casing convention.
func tableColumns(ctx context.Context, d DBTX, schema, table string) (map[string]struct{}, error) {
	const q = `SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2`
	rows, err := d.Query(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = struct{}{}
	}
	return out, rows.Err()
}
