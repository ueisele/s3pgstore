package s3pgstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// MaterializedViewLookupDef binds a typed lookup against an MV
// declared in Config.MaterializedViews. NewMaterializedView
// validates that Name matches a declared MV and that Columns
// line up with the declared shape; schema drift is caught at
// construction, not at first lookup.
//
// From projects the column tuple onto K. The callback receives
// one slice per row whose length is len(Columns); positions
// match the declaration order. Returning an error from From
// propagates out of Lookup with the row's index.
type MaterializedViewLookupDef[K any] struct {
	Name    string
	Columns []string
	From    func(values []string) (K, error)
}

// MaterializedView is a typed lookup handle bound to a single
// MV declared on the Store. Construct via NewMaterializedView.
// Safe for concurrent use.
type MaterializedView[K any] struct {
	def      MaterializedViewLookupDef[K]
	executor Executor
	tableSQL string
	cols     []string // quoted SQL identifiers in select order
}

// NewMaterializedView returns a typed lookup handle for the MV
// named in def. Validates that:
//
//   - def.Name corresponds to a declared MV in Config.MaterializedViews;
//   - def.Columns matches the declaration verbatim (same length,
//     same names, same order — the MV table's column order is
//     the declaration order, and the lookup must mirror it so
//     the SELECT list and From callback stay aligned);
//   - def.From is non-nil.
//
// Schema-shape drift (column added in one place, missing in the
// other) fails here rather than at first Lookup, where the
// error would surface as a SELECT failure with no useful
// pointer to the misconfiguration.
func NewMaterializedView[T, K any](
	store *Store[T], def MaterializedViewLookupDef[K],
) (*MaterializedView[K], error) {
	declared, ok := findDeclaredMV(store, def.Name)
	if !ok {
		return nil, fmt.Errorf(
			"NewMaterializedView: %q is not declared on the "+
				"Store's Config.MaterializedViews", def.Name)
	}

	if !equalSliceCase(def.Columns, declared.Columns) {
		return nil, fmt.Errorf(
			"NewMaterializedView %q: Columns mismatch "+
				"(lookup: %v, declared: %v)",
			def.Name, def.Columns, declared.Columns)
	}
	if def.From == nil {
		return nil, fmt.Errorf(
			"NewMaterializedView %q: From is required", def.Name)
	}

	cols := make([]string, len(def.Columns))
	for i, c := range def.Columns {
		cols[i] = pgx.Identifier{c}.Sanitize()
	}

	return &MaterializedView[K]{
		def:      def,
		executor: store.cfg.Executor,
		tableSQL: store.names.MV(def.Name),
		cols:     cols,
	}, nil
}

// Lookup returns every tuple matching filters projected through
// def.From. Filters are the same PartitionFilter type as Read,
// but resolved against the MV's bare column names (not part_<n>).
// Empty filters returns every row.
//
// Tuples are returned in column-tuple order — the MV's primary
// key is the full column tuple, so this is also the natural
// emission order for set-membership semantics. The project
// callback (def.From) runs once per row in result order; an
// error from From terminates Lookup with the row's position.
//
// Set-membership semantics: a row's presence means "this tuple
// was observed by some Write". A row's absence means "no Write
// ever emitted this tuple". Duplicates are collapsed at write
// time via the all-columns primary key, so each distinct tuple
// appears exactly once.
func (m *MaterializedView[K]) Lookup(
	ctx context.Context, filters []PartitionFilter,
) ([]K, error) {
	where, args, err := translateFilters(filters,
		mvColResolver(m.def.Columns))
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(m.cols, ", "), m.tableSQL)
	if where != "" {
		q += " WHERE " + where
	}
	if len(m.cols) > 0 {
		q += " ORDER BY " + strings.Join(m.cols, ", ")
	}

	var out []K
	err = m.executor.Run(ctx, func(d DBTX) error {
		rows, err := d.Query(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		i := 0
		for rows.Next() {
			values := make([]string, len(m.def.Columns))
			scanArgs := make([]any, len(values))
			for j := range values {
				scanArgs[j] = &values[j]
			}
			if err := rows.Scan(scanArgs...); err != nil {
				return err
			}
			k, err := m.def.From(values)
			if err != nil {
				return fmt.Errorf("from row %d: %w", i, err)
			}
			out = append(out, k)
			i++
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("MV %q Lookup: %w", m.def.Name, err)
	}
	return out, nil
}

// findDeclaredMV looks up an MV by name in the Store's config,
// returning the column-shape-only projection used by the
// validator (Of closure dropped along with the type parameter
// T so the validator operates on it generically).
func findDeclaredMV[T any](
	s *Store[T], name string,
) (mvShape, bool) {
	for _, m := range s.cfg.MaterializedViews {
		if m.Name == name {
			return mvShape{
				Name:    m.Name,
				Columns: m.Columns,
			}, true
		}
	}
	return mvShape{}, false
}

type mvShape struct {
	Name    string
	Columns []string
}

// equalSliceCase compares two string slices element-wise with
// case-insensitive matching. Identifiers in PostgreSQL fold to
// lowercase by default, so an MV declared as Columns:
// ["Customer"] and a lookup using ["customer"] target the same
// physical column — the validator should accept both shapes.
func equalSliceCase(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

// mvWriteRows holds the per-MV tuple data resolved during one
// writePartition invocation. Each entry pairs an MV's name with
// the tuples produced by its Of closure, ready to INSERT inside
// the catalog tx.
//
// Build order matches Config.MaterializedViews, so iteration
// emits MVs in declaration order — deterministic across runs.
type mvWriteRows struct {
	name    string
	columns []string
	rows    [][]string
}

// resolveMVRows runs every declared MV's Of closure against
// records and returns the per-MV tuple sets. Validates that
// each emitted tuple's length matches len(Columns) — shape
// errors surface here, before the catalog tx opens.
func (s *Store[T]) resolveMVRows(records []T) ([]mvWriteRows, error) {
	if len(s.cfg.MaterializedViews) == 0 {
		return nil, nil
	}
	out := make([]mvWriteRows, 0, len(s.cfg.MaterializedViews))
	for _, mv := range s.cfg.MaterializedViews {
		rows := [][]string{}
		for i, rec := range records {
			emitted, err := mv.Of(rec)
			if err != nil {
				return nil, fmt.Errorf(
					"MV %q Of(records[%d]): %w",
					mv.Name, i, err)
			}
			for j, r := range emitted {
				if len(r) != len(mv.Columns) {
					return nil, fmt.Errorf(
						"MV %q row %d (record %d): tuple len "+
							"%d != Columns len %d",
						mv.Name, j, i,
						len(r), len(mv.Columns))
				}
				rows = append(rows, r)
			}
		}
		out = append(out, mvWriteRows{
			name:    mv.Name,
			columns: mv.Columns,
			rows:    rows,
		})
	}
	return out, nil
}

// insertMVRows runs INSERT ... ON CONFLICT DO NOTHING for every
// MV tuple resolved by resolveMVRows. Called inside the catalog
// tx so MV state stays consistent with file state under both
// commit and rollback.
//
// Set-membership semantics: same tuple seen N times is one row;
// distinct tuples accumulate. The PK over all columns lets the
// conflict path collapse no-op writes without a per-row IS
// DISTINCT FROM check.
//
// Empty rows slice is a no-op — the SQL is built per tuple, so
// an MV that emitted nothing for this batch produces no
// statements.
func (s *Store[T]) insertMVRows(
	ctx context.Context, d DBTX, mvs []mvWriteRows,
) error {
	for _, mv := range mvs {
		if len(mv.rows) == 0 {
			continue
		}
		insertSQL := s.names.MVInsertSQL(mv.name, mv.columns)
		for i, r := range mv.rows {
			args := make([]any, len(r))
			for j, v := range r {
				args[j] = v
			}
			if _, err := d.Exec(ctx, insertSQL, args...); err != nil {
				return fmt.Errorf("MV %q row %d: %w",
					mv.name, i, err)
			}
		}
	}
	return nil
}
