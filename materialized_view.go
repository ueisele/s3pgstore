package s3pgstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// MaterializedViewLookupDef binds a typed lookup against an MV
// declared in Config.MaterializedViews. NewMaterializedView
// validates that Name matches a declared MV and that
// KeyColumns / ValueColumns line up with the declared shape;
// schema drift is caught at construction, not at first lookup.
//
// From projects (KeyColumns + ValueColumns) values onto K. The
// callback receives one slice per row whose length is
// len(KeyColumns) + len(ValueColumns); positions match the
// declaration order. Returning an error from From propagates
// out of Lookup with the row's index.
type MaterializedViewLookupDef[K any] struct {
	Name         string
	KeyColumns   []string
	ValueColumns []string
	From         func(values []string) (K, error)
}

// MaterializedView is a typed lookup handle bound to a single
// MV declared on the Store. Construct via NewMaterializedView.
// Safe for concurrent use.
type MaterializedView[K any] struct {
	def      MaterializedViewLookupDef[K]
	executor Executor
	tableSQL string
	allCols  []string // KeyColumns ++ ValueColumns; for filter resolution
	cols     []string // quoted SQL identifiers in select order
}

// NewMaterializedView returns a typed lookup handle for the MV
// named in def. Validates that:
//
//   - def.Name corresponds to a declared MV in Config.MaterializedViews;
//   - def.KeyColumns / def.ValueColumns match the declaration
//     verbatim (same length, same names, same order — the MV
//     table's column order is the declaration order, and the
//     lookup must mirror it so the SELECT list and From callback
//     stay aligned);
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

	if !equalSliceCase(def.KeyColumns, declared.KeyColumns) {
		return nil, fmt.Errorf(
			"NewMaterializedView %q: KeyColumns mismatch "+
				"(lookup: %v, declared: %v)",
			def.Name, def.KeyColumns, declared.KeyColumns)
	}
	if !equalSliceCase(def.ValueColumns, declared.ValueColumns) {
		return nil, fmt.Errorf(
			"NewMaterializedView %q: ValueColumns mismatch "+
				"(lookup: %v, declared: %v)",
			def.Name, def.ValueColumns, declared.ValueColumns)
	}
	if def.From == nil {
		return nil, fmt.Errorf(
			"NewMaterializedView %q: From is required", def.Name)
	}

	allCols := make([]string,
		0, len(def.KeyColumns)+len(def.ValueColumns))
	allCols = append(allCols, def.KeyColumns...)
	allCols = append(allCols, def.ValueColumns...)

	cols := make([]string, len(allCols))
	for i, c := range allCols {
		cols[i] = pgx.Identifier{c}.Sanitize()
	}

	return &MaterializedView[K]{
		def:      def,
		executor: store.cfg.Executor,
		tableSQL: store.names.MV(def.Name),
		allCols:  allCols,
		cols:     cols,
	}, nil
}

// Lookup returns every row matching filters as a K. Filters are
// the same PartitionFilter type as Read, but resolved against
// the MV's bare column names (not part_<n>). Empty filters
// returns every row.
//
// Records are returned in primary-key order — same shape as
// Read's deterministic emission, just keyed by the MV's
// KeyColumns instead of the catalog's partition key. The
// project callback (def.From) runs once per row in result
// order; an error from From terminates Lookup with the row's
// position.
func (m *MaterializedView[K]) Lookup(
	ctx context.Context, filters []PartitionFilter,
) ([]K, error) {
	where, args, err := translateFilters(filters,
		mvColResolver(m.allCols))
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(m.cols, ", "), m.tableSQL)
	if where != "" {
		q += " WHERE " + where
	}
	if len(m.def.KeyColumns) > 0 {
		ord := make([]string, len(m.def.KeyColumns))
		for i, c := range m.def.KeyColumns {
			ord[i] = pgx.Identifier{c}.Sanitize()
		}
		q += " ORDER BY " + strings.Join(ord, ", ")
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
			values := make([]string, len(m.allCols))
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

// findDeclaredMV looks up an MV by name in the Store's config.
// The MaterializedViewDef's typed fields aren't relevant here —
// only the column shape — so we project to a generic shape via
// the unsafe-typed iteration.
//
// We can't do this generically over T (the Store's record type)
// without exposing Config[T]'s MV slice through an interface,
// which would couple every package importing Store[T] to a
// runtime mechanism that's only used at MV construction time.
// Instead we project the Config[T].MaterializedViews shape via
// a small per-Store helper that ignores the Of closure.
func findDeclaredMV[T any](
	s *Store[T], name string,
) (mvShape, bool) {
	for _, m := range s.cfg.MaterializedViews {
		if m.Name == name {
			return mvShape{
				Name:         m.Name,
				KeyColumns:   m.KeyColumns,
				ValueColumns: m.ValueColumns,
			}, true
		}
	}
	return mvShape{}, false
}

// mvShape is the column-only projection of MaterializedViewDef
// used by the validator. Drops the Of closure (write-side) and
// the type parameter T so the validator can operate on it
// generically.
type mvShape struct {
	Name         string
	KeyColumns   []string
	ValueColumns []string
}

// equalSliceCase compares two string slices element-wise with
// case-insensitive matching. Identifiers in PostgreSQL fold to
// lowercase by default, so an MV declared as KeyColumns:
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

// mvWriteRows holds the per-MV row data resolved during one
// writePartition invocation. Each entry pairs an MV's name with
// the rows produced by its Of closure, ready to INSERT inside
// the catalog tx.
//
// Build order matches Config.MaterializedViews, so iteration
// emits MVs in declaration order — deterministic across runs.
type mvWriteRows struct {
	name         string
	keyColumns   []string
	valueColumns []string
	rows         []MVRow
}

// resolveMVRows runs every declared MV's Of closure against
// records and returns the per-MV row sets. Validates that each
// row's Key length matches len(KeyColumns) and Value length
// matches len(ValueColumns) — shape errors surface here, before
// the catalog tx opens.
func (s *Store[T]) resolveMVRows(records []T) ([]mvWriteRows, error) {
	if len(s.cfg.MaterializedViews) == 0 {
		return nil, nil
	}
	out := make([]mvWriteRows, 0, len(s.cfg.MaterializedViews))
	for _, mv := range s.cfg.MaterializedViews {
		rows := []MVRow{}
		for i, rec := range records {
			emitted, err := mv.Of(rec)
			if err != nil {
				return nil, fmt.Errorf(
					"MV %q Of(records[%d]): %w",
					mv.Name, i, err)
			}
			for j, r := range emitted {
				if len(r.Key) != len(mv.KeyColumns) {
					return nil, fmt.Errorf(
						"MV %q row %d (record %d): Key len "+
							"%d != KeyColumns len %d",
						mv.Name, j, i,
						len(r.Key), len(mv.KeyColumns))
				}
				if len(r.Value) != len(mv.ValueColumns) {
					return nil, fmt.Errorf(
						"MV %q row %d (record %d): Value len "+
							"%d != ValueColumns len %d",
						mv.Name, j, i,
						len(r.Value), len(mv.ValueColumns))
				}
				rows = append(rows, r)
			}
		}
		out = append(out, mvWriteRows{
			name:         mv.Name,
			keyColumns:   mv.KeyColumns,
			valueColumns: mv.ValueColumns,
			rows:         rows,
		})
	}
	return out, nil
}

// insertMVRows runs the per-MV INSERT ... ON CONFLICT for every
// row resolved by resolveMVRows. Called inside the catalog tx
// so MV state stays consistent with file state under both
// commit and rollback.
//
// Conflict policy mirrors the MV's shape: DO NOTHING for key-
// only MVs (idempotent first-writer-wins), DO UPDATE SET for
// MVs with ValueColumns (last-writer-wins).
//
// Empty rows slice is a no-op — the SQL is built per row, so an
// MV that emitted nothing for this batch produces no
// statements.
func (s *Store[T]) insertMVRows(
	ctx context.Context, d DBTX, mvs []mvWriteRows,
) error {
	for _, mv := range mvs {
		if len(mv.rows) == 0 {
			continue
		}
		insertSQL := s.names.MVInsertSQL(mv.name,
			mv.keyColumns, mv.valueColumns)
		for i, r := range mv.rows {
			args := make([]any,
				0, len(r.Key)+len(r.Value))
			for _, k := range r.Key {
				args = append(args, k)
			}
			for _, v := range r.Value {
				args = append(args, v)
			}
			if _, err := d.Exec(ctx, insertSQL, args...); err != nil {
				return fmt.Errorf("MV %q row %d: %w",
					mv.name, i, err)
			}
		}
	}
	return nil
}
