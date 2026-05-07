package s3pgstore

import (
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/ueisele/s3pgstore/internal/catalog"
)

// PartitionFilter is a typed predicate over a logical column.
// Constructors: Eq, Prefix, Between, GE, LT, In, And, Or.
//
// Filters are translated to a single WHERE clause at SELECT
// time. Translation uses bound parameters ($1, $2, ...) — never
// string concatenation — so every value crosses the SQL
// boundary as a parameter.
//
// The same PartitionFilter type powers both the Read path
// (against part_<n> columns generated from
// Config.PartitionKeyParts) and MaterializedView.Lookup (against
// the MV's bare KeyColumns / ValueColumns). Translation passes a
// resolver closure that maps the filter's logical column name
// onto the SQL column identifier appropriate for the call site.
type PartitionFilter interface {
	// translate appends the filter's SQL fragment into b and
	// its bound values into args. resolveCol maps the filter's
	// logical column name to the SQL column identifier and
	// validates that it's a known column for the current
	// translation context.
	translate(resolveCol colResolver, b *strings.Builder, args *[]any) error
}

// colResolver maps a filter's logical column name to its SQL
// column identifier. Used to compose the filter mechanism with
// different table layouts (part_<n> for the catalog files
// table, bare identifiers for MV tables).
type colResolver func(logical string) (string, error)

// partColResolver returns a colResolver for the catalog's
// part_<n> column convention. Validates against the configured
// PartitionKeyParts and prepends the part_ prefix.
func partColResolver(parts []string) colResolver {
	return func(p string) (string, error) {
		if !slices.Contains(parts, p) {
			return "", fmt.Errorf(
				"filter references unknown part %q "+
					"(declared parts: %v)", p, parts)
		}
		return catalog.PartColumnPrefix + p, nil
	}
}

// mvColResolver returns a colResolver for an MV table. cols is
// the union of KeyColumns + ValueColumns; identifiers are
// quoted via pgx.Identifier so reserved words and case-
// sensitive names round-trip correctly.
func mvColResolver(cols []string) colResolver {
	return func(c string) (string, error) {
		if !slices.Contains(cols, c) {
			return "", fmt.Errorf(
				"filter references unknown column %q "+
					"(declared columns: %v)", c, cols)
		}
		return pgx.Identifier{c}.Sanitize(), nil
	}
}

// Eq returns a filter matching rows where the column equals
// value.
func Eq(col, value string) PartitionFilter {
	return eqFilter{col: col, value: value}
}

// Prefix returns a filter matching rows where the column starts
// with the given string. Maps to `LIKE 'prefix%'` with the
// prefix passed as a bound parameter — wildcards inside the
// prefix are passed through verbatim, which is fine because the
// values are caller-supplied; if you want a literal '%' or '_'
// in the prefix, escape with the standard SQL backslash
// convention against your locale-dependent ESCAPE clause.
func Prefix(col, prefix string) PartitionFilter {
	return prefixFilter{col: col, prefix: prefix}
}

// Between returns a filter matching rows where from <= column
// < to (half-open). from must be lex-less than to.
func Between(col, from, to string) PartitionFilter {
	return betweenFilter{col: col, from: from, to: to}
}

// GE returns a filter matching rows where column >= value.
func GE(col, value string) PartitionFilter {
	return geFilter{col: col, value: value}
}

// LT returns a filter matching rows where column < value.
func LT(col, value string) PartitionFilter {
	return ltFilter{col: col, value: value}
}

// In returns a filter matching rows where the column is one of
// the given values. Empty values slice produces a vacuous
// filter (`FALSE`), matching no rows.
func In(col string, values ...string) PartitionFilter {
	return inFilter{col: col, values: values}
}

// And combines filters with conjunction. Empty input is a
// no-op (`TRUE`).
func And(filters ...PartitionFilter) PartitionFilter {
	return andFilter(filters)
}

// Or combines filters with disjunction. Empty input is a
// vacuous filter (`FALSE`).
func Or(filters ...PartitionFilter) PartitionFilter {
	return orFilter(filters)
}

// addArg appends v to *args and returns the bound-parameter
// number ("$N") for the new argument.
func addArg(args *[]any, v any) string {
	*args = append(*args, v)
	return fmt.Sprintf("$%d", len(*args))
}

type eqFilter struct{ col, value string }

func (f eqFilter) translate(r colResolver, b *strings.Builder, args *[]any) error {
	col, err := r(f.col)
	if err != nil {
		return err
	}
	fmt.Fprintf(b, "%s = %s", col, addArg(args, f.value))
	return nil
}

type prefixFilter struct{ col, prefix string }

func (f prefixFilter) translate(r colResolver, b *strings.Builder, args *[]any) error {
	col, err := r(f.col)
	if err != nil {
		return err
	}
	fmt.Fprintf(b, "%s LIKE %s", col, addArg(args, f.prefix+"%"))
	return nil
}

type betweenFilter struct{ col, from, to string }

func (f betweenFilter) translate(r colResolver, b *strings.Builder, args *[]any) error {
	col, err := r(f.col)
	if err != nil {
		return err
	}
	if f.from >= f.to {
		return fmt.Errorf(
			"Between(%q, %q, %q): from must be lex-less than to",
			f.col, f.from, f.to)
	}
	fmt.Fprintf(b, "(%s >= %s AND %s < %s)",
		col, addArg(args, f.from),
		col, addArg(args, f.to))
	return nil
}

type geFilter struct{ col, value string }

func (f geFilter) translate(r colResolver, b *strings.Builder, args *[]any) error {
	col, err := r(f.col)
	if err != nil {
		return err
	}
	fmt.Fprintf(b, "%s >= %s", col, addArg(args, f.value))
	return nil
}

type ltFilter struct{ col, value string }

func (f ltFilter) translate(r colResolver, b *strings.Builder, args *[]any) error {
	col, err := r(f.col)
	if err != nil {
		return err
	}
	fmt.Fprintf(b, "%s < %s", col, addArg(args, f.value))
	return nil
}

type inFilter struct {
	col    string
	values []string
}

func (f inFilter) translate(r colResolver, b *strings.Builder, args *[]any) error {
	col, err := r(f.col)
	if err != nil {
		return err
	}
	if len(f.values) == 0 {
		b.WriteString("FALSE")
		return nil
	}
	placeholders := make([]string, len(f.values))
	for i, v := range f.values {
		placeholders[i] = addArg(args, v)
	}
	fmt.Fprintf(b, "%s IN (%s)", col, strings.Join(placeholders, ", "))
	return nil
}

type andFilter []PartitionFilter

func (f andFilter) translate(r colResolver, b *strings.Builder, args *[]any) error {
	if len(f) == 0 {
		b.WriteString("TRUE")
		return nil
	}
	b.WriteString("(")
	for i, sub := range f {
		if i > 0 {
			b.WriteString(" AND ")
		}
		if err := sub.translate(r, b, args); err != nil {
			return err
		}
	}
	b.WriteString(")")
	return nil
}

type orFilter []PartitionFilter

func (f orFilter) translate(r colResolver, b *strings.Builder, args *[]any) error {
	if len(f) == 0 {
		b.WriteString("FALSE")
		return nil
	}
	b.WriteString("(")
	for i, sub := range f {
		if i > 0 {
			b.WriteString(" OR ")
		}
		if err := sub.translate(r, b, args); err != nil {
			return err
		}
	}
	b.WriteString(")")
	return nil
}

// translateFilters joins multiple filters with AND and returns
// the WHERE clause + bound args. Empty filters → empty WHERE
// clause (callers append the result to "WHERE " unconditionally
// when non-empty).
func translateFilters(filters []PartitionFilter, r colResolver) (string, []any, error) {
	if len(filters) == 0 {
		return "", nil, nil
	}
	var (
		b    strings.Builder
		args []any
	)
	if len(filters) == 1 {
		if err := filters[0].translate(r, &b, &args); err != nil {
			return "", nil, err
		}
		return b.String(), args, nil
	}
	if err := andFilter(filters).translate(r, &b, &args); err != nil {
		return "", nil, err
	}
	return b.String(), args, nil
}
