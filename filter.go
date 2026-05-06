package s3pgstore

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ueisele/s3pgstore/internal/catalog"
)

// PartitionFilter is a typed predicate against the part_<n>
// columns generated from Config.PartitionKeyParts. Constructors:
// Eq, Prefix, Between, GE, LT, In, And, Or.
//
// Filters are translated to a single WHERE clause at SELECT
// time. Translation uses bound parameters ($1, $2, ...) — never
// string concatenation — so every value crosses the SQL
// boundary as a parameter.
type PartitionFilter interface {
	// translate appends the filter's SQL fragment into b and
	// its bound values into args. Unknown part names error.
	translate(parts []string, b *strings.Builder, args *[]any) error
}

// Eq returns a filter matching rows where part = value.
func Eq(part, value string) PartitionFilter {
	return eqFilter{part: part, value: value}
}

// Prefix returns a filter matching rows where part starts with
// the given string. Maps to `LIKE 'prefix%'` with the prefix
// passed as a bound parameter — wildcards inside `prefix` are
// passed through verbatim, which is fine because the values
// are caller-supplied; if you want a literal '%' or '_' in the
// prefix, escape with the standard SQL backslash convention
// against your locale-dependent ESCAPE clause.
func Prefix(part, prefix string) PartitionFilter {
	return prefixFilter{part: part, prefix: prefix}
}

// Between returns a filter matching rows where from <= part
// < to (half-open). from must be lex-less than to.
func Between(part, from, to string) PartitionFilter {
	return betweenFilter{part: part, from: from, to: to}
}

// GE returns a filter matching rows where part >= value.
func GE(part, value string) PartitionFilter {
	return geFilter{part: part, value: value}
}

// LT returns a filter matching rows where part < value.
func LT(part, value string) PartitionFilter {
	return ltFilter{part: part, value: value}
}

// In returns a filter matching rows where part is one of the
// given values. Empty values slice produces a vacuous filter
// (`FALSE`), matching no rows.
func In(part string, values ...string) PartitionFilter {
	return inFilter{part: part, values: values}
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

// validatePart confirms p is in parts; used by every leaf
// translate to catch typos at translation time.
func validatePart(p string, parts []string) error {
	if !slices.Contains(parts, p) {
		return fmt.Errorf(
			"s3pgstore: filter references unknown part %q "+
				"(declared parts: %v)", p, parts)
	}
	return nil
}

// addArg appends v to *args and returns the bound-parameter
// number ("$N") for the new argument.
func addArg(args *[]any, v any) string {
	*args = append(*args, v)
	return fmt.Sprintf("$%d", len(*args))
}

type eqFilter struct{ part, value string }

func (f eqFilter) translate(parts []string, b *strings.Builder, args *[]any) error {
	if err := validatePart(f.part, parts); err != nil {
		return err
	}
	fmt.Fprintf(b, "%s%s = %s", catalog.PartColumnPrefix, f.part, addArg(args, f.value))
	return nil
}

type prefixFilter struct{ part, prefix string }

func (f prefixFilter) translate(parts []string, b *strings.Builder, args *[]any) error {
	if err := validatePart(f.part, parts); err != nil {
		return err
	}
	fmt.Fprintf(b, "%s%s LIKE %s", catalog.PartColumnPrefix, f.part,
		addArg(args, f.prefix+"%"))
	return nil
}

type betweenFilter struct{ part, from, to string }

func (f betweenFilter) translate(parts []string, b *strings.Builder, args *[]any) error {
	if err := validatePart(f.part, parts); err != nil {
		return err
	}
	if f.from >= f.to {
		return fmt.Errorf(
			"s3pgstore: Between(%q, %q, %q): from must be lex-less than to",
			f.part, f.from, f.to)
	}
	col := catalog.PartColumnPrefix + f.part
	fmt.Fprintf(b, "(%s >= %s AND %s < %s)",
		col, addArg(args, f.from),
		col, addArg(args, f.to))
	return nil
}

type geFilter struct{ part, value string }

func (f geFilter) translate(parts []string, b *strings.Builder, args *[]any) error {
	if err := validatePart(f.part, parts); err != nil {
		return err
	}
	fmt.Fprintf(b, "%s%s >= %s", catalog.PartColumnPrefix, f.part,
		addArg(args, f.value))
	return nil
}

type ltFilter struct{ part, value string }

func (f ltFilter) translate(parts []string, b *strings.Builder, args *[]any) error {
	if err := validatePart(f.part, parts); err != nil {
		return err
	}
	fmt.Fprintf(b, "%s%s < %s", catalog.PartColumnPrefix, f.part,
		addArg(args, f.value))
	return nil
}

type inFilter struct {
	part   string
	values []string
}

func (f inFilter) translate(parts []string, b *strings.Builder, args *[]any) error {
	if err := validatePart(f.part, parts); err != nil {
		return err
	}
	if len(f.values) == 0 {
		b.WriteString("FALSE")
		return nil
	}
	col := catalog.PartColumnPrefix + f.part
	placeholders := make([]string, len(f.values))
	for i, v := range f.values {
		placeholders[i] = addArg(args, v)
	}
	fmt.Fprintf(b, "%s IN (%s)", col, strings.Join(placeholders, ", "))
	return nil
}

type andFilter []PartitionFilter

func (f andFilter) translate(parts []string, b *strings.Builder, args *[]any) error {
	if len(f) == 0 {
		b.WriteString("TRUE")
		return nil
	}
	b.WriteString("(")
	for i, sub := range f {
		if i > 0 {
			b.WriteString(" AND ")
		}
		if err := sub.translate(parts, b, args); err != nil {
			return err
		}
	}
	b.WriteString(")")
	return nil
}

type orFilter []PartitionFilter

func (f orFilter) translate(parts []string, b *strings.Builder, args *[]any) error {
	if len(f) == 0 {
		b.WriteString("FALSE")
		return nil
	}
	b.WriteString("(")
	for i, sub := range f {
		if i > 0 {
			b.WriteString(" OR ")
		}
		if err := sub.translate(parts, b, args); err != nil {
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
func translateFilters(filters []PartitionFilter, parts []string) (string, []any, error) {
	if len(filters) == 0 {
		return "", nil, nil
	}
	var (
		b    strings.Builder
		args []any
	)
	if len(filters) == 1 {
		if err := filters[0].translate(parts, &b, &args); err != nil {
			return "", nil, err
		}
		return b.String(), args, nil
	}
	// Multiple filters → conjunction.
	if err := andFilter(filters).translate(parts, &b, &args); err != nil {
		return "", nil, err
	}
	return b.String(), args, nil
}
