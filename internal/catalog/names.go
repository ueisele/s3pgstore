// Package catalog holds the schema constants, DDL fragments,
// and shared SQL strings used by the s3pgstore root package and
// every subpackage / cmd binary that touches the catalog
// tables. Centralized here so table names and core SQL can't
// drift between the runtime path and operator tooling.
//
// Callers always go through Names — the table-name struct
// returned by NewNames(prefix) — rather than concatenating
// strings themselves. That guarantees TablePrefix substitution
// happens in exactly one place.
package catalog

import "fmt"

// Bare table-name suffixes appended to the configured
// TablePrefix. These are the only suffixes the library knows
// about; any table outside this set is the operator's
// responsibility (and not subject to the no-DDL-on-runtime
// invariant, since it's not ours).
const (
	TableFiles         = "files"
	TablePartitions    = "partitions"
	TablePendingWrites = "pending_writes"
	// TableMVPrefix is the prefix that follows TablePrefix for
	// per-materialized-view tables: <TablePrefix>mv_<name>.
	TableMVPrefix = "mv_"
)

// Column-name prefixes derived from configuration. part_<n>
// columns mirror Config.PartitionKeyParts; ext_<n> columns
// mirror Config.ExtensionColumns.
const (
	PartColumnPrefix = "part_"
	ExtColumnPrefix  = "ext_"
)

// Names is the substituted table-name set for a given schema +
// table-prefix configuration. The library never writes
// `s3pgstore_files` literally; it always goes through
// Names.Files (and friends). Schema and Prefix are stored in
// already-validated form — callers must not pass untrusted
// strings to NewNames.
type Names struct {
	Schema string // e.g. "public"
	Prefix string // e.g. "s3pgstore_"
}

// NewNames returns a Names struct for the given schema and
// table-prefix. Both arguments must already be validated as
// safe SQL identifiers (the root package's Config validation
// runs before any code reaches here).
func NewNames(schema, prefix string) Names {
	return Names{Schema: schema, Prefix: prefix}
}

// Qualify returns "schema"."table" with both identifiers
// double-quoted. Used for every table reference the library
// emits.
func (n Names) Qualify(table string) string {
	return fmt.Sprintf("%q.%q", n.Schema, table)
}

// Files returns the fully-qualified s3pgstore_files table name.
func (n Names) Files() string {
	return n.Qualify(n.Prefix + TableFiles)
}

// Partitions returns the fully-qualified s3pgstore_partitions
// table name.
func (n Names) Partitions() string {
	return n.Qualify(n.Prefix + TablePartitions)
}

// PendingWrites returns the fully-qualified
// s3pgstore_pending_writes table name.
func (n Names) PendingWrites() string {
	return n.Qualify(n.Prefix + TablePendingWrites)
}

// MV returns the fully-qualified per-MV table name for the
// given declared MV name.
func (n Names) MV(name string) string {
	return n.Qualify(n.Prefix + TableMVPrefix + name)
}

// Bare returns the unqualified table name (no schema, no
// quoting) for diagnostics — e.g. when querying
// information_schema, where the schema and table go in
// separate parameters.
func (n Names) Bare(suffix string) string {
	return n.Prefix + suffix
}

// FilesBare returns the unqualified s3pgstore_files table
// name. Convenience wrapper around Bare.
func (n Names) FilesBare() string { return n.Bare(TableFiles) }

// PartitionsBare returns the unqualified s3pgstore_partitions
// table name.
func (n Names) PartitionsBare() string { return n.Bare(TablePartitions) }

// PendingWritesBare returns the unqualified
// s3pgstore_pending_writes table name.
func (n Names) PendingWritesBare() string { return n.Bare(TablePendingWrites) }

// MVBare returns the unqualified per-MV table name.
func (n Names) MVBare(name string) string {
	return n.Prefix + TableMVPrefix + name
}
