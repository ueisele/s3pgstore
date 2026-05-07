package s3pgstore

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Default values for Config fields. Exported so tests and
// adapters can reference the same constants.
const (
	DefaultSchemaName    = "public"
	DefaultTablePrefix   = "s3pgstore_"
	DefaultNotifyChannel = "s3pgstore_writes"
)

// CompressionCodec selects the parquet compression algorithm
// applied at write time. The zero value (CompressionDefault)
// resolves to Snappy at runtime.
type CompressionCodec int

const (
	CompressionDefault CompressionCodec = iota
	CompressionSnappy
	CompressionZstd
	CompressionGzip
	CompressionUncompressed
)

// ExtensionColumn declares a typed per-file metadata column on
// s3pgstore_files. Each ExtensionColumn becomes an ext_<name>
// column at schema render time; WithMetadata writes values to
// those columns and validates that callers stay within the
// declared set.
//
// Allowed Type values (case-insensitive): TEXT, UUID, TIMESTAMPTZ,
// INT, BIGINT, BOOLEAN, NUMERIC. Anything else fails Config
// validation at New() time — keeps the SQL set well-defined and
// prevents schema-injection via Type.
type ExtensionColumn struct {
	Name string // e.g. "job_id"
	Type string // PostgreSQL type, e.g. "TEXT", "TIMESTAMPTZ", "UUID"
}

// MVRow is one materialized-view row produced by a
// MaterializedViewDef.Of closure. Key length must equal
// len(KeyColumns); Value length must equal len(ValueColumns).
type MVRow struct {
	Key   []string
	Value []string
}

// MaterializedViewDef declares an MV for the write path. Each
// declared MV becomes an s3pgstore_mv_<name> table at schema
// render time. The Of closure emits zero or more rows per
// record; rows are inserted in the same transaction as the
// catalog row, so MV state stays consistent with file state.
type MaterializedViewDef[T any] struct {
	Name         string
	KeyColumns   []string
	ValueColumns []string
	Of           func(T) ([]MVRow, error)
}

// Config holds everything s3pgstore needs to construct a
// Store[T]. Most fields are required; defaults are documented
// per field. Validate via Config.validate() — New() runs it
// before opening any connection.
type Config[T any] struct {
	// Executor is the database access abstraction. Required.
	Executor Executor

	// S3 wiring. All three required.
	Bucket   string
	Prefix   string
	S3Client *s3.Client

	// Schema layout
	SchemaName  string // default "public"
	TablePrefix string // default "s3pgstore_"; must end in "_"

	// Partitioning. PartitionKeyParts and PartitionKeyOf both
	// required.
	PartitionKeyParts []string
	PartitionKeyOf    func(T) string

	// Typed extension columns on s3pgstore_files; queryable as
	// plain SQL. See ExtensionColumn for the allowed Type set.
	ExtensionColumns []ExtensionColumn

	// Materialized views — declared up-front; typed query
	// handles via NewMaterializedView at runtime.
	MaterializedViews []MaterializedViewDef[T]

	// EntityKeyOf and VersionOf together enable read-time
	// dedup. Both fields together or neither — set one without
	// the other and Config validation fails.
	EntityKeyOf func(T) string
	VersionOf   func(T) int64

	// InsertedAtField names a time.Time field on T that the
	// library populates at write-start. Persisted as a real
	// parquet column. Empty disables.
	InsertedAtField string

	// Compression selects the parquet codec. Zero value
	// (CompressionDefault) → snappy.
	Compression CompressionCodec

	// EncodeBufPoolMaxBytes caps the encode-buffer pool entry
	// size. Buffers larger than this are dropped on return
	// instead of being pooled — protects long-lived encoders
	// from holding the largest-ever buffer. Zero uses
	// defaultEncodeBufPoolMaxBytes (48 MiB; see CLAUDE.md
	// § Benchmarks for the reasoning).
	EncodeBufPoolMaxBytes int

	// NotifyChannel is the LISTEN/NOTIFY channel name. Empty
	// disables NOTIFY (the sequencer falls back to interval
	// polling).
	NotifyChannel string // default "s3pgstore_writes"
}

// validate runs every Config invariant the library depends on.
// Called by New(); also exported for tests via Config.Validate.
func (c Config[T]) validate() error {
	var errs []string
	add := func(format string, a ...any) {
		errs = append(errs, fmt.Sprintf(format, a...))
	}

	if c.Executor == nil {
		add("Executor is required")
	}
	if c.Bucket == "" {
		add("Bucket is required")
	}
	if c.S3Client == nil {
		add("S3Client is required")
	}
	if len(c.PartitionKeyParts) == 0 {
		add("PartitionKeyParts must be non-empty")
	}
	for _, p := range c.PartitionKeyParts {
		if !validIdent(p) {
			add("PartitionKeyParts entry %q is not a valid identifier "+
				"(must match %s, max 30 chars)", p, identRegexSrc)
		}
	}
	if c.PartitionKeyOf == nil {
		add("PartitionKeyOf is required")
	}
	if c.TablePrefix != "" && !validTablePrefix(c.TablePrefix) {
		add("TablePrefix %q is invalid (must match %s, max 21 chars, "+
			"must end with '_')", c.TablePrefix, tablePrefixRegexSrc)
	}
	if c.SchemaName != "" && !validSchemaName(c.SchemaName) {
		add("SchemaName %q is not a valid identifier (must match %s, "+
			"max %d chars)", c.SchemaName, identRegexSrc,
			maxSchemaNameLen)
	}

	for i, ec := range c.ExtensionColumns {
		if !validIdent(ec.Name) {
			add("ExtensionColumns[%d].Name %q is not a valid identifier "+
				"(must match %s, max 30 chars)", i, ec.Name, identRegexSrc)
		}
		if !allowedExtensionType(ec.Type) {
			add("ExtensionColumns[%d].Type %q is not allowed (allowed: %s)",
				i, ec.Type, strings.Join(allowedExtensionTypes, ", "))
		}
	}
	seenExt := make(map[string]struct{}, len(c.ExtensionColumns))
	for _, ec := range c.ExtensionColumns {
		k := strings.ToLower(ec.Name)
		if _, dup := seenExt[k]; dup {
			add("ExtensionColumns has duplicate Name %q", ec.Name)
		}
		seenExt[k] = struct{}{}
	}

	for i, mv := range c.MaterializedViews {
		if !validIdent(mv.Name) {
			add("MaterializedViews[%d].Name %q is not a valid identifier "+
				"(must match %s, max 30 chars)", i, mv.Name, identRegexSrc)
		}
		if len(mv.KeyColumns) == 0 {
			add("MaterializedViews[%d] (%q): KeyColumns must be non-empty",
				i, mv.Name)
		}
		if mv.Of == nil {
			add("MaterializedViews[%d] (%q): Of is required",
				i, mv.Name)
		}
		for j, kc := range mv.KeyColumns {
			if !validIdent(kc) {
				add("MaterializedViews[%d].KeyColumns[%d] %q is not a "+
					"valid identifier", i, j, kc)
			}
		}
		for j, vc := range mv.ValueColumns {
			if !validIdent(vc) {
				add("MaterializedViews[%d].ValueColumns[%d] %q is not a "+
					"valid identifier", i, j, vc)
			}
		}
	}
	seenMV := make(map[string]struct{}, len(c.MaterializedViews))
	for _, mv := range c.MaterializedViews {
		k := strings.ToLower(mv.Name)
		if _, dup := seenMV[k]; dup {
			add("MaterializedViews has duplicate Name %q", mv.Name)
		}
		seenMV[k] = struct{}{}
	}

	if (c.EntityKeyOf == nil) != (c.VersionOf == nil) {
		add("EntityKeyOf and VersionOf must be set together " +
			"(or both nil)")
	}

	if c.EncodeBufPoolMaxBytes < 0 {
		add("EncodeBufPoolMaxBytes must be >= 0")
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid Config: %s",
			strings.Join(errs, "; "))
	}
	return nil
}

// Validate is the exported wrapper around the internal
// validation routine. Useful for tests and pre-flight checks.
func (c Config[T]) Validate() error { return c.validate() }

// resolved returns the Config with defaults filled in. Mirrors
// the values New() will use. Callers that need the resolved
// values for SchemaManager / RenderDDL go through the same
// path so prefixes and channel names stay consistent.
func (c Config[T]) resolved() Config[T] {
	out := c
	if out.SchemaName == "" {
		out.SchemaName = DefaultSchemaName
	}
	if out.TablePrefix == "" {
		out.TablePrefix = DefaultTablePrefix
	}
	if out.NotifyChannel == "" {
		out.NotifyChannel = DefaultNotifyChannel
	}
	return out
}

// Identifier validation. PostgreSQL allows up to 63 chars but
// the library composes prefixes (s3pgstore_, mv_, ext_, part_)
// and a name, so we cap user-facing identifiers at 30 to leave
// room for the prefix without surprising the operator with a
// silent truncation. Lowercase + digits + underscore matches
// SQL's case-insensitive folding without forcing quoting.
const (
	identRegexSrc       = `^[a-z][a-z0-9_]*$`
	tablePrefixRegexSrc = `^[a-z][a-z0-9_]*_$`
	maxIdentLen         = 30
	maxTablePrefixLen   = 21
	// maxSchemaNameLen leaves room for PostgreSQL's 63-char
	// NAMEDATALEN limit. Schema is a standalone identifier
	// (not concatenated with our suffixes), so it can be the
	// full 63 chars without breaking downstream table-name
	// composition.
	maxSchemaNameLen = 63
)

//nolint:gochecknoglobals // package-level compiled regexes
var (
	identRegex       = regexp.MustCompile(identRegexSrc)
	tablePrefixRegex = regexp.MustCompile(tablePrefixRegexSrc)
)

func validIdent(s string) bool {
	if len(s) == 0 || len(s) > maxIdentLen {
		return false
	}
	return identRegex.MatchString(s)
}

func validTablePrefix(s string) bool {
	if len(s) == 0 || len(s) > maxTablePrefixLen {
		return false
	}
	return tablePrefixRegex.MatchString(s)
}

func validSchemaName(s string) bool {
	if len(s) == 0 || len(s) > maxSchemaNameLen {
		return false
	}
	return identRegex.MatchString(s)
}

// Allowed PostgreSQL types for ExtensionColumns. Comparison is
// case-insensitive; canonical form is uppercase. Limiting the
// set keeps the SQL well-defined and prevents schema-injection
// through the Type field.
//
//nolint:gochecknoglobals // package-level allowlist
var allowedExtensionTypes = []string{
	"TEXT", "UUID", "TIMESTAMPTZ", "INT", "BIGINT", "BOOLEAN", "NUMERIC",
}

func allowedExtensionType(t string) bool {
	u := strings.ToUpper(strings.TrimSpace(t))
	return slices.Contains(allowedExtensionTypes, u)
}
