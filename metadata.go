package s3pgstore

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// WithMetadata returns a WriteOption that attaches typed
// per-file metadata to the catalog row's ext_<n> columns.
// Every key must be declared in Config.ExtensionColumns; every
// value's Go type must match the declared SQL type.
//
// Validation runs at Write call time (no database round-trip
// needed for the validation itself — only the SQL types from
// Config.ExtensionColumns are consulted). Unknown keys and
// type mismatches are errors at the call site, not silent
// fallbacks.
//
// Composition with idempotency: the metadata of the FIRST
// successful write wins. A retry with the same token but
// different metadata returns the existing row's metadata
// unchanged (the retry's metadata is silently discarded by
// the partial-UNIQUE short-circuit).
func WithMetadata(m map[string]any) WriteOption {
	return metadataOpt{m: m}
}

type metadataOpt struct {
	m map[string]any
}

func (o metadataOpt) applyWrite(opts *writeOpts) {
	opts.metadata = o.m
}

// validateMetadata checks every key in m against the declared
// extension columns and verifies each value's Go type matches
// the declared SQL type. Returns a clear error on the first
// mismatch.
//
// nil m and len(m)==0 are no-ops.
func validateMetadata(m map[string]any, cols []ExtensionColumn) error {
	if len(m) == 0 {
		return nil
	}
	declared := make(map[string]string, len(cols))
	for _, c := range cols {
		declared[c.Name] = strings.ToUpper(strings.TrimSpace(c.Type))
	}
	for k, v := range m {
		sqlType, ok := declared[k]
		if !ok {
			return fmt.Errorf(
				"s3pgstore: WithMetadata: unknown key %q "+
					"(declared keys: %s)",
				k, declaredKeysList(declared))
		}
		if err := checkGoTypeMatchesSQL(k, sqlType, v); err != nil {
			return err
		}
	}
	return nil
}

// metadataValueFor returns the value for column c (in
// declaration order). When m has no entry for c, returns nil
// — pgx encodes nil as SQL NULL, matching the "metadata is
// optional per column" contract.
func metadataValueFor(m map[string]any, c ExtensionColumn) any {
	if m == nil {
		return nil
	}
	v, ok := m[c.Name]
	if !ok {
		return nil
	}
	return v
}

func declaredKeysList(declared map[string]string) string {
	if len(declared) == 0 {
		return "(none)"
	}
	keys := make([]string, 0, len(declared))
	for k := range declared {
		keys = append(keys, k)
	}
	// Sort for stable error messages.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
	return strings.Join(keys, ", ")
}

// checkGoTypeMatchesSQL validates v against the declared SQL
// type. The mapping favours strict matching — implicit
// conversions are limited to nil (any type accepts nil) and
// integer width (any signed integer accepts BIGINT/INT).
//
// SQL types accepted by ExtensionColumn (validated at Config
// time) are: TEXT, UUID, TIMESTAMPTZ, INT, BIGINT, BOOLEAN,
// NUMERIC.
func checkGoTypeMatchesSQL(name, sqlType string, v any) error {
	if v == nil {
		return nil
	}
	mismatch := func(want string) error {
		return fmt.Errorf(
			"s3pgstore: WithMetadata: key %q declared as %s, "+
				"got Go value of type %T (want %s)",
			name, sqlType, v, want)
	}
	switch sqlType {
	case "TEXT":
		if _, ok := v.(string); !ok {
			return mismatch("string")
		}
	case "UUID":
		switch v.(type) {
		case string, uuid.UUID, [16]byte:
			return nil
		default:
			return mismatch("string, uuid.UUID, or [16]byte")
		}
	case "TIMESTAMPTZ":
		if _, ok := v.(time.Time); !ok {
			return mismatch("time.Time")
		}
	case "INT", "BIGINT":
		switch v.(type) {
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			return nil
		default:
			return mismatch("integer (int/int64/...)")
		}
	case "BOOLEAN":
		if _, ok := v.(bool); !ok {
			return mismatch("bool")
		}
	case "NUMERIC":
		switch v.(type) {
		case string, float32, float64,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			return nil
		default:
			return mismatch("numeric (string, int*, float*)")
		}
	default:
		return fmt.Errorf(
			"s3pgstore: unrecognized SQL type %q for key %q",
			sqlType, name)
	}
	return nil
}
