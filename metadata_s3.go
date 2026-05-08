package s3pgstore

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// S3 user-metadata key namespace. AWS prepends `x-amz-meta-` to
// every key on the wire; the strings below are what the SDK
// caller passes to PutObjectInput.Metadata.
//
// All library-controlled keys carry the s3pgstore- prefix to
// avoid collision with caller-declared ExtensionColumns
// (which would also be valid Go identifier strings).
//
// Exported because cmd/s3pgstore-rebuild reads them from HEAD
// responses to reconstruct catalog rows without a body GET.
const (
	S3MetaToken            = "s3pgstore-token"
	S3MetaRecordCount      = "s3pgstore-records"
	S3MetaUncompressedSize = "s3pgstore-uncompressed"
	S3MetaWrittenAtVersion = "s3pgstore-version"
	S3MetaExtPrefix        = "s3pgstore-ext-" // followed by ExtensionColumn name
)

// S3MetadataMaxBytes is the user-metadata size cap AWS enforces
// (sum of key+value bytes, excluding the x-amz-meta- prefix).
// We validate at write time so callers get a deterministic error
// rather than a surprising 400 from S3.
const S3MetadataMaxBytes = 2 << 10 // 2 KiB

// BuildS3Metadata constructs the user-metadata map attached to
// a PUT for a single parquet file. The map is read back by
// cmd/s3pgstore-rebuild (via HEAD only — no body GET) to
// reconstruct the catalog row in DR scenarios.
//
// Library-controlled fields:
//   - s3pgstore-token              (only when set)
//   - s3pgstore-records            (record_count)
//   - s3pgstore-uncompressed       (uncompressed_size in bytes)
//   - s3pgstore-version            (written_at_version)
//
// Caller-controlled fields:
//   - s3pgstore-ext-<name>         (one per ExtensionColumn with
//     a non-nil value; serialized via formatExtForS3)
//
// Returns ErrS3MetadataTooLarge when the resulting map exceeds
// S3MetadataMaxBytes — operators with very wide ExtensionColumns
// should either trim metadata or accept the body-GET fallback in
// rebuild.
//
// Exported because cmd/s3pgstore-rebuild (and operator-side
// tools that simulate a write) need to build the same map.
func BuildS3Metadata(
	token string,
	recordCount, uncompressedSize, writtenAtVersion int64,
	exts []ExtensionColumn,
	values map[string]any,
) (map[string]string, error) {
	out := make(map[string]string, 4+len(exts))
	if token != "" {
		out[S3MetaToken] = token
	}
	out[S3MetaRecordCount] = strconv.FormatInt(recordCount, 10)
	out[S3MetaUncompressedSize] = strconv.FormatInt(uncompressedSize, 10)
	out[S3MetaWrittenAtVersion] = strconv.FormatInt(writtenAtVersion, 10)
	for _, c := range exts {
		v := metadataValueFor(values, c)
		if v == nil {
			continue
		}
		s, err := formatExtForS3(c, v)
		if err != nil {
			return nil, fmt.Errorf(
				"S3 metadata: ext column %q: %w", c.Name, err)
		}
		out[S3MetaExtPrefix+c.Name] = s
	}
	if total := s3MetadataSize(out); total > S3MetadataMaxBytes {
		return nil, fmt.Errorf(
			"%w: %d bytes exceeds S3 user-metadata cap %d",
			ErrS3MetadataTooLarge, total, S3MetadataMaxBytes)
	}
	for k, v := range out {
		if err := validateS3HeaderValue(v); err != nil {
			return nil, fmt.Errorf(
				"S3 metadata: key %q: %w", k, err)
		}
	}
	return out, nil
}

// ErrS3MetadataTooLarge is returned by Write when the assembled
// S3 user-metadata bag exceeds the 2 KiB cap. Wraps errors
// callers can match via errors.Is.
//
//nolint:gochecknoglobals // sentinel
var ErrS3MetadataTooLarge = errors.New(
	"s3pgstore: S3 user-metadata exceeds size limit")

// formatExtForS3 serializes one ExtensionColumn value to its
// canonical S3-metadata string form. Mirrors the type matrix
// validateMetadata enforces at write time so each accepted Go
// type has exactly one round-trip representation.
//
//   - TEXT       string     → as-is
//   - UUID       string|uuid.UUID|[16]byte → 36-char canonical
//   - TIMESTAMPTZ time.Time  → RFC 3339 nano UTC
//   - INT, BIGINT integer    → strconv.FormatInt (signed) or
//     strconv.FormatUint (unsigned)
//   - BOOLEAN    bool        → "true" / "false"
//   - NUMERIC    string|int*|float* → string as-is, ints via
//     strconv, floats via strconv.FormatFloat (with full prec)
func formatExtForS3(c ExtensionColumn, v any) (string, error) {
	switch normalizeExtType(c.Type) {
	case "TEXT":
		return v.(string), nil
	case "UUID":
		switch x := v.(type) {
		case string:
			return x, nil
		case uuid.UUID:
			return x.String(), nil
		case [16]byte:
			return uuid.UUID(x).String(), nil
		}
	case "TIMESTAMPTZ":
		t, ok := v.(time.Time)
		if !ok {
			return "", fmt.Errorf("expected time.Time, got %T", v)
		}
		return t.UTC().Format(time.RFC3339Nano), nil
	case "INT", "BIGINT":
		switch x := v.(type) {
		case int:
			return strconv.FormatInt(int64(x), 10), nil
		case int8:
			return strconv.FormatInt(int64(x), 10), nil
		case int16:
			return strconv.FormatInt(int64(x), 10), nil
		case int32:
			return strconv.FormatInt(int64(x), 10), nil
		case int64:
			return strconv.FormatInt(x, 10), nil
		case uint:
			return strconv.FormatUint(uint64(x), 10), nil
		case uint8:
			return strconv.FormatUint(uint64(x), 10), nil
		case uint16:
			return strconv.FormatUint(uint64(x), 10), nil
		case uint32:
			return strconv.FormatUint(uint64(x), 10), nil
		case uint64:
			return strconv.FormatUint(x, 10), nil
		}
	case "BOOLEAN":
		b, ok := v.(bool)
		if !ok {
			return "", fmt.Errorf("expected bool, got %T", v)
		}
		if b {
			return "true", nil
		}
		return "false", nil
	case "NUMERIC":
		switch x := v.(type) {
		case string:
			return x, nil
		case float32:
			return strconv.FormatFloat(float64(x), 'g', -1, 32), nil
		case float64:
			return strconv.FormatFloat(x, 'g', -1, 64), nil
		case int:
			return strconv.FormatInt(int64(x), 10), nil
		case int64:
			return strconv.FormatInt(x, 10), nil
		}
	}
	return "", fmt.Errorf(
		"unsupported (type=%s, Go=%T) for S3 metadata serialization",
		c.Type, v)
}

// ParseExtFromS3 is the inverse of formatExtForS3 — used by
// cmd/s3pgstore-rebuild to recover the original Go-typed value
// from the HEAD response. The returned value matches the kind
// the SQL INSERT expects for the column's declared type: string
// for TEXT/UUID/NUMERIC, time.Time for TIMESTAMPTZ, int64 for
// INT/BIGINT, bool for BOOLEAN.
//
// The hex-string [16]byte UUID variant doesn't round-trip — we
// always emit the canonical 36-char form, so on read we always
// emit a string that pgx will coerce into the UUID column
// (UUIDs accept text input).
//
// Exported for cmd/s3pgstore-rebuild and operator tools.
func ParseExtFromS3(c ExtensionColumn, raw string) (any, error) {
	switch normalizeExtType(c.Type) {
	case "TEXT":
		return raw, nil
	case "UUID":
		// Validate canonicalization but return the string form;
		// pgx will accept it for the UUID column.
		if _, err := uuid.Parse(raw); err != nil {
			return nil, fmt.Errorf("invalid UUID: %w", err)
		}
		return raw, nil
	case "TIMESTAMPTZ":
		return time.Parse(time.RFC3339Nano, raw)
	case "INT", "BIGINT":
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %w", err)
		}
		return n, nil
	case "BOOLEAN":
		switch raw {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, fmt.Errorf("invalid boolean %q", raw)
	case "NUMERIC":
		// Round-trip as string (NUMERIC accepts string in pgx).
		return raw, nil
	}
	return nil, fmt.Errorf("unrecognized SQL type %q", c.Type)
}

// normalizeExtType returns the SQL type in uppercase trimmed
// form, the canonical shape used by the type-dispatch switches.
// Mirrors metadata.go's validateMetadata normalization so the
// formatter and validator agree on the type.
func normalizeExtType(sqlType string) string {
	out := make([]byte, 0, len(sqlType))
	for i := range sqlType {
		c := sqlType[i]
		if c == ' ' || c == '\t' {
			continue
		}
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		out = append(out, c)
	}
	return string(out)
}

// s3MetadataSize computes the rough byte cost of a metadata
// map under the AWS rule (sum of key+value bytes, excluding
// the x-amz-meta- prefix added on the wire). Conservative
// estimate — each entry contributes len(key) + len(value).
func s3MetadataSize(m map[string]string) int {
	total := 0
	for k, v := range m {
		total += len(k) + len(v)
	}
	return total
}

// validateS3HeaderValue rejects values containing characters
// that aren't safe in an HTTP header. AWS specifically forbids
// CR / LF / NUL and any byte outside printable ASCII +
// horizontal whitespace.
//
// Tighter than RFC 7230 to avoid round-trip surprises (some
// clients normalize whitespace, some folding). Callers wanting
// arbitrary bytes should base64-encode upstream and store the
// encoded form as a TEXT ExtensionColumn.
func validateS3HeaderValue(v string) error {
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == ' ' || c == '\t' {
			continue
		}
		if c < 0x21 || c > 0x7E {
			return fmt.Errorf(
				"non-ASCII or control byte 0x%s at offset %d "+
					"(only printable ASCII + space/tab allowed)",
				hex.EncodeToString([]byte{c}), i)
		}
	}
	return nil
}
