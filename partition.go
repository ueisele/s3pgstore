package s3pgstore

import (
	"fmt"
	"strings"
)

// partitionKeyValues splits a Hive-style partition key
// ("a=v1/b=v2") into the values in the order declared by parts
// (["a", "b"] → ["v1", "v2"]).
//
// Errors:
//   - The number of segments must equal len(parts).
//   - Each segment must be of the form "<name>=<value>".
//   - The segment name must equal the corresponding parts entry.
//   - Empty values ("a=") are rejected — empty values are
//     ambiguous against absent values in the catalog and would
//     violate the part_<n> NOT NULL contract anyway.
//
// Same input produces the same output every time — refactors
// must not introduce non-determinism. The returned slice is a
// fresh allocation; callers may mutate it.
func partitionKeyValues(key string, parts []string) ([]string, error) {
	if key == "" {
		return nil, fmt.Errorf("s3pgstore: partition key is empty")
	}
	segs := strings.Split(key, "/")
	if len(segs) != len(parts) {
		return nil, fmt.Errorf(
			"s3pgstore: partition key %q has %d segments, "+
				"PartitionKeyParts has %d entries",
			key, len(segs), len(parts))
	}
	out := make([]string, len(parts))
	for i, seg := range segs {
		eq := strings.IndexByte(seg, '=')
		if eq <= 0 {
			return nil, fmt.Errorf(
				"s3pgstore: partition key %q segment %d (%q): "+
					"expected <name>=<value>", key, i, seg)
		}
		name, value := seg[:eq], seg[eq+1:]
		if name != parts[i] {
			return nil, fmt.Errorf(
				"s3pgstore: partition key %q segment %d: "+
					"name %q != PartitionKeyParts[%d] %q",
				key, i, name, i, parts[i])
		}
		if value == "" {
			return nil, fmt.Errorf(
				"s3pgstore: partition key %q segment %d (%q): "+
					"empty value not allowed", key, i, seg)
		}
		out[i] = value
	}
	return out, nil
}
