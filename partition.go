package s3pgstore

import "github.com/ueisele/s3pgstore/internal/catalog"

// partitionKeyValues delegates to catalog.PartitionKeyValues so
// the parsing convention stays in one place — same code is used
// by cmd/s3pgstore-rebuild when reconstructing the catalog from
// S3 object keys.
func partitionKeyValues(key string, parts []string) ([]string, error) {
	return catalog.PartitionKeyValues(key, parts)
}
