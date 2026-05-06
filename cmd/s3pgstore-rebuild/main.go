// Command s3pgstore-rebuild reconstructs the s3pgstore catalog
// from S3 alone — disaster-recovery tool. Walks <prefix>/data/
// via S3 LIST (the only place the library uses LIST), reads
// each Parquet file's footer for record_count, and INSERTs
// catalog rows.
package main

func main() {
	// Wired in Phase 15.
}
