// Command s3pgstore-gc reclaims S3 objects whose s3pgstore
// catalog transactions rolled back (or never committed within a
// grace period). Reads its configuration from environment
// variables; supports one-shot and loop modes.
package main

func main() {
	// Wired in Phase 14.
}
