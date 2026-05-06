// Package gc reclaims orphaned S3 objects whose s3pgstore
// catalog transactions rolled back (or never committed within a
// grace period). Ships as a library used by cmd/s3pgstore-gc.
package gc
