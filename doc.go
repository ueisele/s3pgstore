// Package s3pgstore provides PostgreSQL-coordinated Parquet
// storage on S3. The v2 of the s3store family: S3 stores bytes;
// PostgreSQL coordinates.
//
// Capability sketch:
//
//   - Write, WriteWithKey: encode []T as Parquet, PUT to S3, INSERT
//     a catalog row in PostgreSQL. Atomic visibility on commit.
//   - Read, ReadIter, ReadPartitionIter, ReadRangeIter,
//     ReadPartitionRangeIter, ReadEntriesIter,
//     ReadPartitionEntriesIter: query the catalog, fetch matching
//     Parquet files in parallel, decode into []T, optionally dedup
//     latest-per-entity in memory.
//   - Poll, PollRecords, OffsetAt: stream change entries via a
//     gap-free feed_seq assigned by a separate sequencer process.
//   - LookupByToken: probe an idempotency token without writing.
//   - LockPartition: pessimistic per-partition lock.
//
// Catalog tables are operator-managed; the library never runs DDL
// outside SchemaManager.{Create,Drop} (test-only). A small
// Executor abstraction lets the catalog write participate in the
// caller's existing transaction (pgxpool, GORM, database/sql).
//
// Strong consistency: a successful Write is visible to every
// subsequent Read the moment its PostgreSQL transaction commits.
// No marker-gates, no settle windows, no timing knobs.
//
// See s3pgstore-proposal-v2.0.md for the full specification and
// CLAUDE.md for the correctness invariants refactors must preserve.
package s3pgstore
