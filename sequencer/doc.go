// Package sequencer assigns gap-free feed_seq values to
// committed catalog rows. Serializes via pg_advisory_xact_lock
// so only one sequencer instance writes at a time. Ships as a
// library used by cmd/s3pgstore-sequencer.
package sequencer
