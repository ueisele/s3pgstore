package s3pgstore

// Adapted from
// https://github.com/ueisele/s3store/blob/738a8bcbce870887833e158d4dc4e5116a29d4fc/reader_dedup.go
// Copyright (c) 2024-2026 Uwe Eisele. MIT License.
//
// Adapted to s3pgstore: dedup helpers are package-level
// functions (not methods on Reader[T]); the (entity, version)
// comparator is built lazily from Config.EntityKeyOf /
// VersionOf and passed to sortAndDedup. Behavior is identical
// to upstream — same in-place compaction, same tie-break
// semantics, same WithHistory opt-out.

import "sort"

// sortAndDedup sorts records in-place by (entityKey, version)
// ascending and returns the slice with duplicates compacted
// out. Three modes:
//
//   - dedup disabled (entityKey or versionOf nil): records
//     are returned unchanged (input order, no copy, no sort).
//   - includeHistory=true: dedupReplicas collapses records
//     that share (entity, version). Replicas are by
//     definition equivalent (same logical write); the
//     specific record retained is implementation-defined.
//   - default: dedupLatest emits the highest-version record
//     per entity. The sort places that record LAST in each
//     entity group; for ties on max version the lex-later
//     filename wins (since the caller pre-sorts files by
//     S3 key before decode).
//
// In-place compaction: the returned slice shares the input's
// backing array (records[:n], n ≤ len(records)). Callers may
// treat the input slice as consumed.
func sortAndDedup[T any](
	records []T,
	entityKey func(T) string,
	versionOf func(T) int64,
	includeHistory bool,
) []T {
	if entityKey == nil || versionOf == nil {
		return records
	}
	sort.SliceStable(records, func(i, j int) bool {
		ea, eb := entityKey(records[i]), entityKey(records[j])
		if ea != eb {
			return ea < eb
		}
		return versionOf(records[i]) < versionOf(records[j])
	})
	if includeHistory {
		return dedupReplicas(records, entityKey, versionOf)
	}
	return dedupLatest(records, entityKey)
}

// dedupReplicas collapses records that share (entity, version)
// to one in-place. Input MUST be pre-sorted by (entity,
// version) ascending so equal pairs land adjacent.
//
// Which physical record survives a (entity, version) tied
// group is implementation-defined: under the WithHistory
// contract replicas describe the same logical write (a retry,
// zombie, or cross-node race) and are equivalent, so any pick
// is correct. This implementation keeps the FIRST record of
// each tied group.
func dedupReplicas[T any](
	records []T,
	entityKey func(T) string,
	versionOf func(T) int64,
) []T {
	if len(records) == 0 {
		return records
	}
	n := 0
	var prevEntity string
	var prevVersion int64
	first := true
	for _, r := range records {
		e := entityKey(r)
		v := versionOf(r)
		if !first && e == prevEntity && v == prevVersion {
			continue
		}
		records[n] = r
		n++
		prevEntity = e
		prevVersion = v
		first = false
	}
	return records[:n]
}

// dedupLatest keeps the highest-version record per entity
// in-place, emitting one record per entity in entity-sort
// order (ascending). Input MUST be pre-sorted by (entity,
// version) ascending — the LAST record in each contiguous
// entity group is then the latest version for that entity, so
// a single pass with O(1) state suffices.
//
// Tie-break on equal max version: `pending = r` advances on
// every iteration, so when multiple records share the highest
// version the LAST one wins. With stable sort + lex-ordered
// input (the read path sorts files by S3 key before decode)
// this means the lex-later filename wins.
//
// Safety: n ≤ i throughout (each entity group contributes one
// survivor, written before reading the next group's first
// record), so in-place writes never clobber an unread input.
func dedupLatest[T any](
	records []T, entityKey func(T) string,
) []T {
	if len(records) == 0 {
		return records
	}
	n := 0
	var prevEntity string
	var pending T
	first := true
	for _, r := range records {
		e := entityKey(r)
		if !first && e != prevEntity {
			records[n] = pending
			n++
		}
		pending = r
		prevEntity = e
		first = false
	}
	records[n] = pending
	n++
	return records[:n]
}
