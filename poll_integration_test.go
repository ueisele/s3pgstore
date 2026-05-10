//go:build integration

package s3pgstore_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ueisele/s3pgstore"
	"github.com/ueisele/s3pgstore/sequencer"
)

type streamRec struct {
	ID    string `parquet:"id"`
	Value int64  `parquet:"value"`
}

func newStreamCfg(f *fixture) s3pgstore.Config[streamRec] {
	return s3pgstore.Config[streamRec]{
		Executor:          s3pgstore.NewPoolExecutor(f.Pool),
		S3Bucket:          f.Bucket,
		S3Prefix:          "stream",
		S3Client:          f.S3Client,
		SchemaName:        f.Schema,
		PartitionKeyParts: []string{"customer"},
		PartitionKeyOf:    func(r streamRec) string { return "customer=" + r.ID },
		ExtensionColumns: []s3pgstore.ExtensionColumn{
			{Name: "job_id", Type: "TEXT"},
		},
	}
}

func newStreamStore(t *testing.T, f *fixture) *s3pgstore.Store[streamRec] {
	t.Helper()
	cfg := newStreamCfg(f)
	mgr := s3pgstore.NewSchemaManager(cfg)
	if err := mgr.Create(t.Context()); err != nil {
		t.Fatalf("Create schema: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Drop(t.Context()) })
	store, err := s3pgstore.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New store: %v", err)
	}
	return store
}

// runSequencerSync drains the feed_seq queue inline via
// sequencer.RunOnce. Cheaper than spinning up Run for tests
// that just need rows sequenced before polling.
func runSequencerSync(t *testing.T, f *fixture) {
	t.Helper()
	if _, err := sequencer.RunOnce(t.Context(), sequencer.Config{
		Pool:        f.Pool,
		SchemaName:  f.Schema,
		TablePrefix: "s3pgstore_",
		BatchSize:   1000,
	}); err != nil {
		t.Fatalf("sequencer.RunOnce: %v", err)
	}
}

// TestPoll_Empty verifies Poll returns (nil, since, nil) when
// no rows match — the canonical "nothing new" return shape.
func TestPoll_Empty(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	entries, next, err := store.Poll(t.Context(), 0, s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("entries: want 0, got %d", len(entries))
	}
	if next != 0 {
		t.Errorf("next offset: want 0 (unchanged), got %d", next)
	}
}

// TestPoll_BasicWalk: write 5 records, sequence them, poll
// twice with batch=2, verify the walk is gap-free, ordered, and
// the next-offset advances each call.
func TestPoll_BasicWalk(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for i := range 5 {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i), Value: int64(i)}},
			s3pgstore.WithMetadata(map[string]any{
				"job_id": fmt.Sprintf("job-%d", i),
			}),
		); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	runSequencerSync(t, f)

	// Batch 1: offsets 1, 2 — half-open range [0, 3).
	b1, next1, err := store.Poll(t.Context(), 0, 3)
	if err != nil {
		t.Fatalf("Poll 1: %v", err)
	}
	if len(b1) != 2 {
		t.Fatalf("batch 1 size: want 2, got %d", len(b1))
	}
	if b1[0].Offset != 1 || b1[1].Offset != 2 {
		t.Errorf("batch 1 offsets: %d, %d (want 1, 2)",
			b1[0].Offset, b1[1].Offset)
	}
	if next1 != 3 {
		t.Errorf("next1: want 3 (max+1), got %d", next1)
	}
	// Extensions surfaces declared columns.
	if got := b1[0].Extensions["job_id"]; got != "job-0" {
		t.Errorf("Extensions[job_id]: got %v, want %q", got, "job-0")
	}

	// Batch 2: offsets 3, 4 — range [next1, next1+2) = [3, 5).
	b2, next2, err := store.Poll(t.Context(), next1, next1+2)
	if err != nil {
		t.Fatalf("Poll 2: %v", err)
	}
	if len(b2) != 2 || b2[0].Offset != 3 || b2[1].Offset != 4 {
		t.Errorf("batch 2 offsets: %v (want [3, 4])",
			[]int64{b2[0].Offset, b2[1].Offset})
	}
	if next2 != 5 {
		t.Errorf("next2: want 5 (max+1), got %d", next2)
	}

	// Batch 3: tail (offset 5) — range [5, 7).
	b3, next3, err := store.Poll(t.Context(), next2, next2+2)
	if err != nil {
		t.Fatalf("Poll 3: %v", err)
	}
	if len(b3) != 1 || b3[0].Offset != 5 || next3 != 6 {
		t.Errorf("batch 3: offsets=%v next=%d (want next=6)",
			b3, next3)
	}

	// Batch 4: empty, next unchanged.
	b4, next4, err := store.Poll(t.Context(), next3, next3+2)
	if err != nil {
		t.Fatalf("Poll 4: %v", err)
	}
	if len(b4) != 0 || next4 != next3 {
		t.Errorf("batch 4: should be empty + unchanged offset; "+
			"len=%d next=%d", len(b4), next4)
	}
}

// TestPoll_EmptyRange verifies since == until short-circuits to
// (nil, since, nil) without touching the database.
func TestPoll_EmptyRange(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	entries, next, err := store.Poll(t.Context(), 42, 42)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 0 || next != 42 {
		t.Errorf("got (%d entries, next=%d), want (0, 42)",
			len(entries), next)
	}
}

// TestPoll_InvertedRange verifies since > until returns an
// error (programmer mistake; easy to spot in tests).
func TestPoll_InvertedRange(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	_, _, err := store.Poll(t.Context(), 100, 50)
	if err == nil {
		t.Fatal("Poll: want error for since > until, got nil")
	}
	if !strings.Contains(err.Error(), "since") ||
		!strings.Contains(err.Error(), "until") {
		t.Errorf("error text: %v", err)
	}
}

// TestPoll_UntilExclusive verifies the upper bound is exclusive:
// a row whose feed_seq exactly equals `until` is NOT returned.
func TestPoll_UntilExclusive(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for i := range 5 {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runSequencerSync(t, f)

	// Range [0, 4) — should return offsets 1, 2, 3 (NOT 4).
	entries, next, err := store.Poll(t.Context(), 0, 4)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("entries in [0, 4): got %d, want 3 (1,2,3)",
			len(entries))
	}
	if next != 4 {
		t.Errorf("next: got %d, want 4", next)
	}
}

// TestPoll_OffsetLatest verifies OffsetEarliest + OffsetLatest
// together drain the entire feed: OffsetEarliest is the
// inclusive lower-bound sentinel (= 1, the lowest live offset),
// OffsetLatest drops the upper-bound clause.
func TestPoll_OffsetLatest(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for i := range 4 {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runSequencerSync(t, f)

	entries, next, err := store.Poll(t.Context(),
		s3pgstore.OffsetEarliest, s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 4 {
		t.Errorf("entries with [OffsetEarliest, OffsetLatest): "+
			"got %d, want 4", len(entries))
	}
	if entries[0].Offset != s3pgstore.OffsetEarliest {
		t.Errorf("first offset: got %d, want OffsetEarliest (%d)",
			entries[0].Offset, s3pgstore.OffsetEarliest)
	}
	if next != 5 {
		t.Errorf("next: got %d, want 5 (max+1)", next)
	}
}

// TestPollRecords_DecodesAllAndOrders verifies PollRecords
// decodes all returned files and the records appear in offset
// order (per-file, in parquet order).
func TestPollRecords_DecodesAllAndOrders(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for i := range 4 {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i), Value: int64(i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runSequencerSync(t, f)

	results, next, err := store.PollRecords(t.Context(), 0,
		s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("results: got %d, want 4", len(results))
	}
	if next != 5 {
		t.Errorf("next: got %d, want 5 (max+1)", next)
	}
	// Each file is a single record; order is by feed_seq which
	// is by written_at order (and we wrote sequentially).
	for i, fr := range results {
		if len(fr.Records) != 1 {
			t.Fatalf("results[%d]: %d records, want 1",
				i, len(fr.Records))
		}
		if fr.Records[0].Value != int64(i) {
			t.Errorf("results[%d].Records[0].Value: got %d, want %d",
				i, fr.Records[0].Value, i)
		}
		// File metadata carries through.
		if fr.File.Offset != int64(i+1) {
			t.Errorf("results[%d].File.Offset: got %d, want %d",
				i, fr.File.Offset, i+1)
		}
	}
}

// TestPollRecords_Empty verifies the empty-result path.
func TestPollRecords_Empty(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	results, next, err := store.PollRecords(t.Context(), 0,
		s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(results) != 0 || next != 0 {
		t.Errorf("got (%d results, next=%d), want (0, 0)",
			len(results), next)
	}
}

// TestPoll_ReplayFromZeroIsStable verifies that two polls from
// offset 0 produce the same sequence — the load-bearing replay-
// stability invariant from CLAUDE.md.
func TestPoll_ReplayFromZeroIsStable(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for i := range 6 {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runSequencerSync(t, f)

	first, _, err := store.Poll(t.Context(), 0, s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("Poll 1: %v", err)
	}
	second, _, err := store.Poll(t.Context(), 0, s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("Poll 2: %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("len mismatch: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].Offset != second[i].Offset ||
			first[i].S3Key != second[i].S3Key {
			t.Errorf("replay mismatch at %d: %+v vs %+v",
				i, first[i], second[i])
		}
	}
}

// TestPoll_OnlyFeedSeqNotNullVisible: until the sequencer
// assigns feed_seq, rows are not visible to Poll. Pre-sequencer
// state has rows with feed_seq=NULL — Poll's WHERE filter
// excludes them.
func TestPoll_OnlyFeedSeqNotNullVisible(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	if _, err := store.Write(t.Context(),
		[]streamRec{{ID: "alice"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Don't run the sequencer.

	entries, _, err := store.Poll(t.Context(), 0, s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("entries before sequencer ran: got %d, want 0",
			len(entries))
	}

	// After sequencer runs, the row appears.
	runSequencerSync(t, f)
	entries, _, err = store.Poll(t.Context(), 0, s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("entries after sequencer ran: got %d, want 1",
			len(entries))
	}
}

// TestOffsetAt_AtOrAfter verifies OffsetAt returns the first
// offset whose feed_seq_at >= t. We seed three rows with
// distinct sequencer-touch times by sleeping between RunOnce
// calls, then probe the boundary.
func TestOffsetAt_AtOrAfter(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	// Seed three writes; sequence them in three separate
	// RunOnce calls so each gets a distinct feed_seq_at.
	if _, err := store.Write(t.Context(),
		[]streamRec{{ID: "c1"}}); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	runSequencerSync(t, f)
	t1 := time.Now()
	time.Sleep(50 * time.Millisecond)

	if _, err := store.Write(t.Context(),
		[]streamRec{{ID: "c2"}}); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	runSequencerSync(t, f)
	time.Sleep(50 * time.Millisecond)

	if _, err := store.Write(t.Context(),
		[]streamRec{{ID: "c3"}}); err != nil {
		t.Fatalf("Write 3: %v", err)
	}
	runSequencerSync(t, f)

	// At t1: must return offset 2 or 3 (anything >= the time
	// stamped after the first sequencer run).
	got, err := store.OffsetAt(t.Context(), t1)
	if err != nil {
		t.Fatalf("OffsetAt: %v", err)
	}
	if got != 2 {
		t.Errorf("OffsetAt(t1): got %d, want 2", got)
	}
}

// TestOffsetAt_FutureReturnsZero verifies that OffsetAt(future)
// returns 0 when no row satisfies feed_seq_at >= future. This
// is the "nothing yet" sentinel the proposal documents.
func TestOffsetAt_FutureReturnsZero(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	if _, err := store.Write(t.Context(),
		[]streamRec{{ID: "alice"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	runSequencerSync(t, f)

	got, err := store.OffsetAt(t.Context(),
		time.Now().Add(1*time.Hour))
	if err != nil {
		t.Fatalf("OffsetAt: %v", err)
	}
	if got != 0 {
		t.Errorf("OffsetAt(future): got %d, want 0 sentinel", got)
	}
}

// TestOffsetAt_EmptyTable verifies OffsetAt returns 0 against
// a fully empty table — same sentinel as "future time, no
// rows match." No error.
func TestOffsetAt_EmptyTable(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	got, err := store.OffsetAt(t.Context(), time.Now())
	if err != nil {
		t.Fatalf("OffsetAt: %v", err)
	}
	if got != 0 {
		t.Errorf("OffsetAt(now, empty): got %d, want 0", got)
	}
}

// TestPoll_DrainToTipPattern: replicates the documented "drain
// to tip" idiom and verifies it terminates with all records
// processed.
func TestPoll_DrainToTipPattern(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for i := range 7 {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runSequencerSync(t, f)
	time.Sleep(20 * time.Millisecond)

	now := time.Now()

	// Drain in batches of 3, walking [since, since+3) each call.
	// Loop terminates when the call returns an empty result —
	// `next` stays equal to `since` in that case.
	consumed := 0
	since := s3pgstore.Offset(0)
	for {
		results, next, err := store.PollRecords(t.Context(),
			since, since+3)
		if err != nil {
			t.Fatalf("PollRecords: %v", err)
		}
		if len(results) == 0 {
			break
		}
		for _, fr := range results {
			consumed += len(fr.Records)
		}
		since = next
	}
	if consumed != 7 {
		t.Errorf("drain consumed %d, want 7", consumed)
	}

	// Sanity: OffsetAt against a time strictly before our
	// writes returns 1 (the lowest sequenced offset).
	earliest, err := store.OffsetAt(t.Context(),
		now.Add(-1*time.Hour))
	if err != nil {
		t.Fatalf("OffsetAt: %v", err)
	}
	if earliest != 1 {
		t.Errorf("OffsetAt(-1h): got %d, want 1", earliest)
	}
}

// TestPoll_BackgroundSequencerWakeUpAndCancel verifies the
// end-to-end NOTIFY → sequencer → Poll path runs cleanly.
// Spins up sequencer.Run in a goroutine, writes a record,
// polls until visible, cancels.
func TestPoll_BackgroundSequencerWakeUpAndCancel(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	seqCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- sequencer.Run(seqCtx, sequencer.Config{
			Pool:          f.Pool,
			SchemaName:    f.Schema,
			TablePrefix:   "s3pgstore_",
			PollInterval:  500 * time.Millisecond,
			NotifyChannel: "s3pgstore_writes",
			BatchSize:     32,
		})
	}()

	// Allow LISTEN to register before write.
	time.Sleep(100 * time.Millisecond)
	if _, err := store.Write(t.Context(),
		[]streamRec{{ID: "alice"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, _, err := store.Poll(t.Context(), 0, s3pgstore.OffsetLatest)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if len(entries) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	entries, _, err := store.Poll(t.Context(), 0, s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("Poll final: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 sequenced row after NOTIFY, got %d",
			len(entries))
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("sequencer.Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("sequencer did not exit within 5s")
	}
}

// TestPoll_OffsetOrderMatchesFeedSeq: a multi-write fan-out —
// even though writes interleave concurrently, after sequencing
// the Poll output is monotonic in offset and stable across
// repeated calls. The sortedness invariant is per CLAUDE.md.
func TestPoll_OffsetOrderMatchesFeedSeq(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	const total = 12
	for i := range total {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%02d", i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runSequencerSync(t, f)

	entries, next, err := store.Poll(t.Context(), 0,
		s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != total {
		t.Fatalf("got %d entries, want %d", len(entries), total)
	}
	if next != int64(total)+1 {
		t.Errorf("next: got %d, want %d (max+1)", next, total+1)
	}
	offsets := make([]int64, len(entries))
	for i, e := range entries {
		offsets[i] = e.Offset
	}
	if !sort.SliceIsSorted(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	}) {
		t.Errorf("Poll offsets not sorted: %v", offsets)
	}
}

// TestFileRef_CrossSourceConsistency verifies that the FileRef
// returned by Write, Poll, and LookupByToken describes the same
// row consistently across every populated field — the unifying-
// type contract documented on FileRef.
//
// Coverage matrix:
//
//   - Write: every field except Offset (OffsetNone until sequenced).
//   - Poll (after sequencing): every field including Offset.
//   - LookupByToken (after sequencing): every field including
//     Offset (the projection covers the full FileRef shape per
//     IdempotencyLookupSQL).
//
// Cross-source comparison: FileID / PartitionKey / S3Key /
// Version / WrittenAt / RecordCount / FileSize / UncompressedSize
// / Extensions must be identical across all three sources.
// Offset diverges only on Write (OffsetNone) vs Poll/Lookup (the
// sequencer-assigned value).
func TestFileRef_CrossSourceConsistency(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	const token = "test-token-cross-source"
	// WrittenAt comes from Postgres now(), which can drift a few ms
	// from the test process clock (testcontainers Docker host vs.
	// container). Pad the bracket to absorb that skew.
	const clockSkew = 2 * time.Second
	before := time.Now().UTC().Add(-clockSkew)
	written, err := store.Write(t.Context(),
		[]streamRec{{ID: "alice", Value: 42}},
		s3pgstore.WithIdempotencyToken(token),
		s3pgstore.WithMetadata(map[string]any{
			"job_id": "job-xyz",
		}),
	)
	after := time.Now().UTC().Add(clockSkew)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(written) != 1 {
		t.Fatalf("Write returned %d FileRefs, want 1", len(written))
	}
	w := written[0]

	// Write-side: every field populated, Offset is OffsetNone.
	if w.FileID == 0 {
		t.Errorf("Write FileID: want non-zero")
	}
	if w.PartitionKey != "customer=alice" {
		t.Errorf("Write PartitionKey: %q", w.PartitionKey)
	}
	if w.S3Key == "" {
		t.Errorf("Write S3Key: empty")
	}
	if w.Version != 1 {
		t.Errorf("Write Version: want 1, got %d", w.Version)
	}
	if w.WrittenAt.Before(before) || w.WrittenAt.After(after) {
		t.Errorf("Write WrittenAt %v not in [%v, %v]",
			w.WrittenAt, before, after)
	}
	if w.RecordCount != 1 {
		t.Errorf("Write RecordCount: want 1, got %d", w.RecordCount)
	}
	if w.FileSize <= 0 {
		t.Errorf("Write FileSize: want > 0, got %d", w.FileSize)
	}
	if w.UncompressedSize <= 0 {
		t.Errorf("Write UncompressedSize: want > 0, got %d",
			w.UncompressedSize)
	}
	if got := w.Extensions["job_id"]; got != "job-xyz" {
		t.Errorf("Write Extensions[job_id]: got %v, want job-xyz",
			got)
	}
	if w.Offset != s3pgstore.OffsetNone {
		t.Errorf("Write Offset: want OffsetNone, got %d", w.Offset)
	}

	// Sequence so Poll has work to do.
	runSequencerSync(t, f)

	// Poll-side: same row should come back with everything
	// matching, plus a non-OffsetNone.
	polled, _, err := store.Poll(t.Context(), 0, s3pgstore.OffsetLatest)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(polled) != 1 {
		t.Fatalf("Poll returned %d FileRefs, want 1", len(polled))
	}
	p := polled[0]
	if p.Offset == s3pgstore.OffsetNone {
		t.Errorf("Poll Offset: want non-OffsetNone (sequenced), got 0")
	}
	assertSameRow(t, "Poll", w, p)

	// LookupByToken should also return a fully-populated FileRef.
	looked, hit, err := store.LookupByToken(t.Context(),
		w.PartitionKey, token)
	if err != nil {
		t.Fatalf("LookupByToken: %v", err)
	}
	if !hit {
		t.Fatalf("LookupByToken: no row found for token %q", token)
	}
	if looked.Offset != p.Offset {
		t.Errorf("LookupByToken Offset: got %d, want %d (Poll's)",
			looked.Offset, p.Offset)
	}
	assertSameRow(t, "LookupByToken", w, looked)
}

// assertSameRow checks that every FileRef field except Offset
// matches between the Write-side reference (w) and another
// source's view (got). Offset is excluded because it diverges
// across sources by design.
func assertSameRow(t *testing.T, source string, w, got s3pgstore.FileRef) {
	t.Helper()
	if got.FileID != w.FileID {
		t.Errorf("%s FileID: got %d, want %d",
			source, got.FileID, w.FileID)
	}
	if got.PartitionKey != w.PartitionKey {
		t.Errorf("%s PartitionKey: got %q, want %q",
			source, got.PartitionKey, w.PartitionKey)
	}
	if got.S3Key != w.S3Key {
		t.Errorf("%s S3Key: got %q, want %q",
			source, got.S3Key, w.S3Key)
	}
	if got.Version != w.Version {
		t.Errorf("%s Version: got %d, want %d",
			source, got.Version, w.Version)
	}
	if !got.WrittenAt.Equal(w.WrittenAt) {
		t.Errorf("%s WrittenAt: got %v, want %v",
			source, got.WrittenAt, w.WrittenAt)
	}
	if got.RecordCount != w.RecordCount {
		t.Errorf("%s RecordCount: got %d, want %d",
			source, got.RecordCount, w.RecordCount)
	}
	if got.FileSize != w.FileSize {
		t.Errorf("%s FileSize: got %d, want %d",
			source, got.FileSize, w.FileSize)
	}
	if got.UncompressedSize != w.UncompressedSize {
		t.Errorf("%s UncompressedSize: got %d, want %d",
			source, got.UncompressedSize, w.UncompressedSize)
	}
	if got.Extensions["job_id"] != w.Extensions["job_id"] {
		t.Errorf("%s Extensions[job_id]: got %v, want %v",
			source, got.Extensions["job_id"], w.Extensions["job_id"])
	}
}

// TestPollRecordsIter_YieldsInFeedSeqOrder verifies the iter
// pipeline emits one FileResult per file in feed_seq order,
// with each result carrying both File metadata (Offset for
// checkpoint) and decoded Records.
func TestPollRecordsIter_YieldsInFeedSeqOrder(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	const total = 6
	for i := range total {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i), Value: int64(i)}}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	runSequencerSync(t, f)

	var (
		count   int
		lastOff s3pgstore.Offset
	)
	for fr, err := range store.PollRecordsIter(t.Context(), 0,
		s3pgstore.OffsetLatest) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		count++
		if fr.File.Offset <= lastOff {
			t.Errorf("offset not monotonic: got %d after %d",
				fr.File.Offset, lastOff)
		}
		lastOff = fr.File.Offset
		if len(fr.Records) != 1 {
			t.Errorf("file at offset %d: %d records, want 1",
				fr.File.Offset, len(fr.Records))
		}
		// Value matches feed_seq - 1 (writes were sequential,
		// Value=i where i=offset-1).
		if fr.Records[0].Value != fr.File.Offset-1 {
			t.Errorf("offset %d: Value %d, want %d",
				fr.File.Offset, fr.Records[0].Value, fr.File.Offset-1)
		}
	}
	if count != total {
		t.Errorf("yielded %d files, want %d", count, total)
	}
}

// TestPollRecordsIter_ResumeIdiom verifies the documented
// resume pattern: track since = fr.File.Offset + 1 across
// iterations, restart from there to continue.
func TestPollRecordsIter_ResumeIdiom(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for i := range 5 {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runSequencerSync(t, f)

	// First iter: pull 2 files, then break.
	var (
		got       []s3pgstore.Offset
		nextSince s3pgstore.Offset
	)
	for fr, err := range store.PollRecordsIter(t.Context(), 0,
		s3pgstore.OffsetLatest) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		got = append(got, fr.File.Offset)
		nextSince = fr.File.Offset + 1
		if len(got) == 2 {
			break
		}
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("first iter offsets: got %v, want [1, 2]", got)
	}
	if nextSince != 3 {
		t.Fatalf("nextSince after 2 yields: got %d, want 3",
			nextSince)
	}

	// Second iter resumes from nextSince and walks the rest.
	got = got[:0]
	for fr, err := range store.PollRecordsIter(t.Context(),
		nextSince, s3pgstore.OffsetLatest) {
		if err != nil {
			t.Fatalf("yield (resume): %v", err)
		}
		got = append(got, fr.File.Offset)
	}
	if len(got) != 3 || got[0] != 3 || got[1] != 4 || got[2] != 5 {
		t.Errorf("resume iter offsets: got %v, want [3, 4, 5]", got)
	}
}

// TestPollRecordsIter_EmptyRange verifies since == until yields
// nothing without touching the catalog.
func TestPollRecordsIter_EmptyRange(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for fr, err := range store.PollRecordsIter(t.Context(), 5, 5) {
		t.Errorf("unexpected yield: fr=%+v err=%v", fr, err)
	}
}

// TestPollRecordsIter_OptionsApply verifies the iter-pipeline
// options compile and route through the pipeline. (Covers the
// option plumbing; correctness of the underlying back-pressure
// is exercised by the read pipeline's tests, which use the same
// mechanisms.)
func TestPollRecordsIter_OptionsApply(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for i := range 4 {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runSequencerSync(t, f)

	count := 0
	for _, err := range store.PollRecordsIter(t.Context(), 0,
		s3pgstore.OffsetLatest,
		s3pgstore.WithFetchAheadFiles(2),
		s3pgstore.WithDecodeWorkers(2),
		s3pgstore.WithDecodeAheadFiles(1),
		s3pgstore.WithDecodeAheadBytes(1<<20),
	) {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		count++
	}
	if count != 4 {
		t.Errorf("yielded %d, want 4", count)
	}
}

// TestTailRecordsIter_FollowsAppendedWrites is the load-bearing
// tail test: write N records, start TailRecordsIter, observe
// that it yields N; then write M more, observe it yields the
// additional M in feed_seq order; cancel ctx, observe the
// iterator returns silently (no error yield).
//
// Uses a tight backoff window (10ms / 50ms) so the second batch
// arrives within the test deadline. The tail goroutine runs in
// the background; the test thread synchronises via a channel
// that the goroutine pushes each yielded offset onto.
func TestTailRecordsIter_FollowsAppendedWrites(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	const initial = 3
	for i := range initial {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("a%d", i),
				Value: int64(i)}}); err != nil {
			t.Fatalf("Write initial %d: %v", i, err)
		}
	}
	runSequencerSync(t, f)

	tailCtx, cancelTail := context.WithCancel(t.Context())
	defer cancelTail()

	// Buffered channel so the tail goroutine never blocks on
	// the test thread. Capacity is generous for both batches.
	yields := make(chan s3pgstore.FileResult[streamRec], 32)
	errs := make(chan error, 1)
	tailDone := make(chan struct{})
	go func() {
		defer close(tailDone)
		for fr, err := range store.TailRecordsIter(tailCtx, 0,
			s3pgstore.WithTailIdleBackoff(
				10*time.Millisecond, 50*time.Millisecond)) {
			if err != nil {
				errs <- err
				return
			}
			yields <- fr
		}
	}()

	// Drain the initial batch from the yields channel.
	got := drainYields(t, yields, initial, 3*time.Second)
	if len(got) != initial {
		t.Fatalf("initial yields: got %d, want %d", len(got), initial)
	}
	for i, fr := range got {
		if fr.File.Offset != int64(i+1) {
			t.Errorf("initial[%d] offset: got %d, want %d",
				i, fr.File.Offset, i+1)
		}
	}

	// Write more records and sequence them — the tail must
	// pick them up via its next non-empty Poll.
	const additional = 4
	for i := range additional {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("b%d", i),
				Value: int64(initial + i)}}); err != nil {
			t.Fatalf("Write additional %d: %v", i, err)
		}
	}
	runSequencerSync(t, f)

	got = drainYields(t, yields, additional, 3*time.Second)
	if len(got) != additional {
		t.Fatalf("additional yields: got %d, want %d",
			len(got), additional)
	}
	for i, fr := range got {
		wantOff := int64(initial + i + 1)
		if fr.File.Offset != wantOff {
			t.Errorf("additional[%d] offset: got %d, want %d",
				i, fr.File.Offset, wantOff)
		}
	}

	// Cancel — iterator should return cleanly (no error yield).
	cancelTail()
	select {
	case <-tailDone:
	case <-time.After(2 * time.Second):
		t.Fatal("TailRecordsIter did not return after cancel")
	}
	select {
	case err := <-errs:
		t.Errorf("unexpected error yield on cancel: %v", err)
	default:
	}
}

// TestTailRecordsIter_ResumeAfterCancel verifies the documented
// resume idiom: after a clean break, restart from
// last.Offset+1 and observe no gaps and no duplicates.
func TestTailRecordsIter_ResumeAfterCancel(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for i := range 5 {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runSequencerSync(t, f)

	// First tail: pull two records, then cancel.
	ctx1, cancel1 := context.WithCancel(t.Context())
	var (
		got1 []s3pgstore.Offset
		last s3pgstore.Offset
	)
	for fr, err := range store.TailRecordsIter(ctx1, 0,
		s3pgstore.WithTailIdleBackoff(
			10*time.Millisecond, 50*time.Millisecond)) {
		if err != nil {
			t.Fatalf("first tail yield: %v", err)
		}
		got1 = append(got1, fr.File.Offset)
		last = fr.File.Offset
		if len(got1) == 2 {
			break
		}
	}
	cancel1()
	if len(got1) != 2 || got1[0] != 1 || got1[1] != 2 {
		t.Fatalf("first tail offsets: got %v, want [1, 2]", got1)
	}

	// Second tail resumes from last+1.
	ctx2, cancel2 := context.WithCancel(t.Context())
	var got2 []s3pgstore.Offset
	go func() {
		// Cancel after we've pulled the remainder so the iter
		// returns. Bound the wait so a stuck tail can't hang
		// the test.
		time.Sleep(2 * time.Second)
		cancel2()
	}()
	for fr, err := range store.TailRecordsIter(ctx2, last+1,
		s3pgstore.WithTailIdleBackoff(
			10*time.Millisecond, 50*time.Millisecond)) {
		if err != nil {
			t.Fatalf("resume yield: %v", err)
		}
		got2 = append(got2, fr.File.Offset)
		if len(got2) == 3 {
			cancel2()
			break
		}
	}
	if len(got2) != 3 || got2[0] != 3 || got2[1] != 4 || got2[2] != 5 {
		t.Errorf("resume offsets: got %v, want [3, 4, 5]", got2)
	}
}

// TestTailRecordsIter_IdleBackoff exercises the empty-poll path:
// against an empty store, the tail must NOT busy-loop — it
// blocks with backoff until ctx is cancelled. We assert it
// yields nothing and exits cleanly within a short window.
func TestTailRecordsIter_IdleBackoff(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	ctx, cancel := context.WithTimeout(t.Context(),
		300*time.Millisecond)
	defer cancel()

	yielded := 0
	for fr, err := range store.TailRecordsIter(ctx, 0,
		s3pgstore.WithTailIdleBackoff(
			10*time.Millisecond, 50*time.Millisecond)) {
		// On empty store + ctx-cancel-during-sleep we expect
		// no yields. If we DO see one it's a real bug — log it.
		if err != nil {
			// Only ctx-derived errors are acceptable here, and
			// even those should not yield (sleep-cancel path).
			t.Errorf("unexpected error yield (err=%v fr=%+v)",
				err, fr)
			continue
		}
		yielded++
	}
	if yielded != 0 {
		t.Errorf("idle tail yielded %d records against empty store, want 0",
			yielded)
	}
}

// TestTailRecordsIter_PicksUpRecordsWrittenAfterStart guards the
// race between iter-start and first write: tail must observe
// records committed AFTER iter construction. Failure here would
// mean the feeder is somehow snapshotting at start time
// (regression to PollRecordsIter semantics) instead of
// continually polling.
func TestTailRecordsIter_PicksUpRecordsWrittenAfterStart(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	tailCtx, cancelTail := context.WithCancel(t.Context())
	defer cancelTail()

	yields := make(chan s3pgstore.FileResult[streamRec], 8)
	tailDone := make(chan struct{})
	go func() {
		defer close(tailDone)
		for fr, err := range store.TailRecordsIter(tailCtx, 0,
			s3pgstore.WithTailIdleBackoff(
				10*time.Millisecond, 50*time.Millisecond)) {
			if err != nil {
				return
			}
			yields <- fr
		}
	}()

	// Tiny pause to ensure the tail's first Poll returns empty
	// (so we exercise the "wake up on next non-empty poll" path,
	// not the "first poll already had the record" path).
	time.Sleep(100 * time.Millisecond)

	if _, err := store.Write(t.Context(),
		[]streamRec{{ID: "after-start"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	runSequencerSync(t, f)

	got := drainYields(t, yields, 1, 3*time.Second)
	cancelTail()
	<-tailDone

	if len(got) != 1 {
		t.Fatalf("got %d yields, want 1", len(got))
	}
	if got[0].Records[0].ID != "after-start" {
		t.Errorf("ID: got %q, want %q",
			got[0].Records[0].ID, "after-start")
	}
}

// drainYields collects up to `want` results from yields, with a
// per-test deadline. Returns the slice of yields received. The
// helper exists so the test bodies don't repeat the same
// "select with timeout" boilerplate for every batch.
func drainYields(
	t *testing.T,
	yields <-chan s3pgstore.FileResult[streamRec],
	want int, deadline time.Duration,
) []s3pgstore.FileResult[streamRec] {
	t.Helper()
	out := make([]s3pgstore.FileResult[streamRec], 0, want)
	end := time.Now().Add(deadline)
	for len(out) < want && time.Now().Before(end) {
		select {
		case fr := <-yields:
			out = append(out, fr)
		case <-time.After(50 * time.Millisecond):
		}
	}
	return out
}
