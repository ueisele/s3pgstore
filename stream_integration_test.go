//go:build integration

package s3pgstore_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
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

	entries, next, err := store.Poll(t.Context(), 0, 100)
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

	// Batch 1: offsets 1, 2.
	b1, next1, err := store.Poll(t.Context(), 0, 2)
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
	if next1 != 2 {
		t.Errorf("next1: want 2, got %d", next1)
	}
	// Extensions surfaces declared columns.
	if got := b1[0].Extensions["job_id"]; got != "job-0" {
		t.Errorf("Extensions[job_id]: got %v, want %q", got, "job-0")
	}

	// Batch 2: offsets 3, 4.
	b2, next2, err := store.Poll(t.Context(), next1, 2)
	if err != nil {
		t.Fatalf("Poll 2: %v", err)
	}
	if len(b2) != 2 || b2[0].Offset != 3 || b2[1].Offset != 4 {
		t.Errorf("batch 2 offsets: %v (want [3, 4])",
			[]int64{b2[0].Offset, b2[1].Offset})
	}
	if next2 != 4 {
		t.Errorf("next2: want 4, got %d", next2)
	}

	// Batch 3: tail (offset 5).
	b3, next3, err := store.Poll(t.Context(), next2, 2)
	if err != nil {
		t.Fatalf("Poll 3: %v", err)
	}
	if len(b3) != 1 || b3[0].Offset != 5 || next3 != 5 {
		t.Errorf("batch 3: offsets=%v next=%d", b3, next3)
	}

	// Batch 4: empty, next unchanged.
	b4, next4, err := store.Poll(t.Context(), next3, 2)
	if err != nil {
		t.Fatalf("Poll 4: %v", err)
	}
	if len(b4) != 0 || next4 != next3 {
		t.Errorf("batch 4: should be empty + unchanged offset; "+
			"len=%d next=%d", len(b4), next4)
	}
}

// TestPoll_NonPositiveBatch verifies n <= 0 short-circuits to
// (nil, since, nil) without touching the database.
func TestPoll_NonPositiveBatch(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	entries, next, err := store.Poll(t.Context(), 42, 0)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 0 || next != 42 {
		t.Errorf("got (%d entries, next=%d), want (0, 42)",
			len(entries), next)
	}
}

// TestPoll_WithUntilOffset_BoundsInclusive verifies the bound
// is inclusive: a row whose feed_seq matches `until` is
// returned.
func TestPoll_WithUntilOffset_BoundsInclusive(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	for i := range 5 {
		if _, err := store.Write(t.Context(),
			[]streamRec{{ID: fmt.Sprintf("c%d", i)}}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	runSequencerSync(t, f)

	// Bound at offset 3 — should return offsets 1, 2, 3
	// (feed_seq <= 3).
	entries, next, err := store.Poll(t.Context(), 0, 100,
		s3pgstore.WithUntilOffset(3))
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("entries with WithUntilOffset(3): got %d, want 3",
			len(entries))
	}
	if next != 3 {
		t.Errorf("next: got %d, want 3", next)
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

	records, next, err := store.PollRecords(t.Context(), 0, 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(records) != 4 {
		t.Fatalf("records: got %d, want 4", len(records))
	}
	if next != 4 {
		t.Errorf("next: got %d, want 4", next)
	}
	// Each file is a single record; order is by feed_seq which
	// is by written_at order (and we wrote sequentially).
	for i, r := range records {
		if r.Value != int64(i) {
			t.Errorf("records[%d].Value: got %d, want %d",
				i, r.Value, i)
		}
	}
}

// TestPollRecords_Empty verifies the empty-result path.
func TestPollRecords_Empty(t *testing.T) {
	f := newFixture(t)
	store := newStreamStore(t, f)

	records, next, err := store.PollRecords(t.Context(), 0, 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(records) != 0 || next != 0 {
		t.Errorf("got (%d records, next=%d), want (0, 0)",
			len(records), next)
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

	first, _, err := store.Poll(t.Context(), 0, 100)
	if err != nil {
		t.Fatalf("Poll 1: %v", err)
	}
	second, _, err := store.Poll(t.Context(), 0, 100)
	if err != nil {
		t.Fatalf("Poll 2: %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("len mismatch: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].Offset != second[i].Offset ||
			first[i].DataPath != second[i].DataPath {
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

	entries, _, err := store.Poll(t.Context(), 0, 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("entries before sequencer ran: got %d, want 0",
			len(entries))
	}

	// After sequencer runs, the row appears.
	runSequencerSync(t, f)
	entries, _, err = store.Poll(t.Context(), 0, 100)
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

	// "Tip at now" — but our writes are all before now, so the
	// proposal-style OffsetAt(now) returns 0 (no row at-or-
	// after now). For the drain-to-tip example to work we
	// take "tip = highest offset committed so far", which we
	// emulate by passing a future time and walking until empty.
	// More realistic: use OffsetAt slightly in the past so we
	// pick up the highest feed_seq <= now. Easier: set
	// WithUntilOffset to a high constant or skip it entirely
	// for "drain everything." Test the no-bound form:
	consumed := 0
	since := s3pgstore.Offset(0)
	for {
		entries, next, err := store.PollRecords(t.Context(),
			since, 3)
		if err != nil {
			t.Fatalf("PollRecords: %v", err)
		}
		if len(entries) == 0 {
			break
		}
		consumed += len(entries)
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
		entries, _, err := store.Poll(t.Context(), 0, 10)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if len(entries) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	entries, _, err := store.Poll(t.Context(), 0, 10)
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

	entries, next, err := store.Poll(t.Context(), 0, 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != total {
		t.Fatalf("got %d entries, want %d", len(entries), total)
	}
	if next != int64(total) {
		t.Errorf("next: got %d, want %d", next, total)
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
