package metricsrelay

import (
	"math"
	"path/filepath"
	"testing"
	"time"
)

// openStoreAt opens a ledger store on the named file, closed at cleanup —
// the one opener the multi-owner and reopen tests start from.
func openStoreAt(t *testing.T, path string, maxAge time.Duration) *Store {
	t.Helper()
	s, err := OpenStore(path, maxAge)
	if err != nil {
		t.Fatalf("OpenStore(%s): %v", path, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// openTempStore opens a ledger store on a fresh file in the test's temp
// dir — the one opener every single-owner store test starts from.
func openTempStore(t *testing.T, maxAge time.Duration) *Store {
	t.Helper()
	return openStoreAt(t, filepath.Join(t.TempDir(), "metrics.db"), maxAge)
}

// TestStoreDSN pins the connection string every writer on the shared file
// opens through: the path with the busy timeout and WAL pragmas appended —
// without them a colliding writer fails immediately (sqlite's default busy
// timeout is zero) and drops its write.
func TestStoreDSN(t *testing.T) {
	got := StoreDSN("/var/daedalus/metrics.db")
	want := "/var/daedalus/metrics.db?_pragma=busy_timeout%3d5000&_pragma=journal_mode%3dWAL"
	if got != want {
		t.Errorf("StoreDSN = %q, want %q", got, want)
	}
}

// TestStoreRoundTrip pins the ledger table's faithfulness: one entry with
// every field set reads back whole — every json-tagged field its column,
// At at nanosecond precision (the UnixNano round-trip), CostUSD included
// (the column is faithful to the Entry; nothing in the relay writes it) —
// and the ordering contracts hold: Ascending oldest-first, Query
// newest-first. An empty store serves its empty faces.
func TestStoreRoundTrip(t *testing.T) {
	s := openTempStore(t, 0)

	if got := s.Len(); got != 0 {
		t.Errorf("Len(empty) = %d, want 0", got)
	}
	if asc := s.Ascending(); asc != nil {
		t.Errorf("Ascending(empty) = %v, want nil", asc)
	}
	if entries, total, size := s.Query("", 1, 0); entries != nil || total != 0 || size != 50 {
		t.Errorf("Query(empty) = (%d entries, total %d, size %d), want nil/0/50 (the default page)", len(entries), total, size)
	}

	at := time.Unix(1700000000, 123456789)
	in := Entry{
		At: at, Worker: "w-a", Queue: "listq", Alias: "quota-a", Wire: WireAnthropic,
		Task: "listq-iss9", Role: "dev", Round: "3", Model: "glm-4",
		InputTokens: 10, OutputTokens: 5, CacheReadTokens: 4, CacheWriteTokens: 2,
		CostUSD: 0.5, DurationMS: 1500, Status: 429, Source: SourceOtel,
	}
	s.Append(in)

	asc := s.Ascending()
	if len(asc) != 1 {
		t.Fatalf("Ascending = %d entries, want 1", len(asc))
	}
	got := asc[0]
	if got.Worker != in.Worker || got.Queue != in.Queue || got.Alias != in.Alias ||
		got.Wire != in.Wire || got.Task != in.Task || got.Role != in.Role ||
		got.Round != in.Round || got.Model != in.Model ||
		got.InputTokens != in.InputTokens || got.OutputTokens != in.OutputTokens ||
		got.CacheReadTokens != in.CacheReadTokens || got.CacheWriteTokens != in.CacheWriteTokens ||
		got.CostUSD != in.CostUSD || got.DurationMS != in.DurationMS ||
		got.Status != in.Status || got.Source != in.Source {
		t.Errorf("round-tripped entry = %+v, want every field carried (%+v)", got, in)
	}
	if !got.At.Equal(time.Unix(0, at.UnixNano())) {
		t.Errorf("round-tripped At = %v, want the nanosecond-precision write back", got.At)
	}

	// Newest first: two more entries, appended out of At order.
	s.Append(Entry{At: at.Add(-2 * time.Minute), Worker: "w-a"})
	s.Append(Entry{At: at.Add(time.Minute), Worker: "w-a"})
	entries, total, _ := s.Query("", 1, 10)
	if total != 3 || len(entries) != 3 {
		t.Fatalf("Query = %d entries total %d, want all three", len(entries), total)
	}
	for i, want := range []time.Time{at.Add(time.Minute), at, at.Add(-2 * time.Minute)} {
		if !entries[i].At.Equal(time.Unix(0, want.UnixNano())) {
			t.Errorf("Query[%d].At = %v, want %v (newest first)", i, entries[i].At, want)
		}
	}
	asc = s.Ascending()
	if !asc[0].At.Equal(time.Unix(0, at.Add(-2*time.Minute).UnixNano())) || !asc[2].At.Equal(time.Unix(0, at.Add(time.Minute).UnixNano())) {
		t.Errorf("Ascending spans %v..%v, want oldest first", asc[0].At, asc[2].At)
	}
}

// TestStoreQueryPaging pins the store-side paging contract: page counts
// from 1, an unset page size is the 50 default, a worker filter narrows
// while total counts only the filtered rows, a page past the last one is
// empty but still carries the total, and a MaxInt page serves empty off the
// int64 guard instead of wrapping its offset.
func TestStoreQueryPaging(t *testing.T) {
	s := openTempStore(t, 0)
	base := time.Unix(1700000000, 0)
	for i := 0; i < 7; i++ {
		worker := "w1"
		if i%3 == 2 {
			worker = "w2"
		}
		s.Append(Entry{At: base.Add(time.Duration(i) * time.Minute), Worker: worker})
	}

	entries, total, size := s.Query("", 1, 3)
	if total != 7 || size != 3 || len(entries) != 3 {
		t.Fatalf("Query(page 1) = %d entries total %d size %d, want 3/7/3", len(entries), total, size)
	}
	if entries, total, size := s.Query("", 3, 3); total != 7 || size != 3 || len(entries) != 1 {
		t.Errorf("Query(last page) = %d entries total %d size %d, want 1/7/3", len(entries), total, size)
	}
	if entries, total, _ := s.Query("", 4, 3); entries != nil || total != 7 {
		t.Errorf("Query(overflowed page) = %d entries total %d, want nil with the total kept", len(entries), total)
	}

	entries, total, size = s.Query("w2", 0, 0)
	if total != 2 || size != 50 || len(entries) != 2 {
		t.Fatalf("Query(w2, unset page/size) = %d entries total %d size %d, want 2/2/50", len(entries), total, size)
	}
	for _, e := range entries {
		if e.Worker != "w2" {
			t.Errorf("filter let a %q entry through", e.Worker)
		}
	}
	if _, total, _ := s.Query("ghost", 1, 10); total != 0 {
		t.Errorf("Query(unknown worker) total = %d, want 0", total)
	}

	entries, total, size = s.Query("", math.MaxInt, 10)
	if entries != nil || total != 7 || size != 10 {
		t.Errorf("Query(MaxInt page) = %d entries total %d size %d, want nil/7/10 (the guard, no wrap)", len(entries), total, size)
	}
}

// TestStorePrune pins the ledger's retention: an open with a maxAge drops
// the aged rows once at boot and keeps the fresh ones; a maxAge of zero (or
// less) keeps everything forever; and the hourly ride-along prunes at most
// once per hour of appends — an aged row landing mid-hour survives the
// appends, and the next hour's first append drops it.
func TestStorePrune(t *testing.T) {
	t.Run("boot prune drops the aged rows", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "metrics.db")
		seed := openStoreAt(t, path, 0) // no retention: both rows persist
		now := time.Now()
		seed.Append(Entry{At: now.Add(-3 * time.Hour), Worker: "old"})
		seed.Append(Entry{At: now, Worker: "fresh"})
		seed.Close()

		s := openStoreAt(t, path, time.Hour)
		if got := s.Ascending(); len(got) != 1 || got[0].Worker != "fresh" {
			t.Errorf("after the boot prune = %+v, want only the fresh row", got)
		}
	})

	t.Run("zero maxAge keeps everything forever", func(t *testing.T) {
		s := openTempStore(t, 0)
		s.Append(Entry{At: time.Now().Add(-2 * 365 * 24 * time.Hour), Worker: "ancient"})
		if got := s.Len(); got != 1 {
			t.Errorf("Len = %d, want the ancient row kept (maxAge 0 keeps all)", got)
		}
	})

	t.Run("the prune rides appends at most once an hour", func(t *testing.T) {
		s := openTempStore(t, time.Hour) // the boot prune stores this hour's bucket
		// An aged row landing mid-hour: the appends' prune pass is
		// throttled to one per hour, so the row survives them.
		s.Append(Entry{At: time.Now().Add(-3 * time.Hour), Worker: "aged"})
		if got := s.Len(); got != 1 {
			t.Fatalf("Len = %d, want the aged row to survive the throttled hour", got)
		}
		// The next hour's first append (the test forces the bucket over)
		// carries the prune: the aged row goes, the triggering append stays.
		s.lastPrune.Store(0)
		s.Append(Entry{At: time.Now(), Worker: "fresh"})
		got := s.Ascending()
		if len(got) != 1 || got[0].Worker != "fresh" {
			t.Errorf("after the hourly prune = %+v, want only the fresh row", got)
		}
	})
}

// TestLedgerSQLiteModeRoutesThroughTheStore pins the sqlite-mode ledger
// (the daemon's shape): Append, Len, Query, and Ascending all serve the
// store's table, the append observer still fires on every append, and the
// request-side page cap clamps before the store is asked.
func TestLedgerSQLiteModeRoutesThroughTheStore(t *testing.T) {
	s := openTempStore(t, 0)
	l := NewLedger(s)

	var seen []Entry
	l.SetOnAppend(func(e Entry) { seen = append(seen, e) })
	base := time.Unix(1700000000, 0)
	l.Append(Entry{At: base.Add(time.Minute), Worker: "w", Task: "newer"})
	l.Append(Entry{At: base, Worker: "w", Task: "older"})
	if len(seen) != 2 || seen[1].Task != "older" {
		t.Errorf("observer saw %+v, want both appends delivered", seen)
	}

	if got := l.Len(); got != 2 {
		t.Errorf("Len = %d, want the store's row count", got)
	}
	entries, total, size := l.Query("", 1, 1)
	if total != 2 || size != 1 || len(entries) != 1 || entries[0].Task != "newer" {
		t.Errorf("Query = (%+v, total %d, size %d), want the newest row through the store", entries, total, size)
	}
	asc := l.Ascending()
	if len(asc) != 2 || asc[0].Task != "older" || asc[1].Task != "newer" {
		t.Errorf("Ascending = %+v, want the store's oldest-first rows", asc)
	}

	_, _, size = l.Query("", 1, math.MaxInt)
	if size != ledgerPageSizeCap {
		t.Errorf("oversized page size = %d, want the clamp to %d before the store", size, ledgerPageSizeCap)
	}
}
