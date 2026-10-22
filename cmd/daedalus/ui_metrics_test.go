package main

import (
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
	"github.com/ozzono/daedalus/internal/workflows"
)

// TestUsageFormatters pins the usage tables' and tiles' renderers: token
// counts humanize in three tiers, costs carry four decimals until they are
// large enough to read as cents, and a saved rate renders back into the
// form's input in exact decimal form — never scientific notation, which an
// inputmode=decimal field cannot round-trip.
func TestUsageFormatters(t *testing.T) {
	t.Run("fmtTokens tiers", func(t *testing.T) {
		cases := map[int64]string{
			0:         "0",
			999:       "999",
			1000:      "1.0k",
			12_345:    "12.3k",
			750_000:   "750.0k",
			1_000_000: "1.00M",
			2_500_000: "2.50M",
		}
		for n, want := range cases {
			if got := fmtTokens(n); got != want {
				t.Errorf("fmtTokens(%d) = %q, want %q", n, got, want)
			}
		}
	})

	t.Run("fmtUSD precision", func(t *testing.T) {
		cases := map[float64]string{
			0:       "$0.0000",
			0.5:     "$0.5000",
			99.99:   "$99.9900",
			100:     "$100.00",
			123.456: "$123.46",
		}
		for v, want := range cases {
			if got := fmtUSD(v); got != want {
				t.Errorf("fmtUSD(%v) = %q, want %q", v, got, want)
			}
		}
	})

	t.Run("formatRate exact decimals", func(t *testing.T) {
		cases := map[float64]string{
			0:        "0",
			3:        "3",
			1.5:      "1.5",
			0.15:     "0.15",
			0.000015: "0.000015",
		}
		for v, want := range cases {
			if got := formatRate(v); got != want {
				t.Errorf("formatRate(%v) = %q, want %q", v, got, want)
			}
		}
	})
}

// TestEntryCostPricesEachSide pins the render-time pricing: input and
// cache-write bill at the in rate (there is deliberately no fourth rate),
// output at the out rate, cache reads at the cached rate — USD per 1M
// tokens throughout.
func TestEntryCostPricesEachSide(t *testing.T) {
	e := metricsrelay.Entry{
		InputTokens:      1_000_000,
		CacheWriteTokens: 500_000,
		OutputTokens:     2_000_000,
		CacheReadTokens:  4_000_000,
	}
	// (1M + 0.5M)/1M * 3 + 2M/1M * 2 + 4M/1M * 1 = 4.5 + 4 + 4.
	if got := entryCost(e, modelPrice{In: 3, Out: 2, Cached: 1}); math.Abs(got-12.5) > 1e-9 {
		t.Errorf("entryCost = %v, want 12.5", got)
	}
	if got := entryCost(e, modelPrice{}); got != 0 {
		t.Errorf("entryCost at zero rates = %v, want 0", got)
	}
}

// TestModelPriceFileContract pins the price table's record in the daemon
// dir: a missing file is the ordinary first boot and loads empty, a save
// writes the file and reads back identically, a save is an upsert that
// never deletes (a model absent from the post survives), and a hand-mangled
// file loads empty too — refusing the boot over one bad file would trade
// every view for it — with the next save rewriting it whole.
func TestModelPriceFileContract(t *testing.T) {
	dir := useDaemonDir(t)
	path := filepath.Join(dir, "metrics-models.json")

	if p := loadModelPrices(); p != nil {
		t.Errorf("loadModelPrices(no file) = %v, want nil", p)
	}

	m1 := modelPrice{In: 3, Out: 0.000015, Cached: 0.15}
	if err := saveModelPrices(map[string]modelPrice{"m1": m1}); err != nil {
		t.Fatalf("saveModelPrices: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the save wrote no file at %s: %v", path, err)
	}
	if p := loadModelPrices(); len(p) != 1 || p["m1"] != m1 {
		t.Errorf("loadModelPrices(after save) = %+v, want the saved table", p)
	}

	if err := saveModelPrices(map[string]modelPrice{"m2": {In: 1}}); err != nil {
		t.Fatalf("saveModelPrices(second): %v", err)
	}
	p := loadModelPrices()
	if p["m1"] != m1 || p["m2"] != (modelPrice{In: 1}) {
		t.Errorf("loadModelPrices(after upsert) = %+v, want m1 preserved and m2 added", p)
	}

	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if p := loadModelPrices(); len(p) != 0 {
		t.Errorf("loadModelPrices(corrupt) = %v, want an empty table", p)
	}
	if err := saveModelPrices(map[string]modelPrice{"m9": {In: 2}}); err != nil {
		t.Fatalf("saveModelPrices(after corrupt): %v", err)
	}
	if p := loadModelPrices(); len(p) != 1 || p["m9"].In != 2 {
		t.Errorf("loadModelPrices(after rewrite) = %+v, want the corrupt file replaced whole", p)
	}
}

// TestParseRateTakesFiniteNonNegativeOnly pins the rate decoder: only
// finite non-negative numbers are taken — an unparsable, negative, NaN, or
// infinite value lands as 0, never an error (a non-finite rate would also
// make the JSON save fail), and an absent field is 0 too.
func TestParseRateTakesFiniteNonNegativeOnly(t *testing.T) {
	s := func(v string) *string { return &v }
	cases := []struct {
		in   *string
		want float64
	}{
		{nil, 0},
		{s("3"), 3},
		{s(" 0.15 "), 0.15},
		{s("0"), 0},
		{s(""), 0},
		{s("-1"), 0},
		{s("abc"), 0},
		{s("NaN"), 0},
		{s("Inf"), 0},
		{s("+Inf"), 0},
		{s("1e999"), 0}, // ParseFloat range error
	}
	for _, c := range cases {
		if got := parseRate(c.in); got != c.want {
			t.Errorf("parseRate(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestParseModelPricesFormContract pins the submitted form's decode: a row
// is written when any of its three fields is present (the browser submits
// every row, so clearing a row prices the model at 0), absent models are
// absent from the upsert (a save never deletes), only the form's own
// editable rows are taken — a forged post can neither price the unknown
// bucket, invent models, nor reach a model under a field name the form
// never emits — and repeated names keep their first value.
func TestParseModelPricesFormContract(t *testing.T) {
	pricable := map[string]bool{"m1": true, "m2": true}

	upsert := parseModelPrices(url.Values{
		"in[m1]":     {"3"},
		"out[m1]":    {"2"},
		"cached[m1]": {"1.5"},
	}, pricable)
	if len(upsert) != 1 || upsert["m1"] != (modelPrice{In: 3, Out: 2, Cached: 1.5}) {
		t.Errorf("full row = %+v, want m1 at {3 2 1.5}", upsert)
	}

	upsert = parseModelPrices(url.Values{"in[m1]": {""}, "out[m1]": {""}, "cached[m1]": {""}}, pricable)
	if upsert["m1"] != (modelPrice{}) {
		t.Errorf("cleared row = %+v, want m1 written at zeros", upsert)
	}

	upsert = parseModelPrices(url.Values{"out[m2]": {"4"}}, pricable)
	if len(upsert) != 1 || upsert["m2"] != (modelPrice{Out: 4}) {
		t.Errorf("partial row = %+v, want m2 written with out=4 alone", upsert)
	}

	upsert = parseModelPrices(url.Values{"in[m1]": {"3", "9"}}, pricable)
	if upsert["m1"].In != 3 {
		t.Errorf("repeated name = %+v, want the first value kept", upsert)
	}

	upsert = parseModelPrices(url.Values{
		"in[ghost]":   {"9"},  // a model the ledger does not know
		"in[unknown]": {"9"},  // the unknown bucket is never pricable
		"foo[m1]":     {"9"},  // a field prefix the form never emits
		"in[m1":       {"9"},  // no closing bracket
		"worker":      {"w1"}, // a plain key
	}, pricable)
	if len(upsert) != 0 {
		t.Errorf("junk form = %+v, want an empty upsert", upsert)
	}
}

// TestLedgerEntriesReadAscending pins the one full-ring read every fold
// builds on: Query returns newest-first, ledgerEntries hands back ascending
// by At — the first/last bookkeeping in the folds (the loop freeze, the
// drift span, the Last column) depends on chronological order — and a nil
// ledger reads nil so every view renders its empty face.
func TestLedgerEntriesReadAscending(t *testing.T) {
	useDaemonDir(t)
	led := &metricsrelay.Ledger{}
	t0 := time.Now().Add(-time.Hour)
	led.Append(metricsrelay.Entry{At: t0.Add(2 * time.Minute), Worker: "w"})
	led.Append(metricsrelay.Entry{At: t0, Worker: "w"})
	led.Append(metricsrelay.Entry{At: t0.Add(time.Minute), Worker: "w"})

	got := newUIServer(config.Config{}, led, nil).ledgerEntries()
	if len(got) != 3 {
		t.Fatalf("ledgerEntries = %d entries, want 3", len(got))
	}
	for i, want := range []time.Time{t0, t0.Add(time.Minute), t0.Add(2 * time.Minute)} {
		if !got[i].At.Equal(want) {
			t.Errorf("ledgerEntries[%d].At = %v, want %v (ascending)", i, got[i].At, want)
		}
	}
	if e := newUIServer(config.Config{}, nil, nil).ledgerEntries(); e != nil {
		t.Errorf("ledgerEntries(nil ledger) = %v, want nil", e)
	}
}

// TestFoldUsageRows pins the usage fold behind both tables: the token sums,
// the distinct keyed runs, the dev/test/review split, the wire-aware
// status split and cache-hit ratio (anthropic reports reads beside input —
// denominator in + read; openai reports them inside prompt_tokens —
// denominator in), nearest-rank p50/p95, the trailing-hour rate, the
// render-time cost of the priced models only, and the ordering by calls
// descending with the key breaking ties. The same entries folded by worker
// price through each row's own models, not the row key.
func TestFoldUsageRows(t *testing.T) {
	now := time.Now()
	entries := []metricsrelay.Entry{
		// mA, a keyed review three hours back: outside the trailing hour,
		// inside the window.
		{At: now.Add(-3 * time.Hour), Worker: "w-a", Wire: metricsrelay.WireAnthropic, Model: "mA",
			Task: "t1", Role: string(activities.RoleDevReview), Round: "2",
			DurationMS: 1000, Status: 200},
		// mA, a keyed dev call on the anthropic wire: cache reads ride
		// beside input_tokens there.
		{At: now.Add(-10 * time.Second), Worker: "w-a", Wire: metricsrelay.WireAnthropic, Model: "mA",
			Task: "t1", Role: string(activities.RoleDev), Round: "1",
			InputTokens: 1000, OutputTokens: 2000, CacheReadTokens: 500, CacheWriteTokens: 100,
			DurationMS: 2000, Status: 200},
		// mA, a second keyed run and a 429.
		{At: now.Add(-5 * time.Second), Worker: "w-a", Wire: metricsrelay.WireAnthropic, Model: "mA",
			Task: "t2", Role: string(activities.RoleTest), Round: "1",
			InputTokens: 500, OutputTokens: 500, DurationMS: 3000, Status: 429},
		// mB, an unkeyed openai-wire call: cached_tokens ride inside
		// prompt_tokens, and a 5xx lands in the failure column.
		{At: now.Add(-30 * time.Second), Worker: "w-b", Wire: metricsrelay.WireOpenAI, Model: "mB",
			InputTokens: 1000, CacheReadTokens: 200, OutputTokens: 400,
			DurationMS: 3000, Status: 500},
		// A failed call carrying no model: the unknown bucket, rate-less.
		{At: now.Add(-20 * time.Second), Worker: "w-b", Wire: metricsrelay.WireOpenAI,
			Status: 401},
	}

	if got := usageModelKey(metricsrelay.Entry{Model: ""}); got != "unknown" {
		t.Errorf("usageModelKey(empty model) = %q, want unknown", got)
	}
	if got := usageModelKey(metricsrelay.Entry{Model: "mA"}); got != "mA" {
		t.Errorf("usageModelKey(mA) = %q", got)
	}
	if got := usageWorkerKey(entries[0]); got != "w-a" {
		t.Errorf("usageWorkerKey = %q, want the worker", got)
	}

	prices := map[string]modelPrice{"mA": {In: 1, Out: 2, Cached: 0.5}}
	rows := foldUsage(entries, now, prices, usageModelKey)
	if len(rows) != 3 {
		t.Fatalf("foldUsage = %d rows, want 3 (mA, mB, unknown)", len(rows))
	}

	mA := rows[0]
	if mA.Key != "mA" {
		t.Fatalf("first row = %q, want mA (most calls)", mA.Key)
	}
	if mA.Calls != 3 || mA.In != 1500 || mA.Out != 2500 || mA.CacheRead != 500 || mA.CacheWrite != 100 {
		t.Errorf("mA sums = %+v, want 3 calls, 1500 in, 2500 out, 500 read, 100 written", mA)
	}
	if mA.Runs != 2 || mA.Dev != 1 || mA.Test != 1 || mA.Review != 1 {
		t.Errorf("mA splits = %+v, want 2 runs, dev/test/review each 1", mA)
	}
	if mA.OK2xx != 2 || mA.R429 != 1 || mA.R4xx != 0 || mA.R5xx != 0 {
		t.Errorf("mA statuses = %+v, want 2xx=2, 429=1", mA)
	}
	// Reads 500 over the anthropic denominator in + read = 2000.
	if mA.HitPct != "25.0%" {
		t.Errorf("mA HitPct = %q, want 25.0%%", mA.HitPct)
	}
	if mA.P50 != "2s" || mA.P95 != "3s" {
		t.Errorf("mA durations = %q / %q, want nearest-rank 2s / 3s", mA.P50, mA.P95)
	}
	if mA.RPM != "0.0" { // two calls in the trailing hour: 2/60 rounds to 0.0
		t.Errorf("mA RPM = %q, want 0.0", mA.RPM)
	}
	if mA.Last != "just now" {
		t.Errorf("mA Last = %q, want the newest entry's just now", mA.Last)
	}
	if !mA.Priced || math.Abs(mA.Cost-0.00685) > 1e-12 {
		t.Errorf("mA cost = (%v, %v), want priced at 0.00685", mA.Priced, mA.Cost)
	}

	mB := rows[1]
	if mB.Key != "mB" {
		t.Fatalf("second row = %q, want mB (one call, key order over the tie)", mB.Key)
	}
	if mB.Runs != 0 || mB.Dev != 0 || mB.Test != 0 || mB.Review != 0 {
		t.Errorf("mB splits = %+v, want an unkeyed call attributing nothing", mB)
	}
	// The openai wire's denominator is input alone: 200/1000.
	if mB.HitPct != "20.0%" {
		t.Errorf("mB HitPct = %q, want 20.0%%", mB.HitPct)
	}
	if mB.OK2xx != 0 || mB.R429 != 0 || mB.R4xx != 0 || mB.R5xx != 1 {
		t.Errorf("mB statuses = %+v, want the 500 in the 5xx column", mB)
	}
	if mB.Priced || mB.Cost != 0 {
		t.Errorf("mB cost = (%v, %v), want unpriced", mB.Priced, mB.Cost)
	}
	if mB.P50 != "3s" || mB.P95 != "3s" {
		t.Errorf("mB durations = %q / %q, want 3s", mB.P50, mB.P95)
	}

	unknown := rows[2]
	if unknown.Key != "unknown" || unknown.Calls != 1 || unknown.R4xx != 1 {
		t.Errorf("unknown row = %+v, want the 401 in the unknown bucket's 4xx", unknown)
	}
	if unknown.HitPct != "—" {
		t.Errorf("unknown HitPct = %q, want the dash (no tokens, no ratio)", unknown.HitPct)
	}
	if unknown.Priced {
		t.Errorf("unknown row priced = %v, want the bucket never priced", unknown.Priced)
	}

	// The same window folded by worker: w-a prices through mA, w-b's
	// models are both unpriced.
	wrows := foldUsage(entries, now, prices, usageWorkerKey)
	if len(wrows) != 2 || wrows[0].Key != "w-a" || wrows[1].Key != "w-b" {
		t.Fatalf("worker fold = %+v, want w-a then w-b", wrows)
	}
	if !wrows[0].Priced || math.Abs(wrows[0].Cost-0.00685) > 1e-12 {
		t.Errorf("w-a cost = (%v, %v), want priced through its models", wrows[0].Priced, wrows[0].Cost)
	}
	if wrows[1].Priced || wrows[1].Cost != 0 || wrows[1].Calls != 2 {
		t.Errorf("w-b row = %+v, want 2 unpriced calls", wrows[1])
	}
}

// TestLatencyDur pins the usage tables' latency formatter: per-call figures
// round to milliseconds under a second — whole-second rounding collapses
// fast calls into fabricated zeros (a 400ms p50 reading "0s") — and defer
// to the runs-table humanizer above it at a second and beyond.
func TestLatencyDur(t *testing.T) {
	for _, c := range []struct {
		d    time.Duration
		want string
	}{
		{0, "0s"}, // a genuine zero recording reads as one
		{100 * time.Millisecond, "100ms"},
		{400 * time.Millisecond, "400ms"},
		{999 * time.Millisecond, "999ms"},
		{time.Second, "1s"},             // the boundary defers to dur
		{1500 * time.Millisecond, "2s"}, // dur's whole-second rounding
		{90 * time.Second, "1m30s"},
	} {
		if got := latencyDur(c.d); got != c.want {
			t.Errorf("latencyDur(%s) = %q, want %q", c.d, got, c.want)
		}
	}
}

// TestFoldUsageSubSecondLatencies pins the P50/P95 columns routing through
// the latency formatter: the sub-second tail renders as measured — the
// nearest-rank percentiles of the 100/200/300ms shape land at 200ms/300ms,
// not the "0s" a whole-second render fabricated — while a row whose p95
// crosses a second keeps the runs-table humanizer above it.
func TestFoldUsageSubSecondLatencies(t *testing.T) {
	now := time.Now()
	entries := []metricsrelay.Entry{
		{At: now.Add(-3 * time.Second), Worker: "w", Wire: metricsrelay.WireOpenAI, Model: "fast",
			DurationMS: 100, Status: 200},
		{At: now.Add(-2 * time.Second), Worker: "w", Wire: metricsrelay.WireOpenAI, Model: "fast",
			DurationMS: 200, Status: 200},
		{At: now.Add(-1 * time.Second), Worker: "w", Wire: metricsrelay.WireOpenAI, Model: "fast",
			DurationMS: 300, Status: 200},
		{At: now.Add(-4 * time.Second), Worker: "w", Wire: metricsrelay.WireOpenAI, Model: "mixed",
			DurationMS: 100, Status: 200},
		{At: now.Add(-5 * time.Second), Worker: "w", Wire: metricsrelay.WireOpenAI, Model: "mixed",
			DurationMS: 1500, Status: 200},
	}
	rows := foldUsage(entries, now, nil, usageModelKey)
	if len(rows) != 2 {
		t.Fatalf("foldUsage = %d rows, want 2 (fast, mixed)", len(rows))
	}
	if rows[0].Key != "fast" {
		t.Fatalf("first row = %q, want fast (most calls)", rows[0].Key)
	}
	if rows[0].P50 != "200ms" || rows[0].P95 != "300ms" {
		t.Errorf("fast latencies = %q / %q, want 200ms / 300ms", rows[0].P50, rows[0].P95)
	}
	// mixed: sorted [100, 1500] — p50 = 100ms, p95 = the 1500ms tail,
	// which dur renders at whole-second scale.
	if rows[1].P50 != "100ms" || rows[1].P95 != "2s" {
		t.Errorf("mixed latencies = %q / %q, want 100ms / 2s", rows[1].P50, rows[1].P95)
	}
}

// TestFoldLoopsAverages pins the loop tiles' fold: per keyed run, the gap
// from the last implementation call to the first matching review — first
// review only (later review rounds would skew the reading) and the
// implementation side frozen at that review, so a fix round after it
// cannot turn the gap negative. Runs missing either side drop out, and so
// do unkeyed rounds.
func TestFoldLoopsAverages(t *testing.T) {
	base := time.Now().Truncate(time.Second)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	dev, test := string(activities.RoleDev), string(activities.RoleTest)
	devReview, testReview := string(activities.RoleDevReview), string(activities.RoleTestReview)

	entries := []metricsrelay.Entry{
		{At: at(0), Worker: "w", Task: "tA", Role: dev, Round: "1"},
		{At: at(0), Worker: "w", Task: "tB", Role: test, Round: "1"},
		{At: at(0), Worker: "w", Task: "tC", Role: dev, Round: "1"}, // no review ever
		{At: at(0), Worker: "w", Task: "tE", Role: dev, Round: "1"},
		{At: at(0), Worker: "w", Task: "", Role: dev},         // unkeyed, dropped
		{At: at(2), Worker: "w", Task: "tD", Role: devReview}, // review with no impl
		{At: at(4), Worker: "w", Task: "tE", Role: devReview, Round: "1"},
		{At: at(5), Worker: "w", Task: "tA", Role: devReview, Round: "1"},
		// tE's fix round after its first review — frozen out of the gap.
		{At: at(10), Worker: "w", Task: "tE", Role: dev, Round: "2"},
		{At: at(12), Worker: "w", Task: "tB", Role: testReview, Round: "1"},
		// tE's second review — later than the first, ignored.
		{At: at(30), Worker: "w", Task: "tE", Role: devReview, Round: "2"},
	}

	st := foldLoops(entries)
	if st.DevRuns != 2 || st.DevAvg != 4*time.Minute+30*time.Second {
		t.Errorf("dev loop = (%v over %d runs), want 4m30s over 2 (tA 5m, tE frozen at 4m)", st.DevAvg, st.DevRuns)
	}
	if st.TestRuns != 1 || st.TestAvg != 12*time.Minute {
		t.Errorf("test loop = (%v over %d runs), want 12m0s over 1", st.TestAvg, st.TestRuns)
	}

	// An empty window has no loops to average.
	if st := foldLoops(nil); st.DevRuns != 0 || st.TestRuns != 0 {
		t.Errorf("foldLoops(empty) = %+v, want zero runs", st)
	}
}

// TestFoldDriftGroupsRounds pins the context-creep fold: within a keyed
// task+role+round group, last input minus first, averaged over the groups
// with at least two calls — a single call has no span to measure and an
// unkeyed call has no round to group. A shrinking group folds its negative
// growth into the average; nothing clamps.
func TestFoldDriftGroupsRounds(t *testing.T) {
	base := time.Now().Truncate(time.Second)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	entries := []metricsrelay.Entry{
		{At: at(0), Task: "t1", Role: string(activities.RoleDev), Round: "1", InputTokens: 100},
		{At: at(1), Task: "t1", Role: string(activities.RoleDev), Round: "1", InputTokens: 400},
		{At: at(2), Task: "t1", Role: string(activities.RoleDev), Round: "2", InputTokens: 1000},
		{At: at(3), Task: "t1", Role: string(activities.RoleDev), Round: "2", InputTokens: 1100},
		{At: at(4), Task: "t1", Role: string(activities.RoleDev), Round: "2", InputTokens: 1300},
		{At: at(5), Task: "t2", Role: string(activities.RoleTest), Round: "1", InputTokens: 50}, // single
		{At: at(6), Task: "", Role: string(activities.RoleDev), InputTokens: 10},                // unkeyed
		{At: at(7), Task: "", Role: string(activities.RoleDev), InputTokens: 9999},
		{At: at(8), Task: "t3", Role: string(activities.RoleDev), Round: "1", InputTokens: 500},
		{At: at(9), Task: "t3", Role: string(activities.RoleDev), Round: "1", InputTokens: 200},
	}
	avg, groups := foldDrift(entries)
	if groups != 3 || avg != 100 {
		t.Errorf("foldDrift = (%d over %d groups), want 100 over 3 (+300, +300, -300)", avg, groups)
	}
	if avg, groups := foldDrift(nil); avg != 0 || groups != 0 {
		t.Errorf("foldDrift(empty) = (%d, %d), want zero", avg, groups)
	}
}

// TestBuildTilesDashUntilDenominated pins the headline row: every tile
// renders the dash until its denominator exists — tokens per keyed run
// (unkeyed calls join the totals but name no run), tokens per worker, the
// two loop averages, and the per-round input drift.
func TestBuildTilesDashUntilDenominated(t *testing.T) {
	base := time.Now().Truncate(time.Second)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	dev := string(activities.RoleDev)

	if tiles := buildTiles(nil, nil); tiles != (uiDerivedTiles{TokensPerRun: "—", TokensPerWorker: "—", DevLoop: "—", TestLoop: "—", Drift: "—"}) {
		t.Errorf("buildTiles(empty) = %+v, want all dashes", tiles)
	}

	entries := []metricsrelay.Entry{
		{At: at(0), Worker: "w-a", Task: "t1", Role: dev, Round: "1", InputTokens: 100},
		{At: at(1), Worker: "w-a", Task: "t1", Role: dev, Round: "1", InputTokens: 300, OutputTokens: 1000},
		{At: at(2), Worker: "w-a", Task: "t2", Role: dev, Round: "1", InputTokens: 500, OutputTokens: 500},
		{At: at(3), Worker: "w-b", InputTokens: 400, OutputTokens: 100}, // unkeyed
	}
	workers := foldUsage(entries, base, nil, usageWorkerKey)
	tiles := buildTiles(entries, workers)
	// 2900 in+out over 2 keyed runs and 2 workers; t1's round grew 200.
	if tiles.TokensPerRun != "1.4k" || tiles.TokensPerWorker != "1.4k" {
		t.Errorf("tiles = %+v, want 1.4k per run and per worker", tiles)
	}
	if tiles.Drift != "200" {
		t.Errorf("tiles drift = %q, want 200", tiles.Drift)
	}
	if tiles.DevLoop != "—" || tiles.TestLoop != "—" {
		t.Errorf("tiles loops = %q / %q, want dashes (no reviews in the window)", tiles.DevLoop, tiles.TestLoop)
	}
}

// TestFoldRelayTotalsFrozenFace pins the frozen metrics view's counters and
// its per-worker rows: the headline counts over one pass, the window text
// naming the ring's span (or the no-calls note), and worker rows projected
// from the same fold the Models tab reads — ordered by calls, Last the
// newest entry's age, Errors the calls minus the 2xx.
func TestFoldRelayTotalsFrozenFace(t *testing.T) {
	now := time.Now()
	if st := foldRelayTotals(nil, now); st.Calls != 0 || st.Window != "no relayed calls yet" || st.Worker != nil {
		t.Errorf("foldRelayTotals(empty) = %+v, want the no-calls face", st)
	}

	entries := []metricsrelay.Entry{
		{At: now.Add(-3 * time.Hour), Worker: "w-a", InputTokens: 5, OutputTokens: 6, Status: 500},
		{At: now.Add(-2 * time.Minute), Worker: "w-a", InputTokens: 10, OutputTokens: 20, Status: 200},
		{At: now.Add(-20 * time.Second), Worker: "w-b", InputTokens: 7, OutputTokens: 8, Status: 200},
	}
	st := foldRelayTotals(entries, now)
	if st.Calls != 3 || st.Hour != 2 || st.In != 22 || st.Out != 34 {
		t.Errorf("totals = %+v, want 3 calls, 2 in the hour, 22 in, 34 out", st)
	}
	if st.Window != "3.0h window" {
		t.Errorf("window = %q, want the oldest entry's span", st.Window)
	}
	want := []relayWorkerTotals{
		{Worker: "w-a", Calls: 2, In: 15, Out: 26, Errors: 1, Last: "2m ago"},
		{Worker: "w-b", Calls: 1, In: 7, Out: 8, Errors: 0, Last: "just now"},
	}
	if len(st.Worker) != 2 || st.Worker[0] != want[0] || st.Worker[1] != want[1] {
		t.Errorf("worker rows = %+v, want %+v", st.Worker, want)
	}
}

// TestFoldRelayTotalsHeadlineErrors pins the headline error count the
// "Failed calls" tile renders (ui_templates.html's metrics page): the
// non-2xx calls across the window — the figure the tile shows and its
// {{if gt .Relay.Errors 0}} gate turns red, the count the pre-refactor
// relayStats kept and relayTotals' own comment restates ("Errors keeps
// the non-2xx count"). A failure flood must read as failures, not 0.
func TestFoldRelayTotalsHeadlineErrors(t *testing.T) {
	now := time.Now()
	entries := []metricsrelay.Entry{
		{At: now.Add(-3 * time.Hour), Worker: "w-a", Status: 500},
		{At: now.Add(-2 * time.Minute), Worker: "w-a", Status: 200},
		{At: now.Add(-90 * time.Second), Worker: "w-b", Status: 401},
		{At: now.Add(-20 * time.Second), Worker: "w-b", Status: 429},
	}
	if st := foldRelayTotals(entries, now); st.Errors != 3 {
		t.Errorf("headline Errors = %d, want the 3 non-2xx calls (500, 401, 429)", st.Errors)
	}
}

// TestBuildFormRowsGatesTheUnknownBucket pins the price form's rows: every
// ledger-known model gets one, saved rates render back into the inputs,
// unpriced models leave them blank (the ledger is the only source of
// names), and the unknown bucket is named but rate-less even when a price
// sits under that key — a rate on that mixed bucket would be a fabricated
// signal.
func TestBuildFormRowsGatesTheUnknownBucket(t *testing.T) {
	now := time.Now()
	entries := []metricsrelay.Entry{
		{At: now.Add(-3 * time.Minute), Worker: "w", Model: "mA", Status: 200},
		{At: now.Add(-2 * time.Minute), Worker: "w", Model: "mB", Status: 200},
		{At: now.Add(-1 * time.Minute), Worker: "w", Status: 401}, // no model
	}
	models := foldUsage(entries, now, nil, usageModelKey)
	prices := map[string]modelPrice{"mA": {In: 3, Out: 2, Cached: 1.5}, "unknown": {In: 9, Out: 9, Cached: 9}}
	rows := buildFormRows(models, prices)

	if len(rows) != 3 {
		t.Fatalf("form rows = %d, want one per ledger-known model", len(rows))
	}
	if rows[0].Model != "mA" || rows[0].Calls != 1 || rows[0].In != "3" || rows[0].Out != "2" || rows[0].Cached != "1.5" || rows[0].Unknown {
		t.Errorf("mA row = %+v, want the saved rates rendered back", rows[0])
	}
	if rows[1].Model != "mB" || rows[1].In != "" || rows[1].Out != "" || rows[1].Cached != "" || rows[1].Unknown {
		t.Errorf("mB row = %+v, want blank inputs for an unpriced model", rows[1])
	}
	if rows[2].Model != "unknown" || !rows[2].Unknown || rows[2].In != "" || rows[2].Out != "" || rows[2].Cached != "" {
		t.Errorf("unknown row = %+v, want the bucket named but rate-less", rows[2])
	}
}

// TestBuildModelsViewAssemblesTheSurfaces pins the Models tab's assembly:
// one fold over one ledger window feeds both tables, the price form's
// rows, and the derived tiles — the empty window renders its no-calls
// note with nothing else. The charts are the golens-metrics mount's
// (/full-metrics, golens.go), fed per append from the same ledger — the
// view carries no chart of its own to gate on data.
func TestBuildModelsViewAssemblesTheSurfaces(t *testing.T) {
	now := time.Now()
	entries := []metricsrelay.Entry{
		{At: now.Add(-2 * time.Minute), Worker: "w-a", Wire: metricsrelay.WireAnthropic, Model: "mA",
			Task: "t1", Role: string(activities.RoleDev), Round: "1", InputTokens: 1000, OutputTokens: 500, Status: 200},
		{At: now.Add(-1 * time.Minute), Worker: "w-b", Wire: metricsrelay.WireOpenAI, Model: "mB",
			InputTokens: 800, OutputTokens: 400, Status: 200},
		{At: now.Add(-30 * time.Second), Worker: "w-a", Status: 401}, // unknown bucket
	}

	empty := buildModelsView(now, nil, nil)
	if empty.Window != "no relayed calls yet" || len(empty.FormRows) != 0 ||
		len(empty.ModelTable.Rows) != 0 || len(empty.WorkerTable.Rows) != 0 {
		t.Errorf("empty view = %+v, want the no-calls window and no rows", empty)
	}

	v := buildModelsView(now, entries, map[string]modelPrice{"mA": {In: 1}})
	if v.Window == "no relayed calls yet" || strings.HasPrefix(v.Window, "no ") {
		t.Errorf("window = %q, want the ledger's own span", v.Window)
	}
	if len(v.FormRows) != 3 {
		t.Errorf("form rows = %d, want one per ledger-known model", len(v.FormRows))
	}
	if v.ModelTable.Head != "Model" || len(v.ModelTable.Rows) != 3 {
		t.Errorf("model table = %q with %d rows, want Model with 3 (mA, mB, unknown)",
			v.ModelTable.Head, len(v.ModelTable.Rows))
	}
	if v.WorkerTable.Head != "Worker" || len(v.WorkerTable.Rows) != 2 {
		t.Errorf("worker table = %q with %d rows, want Worker with 2 (w-a, w-b)",
			v.WorkerTable.Head, len(v.WorkerTable.Rows))
	}
	// The priced model's fold carries its render-time cost into the table
	// the page renders — the one place cost shows on this tab now that the
	// charts serve at /full-metrics.
	if got := v.ModelTable.Rows[0]; got.Key != "mA" || !got.Priced || math.Abs(got.Cost-0.001) > 1e-12 {
		t.Errorf("first model row = %+v, want mA priced at 0.001 (1M in at $1/1M)", got)
	}
	if v.Tiles.TokensPerRun == "—" {
		t.Errorf("tiles = %+v, want the keyed run's denominator", v.Tiles)
	}
}

// TestRunLedgerCostRowsAndDashes pins the run-detail cost section's fold
// and its id parity with the ledger's attribution: entries match the run by
// its scrubbed Task id (a workflow id carrying the parse anchors scrubs the
// same way worker-side), only priced models contribute, rows order by cost
// descending, and the section is the dash when no prices are saved or the
// run has no priced keyed calls.
func TestRunLedgerCostRowsAndDashes(t *testing.T) {
	useDaemonDir(t)
	led := &metricsrelay.Ledger{}
	base := time.Now().Add(-time.Hour)
	if got := ledgerTaskID("listq-iss9"); got != "listq-iss9" {
		t.Errorf("ledgerTaskID(plain) = %q, want it unchanged", got)
	}
	if got := ledgerTaskID("listq-iss-role-a-round-3"); got != "listq-iss_role_a_round_3" {
		t.Errorf("ledgerTaskID(anchored) = %q, want the anchors scrubbed like the attribution key", got)
	}

	wid := "listq-iss9"
	led.Append(metricsrelay.Entry{At: base, Worker: "w", Wire: metricsrelay.WireAnthropic, Task: wid,
		Role: string(activities.RoleDev), Round: "1", Model: "mA", InputTokens: 1_000_000, Status: 200})
	led.Append(metricsrelay.Entry{At: base.Add(time.Minute), Worker: "w", Wire: metricsrelay.WireAnthropic, Task: wid,
		Role: string(activities.RoleDev), Round: "2", Model: "mA", InputTokens: 500_000, OutputTokens: 1_000_000, Status: 200})
	led.Append(metricsrelay.Entry{At: base.Add(2 * time.Minute), Worker: "w", Wire: metricsrelay.WireOpenAI, Task: wid,
		Role: string(activities.RoleTest), Round: "1", Model: "mC", InputTokens: 1_000_000, Status: 200})
	// mB is unpriced and the last entry is another run's: neither may
	// contribute.
	led.Append(metricsrelay.Entry{At: base.Add(3 * time.Minute), Worker: "w", Task: wid,
		Model: "mB", InputTokens: 2_000_000, Status: 200})
	led.Append(metricsrelay.Entry{At: base.Add(4 * time.Minute), Worker: "w", Task: "listq-other",
		Model: "mA", InputTokens: 9_000_000, Status: 200})

	s := newUIServer(config.Config{}, led, nil)
	if rows, total := s.runLedgerCost(wid); rows != nil || total != "—" {
		t.Errorf("runLedgerCost(unpriced) = (%v, %q), want the dash", rows, total)
	}

	s.swapPrices(map[string]modelPrice{"mA": {In: 3, Out: 2, Cached: 1}, "mC": {In: 1}})
	rows, total := s.runLedgerCost(wid)
	if total != "$7.5000" {
		t.Errorf("total = %q, want $7.5000", total)
	}
	want := []uiRunModelCost{
		{Model: "mA", Calls: 2, Cost: "$6.5000"}, // 3.0 + (1.5 + 2.0)
		{Model: "mC", Calls: 1, Cost: "$1.0000"},
	}
	if len(rows) != 2 || rows[0].Model != want[0].Model || rows[0].Calls != want[0].Calls || rows[0].Cost != want[0].Cost ||
		rows[1].Model != want[1].Model || rows[1].Calls != want[1].Calls || rows[1].Cost != want[1].Cost {
		t.Errorf("rows = %+v, want %+v (priced models only, cost-descending)", rows, want)
	}

	if rows, total := s.runLedgerCost("listq-ghost"); rows != nil || total != "—" {
		t.Errorf("runLedgerCost(run with no priced calls) = (%v, %q), want the dash", rows, total)
	}
}

// seedLedger appends a batch of entries to the harness's ledger.
func seedLedger(h *uiHarness, entries []metricsrelay.Entry) {
	for _, e := range entries {
		h.led.Append(e)
	}
}

// postModels posts the price form without following the redirect, returning
// the status, the Location, and the body (the diagnostic on a failure).
func postModels(t *testing.T, h *uiHarness, form url.Values) (int, string, string) {
	t.Helper()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.PostForm(h.srv.URL+"/models", form)
	if err != nil {
		t.Fatalf("POST /models: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read POST /models: %v", err)
	}
	return resp.StatusCode, resp.Header.Get("Location"), string(body)
}

// setDetailRun teaches the fake frontend one live run: a describe answer
// and the started-event history the detail walk reads.
func setDetailRun(t *testing.T, h *uiHarness, id string) {
	t.Helper()
	h.fake.describes[id] = &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "run-" + id},
			TaskQueue: "listq",
			Status:    enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(time.Now().Add(-time.Hour)),
		},
	}
	h.fake.wfHistory[id] = []*historypb.HistoryEvent{
		uiStartedEvent(t, workflows.PipelineInput{IssueID: "9", RepoPath: "", Flow: "feature-dev"}),
	}
}

// TestUIModelsTab pins the Models tab end to end: the nav tab every page
// carries, the empty face, the form laid over the ledger's own models with
// the unknown bucket's inputs disabled, the save's upsert-and-redirect
// round trip repricing the tab, the forged and malformed posts a hostile
// submit cannot smuggle past the pricable gate, and the 500 with its
// diagnostic when the daemon dir refuses the write.
func TestUIModelsTab(t *testing.T) {
	// relayed entries lays one priced-candidate model (1M in, 2M out) and
	// one failed model-less call — the unknown bucket.
	relayed := func(now time.Time) []metricsrelay.Entry {
		return []metricsrelay.Entry{
			{At: now.Add(-2 * time.Minute), Worker: "w-a", Wire: metricsrelay.WireAnthropic, Model: "mA",
				Task: "t1", Role: string(activities.RoleDev), Round: "1",
				InputTokens: 1_000_000, OutputTokens: 2_000_000, DurationMS: 100, Status: 200},
			{At: now.Add(-1 * time.Minute), Worker: "w-a", Status: 401},
		}
	}

	t.Run("the empty tab and the nav tab", func(t *testing.T) {
		h := newUIHarness(t)
		out := h.get(t, "/models")
		if !strings.Contains(out, `<a class="tab on" href="/models">Models</a>`) {
			t.Errorf("the models tab did not mark itself active:\n%s", out)
		}
		if !strings.Contains(out, "no relayed calls yet") {
			t.Errorf("the empty tab lost the window note:\n%s", out)
		}
		if n := strings.Count(out, "Nothing relayed yet."); n != 2 {
			t.Errorf("the empty tables said so %d times, want both (by model and by worker)", n)
		}
		if n := strings.Count(out, `>—</div>`); n != 5 {
			t.Errorf("the tiles rendered %d dashes, want all five", n)
		}
		if strings.Contains(out, `name="in[`) {
			t.Errorf("the form rendered without ledger-known models:\n%s", out)
		}
		if page := h.get(t, "/"); !strings.Contains(page, `href="/models">Models</a>`) {
			t.Errorf("the landing page's nav lost the models tab:\n%s", page)
		}
	})

	t.Run("the form lays over the ledger's own models", func(t *testing.T) {
		h := newUIHarness(t)
		seedLedger(h, relayed(time.Now()))

		out := h.get(t, "/models")
		for _, want := range []string{
			`name="in[mA]"`, `name="out[mA]"`, `name="cached[mA]"`,
			`disabled value="" title="failed calls carry no model name`,
			"Usage by model", "Usage by worker",
			`>1.00M</td>`, `>2.00M</td>`,
			// The charts serve at the golens-metrics mount now; the tab's
			// coverage card is where the tab points at them.
			`href="/full-metrics"`,
		} {
			if !strings.Contains(out, want) {
				t.Errorf("the models page lacks %q:\n%s", want, out)
			}
		}
		for _, banned := range []string{"Tokens — the whole ledger window", "Cost — priced models only"} {
			if strings.Contains(out, banned) {
				t.Errorf("the page still renders the removed server-built chart %q:\n%s", banned, out)
			}
		}
	})

	t.Run("saving prices reprices the tab", func(t *testing.T) {
		h := newUIHarness(t)
		seedLedger(h, relayed(time.Now()))

		code, loc, _ := postModels(t, h, url.Values{"in[mA]": {"3"}, "out[mA]": {"2"}, "cached[mA]": {"1.5"}})
		if code != http.StatusSeeOther || loc != "/models" {
			t.Fatalf("save answered %d to %q, want 303 back to /models", code, loc)
		}
		if p := loadModelPrices(); len(p) != 1 || p["mA"] != (modelPrice{In: 3, Out: 2, Cached: 1.5}) {
			t.Fatalf("the save wrote %+v, want mA at {3 2 1.5}", p)
		}

		out := h.get(t, "/models")
		for _, want := range []string{
			`name="in[mA]" value="3"`, `name="out[mA]" value="2"`, `name="cached[mA]" value="1.5"`,
			// 1M in at $3 + 2M out at $2 = $7 — the usage table's cost cell,
			// the tab's one priced face since the charts moved to the mount.
			`$7.0000`,
		} {
			if !strings.Contains(out, want) {
				t.Errorf("the repriced page lacks %q:\n%s", want, out)
			}
		}
	})

	t.Run("a forged or malformed save drops and zeros", func(t *testing.T) {
		h := newUIHarness(t)
		seedLedger(h, relayed(time.Now()))

		// Names the ledger does not know — invented models and the unknown
		// bucket — are dropped, so nothing is even written.
		code, _, _ := postModels(t, h, url.Values{"in[ghost]": {"9"}, "cached[unknown]": {"9"}})
		if code != http.StatusSeeOther {
			t.Fatalf("the dropped save answered %d, want the redirect", code)
		}
		if p := loadModelPrices(); len(p) != 0 {
			t.Errorf("the forged save wrote %+v, want nothing", p)
		}

		// A real save, then a hostile one: negative and NaN land as 0 —
		// the row is still written (clearing prices a model at 0), and the
		// server's snapshot swapped to it.
		if code, _, _ := postModels(t, h, url.Values{"in[mA]": {"3"}, "out[mA]": {"2"}}); code != http.StatusSeeOther {
			t.Fatalf("the seeded save answered %d", code)
		}
		if code, _, _ := postModels(t, h, url.Values{"in[mA]": {"-5"}, "out[mA]": {"NaN"}, "cached[mA]": {"1"}}); code != http.StatusSeeOther {
			t.Fatalf("the malformed save answered %d", code)
		}
		if p := loadModelPrices(); p["mA"] != (modelPrice{In: 0, Out: 0, Cached: 1}) {
			t.Errorf("the malformed save wrote %+v, want zeros for -5 and NaN, 1 kept", p["mA"])
		}
		if out := h.get(t, "/models"); !strings.Contains(out, `name="in[mA]" value="0"`) {
			t.Errorf("the form did not render the zeroed rate back:\n%s", out)
		}
	})

	t.Run("a refused write answers 500 with its diagnostic", func(t *testing.T) {
		h := newUIHarness(t)
		seedLedger(h, relayed(time.Now()))
		if err := os.RemoveAll(daemonDir); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(daemonDir, []byte("not a dir"), 0o644); err != nil {
			t.Fatal(err)
		}
		code, _, body := postModels(t, h, url.Values{"in[mA]": {"3"}})
		if code != http.StatusInternalServerError || !strings.Contains(body, "save model prices:") {
			t.Errorf("save over a blocked daemon dir = %d, %q; want 500 naming the save", code, body)
		}
	})
}

// TestUIModelsBodyFragmentKeepsTheFormOut pins the polled region's
// boundary: the page renders the price form above the wrapper and the
// read-only surfaces inside it, and the polled /models/body serves only
// the latter — a poll may never swap the form's editable inputs out from
// under the operator, so no editable input or page chrome may ride the
// fragment.
func TestUIModelsBodyFragmentKeepsTheFormOut(t *testing.T) {
	h := newUIHarness(t)
	seedLedger(h, []metricsrelay.Entry{
		{At: time.Now().Add(-time.Minute), Worker: "w-a", Wire: metricsrelay.WireOpenAI, Model: "mA",
			Task: "t1", Role: string(activities.RoleDev), Round: "1",
			InputTokens: 1000, OutputTokens: 2000, DurationMS: 100, Status: 200},
	})

	page := h.get(t, "/models")
	if !strings.Contains(page, `hx-get="/models/body"`) {
		t.Errorf("the models page lost its polled region wrapper:\n%s", page)
	}
	if !strings.Contains(page, `name="in[mA]"`) {
		t.Errorf("the models page lost the price form:\n%s", page)
	}

	body := h.get(t, "/models/body")
	for _, want := range []string{"Tokens / run", "Usage by model", "Usage by worker", ">mA</td>"} {
		if !strings.Contains(body, want) {
			t.Errorf("the models body fragment lacks %q:\n%s", want, body)
		}
	}
	for _, banned := range []string{`name="in[`, "<form", "<main"} {
		if strings.Contains(body, banned) {
			t.Errorf("the models body fragment grew editable inputs or page chrome (%q):\n%s", banned, body)
		}
	}
}

// TestUIRunDetailLedgerCost pins the run-detail page's ledger cost section:
// the dash while nothing is priced, per-model rows and the total once the
// operator saves rates — the run's own keyed calls only, unpriced models
// contributing nothing — the workflow id matching the ledger's scrubbed
// Task, and the ledger itself never storing a cost (the price table is
// render-time, so a retroactive edit reprices history).
func TestUIRunDetailLedgerCost(t *testing.T) {
	seed := func(t *testing.T, h *uiHarness, wid, task string) {
		t.Helper()
		setDetailRun(t, h, wid)
		base := time.Now().Add(-time.Hour)
		h.led.Append(metricsrelay.Entry{At: base, Worker: "w", Wire: metricsrelay.WireAnthropic, Task: task,
			Role: string(activities.RoleDev), Round: "1", Model: "mA", InputTokens: 1_000_000, Status: 200})
		h.led.Append(metricsrelay.Entry{At: base.Add(time.Minute), Worker: "w", Wire: metricsrelay.WireAnthropic, Task: task,
			Role: string(activities.RoleDev), Round: "2", Model: "mA", InputTokens: 500_000, OutputTokens: 1_000_000, Status: 200})
		h.led.Append(metricsrelay.Entry{At: base.Add(2 * time.Minute), Worker: "w", Wire: metricsrelay.WireOpenAI, Task: task,
			Role: string(activities.RoleTest), Round: "1", Model: "mC", InputTokens: 1_000_000, Status: 200})
		// An unpriced model and another run's call: neither may show.
		h.led.Append(metricsrelay.Entry{At: base.Add(3 * time.Minute), Worker: "w", Task: task,
			Model: "mB", InputTokens: 2_000_000, Status: 200})
		h.led.Append(metricsrelay.Entry{At: base.Add(4 * time.Minute), Worker: "w", Task: "listq-other",
			Model: "mA", InputTokens: 9_000_000, Status: 200})
	}

	t.Run("unpriced, the section is the dash", func(t *testing.T) {
		h := newUIHarness(t)
		seed(t, h, "listq-iss9", "listq-iss9")
		out := h.get(t, "/runs/listq-iss9")
		if !strings.Contains(out, "Ledger cost (priced models only)") {
			t.Errorf("the detail page lost the ledger cost card:\n%s", out)
		}
		if !strings.Contains(out, `<div class="text-sm text-slate-500">—</div>`) {
			t.Errorf("the unpriced section lost the dash face:\n%s", out)
		}
		if strings.Contains(out, `colspan="2">Total</td>`) {
			t.Errorf("the unpriced section rendered a total:\n%s", out)
		}
	})

	t.Run("priced, the section prices the run's own calls", func(t *testing.T) {
		h := newUIHarness(t)
		seed(t, h, "listq-iss9", "listq-iss9")
		if code, _, _ := postModels(t, h, url.Values{
			"in[mA]": {"3"}, "out[mA]": {"2"}, "cached[mA]": {"1"}, "in[mC]": {"1"},
		}); code != http.StatusSeeOther {
			t.Fatalf("the save answered %d", code)
		}

		out := h.get(t, "/runs/listq-iss9")
		for _, want := range []string{
			`<td class="py-1 mono">mA</td><td>2</td><td class="mono">$6.5000</td>`,
			`<td class="py-1 mono">mC</td><td>1</td><td class="mono">$1.0000</td>`,
			`colspan="2">Total</td><td class="mono">$7.5000</td>`,
		} {
			if !strings.Contains(out, want) {
				t.Errorf("the priced section lacks %q:\n%s", want, out)
			}
		}
		if strings.Contains(out, `>mB</td>`) {
			t.Errorf("the unpriced model grew a row:\n%s", out)
		}

		// Cost is render-time: the ledger's own records stay unpriced, so
		// tomorrow's rate edit reprices this window.
		stored, _, _ := h.led.Query("", 1, 1<<30)
		for _, e := range stored {
			if e.CostUSD != 0 {
				t.Errorf("the ledger stored CostUSD %v on %+v, want the reserved zero", e.CostUSD, e)
			}
		}
	})

	t.Run("the run id's parse anchors scrub like the ledger's Task", func(t *testing.T) {
		h := newUIHarness(t)
		seed(t, h, "listq-iss-role-a-round-3", "listq-iss_role_a_round_3")
		if code, _, _ := postModels(t, h, url.Values{"in[mA]": {"3"}}); code != http.StatusSeeOther {
			t.Fatalf("the save answered %d", code)
		}
		out := h.get(t, "/runs/listq-iss-role-a-round-3")
		// The seed's two mA entries at an out rate of 0: $3.00 + $1.50.
		if !strings.Contains(out, `<td class="py-1 mono">mA</td><td>2</td><td class="mono">$4.5000</td>`) ||
			!strings.Contains(out, `colspan="2">Total</td><td class="mono">$4.5000</td>`) {
			t.Errorf("the anchored run id did not match its scrubbed Task:\n%s", out)
		}
	})
}

// TestLedgerEntriesDropsExternalOtel pins the main dashboard's ledger face
// (D6): external otel batches — any other exporter on the host — never
// enter a main-dashboard fold; they render only on the golens-served
// full-metrics page. Relay rows and daedalus otel rows both stay.
func TestLedgerEntriesDropsExternalOtel(t *testing.T) {
	useDaemonDir(t)
	led := &metricsrelay.Ledger{}
	t0 := time.Now().Add(-time.Hour)
	led.Append(metricsrelay.Entry{At: t0, Worker: "w"})
	led.Append(metricsrelay.Entry{At: t0.Add(time.Minute), Source: metricsrelay.SourceOtelExternal, Task: "foreign", InputTokens: 5})
	led.Append(metricsrelay.Entry{At: t0.Add(2 * time.Minute), Source: metricsrelay.SourceOtel, Task: "t1", InputTokens: 7})

	got := newUIServer(config.Config{}, led, nil).ledgerEntries()
	if len(got) != 2 {
		t.Fatalf("ledgerEntries = %d entries, want the external batch dropped", len(got))
	}
	if got[0].Worker != "w" || got[1].Source != metricsrelay.SourceOtel {
		t.Errorf("ledgerEntries = %+v, want the relay row then the daedalus otel row, ascending", got)
	}
}

// TestFoldUsageSplitsOtelEraRows pins the fold's era split: a relay row
// and an otel row of the same model stay separate rows — pre-toggle relay
// history sits beside post-toggle otel data, never summed — the otel row
// stamps Src so the template renders the otel- prefix and its em-dash
// set, same-key-same-calls ties order relay before otel, and the worker
// fold drops otel entries whole (they carry no worker to fold under).
func TestFoldUsageSplitsOtelEraRows(t *testing.T) {
	now := time.Now()
	entries := []metricsrelay.Entry{
		{At: now.Add(-time.Hour), Worker: "w-a", Wire: metricsrelay.WireOpenAI, Model: "mA",
			InputTokens: 100, OutputTokens: 50, DurationMS: 1000, Status: 200},
		{At: now.Add(-time.Minute), Source: metricsrelay.SourceOtel, Task: "t1", Role: "dev", Round: "1",
			Model: "mA", InputTokens: 200, OutputTokens: 100},
	}
	rows := foldUsage(entries, now, nil, usageModelKey)
	if len(rows) != 2 {
		t.Fatalf("model fold = %d rows, want the eras separate", len(rows))
	}
	relay, otel := rows[0], rows[1]
	if relay.Key != "mA" || relay.Src != "" || relay.Calls != 1 || relay.In != 100 {
		t.Errorf("relay row = %+v, want mA's relay era unstamped", relay)
	}
	if otel.Key != "mA" || otel.Src != metricsrelay.SourceOtel || otel.Calls != 1 || otel.In != 200 {
		t.Errorf("otel row = %+v, want mA's otel era stamped and separate", otel)
	}

	// Same key, same calls: the relay row precedes its otel twin so the
	// order is stable across renders despite the map walk.
	tie := []metricsrelay.Entry{
		{At: now.Add(-time.Hour), Source: metricsrelay.SourceOtel, Task: "t1", Model: "mB", InputTokens: 1},
		{At: now.Add(-time.Minute), Worker: "w", Model: "mB", InputTokens: 2},
	}
	rows = foldUsage(tie, now, nil, usageModelKey)
	if len(rows) != 2 || rows[0].Src != "" || rows[1].Src != metricsrelay.SourceOtel {
		t.Errorf("tied rows = %+v, want the relay row before its otel twin", rows)
	}

	// The worker fold has no otel row to show: otel usage carries no
	// worker attribution, and pooling it under a nameless key would read
	// as a literal "otel-" worker.
	wrows := foldUsage(entries, now, nil, usageWorkerKey)
	if len(wrows) != 1 || wrows[0].Key != "w-a" {
		t.Errorf("worker fold = %+v, want only the relay worker", wrows)
	}
}

// TestFoldLoopsDriftAndTotalsIgnoreOtel pins the tiles' relay-only faces:
// an otel batch's stamps are export-batch arrivals, not round-end proxies,
// so the loop, drift, and relay-totals folds skip otel entries whole — a
// multi-batch toggle-on round would otherwise read the loop through the
// export interval and invert the drift tile's sign.
func TestFoldLoopsDriftAndTotalsIgnoreOtel(t *testing.T) {
	base := time.Now().Truncate(time.Second)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	dev := string(activities.RoleDev)

	entries := []metricsrelay.Entry{
		// The relay face: one dev loop of 10 minutes, one drift group of +100.
		{At: at(0), Worker: "w", Task: "t1", Role: dev, Round: "1", InputTokens: 100, Status: 200},
		{At: at(10), Worker: "w", Task: "t1", Role: string(activities.RoleDevReview), Round: "1", Status: 200},
		{At: at(11), Worker: "w", Task: "t1", Role: dev, Round: "1", InputTokens: 200, Status: 200},
		// The otel noise the folds must ignore: a 1-minute "loop" that
		// would drag the average down, and a shrinking "round" that would
		// invert the drift.
		{At: at(0), Source: metricsrelay.SourceOtel, Task: "t1", Role: dev, Round: "1", InputTokens: 5000},
		{At: at(1), Source: metricsrelay.SourceOtel, Task: "t1", Role: string(activities.RoleDevReview), Round: "1"},
		{At: at(2), Source: metricsrelay.SourceOtel, Task: "t9", Role: dev, Round: "1", InputTokens: 1},
		{At: at(3), Source: metricsrelay.SourceOtel, Task: "t9", Role: dev, Round: "1", InputTokens: 0},
	}

	st := foldLoops(entries)
	if st.DevRuns != 1 || st.DevAvg != 10*time.Minute {
		t.Errorf("dev loop = (%v over %d runs), want the relay's 10m alone", st.DevAvg, st.DevRuns)
	}
	avg, groups := foldDrift(entries)
	if groups != 1 || avg != 100 {
		t.Errorf("drift = (%d over %d groups), want the relay group's +100 alone", avg, groups)
	}

	totals := foldRelayTotals(entries, base)
	if totals.Calls != 3 {
		t.Errorf("relay totals = %d calls, want the relay entries only", totals.Calls)
	}
}

// TestBuildTilesSplitDenominators pins the headline tiles' source split:
// TokensPerRun is all-source — both its numerator and its distinct-task
// denominator count otel rounds once, so the populations agree — while
// TokensPerWorker reads the relay face on both sides, its denominator
// (the worker fold) carrying no otel row to divide by.
func TestBuildTilesSplitDenominators(t *testing.T) {
	base := time.Now()
	entries := []metricsrelay.Entry{
		{At: base, Worker: "w-a", Task: "t1", InputTokens: 60, OutputTokens: 40},
		{At: base, Source: metricsrelay.SourceOtel, Task: "t2", InputTokens: 300, OutputTokens: 100},
	}
	workers := foldUsage(entries, base, nil, usageWorkerKey)
	tiles := buildTiles(entries, workers)
	if tiles.TokensPerRun != "250" {
		t.Errorf("TokensPerRun = %q, want 250 (500 over both tasks)", tiles.TokensPerRun)
	}
	if tiles.TokensPerWorker != "100" {
		t.Errorf("TokensPerWorker = %q, want 100 (the relay face over its one worker)", tiles.TokensPerWorker)
	}
}

// TestBuildFormRowsPoolsEraRows pins the price form's era pooling: a model
// folded into two era rows pools into one form row — the form edits rates,
// and rates have no era — with the calls summed and Last the newer era's.
func TestBuildFormRowsPoolsEraRows(t *testing.T) {
	now := time.Now()
	modelRows := []metricsUsageRow{
		{Key: "mA", Src: "", Calls: 3, Last: "3m ago", last: now.Add(-3 * time.Minute)},
		{Key: "mA", Src: metricsrelay.SourceOtel, Calls: 2, Last: "1m ago", last: now.Add(-time.Minute)},
	}
	rows := buildFormRows(modelRows, map[string]modelPrice{"mA": {In: 3, Out: 2, Cached: 1}})
	if len(rows) != 1 {
		t.Fatalf("form rows = %d, want the eras pooled into one", len(rows))
	}
	row := rows[0]
	if row.Model != "mA" || row.Calls != 5 || row.Last != "1m ago" {
		t.Errorf("pooled row = %+v, want 5 calls and the newer era's Last", row)
	}
	if row.In != "3" || row.Out != "2" || row.Cached != "1" {
		t.Errorf("pooled row rates = %+v, want the model's saved rates once", row)
	}
}

// TestUIUsageTableRendersTheOtelEra pins the usage table's era render
// (D1): an otel row shows the otel- key prefix and the em-dash set on the
// per-request columns — an export batch carries no status, latency, or
// cache-ratio signal to show — while the relay row beside it renders its
// real figures, and a same-key tie orders relay before otel.
func TestUIUsageTableRendersTheOtelEra(t *testing.T) {
	h := newUIHarness(t)
	now := time.Now()
	h.led.Append(metricsrelay.Entry{At: now.Add(-time.Hour), Worker: "w", Wire: metricsrelay.WireAnthropic, Model: "mA",
		InputTokens: 1000, OutputTokens: 2000, CacheReadTokens: 500, DurationMS: 1000, Status: 200})
	h.led.Append(metricsrelay.Entry{At: now.Add(-time.Minute), Source: metricsrelay.SourceOtel, Task: "t1", Role: "dev", Round: "1",
		Model: "mA", InputTokens: 300, OutputTokens: 100})

	out := h.get(t, "/models")
	relayAt, otelAt := strings.Index(out, `>mA</td>`), strings.Index(out, `>otel-mA</td>`)
	if relayAt < 0 || otelAt < 0 {
		t.Fatalf("usage table lacks the era rows (relay@%d otel@%d):\n%s", relayAt, otelAt, out)
	}
	if relayAt > otelAt {
		t.Errorf("the otel row rendered before its relay twin:\n%s", out)
	}
	if !strings.Contains(out, ">33.3%<") {
		t.Errorf("the relay row lost its cache-hit ratio (500 reads over 1500 anthropic denominator):\n%s", out)
	}
	// The otel row's per-request columns are dashes, not the zeros an
	// export batch would fabricate.
	if n := strings.Count(out, `<td class="mono">—</td>`); n < 4 {
		t.Errorf("the otel row rendered %d dash cells, want the ratio and latency set:\n%s", n, out)
	}
}
