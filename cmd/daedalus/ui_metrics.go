// The metrics view's data layer: the operator-editable model price table,
// the ledger folds behind the usage tables, and derived tiles. Everything
// reads the relay ledger the way `dash list` does — one clamped full-ring
// query, then plain folds; a 10k-entry pass is microseconds, so no cache
// layer. Cost is computed at render time from the saved prices and never
// stored — a retroactive price edit reprices the whole window on the next
// render, and Entry.CostUSD stays the reserved zero it has always been.
// Charts live in the golens-metrics dashboard (golens.go) at
// /full-metrics, fed per append from the same ledger — the server-built
// SVG charts this file once rendered were removed once that mount served.
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/metricsrelay"
)

// modelPrice is one model's editable rates, USD per 1M tokens. There is
// deliberately no cache-write rate: writes bill at the in rate, and the
// ledger keeps write counts visible in the cache columns.
type modelPrice struct {
	In     float64 `json:"in"`
	Out    float64 `json:"out"`
	Cached float64 `json:"cached"`
}

// modelPricesPath is the price table's record in the daemon dir, beside
// the pid/status files — host-global like everything else in there, and
// per-test isolated wherever the daemon dir is.
func modelPricesPath() string { return filepath.Join(daemonDir, "metrics-models.json") }

// loadModelPrices reads the saved table. A missing file is the ordinary
// first boot; an unparsable one starts empty too — the next save rewrites
// it whole (write-then-rename), and refusing the daemon's boot over price
// JSON would trade every dashboard view for one hand-mangled file.
func loadModelPrices() map[string]modelPrice {
	data, err := os.ReadFile(modelPricesPath())
	if err != nil {
		return nil
	}
	prices := map[string]modelPrice{}
	if json.Unmarshal(data, &prices) != nil {
		return nil
	}
	return prices
}

// saveModelPrices merges the submitted entries into the saved table and
// writes it back atomically (unique-temp write-then-rename — a fixed
// temp name would let two concurrent saves rename each other's
// half-written file and wipe the table). The merge is an upsert that
// never deletes:
// model names do not change and prices are only ever added, so entries
// survive ring rotation and daemon restarts by construction — the only
// removal path is hand-editing the file, the accepted escape hatch.
func saveModelPrices(upsert map[string]modelPrice) error {
	prices := loadModelPrices()
	if prices == nil {
		prices = map[string]modelPrice{}
	}
	for name, p := range upsert {
		prices[name] = p
	}
	data, err := json.MarshalIndent(prices, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(daemonDir, 0o755); err != nil {
		return err
	}
	path := modelPricesPath()
	f, err := os.CreateTemp(daemonDir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) // no-op once the rename has moved it
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// currentPrices snapshots the server's price table. Callers treat the map
// as read-only — a save rebuilds the file and swaps the pointer whole.
func (s *uiServer) currentPrices() map[string]modelPrice {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prices
}

// swapPrices replaces the server's price snapshot — the swap-whole-map
// discipline the fragment cache uses.
func (s *uiServer) swapPrices(prices map[string]modelPrice) {
	s.mu.Lock()
	s.prices = prices
	s.mu.Unlock()
}

// ledgerEntries reads the whole ring once, ascending by At — Query
// returns newest-first, and every first/last bookkeeping in the folds
// depends on chronological order.
func (s *uiServer) ledgerEntries() []metricsrelay.Entry {
	if s.ledger == nil {
		return nil
	}
	entries, _, _ := s.ledger.Query("", 1, 1<<30)
	sort.Slice(entries, func(i, j int) bool { return entries[i].At.Before(entries[j].At) })
	return entries
}

// modelUnknown is the fold key for calls whose usage block carried no
// model — the failures. It renders rate-less everywhere: a price on that
// mixed bucket would be a fabricated signal.
const modelUnknown = "unknown"

// usageModelKey groups by the usage block's model; usageWorkerKey groups
// by worker.
func usageModelKey(e metricsrelay.Entry) string {
	if e.Model == "" {
		return modelUnknown
	}
	return e.Model
}

func usageWorkerKey(e metricsrelay.Entry) string { return e.Worker }

// entryCost prices one entry at render time: input and cache-write at the
// in rate (writes bill at the in rate — there is deliberately no fourth
// rate), output at the out rate, cache reads at the cached rate.
func entryCost(e metricsrelay.Entry, p modelPrice) float64 {
	return float64(e.InputTokens+e.CacheWriteTokens)/1e6*p.In +
		float64(e.OutputTokens)/1e6*p.Out +
		float64(e.CacheReadTokens)/1e6*p.Cached
}

// metricsUsageRow is one fold result row, display-ready — the replicated
// column set both the per-model and the per-worker table render. HitPct,
// P50, P95, RPM, and Last are preformatted (a dash where the denominator
// is zero); Cost stays raw for the usd template func, with Priced gating
// it — unpriced rows render the dash, never a zero.
type metricsUsageRow struct {
	Key                     string
	Calls                   int64
	In, Out                 int64
	CacheRead, CacheWrite   int64
	Runs                    int
	Dev, Test, Review       int64
	HitPct                  string
	OK2xx, R429, R4xx, R5xx int64
	P50, P95, RPM, Last     string
	Priced                  bool
	Cost                    float64

	hitRead, hitDenom int64
	hour              int64
	last              time.Time
}

// latencyDur renders a per-call latency for the usage tables' P50/P95:
// millisecond rounding under a second — whole-second rounding collapses
// fast calls into fabricated zeros (a 400ms p50 reading "0s") — deferring
// to the runs-table humanizer dur above it.
func latencyDur(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	return dur(d)
}

// foldUsage reduces the ascending ledger into one row per key: the token
// sums, the distinct keyed runs, the dev/test/review call split, the
// wire-aware cache-hit ratio (the anthropic wire reports cache reads
// beside input_tokens — denominator in + read; the openai wire reports
// cached_tokens inside prompt_tokens — denominator in; aggregated as
// summed reads over summed denominators, never averaged ratios), the
// status split, nearest-rank p50/p95 durations, the trailing-hour call
// rate, and the render-time cost of the priced models. Rows order by
// call count — the tables' and the charts' shared order.
func foldUsage(entries []metricsrelay.Entry, now time.Time, prices map[string]modelPrice, keyOf func(metricsrelay.Entry) string) []metricsUsageRow {
	byKey := map[string]*metricsUsageRow{}
	durs := map[string][]int64{}
	runs := map[string]map[string]bool{}
	for _, e := range entries {
		k := keyOf(e)
		r := byKey[k]
		if r == nil {
			r = &metricsUsageRow{Key: k}
			byKey[k] = r
		}
		r.Calls++
		r.In += e.InputTokens
		r.Out += e.OutputTokens
		r.CacheRead += e.CacheReadTokens
		r.CacheWrite += e.CacheWriteTokens
		if e.Task != "" {
			if runs[k] == nil {
				runs[k] = map[string]bool{}
			}
			runs[k][e.Task] = true
		}
		switch e.Role {
		case string(activities.RoleDev):
			r.Dev++
		case string(activities.RoleTest):
			r.Test++
		}
		if strings.HasSuffix(e.Role, "-review") {
			r.Review++
		}
		switch {
		case e.Status >= 200 && e.Status < 300:
			r.OK2xx++
		case e.Status == 429:
			r.R429++
		case e.Status >= 400 && e.Status < 500:
			r.R4xx++
		case e.Status >= 500 && e.Status < 600:
			r.R5xx++
		}
		denom := e.InputTokens
		if e.Wire == metricsrelay.WireAnthropic {
			denom += e.CacheReadTokens
		}
		r.hitDenom += denom
		r.hitRead += e.CacheReadTokens
		if now.Sub(e.At) < time.Hour {
			r.hour++
		}
		r.last = e.At // ascending walk: the last write is the newest
		durs[k] = append(durs[k], e.DurationMS)
		if p, ok := prices[e.Model]; ok {
			r.Priced = true
			r.Cost += entryCost(e, p)
		}
	}
	rows := make([]metricsUsageRow, 0, len(byKey))
	for k, r := range byKey {
		r.Runs = len(runs[k])
		sorted := durs[k]
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		r.P50 = latencyDur(time.Duration(percentile(sorted, 50)) * time.Millisecond)
		r.P95 = latencyDur(time.Duration(percentile(sorted, 95)) * time.Millisecond)
		r.HitPct = "—"
		if r.hitDenom > 0 {
			r.HitPct = fmt.Sprintf("%.1f%%", 100*float64(r.hitRead)/float64(r.hitDenom))
		}
		r.RPM = strconv.FormatFloat(float64(r.hour)/60, 'f', 1, 64)
		r.Last = relTime(r.last, now)
		rows = append(rows, *r)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Calls != rows[j].Calls {
			return rows[i].Calls > rows[j].Calls
		}
		return rows[i].Key < rows[j].Key
	})
	return rows
}

// percentile is nearest-rank over an ascending slice: the smallest value
// covering p% of the samples.
func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p*len(sorted) + 99) / 100
	if idx > 0 {
		idx--
	}
	return sorted[min(idx, len(sorted)-1)]
}

// loopStats carries the two loop averages over the runs that have both
// sides of each.
type loopStats struct {
	DevAvg   time.Duration
	DevRuns  int
	TestAvg  time.Duration
	TestRuns int
}

// runLoop is one keyed run's transition bookkeeping: the last
// implementation call before the run's first matching review, and that
// review's time.
type runLoop struct {
	lastImpl time.Time
	review   time.Time
}

// foldLoops averages the dev→review and test→review loop gaps: per keyed
// run, the gap from the last implementation call (dev, or test) to the
// first matching review call. First review only — later review rounds
// re-enter the loop with fixes and would skew the reading — and the
// implementation side freezes at the last call before that review, so a
// fix round after it cannot turn the gap negative. Unkeyed rounds (empty
// Task) drop out. Known approximation, accepted: the gap includes
// non-provider time (local test runs, tool exec) and misses any
// direct-dial calls.
func foldLoops(entries []metricsrelay.Entry) loopStats {
	dev := map[string]*runLoop{}
	test := map[string]*runLoop{}
	for _, e := range entries {
		if e.Task == "" {
			continue
		}
		var m map[string]*runLoop
		impl, review := false, false
		switch e.Role {
		case string(activities.RoleDev):
			m, impl = dev, true
		case string(activities.RoleDevReview):
			m, review = dev, true
		case string(activities.RoleTest):
			m, impl = test, true
		case string(activities.RoleTestReview):
			m, review = test, true
		default:
			continue
		}
		l := m[e.Task]
		if l == nil {
			l = &runLoop{}
			m[e.Task] = l
		}
		switch {
		case impl:
			if l.review.IsZero() {
				l.lastImpl = e.At
			}
		case review && l.review.IsZero():
			l.review = e.At
		}
	}
	st := loopStats{}
	st.DevAvg, st.DevRuns = averageLoop(dev)
	st.TestAvg, st.TestRuns = averageLoop(test)
	return st
}

// averageLoop means one loop map's per-run gaps; the runs without both
// sides contribute nothing.
func averageLoop(m map[string]*runLoop) (avg time.Duration, runs int) {
	for _, l := range m {
		if l.review.IsZero() || l.lastImpl.IsZero() {
			continue
		}
		avg += l.review.Sub(l.lastImpl)
		runs++
	}
	if runs > 0 {
		avg /= time.Duration(runs)
	}
	return avg, runs
}

// foldDrift measures the context-creep signal: within a keyed round (the
// task+role+round group), how far the input grew from the first call to
// the last, averaged over the groups with at least two calls — a single
// call has no first→last span to measure.
func foldDrift(entries []metricsrelay.Entry) (avg int64, groups int) {
	type groupKey struct{ task, role, round string }
	byGroup := map[groupKey]*driftGroup{}
	for _, e := range entries {
		if e.Task == "" {
			continue
		}
		k := groupKey{e.Task, e.Role, e.Round}
		g := byGroup[k]
		if g == nil {
			g = &driftGroup{}
			byGroup[k] = g
		}
		g.calls++
		g.last = e.InputTokens
		if g.calls == 1 {
			g.first = e.InputTokens
		}
	}
	var sum int64
	for _, g := range byGroup {
		if g.calls < 2 {
			continue
		}
		sum += g.last - g.first
		groups++
	}
	if groups > 0 {
		avg = sum / int64(groups)
	}
	return avg, groups
}

type driftGroup struct {
	first, last int64
	calls       int
}

// fmtTokens humanizes a token count for the tables and tiles: raw under a
// thousand, one decimal in thousands, two in millions.
func fmtTokens(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.2fM", float64(n)/1e6)
	case n >= 1000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	default:
		return strconv.FormatInt(n, 10)
	}
}

// fmtUSD renders a cost: four decimals where the figure is small enough
// to need them, cents beyond a hundred dollars.
func fmtUSD(v float64) string {
	if v >= 100 {
		return fmt.Sprintf("$%.2f", v)
	}
	return fmt.Sprintf("$%.4f", v)
}

// relayTotals is the frozen metrics tab's headline counters and slim
// per-worker rows — the face that view has always rendered, now
// projected from the same folds the Models tab reads so the two views
// can never disagree. Errors keeps the non-2xx count.
type relayTotals struct {
	Calls  int64
	Hour   int64
	Errors int64
	In     int64
	Out    int64
	Window string
	Worker []relayWorkerTotals
}

type relayWorkerTotals struct {
	Worker         string
	Calls, In, Out int64
	Errors         int64
	Last           string
}

// foldRelayTotals reduces the ledger to that face: the counters over one
// pass, the worker rows projected from the usage fold.
func foldRelayTotals(entries []metricsrelay.Entry, now time.Time) relayTotals {
	st := relayTotals{Window: "no relayed calls yet"}
	var oldest time.Time
	for _, e := range entries {
		st.Calls++
		if now.Sub(e.At) < time.Hour {
			st.Hour++
		}
		if e.Status < 200 || e.Status >= 300 {
			st.Errors++
		}
		st.In += e.InputTokens
		st.Out += e.OutputTokens
		if oldest.IsZero() || e.At.Before(oldest) {
			oldest = e.At
		}
	}
	for _, r := range foldUsage(entries, now, nil, usageWorkerKey) {
		st.Worker = append(st.Worker, relayWorkerTotals{
			Worker: r.Key,
			Calls:  r.Calls,
			In:     r.In,
			Out:    r.Out,
			Errors: r.Calls - r.OK2xx,
			Last:   r.Last,
		})
	}
	if len(entries) > 0 {
		st.Window = dur(now.Sub(oldest).Round(time.Second)) + " window"
	}
	return st
}

// uiModelFormRow is one price-form row: a ledger-known model, its call
// count and last-seen time, and its saved rates rendered back into the
// inputs. The unknown bucket is named but rate-less (Unknown).
type uiModelFormRow struct {
	Model           string
	Calls           int64
	Last            string
	In, Out, Cached string
	Unknown         bool
}

// buildFormRows lays the form out over every ledger-known model — pricing
// unseen models is speculative, the ledger is the only source of names.
func buildFormRows(modelRows []metricsUsageRow, prices map[string]modelPrice) []uiModelFormRow {
	rows := make([]uiModelFormRow, 0, len(modelRows))
	for _, r := range modelRows {
		row := uiModelFormRow{Model: r.Key, Calls: r.Calls, Last: r.Last, Unknown: r.Key == modelUnknown}
		if p, ok := prices[r.Key]; ok && !row.Unknown {
			row.In = formatRate(p.In)
			row.Out = formatRate(p.Out)
			row.Cached = formatRate(p.Cached)
		}
		rows = append(rows, row)
	}
	return rows
}

// formatRate renders a saved rate back into an input: shortest exact
// decimal form — 'f' keeps the small per-token rates off scientific
// notation.
func formatRate(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// pageModels renders the Models tab: the price form over every
// ledger-known model, plus the usage tables and derived tiles — all from
// one fold and one price snapshot, so the tab can never show a table
// disagreeing with its tiles. The charts serve at /full-metrics (the
// golens-metrics mount, golens.go), fed from the same ledger.
func (s *uiServer) pageModels(w http.ResponseWriter, req *http.Request) {
	now := time.Now()
	v := buildModelsView(now, s.ledgerEntries(), s.currentPrices())
	s.render(w, http.StatusOK, "page_models", struct {
		Chrome uiChrome
		V      uiModelsView
	}{uiChrome{Nav: "models", Title: "daedalus — models"}, v})
}

// fragModels is the Models tab's polled region — the tiles and usage
// tables, re-folded from the ledger each tick. The price form stays
// outside the region (the page template wraps only the read-only
// surfaces): a poll may never swap editable inputs out from under the
// operator — the runs feed's filter-selects rule.
func (s *uiServer) fragModels(w http.ResponseWriter, req *http.Request) {
	v := buildModelsView(time.Now(), s.ledgerEntries(), s.currentPrices())
	s.renderFrag(w, req, "frag_models", v)
}

// saveModels upserts the submitted rates and redirects back to the form.
// Only the form's own editable rows are taken (parseModelPrices), the
// file is rewritten atomically, and the server's snapshot swaps to the
// reloaded table — the next render prices through the new rates.
func (s *uiServer) saveModels(w http.ResponseWriter, req *http.Request) {
	if err := req.ParseForm(); err != nil {
		http.Error(w, "read form: "+err.Error(), http.StatusBadRequest)
		return
	}
	// Pricable = the ledger-known models minus the unknown bucket — the
	// form's editable rows exactly, recomputed fresh so a rotation between
	// render and submit cannot price a name the ledger no longer knows.
	pricable := map[string]bool{}
	for _, m := range foldUsage(s.ledgerEntries(), time.Now(), nil, usageModelKey) {
		if m.Key != modelUnknown {
			pricable[m.Key] = true
		}
	}
	if upsert := parseModelPrices(req.Form, pricable); len(upsert) > 0 {
		if err := saveModelPrices(upsert); err != nil {
			http.Error(w, "save model prices: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	s.swapPrices(loadModelPrices())
	http.Redirect(w, req, "/models", http.StatusSeeOther)
}

// parseModelPrices decodes the submitted form into an upsert set. Field
// names carry the model (in[<model>], out[…], cached[…]). A row is
// written when any of its three fields is present in the post — the
// browser form submits every row, so clearing a row's fields prices
// that model at 0. Within a written row, unparsable and negative input
// lands as 0 (only finite non-negative numbers are taken). A save never
// deletes: keys absent from the post survive. Unknown field prefixes and
// names outside pricable are dropped, so a forged post can neither price
// the unknown bucket, invent models, nor zero a model's saved rates by
// naming it under a field the form never emits.
func parseModelPrices(form url.Values, pricable map[string]bool) map[string]modelPrice {
	type submittedRow struct{ in, out, cached *string }
	rows := map[string]*submittedRow{}
	for key, vals := range form {
		prefix, name, found := strings.Cut(key, "[")
		if !found || !strings.HasSuffix(name, "]") || len(vals) == 0 {
			continue
		}
		name = strings.TrimSuffix(name, "]")
		if !pricable[name] {
			continue
		}
		switch prefix {
		case "in", "out", "cached":
		default:
			continue
		}
		r := rows[name]
		if r == nil {
			r = &submittedRow{}
			rows[name] = r
		}
		switch prefix {
		case "in":
			setOnce(&r.in, vals[0])
		case "out":
			setOnce(&r.out, vals[0])
		case "cached":
			setOnce(&r.cached, vals[0])
		}
	}
	upsert := map[string]modelPrice{}
	for name, r := range rows {
		upsert[name] = modelPrice{In: parseRate(r.in), Out: parseRate(r.out), Cached: parseRate(r.cached)}
	}
	return upsert
}

// setOnce keeps a field's first value — ParseForm has merged the body
// already, this is only idempotence against repeated names.
func setOnce(dst **string, v string) {
	if *dst == nil {
		*dst = &v
	}
}

// parseRate decodes one submitted rate: only finite non-negative
// numbers are taken — an unparsable, negative, NaN, or infinite value
// lands as 0, never an error (a non-finite rate would also make the
// JSON save fail); a field the form never carried is 0 too.
func parseRate(v *string) float64 {
	if v == nil {
		return 0
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(*v), 64)
	if err != nil || f < 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}

// uiDerivedTiles is the models view's headline row. Every tile renders
// the dash when its denominator is zero — an average over nothing is a
// fabricated signal.
type uiDerivedTiles struct {
	TokensPerRun    string
	TokensPerWorker string
	DevLoop         string
	TestLoop        string
	Drift           string
}

// buildTiles averages the whole window: tokens per keyed run, tokens per
// worker, the two loop averages, and the per-round input drift.
func buildTiles(entries []metricsrelay.Entry, workers []metricsUsageRow) uiDerivedTiles {
	t := uiDerivedTiles{TokensPerRun: "—", TokensPerWorker: "—", DevLoop: "—", TestLoop: "—", Drift: "—"}
	var total int64
	runs := map[string]bool{}
	for _, e := range entries {
		total += e.InputTokens + e.OutputTokens
		if e.Task != "" {
			runs[e.Task] = true
		}
	}
	if n := len(runs); n > 0 {
		t.TokensPerRun = fmtTokens(total / int64(n))
	}
	if n := len(workers); n > 0 {
		t.TokensPerWorker = fmtTokens(total / int64(n))
	}
	loops := foldLoops(entries)
	if loops.DevRuns > 0 {
		t.DevLoop = dur(loops.DevAvg)
	}
	if loops.TestRuns > 0 {
		t.TestLoop = dur(loops.TestAvg)
	}
	if drift, groups := foldDrift(entries); groups > 0 {
		t.Drift = fmtTokens(drift)
	}
	return t
}

// ledgerWindow names the span the aggregates describe — the ring holds a
// bounded window, and every figure on the page is that window's figure.
func ledgerWindow(entries []metricsrelay.Entry, now time.Time) string {
	if len(entries) == 0 {
		return "no relayed calls yet"
	}
	return dur(now.Sub(entries[0].At).Round(time.Second)) + " window"
}

// usageTable is the shared usage-table template's data: the key column's
// head and the fold rows beneath it.
type usageTable struct {
	Head string
	Rows []metricsUsageRow
}

// uiModelsView is the Models tab's data: the folds and tiles. The charts
// serve at /full-metrics (the golens-metrics mount, golens.go), fed from
// the same ledger.
type uiModelsView struct {
	Window      string
	FormRows    []uiModelFormRow
	Tiles       uiDerivedTiles
	ModelTable  usageTable
	WorkerTable usageTable
}

// buildModelsView folds one ledger window into those surfaces.
func buildModelsView(now time.Time, entries []metricsrelay.Entry, prices map[string]modelPrice) uiModelsView {
	v := uiModelsView{Window: ledgerWindow(entries, now)}
	models := foldUsage(entries, now, prices, usageModelKey)
	workers := foldUsage(entries, now, prices, usageWorkerKey)
	v.ModelTable = usageTable{Head: "Model", Rows: models}
	v.WorkerTable = usageTable{Head: "Worker", Rows: workers}
	v.FormRows = buildFormRows(models, prices)
	v.Tiles = buildTiles(entries, workers)
	return v
}

// uiPricePreview is the what-if modal's payload: the whole ledger window's
// token totals and the current total cost at the saved snapshot, plus —
// when a model name was submitted — that name's block. Every money figure
// arrives preformatted; a missing price renders the dash, never zero.
type uiPricePreview struct {
	Window                string
	Calls                 int64
	In, Out               int64
	CacheRead, CacheWrite int64
	Current               string
	WhatIf                *uiPriceWhatIf
}

// uiPriceWhatIf is the named model's block: its saved rates and the whole
// window repriced at them. Note carries the no-saved-rates face — the
// fields above it render the dash then.
type uiPriceWhatIf struct {
	Name            string
	In, Out, Cached string
	Total           string
	Delta           string
	Note            string
}

// buildPricePreview folds the ledger window for the what-if modal: the
// token totals, the current total cost (priced entries only — a window
// nothing prices renders the dash, never zero), and, when a name is given,
// the whole window repriced at that one model's saved rates through the
// same entryCost the tables price with. Nothing here writes — the preview
// is stateless, storage still happens only at the form's save.
func buildPricePreview(now time.Time, entries []metricsrelay.Entry, prices map[string]modelPrice, name string) uiPricePreview {
	v := uiPricePreview{Window: ledgerWindow(entries, now)}
	rates, named := prices[name] // zero rates, false when the name is empty or unknown
	var cur, whatIf float64
	priced := false
	for _, e := range entries {
		v.Calls++
		v.In += e.InputTokens
		v.Out += e.OutputTokens
		v.CacheRead += e.CacheReadTokens
		v.CacheWrite += e.CacheWriteTokens
		if p, ok := prices[e.Model]; ok {
			priced = true
			cur += entryCost(e, p)
		}
		if named {
			whatIf += entryCost(e, rates)
		}
	}
	v.Current = "—"
	if priced {
		v.Current = fmtUSD(cur)
	}
	if name != "" {
		w := &uiPriceWhatIf{Name: name, In: "—", Out: "—", Cached: "—", Total: "—", Delta: "—"}
		if named {
			w.In, w.Out, w.Cached = formatRate(rates.In), formatRate(rates.Out), formatRate(rates.Cached)
			w.Total = fmtUSD(whatIf)
			if priced {
				w.Delta = fmtUSD(whatIf - cur)
				if whatIf < cur {
					w.Delta = "-" + fmtUSD(cur-whatIf)
				}
			}
		} else {
			w.Note = "no saved rates for " + name
		}
		v.WhatIf = w
	}
	return v
}

// previewModels serves the what-if modal's fragment (query param model,
// empty allowed): the window's baseline plus the named what-if, nothing
// persisted — no file write, no snapshot swap, no server state. Plain
// render, not renderFrag: the modal is fetched on demand, never polled,
// and a 204 would leave a reopened modal unserved.
func (s *uiServer) previewModels(w http.ResponseWriter, req *http.Request) {
	v := buildPricePreview(time.Now(), s.ledgerEntries(), s.currentPrices(), strings.TrimSpace(req.URL.Query().Get("model")))
	s.render(w, http.StatusOK, "frag_preview", v)
}

// uiRunModelCost is one model's row in the run-detail cost section.
type uiRunModelCost struct {
	Model string
	Calls int64
	Cost  string

	cost float64 // raw, for the sort; Cost carries the rendered figure
}

// runLedgerCost folds the run's own ledger entries into the detail
// page's cost section: per-model rows plus the total, priced models only
// — the dash when no prices are saved or the run has no priced keyed
// calls. Matched by Task, the attribution key's scrubbed workflow id.
func (s *uiServer) runLedgerCost(workflowID string) ([]uiRunModelCost, string) {
	prices := s.currentPrices()
	if len(prices) == 0 {
		return nil, "—"
	}
	task := ledgerTaskID(workflowID)
	byModel := map[string]*uiRunModelCost{}
	var sum float64
	for _, e := range s.ledgerEntries() {
		if e.Task != task {
			continue
		}
		p, ok := prices[e.Model]
		if !ok {
			continue // unpriced models contribute nothing
		}
		r := byModel[e.Model]
		if r == nil {
			r = &uiRunModelCost{Model: e.Model}
			byModel[e.Model] = r
		}
		r.Calls++
		c := entryCost(e, p)
		r.cost += c
		sum += c
	}
	if len(byModel) == 0 {
		return nil, "—"
	}
	rows := make([]uiRunModelCost, 0, len(byModel))
	for _, r := range byModel {
		r.Cost = fmtUSD(r.cost)
		rows = append(rows, *r)
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].cost > rows[j].cost })
	return rows, fmtUSD(sum)
}

// ledgerTaskID recovers the Task value a run's ledger entries carry: the
// attribution key scrubs its parse anchors out of the workflow id, so a
// run-detail match must scrub the same way.
func ledgerTaskID(workflowID string) string {
	return strings.NewReplacer("-role-", "_role_", "-round-", "_round_").Replace(workflowID)
}
