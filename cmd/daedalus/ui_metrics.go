// The metrics view's data layer: the operator-editable model price table,
// the ledger folds behind the usage tables, derived tiles, and charts, and
// the server-built SVG line charts. Everything reads the relay ledger the
// way `metrics list` does — one clamped full-ring query, then plain folds;
// a 10k-entry pass is microseconds, so no cache layer. Cost is computed at
// render time from the saved prices and never stored — a retroactive price
// edit reprices the whole window on the next render, and Entry.CostUSD
// stays the reserved zero it has always been.
package main

import (
	"encoding/json"
	"fmt"
	"html"
	"html/template"
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
		r.P50 = dur(time.Duration(percentile(sorted, 50)) * time.Millisecond)
		r.P95 = dur(time.Duration(percentile(sorted, 95)) * time.Millisecond)
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

// chartTokens formats a bucketed token sum — the buckets accumulate in
// float, the counts are integers, so round back.
func chartTokens(v float64) string { return fmtTokens(int64(v + 0.5)) }

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
// ledger-known model, plus the round-2 surfaces — the usage tables, the
// derived tiles, and the token and cost charts — all from one fold and
// one price snapshot, so the tab can never show a table disagreeing with
// its charts.
func (s *uiServer) pageModels(w http.ResponseWriter, req *http.Request) {
	now := time.Now()
	v := buildModelsView(now, s.ledgerEntries(), s.currentPrices())
	s.render(w, http.StatusOK, "page_models", struct {
		Chrome uiChrome
		V      uiModelsView
	}{uiChrome{Nav: "models", Title: "daedalus — models"}, v})
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

// uiNamedChart is one mini-chart card: its server-built SVG, whose
// caption names the worker or model it covers.
type uiNamedChart struct {
	Chart template.HTML
}

// uiModelsView is the Models tab's data. The token charts render once the
// ledger has entries; the cost charts only when at least one model is
// priced, each captioning itself as covering priced models only.
type uiModelsView struct {
	Window      string
	FormRows    []uiModelFormRow
	Tiles       uiDerivedTiles
	ModelTable  usageTable
	WorkerTable usageTable

	TokenChart       template.HTML
	WorkerCharts     []uiNamedChart
	CostChart        template.HTML
	WorkerCostCharts []uiNamedChart
	ModelCostCharts  []uiNamedChart
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

	g, ok := newChartGrid(entries, now)
	if !ok {
		return v
	}
	v.TokenChart = buildLineChart(g, "Tokens — the whole ledger window", bucketTokens(g, entries))
	byWorker := groupEntries(entries, usageWorkerKey)
	for _, w := range workers {
		v.WorkerCharts = append(v.WorkerCharts, uiNamedChart{
			Chart: buildLineChart(g, w.Key+" — tokens", bucketTokens(g, byWorker[w.Key])),
		})
	}
	if len(prices) > 0 {
		v.CostChart = buildLineChart(g, "Cost — priced models only", bucketCost(g, entries, prices))
		byModel := groupEntries(entries, usageModelKey)
		for _, m := range models {
			if m.Priced {
				v.ModelCostCharts = append(v.ModelCostCharts, uiNamedChart{
					Chart: buildLineChart(g, m.Key+" — cost (priced models only)", bucketCost(g, byModel[m.Key], prices)),
				})
			}
		}
		for _, w := range workers {
			v.WorkerCostCharts = append(v.WorkerCostCharts, uiNamedChart{
				Chart: buildLineChart(g, w.Key+" — cost (priced models only)", bucketCost(g, byWorker[w.Key], prices)),
			})
		}
	}
	return v
}

// groupEntries buckets the window once per key function, so the
// mini-charts filter without rescanning the ledger per card.
func groupEntries(entries []metricsrelay.Entry, keyOf func(metricsrelay.Entry) string) map[string][]metricsrelay.Entry {
	groups := map[string][]metricsrelay.Entry{}
	for _, e := range entries {
		k := keyOf(e)
		groups[k] = append(groups[k], e)
	}
	return groups
}

// chartBuckets is the fixed bucket count every chart spans — 24 equal
// widths over the ledger window (oldest entry → now), one shared grid
// for the global chart and every mini-chart (Q3).
const chartBuckets = 24

// chartGrid is that shared geometry.
type chartGrid struct {
	start time.Time
	step  time.Duration
	now   time.Time
}

// newChartGrid derives the grid from the ascending entries; ok is false
// for an empty window — no entries, no chart.
func newChartGrid(entries []metricsrelay.Entry, now time.Time) (g chartGrid, ok bool) {
	if len(entries) == 0 {
		return g, false
	}
	g = chartGrid{start: entries[0].At, now: now, step: now.Sub(entries[0].At) / chartBuckets}
	if g.step <= 0 {
		g.step = time.Millisecond // a single instant still buckets, never divides by zero
	}
	return g, true
}

// bucketOf indexes an entry's bucket, clamping the right edge in.
func (g chartGrid) bucketOf(t time.Time) int {
	idx := int(t.Sub(g.start) / g.step)
	if idx < 0 {
		return 0
	}
	return min(idx, chartBuckets-1)
}

// chartColor is a series color's light/dark pair, emitted as Tailwind
// arbitrary-value classes so the SVG recolors with the theme toggle (the
// header countdown ring's mechanism). The pairs are validated against
// both dashboard surfaces (white / slate-900); the accepted WARN is
// light-mode aqua at 2.74:1 on white — relieved by the direct labels and
// the tables rendered beside every chart.
type chartColor struct{ Light, Dark string }

func (c chartColor) strokeClass() string {
	return "stroke-[" + c.Light + "] dark:stroke-[" + c.Dark + "]"
}
func (c chartColor) fillClass() string { return "fill-[" + c.Light + "] dark:fill-[" + c.Dark + "]" }
func (c chartColor) bgClass() string   { return "bg-[" + c.Light + "] dark:bg-[" + c.Dark + "]" }

var (
	colorIn   = chartColor{"#2a78d6", "#3987e5"}
	colorOut  = chartColor{"#1baf7a", "#199e70"}
	colorCost = chartColor{"#6250d6", "#9085e9"}
)

// chartSeries is one line: its legend label, its per-bucket sums (one
// shared y-axis across a chart's series), its validated color pair, and
// the value formatter its labels use — tokens and cost never share a
// chart, and cost is single-series, never a dual axis.
type chartSeries struct {
	Label  string
	Points [chartBuckets]float64
	Color  chartColor
	Format func(float64) string
}

// bucketTokens accumulates the entries' in/out sums into a fresh series
// pair on the shared grid.
func bucketTokens(g chartGrid, entries []metricsrelay.Entry) []chartSeries {
	in := chartSeries{Label: "in", Color: colorIn, Format: chartTokens}
	out := chartSeries{Label: "out", Color: colorOut, Format: chartTokens}
	for _, e := range entries {
		i := g.bucketOf(e.At)
		in.Points[i] += float64(e.InputTokens)
		out.Points[i] += float64(e.OutputTokens)
	}
	return []chartSeries{in, out}
}

// bucketCost accumulates the entries' render-time cost into one series,
// priced models only — an unpriced model contributes nothing.
func bucketCost(g chartGrid, entries []metricsrelay.Entry, prices map[string]modelPrice) []chartSeries {
	cost := chartSeries{Label: "cost", Color: colorCost, Format: fmtUSD}
	for _, e := range entries {
		if p, ok := prices[e.Model]; ok {
			cost.Points[g.bucketOf(e.At)] += entryCost(e, p)
		}
	}
	return []chartSeries{cost}
}

// buildLineChart renders one chart's card content: a polyline per series,
// a <title> hover point per bucket — the dashboard's tooltip idiom — the
// legend row with each series' current (last-bucket) value, and the
// window's end labels. No per-bucket grid, no client JS, no chart
// library. Every interpolation is escaped here: the result returns as
// template.HTML, trusted markup.
func buildLineChart(g chartGrid, caption string, series []chartSeries) template.HTML {
	const (
		w, h   = 560.0, 148.0
		topPad = 8.0
		bottom = 132.0
	)
	maxv := 0.0
	for _, s := range series {
		for _, v := range s.Points {
			maxv = max(maxv, v)
		}
	}
	y := func(v float64) float64 {
		if maxv <= 0 {
			return bottom
		}
		return bottom - v/maxv*(bottom-topPad)
	}
	x := func(i int) float64 { return float64(i) * w / (chartBuckets - 1) }

	var b strings.Builder
	b.WriteString(`<div class="font-medium mb-2">` + html.EscapeString(caption) + `</div>`)
	b.WriteString(`<svg viewBox="0 0 560 148" class="w-full h-auto" role="img" aria-label="` + html.EscapeString(caption) + `">`)
	for _, s := range series {
		pts := make([]string, chartBuckets)
		for i, v := range s.Points {
			pts[i] = fmt.Sprintf("%.1f,%.1f", x(i), y(v))
		}
		b.WriteString(`<polyline fill="none" stroke-width="1.5" class="` + s.Color.strokeClass() + `" points="` + strings.Join(pts, " ") + `"/>`)
	}
	for _, s := range series {
		for i, v := range s.Points {
			bucket := g.start.Add(time.Duration(i) * g.step)
			b.WriteString(`<circle cx="` + strconv.FormatFloat(x(i), 'f', 1, 64) +
				`" cy="` + strconv.FormatFloat(y(v), 'f', 1, 64) +
				`" r="2.5" class="` + s.Color.fillClass() +
				`"><title>` + clockOrDate(bucket) + ` · ` + html.EscapeString(s.Label) + ` ` + s.Format(v) + `</title></circle>`)
		}
	}
	b.WriteString(`</svg><div class="flex gap-4 flex-wrap text-xs text-slate-500 mt-1">`)
	for _, s := range series {
		b.WriteString(`<span class="flex items-center gap-1.5"><span class="inline-block h-2 w-2 rounded-full ` + s.Color.bgClass() + `"></span>` +
			html.EscapeString(s.Label) + ` <span class="mono">` + s.Format(s.Points[chartBuckets-1]) + `</span></span>`)
	}
	b.WriteString(`</div><div class="flex justify-between text-[10px] text-slate-500 mono mt-1"><span>` +
		clockOrDate(g.start) + `</span><span>` + clockOrDate(g.now) + `</span></div>`)
	return template.HTML(b.String())
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
