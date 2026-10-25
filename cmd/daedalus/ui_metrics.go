// The metrics view's data layer: the operator-editable model price table
// (the model_prices table of the shared metrics database — nil store in
// memory mode, where the snapshot is the whole store), the ledger folds
// behind the usage tables, and derived tiles. Everything reads the relay
// ledger the way `dash list` does — one full-ledger ascending read (the
// sqlite table; age-pruned, not count-bounded), then plain folds. Cost is
// computed at render time from the saved prices and never stored — a
// retroactive price edit reprices the whole window on the next render,
// and Entry.CostUSD stays the reserved zero it has always been.
// Charts live in the golens-metrics dashboard (golens.go) at
// /full-metrics, fed per append from the same ledger — the server-built
// SVG charts this file once rendered were removed once that mount served.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
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

// modelPricesSchema is the price table's namespaced slice of the shared
// metrics database (Q1: the JSON file retires; the upsert-never-delete
// semantics carry over, and the atomic-write dance is inherited from
// sqlite's transactionality). "in" is quoted — a keyword the parser
// would otherwise choke on.
const modelPricesSchema = `
CREATE TABLE IF NOT EXISTS model_prices (
    model TEXT PRIMARY KEY,
    "in" REAL NOT NULL,
    out REAL NOT NULL,
    cached REAL NOT NULL
)`

// priceStore is the model price table's own connection to the shared
// metrics database — the models screen's saved rates. Opened through
// metricsrelay.StoreDSN, the one helper every connection to the file
// routes through (one file, several owners, each opening its own
// connection; WAL so readers never block, busy timeout for colliding
// writers). Nil means memory mode: the price snapshot is the whole
// store, nothing touches disk.
type priceStore struct {
	db *sql.DB
}

func openPriceStore(path string) (*priceStore, error) {
	db, err := sql.Open("sqlite", metricsrelay.StoreDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open model prices sqlite: %w", err)
	}
	if _, err := db.Exec(modelPricesSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init model prices schema: %w", err)
	}
	return &priceStore{db: db}, nil
}

// Close releases the database connection.
func (p *priceStore) Close() error { return p.db.Close() }

// load reads the saved table. Nil store (memory mode) reads as no table.
// A failed read logs and reads as empty too — refusing the daemon's boot
// over prices would trade every dashboard view for one bad table, and the
// next save rewrites the rows whole.
func (p *priceStore) load() map[string]modelPrice {
	if p == nil {
		return nil
	}
	rows, err := p.db.Query(`SELECT model, "in", out, cached FROM model_prices`)
	if err != nil {
		log.Printf("metrics: read model prices: %v", err)
		return nil
	}
	defer rows.Close()
	prices := map[string]modelPrice{}
	for rows.Next() {
		var name string
		var mp modelPrice
		if err := rows.Scan(&name, &mp.In, &mp.Out, &mp.Cached); err != nil {
			log.Printf("metrics: scan model prices: %v", err)
			return nil
		}
		prices[name] = mp
	}
	if err := rows.Err(); err != nil {
		log.Printf("metrics: read model prices: %v", err)
		return nil
	}
	return prices
}

// upsert merges the submitted entries into the saved table: an upsert
// that never deletes — model names do not change and prices are only ever
// added, so entries survive retention prunes and daemon restarts by
// construction; the only removal path is hand-editing the db, the
// accepted escape hatch. One transaction, so the merge lands whole or
// not at all.
func (p *priceStore) upsert(m map[string]modelPrice) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT INTO model_prices (model, "in", out, cached) VALUES (?, ?, ?, ?)
		ON CONFLICT(model) DO UPDATE SET "in" = excluded."in", out = excluded.out, cached = excluded.cached`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for name, mp := range m {
		if _, err := stmt.Exec(name, mp.In, mp.Out, mp.Cached); err != nil {
			return err
		}
	}
	return tx.Commit()
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

// savePrices merges the submitted entries into the saved table and
// refreshes the snapshot to the reloaded one. sqlite mode: the
// priceStore's transactional upsert, then a reload — the next render
// prices through the new rates. Memory mode: there is no disk, so the
// merge into a fresh clone of the snapshot is the whole save.
func (s *uiServer) savePrices(upsert map[string]modelPrice) error {
	if s.priceStore == nil {
		merged := make(map[string]modelPrice, len(s.currentPrices())+len(upsert))
		for name, p := range s.currentPrices() {
			merged[name] = p
		}
		for name, p := range upsert {
			merged[name] = p
		}
		s.swapPrices(merged)
		return nil
	}
	if err := s.priceStore.upsert(upsert); err != nil {
		return err
	}
	s.swapPrices(s.priceStore.load())
	return nil
}

// ledgerEntries reads the whole ledger once, ascending by At — every
// first/last bookkeeping in the folds depends on chronological order
// (the sqlite read returns it ordered; the memory slice is append-order,
// so the sort stands for both). External otel batches are dropped here
// (D6): they render only on the golens-served full-metrics page, never
// in a main-dashboard fold.
func (s *uiServer) ledgerEntries() []metricsrelay.Entry {
	if s.ledger == nil {
		return nil
	}
	entries := s.ledger.Ascending()
	kept := make([]metricsrelay.Entry, 0, len(entries))
	for _, e := range entries {
		if e.Source != metricsrelay.SourceOtelExternal {
			kept = append(kept, e)
		}
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].At.Before(kept[j].At) })
	return kept
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
// rate), output at the out rate, cache reads at the cached rate. The
// input side is wire-aware (inputTokensExCache): openai's prompt_tokens
// includes the cached reads, so billing it whole at the in rate would
// bill every cached token twice — once inside input, once at the cached
// rate.
func entryCost(e metricsrelay.Entry, p modelPrice) float64 {
	in := inputTokensExCache(e)
	return float64(in+e.CacheWriteTokens)/1e6*p.In +
		float64(e.OutputTokens)/1e6*p.Out +
		float64(e.CacheReadTokens)/1e6*p.Cached
}

// inputTokensExCache is the pricing-grade input figure: the non-cached
// input side each wire reports, so every token class bills exactly once.
// openai's prompt_tokens includes the cached reads (a subset of input);
// the anthropic wire's input_tokens already excludes both cache sides. A
// provider reporting cached > prompt would bill a negative — clamped at
// zero. The ledger entries stay provider-native raw; display surfaces
// render inputTokensTotal instead.
func inputTokensExCache(e metricsrelay.Entry) int64 {
	in := e.InputTokens
	if e.Wire == metricsrelay.WireOpenAI {
		in -= e.CacheReadTokens
		if in < 0 {
			in = 0
		}
	}
	return in
}

// inputTokensTotal is the dash's view normalization: input rendered as
// the whole input side of the call, the cache columns visibly subsets of
// it — the raw wire-native figures read cache reads dwarfing input on
// cache-heavy calls, because openai's input already contains the reads
// (a subset) while the anthropic wire's input excludes both cache sides
// (disjoint). The usage tables, the hit% denominators, the tokens/run
// and drift and Tokens/worker tiles, the price preview, and the golens
// feed's input samples all read this; pricing keeps inputTokensExCache,
// and the -cot usage stamp keeps the claude-native raw counts (claude
// only, disjoint classes there).
func inputTokensTotal(e metricsrelay.Entry) int64 {
	if e.Wire == metricsrelay.WireOpenAI {
		return e.InputTokens // prompt_tokens already includes the cached reads
	}
	// Anthropic semantics — the relay's anthropic wire and the claude
	// otel fold alike: input_tokens excludes both cache sides.
	return e.InputTokens + e.CacheReadTokens + e.CacheWriteTokens
}

// metricsUsageRow is one fold result row, display-ready — the replicated
// column set both the per-model and the per-worker table render. HitPct,
// P50, P95, RPM, and Last are preformatted (a dash where the denominator
// is zero); Cost stays raw for the usd template func, with Priced gating
// it — unpriced rows render the dash, never a zero.
type metricsUsageRow struct {
	Key string
	// Src stamps the row's capture era: "" is the relay's own
	// observation, "otel" an export-batch fold — the template renders the
	// otel- key prefix and D1's em-dash set from it.
	Src                     string
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

// rowKey is foldUsage's grouping key: the fold's own key plus the entry's
// capture era. A relay row and an otel row of the same model stay separate
// rows — pre-otel relay history sits beside otel-folded rows, and
// D1's em-dash set applies per row — never summed into one.
type rowKey struct {
	key  string
	otel bool
}

// foldUsage reduces the ascending ledger into one row per key: the token
// sums (input rendered through inputTokensTotal — the whole input side,
// cache columns subsets of it), the distinct keyed runs, the
// dev/test/review call split, the cache-hit ratio over that same whole
// input (reads are a subset of the denominator on every wire; aggregated
// as summed reads over summed denominators, never averaged ratios), the
// status split, nearest-rank p50/p95 durations, the trailing-hour call
// rate, and the render-time cost of the priced models. Rows order by
// call count — the tables' and the charts' shared order.
func foldUsage(entries []metricsrelay.Entry, now time.Time, prices map[string]modelPrice, keyOf func(metricsrelay.Entry) string) []metricsUsageRow {
	byKey := map[rowKey]*metricsUsageRow{}
	durs := map[rowKey][]int64{}
	runs := map[rowKey]map[string]bool{}
	for _, e := range entries {
		k := rowKey{keyOf(e), e.Source != ""}
		if k.key == "" {
			// An unattributed otel row (a batch folded before the worker
			// attribute was staged, or staged without one) would pool
			// nameless and read as a literal "otel-" — dropped instead.
			// Attributed otel rows list under their worker like any other.
			// The model fold never yields "" (empty models bucket to
			// modelUnknown).
			continue
		}
		r := byKey[k]
		if r == nil {
			r = &metricsUsageRow{Key: k.key, Src: e.Source}
			byKey[k] = r
		}
		r.Calls++
		r.In += inputTokensTotal(e)
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
		r.hitDenom += inputTokensTotal(e)
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
		if rows[i].Key != rows[j].Key {
			return rows[i].Key < rows[j].Key
		}
		// Same key, same calls: the relay row before its otel twin, so
		// the order is stable across renders despite the map walk.
		return rows[i].Src < rows[j].Src
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
// Task) drop out, and so do otel entries: their stamps are export-batch
// arrivals, not round-end proxies, and would read the loop through the
// ~60s export interval. Known approximation, accepted: the gap includes
// non-provider time (local test runs, tool exec) and misses any
// direct-dial calls.
func foldLoops(entries []metricsrelay.Entry) loopStats {
	dev := map[string]*runLoop{}
	test := map[string]*runLoop{}
	for _, e := range entries {
		if e.Task == "" || e.Source != "" {
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
// task+role+round group), how far the wire-normalized whole input
// (inputTokensTotal) grew from the first call to the last, averaged over
// the groups with at least two calls — a single call has no first→last
// span to measure. Relay entries only: an otel batch carries that
// minute's delta, not the round's live context size, so a multi-batch
// otel round would read last−first strongly negative and invert the
// tile's sign.
func foldDrift(entries []metricsrelay.Entry) (avg int64, groups int) {
	type groupKey struct{ task, role, round string }
	byGroup := map[groupKey]*driftGroup{}
	for _, e := range entries {
		if e.Task == "" || e.Source != "" {
			continue
		}
		k := groupKey{e.Task, e.Role, e.Round}
		g := byGroup[k]
		if g == nil {
			g = &driftGroup{}
			byGroup[k] = g
		}
		in := inputTokensTotal(e)
		g.calls++
		g.last = in
		if g.calls == 1 {
			g.first = in
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

// foldRoundSpan averages the wall-clock length of keyed rounds: per keyed
// (task, round) group — one stage round each, since round numbers are
// per-workflow — the span from the earliest call stamp to the latest,
// min/max so iteration order never matters. Relay face only (same gate
// as drift/loops): an otel batch's stamp is its export arrival, not a
// round-end proxy. Zero-span groups (a single call, or two calls on one
// truncated stamp) drop out. Known approximation, accepted: the tail
// after the last call — the response streaming home, the round's
// wrap-up — is structurally missing from the ledger, so the figure reads
// low by at most one final model call per round.
func foldRoundSpan(entries []metricsrelay.Entry) (avg time.Duration, groups int) {
	type groupKey struct{ task, round string }
	first, last := map[groupKey]time.Time{}, map[groupKey]time.Time{}
	for _, e := range entries {
		if e.Task == "" || e.Source != "" {
			continue
		}
		k := groupKey{e.Task, e.Round}
		if f, ok := first[k]; !ok || e.At.Before(f) {
			first[k] = e.At
		}
		if l, ok := last[k]; !ok || e.At.After(l) {
			last[k] = e.At
		}
	}
	var sum time.Duration
	for k := range first {
		span := last[k].Sub(first[k])
		if span <= 0 {
			continue
		}
		sum += span
		groups++
	}
	if groups > 0 {
		avg = sum / time.Duration(groups)
	}
	return avg, groups
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
// pass, the worker rows projected from the usage fold. Only the relay's
// own observations count — otel entries carry no request-level signal
// (D1: no status, no duration) and would read as errors and zeros here.
func foldRelayTotals(entries []metricsrelay.Entry, now time.Time) relayTotals {
	st := relayTotals{Window: "no relayed calls yet"}
	relay := make([]metricsrelay.Entry, 0, len(entries))
	for _, e := range entries {
		if e.Source == "" {
			relay = append(relay, e)
		}
	}
	entries = relay
	var oldest time.Time
	for _, e := range entries {
		st.Calls++
		if now.Sub(e.At) < time.Hour {
			st.Hour++
		}
		if e.Status < 200 || e.Status >= 300 {
			st.Errors++
		}
		st.In += inputTokensTotal(e)
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
// unseen models is speculative, the ledger is the only source of names. A
// model folded into two era rows pools into one form row: the form edits
// rates, and rates have no era — calls sum, Last keeps the newer era's.
func buildFormRows(modelRows []metricsUsageRow, prices map[string]modelPrice) []uiModelFormRow {
	rows := make([]uiModelFormRow, 0, len(modelRows))
	idx := map[string]int{}
	lasts := make([]time.Time, 0, len(modelRows))
	for _, r := range modelRows {
		if i, ok := idx[r.Key]; ok {
			rows[i].Calls += r.Calls
			if r.last.After(lasts[i]) {
				lasts[i] = r.last
				rows[i].Last = r.Last
			}
			continue
		}
		idx[r.Key] = len(rows)
		lasts = append(lasts, r.last)
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
	}{uiChromeFor("models", "daedalus — models"), v})
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
// save refreshes the snapshot to the reloaded table — the next render
// prices through the new rates, and the golens sync re-evaluates the
// price gate (a newly priced model's cost counter and its Model prices
// panel appear; golensSyncLayout).
func (s *uiServer) saveModels(w http.ResponseWriter, req *http.Request) {
	if err := req.ParseForm(); err != nil {
		http.Error(w, "read form: "+err.Error(), http.StatusBadRequest)
		return
	}
	// Pricable = the ledger-known models minus the unknown bucket — the
	// form's editable rows exactly, recomputed fresh so a retention prune
	// between render and submit cannot price a name the ledger no longer
	// knows.
	pricable := map[string]bool{}
	for _, m := range foldUsage(s.ledgerEntries(), time.Now(), nil, usageModelKey) {
		if m.Key != modelUnknown {
			pricable[m.Key] = true
		}
	}
	if upsert := parseModelPrices(req.Form, pricable); len(upsert) > 0 {
		if err := s.savePrices(upsert); err != nil {
			http.Error(w, "save model prices: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	s.golensSyncLayout()
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
	RoundSpan       string
}

// buildTiles averages the whole window: tokens per keyed run, tokens per
// worker, the two loop averages, the per-round input drift, and the
// per-round wall-clock span. The worker tile reads the relay face on
// both sides — its numerator sums relay entries and its denominator
// counts the worker fold's relay rows only, the otel-attributed rows
// excluded — so a worker's otel batch deltas (minute-deltas, not request
// records) never dilute the per-worker figure. TokensPerRun stays
// all-source: both its numerator and its distinct-task denominator count
// otel rounds once, so the populations agree.
func buildTiles(entries []metricsrelay.Entry, workers []metricsUsageRow) uiDerivedTiles {
	t := uiDerivedTiles{TokensPerRun: "—", TokensPerWorker: "—", DevLoop: "—", TestLoop: "—", Drift: "—", RoundSpan: "—"}
	var total, relayTotal int64
	relayWorkers := 0
	runs := map[string]bool{}
	for _, e := range entries {
		in := inputTokensTotal(e)
		total += in + e.OutputTokens
		if e.Source == "" {
			relayTotal += in + e.OutputTokens
		}
		if e.Task != "" {
			runs[e.Task] = true
		}
	}
	for _, r := range workers {
		if r.Src == "" {
			relayWorkers++
		}
	}
	if n := len(runs); n > 0 {
		t.TokensPerRun = fmtTokens(total / int64(n))
	}
	if n := relayWorkers; n > 0 {
		t.TokensPerWorker = fmtTokens(relayTotal / int64(n))
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
	if span, groups := foldRoundSpan(entries); groups > 0 {
		t.RoundSpan = dur(span)
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
		v.In += inputTokensTotal(e)
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

// roundLedgerCosts folds one run's ledger entries into priced per-round
// figures, keyed by the relay's Round — the scheduled activity id the
// worker sends as its attribution (internal/activities/metricsrelay.go).
// The same entryCost pricing the ledger views use; unpriced models
// contribute nothing, so a round with no priced calls is simply absent.
func roundLedgerCosts(entries []metricsrelay.Entry, prices map[string]modelPrice, task string) map[string]float64 {
	costs := map[string]float64{}
	for _, e := range entries {
		if e.Task != task {
			continue
		}
		if p, ok := prices[e.Model]; ok {
			costs[e.Round] += entryCost(e, p)
		}
	}
	return costs
}

// roundCostCell renders the usage table's Cost cell for one round: the
// round's priced ledger fold, or the dash when the operator has no prices
// saved (nil map) or the round has no priced calls. The CLI's
// self-reported cost is never displayed (dash-hardening Q3 — the calc's
// only source is user-input model prices).
func roundCostCell(costs map[string]float64, activityID string) string {
	if costs == nil {
		return "—"
	}
	if c, ok := costs[activityID]; ok {
		return fmtUSD(c)
	}
	return "—"
}
