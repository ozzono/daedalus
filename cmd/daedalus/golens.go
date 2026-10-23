// The golens-metrics embed: the registry behind /full-metrics and the
// feed that fills it. The daemon builds it at boot (startGolens), mounts
// its dashboard beside the htmx UI (ui.go's routes), and points the relay
// ledger's append observer at it, so every recorded provider request
// lands in the registry as metric samples the dashboard charts. Retention
// rides the module's own sqlite layer — the decided posture, no parallel
// schema: the module's roll-up windows persist in the shared metrics
// database across restarts, and so do the per-request rows themselves
// (the relay ledger's table, internal/metricsrelay's store), served by
// `daedalus dash list` and the run detail.
//
// The dashboard is sectioned: a Global band over the six deployment-wide
// metrics, a per-worker band for the config-known worker (with a nested
// Runs drop target), a Model prices band when the price table has
// entries, and a Key aliases band when the provider section configures a
// key_alias. A golens metric is one series per name — labels are
// last-seen display metadata, not dimensions — so every slice is its own
// child metric name (the module's own RED-middleware pattern,
// http_requests_total_<class>): the bounded config vocabularies register
// their slices up front (golensSyncLayout) and the feed records the
// matching names per append. Runs are the unbounded slice: their metrics
// ride the module's auto-register + idle-eviction flow with the All
// Metrics placement, never a per-run layout pin — pins would accumulate
// a dead "metric unavailable" panel per finished run in the persisted
// document. The per-worker Runs band is the module's empty-band contract:
// hidden in view mode, an edit-mode drop target for the run panels an
// operator wants nested under their worker.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/GoLens-Project/golens-metrics"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
)

// The fed metrics' names and label sets. The global six keep their names
// and label sets; the per-slice child metrics build on the same kinds —
// one metric name per slice value, so a per-slice panel exists (golens
// cannot filter a single metric by label). Run metrics embed the worker
// and task so the All Metrics list clusters a run's panels under its
// worker.
const (
	golensTokensIn   = "daedalus_tokens_in_total"
	golensTokensOut  = "daedalus_tokens_out_total"
	golensCacheRead  = "daedalus_tokens_cache_read_total"
	golensCacheWrite = "daedalus_tokens_cache_write_total"
	golensRequests   = "daedalus_requests_total"
	golensDuration   = "daedalus_request_duration_seconds"
)

// The slice kinds the child metrics are built from, and the layout
// document's daemon-owned section ids (the ownership prefix the sync
// merges on).
const (
	golensKIn       = "tokens_in_total"
	golensKOut      = "tokens_out_total"
	golensKRead     = "tokens_cache_read_total"
	golensKWrite    = "tokens_cache_write_total"
	golensKRequests = "requests_total"
	golensKDuration = "request_duration_seconds"

	golensSectionGlobal  = "daedalus-global"
	golensSectionWorker  = "daedalus-worker-"
	golensSectionPrices  = "daedalus-model-prices"
	golensSectionAliases = "daedalus-key-aliases"
)

var (
	golensTokenLabels   = []string{"worker", "model", "role", "alias"}
	golensRequestLabels = []string{"worker", "model", "role", "status", "alias"}
	// golensDurationBounds spans a provider round's realistic latency in
	// seconds — the library's HTTP-shaped defaults (5ms..10s) would pin
	// every LLM call into the top bucket.
	golensDurationBounds = []float64{0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600}
)

// golensSlices is the token/request counter set both the worker and the
// alias sections lay out; the duration histogram joins the worker side
// only (the alias gate covers the token/request families).
var golensSlices = []struct{ kind, label string }{
	{golensKIn, "input tokens"},
	{golensKOut, "output tokens"},
	{golensKRead, "cache-read tokens"},
	{golensKWrite, "cache-write tokens"},
	{golensKRequests, "requests"},
	{golensKDuration, "request duration"},
}

// workerMetricName / aliasMetricName / modelCostMetricName build the
// child metric names. The alias and cost stems differ from the worker
// shape so a worker and an alias sharing a name can never collide.
func workerMetricName(kind, worker string) string { return "daedalus_" + kind + "_" + worker }
func aliasMetricName(kind, alias string) string {
	return "daedalus_alias_" + kind + "_" + alias
}
func modelCostMetricName(model string) string { return "daedalus_model_cost_total_" + model }

// golensRunStem builds the run metric stem for a worker and task. Both
// fields carry underscores by design (worker ids permit them; task values
// are scrubbed workflow ids or verbatim OTLP attributes), so a naive join
// folds distinct runs into one metric pair (worker "w" + task "1_c" vs
// worker "w_1" + task "c"), and even doubling each fragment's underscores
// cannot fix the boundary (worker "w_" + task "x" vs worker "w" + task
// "_x" merge across the join). When neither field contains an underscore
// the join is its own unique split — the common case, unchanged. Any
// dirty pair rides a "0_"-marked byte-length header whose decimal lengths
// parse back to exactly one fragment split. The branches never meet:
// after the fixed prefix a clean stem carries exactly one underscore, a
// marked one at least five.
func golensRunStem(worker, task string) string {
	if !strings.Contains(worker, "_") && !strings.Contains(task, "_") {
		return "daedalus_run_" + worker + "_" + task + "_"
	}
	return "daedalus_run_0_" + strconv.Itoa(len(worker)) + "_" + strconv.Itoa(len(task)) + "_" + worker + "_" + task + "_"
}

// golensBounds types the duration slices: the histogram bounds for the
// duration kind, nil (plain counter) for everything else.
func golensBounds(kind string) []float64 {
	if kind == golensKDuration {
		return golensDurationBounds
	}
	return nil
}

// startGolens builds and starts the daemon's registry: sqlite retention
// into the shared metrics database (metrics.storage_backend/storage_path —
// empty backend defaults sqlite, empty path defaults to the shared
// metrics.db), the layout document persisted into that same database
// (the module's DashboardConfigDB, Q6 — the file-backed layout store is
// retired), and the six global ledger metrics registered up front so the
// samples land typed — a Record for an unknown name auto-registers a
// plain label-less counter, which is not what these are. The per-slice
// child metrics register in golensSyncLayout, which also pins the
// sectioned layout; startGolens keeps only what every deployment carries
// regardless of config.
func startGolens(cfg config.Config) (*golens.Registry, error) {
	gc := golens.DefaultConfig()
	gc.ProjectName = "daedalus"
	gc.DashboardSubtitle = "run metrics"
	// Unlimited label combinations (0): the fed dimensions are bounded
	// vocabularies — the deployment's workers, roles, statuses, and the
	// models it actually uses — while the default 256 cap would drop
	// every new combination past the cap for good (the tracker never
	// prunes), silently amputating a freshly configured model from the
	// panels.
	gc.MaxLabelSeriesPerMetric = 0
	backend := cfg.Metrics.StorageBackend
	if backend == "" {
		backend = "sqlite"
	}
	gc.Storage.Backend = backend
	if backend == "sqlite" {
		path, err := metricsDBPath(cfg)
		if err != nil {
			return nil, fmt.Errorf("resolve metrics db: %w", err)
		}
		// The DSN, not the bare path: the module splices Storage.Path raw
		// into sql.Open pragma-less, so without this its roll-up writer
		// runs sqlite's zero busy timeout — a flush colliding with a
		// ledger insert fails immediately and the window is dropped
		// silently. StoreDSN is what daedalus's own connections open the
		// file with; the module's connection gets the same patience.
		gc.Storage.Path = metricsrelay.StoreDSN(path)
		// The layout document joins the shared db as a single-row JSON
		// document (the module's sanctioned same-db-as-storage.path
		// shape); DashboardConfig stays unset, so no file is written
		// anywhere. Plain path: the module appends its own pragma suffix
		// here, and a pre-DSN'd string would corrupt it.
		gc.UI.DashboardConfigDB = path
	}
	reg, err := golens.New(gc)
	if err != nil {
		return nil, err
	}
	reg.Register(golensTokensIn, golens.CounterType, "Ledger input tokens", golensTokenLabels, nil, 0, 0)
	reg.Register(golensTokensOut, golens.CounterType, "Ledger output tokens", golensTokenLabels, nil, 0, 0)
	reg.Register(golensCacheRead, golens.CounterType, "Ledger cache-read tokens", golensTokenLabels, nil, 0, 0)
	reg.Register(golensCacheWrite, golens.CounterType, "Ledger cache-write tokens", golensTokenLabels, nil, 0, 0)
	reg.Register(golensRequests, golens.CounterType, "Relayed provider requests", golensRequestLabels, nil, 0, 0)
	reg.Register(golensDuration, golens.HistogramType, "Provider request duration", golensTokenLabels, golensDurationBounds, 0, 0)
	if err := reg.Start(context.Background()); err != nil {
		reg.Close()
		return nil, err
	}
	return reg, nil
}

// inputTokensExCache is the feed's wire normalization: every wire feeds
// the same anthropic-style disjoint input, so summed global panels never
// double-count. The openai wire's prompt_tokens includes the cached
// reads (a subset of input); the anthropic wire's input_tokens already
// excludes both cache sides. A provider reporting cached > prompt would
// poison a cumulative counter with a negative — clamped at zero. The
// ledger entries stay provider-native raw; this is the golens feed's
// figure only (the models screen applies the same rule where its pricing
// math needs it, ui_metrics.go's entryCost).
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

// feedGolensEntry is the ledger's append observer: one recorded provider
// request becomes the six global samples plus the per-slice child
// samples — worker, key alias, run, and priced-model cost. Record never
// blocks (golens ingests through a bounded queue), so the relay's request
// path does not wait on the dashboard. The empty model renders as the
// same unknown bucket the usage tables use: failed calls carry no model
// name.
//
// Slices outside the boot vocabulary (a second deployment's worker
// sharing the host daemon, an alias configured after boot) auto-register
// as label-less counters and surface in All Metrics instead of a section
// — the graceful face of the config-known vocabulary. The one exception
// is the duration histogram: an auto-register is always a plain counter,
// which would fold a latency into a running total, so the duration
// records only for the boot-known worker, whose histogram is registered.
func (s *uiServer) feedGolensEntry(e metricsrelay.Entry) {
	reg := s.golens
	if reg == nil {
		return
	}
	model := e.Model
	if model == "" {
		model = modelUnknown
	}
	in := inputTokensExCache(e)
	base := []golens.Label{
		{Name: "worker", Value: e.Worker},
		{Name: "model", Value: model},
		{Name: "role", Value: e.Role},
		{Name: "alias", Value: e.Alias},
	}
	status := append(base, golens.Label{Name: "status", Value: strconv.Itoa(e.Status)})
	reg.Record(golensTokensIn, float64(in), base...)
	reg.Record(golensTokensOut, float64(e.OutputTokens), base...)
	reg.Record(golensCacheRead, float64(e.CacheReadTokens), base...)
	reg.Record(golensCacheWrite, float64(e.CacheWriteTokens), base...)
	reg.Record(golensRequests, 1, status...)
	reg.Record(golensDuration, float64(e.DurationMS)/1000, base...)
	if e.Worker != "" {
		reg.Record(workerMetricName(golensKIn, e.Worker), float64(in))
		reg.Record(workerMetricName(golensKOut, e.Worker), float64(e.OutputTokens))
		reg.Record(workerMetricName(golensKRead, e.Worker), float64(e.CacheReadTokens))
		reg.Record(workerMetricName(golensKWrite, e.Worker), float64(e.CacheWriteTokens))
		reg.Record(workerMetricName(golensKRequests, e.Worker), 1)
		if e.Worker == s.cfg.WorkerName() {
			reg.Record(workerMetricName(golensKDuration, e.Worker), float64(e.DurationMS)/1000)
		}
	}
	if e.Alias != "" {
		reg.Record(aliasMetricName(golensKIn, e.Alias), float64(in))
		reg.Record(aliasMetricName(golensKOut, e.Alias), float64(e.OutputTokens))
		reg.Record(aliasMetricName(golensKRead, e.Alias), float64(e.CacheReadTokens))
		reg.Record(aliasMetricName(golensKWrite, e.Alias), float64(e.CacheWriteTokens))
		reg.Record(aliasMetricName(golensKRequests, e.Alias), 1)
	}
	if e.Task != "" && e.Worker != "" {
		stem := golensRunStem(e.Worker, e.Task)
		reg.Record(stem+golensKIn, float64(in))
		reg.Record(stem+golensKOut, float64(e.OutputTokens))
	}
	// The price gate: a model with a price-table entry stamps its cost
	// counter per request (record-time stamped — the charts keep their
	// per-request prices, the models screen keeps render-time repricing);
	// a model without one records nothing, never a zero-cost line.
	if e.Model != "" {
		if p, ok := s.currentPrices()[e.Model]; ok {
			reg.Record(modelCostMetricName(e.Model), entryCost(e, p))
		}
	}
}

// golensOwnedPanel / golensOwnedSection mark the daemon-owned parts of
// the layout document — panels named for our boot-registered metrics and
// the daedalus- prefixed sections. The sync regenerates exactly those and
// passes everything else (the operator's own pins and bands) through
// untouched. Run panels are the exception on both sides: their metrics
// are the feed's, but their placement is the operator's — a pin into the
// worker's Runs band — so daedalus_run_ ids are never regenerated.
func golensOwnedPanel(id string) bool {
	return strings.HasPrefix(id, "daedalus_") && !strings.HasPrefix(id, "daedalus_run_")
}
func golensOwnedSection(id string) bool { return strings.HasPrefix(id, "daedalus-") }

// golensConfigDoc drives the registry's own /metrics/config handler
// in-process: the layout document's only writer is the module's PUT
// (validate, persist, swap memory atomically), so the sync routes through
// it rather than touching the file the registry owns.
func golensConfigDoc(reg *golens.Registry, method string, body []byte) (*golens.DashboardConfig, error) {
	h := reg.DashboardConfigHTTPHandler()
	if h == nil {
		return nil, fmt.Errorf("dashboard UI disabled")
	}
	req := httptest.NewRequest(method, "/metrics/config", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", http.StatusText(rec.Code), strings.TrimSpace(rec.Body.String()))
	}
	if method == http.MethodPut {
		return nil, nil
	}
	var d golens.DashboardConfig
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// golensBaseBands is the module's built-in layout, appended when a
// document reaches the sync carrying none of it (a pre-sections save):
// All Metrics must stay reachable — it is where the run panels surface.
// Ids and names mirror the module's defaultDashboardSections.
func golensBaseBands() []golens.DashboardSection {
	return []golens.DashboardSection{
		{ID: golens.BlockRuntimeHealth, Name: "Runtime Health", Items: []string{golens.BlockRuntimeHealth}},
		{ID: golens.BlockHistogram, Name: "Histogram Time-Series", Items: []string{golens.BlockHistogram}},
		{ID: golens.BlockHooks, Name: "Hooks", Items: []string{golens.BlockHooks}},
		{ID: "my-charts", Name: "My Charts"},
		{ID: golens.BlockAllMetrics, Name: "All Metrics", Items: []string{golens.BlockAllMetrics}},
		{ID: golens.BlockEndpoints, Name: "Endpoint Latency", Items: []string{golens.BlockEndpoints}},
		{ID: golens.BlockInternals, Name: "Internals", Items: []string{golens.BlockInternals}},
	}
}

// golensMergeLayout merges the wanted daemon-owned layout into the
// current document: wanted panels and bands first, every other panel and
// band preserved behind them. It reports whether the document is missing
// wanted parts or carries stale daemon-owned ones — the only conditions
// that warrant a PUT, so an operator's untouched document is never
// rewritten at boot. The second return is the merged document.
func golensMergeLayout(cur *golens.DashboardConfig, wantPanels []golens.DashboardPanel, wantSections []golens.DashboardSection) (*golens.DashboardConfig, bool) {
	dirty := false
	curPanel := make(map[string]bool, len(cur.Panels))
	for _, p := range cur.Panels {
		curPanel[p.ID] = true
	}
	merged := &golens.DashboardConfig{
		Version:      cur.Version,
		Order:        cur.Order,
		HTTPCardView: cur.HTTPCardView,
		Panels:       wantPanels,
	}
	wantPanel := make(map[string]bool, len(wantPanels))
	for _, p := range wantPanels {
		wantPanel[p.ID] = true
		if !curPanel[p.ID] {
			dirty = true
		}
	}
	for _, p := range cur.Panels {
		if golensOwnedPanel(p.ID) {
			// A slice that left the vocabulary leaves its panel behind.
			if !wantPanel[p.ID] {
				dirty = true
			}
			continue
		}
		merged.Panels = append(merged.Panels, p)
	}
	curSec := make(map[string]golens.DashboardSection, len(cur.Sections))
	for _, sec := range cur.Sections {
		curSec[sec.ID] = sec
	}
	wantSec := make(map[string]bool, len(wantSections))
	// Block placement is daemon-owned like the panels: each block-carrying
	// base band owns the one block it is named for. A copy dragged into any
	// other band is stripped, and a base band left holding no block is
	// re-seated — zero references would let the PUT succeed while silently
	// dropping the block (All Metrics is the run panels' home). Daemon
	// panels come home from base bands too; my-charts, the operator's
	// itemless pin home, has no block and stays a plain operator band.
	baseBlocks := make(map[string]bool)
	baseBandIDs := make(map[string]bool)
	for _, b := range golensBaseBands() {
		if len(b.Items) == 0 {
			continue
		}
		baseBandIDs[b.ID] = true
		for _, item := range b.Items {
			baseBlocks[item] = true
		}
	}
	for i := range wantSections {
		want := wantSections[i]
		wantSec[want.ID] = true
		got, ok := curSec[want.ID]
		// The Runs band (the one want section with a Parent) is the one
		// daemon-owned band whose items are the operator's placements: a
		// filled band keeps them through the merge, only its shape (name,
		// nesting) is canonical. Daemon panels and stray blocks dragged in
		// come back under their canonical bands — the duplicate reference
		// would fail the PUT validator (in two places).
		if ok && want.Parent != "" && len(got.Items) > 0 {
			items := make([]string, 0, len(got.Items))
			for _, item := range got.Items {
				if golensOwnedPanel(item) || baseBlocks[item] {
					dirty = true
					continue
				}
				items = append(items, item)
			}
			wantSections[i].Items = items
		}
		if !ok {
			dirty = true
			continue
		}
		if want.Parent != "" {
			if got.Name != want.Name || got.Parent != want.Parent {
				dirty = true
			}
			continue
		}
		if got.Name != want.Name || got.Parent != want.Parent || !slices.Equal(got.Items, want.Items) {
			dirty = true
		}
	}
	kept := make([]golens.DashboardSection, 0, len(cur.Sections))
	for _, sec := range cur.Sections {
		if golensOwnedSection(sec.ID) {
			if !wantSec[sec.ID] {
				dirty = true
			}
			continue
		}
		// Daemon panels come home from any band; a block comes home from
		// every band but the base band named for it — its self-reference
		// is the one legitimate placement.
		items := make([]string, 0, len(sec.Items))
		for _, item := range sec.Items {
			if golensOwnedPanel(item) || (baseBlocks[item] && item != sec.ID) {
				// the duplicate would fail the PUT validator (in two places)
				dirty = true
				continue
			}
			items = append(items, item)
		}
		if baseBandIDs[sec.ID] && !slices.Contains(items, sec.ID) {
			// the operator moved the band's own block away — seat it back
			items = append(items, sec.ID)
			dirty = true
		}
		if len(items) != len(sec.Items) {
			sec.Items = items
		}
		kept = append(kept, sec)
	}
	hasBase := false
	for _, sec := range kept {
		if !golensOwnedSection(sec.ID) {
			hasBase = true
			break
		}
	}
	if !hasBase {
		kept = append(kept, golensBaseBands()...)
		dirty = true
	}
	merged.Sections = append(wantSections, kept...)
	// A preserved band parented under a daemon band this sync dropped
	// would dangle; the validator rejects dangling parents, so promote it
	// to top level (the module's own heal rule).
	ids := make(map[string]bool, len(merged.Sections))
	for _, sec := range merged.Sections {
		ids[sec.ID] = true
	}
	for i := range merged.Sections {
		if p := merged.Sections[i].Parent; p != "" && !ids[p] {
			merged.Sections[i].Parent = ""
			dirty = true
		}
	}
	return merged, dirty
}

// golensSyncLayout registers the bounded vocabularies' child metrics —
// the config-known worker, the provider section's key_alias, the price
// table's models — and makes the persisted layout document carry the
// sectioned dashboard: Global, the per-worker band with its nested Runs
// drop target, Model prices when prices exist, Key aliases when an alias
// is configured, then everything the document already holds. The config
// PUT path (not a preset) is what places the panels: presets run before
// panels exist and may only place well-known blocks, while these panel
// ids are registered before the PUT runs. Called at boot and after every
// price-table save — the two points the price gate is ruled to be
// evaluated. A failure logs and stands until the next call: the
// dashboard renders unsectioned, never broken.
func (s *uiServer) golensSyncLayout() {
	reg := s.golens
	if reg == nil {
		return
	}
	register := func(name, desc string, bounds []float64) {
		if bounds != nil {
			reg.Register(name, golens.HistogramType, desc, nil, bounds, 0, 0)
			return
		}
		reg.Register(name, golens.CounterType, desc, nil, nil, 0, 0)
	}
	panels := make([]golens.DashboardPanel, 0, 16)
	globalItems := []string{golensTokensIn, golensTokensOut, golensCacheRead, golensCacheWrite, golensRequests, golensDuration}
	for _, name := range globalItems {
		panels = append(panels, golens.DashboardPanel{ID: name, Metric: name})
	}
	sections := []golens.DashboardSection{{ID: golensSectionGlobal, Name: "Global", Items: globalItems}}
	if worker := s.cfg.WorkerName(); worker != "" {
		items := make([]string, 0, len(golensSlices))
		for _, sl := range golensSlices {
			name := workerMetricName(sl.kind, worker)
			register(name, sl.label+" — worker "+worker, golensBounds(sl.kind))
			items = append(items, name)
			panels = append(panels, golens.DashboardPanel{ID: name, Metric: name})
		}
		sections = append(sections,
			golens.DashboardSection{ID: golensSectionWorker + worker, Name: "Worker " + worker, Items: items},
			// The Runs band: empty by design — view mode hides it, edit
			// mode reveals it as the drop target for the per-run panels
			// the feed auto-registers into All Metrics.
			golens.DashboardSection{ID: golensSectionWorker + worker + "-runs", Name: "Runs", Parent: golensSectionWorker + worker},
		)
	}
	if s.cfg.Provider != nil && s.cfg.Provider.KeyAlias != "" {
		alias := s.cfg.Provider.KeyAlias
		items := make([]string, 0, len(golensSlices)-1)
		for _, sl := range golensSlices[:len(golensSlices)-1] {
			name := aliasMetricName(sl.kind, alias)
			register(name, sl.label+" — key alias "+alias, nil)
			items = append(items, name)
			panels = append(panels, golens.DashboardPanel{ID: name, Metric: name})
		}
		sections = append(sections, golens.DashboardSection{ID: golensSectionAliases, Name: "Key aliases", Items: items})
	}
	prices := s.currentPrices()
	models := make([]string, 0, len(prices))
	for m := range prices {
		if m == "" {
			continue // hand-mangled table; nothing costless to place
		}
		models = append(models, m)
	}
	sort.Strings(models)
	if len(models) > 0 {
		items := make([]string, 0, len(models))
		for _, m := range models {
			name := modelCostMetricName(m)
			register(name, "cost (USD, stamped per request) — model "+m, nil)
			items = append(items, name)
			panels = append(panels, golens.DashboardPanel{ID: name, Metric: name})
		}
		sections = append(sections, golens.DashboardSection{ID: golensSectionPrices, Name: "Model prices", Items: items})
	}
	cur, err := golensConfigDoc(reg, http.MethodGet, nil)
	if err != nil {
		fmt.Printf("metrics: read dashboard layout: %v\n", err)
		return
	}
	merged, dirty := golensMergeLayout(cur, panels, sections)
	if !dirty {
		return
	}
	data, err := json.Marshal(merged)
	if err != nil { // unreachable: DashboardConfig is all marshalable
		return
	}
	if _, err := golensConfigDoc(reg, http.MethodPut, data); err != nil {
		fmt.Printf("metrics: persist dashboard layout: %v\n", err)
	}
}
