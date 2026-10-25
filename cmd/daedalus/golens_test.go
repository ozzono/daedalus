package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GoLens-Project/golens-metrics"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
)

// golensMemoryConfig is startGolens's input with the in-memory backend —
// the feed and route tests exercise the registry without sqlite files.
func golensMemoryConfig() config.Config {
	cfg := config.Config{}
	cfg.Metrics.StorageBackend = "memory"
	return cfg
}

// awaitCounter polls the registry until the named counter has folded exactly
// want into its value — Record enqueues non-blockingly and the registry's own
// goroutine folds the samples, so a cumulative value read too early would
// race the fold and read short.
func awaitCounter(t *testing.T, reg *golens.Registry, name string, want float64) golens.MetricSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, ok := reg.Snapshot(name)
		if ok && snap.Value == want {
			return snap
		}
		if time.Now().After(deadline) {
			t.Fatalf("counter %s converged to (%v, ok %v), want %v", name, snap.Value, ok, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// awaitHistogram polls until the named histogram has folded exactly the want
// samples summing to wantSum — same asynchronous fold as awaitCounter, whose
// value check a histogram's Count/Sum pair needs instead.
func awaitHistogram(t *testing.T, reg *golens.Registry, name string, wantCount int64, wantSum float64) golens.MetricSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, ok := reg.Snapshot(name)
		if ok && snap.Count == wantCount && snap.Sum == wantSum {
			return snap
		}
		if time.Now().After(deadline) {
			t.Fatalf("histogram %s converged to (count %d, sum %v, ok %v), want count %d sum %v",
				name, snap.Count, snap.Sum, ok, wantCount, wantSum)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// awaitMetricAbsent proves a metric the feed must never create stays absent.
// Callers converge a metric the same feed call records later first, so the
// record-or-skip decision has already passed — this only guards queue lag.
func awaitMetricAbsent(t *testing.T, reg *golens.Registry, name string) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := reg.Snapshot(name); ok {
			t.Fatalf("metric %s exists, want it never recorded", name)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// golensLabelMap flattens a snapshot's last-seen label values by name.
func golensLabelMap(values []golens.Label) map[string]string {
	m := map[string]string{}
	for _, l := range values {
		m[l.Name] = l.Value
	}
	return m
}

// TestStartGolensBootFaces pins the registry's boot: the default config
// opens the shared metrics db in the config home (creating it a fresh host
// lacks) and registers the six ledger metrics up front so fed samples land
// typed — and writes neither of the two retired daemon-dir files; an
// explicit storage path relocates the shared db; the memory backend keeps
// no file at all; and the two boot failures name their cause — an
// unresolvable config home and an unopenable sqlite path.
func TestStartGolensBootFaces(t *testing.T) {
	sixMetrics := []string{
		golensTokensIn, golensTokensOut, golensCacheRead, golensCacheWrite,
		golensRequests, golensDuration,
	}

	t.Run("defaults create the shared db in the config home", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		dir := useDaemonDir(t)
		reg, err := startGolens(config.Config{})
		if err != nil {
			t.Fatalf("startGolens: %v", err)
		}
		defer reg.Close()
		if _, err := os.Stat(filepath.Join(home, ".config", "daedalus", "metrics.db")); err != nil {
			t.Errorf("the shared metrics db is missing after boot: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, "metrics-golens.db")); !os.IsNotExist(err) {
			t.Errorf("the retired daemon-dir store was created: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, "metrics-golens-layout.json")); !os.IsNotExist(err) {
			t.Errorf("the retired daemon-dir layout file was written: %v", err)
		}
		for _, name := range sixMetrics {
			if _, ok := reg.Snapshot(name); !ok {
				t.Errorf("metric %s is not registered at boot — a Record would auto-register it label-less", name)
			}
		}
	})

	t.Run("an explicit storage path relocates the shared db", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		useDaemonDir(t)
		custom := filepath.Join(t.TempDir(), "relocated", "metrics.db")
		if err := os.MkdirAll(filepath.Dir(custom), 0o755); err != nil {
			t.Fatal(err)
		}
		cfg := config.Config{}
		cfg.Metrics.StorageBackend = "sqlite"
		cfg.Metrics.StoragePath = custom
		reg, err := startGolens(cfg)
		if err != nil {
			t.Fatalf("startGolens: %v", err)
		}
		defer reg.Close()
		if _, err := os.Stat(custom); err != nil {
			t.Errorf("the explicit store is missing after boot: %v", err)
		}
		if _, err := os.Stat(filepath.Join(home, ".config", "daedalus", "metrics.db")); !os.IsNotExist(err) {
			t.Errorf("the default db was created beside the explicit one: %v", err)
		}
	})

	t.Run("the memory backend keeps no file", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		dir := useDaemonDir(t)
		reg, err := startGolens(golensMemoryConfig())
		if err != nil {
			t.Fatalf("startGolens: %v", err)
		}
		defer reg.Close()
		if _, err := os.Stat(filepath.Join(dir, "metrics-golens.db")); !os.IsNotExist(err) {
			t.Errorf("the memory backend wrote a daemon-dir store file: %v", err)
		}
		if _, err := os.Stat(filepath.Join(home, ".config", "daedalus", "metrics.db")); !os.IsNotExist(err) {
			t.Errorf("the memory backend wrote the shared metrics db: %v", err)
		}
	})

	t.Run("an unresolvable config home fails the boot", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		useDaemonDir(t)
		if err := os.MkdirAll(filepath.Join(home, ".config"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(home, ".config", "daedalus"), []byte("not a dir"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := startGolens(config.Config{}); err == nil || !strings.Contains(err.Error(), "resolve metrics db") {
			t.Errorf("startGolens over a blocked config home = %v, want the resolve diagnostic", err)
		}
	})

	t.Run("an unopenable sqlite path fails the boot", func(t *testing.T) {
		useDaemonDir(t)
		cfg := config.Config{}
		cfg.Metrics.StorageBackend = "sqlite"
		cfg.Metrics.StoragePath = filepath.Join(t.TempDir(), "no", "such", "dir", "golens.db")
		reg, err := startGolens(cfg)
		if err == nil {
			reg.Close()
			t.Fatal("startGolens with an unopenable sqlite path = nil error, want the sqlite diagnostic")
		}
		if !strings.Contains(err.Error(), "sqlite") {
			t.Errorf("startGolens error = %v, want it to name the sqlite backend", err)
		}
	})
}

// TestFeedGolensEntryFoldsTheSixMetrics pins the ledger→registry feed: one
// recorded request folds into the four token counters, the request counter
// (which adds the HTTP status label), and the seconds-denominated duration
// histogram; the bounded dimensions ride the declared label sets; and a
// failed call carrying no model lands in the same unknown bucket the usage
// tables use.
func TestFeedGolensEntryFoldsTheSixMetrics(t *testing.T) {
	useDaemonDir(t)
	reg, err := startGolens(golensMemoryConfig())
	if err != nil {
		t.Fatalf("startGolens: %v", err)
	}
	defer reg.Close()
	srv := newUIServer(config.Config{}, &metricsrelay.Ledger{}, reg, nil)

	srv.feedGolensEntry(metricsrelay.Entry{
		Worker: "w-a", Wire: metricsrelay.WireAnthropic, Model: "mA", Role: "dev", Alias: "k1",
		InputTokens: 100, OutputTokens: 200, CacheReadTokens: 30, CacheWriteTokens: 40,
		DurationMS: 1500, Status: 200,
	})
	// A failed call carrying no model: the unknown bucket, its own status.
	srv.feedGolensEntry(metricsrelay.Entry{
		Worker: "w-b", Status: 401, InputTokens: 5, OutputTokens: 6, DurationMS: 500,
	})

	for name, want := range map[string]float64{
		golensTokensIn:   175, // the whole input side: 100 + 30 + 40 anthropic, 5 default
		golensTokensOut:  206,
		golensCacheRead:  30,
		golensCacheWrite: 40,
		golensRequests:   2,
	} {
		awaitCounter(t, reg, name, want)
	}

	dur := awaitHistogram(t, reg, golensDuration, 2, 2.0)
	if dur.Type != "histogram" {
		t.Errorf("duration metric type = %q, want histogram", dur.Type)
	}

	// The declared label sets: tokens carry the bounded dimensions, the
	// request counter adds the HTTP status. The duration histogram was the
	// feed's last Record, so its convergence above settles these too.
	in, ok := reg.Snapshot(golensTokensIn)
	if !ok {
		t.Fatalf("metric %s vanished", golensTokensIn)
	}
	if strings.Join(in.Labels, ",") != strings.Join(golensTokenLabels, ",") {
		t.Errorf("token labels = %v, want %v", in.Labels, golensTokenLabels)
	}
	reqs, ok := reg.Snapshot(golensRequests)
	if !ok {
		t.Fatalf("metric %s vanished", golensRequests)
	}
	if strings.Join(reqs.Labels, ",") != strings.Join(golensRequestLabels, ",") {
		t.Errorf("request labels = %v, want %v", reqs.Labels, golensRequestLabels)
	}

	// Last-seen values ride the newest entry: the failed call's unknown
	// model and its 401 status.
	tokenLabels := golensLabelMap(in.LabelValues)
	if tokenLabels["worker"] != "w-b" || tokenLabels["model"] != modelUnknown || tokenLabels["role"] != "" || tokenLabels["alias"] != "" {
		t.Errorf("token last-seen labels = %v, want the failed call's worker/model/role/alias", tokenLabels)
	}
	requestLabels := golensLabelMap(reqs.LabelValues)
	if requestLabels["status"] != "401" || requestLabels["model"] != modelUnknown {
		t.Errorf("request last-seen labels = %v, want the failed call's status and unknown model", requestLabels)
	}
}

// TestUIRoutesServeTheGolensMountBesideTheExactRoutes pins the route map:
// the daemon's method-scoped /metrics and /metrics/body keep their paths
// (the aggregates page, not the mount's dashboard), the trailing slash
// renders the same page through /metrics/{$}, and the mount owns the rest
// of the namespace — /metrics/data its JSON, /full-metrics the dashboard's
// entry link. Without the embed none of the mount's routes exist and the
// old /metrics/ catch-all is gone with it.
func TestUIRoutesServeTheGolensMountBesideTheExactRoutes(t *testing.T) {
	useDaemonDir(t)
	reg, err := startGolens(golensMemoryConfig())
	if err != nil {
		t.Fatalf("startGolens: %v", err)
	}
	defer reg.Close()
	led := &metricsrelay.Ledger{}
	srv := httptest.NewServer(newUIServer(config.Config{}, led, reg, nil).routes())
	defer srv.Close()

	get := func(t *testing.T, path string) (int, string) {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read GET %s: %v", path, err)
		}
		return resp.StatusCode, string(body)
	}

	// The daemon's exact routes win their paths.
	for _, path := range []string{"/metrics", "/metrics/"} {
		code, body := get(t, path)
		if code != http.StatusOK {
			t.Fatalf("GET %s = %d, want the aggregates page", path, code)
		}
		if !strings.Contains(body, `<h1 class="text-lg font-semibold mb-4">Metrics</h1>`) {
			t.Errorf("GET %s did not render the daemon's aggregates page:\n%s", path, body)
		}
		if strings.Contains(body, `class="brand-title"`) {
			t.Errorf("GET %s rendered the golens dashboard over the aggregates page:\n%s", path, body)
		}
	}

	// The mount serves the rest of the namespace; the named-metric face of
	// its data endpoint answers JSON.
	for _, name := range []string{golensTokensIn, golensTokensOut, golensCacheRead, golensCacheWrite, golensRequests, golensDuration} {
		code, body := get(t, "/metrics/data?metric="+name)
		if code != http.StatusOK || !strings.Contains(body, name) {
			t.Errorf("GET /metrics/data?metric=%s = %d, want the metric's JSON:\n%s", name, code, body)
		}
	}

	code, body := get(t, "/full-metrics")
	if code != http.StatusOK {
		t.Fatalf("GET /full-metrics = %d, want the dashboard", code)
	}
	for _, want := range []string{
		`<h1 class="brand-title">daedalus</h1>`,      // the project name startGolens sets
		`<span class="brand-sub">run metrics</span>`, // its subtitle
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the dashboard lacks %q:\n%s", want, body)
		}
	}

	// Without the embed the mount's routes vanish and the aggregates page
	// keeps its path — the prefix catch-all that once served /metrics/...
	// stays gone.
	bare := httptest.NewServer(newUIServer(config.Config{}, led, nil, nil).routes())
	defer bare.Close()
	bareGet := func(path string) int {
		resp, err := http.Get(bare.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := bareGet("/full-metrics"); code != http.StatusNotFound {
		t.Errorf("embed-less /full-metrics = %d, want 404 (no mount to serve it)", code)
	}
	if code := bareGet("/metrics/ghost"); code != http.StatusNotFound {
		t.Errorf("embed-less /metrics/ghost = %d, want 404 (the catch-all is gone)", code)
	}
	if code := bareGet("/metrics"); code != http.StatusOK {
		t.Errorf("embed-less /metrics = %d, want the aggregates page", code)
	}
}

// TestInputTokensExCacheNormalizesTheWire pins the pricing-grade input
// figure (entryCost's): the anthropic wire reports input and cache reads
// disjoint, the openai wire's prompt_tokens includes the cached reads —
// subtracted so no token class bills twice — and a provider reporting more
// cached than prompt clamps at zero, never a negative on a bill.
func TestInputTokensExCacheNormalizesTheWire(t *testing.T) {
	e := metricsrelay.Entry{InputTokens: 1000, CacheReadTokens: 300}
	if got := inputTokensExCache(e); got != 1000 {
		t.Errorf("anthropic input = %d, want the raw 1000 (input and reads are disjoint)", got)
	}
	e.Wire = metricsrelay.WireOpenAI
	if got := inputTokensExCache(e); got != 700 {
		t.Errorf("openai input = %d, want 700 (the prompt includes the reads)", got)
	}
	e.CacheReadTokens = 1200
	if got := inputTokensExCache(e); got != 0 {
		t.Errorf("over-reported cache = %d, want the 0 clamp", got)
	}
}

// TestFeedGolensEntryWithoutARegistryIsASilentNoop pins the dashboard-less
// face: the htmx-only harnesses and embed-less daedalus runs carry a nil
// registry, and neither the feed nor the sync may notice.
func TestFeedGolensEntryWithoutARegistryIsASilentNoop(t *testing.T) {
	srv := newUIServer(config.Config{}, &metricsrelay.Ledger{}, nil, nil)
	srv.feedGolensEntry(metricsrelay.Entry{Worker: "w", Model: "m", Status: 200})
	srv.golensSyncLayout()
}

// TestFeedGolensEntrySlicesTheChildMetrics pins the feed's per-slice faces,
// all fed from one recorded request: the config-known worker's five counters
// and duration histogram, the key alias's counters, the run's two, and the
// priced model's cost stamp; the input samples carrying the whole input
// side (inputTokensTotal — the openai wire's reads ride inside its prompt,
// so nothing is subtracted here); a foreign worker auto-registering its
// counters but never a duration (an auto-register is a plain counter, which
// would fold a latency into a running total); and an unpriced model
// recording no cost line.
func TestFeedGolensEntrySlicesTheChildMetrics(t *testing.T) {
	useDaemonDir(t)
	reg, err := startGolens(golensMemoryConfig())
	if err != nil {
		t.Fatalf("startGolens: %v", err)
	}
	defer reg.Close()
	cfg := config.Config{WorkerID: "w-a"}
	cfg.Provider = &config.ProviderConfig{KeyAlias: "k1"}
	srv := newUIServer(cfg, &metricsrelay.Ledger{}, reg, nil)
	srv.swapPrices(map[string]modelPrice{"mA": {In: 3, Out: 2, Cached: 1}})
	srv.golensSyncLayout()

	srv.feedGolensEntry(metricsrelay.Entry{
		Worker: "w-a", Wire: metricsrelay.WireOpenAI, Model: "mA", Role: "dev", Alias: "k1", Task: "t1",
		InputTokens: 1000, OutputTokens: 200, CacheReadTokens: 300, CacheWriteTokens: 40,
		DurationMS: 1500, Status: 200,
	})
	srv.feedGolensEntry(metricsrelay.Entry{
		Worker: "w-x", Wire: metricsrelay.WireAnthropic, Model: "mB",
		InputTokens: 10, OutputTokens: 20, DurationMS: 500, Status: 200,
	})

	// The globals fold both entries, the input whole on every wire: the
	// openai prompt carries its reads inside, the anthropic side names no
	// cache classes here.
	awaitCounter(t, reg, golensTokensIn, 1010)
	awaitCounter(t, reg, golensTokensOut, 220)

	awaitCounter(t, reg, workerMetricName(golensKIn, "w-a"), 1000)
	awaitCounter(t, reg, workerMetricName(golensKOut, "w-a"), 200)
	awaitCounter(t, reg, workerMetricName(golensKRead, "w-a"), 300)
	awaitCounter(t, reg, workerMetricName(golensKWrite, "w-a"), 40)
	awaitCounter(t, reg, workerMetricName(golensKRequests, "w-a"), 1)
	dur := awaitHistogram(t, reg, workerMetricName(golensKDuration, "w-a"), 1, 1.5)
	if dur.Type != "histogram" {
		t.Errorf("the worker duration metric = %q, want histogram", dur.Type)
	}

	awaitCounter(t, reg, aliasMetricName(golensKIn, "k1"), 1000)
	awaitCounter(t, reg, aliasMetricName(golensKRequests, "k1"), 1)

	awaitCounter(t, reg, "daedalus_run_w-a_t1_input_tokens_total", 1000)
	awaitCounter(t, reg, "daedalus_run_w-a_t1_tokens_out_total", 200)

	// The cost stamp keeps the pricing-grade figure: the openai input net
	// of its reads (1000 − 300 + the 40 writes).
	wantCost := float64(740)/1e6*3 + float64(200)/1e6*2 + float64(300)/1e6*1
	awaitCounter(t, reg, modelCostMetricName("mA"), wantCost)

	// The foreign worker's feed call is over once its request counter — its
	// last Record — folded, so the two absences are decisions, not races.
	awaitCounter(t, reg, workerMetricName(golensKIn, "w-x"), 10)
	awaitCounter(t, reg, workerMetricName(golensKRequests, "w-x"), 1)
	awaitMetricAbsent(t, reg, workerMetricName(golensKDuration, "w-x"))
	awaitMetricAbsent(t, reg, modelCostMetricName("mB"))
}

// TestGolensOwnedIds pins the ownership prefixes the sync merges on: panels
// named for our boot-registered metrics are the daemon's, the run panels
// never are (their placement is the operator's), and the daedalus- section
// prefix does not swallow a bare daedalus id.
func TestGolensOwnedIds(t *testing.T) {
	for id, want := range map[string]bool{
		golensTokensIn:                           true,
		workerMetricName(golensKRequests, "w-a"): true,
		modelCostMetricName("mA"):                true,
		"daedalus_run_w-a_t1_tokens_in_total":    false, // the operator's placement
		"op-panel":                               false,
		golensSectionGlobal:                      false, // a section id, not a panel
	} {
		if got := golensOwnedPanel(id); got != want {
			t.Errorf("golensOwnedPanel(%q) = %v, want %v", id, got, want)
		}
	}
	for id, want := range map[string]bool{
		golensSectionGlobal:                   true,
		golensSectionWorker + "w-a":           true,
		golensSectionWorker + "w-a" + "-runs": true,
		golensSectionPrices:                   true,
		golensSectionAliases:                  true,
		"my-charts":                           false,
		"daedalus":                            false, // the prefix is "daedalus-"
	} {
		if got := golensOwnedSection(id); got != want {
			t.Errorf("golensOwnedSection(%q) = %v, want %v", id, got, want)
		}
	}
}

// sectionByID finds one band of a layout document by id.
func sectionByID(sections []golens.DashboardSection, id string) (golens.DashboardSection, bool) {
	for _, sec := range sections {
		if sec.ID == id {
			return sec, true
		}
	}
	return golens.DashboardSection{}, false
}

// panelByID finds one pinned panel of a layout document by id.
func panelByID(panels []golens.DashboardPanel, id string) (golens.DashboardPanel, bool) {
	for _, p := range panels {
		if p.ID == id {
			return p, true
		}
	}
	return golens.DashboardPanel{}, false
}

// sectionRefsOnce fails when any item id is referenced by two bands — the
// module's PUT validator refuses a panel in two places, so a merge output
// carrying a duplicate would fail the very persist it feeds.
func sectionRefsOnce(t *testing.T, sections []golens.DashboardSection) {
	t.Helper()
	seen := map[string]string{}
	for _, sec := range sections {
		for _, item := range sec.Items {
			if prev, dup := seen[item]; dup {
				t.Errorf("item %q sits in both %q and %q", item, prev, sec.ID)
			}
			seen[item] = sec.ID
		}
	}
}

// TestGolensMergeLayoutMergeRules pins the layout merge at the document
// level: a fresh document takes the wanted layout over the module's base
// bands, a settled document reports clean so a restart never rewrites it,
// the operator's own panels and bands pass through behind the wanted ones,
// stale daemon-owned parts are dropped, a run panel keeps its operator
// placement, a daemon panel dragged into an operator band comes home, and a
// band parented under a dropped daemon band is promoted — every output
// keeping each panel in exactly one place, or the module's PUT validator
// would refuse the document the sync feeds it.
func TestGolensMergeLayoutMergeRules(t *testing.T) {
	// The want set mirrors the sync's shape for one worker: two panels, the
	// Global band, the worker band, its nested Runs drop target.
	wantPanels := []golens.DashboardPanel{
		{ID: golensTokensIn, Metric: golensTokensIn},
		{ID: workerMetricName(golensKRequests, "w-a"), Metric: workerMetricName(golensKRequests, "w-a")},
	}
	wantSections := func() []golens.DashboardSection {
		return []golens.DashboardSection{
			{ID: golensSectionGlobal, Name: "Global", Items: []string{golensTokensIn}},
			{ID: golensSectionWorker + "w-a", Name: "Worker w-a", Items: []string{workerMetricName(golensKRequests, "w-a")}},
			{ID: golensSectionWorker + "w-a" + "-runs", Name: "Runs", Parent: golensSectionWorker + "w-a"},
		}
	}

	t.Run("a fresh document takes the wanted layout over the base bands", func(t *testing.T) {
		merged, dirty := golensMergeLayout(&golens.DashboardConfig{}, wantPanels, wantSections())
		if !dirty {
			t.Fatal("an empty document reported clean — the first boot would pin nothing")
		}
		if len(merged.Panels) != len(wantPanels) {
			t.Errorf("merged panels = %d, want the %d wanted ones", len(merged.Panels), len(wantPanels))
		}
		if n := len(merged.Sections); n != len(wantSections())+len(golensBaseBands()) {
			t.Fatalf("merged sections = %d, want the wanted bands over the %d base bands", n, len(golensBaseBands()))
		}
		if merged.Sections[0].ID != golensSectionGlobal {
			t.Errorf("the first band is %q, want the Global band", merged.Sections[0].ID)
		}
		if _, ok := sectionByID(merged.Sections, golens.BlockAllMetrics); !ok {
			t.Error("the merged document lost All Metrics — the run panels' home")
		}
		sectionRefsOnce(t, merged.Sections)
	})

	t.Run("a settled document reports clean", func(t *testing.T) {
		merged, _ := golensMergeLayout(&golens.DashboardConfig{}, wantPanels, wantSections())
		again, dirty := golensMergeLayout(merged, wantPanels, wantSections())
		if dirty {
			t.Error("re-merging the merged document reported dirty — every boot would PUT the operator's document anew")
		}
		if !reflect.DeepEqual(merged, again) {
			t.Error("the second merge changed the document")
		}
	})

	t.Run("the operator's panels and bands pass through behind the wanted ones", func(t *testing.T) {
		cur := &golens.DashboardConfig{
			Version: 1,
			Panels:  []golens.DashboardPanel{{ID: "op-panel", Metric: "op_metric"}},
			Sections: []golens.DashboardSection{
				{ID: "my-band", Name: "Mine", Items: []string{"op-panel"}},
				{ID: golens.BlockAllMetrics, Name: "All Metrics", Items: []string{golens.BlockAllMetrics}},
			},
		}
		merged, dirty := golensMergeLayout(cur, wantPanels, wantSections())
		if !dirty {
			t.Fatal("a document missing every wanted part reported clean")
		}
		if _, ok := panelByID(merged.Panels, "op-panel"); !ok {
			t.Error("the operator's panel was dropped")
		}
		mine, ok := sectionByID(merged.Sections, "my-band")
		if !ok {
			t.Fatal("the operator's band was dropped")
		}
		if !slices.Equal(mine.Items, []string{"op-panel"}) {
			t.Errorf("the operator's band items = %v, want [op-panel]", mine.Items)
		}
		if n := len(merged.Sections); n != len(wantSections())+2 {
			t.Errorf("merged sections = %d, want the wanted bands then my-band and All Metrics", n)
		}
		sectionRefsOnce(t, merged.Sections)
	})

	t.Run("stale daemon-owned panels and bands are dropped", func(t *testing.T) {
		stale := workerMetricName(golensKIn, "w-gone")
		cur := &golens.DashboardConfig{
			Version: 1,
			Panels:  []golens.DashboardPanel{{ID: stale, Metric: stale}},
			Sections: []golens.DashboardSection{
				{ID: golensSectionWorker + "w-gone", Name: "Worker w-gone", Items: []string{stale}},
				{ID: golens.BlockAllMetrics, Name: "All Metrics", Items: []string{golens.BlockAllMetrics}},
			},
		}
		merged, dirty := golensMergeLayout(cur, wantPanels, wantSections())
		if !dirty {
			t.Error("carrying stale daemon-owned parts reported clean")
		}
		if _, ok := panelByID(merged.Panels, stale); ok {
			t.Error("the stale worker's panel survived the merge")
		}
		if _, ok := sectionByID(merged.Sections, golensSectionWorker+"w-gone"); ok {
			t.Error("the stale worker's band survived the merge")
		}
		sectionRefsOnce(t, merged.Sections)
	})

	t.Run("a run panel keeps its operator placement in the Runs band", func(t *testing.T) {
		run := "daedalus_run_w-a_t1_tokens_in_total"
		cur := &golens.DashboardConfig{
			Version: 1,
			Panels:  append(append([]golens.DashboardPanel{}, wantPanels...), golens.DashboardPanel{ID: run, Metric: run}),
			Sections: []golens.DashboardSection{
				{ID: golensSectionGlobal, Name: "Global", Items: []string{golensTokensIn}},
				{ID: golensSectionWorker + "w-a", Name: "Worker w-a", Items: []string{workerMetricName(golensKRequests, "w-a")}},
				{ID: golensSectionWorker + "w-a" + "-runs", Name: "Runs", Parent: golensSectionWorker + "w-a", Items: []string{run}},
				{ID: golens.BlockAllMetrics, Name: "All Metrics", Items: []string{golens.BlockAllMetrics}},
			},
		}
		merged, dirty := golensMergeLayout(cur, wantPanels, wantSections())
		if dirty {
			t.Error("a settled document with one operator run pin reported dirty — the pin would be rewritten every boot")
		}
		runs, ok := sectionByID(merged.Sections, golensSectionWorker+"w-a"+"-runs")
		if !ok {
			t.Fatal("the Runs band vanished")
		}
		if !slices.Equal(runs.Items, []string{run}) {
			t.Errorf("the Runs band items = %v, want the operator's run pin kept", runs.Items)
		}
		if _, ok := panelByID(merged.Panels, run); !ok {
			t.Error("the run panel left the document — its band reference would dangle")
		}
		sectionRefsOnce(t, merged.Sections)
	})

	t.Run("a daemon panel dragged into an operator band comes home", func(t *testing.T) {
		cur := &golens.DashboardConfig{
			Version: 1,
			Panels: append(append([]golens.DashboardPanel{}, wantPanels...),
				golens.DashboardPanel{ID: "op-panel", Metric: "op_metric"}),
			Sections: []golens.DashboardSection{
				{ID: "my-charts", Name: "My Charts", Items: []string{"op-panel", golensTokensIn}},
				{ID: golens.BlockAllMetrics, Name: "All Metrics", Items: []string{golens.BlockAllMetrics}},
			},
		}
		merged, dirty := golensMergeLayout(cur, wantPanels, wantSections())
		if !dirty {
			t.Error("stripping a dragged daemon panel reported clean")
		}
		mine, ok := sectionByID(merged.Sections, "my-charts")
		if !ok {
			t.Fatal("the operator's band was dropped")
		}
		if !slices.Equal(mine.Items, []string{"op-panel"}) {
			t.Errorf("the operator's own pins = %v, want [op-panel] untouched", mine.Items)
		}
		// Exactly one reference remains — its canonical daemon band. Two
		// would fail the PUT validator.
		n := 0
		for _, sec := range merged.Sections {
			if slices.Contains(sec.Items, golensTokensIn) {
				n++
			}
		}
		if n != 1 {
			t.Errorf("the dragged panel is referenced %d times after the merge, want once", n)
		}
		sectionRefsOnce(t, merged.Sections)
	})

	t.Run("a band parented under a dropped daemon band is promoted", func(t *testing.T) {
		cur := &golens.DashboardConfig{
			Version: 1,
			Sections: []golens.DashboardSection{
				{ID: golensSectionWorker + "w-gone", Name: "Worker w-gone"},
				{ID: "my-band", Name: "Mine", Parent: golensSectionWorker + "w-gone"},
				{ID: golens.BlockAllMetrics, Name: "All Metrics", Items: []string{golens.BlockAllMetrics}},
			},
		}
		merged, dirty := golensMergeLayout(cur, wantPanels, wantSections())
		if !dirty {
			t.Error("promoting a dangling band reported clean")
		}
		mine, ok := sectionByID(merged.Sections, "my-band")
		if !ok {
			t.Fatal("the operator's band was dropped")
		}
		if mine.Parent != "" {
			t.Errorf("my-band still parents under %q — the validator refuses a dangling parent", mine.Parent)
		}
	})
}

// TestGolensSyncLayoutSeesTheConfigVocabularies pins the boot sync against a
// real registry: the config-known worker's child metrics register typed (the
// duration a histogram, the rest counters), the alias and priced-model
// panels place, the document lands sectioned and persisted in the daemon
// dir, a hand-mangled nameless price places nothing, and a second sync
// leaves the document byte-stable — the face a restart meets.
func TestGolensSyncLayoutSeesTheConfigVocabularies(t *testing.T) {
	// setup builds one registry and server: an isolated daemon dir, the
	// named config, and an optional price table to swap in.
	setup := func(t *testing.T, cfg config.Config, prices map[string]modelPrice) (*golens.Registry, *uiServer) {
		t.Helper()
		useDaemonDir(t)
		reg, err := startGolens(golensMemoryConfig())
		if err != nil {
			t.Fatalf("startGolens: %v", err)
		}
		t.Cleanup(func() { reg.Close() })
		srv := newUIServer(cfg, &metricsrelay.Ledger{}, reg, nil)
		if prices != nil {
			srv.swapPrices(prices)
		}
		return reg, srv
	}
	sectioned := func() config.Config {
		cfg := config.Config{WorkerID: "w-a"}
		cfg.Provider = &config.ProviderConfig{KeyAlias: "k1"}
		return cfg
	}

	t.Run("the vocabularies register typed and the sections place", func(t *testing.T) {
		reg, srv := setup(t, sectioned(), map[string]modelPrice{"zB": {In: 1}, "mA": {In: 3}, "": {In: 9}})
		srv.golensSyncLayout()

		if snap, ok := reg.Snapshot(workerMetricName(golensKDuration, "w-a")); !ok || snap.Type != "histogram" {
			t.Errorf("the worker duration metric = (%q, ok %v), want a registered histogram", snap.Type, ok)
		}
		for _, sl := range golensSlices[:len(golensSlices)-1] {
			if _, ok := reg.Snapshot(workerMetricName(sl.kind, "w-a")); !ok {
				t.Errorf("the worker %s metric is not registered", sl.kind)
			}
			if _, ok := reg.Snapshot(aliasMetricName(sl.kind, "k1")); !ok {
				t.Errorf("the alias %s metric is not registered", sl.kind)
			}
		}
		for _, m := range []string{"mA", "zB"} {
			if _, ok := reg.Snapshot(modelCostMetricName(m)); !ok {
				t.Errorf("the cost metric for %s is not registered", m)
			}
		}
		if _, ok := reg.Snapshot(modelCostMetricName("")); ok {
			t.Error("the nameless price placed a cost metric")
		}

		doc, err := golensConfigDoc(reg, http.MethodGet, nil)
		if err != nil {
			t.Fatalf("read the layout document: %v", err)
		}
		global, ok := sectionByID(doc.Sections, golensSectionGlobal)
		if !ok {
			t.Fatal("the Global band is missing")
		}
		if want := []string{golensTokensIn, golensTokensOut, golensCacheRead, golensCacheWrite, golensRequests, golensDuration}; !slices.Equal(global.Items, want) {
			t.Errorf("Global items = %v, want %v", global.Items, want)
		}
		worker, ok := sectionByID(doc.Sections, golensSectionWorker+"w-a")
		if !ok {
			t.Fatal("the worker band is missing")
		}
		wantWorker := make([]string, 0, len(golensSlices))
		for _, sl := range golensSlices {
			wantWorker = append(wantWorker, workerMetricName(sl.kind, "w-a"))
		}
		if !slices.Equal(worker.Items, wantWorker) {
			t.Errorf("worker band items = %v, want %v", worker.Items, wantWorker)
		}
		runs, ok := sectionByID(doc.Sections, golensSectionWorker+"w-a"+"-runs")
		if !ok {
			t.Fatal("the Runs band is missing")
		}
		if runs.Parent != golensSectionWorker+"w-a" || len(runs.Items) != 0 {
			t.Errorf("the Runs band = (parent %q, items %v), want the empty drop target nested under the worker band", runs.Parent, runs.Items)
		}
		aliases, ok := sectionByID(doc.Sections, golensSectionAliases)
		if !ok {
			t.Fatal("the alias band is missing")
		}
		wantAlias := make([]string, 0, len(golensSlices)-1)
		for _, sl := range golensSlices[:len(golensSlices)-1] {
			wantAlias = append(wantAlias, aliasMetricName(sl.kind, "k1"))
		}
		if !slices.Equal(aliases.Items, wantAlias) {
			t.Errorf("alias band items = %v, want %v", aliases.Items, wantAlias)
		}
		prices, ok := sectionByID(doc.Sections, golensSectionPrices)
		if !ok {
			t.Fatal("the prices band is missing")
		}
		if want := []string{modelCostMetricName("mA"), modelCostMetricName("zB")}; !slices.Equal(prices.Items, want) {
			t.Errorf("prices band items = %v, want the models sorted %v", prices.Items, want)
		}
		// The file-backed layout store is retired: the document lives in
		// the registry (sqlite mode persists it into the shared db —
		// TestGolensLayoutPersistsInTheSharedDB), never in a file.
		if _, err := os.Stat(filepath.Join(daemonDir, "metrics-golens-layout.json")); !os.IsNotExist(err) {
			t.Errorf("the retired layout file was written in the daemon dir: %v", err)
		}
	})

	t.Run("a second sync leaves the document stable", func(t *testing.T) {
		reg, srv := setup(t, sectioned(), map[string]modelPrice{"mA": {In: 3}})
		srv.golensSyncLayout()
		before, err := golensConfigDoc(reg, http.MethodGet, nil)
		if err != nil {
			t.Fatalf("read the layout document: %v", err)
		}
		srv.golensSyncLayout()
		after, err := golensConfigDoc(reg, http.MethodGet, nil)
		if err != nil {
			t.Fatalf("read the layout document: %v", err)
		}
		if !reflect.DeepEqual(before, after) {
			t.Error("a second sync rewrote the document — a restart would PUT the operator's layout anew")
		}
	})

	t.Run("an empty deployment lays only the Global band", func(t *testing.T) {
		reg, srv := setup(t, config.Config{}, nil)
		srv.golensSyncLayout()
		if _, ok := reg.Snapshot(workerMetricName(golensKIn, "w-a")); ok {
			t.Error("a worker metric registered without a configured worker")
		}
		doc, err := golensConfigDoc(reg, http.MethodGet, nil)
		if err != nil {
			t.Fatalf("read the layout document: %v", err)
		}
		if n := len(doc.Sections); n != 1+len(golensBaseBands()) {
			t.Fatalf("sections = %d, want the Global band over the %d base bands", n, len(golensBaseBands()))
		}
		if doc.Sections[0].ID != golensSectionGlobal {
			t.Errorf("the first band is %q, want the Global band", doc.Sections[0].ID)
		}
		if _, ok := sectionByID(doc.Sections, golensSectionPrices); ok {
			t.Error("a prices band placed without a price table")
		}
	})
}

// TestGolensLayoutPersistsInTheSharedDB pins the layout document's home in
// sqlite mode: it persists into the shared metrics database (the module's
// DashboardConfigDB, the retired file store's replacement) and survives
// closing the registry and reopening it on the same file — the face a
// daemon restart meets — while the ledger's own store shares that file
// untouched: one db, several owners.
func TestGolensLayoutPersistsInTheSharedDB(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dbPath := filepath.Join(t.TempDir(), "shared-metrics.db")
	// The ledger's co-owner connection, open across the whole dance: the
	// layout writes must not disturb its table.
	ledger, err := metricsrelay.OpenStore(dbPath, 0)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { ledger.Close() })
	ledger.Append(metricsrelay.Entry{At: time.Now(), Worker: "w"})

	cfg := config.Config{}
	cfg.Metrics.StorageBackend = "sqlite"
	cfg.Metrics.StoragePath = dbPath

	reg, err := startGolens(cfg)
	if err != nil {
		t.Fatalf("startGolens: %v", err)
	}
	srv := newUIServer(cfg, &metricsrelay.Ledger{}, reg, nil)
	srv.golensSyncLayout()
	before, err := golensConfigDoc(reg, http.MethodGet, nil)
	if err != nil {
		t.Fatalf("read the layout document: %v", err)
	}
	if len(before.Sections) == 0 {
		t.Fatal("the sync pinned no sections")
	}
	if err := reg.Close(); err != nil {
		t.Fatalf("close the registry: %v", err)
	}

	reg2, err := startGolens(cfg)
	if err != nil {
		t.Fatalf("startGolens(restarted): %v", err)
	}
	defer reg2.Close()
	after, err := golensConfigDoc(reg2, http.MethodGet, nil)
	if err != nil {
		t.Fatalf("read the restarted layout document: %v", err)
	}
	if !reflect.DeepEqual(before.Sections, after.Sections) {
		t.Errorf("the restarted registry's sections differ:\nbefore %+v\nafter %+v", before.Sections, after.Sections)
	}

	// The co-owner's rows rode through the layout writes untouched.
	remaining := ledger.Ascending()
	if len(remaining) != 1 || remaining[0].Worker != "w" {
		t.Errorf("the ledger's rows after the layout persistence = %+v, want the one entry intact", remaining)
	}
}

// TestSaveModelsResyncsTheGolensLayout pins the save-side price gate end to
// end: a save re-runs the sync, so the newly priced model's cost panel takes
// its place in the persisted document, and the feed stamps its cost counter
// only from then on — entries fed before the save record none.
func TestSaveModelsResyncsTheGolensLayout(t *testing.T) {
	useDaemonDir(t)
	reg, err := startGolens(golensMemoryConfig())
	if err != nil {
		t.Fatalf("startGolens: %v", err)
	}
	defer reg.Close()
	led := &metricsrelay.Ledger{}
	srv := newUIServer(config.Config{WorkerID: "w-a"}, led, reg, nil)
	// The daemon's boot order (runMetricsDaemon): the layout sync registers
	// the worker's duration histogram before the relay can feed a sample.
	srv.golensSyncLayout()
	led.SetOnAppend(func(e metricsrelay.Entry) { srv.feedGolensEntry(e) })
	ts := httptest.NewServer(srv.routes())
	defer ts.Close()

	entry := metricsrelay.Entry{
		Worker: "w-a", Wire: metricsrelay.WireAnthropic, Model: "mA",
		InputTokens: 1_000_000, OutputTokens: 2_000_000, DurationMS: 100, Status: 200,
	}
	led.Append(entry)
	// The pre-save feed has folded: the duration histogram is that feed
	// call's last Record, so its convergence settles the cost-gate absence
	// below as a decision, not a race.
	awaitHistogram(t, reg, workerMetricName(golensKDuration, "w-a"), 1, 0.1)
	awaitMetricAbsent(t, reg, modelCostMetricName("mA"))

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.PostForm(ts.URL+"/models", url.Values{
		"in[mA]": {"3"}, "out[mA]": {"2"}, "cached[mA]": {"1.5"},
	})
	if err != nil {
		t.Fatalf("POST /models: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("the save answered %d, want the redirect to the form", resp.StatusCode)
	}

	doc, err := golensConfigDoc(reg, http.MethodGet, nil)
	if err != nil {
		t.Fatalf("read the layout document: %v", err)
	}
	prices, ok := sectionByID(doc.Sections, golensSectionPrices)
	if !ok {
		t.Fatal("the save left no Model prices band — the sync did not re-run")
	}
	if !slices.Contains(prices.Items, modelCostMetricName("mA")) {
		t.Errorf("the prices band items = %v, want the newly priced model's cost panel", prices.Items)
	}

	led.Append(entry)                                    // the post-save feed stamps the cost
	awaitCounter(t, reg, modelCostMetricName("mA"), 7.0) // 1M in at $3 + 2M out at $2
}
