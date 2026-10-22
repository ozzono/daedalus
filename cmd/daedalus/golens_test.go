package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

// awaitSnapshot polls the registry until the named metric has swallowed
// the samples fed so far — Record enqueues non-blockingly and the
// registry's own goroutine folds them, so a just-fed sample converges
// asynchronously.
func awaitSnapshot(t *testing.T, reg *golens.Registry, name string) golens.MetricSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, ok := reg.Snapshot(name)
		// A counter folds into Value (Count stays 0), a histogram into
		// Count/Sum — either nonzero means the sample landed.
		if ok && (snap.Count > 0 || snap.Value != 0) {
			return snap
		}
		if time.Now().After(deadline) {
			t.Fatalf("metric %s never converged (snapshot %+v, ok %v)", name, snap, ok)
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
// opens the sqlite store beside the daemon's files (creating the daemon
// dir a fresh host lacks) and registers the six ledger metrics up front
// so fed samples land typed; an explicit storage path relocates the store;
// the memory backend keeps no file; and the two boot failures name their
// cause — an unwritable daemon dir and an unopenable sqlite path.
func TestStartGolensBootFaces(t *testing.T) {
	sixMetrics := []string{
		golensTokensIn, golensTokensOut, golensCacheRead, golensCacheWrite,
		golensRequests, golensDuration,
	}

	t.Run("defaults create the sqlite store beside the daemon's files", func(t *testing.T) {
		dir := useDaemonDir(t)
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		reg, err := startGolens(config.Config{})
		if err != nil {
			t.Fatalf("startGolens: %v", err)
		}
		defer reg.Close()
		if _, err := os.Stat(filepath.Join(dir, "metrics-golens.db")); err != nil {
			t.Errorf("the default sqlite store is missing after boot: %v", err)
		}
		for _, name := range sixMetrics {
			if _, ok := reg.Snapshot(name); !ok {
				t.Errorf("metric %s is not registered at boot — a Record would auto-register it label-less", name)
			}
		}
	})

	t.Run("an explicit storage path relocates the store", func(t *testing.T) {
		dir := useDaemonDir(t)
		custom := filepath.Join(t.TempDir(), "relocated", "golens.db")
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
		if _, err := os.Stat(filepath.Join(dir, "metrics-golens.db")); !os.IsNotExist(err) {
			t.Errorf("the default store was created beside the explicit one: %v", err)
		}
	})

	t.Run("the memory backend keeps no file", func(t *testing.T) {
		dir := useDaemonDir(t)
		reg, err := startGolens(golensMemoryConfig())
		if err != nil {
			t.Fatalf("startGolens: %v", err)
		}
		defer reg.Close()
		if _, err := os.Stat(filepath.Join(dir, "metrics-golens.db")); !os.IsNotExist(err) {
			t.Errorf("the memory backend wrote a store file: %v", err)
		}
	})

	t.Run("an unwritable daemon dir fails the boot", func(t *testing.T) {
		dir := useDaemonDir(t)
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dir, []byte("not a dir"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := startGolens(config.Config{}); err == nil || !strings.Contains(err.Error(), "create daemon dir") {
			t.Errorf("startGolens over a blocked daemon dir = %v, want the mkdir diagnostic", err)
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

	feedGolensEntry(reg, metricsrelay.Entry{
		Worker: "w-a", Wire: metricsrelay.WireAnthropic, Model: "mA", Role: "dev", Alias: "k1",
		InputTokens: 100, OutputTokens: 200, CacheReadTokens: 30, CacheWriteTokens: 40,
		DurationMS: 1500, Status: 200,
	})
	// A failed call carrying no model: the unknown bucket, its own status.
	feedGolensEntry(reg, metricsrelay.Entry{
		Worker: "w-b", Status: 401, InputTokens: 5, OutputTokens: 6, DurationMS: 500,
	})

	for name, want := range map[string]float64{
		golensTokensIn:   105,
		golensTokensOut:  206,
		golensCacheRead:  30,
		golensCacheWrite: 40,
		golensRequests:   2,
	} {
		snap := awaitSnapshot(t, reg, name)
		if snap.Value != want {
			t.Errorf("%s = %v, want %v", name, snap.Value, want)
		}
	}

	dur := awaitSnapshot(t, reg, golensDuration)
	if dur.Count != 2 || dur.Sum != 2.0 {
		t.Errorf("duration histogram = (count %d, sum %v), want 2 samples summing 2.0s (1500ms + 500ms)", dur.Count, dur.Sum)
	}
	if dur.Type != "histogram" {
		t.Errorf("duration metric type = %q, want histogram", dur.Type)
	}

	// The declared label sets: tokens carry the bounded dimensions, the
	// request counter adds the HTTP status.
	in := awaitSnapshot(t, reg, golensTokensIn)
	if strings.Join(in.Labels, ",") != strings.Join(golensTokenLabels, ",") {
		t.Errorf("token labels = %v, want %v", in.Labels, golensTokenLabels)
	}
	reqs := awaitSnapshot(t, reg, golensRequests)
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
	srv := httptest.NewServer(newUIServer(config.Config{}, led, reg).routes())
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
	bare := httptest.NewServer(newUIServer(config.Config{}, led, nil).routes())
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
